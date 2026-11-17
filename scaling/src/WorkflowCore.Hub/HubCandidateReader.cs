using System.Text;
using Npgsql;
using NpgsqlTypes;

namespace WorkflowCore.Hub;

// One stream is one stable bucket and one state. These types are deliberately
// internal until the generation-scoped coordinator enforces process budgets.
internal enum HubCandidateStream { Ready, Expired, ReadyDue }
internal sealed record HubCandidateHorizon(long UpperSequence, DateTime Cutoff);
internal sealed record HubCandidateCursor(long Sequence, DateTime? Deadline = null);
internal sealed record HubTaskCandidate(Guid Id, long Sequence, string HubId, int PartitionId,
    string TaskType, int Version, Guid? WorkflowId, string? DefinitionId, int? DefinitionVersion,
    long Epoch, DateTime Deadline, string[] ResourceKeys, bool ResourceKeysComplete = true,
    bool WorkflowMetadataComplete = true, bool EnrichmentLoaded = true,
    DateTimeOffset WorkflowMetadataExpiresAt = default);
internal readonly record struct HubWorkflowCacheSnapshot(int Entries, long Bytes, long Hits, long Misses, long Evictions);
internal sealed record HubCandidateOversized(Guid Id, long Sequence, DateTime Deadline);
internal sealed record HubCandidatePage(IReadOnlyList<HubTaskCandidate> Items,
    HubCandidateCursor? Next, bool EndOfStream, int MetadataBytes,
    IReadOnlyList<HubCandidateOversized>? Oversized = null);
internal sealed record HubCandidateHydrationPage(IReadOnlyList<HubTaskCandidate> Items,
    int Oversized, int MetadataBytes);

internal interface IHubCandidateDiscovery
{
    Task<HubCandidateHorizon> CaptureHorizonAsync(string hub, CancellationToken ct);
    Task<HubCandidatePage> ReadPageAsync(string hub, int bucket, HubCandidateStream stream,
        HubCandidateHorizon horizon, HubCandidateCursor? cursor, int pageSize, int byteLimit, CancellationToken ct);
    Task<HubCandidateHydrationPage> HydrateAsync(string hub, int matchingPartition, int partitionCount,
        IReadOnlyList<Guid> ids, int byteLimit, CancellationToken ct);
}

internal sealed class HubCandidateReader : IHubCandidateDiscovery
{
    private readonly record struct WorkflowKey(string Hub, int MatchingPartition, Guid WorkflowId);
    private sealed record WorkflowMetadata(string? DefinitionId, int? DefinitionVersion, bool Complete, long Bytes, DateTimeOffset ExpiresAt);
    private sealed record WorkflowCacheNode(WorkflowKey Key, WorkflowMetadata Metadata);
    private sealed class WorkflowScope { public int Entries; public long Bytes; }
    private readonly HubTaskStore tasks;
    private readonly int _workflowCacheEntriesPerPartition;
    private readonly int _workflowCacheEntriesPerProcess;
    private readonly long _workflowCacheBytesPerPartition;
    private readonly long _workflowCacheBytesPerProcess;
    private readonly TimeSpan _workflowCacheTtl;
    private const int WorkflowDefinitionMaxUtf8Bytes = 4096;
    private readonly object _workflowCacheLock = new();
    private readonly Dictionary<WorkflowKey, LinkedListNode<WorkflowCacheNode>> _workflowCache = [];
    private readonly Dictionary<(string Hub, int MatchingPartition), WorkflowScope> _workflowScopes = [];
    private readonly LinkedList<WorkflowCacheNode> _workflowLru = [];
    private long _workflowCacheBytes;
    private long _workflowCacheHits;
    private long _workflowCacheMisses;
    private long _workflowCacheEvictions;

    internal HubCandidateReader(HubTaskStore tasks, int workflowCacheEntriesPerPartition = 256,
        int workflowCacheEntriesPerProcess = 4096, long workflowCacheBytesPerPartition = 1024 * 1024,
        long workflowCacheBytesPerProcess = 8 * 1024 * 1024, TimeSpan? workflowCacheTtl = null)
    {
        if (workflowCacheEntriesPerPartition < 1 || workflowCacheEntriesPerProcess < workflowCacheEntriesPerPartition ||
            workflowCacheBytesPerPartition < 512 || workflowCacheBytesPerProcess < workflowCacheBytesPerPartition)
            throw new ArgumentOutOfRangeException(nameof(workflowCacheEntriesPerPartition));
        this.tasks = tasks;
        _workflowCacheEntriesPerPartition = workflowCacheEntriesPerPartition;
        _workflowCacheEntriesPerProcess = workflowCacheEntriesPerProcess;
        _workflowCacheBytesPerPartition = workflowCacheBytesPerPartition;
        _workflowCacheBytesPerProcess = workflowCacheBytesPerProcess;
        _workflowCacheTtl = workflowCacheTtl ?? TimeSpan.FromSeconds(30);
        if (_workflowCacheTtl <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(workflowCacheTtl));
    }

    internal HubWorkflowCacheSnapshot WorkflowCacheSnapshot
    { get { lock (_workflowCacheLock) return new(_workflowCache.Count, _workflowCacheBytes,
        _workflowCacheHits, _workflowCacheMisses, _workflowCacheEvictions); } }

    internal const string WorkflowMetadataSql = """
        SELECT id,
               CASE WHEN octet_length(definition_id)<=@definitionByteLimit THEN definition_id END,
               definition_version,
               octet_length(definition_id)>@definitionByteLimit AS too_large
        FROM wfc_scale.hub_workflow
        WHERE hub_id=@hub AND id=ANY(@ids)
        """;

    internal const string HydrationSql = """
        WITH requested AS MATERIALIZED (
          SELECT unnest(@ids::uuid[]) AS id
        ), page AS MATERIALIZED (
          SELECT t.id,t.sequence,t.hub_id,t.partition_id,t.task_type,t.task_version,t.workflow_id,
                 t.epoch,t.due_at,t.lease_until,t.state
          FROM requested r JOIN wfc_scale.hub_task t ON t.id=r.id
          WHERE t.hub_id=@hub AND t.partition_id % @partitionCount=@matchingPartition
            AND t.attempts<t.max_attempts
            AND ((t.state=0 AND t.due_at<=clock_timestamp())
                 OR (t.state=1 AND t.lease_until<clock_timestamp()))
        ), budget AS MATERIALIZED (
          SELECT GREATEST(1,@byteLimit/GREATEST(count(*),1)) AS per_candidate FROM page
        ), resource_sizes AS MATERIALIZED (
          SELECT c.task_id,sum(octet_length(c.resource_key)+24) AS bytes
          FROM wfc_scale.hub_task_constraint c JOIN page p ON p.id=c.task_id GROUP BY c.task_id
        ), base_size AS MATERIALIZED (
          SELECT p.id,128+octet_length(p.hub_id)+octet_length(p.task_type) AS bytes,b.per_candidate
          FROM page p CROSS JOIN budget b
        ), resources AS MATERIALIZED (
          SELECT c.task_id,array_agg(c.resource_key ORDER BY c.resource_key) AS keys
          FROM wfc_scale.hub_task_constraint c JOIN resource_sizes s ON s.task_id=c.task_id
          JOIN base_size b ON b.id=c.task_id
          WHERE b.bytes<=b.per_candidate AND b.bytes+s.bytes<=b.per_candidate
          GROUP BY c.task_id
        )
        SELECT p.id,p.sequence,CASE WHEN b.bytes<=b.per_candidate THEN p.hub_id END,p.partition_id,
               CASE WHEN b.bytes<=b.per_candidate THEN p.task_type END,p.task_version,p.workflow_id,
               p.epoch,
               CASE WHEN p.lease_until<clock_timestamp() AND p.state=1 THEN p.lease_until ELSE p.due_at END AS deadline,
               COALESCE(r.keys,ARRAY[]::text[]),COALESCE(s.bytes,0),
               b.bytes>b.per_candidate AS oversized,
               COALESCE(r.keys IS NOT NULL OR s.bytes IS NULL OR s.bytes=0,FALSE)
        FROM page p JOIN base_size b ON b.id=p.id
        LEFT JOIN resource_sizes s ON s.task_id=p.id LEFT JOIN resources r ON r.task_id=p.id
        ORDER BY p.sequence,p.id
        """;

    public async Task<HubCandidateHorizon> CaptureHorizonAsync(string hub, CancellationToken ct)
    {
        // The global committed maximum is a conservative finite bound for any
        // hub and uses the existing unique sequence index, without scanning
        // historical rows belonging to that hub or adding another index.
        await using var command = tasks.DataSource.CreateCommand("""
            SELECT COALESCE(max(sequence),0), clock_timestamp()
            FROM wfc_scale.hub_task
            """);
        await using var reader = await command.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return new(reader.GetInt64(0), reader.GetDateTime(1));
    }

    // Shared with the prepared-plan regression test so it explains the exact
    // materialized page and expansion used by production discovery.
    internal static string BuildPageSql(HubCandidateStream stream)
    {
        var ready = stream is HubCandidateStream.Ready or HubCandidateStream.ReadyDue;
        var byDeadline = stream is HubCandidateStream.Expired or HubCandidateStream.ReadyDue;
        if (!ready && stream != HubCandidateStream.Expired) throw new ArgumentException("Invalid stream");
        return """
            WITH page AS MATERIALIZED (
              SELECT id,sequence,hub_id,partition_id,task_type,task_version,workflow_id,epoch,
                     /*deadline*/ AS deadline
              FROM wfc_scale.hub_task
              WHERE hub_id=@hub AND partition_id % 64=@bucket AND attempts < max_attempts
                AND sequence<=@upper AND /*predicate*/
              ORDER BY /*ordering*/ LIMIT @limit
            ), page_budget AS MATERIALIZED (
              SELECT GREATEST(1,@byteLimit/GREATEST(@limit,1)) AS per_candidate
            )
            SELECT p.id,p.sequence,
                   CASE WHEN 128+octet_length(p.hub_id)+octet_length(p.task_type)<=b.per_candidate THEN p.hub_id END,
                   p.partition_id,
                   CASE WHEN 128+octet_length(p.hub_id)+octet_length(p.task_type)<=b.per_candidate THEN p.task_type END,
                   p.task_version,p.workflow_id,p.epoch,p.deadline,
                   128+octet_length(p.hub_id)+octet_length(p.task_type)>b.per_candidate AS too_large
            FROM page p CROSS JOIN page_budget b ORDER BY /*outer_order*/
            """.Replace("/*deadline*/", ready ? "due_at" : "lease_until")
            .Replace("/*predicate*/", stream switch {
                HubCandidateStream.Ready => "state=0 AND due_at<=@cutoff AND sequence>@after",
                HubCandidateStream.ReadyDue => "state=0 AND due_at<=@cutoff AND (due_at,sequence)>(@afterDeadline,@after)",
                _ => "state=1 AND lease_until<@cutoff AND (lease_until,sequence)>(@afterDeadline,@after)" })
            .Replace("/*ordering*/", !byDeadline ? "sequence" : ready ? "due_at,sequence" : "lease_until,sequence")
            .Replace("/*outer_order*/", !byDeadline ? "p.sequence" : "p.deadline,p.sequence");
    }

    public async Task<HubCandidatePage> ReadPageAsync(string hub, int bucket, HubCandidateStream stream,
        HubCandidateHorizon horizon, HubCandidateCursor? cursor, int pageSize, int byteLimit, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(hub) || bucket is < 0 or >= 64 || pageSize is < 1 or > 1024 ||
            byteLimit is < 256 or > 16 * 1024 * 1024 || horizon.UpperSequence < 0 ||
            (stream != HubCandidateStream.Ready && cursor is not null && cursor.Deadline is null))
            throw new ArgumentException("Invalid candidate page request");
        var byDeadline = stream is HubCandidateStream.Expired or HubCandidateStream.ReadyDue;
        var sql = BuildPageSql(stream);
        await using var command = tasks.DataSource.CreateCommand(sql);
        command.Parameters.AddWithValue("hub", hub);
        command.Parameters.AddWithValue("bucket", bucket);
        command.Parameters.AddWithValue("upper", horizon.UpperSequence);
        command.Parameters.AddWithValue("cutoff", horizon.Cutoff);
        command.Parameters.AddWithValue("after", cursor?.Sequence ?? 0L);
        // Reserve at least 256 bytes per returned row so both the SQL-side
        // text guard and the managed cursor can account for one oversized
        // marker or one normal base row at the minimum legal budget.
        var effectivePageSize = Math.Min(pageSize, Math.Max(1, byteLimit / 256));
        command.Parameters.AddWithValue("limit", effectivePageSize);
        command.Parameters.AddWithValue("byteLimit", byteLimit);
        if (byDeadline) command.Parameters.Add("afterDeadline", NpgsqlDbType.TimestampTz).Value = cursor?.Deadline ?? DateTime.SpecifyKind(DateTime.MinValue, DateTimeKind.Utc);
        var items = new List<HubTaskCandidate>();
        var oversized = new List<HubCandidateOversized>();
        var bytes = 0;
        var rows = 0;
        var interrupted = false;
        HubCandidateCursor? lastProcessed = cursor;
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var sequence = reader.GetInt64(1);
            var deadline = reader.GetDateTime(8);
            if (reader.GetBoolean(9))
            {
                const int markerSize = 64;
                if (bytes + markerSize > byteLimit) { interrupted = true; break; }
                oversized.Add(new(reader.GetGuid(0), sequence, deadline));
                bytes += markerSize;
            }
            else
            {
                // Discovery pages transport only bounded task identity/routing
                // fields. Workflow/resource enrichment is fetched only for new
                // candidate IDs; cached entries are reused by the registry.
                var hubId = reader.GetString(2);
                var taskType = reader.GetString(4);
                Guid? workflowId = reader.IsDBNull(6) ? null : reader.GetGuid(6);
                var item = new HubTaskCandidate(reader.GetGuid(0), sequence, hubId,
                    reader.GetInt32(3), taskType, reader.GetInt32(5), workflowId,
                    null, null, reader.GetInt64(7), deadline, [], ResourceKeysComplete: false,
                    WorkflowMetadataComplete: !workflowId.HasValue, EnrichmentLoaded: false);
                var size = 128 + Encoding.UTF8.GetByteCount(item.HubId) + Encoding.UTF8.GetByteCount(item.TaskType);
                if (bytes + size > byteLimit) { interrupted = true; break; }
                items.Add(item);
                bytes += size;
            }
            rows++;
            lastProcessed = new(sequence, byDeadline ? deadline : null);
        }
        return new(items, lastProcessed, !interrupted && rows < effectivePageSize, bytes, oversized);

    }

    public async Task<HubCandidateHydrationPage> HydrateAsync(string hub, int matchingPartition,
        int partitionCount, IReadOnlyList<Guid> ids, int byteLimit, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(hub) || matchingPartition < 0 || partitionCount is < 1 or > 64 ||
            64 % partitionCount != 0 || matchingPartition >= partitionCount || ids.Count is < 1 or > 1024 ||
            byteLimit is < 256 or > 16 * 1024 * 1024)
            throw new ArgumentException("Invalid candidate hydration request");

        var requestedIds = ids.Distinct().ToArray();
        if (requestedIds.Length > byteLimit / 16)
            throw new ArgumentException("Candidate hydration ID array exceeds its byte budget", nameof(ids));
        // UUID parameters consume managed memory too; reserve their exact wire
        // size before SQL divides the remaining metadata budget across eligible rows.
        var batchMetadataLimit = HydrationMetadataBudget(byteLimit, requestedIds.Length);
        var resourceBudget = batchMetadataLimit;
        await using var command = tasks.DataSource.CreateCommand(HydrationSql);
        command.Parameters.AddWithValue("ids", requestedIds);
        command.Parameters.AddWithValue("hub", hub);
        command.Parameters.AddWithValue("matchingPartition", matchingPartition);
        command.Parameters.AddWithValue("partitionCount", partitionCount);
        command.Parameters.AddWithValue("byteLimit", resourceBudget);

        var items = new List<HubTaskCandidate>(requestedIds.Length);
        long bytes = 0;
        var oversized = 0;
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (reader.GetBoolean(11)) { oversized++; continue; }
            var keysComplete = reader.GetBoolean(12);
            var keys = keysComplete ? reader.GetFieldValue<string[]>(9) : [];
            var item = new HubTaskCandidate(reader.GetGuid(0), reader.GetInt64(1), reader.GetString(2),
                reader.GetInt32(3), reader.GetString(4), reader.GetInt32(5), reader.IsDBNull(6) ? null : reader.GetGuid(6),
                null, null, reader.GetInt64(7), reader.GetDateTime(8), keys, keysComplete,
                WorkflowMetadataComplete: reader.IsDBNull(6), EnrichmentLoaded: true,
                WorkflowMetadataExpiresAt: reader.IsDBNull(6) ? default : DateTimeOffset.UtcNow + _workflowCacheTtl);
            var size = 128 + Encoding.UTF8.GetByteCount(item.HubId) + Encoding.UTF8.GetByteCount(item.TaskType) +
                keys.Sum(k => 24 + Encoding.UTF8.GetByteCount(k));
            if (bytes + size > resourceBudget)
                throw new InvalidOperationException("Candidate hydration violated its bounded SQL metadata contract");
            bytes += size;
            items.Add(item);
        }
        var workflowBudget = Math.Max(0, batchMetadataLimit - checked((int)bytes));
        var workflows = await ResolveWorkflowMetadataAsync(hub, matchingPartition,
            items.Where(x => x.WorkflowId.HasValue).Select(x => x.WorkflowId!.Value).Distinct().ToArray(),
            workflowBudget, ct);
        for (var i = 0; i < items.Count; i++)
        {
            var item = items[i];
            if (item.WorkflowId is not { } workflowId) continue;
            if (workflows.TryGetValue(workflowId, out var metadata) && bytes + metadata.Bytes <= batchMetadataLimit)
            {
                items[i] = item with { DefinitionId = metadata.DefinitionId,
                    DefinitionVersion = metadata.DefinitionVersion, WorkflowMetadataComplete = metadata.Complete,
                    WorkflowMetadataExpiresAt = metadata.ExpiresAt };
                bytes += metadata.Bytes;
            }
        }
        if (bytes > batchMetadataLimit)
            throw new InvalidOperationException("Candidate hydration exceeded its combined metadata budget");
        return new(items, oversized, checked((int)bytes));
    }

    private async Task<Dictionary<Guid, WorkflowMetadata>> ResolveWorkflowMetadataAsync(string hub,
        int matchingPartition, Guid[] ids, int byteBudget, CancellationToken ct)
    {
        if (ids.Length == 0) return [];
        var result = new Dictionary<Guid, WorkflowMetadata>(ids.Length);
        var missing = new List<Guid>();
        var now = DateTimeOffset.UtcNow;
        lock (_workflowCacheLock)
        {
            foreach (var id in ids)
            {
                var key = new WorkflowKey(hub, matchingPartition, id);
                if (_workflowCache.TryGetValue(key, out var node) && node.Value.Metadata.ExpiresAt > now)
                {
                    _workflowLru.Remove(node); _workflowLru.AddFirst(node);
                    result[id] = node.Value.Metadata;
                    _workflowCacheHits++;
                    HubTelemetry.CandidateWorkflowCacheHits.Add(1);
                }
                else
                {
                    if (node is not null) RemoveWorkflowNodeLocked(node);
                    missing.Add(id);
                    _workflowCacheMisses++;
                    HubTelemetry.CandidateWorkflowCacheMisses.Add(1);
                }
            }
        }
        if (missing.Count == 0) return result;
        var perDefinitionLimit = missing.Count == 0 ? 0 : Math.Max(0, byteBudget / missing.Count - 32);
        perDefinitionLimit = Math.Min(perDefinitionLimit, WorkflowDefinitionMaxUtf8Bytes);
        if (perDefinitionLimit <= 0)
        {
            foreach (var id in missing) result[id] = new(null, null, false, 0, DateTimeOffset.UtcNow + _workflowCacheTtl);
            return result;
        }
        await using var command = tasks.DataSource.CreateCommand(WorkflowMetadataSql);
        HubTelemetry.CandidateWorkflowMetadataCalls.Add(1);
        command.Parameters.AddWithValue("hub", hub);
        command.Parameters.AddWithValue("ids", missing.ToArray());
        command.Parameters.AddWithValue("definitionByteLimit", perDefinitionLimit);
        var loaded = new Dictionary<Guid, WorkflowMetadata>();
        var workflowRows = 0;
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                workflowRows++;
                if (reader.GetBoolean(3) || reader.IsDBNull(1) || reader.IsDBNull(2))
                {
                    loaded[reader.GetGuid(0)] = new(null, null, false, 0, DateTimeOffset.UtcNow + _workflowCacheTtl);
                    continue;
                }
                var definition = reader.GetString(1);
                var size = Encoding.UTF8.GetByteCount(definition) + 8L;
                if (size > perDefinitionLimit)
                    loaded[reader.GetGuid(0)] = new(null, null, false, 0, DateTimeOffset.UtcNow + _workflowCacheTtl);
                else loaded[reader.GetGuid(0)] = new(definition, reader.GetInt32(2), true, size,
                    DateTimeOffset.UtcNow + _workflowCacheTtl);
            }
        }
        HubTelemetry.CandidateWorkflowMetadataRows.Add(workflowRows);
        foreach (var id in missing)
            if (!loaded.ContainsKey(id)) loaded[id] = new(null, null, false, 0, DateTimeOffset.UtcNow + _workflowCacheTtl);
        lock (_workflowCacheLock)
        {
            foreach (var pair in loaded)
            {
                var key = new WorkflowKey(hub, matchingPartition, pair.Key);
                AddWorkflowNodeLocked(key, pair.Value);
                result[pair.Key] = pair.Value;
            }
        }
        return result;
    }

    private void AddWorkflowNodeLocked(WorkflowKey key, WorkflowMetadata metadata)
    {
        var scopeKey = (key.Hub, key.MatchingPartition);
        var size = metadata.Bytes + 128 + Encoding.UTF8.GetByteCount(key.Hub);
        while (_workflowCache.ContainsKey(key)) RemoveWorkflowNodeLocked(_workflowCache[key]);
        while (true)
        {
            _workflowScopes.TryGetValue(scopeKey, out var scope);
            var overProcess = _workflowCache.Count >= _workflowCacheEntriesPerProcess ||
                _workflowCacheBytes + size > _workflowCacheBytesPerProcess;
            var overScope = scope is not null && (scope.Entries >= _workflowCacheEntriesPerPartition ||
                scope.Bytes + size > _workflowCacheBytesPerPartition);
            if (!overProcess && !overScope) break;
            if (_workflowLru.Last is not { } last) return;
            RemoveWorkflowNodeLocked(last);
        }
        if (size > _workflowCacheBytesPerPartition || size > _workflowCacheBytesPerProcess) return;
        if (!_workflowScopes.TryGetValue(scopeKey, out var targetScope))
            _workflowScopes.Add(scopeKey, targetScope = new WorkflowScope());
        var node = _workflowLru.AddFirst(new WorkflowCacheNode(key, metadata));
        _workflowCache.Add(key, node); _workflowCacheBytes += size;
        targetScope.Entries++; targetScope.Bytes += size;
    }

    private void RemoveWorkflowNodeLocked(LinkedListNode<WorkflowCacheNode> node)
    {
        var value = node.Value;
        _workflowLru.Remove(node); _workflowCache.Remove(value.Key); _workflowCacheEvictions++;
        HubTelemetry.CandidateWorkflowCacheEvictions.Add(1);
        var size = value.Metadata.Bytes + 128 + Encoding.UTF8.GetByteCount(value.Key.Hub);
        _workflowCacheBytes -= size;
        var scopeKey = (value.Key.Hub, value.Key.MatchingPartition);
        if (_workflowScopes.TryGetValue(scopeKey, out var scope))
        {
            scope.Entries--; scope.Bytes -= size;
            if (scope.Entries == 0) _workflowScopes.Remove(scopeKey);
        }
    }

    // Kept beside the SQL so plan reproduction and production use precisely
    // the same parameter budget after UUID array wire bytes are reserved.
    internal static int HydrationMetadataBudget(int byteLimit, int requestedIdCount)
    {
        if (requestedIdCount < 1 || byteLimit < requestedIdCount * 16)
            throw new ArgumentOutOfRangeException(nameof(requestedIdCount));
        return byteLimit - requestedIdCount * 16;
    }
}

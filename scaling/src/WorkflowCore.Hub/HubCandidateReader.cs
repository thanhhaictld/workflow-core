using System.Text;
using Npgsql;
using NpgsqlTypes;

namespace WorkflowCore.Hub;

// One stream is one stable bucket and one state. These types are deliberately
// internal until the generation-scoped coordinator enforces process budgets.
internal enum HubCandidateStream { Ready, Expired, ReadyDue }
internal sealed record HubCandidateHorizon(long UpperSequence, DateTime Cutoff, bool HasExpiredCandidates);
internal sealed record HubCandidateCursor(long Sequence, DateTime? Deadline = null);
internal sealed record HubTaskCandidate(Guid Id, long Sequence, string HubId, int PartitionId,
    string TaskType, int Version, Guid? WorkflowId, string? DefinitionId, int? DefinitionVersion,
    long Epoch, DateTime Deadline,
    bool WorkflowMetadataComplete = true,
    DateTimeOffset WorkflowMetadataExpiresAt = default);
internal readonly record struct HubWorkflowCacheSnapshot(int Entries, long Bytes, long Hits, long Misses, long Evictions, long LookupCalls);
internal sealed record HubCandidateOversized(Guid Id, long Sequence, DateTime Deadline);
internal sealed record HubCandidatePage(IReadOnlyList<HubTaskCandidate> Items,
    HubCandidateCursor? Next, bool EndOfStream, int MetadataBytes,
    IReadOnlyList<HubCandidateOversized>? Oversized = null,
    Exception? WorkflowMetadataError = null);
internal sealed record HubCandidateHydrationPage(IReadOnlyList<HubTaskCandidate> Items,
    int Oversized, int MetadataBytes, Exception? WorkflowMetadataError = null,
    IReadOnlyList<Guid>? OversizedIds = null);

internal interface IHubCandidateDiscovery
{
    Task<HubCandidateHorizon> CaptureHorizonAsync(string hub, int matchingPartition, int partitionCount, CancellationToken ct);
    Task<HubCandidatePage> ReadPageAsync(string hub, int bucket, HubCandidateStream stream,
        HubCandidateHorizon horizon, HubCandidateCursor? cursor, int pageSize, int byteLimit, CancellationToken ct);
    Task<HubCandidateHydrationPage> HydrateAsync(string hub, int matchingPartition, int partitionCount,
        IReadOnlyList<Guid> ids, int byteLimit, CancellationToken ct);
}

internal sealed class HubCandidateReader : IHubCandidateDiscovery
{
    private readonly record struct WorkflowKey(string Hub, Guid WorkflowId);
    private sealed record WorkflowMetadata(string? DefinitionId, int? DefinitionVersion, bool Complete,
        long WireBytes, long ManagedBytes, DateTimeOffset ExpiresAt);
    private sealed record WorkflowCacheNode(WorkflowKey Key, WorkflowMetadata Metadata, HashSet<int> Partitions);
    private sealed class WorkflowScope { public int Entries; public long Bytes; }
    private sealed record WorkflowMetadataResolution(Dictionary<Guid, WorkflowMetadata> Values, Exception? Error);
    private readonly HubTaskStore tasks;
    private readonly int _workflowCacheEntriesPerPartition;
    private readonly int _workflowCacheEntriesPerProcess;
    private readonly long _workflowCacheBytesPerPartition;
    private readonly long _workflowCacheBytesPerProcess;
    private readonly TimeSpan _workflowCacheTtl;
    private const int WorkflowDefinitionMaxUtf8Bytes = 4096;
    private const long WorkflowNodeManagedBytes = 256;
    // Charge scope bookkeeping per entry, conservatively overcounting shared scopes.
    private const long WorkflowScopeManagedBytes = 128;
    private readonly object _workflowCacheLock = new();
    // A single bounded metadata lookup prevents concurrent cold batches from
    // duplicating the same workflow probes. The cache is checked after taking
    // the gate so a waiter observes the first batch's results.
    private readonly SemaphoreSlim _workflowFetchGate = new(1, 1);
    private readonly Dictionary<WorkflowKey, LinkedListNode<WorkflowCacheNode>> _workflowCache = [];
    private readonly Dictionary<(string Hub, int Partition), WorkflowScope> _workflowScopes = [];
    private readonly LinkedList<WorkflowCacheNode> _workflowLru = [];
    private long _workflowCacheBytes;
    private long _workflowCacheHits;
    private long _workflowCacheMisses;
    private long _workflowCacheEvictions;
    private long _workflowLookupCalls;

    internal HubCandidateReader(HubTaskStore tasks, int workflowCacheEntriesPerPartition = 10000,
        int workflowCacheEntriesPerProcess = 10000, long workflowCacheBytesPerPartition = 32 * 1024 * 1024,
        long workflowCacheBytesPerProcess = 32 * 1024 * 1024, TimeSpan? workflowCacheTtl = null,
        bool? metadataEnrichmentEnabled = null)
    {
        if (workflowCacheEntriesPerPartition < 1 || workflowCacheEntriesPerProcess < workflowCacheEntriesPerPartition ||
            workflowCacheBytesPerPartition < 512 || workflowCacheBytesPerProcess < workflowCacheBytesPerPartition)
            throw new ArgumentOutOfRangeException(nameof(workflowCacheEntriesPerPartition));
        this.tasks = tasks;
        // Retain the optional argument for source compatibility with existing
        // internal callers; candidate discovery now always uses identity pages.
        _workflowCacheEntriesPerPartition = workflowCacheEntriesPerPartition;
        _workflowCacheEntriesPerProcess = workflowCacheEntriesPerProcess;
        _workflowCacheBytesPerPartition = workflowCacheBytesPerPartition;
        _workflowCacheBytesPerProcess = workflowCacheBytesPerProcess;
        _workflowCacheTtl = workflowCacheTtl ?? TimeSpan.FromMinutes(10);
        if (_workflowCacheTtl <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(workflowCacheTtl));
    }

    internal bool MetadataEnrichmentEnabled => true;
    internal static bool ParseMetadataEnrichmentFlag(string? value) =>
        value is "true" or "1" or "True";

    internal HubWorkflowCacheSnapshot WorkflowCacheSnapshot
    { get { lock (_workflowCacheLock) return new(_workflowCache.Count, _workflowCacheBytes,
        _workflowCacheHits, _workflowCacheMisses, _workflowCacheEvictions, _workflowLookupCalls); } }

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
        )
        SELECT p.id,p.sequence,CASE WHEN 128+octet_length(p.hub_id)+octet_length(p.task_type)<=@perCandidateByteLimit THEN p.hub_id END,p.partition_id,
               CASE WHEN 128+octet_length(p.hub_id)+octet_length(p.task_type)<=@perCandidateByteLimit THEN p.task_type END,p.task_version,p.workflow_id,
               p.epoch,
               CASE WHEN p.lease_until<clock_timestamp() AND p.state=1 THEN p.lease_until ELSE p.due_at END AS deadline,
               128+octet_length(p.hub_id)+octet_length(p.task_type)>@perCandidateByteLimit AS oversized
        FROM page p
        ORDER BY p.sequence,p.id
        """;

    public async Task<HubCandidateHorizon> CaptureHorizonAsync(string hub, int matchingPartition, int partitionCount, CancellationToken ct)
    {
        // Both lookups use the same statement snapshot, so any lease visible to
        // EXISTS is already within the global sequence bound. The statement
        // timestamp gives the probe and subsequent pages the same cutoff.
        await using var command = tasks.DataSource.CreateCommand("""
            SELECT COALESCE((SELECT max(sequence) FROM wfc_scale.hub_task),0),
                   statement_timestamp(),
                   EXISTS (
                       SELECT 1
                       FROM wfc_scale.hub_task
                       WHERE hub_id=@hub AND partition_id % @partitionCount=@matchingPartition
                         AND state=1 AND attempts < max_attempts AND lease_until<statement_timestamp()
                   )
            """);
        command.Parameters.AddWithValue("hub", hub);
        command.Parameters.AddWithValue("matchingPartition", matchingPartition);
        command.Parameters.AddWithValue("partitionCount", partitionCount);
        await using var reader = await command.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return new(reader.GetInt64(0), reader.GetDateTime(1), reader.GetBoolean(2));
    }

    // One bounded keyset page returns eligible task identity.
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
            )
            SELECT p.id,p.sequence,
                   CASE WHEN 128+octet_length(p.hub_id)+octet_length(p.task_type)<=@byteLimit THEN p.hub_id END,
                   p.partition_id,
                   CASE WHEN 128+octet_length(p.hub_id)+octet_length(p.task_type)<=@byteLimit THEN p.task_type END,
                   p.task_version,p.workflow_id,p.epoch,p.deadline,
                   128+octet_length(p.hub_id)+octet_length(p.task_type)>@byteLimit AS too_large
            FROM page p
            ORDER BY /*outer_order*/
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
        command.Parameters.AddWithValue("limit", pageSize);
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
                var hubId = reader.GetString(2);
                var taskType = reader.GetString(4);
                Guid? workflowId = reader.IsDBNull(6) ? null : reader.GetGuid(6);
                var item = new HubTaskCandidate(reader.GetGuid(0), sequence, hubId,
                    reader.GetInt32(3), taskType, reader.GetInt32(5), workflowId,
                    null, null, reader.GetInt64(7), deadline,
                    WorkflowMetadataComplete: !IsWorkflowTask(taskType));
                var size = 128 + Encoding.UTF8.GetByteCount(item.HubId) + Encoding.UTF8.GetByteCount(item.TaskType);
                if (bytes + size > byteLimit) { interrupted = true; break; }
                items.Add(item);
                bytes += size;
            }
            rows++;
            lastProcessed = new(sequence, byDeadline ? deadline : null);
        }
        // Metadata lookup may need another pooled connection. Release this
        // reader before it starts, including when the page was interrupted.
        await reader.CloseAsync();
        await command.DisposeAsync();
        Exception? workflowMetadataError = null;
        var workflowIds = items.Where(x => x.WorkflowId.HasValue && IsWorkflowTask(x.TaskType))
            .Select(x => x.WorkflowId!.Value).Distinct().ToArray();
        if (workflowIds.Length > 0)
        {
            var resolution = await ResolveWorkflowMetadataAsync(hub, bucket, workflowIds,
                Math.Max(0, byteLimit - bytes), ct);
            workflowMetadataError = resolution.Error;
            for (var i = 0; i < items.Count; i++)
            {
                if (items[i].WorkflowId is not { } workflowId || !resolution.Values.TryGetValue(workflowId, out var metadata)) continue;
                if (metadata.WireBytes > 0 && bytes + metadata.WireBytes > byteLimit) continue;
                items[i] = items[i] with { DefinitionId = metadata.DefinitionId,
                    DefinitionVersion = metadata.DefinitionVersion, WorkflowMetadataComplete = metadata.Complete,
                    WorkflowMetadataExpiresAt = metadata.ExpiresAt };
                bytes = checked(bytes + (int)metadata.WireBytes);
            }
        }
        return new(items, lastProcessed, !interrupted && rows < pageSize, bytes, oversized, workflowMetadataError);

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
        // size and split the remaining batch budget into conservative per-row guards.
        var batchMetadataLimit = HydrationMetadataBudget(byteLimit, requestedIds.Length);
        var result = await HydrateBatchAsync(requestedIds, batchMetadataLimit);
        var items = result.Items.ToList();
        var bytes = result.MetadataBytes;
        var metadataError = result.WorkflowMetadataError;
        var pending = result.OversizedIds?.ToArray() ?? [];
        // The first pass shares a conservative row cap across the full page.
        // Retry rows that only exceeded that share in successively smaller,
        // bounded batches, charging each retry to the remaining page budget.
        var retryBatchSize = Math.Max(1, pending.Length / 2);
        while (pending.Length > 0 && retryBatchSize > 0)
        {
            var nextPending = new List<Guid>();
            var recovered = 0;
            for (var offset = 0; offset < pending.Length; offset += retryBatchSize)
            {
                var batch = pending.Skip(offset).Take(retryBatchSize).ToArray();
                var remaining = batchMetadataLimit - bytes;
                if (remaining < 128) { nextPending.AddRange(batch); continue; }
                var retried = await HydrateBatchAsync(batch, remaining);
                items.AddRange(retried.Items);
                bytes = checked(bytes + retried.MetadataBytes);
                metadataError ??= retried.WorkflowMetadataError;
                var oversizedIds = retried.OversizedIds ?? [];
                nextPending.AddRange(oversizedIds);
                recovered += batch.Length - oversizedIds.Count;
            }
            if (nextPending.Count == 0) { pending = []; break; }
            if (recovered == 0)
            {
                if (retryBatchSize == 1) { pending = nextPending.ToArray(); break; }
                retryBatchSize = Math.Max(1, retryBatchSize / 2);
            }
            pending = nextPending.ToArray();
        }
        if (bytes > batchMetadataLimit)
            throw new InvalidOperationException("Candidate hydration exceeded its combined metadata budget");
        return new(items, pending.Length, checked((int)bytes), metadataError, pending);

        async Task<HubCandidateHydrationPage> HydrateBatchAsync(Guid[] batchIds, int metadataBudget)
        {
            var batchItems = new List<HubTaskCandidate>(batchIds.Length);
            long batchBytes = 0;
            var oversizedIds = new List<Guid>();
            {
                await using var command = tasks.DataSource.CreateCommand(HydrationSql);
                command.Parameters.AddWithValue("ids", batchIds);
                command.Parameters.AddWithValue("hub", hub);
                command.Parameters.AddWithValue("matchingPartition", matchingPartition);
                command.Parameters.AddWithValue("partitionCount", partitionCount);
                command.Parameters.AddWithValue("perCandidateByteLimit", Math.Max(0, metadataBudget / batchIds.Length));
                await using var reader = await command.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    if (reader.GetBoolean(9)) { oversizedIds.Add(reader.GetGuid(0)); continue; }
                    var item = new HubTaskCandidate(reader.GetGuid(0), reader.GetInt64(1), reader.GetString(2),
                        reader.GetInt32(3), reader.GetString(4), reader.GetInt32(5), reader.IsDBNull(6) ? null : reader.GetGuid(6),
                        null, null, reader.GetInt64(7), reader.GetDateTime(8),
                        WorkflowMetadataComplete: !IsWorkflowTask(reader.GetString(4)),
                        WorkflowMetadataExpiresAt: IsWorkflowTask(reader.GetString(4))
                            ? DateTimeOffset.UtcNow + _workflowCacheTtl : default);
                    var size = 128 + Encoding.UTF8.GetByteCount(item.HubId) + Encoding.UTF8.GetByteCount(item.TaskType);
                    if (batchBytes + size > metadataBudget)
                        throw new InvalidOperationException("Candidate hydration violated its bounded SQL metadata contract");
                    batchBytes += size;
                    batchItems.Add(item);
                }
            }
            var workflowBudget = Math.Max(0, metadataBudget - checked((int)batchBytes));
            var workflowResolution = await ResolveWorkflowMetadataAsync(hub, matchingPartition,
                batchItems.Where(x => x.WorkflowId.HasValue && IsWorkflowTask(x.TaskType))
                .Select(x => x.WorkflowId!.Value).Distinct().ToArray(),
                workflowBudget, ct);
            for (var i = 0; i < batchItems.Count; i++)
            {
                var item = batchItems[i];
                if (item.WorkflowId is not { } workflowId || !IsWorkflowTask(item.TaskType)) continue;
                if (workflowResolution.Values.TryGetValue(workflowId, out var metadata) && batchBytes + metadata.WireBytes <= metadataBudget)
                {
                    batchItems[i] = item with { DefinitionId = metadata.DefinitionId,
                        DefinitionVersion = metadata.DefinitionVersion, WorkflowMetadataComplete = metadata.Complete,
                        WorkflowMetadataExpiresAt = metadata.ExpiresAt };
                    batchBytes += metadata.WireBytes;
                }
            }
            if (batchBytes > metadataBudget)
                throw new InvalidOperationException("Candidate hydration exceeded its combined metadata budget");
            return new(batchItems, oversizedIds.Count, checked((int)batchBytes), workflowResolution.Error, oversizedIds);
        }
    }

    private async Task<WorkflowMetadataResolution> ResolveWorkflowMetadataAsync(string hub,
        int matchingPartition, Guid[] ids, int byteBudget, CancellationToken ct)
    {
        await _workflowFetchGate.WaitAsync(ct);
        try { return await ResolveWorkflowMetadataUnderGateAsync(hub, matchingPartition, ids, byteBudget, ct); }
        finally { _workflowFetchGate.Release(); }
    }

    private async Task<WorkflowMetadataResolution> ResolveWorkflowMetadataUnderGateAsync(string hub,
        int matchingPartition, Guid[] ids, int byteBudget, CancellationToken ct)
    {
        if (ids.Length == 0) return new([], null);
        var result = new Dictionary<Guid, WorkflowMetadata>(ids.Length);
        var missing = new List<Guid>();
        var now = DateTimeOffset.UtcNow;
        lock (_workflowCacheLock)
        {
            foreach (var id in ids)
            {
                var key = new WorkflowKey(hub, id);
                if (_workflowCache.TryGetValue(key, out var node) && node.Value.Metadata.ExpiresAt > now)
                {
                    if (!ChargeWorkflowScopeLocked(node, matchingPartition))
                    {
                        // Return a valid lookup without retaining an uncharged scope.
                        result[id] = node.Value.Metadata;
                        continue;
                    }
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
        if (missing.Count == 0) return new(result, null);
        var perDefinitionLimit = missing.Count == 0 ? 0 : Math.Max(0, byteBudget / missing.Count - 32);
        perDefinitionLimit = Math.Min(perDefinitionLimit, WorkflowDefinitionMaxUtf8Bytes);
        if (perDefinitionLimit <= 0)
        {
            foreach (var id in missing) result[id] = new(null, null, false, 0, 0, DateTimeOffset.UtcNow + _workflowCacheTtl);
            return new(result, null);
        }
        await using var command = tasks.DataSource.CreateCommand(WorkflowMetadataSql);
        lock (_workflowCacheLock) _workflowLookupCalls++;
        HubTelemetry.CandidateWorkflowMetadataCalls.Add(1);
        command.Parameters.AddWithValue("hub", hub);
        command.Parameters.AddWithValue("ids", missing.ToArray());
        command.Parameters.AddWithValue("definitionByteLimit", perDefinitionLimit);
        var loaded = new Dictionary<Guid, WorkflowMetadata>();
        var workflowRows = 0;
        Exception? workflowError = null;
        try
        {
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    workflowRows++;
                    if (reader.GetBoolean(3) || reader.IsDBNull(1) || reader.IsDBNull(2))
                    {
                        loaded[reader.GetGuid(0)] = new(null, null, false, 0, 0, DateTimeOffset.UtcNow + _workflowCacheTtl);
                        continue;
                    }
                    var definition = reader.GetString(1);
                    var size = Encoding.UTF8.GetByteCount(definition) + 8L;
                    if (size > perDefinitionLimit)
                        loaded[reader.GetGuid(0)] = new(null, null, false, 0, 0, DateTimeOffset.UtcNow + _workflowCacheTtl);
                    else loaded[reader.GetGuid(0)] = new(definition, reader.GetInt32(2), true, size,
                        ManagedStringBytes(definition), DateTimeOffset.UtcNow + _workflowCacheTtl);
                }
            }
        }
        catch (Exception error) when (!ct.IsCancellationRequested)
        {
            workflowError = error;
            HubTelemetry.CandidateWorkflowMetadataFailures.Add(1);
        }
        HubTelemetry.CandidateWorkflowMetadataRows.Add(workflowRows);
        if (workflowError is not null)
        {
            foreach (var id in missing)
                if (!result.ContainsKey(id)) result[id] = new(null, null, false, 0, 0, DateTimeOffset.UtcNow + _workflowCacheTtl);
            return new(result, workflowError);
        }
        foreach (var id in missing)
            if (!loaded.ContainsKey(id)) loaded[id] = new(null, null, false, 0, 0, DateTimeOffset.UtcNow + _workflowCacheTtl);
        lock (_workflowCacheLock)
        {
            foreach (var pair in loaded)
            {
                var key = new WorkflowKey(hub, pair.Key);
                AddWorkflowNodeLocked(key, pair.Value, matchingPartition);
                result[pair.Key] = pair.Value;
            }
        }
        return new(result, null);
    }

    private void AddWorkflowNodeLocked(WorkflowKey key, WorkflowMetadata metadata, int partition)
    {
        var size = metadata.ManagedBytes + WorkflowNodeManagedBytes + WorkflowScopeManagedBytes + ManagedStringBytes(key.Hub);
        while (_workflowCache.ContainsKey(key)) RemoveWorkflowNodeLocked(_workflowCache[key]);
        if (size > _workflowCacheBytesPerPartition || size > _workflowCacheBytesPerProcess) return;
        var scopeKey = (key.Hub, partition);
        while (true)
        {
            _workflowScopes.TryGetValue(scopeKey, out var scope);
            var overScope = scope is not null && (scope.Entries >= _workflowCacheEntriesPerPartition ||
                scope.Bytes + size > _workflowCacheBytesPerPartition);
            var overProcess = _workflowCache.Count >= _workflowCacheEntriesPerProcess ||
                _workflowCacheBytes + size > _workflowCacheBytesPerProcess;
            if (!overScope && !overProcess) break;
            var victim = _workflowLru.Last;
            if (overScope)
                while (victim is not null && (victim.Value.Key.Hub != key.Hub || !victim.Value.Partitions.Contains(partition)))
                    victim = victim.Previous;
            if (victim is null) return;
            RemoveWorkflowNodeLocked(victim);
        }
        if (!_workflowScopes.TryGetValue(scopeKey, out var target))
            _workflowScopes.Add(scopeKey, target = new());
        target.Entries++; target.Bytes += size;
        var node = _workflowLru.AddFirst(new WorkflowCacheNode(key, metadata, [partition]));
        _workflowCache.Add(key, node); _workflowCacheBytes += size;
    }

    private void RemoveWorkflowNodeLocked(LinkedListNode<WorkflowCacheNode> node)
    {
        var value = node.Value;
        _workflowLru.Remove(node); _workflowCache.Remove(value.Key); _workflowCacheEvictions++;
        HubTelemetry.CandidateWorkflowCacheEvictions.Add(1);
        var size = value.Metadata.ManagedBytes + WorkflowNodeManagedBytes + WorkflowScopeManagedBytes + ManagedStringBytes(value.Key.Hub);
        _workflowCacheBytes -= size + (value.Partitions.Count - 1L) * WorkflowScopeManagedBytes;
        foreach (var partition in value.Partitions)
        {
            var scopeKey = (value.Key.Hub, partition);
            var scope = _workflowScopes[scopeKey];
            scope.Entries--; scope.Bytes -= size;
            if (scope.Entries == 0) _workflowScopes.Remove(scopeKey);
        }
    }

    private bool ChargeWorkflowScopeLocked(LinkedListNode<WorkflowCacheNode> node, int partition)
    {
        var value = node.Value;
        if (value.Partitions.Contains(partition)) return true;
        var size = value.Metadata.ManagedBytes + WorkflowNodeManagedBytes + WorkflowScopeManagedBytes + ManagedStringBytes(value.Key.Hub);
        if (size > _workflowCacheBytesPerPartition) return false;
        var scopeKey = (value.Key.Hub, partition);
        while (true)
        {
            _workflowScopes.TryGetValue(scopeKey, out var scope);
            var overScope = scope is not null && (scope.Entries >= _workflowCacheEntriesPerPartition ||
                scope.Bytes + size > _workflowCacheBytesPerPartition);
            var overProcess = _workflowCacheBytes + WorkflowScopeManagedBytes > _workflowCacheBytesPerProcess;
            if (!overScope && !overProcess) break;
            var victim = _workflowLru.Last;
            while (victim is not null && (victim == node ||
                   (overScope && (victim.Value.Key.Hub != value.Key.Hub || !victim.Value.Partitions.Contains(partition)))))
                victim = victim.Previous;
            if (victim is null) return false;
            RemoveWorkflowNodeLocked(victim);
        }
        if (!_workflowScopes.TryGetValue(scopeKey, out var target))
            _workflowScopes.Add(scopeKey, target = new());
        target.Entries++; target.Bytes += size;
        value.Partitions.Add(partition);
        _workflowCacheBytes += WorkflowScopeManagedBytes;
        return true;
    }

    private static long ManagedStringBytes(string value) => 32L + 2L * value.Length;

    private static bool IsWorkflowTask(string taskType) =>
        taskType is "__workflow__" or "__native_workflow__";

    // Kept beside the SQL so plan reproduction and production use precisely
    // the same parameter budget after UUID array wire bytes are reserved.
    internal static int HydrationMetadataBudget(int byteLimit, int requestedIdCount)
    {
        if (requestedIdCount < 1 || byteLimit < requestedIdCount * 16)
            throw new ArgumentOutOfRangeException(nameof(requestedIdCount));
        return byteLimit - requestedIdCount * 16;
    }
}

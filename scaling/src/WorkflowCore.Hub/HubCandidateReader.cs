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
    long Epoch, DateTime Deadline, string[] ResourceKeys, bool ResourceKeysComplete = true);
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

internal sealed class HubCandidateReader(HubTaskStore tasks) : IHubCandidateDiscovery
{
    private const string HydrationSql = """
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
          SELECT p.id,128+octet_length(p.hub_id)+octet_length(p.task_type)+
            COALESCE(octet_length(w.definition_id),0) AS bytes,b.per_candidate
          FROM page p LEFT JOIN wfc_scale.hub_workflow w ON w.id=p.workflow_id CROSS JOIN budget b
        ), resources AS (
          SELECT c.task_id,array_agg(c.resource_key ORDER BY c.resource_key) AS keys
          FROM wfc_scale.hub_task_constraint c JOIN resource_sizes s ON s.task_id=c.task_id
          JOIN base_size b ON b.id=c.task_id
          WHERE b.bytes<=b.per_candidate AND b.bytes+s.bytes<=b.per_candidate
          GROUP BY c.task_id
        )
        SELECT p.id,p.sequence,CASE WHEN b.bytes<=b.per_candidate THEN p.hub_id END,p.partition_id,
               CASE WHEN b.bytes<=b.per_candidate THEN p.task_type END,p.task_version,p.workflow_id,
               CASE WHEN b.bytes<=b.per_candidate THEN w.definition_id END,w.definition_version,p.epoch,
               CASE WHEN p.lease_until<clock_timestamp() AND p.state=1 THEN p.lease_until ELSE p.due_at END AS deadline,
               COALESCE(r.keys,ARRAY[]::text[]),COALESCE(s.bytes,0),
               b.bytes>b.per_candidate AS oversized,
               COALESCE(r.keys IS NOT NULL OR s.bytes IS NULL OR s.bytes=0,FALSE)
        FROM page p LEFT JOIN wfc_scale.hub_workflow w ON w.id=p.workflow_id
        JOIN base_size b ON b.id=p.id
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
            ), resource_sizes AS MATERIALIZED (
              SELECT c.task_id, sum(octet_length(c.resource_key) + 24) AS bytes
              FROM wfc_scale.hub_task_constraint c JOIN page p ON p.id=c.task_id
              GROUP BY c.task_id
            ), resources AS (
              SELECT c.task_id, array_agg(c.resource_key ORDER BY c.resource_key) AS keys
              FROM wfc_scale.hub_task_constraint c JOIN resource_sizes s ON s.task_id=c.task_id
              WHERE s.bytes<=@byteLimit GROUP BY c.task_id
            )
            SELECT p.id,p.sequence,
                   CASE WHEN length.too_large THEN NULL ELSE p.hub_id END,
                   p.partition_id,CASE WHEN length.too_large THEN NULL ELSE p.task_type END,
                   p.task_version,p.workflow_id,
                   CASE WHEN length.too_large THEN NULL ELSE w.definition_id END,
                   w.definition_version,p.epoch,p.deadline,COALESCE(r.keys,ARRAY[]::text[]),
                   COALESCE(s.bytes,0),length.too_large
            FROM page p LEFT JOIN wfc_scale.hub_workflow w ON w.id=p.workflow_id
            CROSS JOIN LATERAL (SELECT (128 + octet_length(p.hub_id) + octet_length(p.task_type) +
                COALESCE(octet_length(w.definition_id),0)) > @byteLimit AS too_large) length
            LEFT JOIN resource_sizes s ON s.task_id=p.id
            LEFT JOIN resources r ON r.task_id=p.id ORDER BY /*outer_order*/
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
            var deadline = reader.GetDateTime(10);
            if (reader.GetBoolean(13))
            {
                const int markerSize = 64;
                if (bytes + markerSize > byteLimit) { interrupted = true; break; }
                oversized.Add(new(reader.GetGuid(0), sequence, deadline));
                bytes += markerSize;
            }
            else
            {
                // Missing keys mean unknown constraints, never an unconstrained task.
                // The claim transaction always loads and validates all resources.
                var complete = reader.GetInt64(12) <= byteLimit;
                var keys = complete ? reader.GetFieldValue<string[]>(11) : [];
                var item = new HubTaskCandidate(reader.GetGuid(0), sequence, reader.GetString(2),
                    reader.GetInt32(3), reader.GetString(4), reader.GetInt32(5), reader.IsDBNull(6) ? null : reader.GetGuid(6),
                    reader.IsDBNull(7) ? null : reader.GetString(7), reader.IsDBNull(8) ? null : reader.GetInt32(8),
                    reader.GetInt64(9), deadline, keys, complete);
                var size = 128 + Encoding.UTF8.GetByteCount(item.HubId) + Encoding.UTF8.GetByteCount(item.TaskType) +
                    (item.DefinitionId is null ? 0 : Encoding.UTF8.GetByteCount(item.DefinitionId)) + keys.Sum(k => 24 + Encoding.UTF8.GetByteCount(k));
                if (size > byteLimit && keys.Length > 0)
                {
                    item = item with { ResourceKeys = [], ResourceKeysComplete = false };
                    size = 128 + Encoding.UTF8.GetByteCount(item.HubId) + Encoding.UTF8.GetByteCount(item.TaskType) +
                        (item.DefinitionId is null ? 0 : Encoding.UTF8.GetByteCount(item.DefinitionId));
                }
                if (bytes + size > byteLimit) { interrupted = true; break; }
                items.Add(item);
                bytes += size;
            }
            rows++;
            lastProcessed = new(sequence, byDeadline ? deadline : null);
        }
        return new(items, lastProcessed, !interrupted && rows < pageSize, bytes, oversized);

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
        var batchMetadataLimit = byteLimit - requestedIds.Length * 16;
        await using var command = tasks.DataSource.CreateCommand(HydrationSql);
        command.Parameters.AddWithValue("ids", requestedIds);
        command.Parameters.AddWithValue("hub", hub);
        command.Parameters.AddWithValue("matchingPartition", matchingPartition);
        command.Parameters.AddWithValue("partitionCount", partitionCount);
        command.Parameters.AddWithValue("byteLimit", batchMetadataLimit);

        var items = new List<HubTaskCandidate>(requestedIds.Length);
        var bytes = 0;
        var oversized = 0;
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (reader.GetBoolean(13)) { oversized++; continue; }
            var keysComplete = reader.GetBoolean(14);
            var keys = keysComplete ? reader.GetFieldValue<string[]>(11) : [];
            var item = new HubTaskCandidate(reader.GetGuid(0), reader.GetInt64(1), reader.GetString(2),
                reader.GetInt32(3), reader.GetString(4), reader.GetInt32(5), reader.IsDBNull(6) ? null : reader.GetGuid(6),
                reader.IsDBNull(7) ? null : reader.GetString(7), reader.IsDBNull(8) ? null : reader.GetInt32(8),
                reader.GetInt64(9), reader.GetDateTime(10), keys, keysComplete);
            var size = 128 + Encoding.UTF8.GetByteCount(item.HubId) + Encoding.UTF8.GetByteCount(item.TaskType) +
                (item.DefinitionId is null ? 0 : Encoding.UTF8.GetByteCount(item.DefinitionId)) +
                keys.Sum(k => 24 + Encoding.UTF8.GetByteCount(k));
            if (bytes + size > batchMetadataLimit)
                throw new InvalidOperationException("Candidate hydration violated its bounded SQL metadata contract");
            bytes += size;
            items.Add(item);
        }
        return new(items, oversized, bytes);
    }
}

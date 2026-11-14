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

internal interface IHubCandidateDiscovery
{
    Task<HubCandidateHorizon> CaptureHorizonAsync(string hub, CancellationToken ct);
    Task<HubCandidatePage> ReadPageAsync(string hub, int bucket, HubCandidateStream stream,
        HubCandidateHorizon horizon, HubCandidateCursor? cursor, int pageSize, int byteLimit, CancellationToken ct);
}

internal sealed class HubCandidateReader(HubTaskStore tasks) : IHubCandidateDiscovery
{
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
}

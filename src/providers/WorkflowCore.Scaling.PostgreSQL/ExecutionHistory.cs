using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;
using Npgsql;
using WorkflowCore.Models;
using WorkflowCore.Persistence.PostgreSQL;

namespace WorkflowCore.Scaling.PostgreSQL;

/// <summary>
/// Immutable, redacted events. The ID is the pagination cursor. A revision is
/// the committed state generation (or the generation observed by a dispatch);
/// it is NOT a unique event sequence.
/// </summary>
public sealed record ExecutionHistoryEntry(
    long Id, Guid WorkflowId, long Revision, int EventIndex, string EventType,
    string? PointerId, int? StepId, int? Attempt, string? WorkerId,
    string Details, DateTime OccurredAt);

public sealed class ExecutionHistoryReader
{
    private readonly string _connectionString;
    public ExecutionHistoryReader(string connectionString) => _connectionString = connectionString;

    public async Task<IReadOnlyList<ExecutionHistoryEntry>> ReadAsync(Guid workflowId,
        long afterId = 0, int limit = 100, CancellationToken ct = default)
    {
        if (workflowId == Guid.Empty) throw new ArgumentException("Workflow ID is required", nameof(workflowId));
        if (afterId < 0) throw new ArgumentOutOfRangeException(nameof(afterId));
        if (limit is < 1 or > 500) throw new ArgumentOutOfRangeException(nameof(limit));
        var results = new List<ExecutionHistoryEntry>();
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand("""
            SELECT id,workflow_id,revision,event_index,event_type,pointer_id,
                   step_id,attempt,worker_id,details::text,occurred_at
            FROM wfc_scale.execution_history
            WHERE workflow_id = @workflow AND id > @after
            ORDER BY id LIMIT @limit
            """, connection);
        cmd.Parameters.AddWithValue("workflow", workflowId);
        cmd.Parameters.AddWithValue("after", afterId);
        cmd.Parameters.AddWithValue("limit", limit);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            results.Add(new ExecutionHistoryEntry(
                reader.GetInt64(0), reader.GetGuid(1), reader.GetInt64(2),
                reader.GetInt32(3), reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetInt32(6),
                reader.IsDBNull(7) ? null : reader.GetInt32(7),
                reader.IsDBNull(8) ? null : reader.GetString(8),
                reader.GetString(9), reader.GetDateTime(10)));
        return results;
    }
}

internal static class ExecutionHistory
{
    // Never serialize workflow Data, pointer Outcome/PersistenceData, signal
    // payloads, exception messages, or credentials into the audit journal.
    internal static Task<int> AppendAsync(PostgresContext db, Guid workflowId,
        long revision, int eventIndex, string eventType, CancellationToken ct,
        string? pointerId = null, int? stepId = null, int? attempt = null,
        string? worker = null, string? eventKey = null,
        IReadOnlyDictionary<string, string?>? details = null)
    {
        var metadata = JsonConvert.SerializeObject(details ?? new Dictionary<string, string?>());
        return db.Database.ExecuteSqlInterpolatedAsync($"""
            INSERT INTO wfc_scale.execution_history
                (workflow_id,revision,event_index,event_type,event_key,
                 pointer_id,step_id,attempt,worker_id,details)
            VALUES ({workflowId},{revision},{eventIndex},{eventType},{eventKey},
                    {pointerId},{stepId},{attempt},{worker},CAST({metadata} AS jsonb))
            ON CONFLICT (event_key) DO NOTHING
            """, ct);
    }

    /// <summary>
    /// Commit dispatch evidence BEFORE running an external step. The INSERT is
    /// guarded by both live leases; it does not advance workflow state or fence
    /// revision. A missing terminal record means an UNKNOWN outcome.
    /// </summary>
    internal static async Task<IReadOnlyList<ExecutionPointer>> DispatchAsync(
        PostgresContext db, WorkflowInstance instance, LeasedWork work,
        WorkflowFence fence, string owner, CancellationToken ct)
    {
        var eligible = instance.Status == WorkflowStatus.Runnable
            ? instance.ExecutionPointers.Where(p => p.Active &&
                (!p.SleepUntil.HasValue || p.SleepUntil.Value < DateTime.UtcNow))
                .OrderBy(p => p.Id, StringComparer.Ordinal).ToArray()
            : Array.Empty<ExecutionPointer>();

        await using var tx = await db.Database.BeginTransactionAsync(ct);
        // Verify leases even if there are no pointers to dispatch.
        var valid = await db.Database.SqlQuery<int>($"""
            SELECT 1 AS "Value"
            FROM wfc_scale.work_item w
            JOIN wfc_scale.workflow_gate g ON g.workflow_id = w.item_id
            WHERE w.id = {work.Id} AND w.state = 1 AND w.owner = {owner}
              AND w.epoch = {work.Epoch} AND w.lease_until > now()
              AND g.workflow_id = {fence.WorkflowId} AND g.owner = {fence.Owner}
              AND g.epoch = {fence.Epoch} AND g.revision = {fence.Revision}
              AND g.lease_until > now()
            FOR UPDATE OF w,g
            """).AnyAsync(ct);
        if (!valid) throw new InvalidOperationException("Cannot dispatch: queue or workflow fence expired");
        var index = 0;
        foreach (var pointer in eligible)
        {
            await AppendAsync(db, work.ItemId, fence.Revision, index++,
                "StepAttemptDispatched", ct, pointer.Id, pointer.StepId,
                work.Attempts, owner, $"dispatch:{work.Id:N}:{work.Epoch}:{pointer.Id}");
        }
        await tx.CommitAsync(ct);
        return eligible;
    }

    internal static async Task AppendOutcomesAsync(PostgresContext db, Guid workflowId,
        long revision, WorkflowInstance before, WorkflowInstance after,
        WorkflowExecutorResult? result, IReadOnlyList<ExecutionPointer> dispatched,
        LeasedWork work, string owner, CancellationToken ct)
    {
        var index = 0;
        var oldPointers = before.ExecutionPointers.ToDictionary(p => p.Id, StringComparer.Ordinal);
        var newPointers = after.ExecutionPointers.ToDictionary(p => p.Id, StringComparer.Ordinal);
        foreach (var pointer in after.ExecutionPointers.OrderBy(p => p.Id, StringComparer.Ordinal))
        {
            if (!oldPointers.TryGetValue(pointer.Id, out var original))
            {
                await AppendAsync(db, workflowId, revision, index++, "StepScheduled", ct,
                    pointer.Id, pointer.StepId, work.Attempts, owner);
                continue;
            }
            if (!original.EndTime.HasValue && pointer.EndTime.HasValue)
                await AppendAsync(db, workflowId, revision, index++,
                    pointer.Status == PointerStatus.Complete ? "StepCompleted" : "StepEnded",
                    ct, pointer.Id, pointer.StepId, work.Attempts, owner,
                    details: new Dictionary<string, string?> { ["status"] = pointer.Status.ToString() });
            if (original.Status != pointer.Status)
                await AppendAsync(db, workflowId, revision, index++, "StepStateChanged", ct,
                    pointer.Id, pointer.StepId, work.Attempts, owner,
                    details: new Dictionary<string, string?>
                    {
                        ["from"] = original.Status.ToString(), ["to"] = pointer.Status.ToString()
                    });
            if (pointer.RetryCount > original.RetryCount)
                await AppendAsync(db, workflowId, revision, index++, "StepRetryScheduled", ct,
                    pointer.Id, pointer.StepId, work.Attempts, owner);
        }
        if (result is not null)
            foreach (var error in result.Errors)
            {
                var pointer = newPointers.GetValueOrDefault(error.ExecutionPointerId);
                await AppendAsync(db, workflowId, revision, index++, "StepFailed", ct,
                    error.ExecutionPointerId, pointer?.StepId, work.Attempts, owner);
            }
        foreach (var pointer in dispatched)
        {
            var actual = newPointers.GetValueOrDefault(pointer.Id);
            await AppendAsync(db, workflowId, revision, index++,
                "StepAttemptCommitted", ct, pointer.Id, pointer.StepId, work.Attempts,
                owner, $"committed:{work.Id:N}:{work.Epoch}:{pointer.Id}",
                new Dictionary<string, string?>
                {
                    ["status"] = actual?.Status.ToString() ?? "Missing",
                    ["completed"] = actual?.EndTime.HasValue == true ? "true" : "false"
                });
        }
        if (before.Status != after.Status)
            await AppendAsync(db, workflowId, revision, index++, "WorkflowStatusChanged", ct,
                worker: owner, details: new Dictionary<string, string?>
                {
                    ["from"] = before.Status.ToString(), ["to"] = after.Status.ToString()
                });
    }
}

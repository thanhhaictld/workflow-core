using Npgsql;

namespace WorkflowCore.Scaling.PostgreSQL;

public sealed record WorkQueueStats(
    long Ready, long Leased, long Completed, long Dead,
    TimeSpan? OldestReadyAge);

/// <summary>
/// Operator diagnostics and dead-letter replay. Keep this endpoint restricted
/// to trusted internal operators; replay may repeat external side effects.
/// </summary>
public static class WorkQueueAdministration
{
    public static async Task<WorkQueueStats> GetStatsAsync(
        this FencedWorkQueue queue, string queueName, CancellationToken ct = default)
    {
        await using var cmd = queue.DataSource.CreateCommand("""
            SELECT count(*) FILTER (WHERE state = 0 AND due_at <= now()),
                   count(*) FILTER (WHERE state = 1),
                   count(*) FILTER (WHERE state = 2),
                   count(*) FILTER (WHERE state = 3),
                   max(now() - due_at) FILTER (WHERE state = 0 AND due_at <= now())
            FROM wfc_scale.work_item WHERE queue_name = @queue
            """);
        cmd.Parameters.AddWithValue("queue", queueName);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return new WorkQueueStats(0, 0, 0, 0, null);
        return new WorkQueueStats(reader.GetInt64(0), reader.GetInt64(1),
            reader.GetInt64(2), reader.GetInt64(3),
            reader.IsDBNull(4) ? null : reader.GetTimeSpan(4));
    }

    public static async Task<bool> RequeueDeadAsync(
        this FencedWorkQueue queue, Guid workId, CancellationToken ct = default)
    {
        await using var cmd = queue.DataSource.CreateCommand("""
            UPDATE wfc_scale.work_item
            SET state = 0, owner = NULL, lease_until = NULL,
                due_at = now(), attempts = 0, epoch = epoch + 1,
                last_error = NULL, updated_at = now()
            WHERE id = @id AND state = 3
            """);
        cmd.Parameters.AddWithValue("id", workId);
        return await cmd.ExecuteNonQueryAsync(ct) == 1;
    }
}

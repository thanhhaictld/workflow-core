using Npgsql;
using NpgsqlTypes;

namespace WorkflowCore.Scaling.PostgreSQL;

public sealed record LeasedWork(Guid Id, string Queue, Guid ItemId, string ItemKey, long Epoch, int Attempts);
public sealed record WorkflowFence(Guid WorkflowId, string Owner, long Epoch, long Revision);

/// <summary>
/// Claims and deduplicates work with PostgreSQL, not in-memory timers.
/// Delivery is at-least-once; callers must implement idempotent side effects.
/// </summary>
public sealed class FencedWorkQueue : IAsyncDisposable
{
    private readonly NpgsqlDataSource _dataSource;
    public FencedWorkQueue(string connectionString) => _dataSource = NpgsqlDataSource.Create(connectionString);
    public NpgsqlDataSource DataSource => _dataSource;

    public async Task<IReadOnlyList<LeasedWork>> ClaimAsync(string queue, string owner, int count,
        TimeSpan lease, int maxAttempts, CancellationToken ct)
    {
        if (count < 1 || lease <= TimeSpan.Zero || maxAttempts < 1)
            throw new ArgumentOutOfRangeException(nameof(count));
        await using var cmd = _dataSource.CreateCommand("""
            WITH candidates AS (
                SELECT id FROM wfc_scale.work_item
                WHERE queue_name = @queue AND attempts < @maximum
                  AND ((state = 0 AND due_at <= now())
                    OR (state = 1 AND lease_until < now()))
                ORDER BY due_at, id
                FOR UPDATE SKIP LOCKED
                LIMIT @count
            )
            UPDATE wfc_scale.work_item w
            SET state = 1, owner = @owner, epoch = epoch + 1,
                lease_until = now() + @lease, attempts = attempts + 1,
                updated_at = now()
            FROM candidates WHERE w.id = candidates.id
            RETURNING w.id, w.queue_name, w.item_id, w.item_key, w.epoch, w.attempts
            """);
        cmd.Parameters.AddWithValue("queue", queue);
        cmd.Parameters.AddWithValue("owner", owner);
        cmd.Parameters.AddWithValue("maximum", maxAttempts);
        cmd.Parameters.AddWithValue("count", count);
        cmd.Parameters.AddWithValue("lease", NpgsqlDbType.Interval, lease);
        var result = new List<LeasedWork>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            result.Add(new(reader.GetGuid(0), reader.GetString(1), reader.GetGuid(2),
                reader.GetString(3), reader.GetInt64(4), reader.GetInt32(5)));
        return result;
    }

    public async Task<WorkflowFence?> AcquireWorkflowAsync(Guid id, string owner, TimeSpan lease, CancellationToken ct)
    {
        await using var cmd = _dataSource.CreateCommand("""
            INSERT INTO wfc_scale.workflow_gate(workflow_id, owner, epoch, revision, lease_until)
            VALUES (@id, @owner, 1, 0, now() + @lease)
            ON CONFLICT (workflow_id) DO UPDATE
              SET owner = excluded.owner, epoch = workflow_gate.epoch + 1,
                  lease_until = excluded.lease_until
            WHERE workflow_gate.owner IS NULL OR workflow_gate.lease_until < now()
            RETURNING workflow_id, owner, epoch, revision
            """);
        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("owner", owner);
        cmd.Parameters.AddWithValue("lease", NpgsqlDbType.Interval, lease);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct)
            ? new WorkflowFence(reader.GetGuid(0), reader.GetString(1), reader.GetInt64(2), reader.GetInt64(3))
            : null;
    }

    public async Task<bool> RenewAsync(LeasedWork work, WorkflowFence? fence, string owner,
        TimeSpan lease, CancellationToken ct)
    {
        // A single transaction ensures that an expired workflow gate cannot be
        // renewed independently from a leased work item.
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await using var cmd = new NpgsqlCommand("""
            UPDATE wfc_scale.work_item SET lease_until = now() + @lease, updated_at = now()
            WHERE id = @id AND state = 1 AND owner = @owner
              AND epoch = @epoch AND lease_until > now()
            """, connection, transaction);
        cmd.Parameters.AddWithValue("id", work.Id);
        cmd.Parameters.AddWithValue("owner", owner);
        cmd.Parameters.AddWithValue("epoch", work.Epoch);
        cmd.Parameters.AddWithValue("lease", NpgsqlDbType.Interval, lease);
        if (await cmd.ExecuteNonQueryAsync(ct) != 1)
            return false;
        if (fence is not null)
        {
            await using var gate = new NpgsqlCommand("""
                UPDATE wfc_scale.workflow_gate SET lease_until = now() + @lease
                WHERE workflow_id = @id AND owner = @owner AND epoch = @epoch
                  AND lease_until > now()
                """, connection, transaction);
            gate.Parameters.AddWithValue("id", fence.WorkflowId);
            gate.Parameters.AddWithValue("owner", owner);
            gate.Parameters.AddWithValue("epoch", fence.Epoch);
            gate.Parameters.AddWithValue("lease", NpgsqlDbType.Interval, lease);
            if (await gate.ExecuteNonQueryAsync(ct) != 1)
                return false;
        }
        await transaction.CommitAsync(ct);
        return true;
    }

    public async Task<bool> RetryAsync(LeasedWork work, string owner, TimeSpan delay,
        int maxAttempts, string? error, CancellationToken ct)
    {
        // Queue transition and its audit event commit atomically. No exception
        // message is copied to history: it may contain application secrets.
        await using var cmd = _dataSource.CreateCommand("""
            WITH changed AS (
                UPDATE wfc_scale.work_item
                SET state = CASE WHEN attempts >= @max THEN 3 ELSE 0 END,
                    due_at = now() + @delay, owner = NULL, lease_until = NULL,
                    updated_at = now(), last_error = @error
                WHERE id = @id AND owner = @owner AND state = 1
                  AND epoch = @epoch AND lease_until > now()
                RETURNING id,queue_name,item_id,epoch,attempts,state
            ), journal AS (
                INSERT INTO wfc_scale.execution_history
                    (workflow_id,revision,event_index,event_type,event_key,attempt,worker_id)
                SELECT c.item_id,COALESCE(g.revision,0),0,
                       CASE WHEN c.state = 3 THEN 'WorkDeadLettered' ELSE 'WorkRetryScheduled' END,
                       'retry:' || c.id::text || ':' || c.epoch::text,
                       c.attempts,@owner
                FROM changed c
                LEFT JOIN wfc_scale.workflow_gate g ON g.workflow_id = c.item_id
                WHERE c.queue_name = 'workflow'
                ON CONFLICT (event_key) DO NOTHING
                RETURNING id
            )
            SELECT count(*) FROM changed
            """);
        cmd.Parameters.AddWithValue("id", work.Id);
        cmd.Parameters.AddWithValue("owner", owner);
        cmd.Parameters.AddWithValue("epoch", work.Epoch);
        cmd.Parameters.AddWithValue("max", maxAttempts);
        cmd.Parameters.AddWithValue("delay", NpgsqlDbType.Interval, delay);
        cmd.Parameters.AddWithValue("error", NpgsqlDbType.Text, (object?)error ?? DBNull.Value);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(ct)) == 1;
    }

    /// <summary>
    /// Put a work item back without consuming a retry attempt when another
    /// node already owns the workflow. This is scheduling contention, not a
    /// failed workflow step.
    /// </summary>
    public async Task<bool> DeferAsync(LeasedWork work, string owner,
        TimeSpan delay, CancellationToken ct)
    {
        await using var cmd = _dataSource.CreateCommand("""
            UPDATE wfc_scale.work_item
            SET state = 0, owner = NULL, lease_until = NULL,
                due_at = now() + @delay, attempts = greatest(0, attempts - 1),
                updated_at = now()
            WHERE id = @id AND state = 1 AND owner = @owner
              AND epoch = @epoch AND lease_until > now()
            """);
        cmd.Parameters.AddWithValue("id", work.Id);
        cmd.Parameters.AddWithValue("owner", owner);
        cmd.Parameters.AddWithValue("epoch", work.Epoch);
        cmd.Parameters.AddWithValue("delay", NpgsqlDbType.Interval, delay);
        return await cmd.ExecuteNonQueryAsync(ct) == 1;
    }

    public async Task ReleaseGateAsync(WorkflowFence fence, CancellationToken ct)
    {
        await using var cmd = _dataSource.CreateCommand("""
            UPDATE wfc_scale.workflow_gate SET owner = NULL, lease_until = NULL
            WHERE workflow_id = @id AND owner = @owner AND epoch = @epoch
            """);
        cmd.Parameters.AddWithValue("id", fence.WorkflowId);
        cmd.Parameters.AddWithValue("owner", fence.Owner);
        cmd.Parameters.AddWithValue("epoch", fence.Epoch);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<int> DeadLetterExhaustedAsync(int maxAttempts, CancellationToken ct)
    {
        await using var cmd = _dataSource.CreateCommand("""
            WITH changed AS (
                UPDATE wfc_scale.work_item
                SET state = 3, owner = NULL, lease_until = NULL, updated_at = now(),
                    last_error = coalesce(last_error, 'lease expired after maximum attempts')
                WHERE state = 1 AND lease_until < now() AND attempts >= @max
                RETURNING id,queue_name,item_id,epoch,attempts
            ), journal AS (
                INSERT INTO wfc_scale.execution_history
                    (workflow_id,revision,event_index,event_type,event_key,attempt)
                SELECT c.item_id,COALESCE(g.revision,0),0,'WorkDeadLettered',
                       'expired:' || c.id::text || ':' || c.epoch::text,c.attempts
                FROM changed c
                LEFT JOIN wfc_scale.workflow_gate g ON g.workflow_id = c.item_id
                WHERE c.queue_name = 'workflow'
                ON CONFLICT (event_key) DO NOTHING
                RETURNING id
            )
            SELECT count(*) FROM changed
            """);
        cmd.Parameters.AddWithValue("max", maxAttempts);
        return Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));
    }

    public ValueTask DisposeAsync() => _dataSource.DisposeAsync();
}

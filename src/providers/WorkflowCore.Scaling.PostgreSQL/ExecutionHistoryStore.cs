using Microsoft.EntityFrameworkCore;
using Npgsql;
using System.Text.Json;
using WorkflowCore.Persistence.PostgreSQL;

namespace WorkflowCore.Scaling.PostgreSQL;

/// <summary>A journal entry. Details should contain only non-sensitive metadata.</summary>
public sealed record ExecutionHistoryEvent(
    Guid WorkflowId,
    string EventType,
    Guid? WorkItemId = null,
    string? ExecutionPointerId = null,
    int? StepId = null,
    Guid? StepAttemptId = null,
    long? Revision = null,
    string? Details = null)
{
    public DateTime OccurredAt { get; init; } = DateTime.UtcNow;
    public static string Json(object value) => JsonSerializer.Serialize(value);
}

public sealed record ExecutionHistoryRecord(
    long HistoryId,
    DateTime RecordedAt,
    ExecutionHistoryEvent Event);

/// <summary>
/// Appends journal records. Calls made with a PostgresContext participate in
/// its current transaction; independent appends are durable before step bodies.
/// </summary>
public sealed class ExecutionHistoryStore
{
    private readonly string _connectionString;
    private readonly string _schema;

    public ExecutionHistoryStore(string connectionString, string schema = "wfc")
    {
        _connectionString = connectionString;
        _schema = schema;
    }

    public async Task AppendAsync(ExecutionHistoryEvent entry, CancellationToken ct = default)
    {
        await using var db = new PostgresContext(_connectionString, _schema);
        await AppendInTransactionAsync(db, entry, ct);
    }

    public static async Task AppendInTransactionAsync(
        PostgresContext db, ExecutionHistoryEvent entry, CancellationToken ct)
    {
        await db.Database.ExecuteSqlInterpolatedAsync($"""
            INSERT INTO wfc_scale.execution_history
                (workflow_id, event_type, work_item_id, execution_pointer_id,
                 step_id, step_attempt_id, revision, details, occurred_at)
            VALUES ({entry.WorkflowId}, {entry.EventType}, {entry.WorkItemId},
                    {entry.ExecutionPointerId}, {entry.StepId},
                    {entry.StepAttemptId}, {entry.Revision},
                    CAST({entry.Details} AS jsonb), {entry.OccurredAt})
            """, ct);
    }

    /// <summary>
    /// Page the journal using the increasing identity value. An identity is
    /// not a commit-order guarantee while concurrent transactions are open.
    /// </summary>
    public async Task<IReadOnlyList<ExecutionHistoryRecord>> ReadAsync(
        Guid workflowId, long afterHistoryId = 0, int take = 100,
        CancellationToken ct = default)
    {
        if (take is < 1 or > 1000)
            throw new ArgumentOutOfRangeException(nameof(take), "Use 1..1000.");
        if (afterHistoryId < 0)
            throw new ArgumentOutOfRangeException(nameof(afterHistoryId));

        var records = new List<ExecutionHistoryRecord>();
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand("""
            SELECT history_id, recorded_at, occurred_at, event_type, work_item_id,
                   execution_pointer_id, step_id, step_attempt_id, revision,
                   details::text
            FROM wfc_scale.execution_history
            WHERE workflow_id = @workflowId AND history_id > @after
            ORDER BY history_id
            LIMIT @take
            """, connection);
        cmd.Parameters.AddWithValue("workflowId", workflowId);
        cmd.Parameters.AddWithValue("after", afterHistoryId);
        cmd.Parameters.AddWithValue("take", take);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            records.Add(new ExecutionHistoryRecord(
                reader.GetInt64(0),
                reader.GetDateTime(1),
                new ExecutionHistoryEvent(
                    workflowId,
                    reader.GetString(3),
                    reader.IsDBNull(4) ? null : reader.GetGuid(4),
                    reader.IsDBNull(5) ? null : reader.GetString(5),
                    reader.IsDBNull(6) ? null : reader.GetInt32(6),
                    reader.IsDBNull(7) ? null : reader.GetGuid(7),
                    reader.IsDBNull(8) ? null : reader.GetInt64(8),
                    reader.IsDBNull(9) ? null : reader.GetString(9))
                { OccurredAt = reader.GetDateTime(2) }));
        }
        return records;
    }
}

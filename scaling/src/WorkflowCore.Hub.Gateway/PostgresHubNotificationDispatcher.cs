using Npgsql;
using WorkflowCore.Hub;

namespace WorkflowCore.Hub.Gateway;

// Deletion and pg_notify happen in one transaction: rollback retains the rows,
// and PostgreSQL publishes the notifications only after a successful commit.
internal sealed class PostgresHubNotificationDispatcher : BackgroundService
{
    private readonly HubTaskStore _store;
    private readonly ILogger<PostgresHubNotificationDispatcher> _logger;
    private readonly int _batchSize = ReadSetting("WFC_HUB_OUTBOX_BATCH_SIZE", 128, 1, 1024);
    private readonly int _minDelay = ReadSetting("WFC_HUB_OUTBOX_MIN_DELAY_MS", 10, 1, 1000);
    private readonly int _maxDelay = ReadSetting("WFC_HUB_OUTBOX_MAX_DELAY_MS", 1000, 1, 60000);

    public PostgresHubNotificationDispatcher(HubTaskStore store, ILogger<PostgresHubNotificationDispatcher> logger)
    {
        _store = store; _logger = logger;
        if (_minDelay > _maxDelay) throw new ArgumentException("Outbox minimum delay exceeds maximum delay");
    }

    private static int ReadSetting(string name, int fallback, int min, int max)
    {
        var value = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrEmpty(value)) return fallback;
        if (!int.TryParse(value, out var parsed) || parsed < min || parsed > max)
            throw new ArgumentException($"{name} must be between {min} and {max}");
        return parsed;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var delay = _minDelay;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var count = await DispatchAsync(stoppingToken);
                if (count > 0) delay = _minDelay;
                else delay = Math.Min(_maxDelay, delay * 2);
                if (count == _batchSize) continue;
                var jittered = Math.Clamp((int)(delay * (0.8 + Random.Shared.NextDouble() * 0.4)), _minDelay, _maxDelay);
                await Task.Delay(jittered, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "PostgreSQL notification outbox dispatch failed");
                await Task.Delay(_maxDelay, stoppingToken);
            }
        }
    }

    internal async Task<int> DispatchAsync(CancellationToken ct,
        Func<NpgsqlTransaction, CancellationToken, Task>? beforeCommit = null)
    {
        await using var db = await _store.DataSource.OpenConnectionAsync(ct);
        await using var tx = await db.BeginTransactionAsync(ct);
        await using var cmd = new NpgsqlCommand("""
            WITH picked AS MATERIALIZED (
                SELECT id FROM wfc_scale.hub_notification_outbox ORDER BY id
                LIMIT @batch FOR UPDATE SKIP LOCKED
            ), removed AS (
                DELETE FROM wfc_scale.hub_notification_outbox o USING picked p
                WHERE o.id=p.id RETURNING o.hub_id,o.matching_payload,o.created_at
            )
            SELECT pg_notify('wfc_hub_task_ready',hub_id),
                   pg_notify('wfc_hub_matching_ready',matching_payload),
                   extract(epoch FROM (clock_timestamp()-created_at))::double precision
            FROM removed
            """, db, tx);
        cmd.Parameters.AddWithValue("batch", _batchSize);
        var count = 0; double oldest = 0;
        await using (var rows = await cmd.ExecuteReaderAsync(ct))
            while (await rows.ReadAsync(ct)) { count++; oldest = Math.Max(oldest, rows.GetDouble(2)); }
        if (beforeCommit is not null) await beforeCommit(tx, ct);
        await tx.CommitAsync(ct);
        HubTelemetry.NotificationOutboxRows.Add(count);
        if (count > 0) HubTelemetry.NotificationOutboxAge.Record(oldest);
        return count;
    }
}

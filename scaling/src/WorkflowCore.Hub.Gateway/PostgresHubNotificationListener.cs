using System.Threading.Channels;
using Npgsql;
using WorkflowCore.Hub;

namespace WorkflowCore.Hub.Gateway;

/// <summary>
/// Receives transaction-committed, advisory PostgreSQL wakeups across Hub pods.
/// Does not claim work, maintain ownership or replace the periodic DB sweep.
/// One unpooled LISTEN connection is held per Hub process, outside the task pool.
/// </summary>
public sealed class PostgresHubNotificationListener : BackgroundService
{
    public const string ChannelName = "wfc_hub_task_ready";
    private static readonly TimeSpan CoalesceWindow = TimeSpan.FromMilliseconds(25);
    private readonly string _connectionString;
    private readonly HubReadinessBroadcaster _readiness;
    private readonly ILogger<PostgresHubNotificationListener> _logger;

    public PostgresHubNotificationListener(string connectionString,
        HubReadinessBroadcaster readiness,
        ILogger<PostgresHubNotificationListener> logger)
    {
        // LISTEN is session-scoped; a dedicated, non-pooled connection must
        // never pass through a transaction-pooling PgBouncer endpoint.
        var options = new NpgsqlConnectionStringBuilder(connectionString)
        {
            Pooling = false,
            ApplicationName = "workflow-hub-notify"
        };
        _connectionString = options.ConnectionString;
        _readiness = readiness;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var retryDelay = TimeSpan.FromSeconds(1);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await using var connection = new NpgsqlConnection(_connectionString);
                var hints = Channel.CreateBounded<string>(new BoundedChannelOptions(512)
                {
                    SingleReader = true,
                    SingleWriter = true,
                    FullMode = BoundedChannelFullMode.DropOldest
                });
                connection.Notification += (_, args) =>
                {
                    if (args.Channel == ChannelName && args.Payload is { Length: > 0 and <= 80 })
                    {
                        HubTelemetry.PostgresHintsReceived.Add(1);
                        hints.Writer.TryWrite(args.Payload);
                    }
                };

                await connection.OpenAsync(stoppingToken);
                await using (var command = new NpgsqlCommand("LISTEN " + ChannelName, connection))
                    await command.ExecuteNonQueryAsync(stoppingToken);

                HubTelemetry.PostgresListenersActive.Add(1);
                _logger.LogInformation("PostgreSQL work-ready notification listener connected");
                retryDelay = TimeSpan.FromSeconds(1);
                // LISTEN registration precedes this query, avoiding the
                // subscribe/snapshot race. Existing ready work is recovered
                // even when no new event is produced.
                var publishing = PublishHintsAsync(hints.Reader, stoppingToken);
                try
                {
                    await _readiness.ReconcileReadyAsync(stoppingToken);
                    while (!stoppingToken.IsCancellationRequested)
                        await connection.WaitAsync(stoppingToken);
                }
                finally
                {
                    HubTelemetry.PostgresListenersActive.Add(-1);
                    hints.Writer.TryComplete();
                    try { await publishing; }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                HubTelemetry.PostgresListenerFailures.Add(1);
                _logger.LogWarning(ex,
                    "PostgreSQL notification listener disconnected; polling remains active");
            }

            try { await Task.Delay(retryDelay, stoppingToken); }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            retryDelay = TimeSpan.FromSeconds(Math.Min(30, retryDelay.TotalSeconds * 2));
        }
    }

    private async Task PublishHintsAsync(ChannelReader<string> reader, CancellationToken ct)
    {
        while (await reader.WaitToReadAsync(ct))
        {
            // Coalesce bursts (including multiple task rows in one workflow)
            // without creating a REST claim per PostgreSQL notification.
            await Task.Delay(CoalesceWindow, ct);
            var hubs = new HashSet<string>(StringComparer.Ordinal);
            while (reader.TryRead(out var hub))
                hubs.Add(hub);
            foreach (var hub in hubs)
            {
                _readiness.Publish(hub);
                HubTelemetry.PostgresHintsForwarded.Add(1);
            }
        }
    }
}

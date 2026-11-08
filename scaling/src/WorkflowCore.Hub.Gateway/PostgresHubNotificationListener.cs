using System.Threading.Channels;
using System.Text.Json;
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
    public const string MatchingChannelName = "wfc_hub_matching_ready";
    private sealed record Hint(string Hub, string? Type, int Version, bool Legacy, int? Partition = null);
    private static readonly TimeSpan CoalesceWindow = TimeSpan.FromMilliseconds(25);
    private int _listening;
    public bool IsListening => Volatile.Read(ref _listening) == 1;
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
            ApplicationName = "workflow-hub-notify",
            KeepAlive = 30
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
                var hints = Channel.CreateBounded<Hint>(new BoundedChannelOptions(512)
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
                        hints.Writer.TryWrite(new Hint(args.Payload, null, 0, true));
                    }
                    else if (args.Channel == MatchingChannelName && args.Payload.Length <= 512)
                    {
                        try
                        {
                            using var document = JsonDocument.Parse(args.Payload);
                            var root = document.RootElement;
                            var hub = root.GetProperty("hub").GetString();
                            if (string.IsNullOrWhiteSpace(hub) || hub.Length > 80) return;
                            string? type = root.TryGetProperty("type", out var kind) ?
                                kind.GetString() : null;
                            int version = root.TryGetProperty("version", out var ver) ?
                                ver.GetInt32() : 0;
                            if (type is { Length: > 80 } || (type is not null && version < 1))
                                return;
                            int? partition = root.TryGetProperty("partition", out var p)
                                ? p.GetInt32() : null;
                            if (partition < 0) return;
                            HubTelemetry.PostgresHintsReceived.Add(1);
                            hints.Writer.TryWrite(new Hint(hub, type, version, false, partition));
                        }
                        catch (JsonException)
                        {
                            // An advisory hint must never tear down LISTEN.
                        }
                    }
                };

                await connection.OpenAsync(stoppingToken);
                await using (var command = new NpgsqlCommand("LISTEN " + ChannelName, connection))
                    await command.ExecuteNonQueryAsync(stoppingToken);
                await using (var command = new NpgsqlCommand("LISTEN " + MatchingChannelName, connection))
                    await command.ExecuteNonQueryAsync(stoppingToken);

                Volatile.Write(ref _listening, 1);
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
                    Volatile.Write(ref _listening, 0);
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

    private async Task PublishHintsAsync(ChannelReader<Hint> reader, CancellationToken ct)
    {
        while (await reader.WaitToReadAsync(ct))
        {
            // Coalesce committed notifications from multi-task transactions.
            await Task.Delay(CoalesceWindow, ct);
            var legacy = new HashSet<string>(StringComparer.Ordinal);
            var typed = new HashSet<(string Hub, string? Type, int Version, int? Partition)>();
            while (reader.TryRead(out var hint))
            {
                if (hint.Legacy) legacy.Add(hint.Hub);
                else typed.Add((hint.Hub, hint.Type, hint.Version, hint.Partition));
            }
            foreach (var hub in legacy)
            {
                // Legacy SSE (and external clients) continue receiving hub-wide hints.
                _readiness.PublishLegacy(hub);
                HubTelemetry.PostgresHintsForwarded.Add(1);
            }
            foreach (var hint in typed)
            {
                _readiness.PublishMatchingHint(hint.Hub, hint.Type, hint.Version, hint.Partition);
                HubTelemetry.PostgresHintsForwarded.Add(1);
            }
        }
    }
}

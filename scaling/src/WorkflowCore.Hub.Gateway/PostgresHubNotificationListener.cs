using System.Threading.Channels;
using System.Text.Json;
using System.Text;
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
    private sealed record Hint(string Hub, string? Type, int Version, bool Legacy,
        int? Partition = null, bool Recovery = false);
    private static readonly TimeSpan CoalesceWindow = TimeSpan.FromMilliseconds(25);
    private const int MaxHintsPerBatch = 128;
    private int _listening;
    private int _recoveryRequired;
    public bool IsListening => Volatile.Read(ref _listening) == 1;
    internal static bool Enabled
    {
        get
        {
            var flag = Environment.GetEnvironmentVariable("WFC_HUB_PG_NOTIFY_ENABLED");
            return flag != "0" && !string.Equals(flag, "false", StringComparison.OrdinalIgnoreCase);
        }
    }
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
                    FullMode = BoundedChannelFullMode.Wait
                });
                void Enqueue(Hint hint)
                {
                    if (hints.Writer.TryWrite(hint)) return;
                    Interlocked.Exchange(ref _recoveryRequired, 1);
                }
                void InvalidHint()
                {
                    Interlocked.Exchange(ref _recoveryRequired, 1);
                    Enqueue(new Hint(string.Empty, null, 0, false, Recovery: true));
                }
                connection.Notification += (_, args) =>
                {
                    if (args.Channel == ChannelName && args.Payload is { Length: > 0 } && Encoding.UTF8.GetByteCount(args.Payload) <= 80)
                    {
                        HubTelemetry.PostgresHintsReceived.Add(1);
                        Enqueue(new Hint(args.Payload, null, 0, true));
                    }
                    else if (args.Channel == MatchingChannelName && Encoding.UTF8.GetByteCount(args.Payload) <= 512)
                    {
                        try
                        {
                            using var document = JsonDocument.Parse(args.Payload);
                            var root = document.RootElement;
                            if (root.ValueKind != JsonValueKind.Object ||
                                !root.TryGetProperty("hub", out var hubElement) || hubElement.ValueKind != JsonValueKind.String)
                            { InvalidHint(); return; }
                            var hub = hubElement.GetString();
                            if (string.IsNullOrWhiteSpace(hub) || Encoding.UTF8.GetByteCount(hub) > 80)
                            { InvalidHint(); return; }
                            string? type = null;
                            if (root.TryGetProperty("type", out var kind))
                            {
                                if (kind.ValueKind != JsonValueKind.String) { InvalidHint(); return; }
                                type = kind.GetString();
                                if (type is { } value && Encoding.UTF8.GetByteCount(value) > 80) { InvalidHint(); return; }
                            }
                            var version = 0;
                            if (root.TryGetProperty("version", out var ver) &&
                                (ver.ValueKind != JsonValueKind.Number || !ver.TryGetInt32(out version) || version < 0))
                            { InvalidHint(); return; }
                            int? partition = null;
                            if (root.TryGetProperty("partition", out var p))
                            {
                                // This is the task's actual routing partition, not
                                // the 0..63 discovery bucket. Ownership and bucket
                                // routing apply their respective modulo operations.
                                if (p.ValueKind != JsonValueKind.Number || !p.TryGetInt32(out var parsedPartition) || parsedPartition is < 0 or >= 4096)
                                { InvalidHint(); return; }
                                partition = parsedPartition;
                            }
                            if (root.TryGetProperty("taskId", out var idElement))
                            {
                                // Accept a stale exact-ID payload from an older
                                // producer as a coarse partition rescan.
                                if (idElement.ValueKind != JsonValueKind.String || !Guid.TryParse(idElement.GetString() ?? string.Empty, out Guid _) || partition is null)
                                { InvalidHint(); return; }
                            }
                            if (type is not null && version < 1) { InvalidHint(); return; }
                            HubTelemetry.PostgresHintsReceived.Add(1);
                            Enqueue(new Hint(hub, type, version, false, partition));
                        }
                        catch (Exception ex) when (ex is JsonException or InvalidOperationException or FormatException)
                        {
                            InvalidHint();
                        }
                    }
                    else if (args.Channel == MatchingChannelName) InvalidHint();
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
            var recovery = false;
            var drained = 0;
            while (drained < MaxHintsPerBatch && reader.TryRead(out var hint))
            {
                drained++;
                if (hint.Recovery) { recovery = true; continue; }
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
            // Consume once at the publication boundary; a concurrent overflow
            // after this exchange remains set for the next bounded batch.
            recovery |= Interlocked.Exchange(ref _recoveryRequired, 0) != 0;
            if (recovery)
            {
                HubTelemetry.CandidateRecoveryRequests.Add(1,
                    new KeyValuePair<string, object?>("reason", "listener_overflow_or_invalid_payload"));
                _readiness.PublishCandidateRecoveryRequired();
            }
        }
    }

}

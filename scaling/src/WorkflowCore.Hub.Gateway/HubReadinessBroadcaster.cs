using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Diagnostics;
using System.Text.Json;
using System.Threading.Channels;
using WorkflowCore.Hub;

namespace WorkflowCore.Hub.Gateway;

/// <summary>
/// Ephemeral, coalesced SSE readiness hints. PostgreSQL tasks are authoritative.
/// Every Hub replica scans for ready hubs; missing a hint never loses work.
/// </summary>
public sealed class HubReadinessBroadcaster(
    HubTaskStore store, HubWorkflowStore workflows,
    ILogger<HubReadinessBroadcaster> logger) : BackgroundService
{
    private sealed record Subscriber(HashSet<string> Hubs, Channel<string> Channel);
    private readonly ConcurrentDictionary<Guid, Subscriber> _clients = new();
    private readonly bool _eventGated =
        Environment.GetEnvironmentVariable("WFC_HUB_MATCHING_EVENT_GATED") is "true" or "1" or "True";
    private long _lastMatchingRecoveryAt;
    private readonly SemaphoreSlim _matchingRecoveryGate = new(1, 1);

    // Optional in-process matching subscriber; SSE remains available for fallback.
    public event Action<string, string?, int, int?>? TaskReady;

    public void Publish(string hub, string? taskType = null, int version = 0, int? taskPartitionId = null)
    {
        // In event-gated mode, committed PostgreSQL task notifications carry
        // the authoritative task type and partition. Gateway-side generic
        // publishes are only hints for legacy SSE, not 4 speculative SQL claims.
        if (!_eventGated && !string.IsNullOrEmpty(hub))
            TaskReady?.Invoke(hub, taskType, version, taskPartitionId);
        PublishLegacy(hub);
    }

    /// <summary>Old channel still feeds SSE; typed matching has its own channel.</summary>
    public void PublishLegacy(string hub)
    {
        foreach (var client in _clients.Values)
            if (client.Hubs.Contains(hub) && client.Channel.Writer.TryWrite(hub))
                HubTelemetry.SseNotifications.Add(1, HubTelemetry.HubTag(hub));
    }

    /// <summary>In-process hint for a task capability or a permit-release rescan.</summary>
    public void PublishMatchingHint(string hub, string? type, int version, int? taskPartitionId = null)
    {
        if (!string.IsNullOrEmpty(hub)) TaskReady?.Invoke(hub, type, version, taskPartitionId);
    }

    public async IAsyncEnumerable<string> Subscribe(
        IReadOnlyCollection<string> hubs, [EnumeratorCancellation] CancellationToken ct)
    {
        var id = Guid.NewGuid();
        var channel = Channel.CreateBounded<string>(new BoundedChannelOptions(16)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true, SingleWriter = false
        });
        var subscriber = new Subscriber(new HashSet<string>(hubs, StringComparer.Ordinal), channel);
        _clients[id] = subscriber;
        HubTelemetry.SseConnections.Add(1);
        try
        {
            // A subscriber always receives a wakeup on initial connection.
            foreach (var hub in hubs)
                channel.Writer.TryWrite(hub);
            await foreach (var hub in channel.Reader.ReadAllAsync(ct))
                yield return hub;
        }
        finally
        {
            _clients.TryRemove(id, out _);
            HubTelemetry.SseConnections.Add(-1);
            channel.Writer.TryComplete();
        }
    }

    /// <summary>Catch up after LISTEN/reconnect; also used by the periodic safety sweep.</summary>
    public async Task ReconcileReadyAsync(CancellationToken ct)
    {
        // Legacy SSE still gets its 2-second safety sweep.
        foreach (var hub in await store.ReadyHubsAsync(ct))
        {
            if (_eventGated) PublishLegacy(hub);
            else Publish(hub);
        }
        if (!_eventGated) return;

        // Recovery must not turn into a worker *SQL-claim storm*: every
        // 10 seconds one Hub-wide query yields task type + partition hints.
        // Newly committed tasks normally use PostgreSQL LISTEN/NOTIFY.
        var now = Stopwatch.GetTimestamp();
        if (_lastMatchingRecoveryAt != 0 &&
            Stopwatch.GetElapsedTime(Interlocked.Read(ref _lastMatchingRecoveryAt)).TotalSeconds < 10)
            return;
        if (!await _matchingRecoveryGate.WaitAsync(0, ct)) return;
        try
        {
            now = Stopwatch.GetTimestamp();
            if (_lastMatchingRecoveryAt != 0 &&
                Stopwatch.GetElapsedTime(Interlocked.Read(ref _lastMatchingRecoveryAt)).TotalSeconds < 10)
                return;
            // A failed scan must be retried on the next sweep.
            var ready = await store.ReadyMatchingHintsAsync(ct);
            foreach (var hint in ready)
                PublishMatchingHint(hint.Hub, hint.TaskType, hint.Version, hint.Partition);
            Interlocked.Exchange(ref _lastMatchingRecoveryAt, now);
        }
        finally { _matchingRecoveryGate.Release(); }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(2));
        var ticks = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await store.DeadLetterExpiredAsync(stoppingToken);
                await workflows.ReconcileDeadAsync(stoppingToken);
                await ReconcileReadyAsync(stoppingToken);
                // SSE comments keep intermediaries from closing truly idle streams.
                if (++ticks % 5 == 0)
                    foreach (var client in _clients.Values)
                        client.Channel.Writer.TryWrite("");
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex) { logger.LogWarning(ex, "Hub readiness sweep failed"); }
            try
            {
                if (!await timer.WaitForNextTickAsync(stoppingToken)) break;
            }
            catch (OperationCanceledException) { break; }
        }
    }
}

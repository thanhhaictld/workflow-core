using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Channels;
using WorkflowCore.Hub;

namespace WorkflowCore.Hub.Gateway;

/// <summary>
/// Ephemeral, coalesced SSE readiness hints. PostgreSQL tasks are authoritative.
/// Every Hub replica scans for ready hubs; missing a hint never loses work.
/// </summary>
public sealed class HubReadinessBroadcaster(
    HubTaskStore store, ILogger<HubReadinessBroadcaster> logger) : BackgroundService
{
    private sealed record Subscriber(HashSet<string> Hubs, Channel<string> Channel);
    private readonly ConcurrentDictionary<Guid, Subscriber> _clients = new();

    public void Publish(string hub)
    {
        foreach (var client in _clients.Values)
            if (client.Hubs.Contains(hub))
                client.Channel.Writer.TryWrite(hub);
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
            channel.Writer.TryComplete();
        }
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
                foreach (var hub in await store.ReadyHubsAsync(stoppingToken))
                    Publish(hub);
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

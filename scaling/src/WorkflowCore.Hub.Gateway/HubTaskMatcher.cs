using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Channels;
using WorkflowCore.Hub;

namespace WorkflowCore.Hub.Gateway;

/// <summary>
/// In-memory worker demand, with PostgreSQL remaining the authority for task
/// admission and durable leases. Capability-aware hints avoid waking a
/// workflow poller for an activity (or vice versa).
/// </summary>
public sealed class HubTaskMatcher
{
    private sealed class Waiter(HubClaimRequest claim)
    {
        public string Hub { get; } = claim.Hubs[0];
        private readonly HashSet<(string Type, int Version)> _types =
            claim.Capabilities.Select(c => (c.TaskType, c.Version)).ToHashSet();
        public bool Supports(string? type, int version) =>
            type is null || _types.Contains((type, version));
        public Channel<bool> Signal { get; } = Channel.CreateBounded<bool>(
            new BoundedChannelOptions(1) {
                SingleReader = true, SingleWriter = false,
                FullMode = BoundedChannelFullMode.DropWrite
            });
    }

    private readonly HubTaskStore _tasks;
    private readonly HubMatchingOwnership _ownership;
    private readonly object _sync = new();
    private readonly Dictionary<string, LinkedList<Waiter>> _pending = new(StringComparer.Ordinal);
    // An initial DB catch-up scan is necessary for existing backlog. Coalesce
    // many identical worker registrations into one probe per capability lane.
    private readonly ConcurrentDictionary<string, long> _lastProbe = new();
    private int _count;
    private const int MaxPending = 2048;
    public const int WaitSeconds = 20;

    public HubTaskMatcher(HubTaskStore tasks, HubMatchingOwnership ownership,
        HubReadinessBroadcaster readiness)
    {
        _tasks = tasks;
        _ownership = ownership;
        readiness.TaskReady += NotifyReady;
    }

    public int PendingCount { get { lock (_sync) return _count; } }

    public void NotifyReady(string hub, string? type = null, int version = 0)
    {
        lock (_sync)
        {
            if (!_pending.TryGetValue(hub, out var waiters) || waiters.Count == 0) return;
            for (var node = waiters.First; node is not null; node = node.Next)
            {
                if (!node.Value.Supports(type, version)) continue;
                var selected = node.Value;
                waiters.Remove(node);
                waiters.AddLast(selected); // round-robin among compatible workers
                selected.Signal.Writer.TryWrite(true);
                break;
            }
        }
    }

    private bool ShouldProbe(HubClaimRequest request)
    {
        var key = request.Hubs[0] + "|" + string.Join(",",
            request.Capabilities
                .Select(c => c.TaskType + ":" + c.Version)
                .OrderBy(x => x, StringComparer.Ordinal));
        var now = Stopwatch.GetTimestamp();
        while (true)
        {
            if (!_lastProbe.TryGetValue(key, out var previous))
            {
                if (_lastProbe.TryAdd(key, now)) return true;
                continue;
            }
            if (Stopwatch.GetElapsedTime(previous).TotalSeconds < 5) return false;
            if (_lastProbe.TryUpdate(key, now, previous)) return true;
        }
    }

    public async Task<IReadOnlyList<HubLeasedTask>> LongPollAsync(
        HubClaimRequest request, CancellationToken ct)
    {
        var hub = request.Hubs[0];
        var waiter = new Waiter(request);
        lock (_sync)
        {
            if (_count >= MaxPending)
                throw new InvalidOperationException("Matching waiter capacity exceeded");
            if (!_pending.TryGetValue(hub, out var list))
                _pending[hub] = list = new LinkedList<Waiter>();
            list.AddLast(waiter);
            _count++;
        }

        try
        {
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
            deadline.CancelAfter(TimeSpan.FromSeconds(WaitSeconds));
            var shouldClaim = ShouldProbe(request);
            while (!deadline.IsCancellationRequested)
            {
                if (!await _ownership.StillOwnsAsync(hub, ct))
                    throw new InvalidOperationException("Matching owner lease was lost");

                if (shouldClaim)
                {
                    var claimed = await _tasks.ClaimAsync(request.WorkerId,
                        request.Hubs, request.Capabilities, request.MaxTasks,
                        TimeSpan.FromSeconds(45), ct, request.Workflows);
                    if (claimed.Count > 0)
                    {
                        // Another compatible waiter can drain remaining backlog.
                        Remove(waiter);
                        var example = claimed[0];
                        NotifyReady(hub, example.TaskType, example.Version);
                        return claimed;
                    }
                }
                try
                {
                    if (!await waiter.Signal.Reader.WaitToReadAsync(deadline.Token))
                        break;
                    while (waiter.Signal.Reader.TryRead(out _)) { }
                    shouldClaim = true;
                }
                catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                {
                    break;
                }
            }
            return Array.Empty<HubLeasedTask>();
        }
        finally
        {
            Remove(waiter);
        }
    }

    private void Remove(Waiter waiter)
    {
        lock (_sync)
        {
            if (!_pending.TryGetValue(waiter.Hub, out var list)) return;
            if (!list.Remove(waiter)) return;
            _count--;
            if (list.Count == 0) _pending.Remove(waiter.Hub);
        }
    }
}

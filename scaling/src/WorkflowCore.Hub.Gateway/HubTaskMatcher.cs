using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Channels;
using WorkflowCore.Hub;

namespace WorkflowCore.Hub.Gateway;

/// <summary>
/// Waiters live in memory; only an actual admitted claim writes a durable lease.
/// Readiness signals (PostgreSQL NOTIFY or recovery sweep) wake one compatible
/// waiter. No time-based worker claim loop runs while an HTTP request is parked.
/// </summary>
public sealed class HubTaskMatcher
{
    private sealed class Waiter(string hub)
    {
        public string Hub { get; } = hub;
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

    public void NotifyReady(string hub)
    {
        lock (_sync)
        {
            if (!_pending.TryGetValue(hub, out var waiters) || waiters.Count == 0) return;
            // Round-robin instead of waking every pending worker.
            var selected = waiters.First!.Value;
            waiters.RemoveFirst();
            waiters.AddLast(selected);
            selected.Signal.Writer.TryWrite(true);
        }
    }

    public async Task<IReadOnlyList<HubLeasedTask>> LongPollAsync(
        HubClaimRequest request, CancellationToken ct)
    {
        var hub = request.Hubs[0];
        var waiter = new Waiter(hub);
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
            while (!deadline.IsCancellationRequested)
            {
                if (!await _ownership.StillOwnsAsync(hub, ct))
                    throw new InvalidOperationException("Matching owner lease was lost");

                var claimed = await _tasks.ClaimAsync(request.WorkerId,
                    request.Hubs, request.Capabilities, request.MaxTasks,
                    TimeSpan.FromSeconds(45), ct, request.Workflows);
                if (claimed.Count > 0)
                {
                    // Another pending worker may be able to claim the remaining
                    // backlog. Deregister first so the wakeup is not wasted.
                    Remove(waiter);
                    NotifyReady(hub);
                    return claimed;
                }
                // A signal received during the claim remains buffered.
                // With no signal, this waits in memory; no database polling.
                try
                {
                    if (!await waiter.Signal.Reader.WaitToReadAsync(deadline.Token))
                        break;
                    while (waiter.Signal.Reader.TryRead(out _)) { }
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

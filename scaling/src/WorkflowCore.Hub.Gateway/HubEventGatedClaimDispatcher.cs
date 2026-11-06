using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Channels;
using WorkflowCore.Hub;

namespace WorkflowCore.Hub.Gateway;

/// <summary>
/// Per-Hub/per-matching-shard single-flight task acquisition. Worker demand
/// waits in memory. Notifications are coalesced while a claim is running;
/// a successful batch wakes one more compatible waiter to drain backlog.
/// Empty claims stop the drain until a new hint or a bounded catch-up probe.
///
/// This is deliberately NOT an in-memory task queue: PostgreSQL remains the
/// authority for eligibility, FIFO permits, task leasing and fencing.
/// </summary>
public sealed class HubEventGatedClaimDispatcher
{
    private readonly record struct Key(string Hub, int Partition);
    private readonly record struct Hint(string? Type, int Version);

    private sealed class Waiter(HubClaimRequest request)
    {
        public HubClaimRequest Request { get; } = request;
        public Key Key { get; } = new(request.Hubs[0], request.MatchingPartition ?? 0);
        public HashSet<(string Type, int Version)> Capabilities { get; } =
            request.Capabilities.Select(c => (c.TaskType, c.Version)).ToHashSet();
        public Channel<bool> Wake { get; } = Channel.CreateBounded<bool>(
            new BoundedChannelOptions(1) {
                SingleWriter = false, SingleReader = true,
                FullMode = BoundedChannelFullMode.DropOldest
            });
        public Hint? Assigned { get; set; }
        public bool Supports(Hint hint) =>
            hint.Type is null || Capabilities.Contains((hint.Type, hint.Version));
    }

    private sealed class Lane
    {
        public long LastTouched { get; set; } = Stopwatch.GetTimestamp();
        public LinkedList<Waiter> Waiters { get; } = new();
        public HashSet<Hint> Pending { get; } = new();
        public Waiter? Selected { get; set; }
        public bool Claiming { get; set; }
    }

    private readonly Func<HubClaimRequest, CancellationToken, Task<IReadOnlyList<HubLeasedTask>>> _claim;
    private readonly Func<HubClaimRequest, CancellationToken, Task<bool>> _stillOwns;
    private readonly object _gate = new();
    private readonly Dictionary<Key, Lane> _lanes = new();
    private readonly ConcurrentDictionary<string, long> _lastCatchupProbe = new();
    private readonly bool _sharded;
    private readonly int _partitionCount;
    private readonly TimeSpan _pollTimeout;
    private int _waiters;
    private const int MaxWaiters = 2048;
    private const int MaxCachedLanes = 4096;
    private long _lastCacheTrim;

    public HubEventGatedClaimDispatcher(
        Func<HubClaimRequest, CancellationToken, Task<IReadOnlyList<HubLeasedTask>>> claim,
        Func<HubClaimRequest, CancellationToken, Task<bool>> stillOwns,
        bool sharded, int partitionCount, TimeSpan? pollTimeout = null)
    {
        if (partitionCount is < 1 or > 64) throw new ArgumentOutOfRangeException(nameof(partitionCount));
        _claim = claim;
        _stillOwns = stillOwns;
        _sharded = sharded;
        _partitionCount = sharded ? partitionCount : 1;
        _pollTimeout = pollTimeout ?? TimeSpan.FromSeconds(sharded ? 4 : 20);
        if (_pollTimeout <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(pollTimeout));
    }

    public int PendingWaiters { get { lock (_gate) return _waiters; } }

    public void NotifyReady(string hub, string? type = null, int version = 0,
        int? taskPartitionId = null)
    {
        lock (_gate)
        {
            if (taskPartitionId.HasValue)
            {
                var shard = _sharded ? taskPartitionId.Value % _partitionCount : 0;
                if (shard >= 0) AddHint(new Key(hub, shard), new Hint(type, version));
            }
            else
            {
                // A released global/keyed permit may unblock a different
                // partition. Coalesce to at most one pending probe per shard,
                // rather than waking every polling worker.
                for (var shard = 0; shard < _partitionCount; shard++)
                    AddHint(new Key(hub, shard), new Hint(type, version));
            }
        }
    }

    private void AddHint(Key key, Hint hint)
    {
        var now = Stopwatch.GetTimestamp();
        // Retain bounded readiness hints across worker shard rotation. A
        // successful batch may leave more tasks behind after its last waiter
        // returns; discarding that drain token stranded ready activities.
        if (!_lanes.TryGetValue(key, out var lane))
        {
            if (_lanes.Count >= MaxCachedLanes ||
                _lastCacheTrim == 0 ||
                Stopwatch.GetElapsedTime(_lastCacheTrim, now).TotalSeconds >= 30)
                TrimIdle(now);
            if (_lanes.Count >= MaxCachedLanes) return; // periodic DB scan recovers
            _lanes[key] = lane = new Lane();
        }
        lane.LastTouched = now;
        lane.Pending.Add(hint);
        WakeOne(lane);
    }

    private void TrimIdle(long now)
    {
        _lastCacheTrim = now;
        foreach (var (key, lane) in _lanes.ToArray())
            if (!lane.Claiming && lane.Waiters.Count == 0 &&
                Stopwatch.GetElapsedTime(lane.LastTouched, now).TotalSeconds > 30)
                _lanes.Remove(key);
    }

    private static void WakeOne(Lane lane)
    {
        if (lane.Claiming || lane.Selected is not null) return;
        foreach (var hint in lane.Pending)
        {
            for (var node = lane.Waiters.First; node is not null; node = node.Next)
            {
                var waiter = node.Value;
                if (!waiter.Supports(hint)) continue;
                lane.Selected = waiter;
                waiter.Assigned = hint;
                lane.Waiters.Remove(node);
                lane.Waiters.AddLast(waiter);
                waiter.Wake.Writer.TryWrite(true);
                return;
            }
        }
    }

    private bool ShouldProbe(HubClaimRequest request)
    {
        var key = request.Hubs[0] + "#" + (request.MatchingPartition ?? 0) + "|" +
            string.Join(",", request.Capabilities
                .Select(c => c.TaskType + ":" + c.Version)
                .OrderBy(s => s, StringComparer.Ordinal));
        var now = Stopwatch.GetTimestamp();
        while (true)
        {
            if (!_lastCatchupProbe.TryGetValue(key, out var previous))
            {
                if (_lastCatchupProbe.TryAdd(key, now)) return true;
                continue;
            }
            if (Stopwatch.GetElapsedTime(previous).TotalSeconds < 5) return false;
            if (_lastCatchupProbe.TryUpdate(key, now, previous)) return true;
        }
    }

    public async Task<IReadOnlyList<HubLeasedTask>> LongPollAsync(
        HubClaimRequest request, CancellationToken ct)
    {
        if (request.Hubs is not { Length: 1 })
            throw new ArgumentException("One Hub required for matching");

        var waiter = new Waiter(request);
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
        deadline.CancelAfter(_pollTimeout);
        lock (_gate)
        {
            if (_waiters >= MaxWaiters)
                throw new InvalidOperationException("Matching waiter capacity exceeded");
            if (!_lanes.TryGetValue(waiter.Key, out var lane))
                _lanes[waiter.Key] = lane = new Lane();
            lane.LastTouched = Stopwatch.GetTimestamp();
            lane.Waiters.AddLast(waiter);
            _waiters++;
            if (ShouldProbe(request))
            {
                // Start-up / missed-NOTIFY recovery is capability-scoped.
                foreach (var capability in request.Capabilities)
                    lane.Pending.Add(new Hint(capability.TaskType, capability.Version));
            }
            WakeOne(lane);
        }

        try
        {
            while (true)
            {
                try
                {
                    if (!await waiter.Wake.Reader.WaitToReadAsync(deadline.Token))
                        break;
                }
                catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                {
                    break;
                }
                while (waiter.Wake.Reader.TryRead(out _)) { }

                Hint? work = null;
                lock (_gate)
                {
                    var lane = _lanes[waiter.Key];
                    if (lane.Selected == waiter && !lane.Claiming &&
                        waiter.Assigned is Hint selected)
                    {
                        work = selected;
                        lane.Claiming = true;
                        lane.Selected = null;
                        lane.Pending.Remove(selected);
                        waiter.Assigned = null;
                    }
                }
                if (work is null) continue;

                IReadOnlyList<HubLeasedTask> leases = Array.Empty<HubLeasedTask>();
                var completed = false;
                try
                {
                    if (!await _stillOwns(request, ct))
                        throw new InvalidOperationException("Matching owner lease was lost");
                    using var span = HubTelemetry.Traces.StartActivity("hub.matching.claim_drain");
                    span?.SetTag("matching.partition", waiter.Key.Partition);
                    span?.SetTag("matching.partition_count", _partitionCount);
                    span?.SetTag("matching.capability_count", request.Capabilities.Length);
                    var started = Stopwatch.GetTimestamp();
                    leases = await _claim(request, ct);
                    var seconds = Stopwatch.GetElapsedTime(started).TotalSeconds;
                    span?.SetTag("tasks.claimed", leases.Count);
                    HubTelemetry.MatchingDispatchDuration.Record(seconds,
                        new KeyValuePair<string, object?>("outcome",
                            leases.Count > 0 ? "leased" : "empty"));
                    HubTelemetry.MatchingDispatchAttempts.Add(1,
                        new KeyValuePair<string, object?>("outcome",
                            leases.Count > 0 ? "leased" : "empty"));
                    completed = true;
                }
                finally
                {
                    lock (_gate)
                    {
                        var lane = _lanes[waiter.Key];
                        lane.Claiming = false;
                        if (completed && leases.Count > 0)
                        {
                            // Exactly one follow-up probe per capability to
                            // drain tasks left behind by a successful batch.
                            foreach (var task in leases)
                                lane.Pending.Add(new Hint(task.TaskType, task.Version));
                        }
                        // An unsuccessful claim consumes its hint; a new
                        // task/permit signal is needed for another DB call.
                        // Errors recover on the periodic catch-up sweep.
                        WakeOne(lane);
                    }
                }

                if (leases.Count > 0) return leases;
            }
            return Array.Empty<HubLeasedTask>();
        }
        finally
        {
            lock (_gate)
            {
                if (_lanes.TryGetValue(waiter.Key, out var lane))
                {
                    lane.Waiters.Remove(waiter);
                    if (lane.Selected == waiter)
                    {
                        lane.Selected = null;
                        waiter.Assigned = null;
                    }
                    _waiters--;
                    WakeOne(lane);
                    // Keep pending work signals even if the final worker just
                    // rotated to another shard; a later poller must see them.
                    lane.LastTouched = Stopwatch.GetTimestamp();
                    if (lane.Waiters.Count == 0 && !lane.Claiming &&
                        lane.Pending.Count == 0)
                        _lanes.Remove(waiter.Key);
                }
            }
            waiter.Wake.Writer.TryComplete();
        }
    }
}

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
        public int Partition { get; } = claim.MatchingPartition ?? 0;
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
    private readonly HubCandidateAcquisition? _candidate;
    private readonly HubMatchingOwnership _ownership;
    private readonly HubEventGatedClaimDispatcher? _eventGated;
    private readonly object _sync = new();
    private readonly Dictionary<(string Hub, int Partition), LinkedList<Waiter>> _pending = new();
    // An initial DB catch-up scan is necessary for existing backlog. Coalesce
    // many identical worker registrations into one probe per capability lane.
    private readonly ConcurrentDictionary<string, long> _lastProbe = new();
    private int _count;
    private const int MaxPending = 2048;
    public const int WaitSeconds = 20;

    public HubTaskMatcher(HubTaskStore tasks, HubMatchingOwnership ownership,
        HubReadinessBroadcaster readiness, HubCandidateAcquisition? candidate = null)
    {
        _tasks = tasks;
        _candidate = candidate;
        _ownership = ownership;
        if (Environment.GetEnvironmentVariable("WFC_HUB_MATCHING_EVENT_GATED") is "true" or "1" or "True")
        {
            _eventGated = new HubEventGatedClaimDispatcher(
                (request, ct) => tasks.ClaimAsync(request.WorkerId,
                    request.Hubs, request.Capabilities, request.MaxTasks,
                    TimeSpan.FromSeconds(45), ct, request.Workflows,
                    request.MatchingPartition, request.MatchingPartitionCount,
                    deferWhenBusy: true),
                (request, ct) => ownership.Sharded
                    ? ownership.StillOwnsPartitionAsync(request.Hubs[0],
                        request.MatchingPartition!.Value, ct)
                    : ownership.StillOwnsAsync(request.Hubs[0], ct),
                ownership.Sharded, ownership.PartitionCount,
                capabilityScopedClaims: Environment.GetEnvironmentVariable("WFC_HUB_MATCHING_SCOPED_CLAIMS") is "true" or "1" or "True");
        }
        readiness.TaskReady += NotifyReady;
    }

    public int PendingCount => _eventGated is not null
        ? _eventGated.PendingWaiters
        : ReadPendingCount();

    private int ReadPendingCount() { lock (_sync) return _count; }

    public void NotifyReady(string hub, string? type = null, int version = 0,
        int? taskPartitionId = null)
    {
        if (_eventGated is not null)
        {
            _eventGated.NotifyReady(hub, type, version, taskPartitionId);
            return;
        }
        lock (_sync)
        {
            // Null partition means "permit released / recovery scan": any
            // matching shard may contain work. Typed enqueue targets one shard.
            var partition = taskPartitionId.HasValue && _ownership.Sharded
                ? taskPartitionId.Value % _ownership.PartitionCount : (int?)null;
            if (partition.HasValue)
                NotifyOne(hub, partition.Value, type, version);
            else
                for (var shard = 0; shard < _ownership.PartitionCount; shard++)
                    NotifyOne(hub, shard, type, version);
        }
    }

    private void NotifyOne(string hub, int partition, string? type, int version)
    {
        if (!_pending.TryGetValue((hub, partition), out var waiters) || waiters.Count == 0) return;
        for (var node = waiters.First; node is not null; node = node.Next)
        {
            if (!node.Value.Supports(type, version)) continue;
            var selected = node.Value;
            waiters.Remove(node);
            waiters.AddLast(selected);
            selected.Signal.Writer.TryWrite(true);
            break;
        }
    }

    private bool ShouldProbe(HubClaimRequest request)
    {
        var key = request.Hubs[0] + "#" + (request.MatchingPartition ?? 0) + "|" +
            string.Join(",",
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
        if (_candidate?.Enabled == true)
        {
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
            deadline.CancelAfter(TimeSpan.FromSeconds(4));
            try { return await _candidate.AcquireAsync(request,deadline.Token); }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested) { return []; }
            // Candidate memory is bounded. If this owner cannot reserve another
            // credit, keep the worker moving through the existing fenced claim path.
            catch (HubCandidateCapacityException) { }
        }
        if (_eventGated is not null)
            return await _eventGated.LongPollAsync(request, ct);
        var hub = request.Hubs[0];
        var shard = request.MatchingPartition ?? 0;
        var waiter = new Waiter(request);
        lock (_sync)
        {
            if (_count >= MaxPending)
                throw new InvalidOperationException("Matching waiter capacity exceeded");
            if (!_pending.TryGetValue((hub, shard), out var list))
                _pending[(hub, shard)] = list = new LinkedList<Waiter>();
            list.AddLast(waiter);
            _count++;
        }

        try
        {
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
            // Workers rotate shards after a bounded wait when demand is sparse;
            // no shard is permanently stranded without a dedicated poller.
            deadline.CancelAfter(TimeSpan.FromSeconds(
                _ownership.Sharded ? 4 : WaitSeconds));
            var shouldClaim = ShouldProbe(request);
            while (!deadline.IsCancellationRequested)
            {
                var stillOwner = _ownership.Sharded
                    ? await _ownership.StillOwnsPartitionAsync(hub, shard, ct)
                    : await _ownership.StillOwnsAsync(hub, ct);
                if (!stillOwner)
                    throw new InvalidOperationException("Matching owner lease was lost");

                if (shouldClaim)
                {
                    var claimed = await _tasks.ClaimAsync(request.WorkerId,
                        request.Hubs, request.Capabilities, request.MaxTasks,
                        TimeSpan.FromSeconds(45), ct, request.Workflows,
                        request.MatchingPartition, request.MatchingPartitionCount);
                    if (claimed.Count > 0)
                    {
                        // Another compatible waiter can drain remaining backlog.
                        Remove(waiter);
                        var example = claimed[0];
                        NotifyReady(hub, example.TaskType, example.Version,
                            example.PartitionId);
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
            if (!_pending.TryGetValue((waiter.Hub, waiter.Partition), out var list)) return;
            if (!list.Remove(waiter)) return;
            _count--;
            if (list.Count == 0) _pending.Remove((waiter.Hub, waiter.Partition));
        }
    }
}

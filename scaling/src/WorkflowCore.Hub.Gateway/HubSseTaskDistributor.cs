using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using System.Threading.Channels;
using WorkflowCore.Hub;

namespace WorkflowCore.Hub.Gateway;

/// <summary>
/// Owner-fenced, server-originated SSE task delivery. Workers grant ONE-SHOT
/// execution credits; each credit can produce exactly one task.leased event
/// or one credit.unused event. Credits belong to the worker rather than being
/// statically assigned to a matching shard. No extra durable queue is created.
/// </summary>
public sealed class HubSseTaskDistributor
{
    private readonly Func<HubClaimRequest, CancellationToken, Task<IReadOnlyList<HubLeasedTask>>> _poll;
    private readonly object _registryGate = new();
    private readonly ConcurrentDictionary<Guid, Session> _sessions = new();
    private readonly ConcurrentDictionary<(string Worker, string Hub, int Shard), Guid> _active = new();
    private const int MaxSessions = 2048;
    private static readonly TimeSpan OwnerCheckInterval = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan CreditClaimTimeout = TimeSpan.FromSeconds(2);
    private readonly int _mixedClaimBatchSize;
    private readonly TimeSpan _creditCoalesceWindow;
    private long _claimCalls, _claimedTasks, _multiTaskClaims;
    private long _mixedClaimCalls, _mixedClaimedTasks, _mixedMultiTaskClaims;
    private int _maxTasksClaimed, _mixedMaxTasksClaimed;

    // Diagnostic counters report REAL leased tasks, not requested MaxTasks.
    // Keep them bounded and process-local; a multi-replica benchmark must sum
    // both active Hub owners instead of trusting a single Gateway pod.
    public long ClaimCalls => Interlocked.Read(ref _claimCalls);
    public long ClaimedTasks => Interlocked.Read(ref _claimedTasks);
    public long MultiTaskClaims => Interlocked.Read(ref _multiTaskClaims);
    public int MaxTasksClaimed => Volatile.Read(ref _maxTasksClaimed);
    public long MixedClaimCalls => Interlocked.Read(ref _mixedClaimCalls);
    public long MixedClaimedTasks => Interlocked.Read(ref _mixedClaimedTasks);
    public long MixedMultiTaskClaims => Interlocked.Read(ref _mixedMultiTaskClaims);
    public int MixedMaxTasksClaimed => Volatile.Read(ref _mixedMaxTasksClaimed);
    public int MixedClaimBatchSize => _mixedClaimBatchSize;

    public HubSseTaskDistributor(HubTaskMatcher matcher) : this(matcher.LongPollAsync) { }

    // Optional args make the single-claim baseline and batched mode testable
    // without process-global environment changes in parallel test runs.
    public HubSseTaskDistributor(Func<HubClaimRequest, CancellationToken,
        Task<IReadOnlyList<HubLeasedTask>>> poll, int? mixedClaimBatchSize = null,
        int? creditCoalesceMilliseconds = null)
    {
        _poll = poll;
        var sizeSetting = Environment.GetEnvironmentVariable("WFC_HUB_SSE_CLAIM_BATCH_SIZE");
        var delaySetting = Environment.GetEnvironmentVariable("WFC_HUB_SSE_CREDIT_COALESCE_MS");
        var requestedSize = mixedClaimBatchSize ??
            (sizeSetting is null ? 1 : int.TryParse(sizeSetting, out var parsed) ? parsed : -1);
        var requestedDelay = creditCoalesceMilliseconds ??
            (delaySetting is null ? 0 : int.TryParse(delaySetting, out var parsed) ? parsed : -1);
        if (requestedSize is < 1 or > 32 || requestedDelay is < 0 or > 5)
            throw new ArgumentException("SSE mixed task claim batch must be 1..32 with credit coalescing 0..5ms");
        _mixedClaimBatchSize = requestedSize;
        _creditCoalesceWindow = TimeSpan.FromMilliseconds(requestedDelay);
    }

    public sealed record Subscribe(HubClaimRequest Claim, int Capacity, int InitialCredits);
    public sealed record Credit(Guid SessionId, long OwnerEpoch, string WorkerId,
        string HubId, int MatchingPartition, Guid CreditId);

    private sealed class Session(Subscribe subscribe, long ownerEpoch)
    {
        private readonly object _sync = new();
        private readonly Channel<Guid> _credits = Channel.CreateBounded<Guid>(
            new BoundedChannelOptions(64)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait
            });
        private readonly HashSet<Guid> _accepted = [];
        private readonly Queue<Guid> _acceptedOrder = new();
        private int _pending;
        private int _claiming;

        public Guid Id { get; } = Guid.NewGuid();
        public long OwnerEpoch { get; } = ownerEpoch;
        public Subscribe Request { get; } = subscribe;
        public CancellationTokenSource Closed { get; } = new();

        public bool Offer(Guid creditId)
        {
            lock (_sync)
            {
                // An HTTP retry of the SAME credit ID must not mint another slot.
                if (_accepted.Contains(creditId)) return true;
                if (_pending + _claiming >= Request.Capacity)
                    return false;
                if (!_credits.Writer.TryWrite(creditId))
                    return false;
                _accepted.Add(creditId);
                _acceptedOrder.Enqueue(creditId);
                _pending++;
                while (_acceptedOrder.Count > 512)
                    _accepted.Remove(_acceptedOrder.Dequeue());
                return true;
            }
        }

        public async Task<List<Guid>> NextAsync(int maxGrants, TimeSpan coalesce, CancellationToken ct)
        {
            var id = await _credits.Reader.ReadAsync(ct);
            var grants = new List<Guid>(maxGrants) { id };
            // IMPORTANT: mark the first popped credit as claiming BEFORE
            // waiting for more. Otherwise Offer could oversubscribe capacity
            // during the aggregation window.
            lock (_sync)
            {
                _pending--;
                _claiming = 1;
                Drain();
            }
            if (grants.Count >= maxGrants || coalesce <= TimeSpan.Zero) return grants;
            var started = Stopwatch.GetTimestamp();
            while (grants.Count < maxGrants)
            {
                var remaining = coalesce - Stopwatch.GetElapsedTime(started);
                if (remaining <= TimeSpan.Zero) break;
                using var wait = CancellationTokenSource.CreateLinkedTokenSource(ct);
                wait.CancelAfter(remaining);
                try
                {
                    if (!await _credits.Reader.WaitToReadAsync(wait.Token)) break;
                }
                catch (OperationCanceledException) when (!ct.IsCancellationRequested) { break; }
                lock (_sync) Drain();
            }
            return grants;

            void Drain()
            {
                while (grants.Count < maxGrants && _credits.Reader.TryRead(out var next))
                {
                    grants.Add(next);
                    _pending--;
                    _claiming++;
                }
            }
        }

        public void CompleteClaim()
        {
            lock (_sync)
                _claiming = 0;
        }

        public void Cancel()
        {
            try { Closed.Cancel(); } catch (ObjectDisposedException) { }
            _credits.Writer.TryComplete();
        }
    }

    public int ActiveSessions => _sessions.Count;

    private void UpdateMaximum(int count, bool nativeOnly)
    {
        static void Raise(ref int location, int value)
        {
            var old = Volatile.Read(ref location);
            while (value > old)
            {
                var observed = Interlocked.CompareExchange(ref location, value, old);
                if (observed == old) return;
                old = observed;
            }
        }
        Raise(ref _maxTasksClaimed, count);
        if (!nativeOnly) Raise(ref _mixedMaxTasksClaimed, count);
    }

    public bool Offer(Credit credit)
    {
        if (!_sessions.TryGetValue(credit.SessionId, out var session) ||
            session.OwnerEpoch != credit.OwnerEpoch ||
            session.Request.Claim.WorkerId != credit.WorkerId ||
            session.Request.Claim.Hubs[0] != credit.HubId ||
            session.Request.Claim.MatchingPartition != credit.MatchingPartition ||
            credit.CreditId == Guid.Empty)
            return false;
        return session.Offer(credit.CreditId);
    }

    /// <summary>
    /// Owner validation is independent of worker credit availability: a
    /// zero-credit stream must still close when its matching epoch is lost.
    /// PostgreSQL still independently fences every SQL claim.
    /// </summary>
    public async Task StreamAsync(Subscribe subscribe, long ownerEpoch,
        Func<CancellationToken, Task<bool>> stillOwns,
        Func<string, CancellationToken, Task> send, CancellationToken ct)
    {
        if (subscribe.Claim.Hubs is not { Length: 1 } ||
            subscribe.Claim.Capabilities is not { Length: > 0 and <= 128 } ||
            subscribe.Claim.MaxTasks != 1 ||
            subscribe.Claim.MatchingPartition is null ||
            subscribe.Claim.WorkerId is not { Length: > 0 and <= 128 } ||
            subscribe.Capacity is < 1 or > 64 ||
            // A worker must explicitly offer actual free slots after it
            // receives its fenced session epoch. No speculative initial grants.
            subscribe.InitialCredits != 0 || ownerEpoch < 1)
            throw new ArgumentException("Invalid SSE owner/session/credit registration");

        var session = new Session(subscribe, ownerEpoch);
        // Native workflow lanes retain their existing batch limit. The same
        // PostgreSQL batch claim supports remote/managed workflows, activities,
        // and standalone tasks; unlock it only with an explicit feature flag.
        // Every accepted grant represents an actual worker execution slot.
        var nativeOnly = subscribe.Claim.Capabilities.All(c =>
            c.TaskType == "__native_workflow__");
        var maxGrants = Math.Min(subscribe.Capacity,
            nativeOnly ? 32 : _mixedClaimBatchSize);
        var coalesce = !nativeOnly && maxGrants > 1
            ? _creditCoalesceWindow : TimeSpan.Zero;
        var connectedAt = Stopwatch.GetTimestamp();
        var dispatched = false;
        var key = (subscribe.Claim.WorkerId, subscribe.Claim.Hubs[0],
            subscribe.Claim.MatchingPartition.Value);
        lock (_registryGate)
        {
            if (_sessions.Count >= MaxSessions)
                throw new InvalidOperationException("SSE session capacity exceeded");
            if (_active.TryGetValue(key, out var previous) &&
                _sessions.TryGetValue(previous, out var prior))
                prior.Cancel();
            _sessions[session.Id] = session;
            _active[key] = session.Id;
        }
        HubTelemetry.SseTaskSessions.Add(1);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, session.Closed.Token);
        var token = linked.Token;
        try
        {
            await send("event: session\ndata: " +
                JsonSerializer.Serialize(new { sessionId = session.Id, ownerEpoch }) +
                "\n\n", token);
            while (!token.IsCancellationRequested)
            {
                // Never block indefinitely waiting for worker credits: the
                // matching owner epoch may be revoked while capacity is zero.
                List<Guid> grants;
                using (var check = CancellationTokenSource.CreateLinkedTokenSource(token))
                {
                    check.CancelAfter(OwnerCheckInterval);
                    try { grants = await session.NextAsync(maxGrants, coalesce, check.Token); }
                    catch (OperationCanceledException) when (!token.IsCancellationRequested)
                    {
                        if (!await stillOwns(token))
                            throw new InvalidOperationException("SSE matching epoch was revoked");
                        await send(": keepalive\n\n", token);
                        continue;
                    }
                }

                try
                {
                    if (!await stillOwns(token))
                        throw new InvalidOperationException("SSE matching epoch was revoked");
                    IReadOnlyList<HubLeasedTask> leases;
                    using (var claim = CancellationTokenSource.CreateLinkedTokenSource(token))
                    {
                        claim.CancelAfter(CreditClaimTimeout);
                        try { leases = await _poll(subscribe.Claim with { MaxTasks = grants.Count }, claim.Token); }
                        catch (OperationCanceledException) when (!token.IsCancellationRequested)
                        {
                            // A timed-out claim did not deliver a lease.
                            // The worker may move its free credit to a hot shard.
                            leases = [];
                        }
                    }
                    if (leases.Count > grants.Count)
                        throw new InvalidOperationException("SSE claim exceeded reserved credits");
                    Interlocked.Increment(ref _claimCalls);
                    Interlocked.Add(ref _claimedTasks, leases.Count);
                    if (leases.Count > 1) Interlocked.Increment(ref _multiTaskClaims);
                    UpdateMaximum(leases.Count, nativeOnly);
                    if (!nativeOnly)
                    {
                        Interlocked.Increment(ref _mixedClaimCalls);
                        Interlocked.Add(ref _mixedClaimedTasks, leases.Count);
                        if (leases.Count > 1) Interlocked.Increment(ref _mixedMultiTaskClaims);
                    }
                    var lane = new KeyValuePair<string, object?>(
                        "lane", nativeOnly ? "native_workflow" : "mixed");
                    HubTelemetry.SseClaimRequestedCredits.Record(grants.Count, lane);
                    HubTelemetry.SseClaimLeasedTasks.Record(leases.Count, lane);
                    for (var i = 0; i < grants.Count; i++)
                    {
                        var grant = grants[i];
                        if (i >= leases.Count)
                        {
                            await send("event: credit.unused\ndata: " +
                                JsonSerializer.Serialize(new { creditId = grant }) + "\n\n", token);
                            continue;
                        }
                        // A revoked owner's undelivered leases expire normally.
                        if (!await stillOwns(token))
                            throw new InvalidOperationException("SSE owner changed after claim");
                        HubTelemetry.SseTasksDispatched.Add(1);
                        await send("event: task.leased\ndata: " +
                            JsonSerializer.Serialize(new { creditId = grant, task = leases[i] },
                                new JsonSerializerOptions(JsonSerializerDefaults.Web)) +
                            "\n\n", token);
                        if (!dispatched)
                        {
                            dispatched = true;
                            HubTelemetry.SseSessionFirstDispatch.Record(
                                Stopwatch.GetElapsedTime(connectedAt).TotalSeconds);
                        }
                    }
                }
                finally
                {
                    session.CompleteClaim();
                }
            }
        }
        finally
        {
            session.Cancel();
            lock (_registryGate)
            {
                _sessions.TryRemove(session.Id, out _);
                if (_active.TryGetValue(key, out var current) && current == session.Id)
                    _active.TryRemove(key, out _);
            }
            HubTelemetry.SseTaskSessions.Add(-1);
            session.Closed.Dispose();
        }
    }
}

using System.Collections.Concurrent;
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

    public HubSseTaskDistributor(HubTaskMatcher matcher) : this(matcher.LongPollAsync) { }

    // Injected matcher enables deterministic unit tests without a database.
    public HubSseTaskDistributor(Func<HubClaimRequest, CancellationToken,
        Task<IReadOnlyList<HubLeasedTask>>> poll) => _poll = poll;

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
        private bool _claiming;

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
                if (_pending + (_claiming ? 1 : 0) >= Request.Capacity)
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

        public async Task<Guid> NextAsync(CancellationToken ct)
        {
            var id = await _credits.Reader.ReadAsync(ct);
            lock (_sync)
            {
                _pending--;
                _claiming = true;
            }
            return id;
        }

        public void CompleteClaim()
        {
            lock (_sync)
                _claiming = false;
        }

        public void Cancel()
        {
            try { Closed.Cancel(); } catch (ObjectDisposedException) { }
            _credits.Writer.TryComplete();
        }
    }

    public int ActiveSessions => _sessions.Count;

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
                Guid grant;
                using (var check = CancellationTokenSource.CreateLinkedTokenSource(token))
                {
                    check.CancelAfter(OwnerCheckInterval);
                    try { grant = await session.NextAsync(check.Token); }
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
                        try { leases = await _poll(subscribe.Claim, claim.Token); }
                        catch (OperationCanceledException) when (!token.IsCancellationRequested)
                        {
                            // A timed-out claim did not deliver a lease.
                            // The worker may move its free credit to a hot shard.
                            leases = [];
                        }
                    }
                    if (leases.Count > 1)
                        throw new InvalidOperationException("One SSE grant must not lease multiple tasks");
                    if (leases.Count == 0)
                    {
                        await send("event: credit.unused\ndata: " +
                            JsonSerializer.Serialize(new { creditId = grant }) + "\n\n", token);
                        continue;
                    }
                    // Ownership may change between SQL claim and SSE delivery.
                    // Do not dispatch a lease through a revoked owner. The
                    // database lease remains fenced until normal recovery.
                    if (!await stillOwns(token))
                        throw new InvalidOperationException("SSE owner changed after claim");
                    HubTelemetry.SseTasksDispatched.Add(1);
                    await send("event: task.leased\ndata: " +
                        JsonSerializer.Serialize(new { creditId = grant, task = leases[0] },
                            new JsonSerializerOptions(JsonSerializerDefaults.Web)) +
                        "\n\n", token);
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

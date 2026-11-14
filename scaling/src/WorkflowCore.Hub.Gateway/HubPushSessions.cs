using System.Collections.Concurrent;
using System.Threading.Channels;
using WorkflowCore.Hub;

namespace WorkflowCore.Hub.Gateway;

/// <summary>Ephemeral bounded credit sessions; PostgreSQL remains the lease authority.</summary>
public sealed class HubPushSessions
{
    public sealed class Session(HubPushRegistration registration, HubMatchingPartitionLease owner)
    {
        public Guid Id { get; } = Guid.NewGuid();
        public HubPushRegistration Registration { get; } = registration;
        public HubMatchingPartitionLease Owner { get; } = owner;
        public object Gate { get; } = new();
        public Channel<bool> Wake { get; } = Channel.CreateBounded<bool>(new BoundedChannelOptions(1)
            { FullMode = BoundedChannelFullMode.DropWrite });
        public DateTime Expires { get; set; } = DateTime.UtcNow.AddSeconds(TtlSeconds);
        public long Sequence { get; set; } = -1;
        public bool Connected { get; set; }
        public bool Closed { get; set; }
        public bool Revoking { get; set; }
        public int FreeCredits { get; set; }
        public bool Claiming { get; set; }
        // false means the worker has not yet included this delivery in a
        // capacity snapshot, so its slot must still be deducted from that snapshot.
        public Dictionary<Guid, bool> Deliveries { get; } = new();
        public int Available => Math.Max(0, Math.Min(
            FreeCredits - Deliveries.Count(d => !d.Value) - (Claiming ? 1 : 0),
            Registration.MaxCredits - Deliveries.Count - (Claiming ? 1 : 0)));
    }

    public const int TtlSeconds = 15;
    private const int MaxSessions = 8192;
    private readonly object _registrationGate = new();
    private readonly ConcurrentDictionary<Guid, Session> _sessions = new();
    private readonly HashSet<string> _workers = new(StringComparer.Ordinal);
    private DateTime _lastPrune;
    private readonly ConcurrentDictionary<HubClaimRequest, Session> _requests =
        new(ReferenceEqualityComparer.Instance);
    private readonly HubEventGatedClaimDispatcher _matcher;
    private readonly HubTaskStore _tasks;
    private readonly HubMatchingOwnership _ownership;
    private readonly ILogger<HubPushSessions> _logger;
    // Fault-injection hook for an isolated failover test; never enabled by default.
    private readonly int _testBeforeSendMs = int.TryParse(
        Environment.GetEnvironmentVariable("WFC_HUB_PUSH_TEST_BEFORE_SEND_MS"), out var delay)
        ? Math.Clamp(delay, 0, 60000) : 0;
    private long _claims, _emptyClaims, _deliveries, _endedSessions;
    public long Claims => Interlocked.Read(ref _claims);
    public long EmptyClaims => Interlocked.Read(ref _emptyClaims);
    public long Deliveries => Interlocked.Read(ref _deliveries);
    public long EndedSessions => Interlocked.Read(ref _endedSessions);
    public bool Enabled { get; } = Environment.GetEnvironmentVariable("WFC_HUB_PUSH_ENABLED") is "true" or "1" or "True";

    public HubPushSessions(HubTaskStore tasks, HubMatchingOwnership ownership,
        HubReadinessBroadcaster readiness, ILogger<HubPushSessions> logger)
    {
        _tasks = tasks;
        _ownership = ownership;
        _logger = logger;
        _matcher = new HubEventGatedClaimDispatcher(ClaimAsync, (_, _) => Task.FromResult(true),
            ownership.PartitionCount > 1, ownership.PartitionCount, maxWaiters: MaxSessions);
        if (Enabled) readiness.TaskReady += _matcher.NotifyReady;
    }

    private async Task<IReadOnlyList<HubLeasedTask>> ClaimAsync(HubClaimRequest request, CancellationToken ct)
    {
        if (!_requests.TryGetValue(request, out var session))
            throw new InvalidOperationException("Push session no longer exists");
        Interlocked.Increment(ref _claims);
        var result = await _tasks.ClaimAsync(request.WorkerId, request.Hubs, request.Capabilities, 1,
            TimeSpan.FromSeconds(45), ct, request.Workflows, request.MatchingPartition,
            request.MatchingPartitionCount, deferWhenBusy: true,
            ownerFence: new HubClaimOwnerFence(session.Owner.OwnerId, session.Owner.Epoch));
        if (result.Count == 0) Interlocked.Increment(ref _emptyClaims);
        return result;
    }

    public async Task<(Session? Session, HubMatchingPartitionLease Owner)> RegisterAsync(
        HubPushRegistration registration, CancellationToken ct)
    {
        if (!Enabled || !_ownership.Enabled) throw new InvalidOperationException("Push disabled");
        if (string.IsNullOrWhiteSpace(registration.WorkerId) || registration.WorkerId.Length > 128 ||
            string.IsNullOrWhiteSpace(registration.HubId) || registration.HubId.Length > 80 ||
            registration.PartitionCount != _ownership.PartitionCount || registration.Partition < 0 ||
            registration.Partition >= registration.PartitionCount ||
            registration.MaxCredits is < 1 or > 256 ||
            registration.Capabilities is not { Length: > 0 and <= 128 } ||
            registration.Capabilities.Any(c => c.HubId != registration.HubId ||
                string.IsNullOrWhiteSpace(c.TaskType) || c.TaskType.Length > 80 || c.Version < 1) ||
            (registration.Capabilities.Any(c => c.TaskType is "__workflow__" or "__native_workflow__") &&
                registration.Workflows is not { Length: > 0 }) ||
            registration.Workflows?.Length > 128 ||
            registration.Workflows?.Any(w => w.HubId != registration.HubId ||
                string.IsNullOrWhiteSpace(w.DefinitionId) || w.DefinitionId.Length > 128 || w.Version < 1) == true)
            throw new ArgumentException("Invalid push registration");
        var owner = await _ownership.ResolvePartitionAsync(registration.HubId, registration.Partition, ct)
            ?? await _ownership.AcquirePartitionAsync(registration.HubId, registration.Partition, ct);
        if (!_ownership.IsLocal(owner)) return (null, owner);
        lock (_registrationGate)
        {
            if (DateTime.UtcNow - _lastPrune > TimeSpan.FromSeconds(TtlSeconds))
            {
                _lastPrune = DateTime.UtcNow;
                foreach (var pair in _sessions)
                    lock (pair.Value.Gate)
                        if (pair.Value.Expires < DateTime.UtcNow && !pair.Value.Connected &&
                            _sessions.TryRemove(pair.Key, out var expired))
                        {
                            expired.Closed = true;
                            _workers.Remove(expired.Registration.WorkerId);
                        }
            }
            if (_workers.Contains(registration.WorkerId))
                throw new InvalidOperationException("Worker already has a push session on this owner");
            if (_sessions.Count >= MaxSessions) throw new InvalidOperationException("Push session capacity exceeded");
            var session = new Session(registration, owner);
            if (!_sessions.TryAdd(session.Id, session)) throw new InvalidOperationException("Session collision");
            _workers.Add(registration.WorkerId);
            return (session, owner);
        }
    }

    public Session? Find(Guid id) => _sessions.TryGetValue(id, out var session) ? session : null;
    public int ConnectedCount => _sessions.Values.Count(s => { lock (s.Gate) return s.Connected && !s.Closed && s.Expires > DateTime.UtcNow; });

    public bool Connect(Session session)
    {
        lock (session.Gate)
        {
            if (session.Connected || session.Closed || session.Expires <= DateTime.UtcNow) return false;
            session.Connected = true;
            return true;
        }
    }

    public bool Update(Session session, HubPushCapacity update)
    {
        lock (session.Gate)
        {
            if (!session.Connected || session.Closed || session.Expires <= DateTime.UtcNow ||
                update.Sequence <= session.Sequence || update.FreeCredits < 0 ||
                update.FreeCredits > session.Registration.MaxCredits ||
                update.SeenDeliveryIds?.Length > session.Registration.MaxCredits ||
                update.ReleasedDeliveryIds?.Length > session.Registration.MaxCredits ||
                (session.Revoking && update.FreeCredits != 0)) return false;
            session.Sequence = update.Sequence;
            session.Expires = DateTime.UtcNow.AddSeconds(TtlSeconds);
            session.FreeCredits = update.FreeCredits;
            if (update.SeenDeliveryIds is not null)
                foreach (var id in update.SeenDeliveryIds)
                    if (session.Deliveries.ContainsKey(id)) session.Deliveries[id] = true;
            if (update.ReleasedDeliveryId is Guid released) session.Deliveries.Remove(released);
            if (update.ReleasedDeliveryIds is not null)
                foreach (var id in update.ReleasedDeliveryIds) session.Deliveries.Remove(id);
            session.Wake.Writer.TryWrite(true);
            return true;
        }
    }

    // A successful revocation is a barrier: no claim or delivery remains and
    // the closed session cannot accept or reserve another credit.
    public bool Revoke(Session session)
    {
        lock (session.Gate)
        {
            session.FreeCredits = 0;
            session.Revoking = true;
            if (session.Claiming || session.Deliveries.Count != 0) return false;
            session.Closed = true;
            session.Wake.Writer.TryWrite(true);
            return true;
        }
    }

    public void Close(Session session)
    {
        lock (session.Gate) { session.Closed = true; session.Connected = false; session.FreeCredits = 0; }
        lock (_registrationGate)
        {
            if (_sessions.TryRemove(session.Id, out _))
            {
                _workers.Remove(session.Registration.WorkerId);
                Interlocked.Increment(ref _endedSessions);
            }
        }
    }

    public async Task RunAsync(Session session, Func<HubPushAssignment, CancellationToken, Task> send,
        CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                bool claim;
                lock (session.Gate)
                {
                    if (session.Closed || session.Expires <= DateTime.UtcNow) break;
                    claim = session.Available > 0 && !session.Claiming;
                    if (claim) session.Claiming = true;
                }
                if (!claim)
                {
                    using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    timeout.CancelAfter(TimeSpan.FromSeconds(1));
                    try { await session.Wake.Reader.ReadAsync(timeout.Token); }
                    catch (OperationCanceledException) when (!ct.IsCancellationRequested) { }
                    continue;
                }
                var r = session.Registration;
                var request = new HubClaimRequest(r.WorkerId, [r.HubId], r.Capabilities, 1,
                    r.Workflows, r.Partition, r.PartitionCount);
                _requests[request] = session;
                IReadOnlyList<HubLeasedTask> claimed;
                // Bound a reservation even if the worker disappears while SQL
                // is waiting. Cancellation after COMMIT remains uncertain and
                // must never cause early release of the task lease.
                using var claimDeadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
                claimDeadline.CancelAfter(TimeSpan.FromSeconds(10));
                try { claimed = await _matcher.LongPollAsync(request, claimDeadline.Token); }
                catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                { claimed = Array.Empty<HubLeasedTask>(); }
                finally { _requests.TryRemove(request, out _); }
                HubPushAssignment? assignment = null;
                lock (session.Gate)
                {
                    session.Claiming = false;
                    if (claimed.Count != 0)
                    {
                        var delivery = Guid.NewGuid();
                        session.Deliveries.Add(delivery, false);
                        if (!session.Closed && session.Expires > DateTime.UtcNow)
                            assignment = new(session.Id, delivery, r.WorkerId, claimed[0]);
                    }
                }
                if (assignment is not null)
                {
                    if (_testBeforeSendMs > 0) await Task.Delay(_testBeforeSendMs, ct);
                    await send(assignment, ct); // failure is uncertain: leave the DB lease intact
                    Interlocked.Increment(ref _deliveries);
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception ex) { _logger.LogWarning(ex, "Push session ended"); }
        finally
        {
            Close(session);
        }
    }
}

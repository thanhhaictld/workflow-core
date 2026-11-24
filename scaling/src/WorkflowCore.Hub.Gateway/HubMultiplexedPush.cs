using System.Threading.Channels;
using WorkflowCore.Hub;

namespace WorkflowCore.Hub.Gateway;

/// <summary>One SSE stream per processor on this Gateway, with one-shot shard grants.</summary>
internal sealed class HubMultiplexedPush(
    HubPushSessions legacy, HubMatchingOwnership ownership, HubMatchingPartitionLeaseStore partitions,
    HubTaskStore tasks, HubCandidateAcquisition candidate, ILogger<HubMultiplexedPush> logger)
{
    internal sealed class Session(HubPushProcessorRegistration registration, HubPushOwnerSession identity)
    {
        public HubPushProcessorRegistration Registration { get; } = registration;
        public HubPushOwnerSession Identity { get; } = identity;
        public Dictionary<(string, HubPushTaskKind), HubPushCapabilities> Capabilities { get; } =
            registration.Capabilities.ToDictionary(c => (c.HubId, c.Kind));
        public Dictionary<long, HubPushGrant> Pending { get; } = new();
        public Channel<HubPushGrantResult> Results { get; } = Channel.CreateBounded<HubPushGrantResult>(
            registration.ActivitySlots + registration.WorkflowSlots);
        public CancellationTokenSource Stop { get; } = new();
        public DateTime Expires { get; set; } = DateTime.UtcNow.AddSeconds(HubPushSessions.TtlSeconds);
        public long Sequence { get; set; }
        public bool Connected { get; set; }
        public bool Closing { get; set; }
    }

    private readonly object _gate = new();
    private readonly Dictionary<Guid, Session> _sessions = new();
    private readonly HashSet<Guid> _processors = new();
    private readonly Guid _instance = Guid.NewGuid();
    private DateTime _lastPrune;
    private long _claims, _emptyClaims, _deliveries, _ended;
    private readonly int _beforeSendMs = int.TryParse(
        Environment.GetEnvironmentVariable("WFC_HUB_PUSH_TEST_BEFORE_SEND_MS"), out var delay)
        ? Math.Clamp(delay, 0, 60000) : 0;

    public bool Enabled => legacy.Enabled && ownership.Enabled;
    public long Claims => Interlocked.Read(ref _claims);
    public long EmptyClaims => Interlocked.Read(ref _emptyClaims);
    public long Deliveries => Interlocked.Read(ref _deliveries);
    public long EndedSessions => Interlocked.Read(ref _ended);
    public int ConnectedCount
    {
        get { lock (_gate) return _sessions.Values.Count(s => s.Connected); }
    }

    public Session? Register(HubPushProcessorRegistration registration)
    {
        if (registration.ProcessorId == Guid.Empty || registration.PartitionCount != ownership.PartitionCount ||
            registration.ActivitySlots is < 0 or > 256 || registration.WorkflowSlots is < 0 or > 256 ||
            registration.ActivitySlots + registration.WorkflowSlots == 0 ||
            registration.Capabilities is not { Length: > 0 and <= 256 } ||
            registration.Capabilities.Any(c => c is null || !ValidCapabilities(c)) ||
            registration.Capabilities.Select(c => (c.HubId, c.Kind)).Distinct().Count() !=
                registration.Capabilities.Length)
            throw new ArgumentException("Invalid processor push registration");
        lock (_gate)
        {
            var now = DateTime.UtcNow;
            if (now - _lastPrune > TimeSpan.FromSeconds(5))
            {
                _lastPrune = now;
                foreach (var session in _sessions.Values.Where(s => !s.Connected && s.Expires <= now).ToArray())
                {
                    session.Closing = true;
                    RemoveIfDrained(session);
                }
            }
            if (_sessions.Count >= 8192 || !_processors.Add(registration.ProcessorId)) return null;
            var identity = new HubPushOwnerSession(Guid.NewGuid(), _instance, ownership.NodeId);
            var result = new Session(registration, identity);
            _sessions.Add(identity.SessionId, result);
            return result;
        }
    }

    private static bool ValidCapabilities(HubPushCapabilities c) =>
        !string.IsNullOrWhiteSpace(c.HubId) && c.HubId.Length <= 80 &&
        c.Kind is HubPushTaskKind.Activity or HubPushTaskKind.Workflow &&
        c.Tasks is { Length: > 0 and <= 128 } &&
        c.Tasks.All(t => t is not null && t.HubId == c.HubId && t.Version > 0 &&
            !string.IsNullOrWhiteSpace(t.TaskType) && t.TaskType.Length <= 80 &&
            IsWorkflow(t.TaskType) == (c.Kind == HubPushTaskKind.Workflow)) &&
        (c.Kind == HubPushTaskKind.Activity ? c.Workflows is null or { Length: 0 } :
            c.Workflows is { Length: > 0 and <= 128 } && c.Workflows.All(w =>
                w is not null && w.HubId == c.HubId && w.Version > 0 &&
                !string.IsNullOrWhiteSpace(w.DefinitionId) && w.DefinitionId.Length <= 128));

    private static bool IsWorkflow(string taskType) => taskType is "__workflow__" or "__native_workflow__";

    public Session? Connect(Guid id)
    {
        lock (_gate)
        {
            if (!_sessions.TryGetValue(id, out var s) || s.Closing || s.Connected || s.Expires <= DateTime.UtcNow)
                return null;
            s.Connected = true;
            return s;
        }
    }

    public bool Offer(Guid id, HubPushGrant[] grants)
    {
        Session session;
        lock (_gate)
        {
            if (!_sessions.TryGetValue(id, out session!) || session.Closing || !session.Connected ||
                session.Expires <= DateTime.UtcNow || grants.Length > 512) return false;
            var sequence = session.Sequence;
            var activity = 0;
            var workflow = 0;
            foreach (var grant in grants)
            {
                if (grant is null || !ValidGrant(grant) || grant.Sequence <= sequence ||
                    !session.Capabilities.ContainsKey((grant.HubId, grant.Kind))) return false;
                sequence = grant.Sequence;
                if (grant.Kind == HubPushTaskKind.Activity) activity += grant.Credits;
                else workflow += grant.Credits;
            }
            foreach (var grant in session.Pending.Values)
                if (grant.Kind == HubPushTaskKind.Activity) activity += grant.Credits;
                else workflow += grant.Credits;
            if (activity > session.Registration.ActivitySlots || workflow > session.Registration.WorkflowSlots)
                return false;
            foreach (var grant in grants) session.Pending.Add(grant.Sequence, grant);
            session.Sequence = sequence;
            session.Expires = DateTime.UtcNow.AddSeconds(HubPushSessions.TtlSeconds);
        }
        foreach (var grant in grants) _ = AdmitAsync(session, grant);
        return true;
    }

    private bool ValidGrant(HubPushGrant grant) => grant.Partition >= 0 &&
        grant.Partition < ownership.PartitionCount && grant.Sequence > 0 && grant.OwnerEpoch > 0 &&
        !string.IsNullOrWhiteSpace(grant.HubId) && grant.HubId.Length <= 80 &&
        grant.Credits is >= 1 and <= 4 && grant.Kind is HubPushTaskKind.Activity or HubPushTaskKind.Workflow;

    private async Task AdmitAsync(Session session, HubPushGrant grant)
    {
        IReadOnlyList<HubLeasedTask> leases = [];
        var uncertain = false;
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(session.Stop.Token);
        deadline.CancelAfter(TimeSpan.FromMilliseconds(850));
        try
        {
            var owner = ownership.LocalPartition(grant.HubId, grant.Partition, grant.OwnerEpoch);
            if (owner is not null && !deadline.IsCancellationRequested)
            {
                var capabilities = session.Capabilities[(grant.HubId, grant.Kind)];
                var worker = new HubPushGrantRecovery(session.Identity, grant).LeaseOwner;
                var request = new HubClaimRequest(worker, [grant.HubId], capabilities.Tasks, grant.Credits,
                    capabilities.Workflows, grant.Partition, ownership.PartitionCount);
                Interlocked.Increment(ref _claims);
                if (candidate.Enabled)
                {
                    try { leases = await candidate.AcquireAsync(request, deadline.Token, owner); }
                    catch (HubCandidateCapacityException) when (!deadline.IsCancellationRequested)
                    { leases = await ClaimSingleAsync(request, owner); }
                }
                else leases = await ClaimSingleAsync(request, owner);
            }
        }
        // Candidate cancellation waits for any active admission. Only its
        // demand token can prove that a timeout returned no committed leases.
        catch (OperationCanceledException e) when (e.CancellationToken == deadline.Token) { }
        catch (Exception e)
        {
            uncertain = true;
            logger.LogDebug(e, "Push grant {Grant} admission requires reconciliation", grant.Sequence);
        }
        if (leases.Count == 0 && !uncertain) Interlocked.Increment(ref _emptyClaims);
        bool overflow;
        lock (_gate)
        {
            session.Pending.Remove(grant.Sequence);
            overflow = !session.Closing && !session.Results.Writer.TryWrite(
                new HubPushGrantResult(session.Identity.SessionId, grant, leases.ToArray(), uncertain));
            RemoveIfDrained(session);
        }
        if (overflow) Close(session.Identity.SessionId);
    }

    // The compatibility claim always takes the same durable owner fence.
    // Keep its DB transaction independent of the transient SSE request.
    private Task<IReadOnlyList<HubLeasedTask>> ClaimSingleAsync(
        HubClaimRequest request, HubMatchingPartitionLease owner) =>
        tasks.ClaimAsync(request.WorkerId, request.Hubs, request.Capabilities, 1, TimeSpan.FromSeconds(45),
            CancellationToken.None, request.Workflows, request.MatchingPartition,
            request.MatchingPartitionCount, deferWhenBusy: true,
            ownerFence: new HubClaimOwnerFence(owner.OwnerId, owner.Epoch));

    public bool IsExpired(Session session)
    {
        lock (_gate) return session.Expires <= DateTime.UtcNow;
    }

    public async Task SendAsync(HubPushGrantResult frame,
        Func<HubPushGrantResult, CancellationToken, Task> send, CancellationToken ct)
    {
        if (_beforeSendMs > 0 && frame.Tasks.Length > 0) await Task.Delay(_beforeSendMs, ct);
        await send(frame, ct);
        Interlocked.Add(ref _deliveries, frame.Tasks.Length);
    }

    public void Close(Guid id, bool streamEnded = false)
    {
        Session session;
        lock (_gate)
        {
            if (!_sessions.TryGetValue(id, out session!)) return;
            session.Closing = true;
            if (streamEnded) session.Connected = false;
            RemoveIfDrained(session);
        }
        session.Stop.Cancel();
        // A dead SSE stream cannot consume uncommitted matcher credits.
        // Do not release committed or ambiguous leases: they remain fenced.
        if (candidate.Enabled) candidate.CancelUncommittedSessionCredits(session.Identity.SessionId);
    }

    private void RemoveIfDrained(Session session)
    {
        if (!session.Closing || session.Connected || session.Pending.Count != 0 ||
            !_sessions.Remove(session.Identity.SessionId)) return;
        _processors.Remove(session.Registration.ProcessorId);
        Interlocked.Increment(ref _ended);
    }

    public async Task<bool> IsRetiredAsync(HubPushGrantRecovery recovery, CancellationToken ct)
    {
        if (recovery.Session is null || recovery.Grant is null || !ValidGrant(recovery.Grant) ||
            recovery.Session.SessionId == Guid.Empty || recovery.Session.InstanceId == Guid.Empty ||
            string.IsNullOrWhiteSpace(recovery.Session.OwnerId) || recovery.Session.OwnerId.Length > 128)
            throw new ArgumentException("Invalid push grant recovery");
        bool stopped;
        lock (_gate)
        {
            // A missing session is proof only on the exact server process that
            // issued it. A new server at the same URL must use the SQL fence.
            stopped = recovery.Session.InstanceId == _instance && recovery.Session.OwnerId == ownership.NodeId &&
                (!_sessions.TryGetValue(recovery.Session.SessionId, out var s) ||
                    (s.Closing || recovery.Grant.Sequence <= s.Sequence) &&
                    !s.Pending.ContainsKey(recovery.Grant.Sequence));
        }
        return await partitions.IsPushGrantRetiredAsync(recovery, stopped, ct);
    }
}

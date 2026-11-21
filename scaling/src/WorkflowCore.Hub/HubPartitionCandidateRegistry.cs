namespace WorkflowCore.Hub;

// Internal, opt-in building blocks. Nothing starts this registry from the gateway yet.
internal sealed record HubCandidateCoordinatorOptions(
    int PageSize = 128, int PageWireBytes = 65536,
    int PartitionEntries = 2048, int ProcessEntries = 32768,
    long PartitionBytes = 4 * 1024 * 1024, long ProcessBytes = 64 * 1024 * 1024,
    int PartitionCredits = 2048, int ProcessCredits = 16384, int ProcessPartitions = 1024,
    int SequenceProbeEverySweeps = 8, TimeSpan? RecoveryCadence = null,
    TimeSpan? AdmissionCooldown = null, bool RequireDemand = false,
    TimeSpan? SignalThrottleCadence = null)
{
    // Reserve both page objects and temporary materialization. UTF-8 wire bytes
    // alone undercount managed UTF-16 strings, array slots and object headers.
    internal long PageReservationBytes => (long)PageWireBytes * 4 + (long)PageSize * 1024;
    internal TimeSpan Recovery => RecoveryCadence ?? TimeSpan.FromSeconds(10);
    internal TimeSpan Cooldown => AdmissionCooldown ?? TimeSpan.FromSeconds(1);
    internal TimeSpan SignalThrottle => SignalThrottleCadence ?? TimeSpan.FromMilliseconds(100);
    internal void Validate()
    {
        if (PageSize is < 1 or > 1024 || PageWireBytes is < 256 or > 16 * 1024 * 1024 ||
            PartitionEntries < PageSize || ProcessEntries < PartitionEntries ||
            PartitionBytes < PageReservationBytes || ProcessBytes < PartitionBytes ||
            PartitionCredits < 1 || ProcessCredits < PartitionCredits || ProcessPartitions < 1 || SequenceProbeEverySweeps < 1 ||
            Recovery <= TimeSpan.Zero || Cooldown <= TimeSpan.Zero || SignalThrottle <= TimeSpan.Zero)
            throw new ArgumentException("Invalid candidate coordinator limits");
    }
}

internal sealed record HubCandidateCredit(string Id, string WorkerId,
    IReadOnlyList<HubCapability> Capabilities, IReadOnlyList<HubWorkflowCapability>? Workflows);
internal sealed record HubCandidateReservation(Guid Id, HubMatchingPartitionLease Owner,
    int PartitionCount, HubCandidateCredit Credit, HubTaskCandidate Candidate, CancellationToken OwnerStopped);
internal enum HubCandidateResolution { Leased, Unavailable, AdmissionDeferred, OwnerLost, CommitOutcomeUnknown }
internal enum HubCandidateUncertainResolution { LeaseConfirmed, AuthoritativeNoCommit, RetireExpiredCredit }
internal sealed record HubCandidateOversizedObservation(string Hub, int Bucket, HubCandidateStream Stream, HubCandidateOversized Entry);
internal sealed record HubCandidateCoordinatorSnapshot(int Partitions, int Entries, long Bytes,
    int Credits, int Reservations, int Uncertain, int InFlightPages, long OversizedCount,
    HubCandidateOversizedObservation? LastOversized, long CompletedSweeps);

/// <summary>
/// One process-wide registry. All state transitions, including credit and ID
/// reservations, run under one lock; SQL discovery turns may run concurrently
/// across partitions but never concurrently within a partition.
/// The host must run RunAsync exactly once and bind its lifecycle to ownership.
/// </summary>
internal sealed class HubPartitionCandidateRegistry : IDisposable
{
    private sealed class Entry(HubTaskCandidate candidate, long size, long order)
    {
        public HubTaskCandidate Candidate = candidate;
        public long Size = size;
        public long Order = order;
        public DateTimeOffset DeferredUntil;
        public Guid? Reservation;
    }
    private sealed class Credit(HubCandidateCredit value, long bytes)
    {
        public HubCandidateCredit Value = value;
        public long Bytes = bytes;
        public Guid? Reservation;
        public bool Granted;
    }
    private sealed class Stream(int bucket, HubCandidateStream kind)
    {
        public readonly int Bucket = bucket;
        public readonly HubCandidateStream Kind = kind;
        public HubCandidateCursor? Cursor;
        public bool Done;
    }
    private sealed class Partition(HubMatchingPartitionLease owner, int count)
    {
        public readonly HubMatchingPartitionLease Owner = owner;
        public readonly int Count = count;
        public readonly CancellationTokenSource Stop = new();
        public readonly Dictionary<Guid, Entry> Entries = new();
        public readonly Dictionary<(string Type, int Version), SortedSet<(long Sequence, Guid Id)>> Capabilities = new();
        public readonly LinkedList<Credit> Credits = new();
        public readonly LinkedList<Guid> Pending = new();
        public readonly List<Stream> Streams = [];
        public readonly HashSet<int> HintedBuckets = [];
        // At most 64 independent signal buckets per owner; an updated signal
        // resets its cursor without restarting the full recovery sweep.
        public readonly Dictionary<int, (HubCandidateCursor? Cursor, int Version)> SignalBuckets = [];
        public DateTimeOffset NextSignalQuery;
        public readonly Queue<Guid> ExactIds = new();
        public readonly HashSet<Guid> ExactSet = [];
        public int WaitingDemands;
        public HubCandidateHorizon? Horizon;
        public int NextStream;
        public bool Dirty = true;
        public bool HeadRequested;
        public int ForwardSinceHead;
        public int ExactHydrationsSinceRecovery;
        public int NextHeadBucket;
        public bool Active = true;
        public bool TurnInFlight;
        public long Bytes;
        public bool BaseReleased;
        public int InFlightEntries;
        public long InFlightBytes;
        public long Sweeps;
        public DateTimeOffset NextSweep;
    }
    private sealed class Reservation(HubCandidateReservation value, Partition partition, Entry entry, Credit credit)
    {
        public readonly HubCandidateReservation Value = value;
        public readonly Partition Partition = partition;
        public readonly Entry Entry = entry;
        public readonly Credit Credit = credit;
        public bool Dispatched;
        public bool Uncertain;
    }

    private readonly object _sync = new();
    private readonly IHubCandidateDiscovery _reader;
    private const long PartitionOverhead = 16384; // bounded stream/cursor and container overhead
    private readonly HubCandidateCoordinatorOptions _options;
    private readonly TimeProvider _clock;
    private readonly Dictionary<(string Hub, int Partition), Partition> _partitions = new();
    // Retired generations remain charged until all dispatched/granted credits
    // and in-flight page materialization are explicitly resolved.
    private readonly HashSet<Partition> _accountedPartitions = new();
    private readonly Dictionary<string, (Partition Partition, Credit Credit)> _credits = new(StringComparer.Ordinal);
    private readonly Dictionary<Guid, Reservation> _reservations = new();
    private readonly HashSet<(string Hub, Guid Task)> _reservedTasks = new();
    private int _nextPartition;
    private int _entries;
    private long _bytes;
    private int _inFlightPages;
    private int _pendingExactIds;
    private long _order;
    private long _oversized;
    private HubCandidateOversizedObservation? _lastOversized;
    private long _completedSweeps;
    private bool _disposed;
    private int _running;

    internal HubPartitionCandidateRegistry(IHubCandidateDiscovery reader, HubCandidateCoordinatorOptions? options = null, TimeProvider? clock = null)
    { _reader = reader; _options = options ?? new(); _options.Validate(); _clock = clock ?? TimeProvider.System; }

    internal void StartGeneration(HubMatchingPartitionLease owner, int count)
    {
        if (count is < 1 or > 64 || 64 % count != 0 || owner.Partition < 0 || owner.Partition >= count ||
            owner.Epoch < 1 || string.IsNullOrWhiteSpace(owner.HubId) || owner.HubId.Length > 80 ||
            string.IsNullOrWhiteSpace(owner.OwnerId) || owner.OwnerId.Length > 128 ||
            string.IsNullOrWhiteSpace(owner.OwnerUrl) || owner.OwnerUrl.Length > 2048) throw new ArgumentException("Unsupported candidate topology");
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            var key = (owner.HubId, owner.Partition);
            if (_partitions.TryGetValue(key, out var previous))
            {
                if (previous.Owner.Epoch == owner.Epoch && previous.Owner.OwnerId == owner.OwnerId)
                { if (previous.Count != count) throw new InvalidOperationException("Topology changed within generation"); return; }
                if (owner.Epoch <= previous.Owner.Epoch) throw new InvalidOperationException("Stale generation");
                StopLocked(previous);
                _partitions.Remove(key);
            }
            if (_accountedPartitions.Count >= _options.ProcessPartitions ||
                _bytes + PartitionOverhead > _options.ProcessBytes ||
                GroupBytes(owner.HubId,owner.Partition) + PartitionOverhead > _options.PartitionBytes ||
                PartitionOverhead + _options.PageReservationBytes > _options.PartitionBytes)
                throw new InvalidOperationException("Candidate partition capacity exceeded");
            var created = new Partition(owner, count) { Bytes = PartitionOverhead };
            _partitions[key] = created; _accountedPartitions.Add(created);
            _bytes += PartitionOverhead;
        }
    }
    internal void StopGeneration(string hub, int partition, long epoch)
    {
        lock (_sync) if (_partitions.TryGetValue((hub, partition), out var p) && p.Owner.Epoch == epoch)
        { StopLocked(p); _partitions.Remove((hub, partition)); }
    }
    private void StopLocked(Partition p)
    {
        if (!p.Active) return;
        p.Active = false;
        _ = p.Stop.CancelAsync();
        foreach (var entry in p.Entries.Values.ToArray())
            if (entry.Reservation is null) RemoveEntryLocked(p, entry);
        while (p.ExactIds.TryDequeue(out var exactId))
        {
            if (!p.ExactSet.Remove(exactId)) continue;
            p.Bytes -= 64; _bytes -= 64; _pendingExactIds--;
        }
        foreach (var credit in p.Credits.ToArray())
        {
            if (credit.Reservation is not Guid id) { if (!credit.Granted) RemoveCreditLocked(p, credit); }
            else if (_reservations.TryGetValue(id, out var reservation) && !reservation.Dispatched)
                FinishLocked(reservation, false, false);
            // Dispatched claims may have committed. The candidate and credit
            // stay charged until the caller supplies a definite resolution.
        }
        p.Pending.Clear(); p.SignalBuckets.Clear();
        ReleaseStoppedPartitionLocked(p);
    }

    internal bool AddCredit(string hub, int partition, long epoch, HubCandidateCredit credit)
    {
        var item = CreateCredit(credit);
        lock (_sync)
        {
            if (!GetLocked(hub, partition, epoch, out var p) || !CanAddCreditLocked(p, item)) return false;
            InstallCreditLocked(p, item);
            MatchLocked(p, _clock.GetUtcNow());
            return true;
        }
    }

    private static Credit CreateCredit(HubCandidateCredit credit)
    {
        ArgumentNullException.ThrowIfNull(credit);
        if (string.IsNullOrWhiteSpace(credit.Id) || credit.Id.Length > 256 ||
            string.IsNullOrWhiteSpace(credit.WorkerId) || credit.WorkerId.Length > 128 ||
            credit.Capabilities.Count is < 1 or > 128 || credit.Workflows?.Count > 128)
            throw new ArgumentException("Invalid candidate credit");

        var copy = credit with { Capabilities = credit.Capabilities.ToArray(), Workflows = credit.Workflows?.ToArray() };
        return new Credit(copy, CreditSize(copy));
    }

    private bool CanAddCreditLocked(Partition p, Credit credit) =>
        !_credits.ContainsKey(credit.Value.Id) &&
        GroupCredits(p.Owner.HubId, p.Owner.Partition) < _options.PartitionCredits &&
        _credits.Count < _options.ProcessCredits &&
        GroupBytes(p.Owner.HubId, p.Owner.Partition) + credit.Bytes <= _options.PartitionBytes &&
        _bytes + credit.Bytes <= _options.ProcessBytes;

    private void InstallCreditLocked(Partition p, Credit credit)
    {
        p.Credits.AddLast(credit);
        _credits.Add(credit.Value.Id, (p, credit));
        p.Bytes += credit.Bytes;
        _bytes += credit.Bytes;
    }

    internal bool HasCandidateCapacity(string hub, int partition, long epoch)
    {
        lock (_sync)
            return GetLocked(hub, partition, epoch, out var p) && p.Entries.Count > 0 &&
                GroupCredits(hub, partition) < _options.PartitionCredits && _credits.Count < _options.ProcessCredits;
    }
    internal bool AddWaitingDemand(string hub, int partition, long epoch)
    {
        lock (_sync)
        {
            if (!GetLocked(hub, partition, epoch, out var p)) return false;
            if (p.WaitingDemands++ == 0 && !p.Credits.Any(c => !c.Granted && c.Reservation is null))
                p.Dirty = true;
            return true;
        }
    }
    internal void RemoveWaitingDemand(string hub, int partition, long epoch)
    {
        lock (_sync)
            if (GetLocked(hub, partition, epoch, out var p) && p.WaitingDemands > 0) p.WaitingDemands--;
    }
    internal bool AddMatchedCredit(string hub, int partition, long epoch, HubCandidateCredit credit)
    {
        lock (_sync)
        {
            if (!GetLocked(hub, partition, epoch, out var p)) return false;
            var now = _clock.GetUtcNow();
            MatchLocked(p, now);
            var entry = FindEligibleCandidateLocked(p, credit, now);
            if (entry is null) return false;
            var item = CreateCredit(credit);
            if (!CanAddCreditLocked(p, item)) return false;
            InstallCreditLocked(p, item);
            ReserveLocked(p, item, entry);
            return true;
        }
    }

    internal bool CancelCredit(string creditId)
    {
        lock (_sync)
        {
            if (!_credits.TryGetValue(creditId, out var pair) || pair.Credit.Granted) return false;
            if (pair.Credit.Reservation is Guid id)
            {
                if (!_reservations.TryGetValue(id, out var reservation) || reservation.Dispatched) return false;
                // This reservation has never left the local drain: no database
                // transaction can exist, so its candidate is safe to retain.
                FinishLocked(reservation, true, false);
            }
            RemoveCreditLocked(pair.Partition, pair.Credit);
            if (pair.Partition.Active) MatchLocked(pair.Partition, _clock.GetUtcNow());
            return true;
        }
    }
    // Called only when transport/session accounting confirms the granted slot
    // is no longer outstanding. Owner loss alone never releases it.
    internal bool ReleaseGrantedCredit(string creditId)
    {
        lock (_sync)
        {
            if (!_credits.TryGetValue(creditId, out var pair) || !pair.Credit.Granted) return false;
            RemoveCreditLocked(pair.Partition, pair.Credit);
            ReleaseStoppedPartitionLocked(pair.Partition);
            return true;
        }
    }
    internal HubCandidateReservation? TakeReservation(string hub, int partition, long epoch)
    {
        lock (_sync)
        {
            if (!GetLocked(hub, partition, epoch, out var p)) return null;
            while (p.Pending.First is not null)
            {
                var id = p.Pending.First.Value; p.Pending.RemoveFirst();
                if (_reservations.TryGetValue(id, out var r) && !r.Dispatched)
                { r.Dispatched = true; return r.Value; }
            }
            return null;
        }
    }
    internal bool HasPending(string hub, int partition, long epoch)
    {
        lock (_sync) return GetLocked(hub, partition, epoch, out var p) && p.Pending.Count != 0;
    }
    // A dispatched reservation that has not entered admission can be retired
    // atomically without allowing the credit to rematch another candidate.
    internal void AbandonBeforeAdmission(Guid id)
    {
        lock (_sync)
        {
            if (!_reservations.TryGetValue(id,out var r) || !r.Dispatched || r.Uncertain)
                throw new InvalidOperationException("Reservation cannot be abandoned");
            var p=r.Partition; var credit=r.Credit;
            FinishLocked(r, true, false);
            RemoveCreditLocked(p,credit);
            ReleaseStoppedPartitionLocked(p);
            if (p.Active) MatchLocked(p,_clock.GetUtcNow());
        }
    }
    internal IReadOnlyList<HubMatchingPartitionLease> ActiveOwners()
    { lock (_sync) return _partitions.Values.Where(x=>x.Active).Select(x=>x.Owner).ToArray(); }
    internal void Complete(Guid id, HubCandidateResolution result, DateTimeOffset now)
    {
        if (!Enum.IsDefined(result)) throw new ArgumentOutOfRangeException(nameof(result));
        lock (_sync)
        {
            if (!_reservations.TryGetValue(id, out var r) || !r.Dispatched || r.Uncertain)
                throw new InvalidOperationException("Unknown or unresolved reservation");
            if (result == HubCandidateResolution.CommitOutcomeUnknown) { r.Uncertain = true; return; }
            if (result == HubCandidateResolution.OwnerLost)
            {
                FinishLocked(r, false, false);
                if (_partitions.TryGetValue((r.Partition.Owner.HubId,r.Partition.Owner.Partition), out var current) && ReferenceEquals(current,r.Partition))
                { StopLocked(current); _partitions.Remove((current.Owner.HubId,current.Owner.Partition)); }
                return;
            }
            if (result == HubCandidateResolution.AdmissionDeferred) r.Entry.DeferredUntil = now + _options.Cooldown;
            FinishLocked(r, result == HubCandidateResolution.AdmissionDeferred,
                result == HubCandidateResolution.Leased);
            if (r.Partition.Active) MatchLocked(r.Partition, now);
        }
    }
    // Only an authoritative outcome supplied by admission/session recovery can
    // settle uncertainty. A negative current-lease lookup is not such evidence.
    internal void ResolveUncertain(Guid id, HubCandidateUncertainResolution resolution, DateTimeOffset now)
    {
        if (!Enum.IsDefined(resolution)) throw new ArgumentOutOfRangeException(nameof(resolution));
        lock (_sync)
        {
            if (!_reservations.TryGetValue(id, out var r) || !r.Uncertain) throw new InvalidOperationException("Reservation is not uncertain");
            FinishLocked(r, false, resolution is HubCandidateUncertainResolution.LeaseConfirmed or HubCandidateUncertainResolution.RetireExpiredCredit);
            if (r.Partition.Active) MatchLocked(r.Partition, now);
        }
    }
    private void FinishLocked(Reservation r, bool retainCandidate, bool consumeCredit)
    {
        var p = r.Partition; var entry = r.Entry; var credit = r.Credit;
        p.Pending.Remove(r.Value.Id);
        _reservations.Remove(r.Value.Id); _reservedTasks.Remove((p.Owner.HubId, entry.Candidate.Id));
        entry.Reservation = null; credit.Reservation = null;
        if (!retainCandidate || !p.Active) RemoveEntryLocked(p, entry);
        if (consumeCredit) credit.Granted = true;
        else if (!p.Active) RemoveCreditLocked(p, credit);
        // Rotating a released credit prevents one worker monopolizing matches.
        else { p.Credits.Remove(credit); p.Credits.AddLast(credit); }
        ReleaseStoppedPartitionLocked(p);
    }
    internal void Notify(string hub, int partition, long epoch, int? bucket = null)
    {
        lock (_sync)
            if (GetLocked(hub, partition, epoch, out var p))
            {
                if (bucket is >= 0 and < 64 && bucket % p.Count == p.Owner.Partition)
                {
                    // Bucket signals are advisory, not full sweep requests.
                    if (!p.SignalBuckets.TryGetValue(bucket.Value, out var previous))
                        p.SignalBuckets[bucket.Value] = (null, 1);
                    else
                    {
                        p.SignalBuckets[bucket.Value] = (null, unchecked(previous.Version + 1));
                        HubTelemetry.CandidateSignalHintsCoalesced.Add(1);
                    }
                    p.HintedBuckets.Add(bucket.Value);
                }
                else
                {
                    // Unknown bucket is a deliberate hub-wide recovery signal.
                    p.Dirty = true;
                    p.HeadRequested = true;
                }
                MatchLocked(p, _clock.GetUtcNow());
            }
    }
    internal bool NotifyTask(string hub, int partition, long epoch, Guid taskId)
    {
        lock (_sync)
        {
            if (!GetLocked(hub, partition, epoch, out var p)) return false;
            if (p.ExactSet.Contains(taskId))
            { HubTelemetry.CandidateHintsDeduped.Add(1); return true; }
            var perPartitionLimit = Math.Min(_options.PartitionEntries, Math.Max(_options.PageSize, _options.PageSize * 8));
            if (p.ExactSet.Count >= perPartitionLimit || _pendingExactIds >= _options.ProcessEntries ||
                GroupBytes(p.Owner.HubId, p.Owner.Partition) + 64 > _options.PartitionBytes || _bytes + 64 > _options.ProcessBytes)
            {
                p.Dirty = true;
                p.HeadRequested = true;
                HubTelemetry.CandidateHintsDropped.Add(1);
                return false;
            }
            p.ExactSet.Add(taskId);
            p.ExactIds.Enqueue(taskId);
            p.Bytes += 64; _bytes += 64; _pendingExactIds++;
            HubTelemetry.CandidateHintsReceived.Add(1);
            return true;
        }
    }
    private bool GetLocked(string hub, int partition, long epoch, out Partition p) =>
        _partitions.TryGetValue((hub, partition), out p!) && p.Active && p.Owner.Epoch == epoch;

    // One bounded discovery-page turn at most, with no more than one batched
    // task hydration and one workflow metadata fetch. Notifications set Dirty
    // but never reset a live horizon or cursor. Empty buckets consume turns too,
    // making their fanout explicit and fair between partitions.
    internal async Task<bool> RunOneTurnAsync(DateTimeOffset now, CancellationToken ct,
        Action<Exception>? reportError = null)
    {
        Partition? p = null; Stream? stream = null; Guid[]? hydrateIds = null;
        bool capture = false; bool booked = false; bool head = false;
        int? targetedBucket = null; int targetedVersion = 0;
        try
        {
            lock (_sync)
            {
                if (_disposed || _partitions.Count == 0) return false;
                var all = _partitions.Values.ToArray();
                for (var i = 0; i < all.Length; i++)
                {
                    var index = (_nextPartition + i) % all.Length; var candidate = all[index];
                    // A capture/page/hydration turn mutates the partition's cursor
                    // and reserved-page accounting. Never overlap two such turns.
                    if (!candidate.Active || candidate.TurnInFlight) continue;
                    MatchLocked(candidate, now);
                    if (_options.RequireDemand && !candidate.Credits.Any(c => !c.Granted && c.Reservation is null) &&
                        (candidate.WaitingDemands == 0 || GroupCredits(candidate.Owner.HubId, candidate.Owner.Partition) >= _options.PartitionCredits ||
                         _credits.Count >= _options.ProcessCredits)) continue;
                    var targetedDue = candidate.SignalBuckets.Count > 0 && now >= candidate.NextSignalQuery;
                    if (candidate.ExactIds.Count == 0 && candidate.Horizon is null &&
                        !candidate.Dirty && now < candidate.NextSweep && !targetedDue) continue;
                    p = candidate; p.TurnInFlight = true;
                    _nextPartition = (index + 1) % all.Length; break;
                }
                if (p is null) return false;
                var recoveryTurnDue = p.Dirty || p.Horizon is not null || now >= p.NextSweep;
                if (p.ExactIds.Count > 0 && !(p.ExactHydrationsSinceRecovery >= 8 && recoveryTurnDue))
                {
                    if (!BookPageLocked(p))
                    {
                        // Exact IDs are advisory and recovery is authoritative.
                        // Release their queue budget so a bounded indexed page
                        // can make progress when the process is under pressure.
                        var droppedIds = p.ExactSet.Count;
                        p.ExactIds.Clear(); p.ExactSet.Clear();
                        var released = (long)droppedIds * 64;
                        p.Bytes -= released; _bytes -= released; _pendingExactIds -= droppedIds;
                        p.Dirty = true; p.HeadRequested = true;
                        HubTelemetry.CandidateHintsDropped.Add(1);
                        HubTelemetry.CandidateRecoveryRequests.Add(1,
                            new KeyValuePair<string, object?>("reason", "exact_queue_memory_pressure"));
                        return false;
                    }
                    var maxIdsByWireBudget = Math.Max(1, _options.PageWireBytes / 256);
                    var count = Math.Min(Math.Min(_options.PageSize, maxIdsByWireBudget), p.ExactIds.Count);
                    hydrateIds = new Guid[count];
                    for (var i = 0; i < count; i++)
                    {
                        var id = p.ExactIds.Dequeue();
                        p.ExactSet.Remove(id);
                        p.Bytes -= 64; _bytes -= 64; _pendingExactIds--;
                        hydrateIds[i] = id;
                    }
                    p.ExactHydrationsSinceRecovery++;
                    booked = true;
                }
                else if (p.Horizon is null && !p.Dirty && now < p.NextSweep &&
                         p.SignalBuckets.Count > 0 && now >= p.NextSignalQuery)
                {
                    // Single indexed ReadyDue page, without scanning every
                    // physical bucket or capturing a global sequence horizon.
                    var first = p.SignalBuckets.First();
                    targetedBucket = first.Key; targetedVersion = first.Value.Version;
                    stream = new(first.Key, HubCandidateStream.ReadyDue) { Cursor = first.Value.Cursor };
                    if (!BookPageLocked(p)) return false;
                    booked = true;
                    p.NextSignalQuery = now + _options.SignalThrottle;
                }
                else if (p.Horizon is null) { capture = true; p.Dirty = false; p.HeadRequested = false; p.ForwardSinceHead = 0; }
                else
                {
                    // One bounded head catch-up for every eight forward pages
                    // at most; it never moves or resets the forward cursors.
                    if (p.HeadRequested && p.ForwardSinceHead >= 8)
                    {
                        var buckets = 64 / p.Count;
                        var bucket = p.HintedBuckets.Count != 0 ? p.HintedBuckets.First() : p.Owner.Partition + (p.NextHeadBucket++ % buckets) * p.Count;
                        p.HintedBuckets.Remove(bucket);
                        stream = new(bucket,HubCandidateStream.ReadyDue); head = true;
                        p.HeadRequested = false; p.ForwardSinceHead = 0;
                    }
                    for (var i = 0; stream is null && i < p.Streams.Count; i++)
                    {
                        var index = (p.NextStream + i) % p.Streams.Count;
                        if (p.Streams[index].Done) continue;
                        stream = p.Streams[index]; p.NextStream = (index + 1) % p.Streams.Count; break;
                    }
                    if (stream is null) { EndSweepLocked(p, now); return true; }
                    if (!BookPageLocked(p))
                    { if (head) { p.HeadRequested = true; p.ForwardSinceHead = 8; } return false; }
                    booked = true;
                }
            }
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, p.Stop.Token);
            if (hydrateIds is not null)
            {
                HubTelemetry.CandidateHydrationCalls.Add(1);
                var hydrated = await _reader.HydrateAsync(p.Owner.HubId, p.Owner.Partition, p.Count,
                    hydrateIds, _options.PageWireBytes, linked.Token);
                if (hydrated.WorkflowMetadataError is { } workflowError) reportError?.Invoke(workflowError);
                HubTelemetry.CandidateHydrationRows.Add(hydrated.Items.Count);
                HubTelemetry.CandidateHydrationOversized.Add(hydrated.Oversized);
                lock (_sync) if (p.Active) ApplyHydratedLocked(p, hydrateIds, hydrated.Items, now);
                return true;
            }
            if (capture)
            {
                var horizon = await _reader.CaptureHorizonAsync(p.Owner.HubId, linked.Token);
                lock (_sync) if (p.Active)
                {
                    p.Horizon = horizon; p.Streams.Clear(); p.NextStream = 0;
                    var sequenceProbe = (p.Sweeps + 1) % _options.SequenceProbeEverySweeps == 0;
                    var buckets = Enumerable.Range(0, 64 / p.Count).Select(i => p.Owner.Partition + i * p.Count)
                        .OrderByDescending(p.HintedBuckets.Contains).ToArray();
                    p.HintedBuckets.Clear();
                    foreach (var b in buckets)
                    {
                        p.Streams.Add(new(b, HubCandidateStream.ReadyDue));
                        p.Streams.Add(new(b, HubCandidateStream.Expired));
                        if (sequenceProbe) p.Streams.Add(new(b, HubCandidateStream.Ready));
                    }
                }
                return true;
            }
            var horizon = p.Horizon ?? new HubCandidateHorizon(long.MaxValue, now.UtcDateTime);
            if (targetedBucket.HasValue) HubTelemetry.CandidateSignalBucketQueries.Add(1);
            var page = await _reader.ReadPageAsync(p.Owner.HubId, stream!.Bucket, stream.Kind, horizon, stream.Cursor,
                _options.PageSize, _options.PageWireBytes, linked.Token);
            HubTelemetry.CandidateDiscoveryRows.Add(page.Items.Count);
            if (page.WorkflowMetadataError is { } metadataError) reportError?.Invoke(metadataError);
            // A base identity larger than the entire configured page cannot be
            // safely materialized within this owner's memory reservation. Surface
            // the limit rather than silently dropping the row on every sweep.
            foreach (var marker in page.Oversized ?? [])
                reportError?.Invoke(new InvalidOperationException(
                    $"Candidate {marker.Id} exceeds the configured page identity limit of {_options.PageWireBytes} bytes"));
            lock (_sync)
            {
                if (!p.Active) return true;
                foreach (var marker in page.Oversized ?? [])
                {
                    _oversized++; _lastOversized = new(p.Owner.HubId,stream.Bucket,stream.Kind,marker);
                    if (p.Entries.TryGetValue(marker.Id, out var old) && old.Reservation is null &&
                        !_reservedTasks.Contains((p.Owner.HubId, marker.Id))) RemoveEntryLocked(p, old);
                }
                foreach (var item in page.Items)
                {
                    if (item.HubId != p.Owner.HubId || item.PartitionId % p.Count != p.Owner.Partition || item.PartitionId % 64 != stream.Bucket)
                        throw new InvalidOperationException("Discovery returned a candidate outside its owner stream");
                    var deferredUntil = default(DateTimeOffset);
                    if (p.Entries.TryGetValue(item.Id, out var old))
                    {
                        if (old.Reservation is not null || old.Candidate.Epoch > item.Epoch) continue;
                        if (old.Candidate.Epoch == item.Epoch) deferredUntil = old.DeferredUntil;
                        RemoveEntryLocked(p, old);
                    }
                    if (_reservedTasks.Contains((p.Owner.HubId,item.Id))) continue;
                    var size = CandidateSize(item);
                    if (size > p.InFlightBytes || p.InFlightEntries < 1) throw new InvalidOperationException("Page exceeded managed memory reservation");
                    p.InFlightBytes -= size; _bytes -= size; p.Bytes -= size;
                    p.InFlightEntries--; _entries--;
                    var entry = new Entry(item,size,++_order) { DeferredUntil = deferredUntil }; p.Entries.Add(item.Id,entry);
                    var key = (item.TaskType,item.Version);
                    if (!p.Capabilities.TryGetValue(key, out var queue)) p.Capabilities.Add(key,queue = new());
                    queue.Add((item.Sequence,item.Id));
                    p.Bytes += size; _bytes += size; _entries++;
                    MatchLocked(p, now);
                }
                if (targetedBucket is int bucket)
                {
                    // A newer notification invalidates the cursor captured by
                    // this query. Never drop a newly committed wake-up.
                    if (p.SignalBuckets.TryGetValue(bucket, out var current) &&
                        current.Version == targetedVersion)
                    {
                        if (page.EndOfStream) p.SignalBuckets.Remove(bucket);
                        else p.SignalBuckets[bucket] = (page.Next, current.Version);
                    }
                }
                else if (!head)
                {
                    stream.Cursor = page.Next;
                    stream.Done = page.EndOfStream;
                    p.ForwardSinceHead++;
                    p.ExactHydrationsSinceRecovery = 0;
                    if (p.Streams.All(x => x.Done)) EndSweepLocked(p, now);
                }
            }
            return true;
        }
        catch (OperationCanceledException) when (p is not null && p.Stop.IsCancellationRequested && !ct.IsCancellationRequested) { return true; }
        catch (OperationCanceledException)
        {
            if (p is not null) lock (_sync) if (p.Active)
            {
                p.Dirty = true;
            }
            throw;
        }
        catch
        {
            if (p is not null) lock (_sync) if (p.Active)
            {
                if (capture || hydrateIds is not null) p.Dirty = true;
                if (hydrateIds is not null) p.ExactHydrationsSinceRecovery = Math.Max(8, p.ExactHydrationsSinceRecovery);
                if (hydrateIds is not null) RequeueExactLocked(p, hydrateIds);
                if (hydrateIds is not null) HubTelemetry.CandidateRecoveryRequests.Add(1,
                    new KeyValuePair<string, object?>("reason", "hydration_failure"));
                if (head) { p.HeadRequested = true; p.ForwardSinceHead = 8; }
            }
            throw;
        }
        finally
        {
            if (p is not null) lock (_sync)
            {
                if (booked) ReleasePageLocked(p);
                p.TurnInFlight = false;
                ReleaseStoppedPartitionLocked(p);
            }
        }
    }
    private void ApplyHydratedLocked(Partition p, IReadOnlyList<Guid> requested, IReadOnlyList<HubTaskCandidate> items, DateTimeOffset now)
    {
        var found = new Dictionary<Guid, HubTaskCandidate>(items.Count);
        foreach (var item in items)
        {
            if (item.HubId != p.Owner.HubId || item.PartitionId % p.Count != p.Owner.Partition || !requested.Contains(item.Id))
            {
                p.Dirty = true;
                HubTelemetry.CandidateHintsDropped.Add(1);
                continue;
            }
            found[item.Id] = item;
        }
        foreach (var id in requested)
        {
            found.TryGetValue(id, out var item);
            if (p.Entries.TryGetValue(id, out var old))
            {
                // Admission owns reservations through ambiguous outcomes; a hint
                // cannot rewrite or retire metadata currently being claimed.
                if (old.Reservation is not null || _reservedTasks.Contains((p.Owner.HubId, id))) continue;
                if (item is null)
                { RemoveEntryLocked(p, old); continue; }
                if (old.Candidate.Epoch > item.Epoch) continue;
                var deferred = old.Candidate.Epoch == item.Epoch ? old.DeferredUntil : default;
                RemoveEntryLocked(p, old);
                AddHydratedEntryLocked(p, item, deferred, now);
            }
            else if (item is not null && !_reservedTasks.Contains((p.Owner.HubId, id)))
                AddHydratedEntryLocked(p, item, default, now);
        }
    }
    private void AddHydratedEntryLocked(Partition p, HubTaskCandidate item, DateTimeOffset deferred, DateTimeOffset now)
    {
        var size = CandidateSize(item);
        if (p.InFlightEntries < 1 || size > p.InFlightBytes)
        { p.Dirty = true; HubTelemetry.CandidateHintsDropped.Add(1); return; }
        p.InFlightEntries--; p.InFlightBytes -= size;
        _entries--; _bytes -= size; p.Bytes -= size;
        var entry = new Entry(item, size, ++_order) { DeferredUntil = deferred };
        p.Entries[item.Id] = entry;
        var key = (item.TaskType, item.Version);
        if (!p.Capabilities.TryGetValue(key, out var queue)) p.Capabilities.Add(key, queue = new());
        queue.Add((item.Sequence, item.Id));
        p.Bytes += size; _bytes += size; _entries++;
        MatchLocked(p, now);
    }
    private void RequeueExactLocked(Partition p, IReadOnlyList<Guid> ids)
    {
        foreach (var id in ids)
        {
            if (p.ExactSet.Contains(id)) continue;
            if (p.ExactSet.Count >= Math.Min(_options.PartitionEntries, Math.Max(_options.PageSize, _options.PageSize * 8)) ||
                _pendingExactIds >= _options.ProcessEntries || GroupBytes(p.Owner.HubId,p.Owner.Partition) + 64 > _options.PartitionBytes || _bytes + 64 > _options.ProcessBytes)
            { p.Dirty = true; p.HeadRequested = true; HubTelemetry.CandidateHintsDropped.Add(1); continue; }
            p.ExactIds.Enqueue(id); p.ExactSet.Add(id); p.Bytes += 64; _bytes += 64; _pendingExactIds++;
        }
    }
    internal async Task RunAsync(TimeSpan turnInterval, Action<Exception> reportError, CancellationToken ct)
    {
        if (turnInterval <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(turnInterval));
        ArgumentNullException.ThrowIfNull(reportError);
        if (Interlocked.CompareExchange(ref _running,1,0) != 0) throw new InvalidOperationException("Candidate scheduler is already running");
        try
        {
            using var timer = new PeriodicTimer(turnInterval);
            while (await timer.WaitForNextTickAsync(ct))
            {
                try { await RunOneTurnAsync(_clock.GetUtcNow(), ct, reportError); }
                catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
                catch (Exception error) { reportError(error); }
            }
        }
        finally { Volatile.Write(ref _running,0); }
    }
    private void EndSweepLocked(Partition p, DateTimeOffset now)
    {
        p.Horizon = null; p.Streams.Clear(); p.Sweeps++; _completedSweeps++;
        p.NextSweep = now + _options.Recovery;
    }
    private bool BookPageLocked(Partition p)
    {
        var count = _options.PageSize; var bytes = _options.PageReservationBytes;
        // Evict only unreserved metadata; never evict a claim or uncertain credit.
        while (GroupEntries(p.Owner.HubId,p.Owner.Partition) + count > _options.PartitionEntries ||
               GroupBytes(p.Owner.HubId,p.Owner.Partition) + bytes > _options.PartitionBytes)
            if (!EvictOneLocked(p)) return false;
        while (_entries + count > _options.ProcessEntries || _bytes + bytes > _options.ProcessBytes)
            if (!EvictOneLocked(null)) return false;
        p.InFlightEntries += count; p.InFlightBytes += bytes; p.Bytes += bytes;
        _entries += count; _bytes += bytes; _inFlightPages++;
        return true;
    }
    private void ReleasePageLocked(Partition p)
    {
        _entries -= p.InFlightEntries; _bytes -= p.InFlightBytes; p.Bytes -= p.InFlightBytes;
        p.InFlightEntries = 0; p.InFlightBytes = 0; _inFlightPages--;
        ReleaseStoppedPartitionLocked(p);
    }
    private void ReleaseStoppedPartitionLocked(Partition p)
    {
        if (!p.Active && !p.BaseReleased && p.Entries.Count == 0 && p.Credits.Count == 0 && p.InFlightBytes == 0)
        { p.BaseReleased = true; p.Bytes -= PartitionOverhead; _bytes -= PartitionOverhead; _accountedPartitions.Remove(p); }
    }
    private bool EvictOneLocked(Partition? preferred)
    {
        IEnumerable<Partition> partitions = preferred is null ? _partitions.Values : [preferred];
        Partition? victimPartition = null; Entry? victim = null;
        foreach (var p in partitions)
            foreach (var e in p.Entries.Values)
                if (e.Reservation is null && (victim is null || e.Order < victim.Order)) { victim = e; victimPartition = p; }
        if (victim is null) return false;
        RemoveEntryLocked(victimPartition!,victim); return true;
    }
    private void MatchLocked(Partition p, DateTimeOffset now)
    {
        if (!p.Active) return;
        foreach (var credit in p.Credits)
        {
            if (credit.Reservation is not null || credit.Granted) continue;
            var entry = FindEligibleCandidateLocked(p, credit.Value, now);
            if (entry is not null) ReserveLocked(p, credit, entry);
        }
    }

    private Entry? FindEligibleCandidateLocked(Partition p, HubCandidateCredit credit, DateTimeOffset now)
    {
        var started = System.Diagnostics.Stopwatch.GetTimestamp();
        var checkedEntries = 0;
        var compatibilityChecks = 0;
        Entry? best = null;
        foreach (var capability in credit.Capabilities)
        {
            if (capability.HubId != p.Owner.HubId ||
                !p.Capabilities.TryGetValue((capability.TaskType, capability.Version), out var queue)) continue;
            foreach (var (sequence, taskId) in queue)
            {
                checkedEntries++;
                if (best is not null &&
                    (sequence, taskId).CompareTo((best.Candidate.Sequence, best.Candidate.Id)) >= 0) break;
                var entry = p.Entries[taskId];
                if (entry.Reservation is not null || entry.DeferredUntil > now ||
                    _reservedTasks.Contains((p.Owner.HubId, taskId))) continue;
                compatibilityChecks++;
                if (!CompatibleWorkflow(credit, entry.Candidate)) continue;
                best = entry;
                break;
            }
        }
        HubTelemetry.CandidateMatcherLockDuration.Record(System.Diagnostics.Stopwatch.GetElapsedTime(started).TotalSeconds);
        HubTelemetry.CandidateMatcherEntriesChecked.Record(checkedEntries);
        HubTelemetry.CandidateMatcherCompatibilityChecks.Record(compatibilityChecks);
        if (best is null) HubTelemetry.CandidateMatcherUnmatchedCredits.Record(1);
        return best;
    }

    private void ReserveLocked(Partition p, Credit credit, Entry entry)
    {
        var id = Guid.NewGuid();
        var value = new HubCandidateReservation(id, p.Owner, p.Count, credit.Value, entry.Candidate, p.Stop.Token);
        entry.Reservation = id;
        credit.Reservation = id;
        _reservedTasks.Add((p.Owner.HubId, entry.Candidate.Id));
        _reservations.Add(id, new Reservation(value, p, entry, credit));
        p.Pending.AddLast(id);
    }

    private static bool CompatibleWorkflow(HubCandidateCredit credit, HubTaskCandidate task)
    {
        if (task.TaskType is not ("__workflow__" or "__native_workflow__") || credit.Workflows is null) return true;
        // A missing/oversized workflow definition is advisory-unknown. Let the
        // fenced DB admission check decide so unavailable metadata cannot strand
        // otherwise runnable work indefinitely.
        if (!task.WorkflowMetadataComplete) return true;
        return task.DefinitionId is not null && task.DefinitionVersion.HasValue && credit.Workflows.Any(w =>
            w.HubId == task.HubId && w.DefinitionId == task.DefinitionId && w.Version == task.DefinitionVersion);
    }
    private long GroupBytes(string hub, int partition) => _accountedPartitions
        .Where(p => p.Owner.HubId == hub && p.Owner.Partition == partition).Sum(p => p.Bytes);
    private int GroupEntries(string hub, int partition) => _accountedPartitions
        .Where(p => p.Owner.HubId == hub && p.Owner.Partition == partition).Sum(p => p.Entries.Count + p.InFlightEntries);
    private int GroupCredits(string hub, int partition) => _accountedPartitions
        .Where(p => p.Owner.HubId == hub && p.Owner.Partition == partition).Sum(p => p.Credits.Count);
    private static long StringSize(string? value) => value is null ? 0 : 32L + 2L * value.Length;
    private static long CandidateSize(HubTaskCandidate c) => 512L + StringSize(c.HubId) + StringSize(c.TaskType) +
        StringSize(c.DefinitionId);
    private static long CreditSize(HubCandidateCredit c) => 512L + StringSize(c.Id) + StringSize(c.WorkerId) +
        c.Capabilities.Sum(x => 128L + StringSize(x.HubId) + StringSize(x.TaskType)) +
        (c.Workflows?.Sum(x => 128L + StringSize(x.HubId) + StringSize(x.DefinitionId)) ?? 0);
    private void RemoveEntryLocked(Partition p, Entry e)
    {
        if (!p.Entries.Remove(e.Candidate.Id)) return;
        var key = (e.Candidate.TaskType,e.Candidate.Version);
        var queue = p.Capabilities[key]; queue.Remove((e.Candidate.Sequence,e.Candidate.Id));
        if (queue.Count == 0) p.Capabilities.Remove(key);
        p.Bytes -= e.Size; _bytes -= e.Size; _entries--;
    }
    private void RemoveCreditLocked(Partition p, Credit c)
    { if (_credits.Remove(c.Value.Id)) { p.Credits.Remove(c); p.Bytes -= c.Bytes; _bytes -= c.Bytes; } }
    internal HubCandidateCoordinatorSnapshot Snapshot()
    {
        lock (_sync) return new(_partitions.Count,_entries,_bytes,_credits.Count,_reservations.Count,
            _reservations.Values.Count(x => x.Uncertain),_inFlightPages,_oversized,_lastOversized,_completedSweeps);
    }
    public void Dispose()
    {
        lock (_sync)
        {
            if (_disposed) return;
            _disposed = true;
            foreach (var p in _partitions.Values.ToArray()) StopLocked(p);
            _partitions.Clear();
        }
    }
}

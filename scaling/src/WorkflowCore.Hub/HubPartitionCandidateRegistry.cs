namespace WorkflowCore.Hub;

// Internal, opt-in building blocks. Nothing starts this registry from the gateway yet.
internal sealed record HubCandidateCoordinatorOptions(
    int PageSize = 128, int PageWireBytes = 65536,
    int PartitionEntries = 2048, int ProcessEntries = 32768,
    long PartitionBytes = 4 * 1024 * 1024, long ProcessBytes = 64 * 1024 * 1024,
    int PartitionCredits = 2048, int ProcessCredits = 16384, int ProcessPartitions = 1024,
    int SequenceProbeEverySweeps = 8, TimeSpan? RecoveryCadence = null,
    TimeSpan? ResourceCooldown = null)
{
    // Reserve both page objects and temporary materialization. UTF-8 wire bytes
    // alone undercount managed UTF-16 strings, array slots and object headers.
    internal long PageReservationBytes => (long)PageWireBytes * 4 + (long)PageSize * 1024;
    internal TimeSpan Recovery => RecoveryCadence ?? TimeSpan.FromSeconds(10);
    internal TimeSpan Cooldown => ResourceCooldown ?? TimeSpan.FromSeconds(1);
    internal void Validate()
    {
        if (PageSize is < 1 or > 1024 || PageWireBytes is < 256 or > 16 * 1024 * 1024 ||
            PartitionEntries < PageSize || ProcessEntries < PartitionEntries ||
            PartitionBytes < PageReservationBytes || ProcessBytes < PartitionBytes ||
            PartitionCredits < 1 || ProcessCredits < PartitionCredits || ProcessPartitions < 1 || SequenceProbeEverySweeps < 1 ||
            Recovery <= TimeSpan.Zero || Cooldown <= TimeSpan.Zero)
            throw new ArgumentException("Invalid candidate coordinator limits");
    }
}

internal sealed record HubCandidateCredit(string Id, string WorkerId,
    IReadOnlyList<HubCapability> Capabilities, IReadOnlyList<HubWorkflowCapability>? Workflows);
internal sealed record HubCandidateReservation(Guid Id, HubMatchingPartitionLease Owner,
    int PartitionCount, HubCandidateCredit Credit, HubTaskCandidate Candidate, CancellationToken OwnerStopped);
internal enum HubCandidateResolution { Leased, Unavailable, ResourceBlocked, AdmissionDeferred, OwnerLost, CommitOutcomeUnknown }
internal enum HubCandidateUncertainResolution { LeaseConfirmed, AuthoritativeNoCommit, RetireExpiredCredit }
internal sealed record HubCandidateOversizedObservation(string Hub, int Bucket, HubCandidateStream Stream, HubCandidateOversized Entry);
internal sealed record HubCandidateCoordinatorSnapshot(int Partitions, int Entries, long Bytes,
    int Credits, int Reservations, int Uncertain, int InFlightPages, long OversizedCount,
    HubCandidateOversizedObservation? LastOversized, long CompletedSweeps);

/// <summary>
/// One process-wide registry. All state transitions, including credit and ID
/// reservations, run under one lock; SQL page reads run through one scheduler.
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
        public readonly Dictionary<string, DateTimeOffset> Blocked = new(StringComparer.Ordinal);
        public readonly List<Stream> Streams = [];
        public HubCandidateHorizon? Horizon;
        public int NextStream;
        public bool Dirty = true;
        public bool HeadRequested;
        public int ForwardSinceHead;
        public int NextHeadBucket;
        public bool Active = true;
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
    private readonly SemaphoreSlim _scheduler = new(1, 1);
    private int _nextPartition;
    private int _entries;
    private long _bytes;
    private int _inFlightPages;
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
        foreach (var credit in p.Credits.ToArray())
        {
            if (credit.Reservation is not Guid id) { if (!credit.Granted) RemoveCreditLocked(p, credit); }
            else if (_reservations.TryGetValue(id, out var reservation) && !reservation.Dispatched)
                FinishLocked(reservation, false, false);
            // Dispatched claims may have committed. The candidate and credit
            // stay charged until the caller supplies a definite resolution.
        }
        p.Pending.Clear();
        foreach (var key in p.Blocked.Keys.ToArray()) RemoveBlockedLocked(p,key);
        ReleaseStoppedPartitionLocked(p);
    }

    internal bool AddCredit(string hub, int partition, long epoch, HubCandidateCredit credit)
    {
        ArgumentNullException.ThrowIfNull(credit);
        if (string.IsNullOrWhiteSpace(credit.Id) || credit.Id.Length > 256 ||
            string.IsNullOrWhiteSpace(credit.WorkerId) || credit.WorkerId.Length > 128 ||
            credit.Capabilities.Count is < 1 or > 128 || credit.Workflows?.Count > 128)
            throw new ArgumentException("Invalid candidate credit");
        var copy = credit with { Capabilities = credit.Capabilities.ToArray(), Workflows = credit.Workflows?.ToArray() };
        var bytes = CreditSize(copy);
        lock (_sync)
        {
            if (!GetLocked(hub, partition, epoch, out var p) || _credits.ContainsKey(credit.Id) ||
                GroupCredits(hub,partition) >= _options.PartitionCredits || _credits.Count >= _options.ProcessCredits ||
                GroupBytes(hub,partition) + bytes > _options.PartitionBytes || _bytes + bytes > _options.ProcessBytes) return false;
            var item = new Credit(copy, bytes); p.Credits.AddLast(item); _credits.Add(copy.Id, (p, item));
            p.Bytes += bytes; _bytes += bytes;
            MatchLocked(p, _clock.GetUtcNow());
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
    internal void Complete(Guid id, HubCandidateResolution result, DateTimeOffset now, string? blockedResource = null)
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
            if (result == HubCandidateResolution.ResourceBlocked)
            {
                var until = now + _options.Cooldown;
                r.Entry.DeferredUntil = until;
                if (!string.IsNullOrEmpty(blockedResource))
                {
                    AddBlockedLocked(r.Partition,blockedResource,until,now);
                    foreach (var e in r.Partition.Entries.Values)
                        if (e.Reservation is null && e.Candidate.ResourceKeysComplete && e.Candidate.ResourceKeys.Contains(blockedResource, StringComparer.Ordinal))
                            e.DeferredUntil = until;
                }
            }
            FinishLocked(r, result is HubCandidateResolution.ResourceBlocked or HubCandidateResolution.AdmissionDeferred,
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
    internal void Notify(string hub, int partition, long epoch, string? resource = null)
    {
        lock (_sync)
            if (GetLocked(hub, partition, epoch, out var p))
            {
                p.Dirty = true;
                p.HeadRequested = true;
                if (resource is not null)
                {
                    RemoveBlockedLocked(p, resource);
                    foreach (var e in p.Entries.Values)
                        if (!e.Candidate.ResourceKeysComplete || e.Candidate.ResourceKeys.Contains(resource, StringComparer.Ordinal)) e.DeferredUntil = default;
                }
                MatchLocked(p, _clock.GetUtcNow());
            }
    }
    private bool GetLocked(string hub, int partition, long epoch, out Partition p) =>
        _partitions.TryGetValue((hub, partition), out p!) && p.Active && p.Owner.Epoch == epoch;

    // One bounded scheduling turn and one discovery query at most. Notifications
    // set Dirty but never reset a live horizon or cursor. Empty buckets consume
    // turns too, making their fanout explicit and fair between partitions.
    internal async Task<bool> RunOneTurnAsync(DateTimeOffset now, CancellationToken ct)
    {
        if (!await _scheduler.WaitAsync(0, ct)) return false;
        Partition? p = null; Stream? stream = null; bool capture = false; bool booked = false; bool head = false;
        try
        {
            lock (_sync)
            {
                if (_disposed || _partitions.Count == 0) return false;
                var all = _partitions.Values.ToArray();
                for (var i = 0; i < all.Length; i++)
                {
                    var index = (_nextPartition + i) % all.Length; var candidate = all[index];
                    MatchLocked(candidate, now);
                    if (candidate.Horizon is null && !candidate.Dirty && now < candidate.NextSweep) continue;
                    p = candidate; _nextPartition = (index + 1) % all.Length; break;
                }
                if (p is null) return false;
                if (p.Horizon is null) { capture = true; p.Dirty = false; p.HeadRequested = false; p.ForwardSinceHead = 0; }
                else
                {
                    // One bounded head catch-up for every eight forward pages
                    // at most; it never moves or resets the forward cursors.
                    if (p.HeadRequested && p.ForwardSinceHead >= 8)
                    {
                        var buckets = 64 / p.Count;
                        var bucket = p.Owner.Partition + (p.NextHeadBucket++ % buckets) * p.Count;
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
            if (capture)
            {
                var horizon = await _reader.CaptureHorizonAsync(p.Owner.HubId, linked.Token);
                lock (_sync) if (p.Active)
                {
                    p.Horizon = horizon; p.Streams.Clear(); p.NextStream = 0;
                    var sequenceProbe = (p.Sweeps + 1) % _options.SequenceProbeEverySweeps == 0;
                    for (var b = p.Owner.Partition; b < 64; b += p.Count)
                    {
                        p.Streams.Add(new(b, HubCandidateStream.ReadyDue));
                        p.Streams.Add(new(b, HubCandidateStream.Expired));
                        if (sequenceProbe) p.Streams.Add(new(b, HubCandidateStream.Ready));
                    }
                }
                return true;
            }
            var page = await _reader.ReadPageAsync(p.Owner.HubId, stream!.Bucket, stream.Kind, p.Horizon!, stream.Cursor,
                _options.PageSize, _options.PageWireBytes, linked.Token);
            lock (_sync)
            {
                if (!p.Active) return true;
                foreach (var marker in page.Oversized ?? []) { _oversized++; _lastOversized = new(p.Owner.HubId,stream.Bucket,stream.Kind,marker); }
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
                if (!head)
                {
                    stream.Cursor = page.Next;
                    stream.Done = page.EndOfStream;
                    p.ForwardSinceHead++;
                    if (p.Streams.All(x => x.Done)) EndSweepLocked(p, now);
                }
            }
            return true;
        }
        catch (OperationCanceledException) when (p is not null && p.Stop.IsCancellationRequested && !ct.IsCancellationRequested) { return true; }
        catch
        {
            if (p is not null) lock (_sync) if (p.Active)
            { if (capture) p.Dirty = true; if (head) { p.HeadRequested = true; p.ForwardSinceHead = 8; } }
            throw;
        }
        finally
        {
            if (booked && p is not null) lock (_sync) ReleasePageLocked(p);
            _scheduler.Release();
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
                try { await RunOneTurnAsync(_clock.GetUtcNow(), ct); }
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
            if (!EvictOneLocked(p) && !EvictBlockedLocked(p)) return false;
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
        foreach (var key in p.Blocked.Where(x => x.Value <= now).Select(x => x.Key).ToArray()) RemoveBlockedLocked(p,key);
        foreach (var credit in p.Credits.ToArray())
        {
            if (credit.Reservation is not null || credit.Granted) continue;
            Entry? entry = null;
            foreach (var capability in credit.Value.Capabilities)
            {
                if (capability.HubId != p.Owner.HubId || !p.Capabilities.TryGetValue((capability.TaskType,capability.Version), out var queue)) continue;
                foreach (var (_, taskId) in queue)
                {
                    var e = p.Entries[taskId];
                    if (entry is not null && e.Candidate.Sequence >= entry.Candidate.Sequence) break;
                    if (e.Reservation is null && e.DeferredUntil <= now &&
                        !_reservedTasks.Contains((p.Owner.HubId,e.Candidate.Id)) &&
                        (!e.Candidate.ResourceKeysComplete || !e.Candidate.ResourceKeys.Any(p.Blocked.ContainsKey)) && Compatible(credit.Value,e.Candidate))
                    { entry = e; break; }
                }
            }
            if (entry is null) continue;
            var id = Guid.NewGuid();
            var value = new HubCandidateReservation(id,p.Owner,p.Count,credit.Value,entry.Candidate,p.Stop.Token);
            var reservation = new Reservation(value,p,entry,credit);
            entry.Reservation = id; credit.Reservation = id; _reservedTasks.Add((p.Owner.HubId,entry.Candidate.Id));
            _reservations.Add(id,reservation); p.Pending.AddLast(id);
        }
    }
    private void AddBlockedLocked(Partition p, string key, DateTimeOffset until, DateTimeOffset now)
    {
        if (key.Length > 4096) return;
        foreach (var expired in p.Blocked.Where(x => x.Value <= now).Select(x => x.Key).ToArray()) RemoveBlockedLocked(p,expired);
        if (p.Blocked.ContainsKey(key)) { p.Blocked[key] = until; return; }
        var size = 128 + StringSize(key);
        while (p.Blocked.Count >= _options.PartitionEntries) if (!EvictBlockedLocked(p)) return;
        if (GroupBytes(p.Owner.HubId,p.Owner.Partition) + size > _options.PartitionBytes || _bytes + size > _options.ProcessBytes) return;
        p.Blocked.Add(key,until); p.Bytes += size; _bytes += size;
    }
    private void RemoveBlockedLocked(Partition p, string key)
    {
        if (p.Blocked.Remove(key)) { var size = 128 + StringSize(key); p.Bytes -= size; _bytes -= size; }
    }
    private bool EvictBlockedLocked(Partition p)
    {
        if (p.Blocked.Count == 0) return false;
        var key = p.Blocked.MinBy(x => x.Value).Key; RemoveBlockedLocked(p,key); return true;
    }
    private static bool Compatible(HubCandidateCredit credit, HubTaskCandidate task)
    {
        if (!credit.Capabilities.Any(c => c.HubId == task.HubId && c.TaskType == task.TaskType && c.Version == task.Version)) return false;
        if (task.TaskType is not ("__workflow__" or "__native_workflow__") || credit.Workflows is null) return true;
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
        StringSize(c.DefinitionId) + c.ResourceKeys.Sum(k => 16L + StringSize(k));
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

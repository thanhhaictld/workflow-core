using System.Collections.Concurrent;
using System.Threading.Channels;
using WorkflowCore.Hub;

namespace WorkflowCore.Hub.Gateway;

internal sealed class HubCandidateCapacityException()
    : InvalidOperationException("Candidate demand capacity exceeded");

// Candidate mode is opt-in. One instance owns all generations and transports;
// workers never receive direct access to discovery or to PostgreSQL.
public sealed class HubCandidateAcquisition : BackgroundService
{
    private sealed class Demand(HubClaimRequest request)
    {
        public readonly HubClaimRequest Request = request;
        public readonly long CreatedAt = System.Diagnostics.Stopwatch.GetTimestamp();
        public readonly object Gate = new();
        public bool Admitting;
        public bool CancelRequested;
        public CancellationToken Cancellation;
        public readonly List<string> Credits = [];
        public readonly TaskCompletionSource<IReadOnlyList<HubLeasedTask>> Result = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
    private sealed record Retained(string Credit, Guid? Reservation, HubLeasedTask Lease, string Worker);
    private readonly HubTaskStore _tasks;
    private readonly HubMatchingOwnership _ownership;
    private readonly HubPartitionCandidateRegistry _registry;
    private readonly ILogger<HubCandidateAcquisition> _logger;
    private readonly ConcurrentDictionary<string,Demand> _demands = new(StringComparer.Ordinal);
    private readonly object _waitingGate = new();
    private readonly LinkedList<(Demand Demand, HubMatchingPartitionLease Owner)> _waiting = new();
    private readonly bool _backpressure = Environment.GetEnvironmentVariable("WFC_HUB_CANDIDATE_BACKPRESSURE") is "true" or "1" or "True";
    private readonly bool _crossWorkerBatch = Environment.GetEnvironmentVariable("WFC_HUB_CANDIDATE_CROSS_WORKER_BATCH") is "true" or "1" or "True";
    private readonly bool _concurrentAdmission = Environment.GetEnvironmentVariable("WFC_HUB_CANDIDATE_CONCURRENT_ADMISSION") is "true" or "1" or "True";
    private readonly SemaphoreSlim _admissionSlots;
    private readonly int _batchSize;
    private readonly object _inflightGate = new();
    private readonly HashSet<(string Hub, int Partition)> _inflightPartitions = [];
    private readonly HashSet<Task> _inflightTasks = [];
    private Task? _reconciliationTask;
    private int _nextAdmissionPartition;
    private readonly Func<HubMatchingPartitionLease, CancellationToken, Task>? _beforeAdmission;
    private readonly ConcurrentDictionary<(Guid Id,long Epoch),Retained> _retained = new();
    private readonly Channel<bool> _wake = Channel.CreateBounded<bool>(new BoundedChannelOptions(1)
        { FullMode = BoundedChannelFullMode.DropWrite, SingleReader = true, SingleWriter = false });
    public bool Enabled => _ownership.CandidateTopology;

    public HubCandidateAcquisition(HubTaskStore tasks, HubMatchingOwnership ownership,
        HubReadinessBroadcaster readiness, ILogger<HubCandidateAcquisition> logger)
        : this(tasks, ownership, readiness, logger, null, null) { }

    internal HubCandidateAcquisition(HubTaskStore tasks, HubMatchingOwnership ownership,
        HubReadinessBroadcaster readiness, ILogger<HubCandidateAcquisition> logger,
        HubCandidateCoordinatorOptions? options, bool? backpressure,
        Func<HubMatchingPartitionLease, CancellationToken, Task>? beforeAdmission = null)
    {
        _beforeAdmission = beforeAdmission;
        _backpressure = backpressure ?? _backpressure;
        var configuredConcurrency = Environment.GetEnvironmentVariable("WFC_HUB_CANDIDATE_ADMISSION_CONCURRENCY");
        var concurrencySetting = "WFC_HUB_CANDIDATE_ADMISSION_CONCURRENCY";
        if (configuredConcurrency is not null && (!int.TryParse(configuredConcurrency, out var parsed) || parsed is < 1 or > 32))
            throw new ArgumentException($"{concurrencySetting} must be 1..32");
        _admissionSlots = new(configuredConcurrency is null ? 2 : int.Parse(configuredConcurrency));
        var configuredBatch = Environment.GetEnvironmentVariable("WFC_HUB_CANDIDATE_BATCH_SIZE");
        if (configuredBatch is not null && (!int.TryParse(configuredBatch, out var batch) || batch is < 1 or > 32))
            throw new ArgumentException("WFC_HUB_CANDIDATE_BATCH_SIZE must be 1..32");
        _batchSize = configuredBatch is null ? 32 : int.Parse(configuredBatch);
        _tasks=tasks; _ownership=ownership; _logger=logger;
        _registry=new(new HubCandidateReader(tasks), (options ?? new()) with { RequireDemand = _backpressure });
        if (Enabled)
        {
            ownership.PartitionAcquired += Acquired;
            ownership.PartitionLost += Lost;
            readiness.CandidateRescanRequested += Hint;
            readiness.CandidateTaskChanged += TaskChanged;
            readiness.CandidateRecoveryRequired += RecoveryRequired;
        }
    }
    private void Acquired(HubMatchingPartitionLease owner)
    {
        _registry.StartGeneration(owner,_ownership.PartitionCount);
        _wake.Writer.TryWrite(true);
    }
    private void Lost(HubMatchingPartitionLease owner)
    {
        _registry.StopGeneration(owner.HubId,owner.Partition,owner.Epoch);
        _wake.Writer.TryWrite(true);
    }
    private void Hint(string hub,string? type,int version,int? taskPartition)
    {
        foreach(var owner in _registry.ActiveOwners())
            if(owner.HubId==hub && (taskPartition is null || taskPartition.Value % _ownership.PartitionCount == owner.Partition))
                _registry.Notify(hub,owner.Partition,owner.Epoch,bucket: !_backpressure || taskPartition is null ? null : taskPartition.Value % 64);
        _wake.Writer.TryWrite(true);
    }
    private void TaskChanged(string hub, Guid taskId, int taskPartition, string? type, int version)
    {
        foreach (var owner in _registry.ActiveOwners())
            if (owner.HubId == hub && taskPartition % _ownership.PartitionCount == owner.Partition)
                _registry.NotifyTask(hub, owner.Partition, owner.Epoch, taskId);
        _wake.Writer.TryWrite(true);
    }
    private void RecoveryRequired()
    {
        foreach (var owner in _registry.ActiveOwners())
            _registry.Notify(owner.HubId, owner.Partition, owner.Epoch);
        _wake.Writer.TryWrite(true);
    }
    internal int GenerationCount => _registry.Snapshot().Partitions;
    public int CandidateCredits => _registry.Snapshot().Credits;
    public int CandidateReservations => _registry.Snapshot().Reservations;
    public int CandidateUncertain => _registry.Snapshot().Uncertain;
    public int CandidateBuffered => _registry.Snapshot().Entries;
    public int AdmissionActive { get { lock (_inflightGate) return _inflightTasks.Count; } }
    internal (int Waiting, int Credits) DemandSnapshot()
    {
        lock (_waitingGate) return (_waiting.Count, _registry.Snapshot().Credits);
    }
    internal string[] AllocatedDemandWorkers() => _demands.Values.Select(x => x.Request.WorkerId).Distinct().ToArray();

    public async Task<IReadOnlyList<HubLeasedTask>> AcquireAsync(HubClaimRequest request, CancellationToken ct)
    {
        if (!Enabled || request.Hubs.Length!=1 || request.MatchingPartition is null || request.MatchingPartitionCount!=_ownership.PartitionCount)
            throw new InvalidOperationException("Candidate matching is unavailable for this topology");
        var hub=request.Hubs[0]; var partition=request.MatchingPartition.Value;
        var owner=_registry.ActiveOwners().FirstOrDefault(x=>x.HubId==hub && x.Partition==partition);
        if(owner is null) throw new InvalidOperationException("Matching generation is unavailable");
        var demand=new Demand(request);
        LinkedListNode<(Demand Demand, HubMatchingPartitionLease Owner)>? waiting = null;
        try
        {
            if (_backpressure)
            {
                lock (_waitingGate)
                {
                    // Push registrations are bounded independently to 8192 sessions.
                    if (_waiting.Count >= 8192) throw new InvalidOperationException("Candidate waiting demand capacity exceeded");
                    if (!_registry.AddWaitingDemand(hub, partition, owner.Epoch))
                        throw new InvalidOperationException("Matching owner generation lost");
                    waiting = _waiting.AddLast((demand, owner));
                    HubTelemetry.CandidateWaitingSessions.Add(1);
                }
            }
            else
            {
                AllocateCredits(demand, owner);
                if(demand.Credits.Count==0)
                {
                    HubTelemetry.CandidateCapacityFallbacks.Add(1);
                    throw new HubCandidateCapacityException();
                }
            }
            _wake.Writer.TryWrite(true);
            using var cancellation=ct.Register(() =>
            {
                lock(demand.Gate)
                {
                    demand.CancelRequested=true; demand.Cancellation=ct;
                    if(!demand.Admitting) demand.Result.TrySetCanceled(ct);
                }
            });
            return await demand.Result.Task;
        }
        finally
        {
            if (waiting is not null) lock (_waitingGate) if (waiting.List is not null)
            {
                _waiting.Remove(waiting);
                _registry.RemoveWaitingDemand(hub, partition, owner.Epoch);
                HubTelemetry.CandidateWaitingSessions.Add(-1);
            }
            foreach(var credit in demand.Credits)
            {
                _registry.CancelCredit(credit);
                _demands.TryRemove(credit,out _);
            }
            _wake.Writer.TryWrite(true);
        }
    }

    private void AllocateCredits(Demand demand, HubMatchingPartitionLease owner)
    {
        var request = demand.Request;
        for(var i=0;i<request.MaxTasks;i++)
        {
            var id=Guid.NewGuid().ToString("N");
            var credit=new HubCandidateCredit(id,request.WorkerId,request.Capabilities,request.Workflows);
            _demands[id]=demand;
            var added = _backpressure
                ? _registry.AddMatchedCredit(owner.HubId,owner.Partition,owner.Epoch,credit)
                : _registry.AddCredit(owner.HubId,owner.Partition,owner.Epoch,credit);
            if(!added) { _demands.TryRemove(id,out _); break; }
            demand.Credits.Add(id);
        }
    }

    private void AllocateWaiting()
    {
        lock (_waitingGate)
        {
            var owners = _registry.ActiveOwners().Select(x => (x.HubId, x.Partition, x.Epoch)).ToHashSet();
            // Bound each pass; rotating untouched demand ensures large idle
            // fleets do not monopolize the discovery/admission scheduler.
            var remaining = Math.Min(_waiting.Count, 128);
            while (remaining-- > 0 && _waiting.First is { } node)
            {
                _waiting.RemoveFirst();
                var (demand, owner) = node.Value;
                lock (demand.Gate)
                {
                    if (demand.Result.Task.IsCompleted || demand.CancelRequested)
                    {
                        _registry.RemoveWaitingDemand(owner.HubId, owner.Partition, owner.Epoch);
                        HubTelemetry.CandidateWaitingSessions.Add(-1); continue;
                    }
                    if (!owners.Contains((owner.HubId, owner.Partition, owner.Epoch)))
                    {
                        demand.Result.TrySetException(new InvalidOperationException("Matching owner generation lost"));
                        HubTelemetry.CandidateWaitingSessions.Add(-1);
                        continue;
                    }
                    if (!_registry.HasCandidateCapacity(owner.HubId, owner.Partition, owner.Epoch))
                    { _waiting.AddLast(node); continue; }
                    AllocateCredits(demand, owner);
                    if (demand.Credits.Count == 0) _waiting.AddLast(node);
                    else
                    {
                        _registry.RemoveWaitingDemand(owner.HubId, owner.Partition, owner.Epoch);
                        HubTelemetry.CandidateWaitingSessions.Add(-1);
                        HubTelemetry.CandidateCapacityWait.Record(System.Diagnostics.Stopwatch.GetElapsedTime(demand.CreatedAt).TotalSeconds);
                    }
                }
            }
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if(!Enabled) return;
        using var timer=new PeriodicTimer(TimeSpan.FromMilliseconds(50));
        var timerTick=timer.WaitForNextTickAsync(stoppingToken).AsTask();
        var wake=_wake.Reader.ReadAsync(stoppingToken).AsTask();
        var reaper=0;
        try
        {
            while(true)
            {
                var signaled=await Task.WhenAny(timerTick,wake);
                if(signaled==timerTick)
                {
                    if(!await timerTick) break;
                    timerTick=timer.WaitForNextTickAsync(stoppingToken).AsTask();
                }
                else
                {
                    try { await wake; } catch(ChannelClosedException) { break; }
                    while(_wake.Reader.TryRead(out _)) { }
                    wake=_wake.Reader.ReadAsync(stoppingToken).AsTask();
                }
                try
                {
                    if (_backpressure) AllocateWaiting();
                    // Keep each activation bounded; if it uses the whole budget,
                    // queue one coalesced continuation so long sweeps do not wait
                    // for a timer tick and partition rotation remains fair.
                    var turns=0;
                    for(;turns<16;turns++)
                    {
                        var progressed=await _registry.RunOneTurnAsync(DateTimeOffset.UtcNow,stoppingToken);
                        if (_backpressure && progressed) AllocateWaiting();
                        await DrainAsync(stoppingToken);
                        var snapshot=_registry.Snapshot();
                        HubTelemetry.PublishCandidateSnapshot(snapshot.Credits,snapshot.Reservations,
                            snapshot.Entries,snapshot.Bytes);
                        if(++reaper%20==0)
                        {
                            // Lease reconciliation uses its own bounded DB batches and must
                            // not hold discovery or serialized admission while querying.
                            if (_reconciliationTask is null || _reconciliationTask.IsCompleted)
                                _reconciliationTask = ReapWithLoggingAsync(stoppingToken);
                        }
                        if(!progressed) break;
                    }
                    if(turns==16) _wake.Writer.TryWrite(true);
                }
                catch(OperationCanceledException) when(stoppingToken.IsCancellationRequested) { break; }
                catch(Exception ex)
                {
                    _logger.LogWarning(ex,"Candidate acquisition loop failed");
                    foreach(var demand in _demands.Values.Distinct()) demand.Result.TrySetException(ex);
                }
            }
        }
        catch(OperationCanceledException) when(stoppingToken.IsCancellationRequested) { }
        finally
        {
            Task[] inflight;
            lock (_inflightGate) inflight = _inflightTasks.ToArray();
            try { await Task.WhenAll(inflight); } catch (Exception ex) { _logger.LogWarning(ex, "Candidate admissions stopped with an error"); }
            if (_reconciliationTask is not null) await _reconciliationTask;
            lock (_waitingGate)
                foreach (var item in _waiting) item.Demand.Result.TrySetCanceled(stoppingToken);
            foreach(var demand in _demands.Values.Distinct()) demand.Result.TrySetCanceled(stoppingToken);
            _registry.Dispose();
        }
    }

    // Completion/failure acknowledgement is authoritative lease retirement.
    // It can safely release candidate memory before the original lease deadline.
    internal void ReleaseCompletedLease(Guid id,long epoch)
    {
        if(!_retained.TryRemove((id,epoch),out var item)) return;
        if(item.Reservation is Guid reservation)
            _registry.ResolveUncertain(reservation,HubCandidateUncertainResolution.LeaseConfirmed,DateTimeOffset.UtcNow);
        _registry.ReleaseGrantedCredit(item.Credit);
        _wake.Writer.TryWrite(true);
    }
    private async Task DrainAsync(CancellationToken ct)
    {
        var owners = _registry.ActiveOwners();
        var start = _nextAdmissionPartition;
        for (var i = 0; i < owners.Count; i++)
        {
            var index = (start + i) % owners.Count;
            var owner = owners[index];
            if (_concurrentAdmission)
            {
                if (!_registry.HasPending(owner.HubId, owner.Partition, owner.Epoch)) continue;
                lock (_inflightGate)
                {
                    var key = (owner.HubId, owner.Partition);
                    if (_inflightPartitions.Contains(key) || !_admissionSlots.Wait(0)) continue;
                    _inflightPartitions.Add(key);
                    Task? task = null;
                    task = Task.Run(async () =>
                    {
                        try { await DrainOwnerAsync(owner, ct); }
                        catch (Exception ex) { _logger.LogWarning(ex, "Candidate admission failed"); }
                        finally
                        {
                            lock (_inflightGate) { _inflightPartitions.Remove(key); _inflightTasks.Remove(task!); }
                            _admissionSlots.Release(); _wake.Writer.TryWrite(true);
                        }
                    }, CancellationToken.None);
                    _inflightTasks.Add(task);
                    _nextAdmissionPartition = (index + 1) % owners.Count;
                }
            }
            else await DrainOwnerAsync(owner, ct);
        }
    }

    private async Task DrainOwnerAsync(HubMatchingPartitionLease owner, CancellationToken ct)
    {
            if (_beforeAdmission is not null) await _beforeAdmission(owner, ct);
            var picked=new List<HubCandidateReservation>();
            for(var i=0;i<_batchSize;i++)
            { var r=_registry.TakeReservation(owner.HubId,owner.Partition,owner.Epoch); if(r is null) break; picked.Add(r); }
            if (_crossWorkerBatch)
            {
                var mixed = picked.Where(r => r.Candidate.ResourceKeysComplete && r.Candidate.ResourceKeys.Length == 0).ToList();
                if (mixed.Count > 1)
                {
                    await AdmitMixedAsync(mixed, ct);
                    picked.RemoveAll(mixed.Contains);
                }
            }
            foreach(var group in picked.GroupBy(x=>_demands.TryGetValue(x.Credit.Id,out var d)?d:null))
            {
                var reservations=group.ToList(); var demand=group.Key;
                if(demand is null || demand.Result.Task.IsCompleted)
                { foreach(var r in reservations) _registry.AbandonBeforeAdmission(r.Id); continue; }
                // Only metadata with a complete empty resource set may take the
                // atomic batch path. Other tasks use a single fenced claim.
                var first=reservations[0]; var unconstrained=first.Candidate.ResourceKeysComplete && first.Candidate.ResourceKeys.Length==0;
                var batch=unconstrained ? reservations.Where(r=>r.Candidate.ResourceKeysComplete && r.Candidate.ResourceKeys.Length==0).ToList() : [first];
                foreach(var extra in reservations.Except(batch)) { _registry.AbandonBeforeAdmission(extra.Id); _demands.TryRemove(extra.Credit.Id,out _); }
                await AdmitAsync(demand,batch,ct);
            }
    }

    private async Task AdmitMixedAsync(List<HubCandidateReservation> reservations, CancellationToken ct)
    {
        var groups = reservations.GroupBy(x => _demands.TryGetValue(x.Credit.Id, out var d) ? d : null).ToArray();
        var active = new List<(Demand Demand, List<HubCandidateReservation> Reservations)>();
        foreach (var group in groups)
        {
            var items = group.ToList(); var demand = group.Key;
            if (demand is null) { foreach (var r in items) _registry.AbandonBeforeAdmission(r.Id); continue; }
            lock (demand.Gate)
            {
                if (demand.Result.Task.IsCompleted || demand.CancelRequested)
                { foreach (var r in items) _registry.AbandonBeforeAdmission(r.Id); continue; }
                demand.Admitting = true;
                active.Add((demand, items));
            }
        }
        if (active.Count == 0) return;
        var first = active[0].Reservations[0]; var firstRequest = active[0].Demand.Request;
        HubCandidateBatchResult result;
        var started = System.Diagnostics.Stopwatch.GetTimestamp();
        try
        {
            var items = active.SelectMany(x => x.Reservations.Select(r => new HubCandidateBatchItem(
                r.Candidate.Id, r.Candidate.Epoch, x.Demand.Request.WorkerId,
                x.Demand.Request.Capabilities, x.Demand.Request.Workflows))).ToArray();
            HubTelemetry.CandidateBatchFill.Record(items.Length);
            foreach (var (demand, _) in active)
                HubTelemetry.CandidateDemandToAdmissionDuration.Record(
                    System.Diagnostics.Stopwatch.GetElapsedTime(demand.CreatedAt).TotalSeconds);
            result = await _tasks.ClaimCandidateBatchAsync(new(items, firstRequest.WorkerId, first.Owner.HubId,
                first.Owner.Partition, first.PartitionCount, firstRequest.Capabilities, firstRequest.Workflows,
                first.Owner, TimeSpan.FromSeconds(45)), ct);
        }
        catch (Exception error)
        {
            foreach (var (demand, items) in active)
            {
                foreach (var r in items) _registry.Complete(r.Id, HubCandidateResolution.Unavailable, DateTimeOffset.UtcNow);
                lock (demand.Gate) { demand.Admitting = false; demand.Result.TrySetException(error); }
            }
            return;
        }
        finally { HubTelemetry.CandidateAdmissionLatency.Record(System.Diagnostics.Stopwatch.GetElapsedTime(started).TotalSeconds); }
        var leases = result.Leases.ToDictionary(x => x.Id);
        if (result.Status == HubCandidateClaimStatus.Leased) HubTelemetry.CandidateLeased.Add(result.Leases.Count);
        var completed = new List<(Demand Demand, List<HubLeasedTask> Leases)>();
        foreach (var (demand, items) in active)
        {
            var delivered = new List<HubLeasedTask>();
            foreach (var r in items)
            {
                var hasLease = leases.TryGetValue(r.Candidate.Id, out var lease);
                var outcome = result.Status switch {
                    HubCandidateClaimStatus.Leased when hasLease => HubCandidateResolution.Leased,
                    HubCandidateClaimStatus.CommitOutcomeUnknown when hasLease => HubCandidateResolution.CommitOutcomeUnknown,
                    HubCandidateClaimStatus.AdmissionDeferred => HubCandidateResolution.AdmissionDeferred,
                    HubCandidateClaimStatus.OwnerLost => HubCandidateResolution.OwnerLost,
                    _ => HubCandidateResolution.Unavailable };
                _registry.Complete(r.Id, outcome, DateTimeOffset.UtcNow);
                if (hasLease && outcome is HubCandidateResolution.Leased or HubCandidateResolution.CommitOutcomeUnknown)
                {
                    _retained[(lease!.Id, lease.Epoch)] = new(r.Credit.Id,
                        outcome == HubCandidateResolution.CommitOutcomeUnknown ? r.Id : null, lease, demand.Request.WorkerId);
                    delivered.Add(lease);
                }
            }
            completed.Add((demand, delivered));
        }
        // Retain every committed or potentially committed lease before any
        // demand can resume and deliver its own assignments.
        foreach (var (demand, delivered) in completed)
        {
            lock (demand.Gate)
            {
                demand.Admitting = false;
                if (result.Status == HubCandidateClaimStatus.OwnerLost)
                    demand.Result.TrySetException(new InvalidOperationException("Matching owner generation lost"));
                else if (result.Status == HubCandidateClaimStatus.CommitOutcomeUnknown && delivered.Count != 0)
                    demand.Result.TrySetException(new InvalidOperationException("Candidate commit outcome is uncertain"));
                else if (delivered.Count != 0) demand.Result.TrySetResult(delivered);
                else if (demand.CancelRequested) demand.Result.TrySetCanceled(demand.Cancellation);
            }
        }
    }
    private async Task AdmitAsync(Demand demand,List<HubCandidateReservation> reservations,CancellationToken ct)
    {
        lock(demand.Gate)
        {
            if(demand.Result.Task.IsCompleted)
            { foreach(var r in reservations) _registry.AbandonBeforeAdmission(r.Id); return; }
            demand.Admitting=true;
        }
        HubTelemetry.CandidateBatchFill.Record(reservations.Count);
        HubTelemetry.CandidateDemandToAdmissionDuration.Record(
            System.Diagnostics.Stopwatch.GetElapsedTime(demand.CreatedAt).TotalSeconds);
        var first=reservations[0]; var request=demand.Request;
        HubCandidateClaimStatus status; IReadOnlyList<HubLeasedTask> leases; string? blocked=null;
        try
        {
            if(reservations.Count>1)
            {
                var batch=new HubCandidateBatchClaim(reservations.Select(r=>new HubCandidateBatchItem(r.Candidate.Id,r.Candidate.Epoch)).ToArray(),
                    request.WorkerId,first.Owner.HubId,first.Owner.Partition,first.PartitionCount,request.Capabilities,request.Workflows,first.Owner,TimeSpan.FromSeconds(45));
                var result=await _tasks.ClaimCandidateBatchAsync(batch,ct); status=result.Status; leases=result.Leases;
            }
            else
            {
                var claim=new HubCandidateClaim(first.Candidate.Id,first.Candidate.Epoch,request.WorkerId,first.Owner.HubId,
                    first.Owner.Partition,first.PartitionCount,request.Capabilities,request.Workflows,first.Owner,TimeSpan.FromSeconds(45));
                var result=await _tasks.ClaimCandidateAsync(claim,ct); status=result.Status; leases=result.Lease is null ? [] : [result.Lease]; blocked=result.ResourceKey;
            }
        }
        catch(Exception error)
        {
            foreach(var r in reservations) _registry.Complete(r.Id,HubCandidateResolution.Unavailable,DateTimeOffset.UtcNow);
            lock(demand.Gate) { demand.Admitting=false; demand.Result.TrySetException(error); }
            return;
        }
        var byId=leases.ToDictionary(x=>x.Id);
        if (status == HubCandidateClaimStatus.Leased) HubTelemetry.CandidateLeased.Add(leases.Count);
        foreach(var r in reservations)
        {
            var hasLease=byId.TryGetValue(r.Candidate.Id,out var lease);
            var outcome=status switch {
                HubCandidateClaimStatus.Leased when hasLease => HubCandidateResolution.Leased,
                HubCandidateClaimStatus.CommitOutcomeUnknown when hasLease => HubCandidateResolution.CommitOutcomeUnknown,
                HubCandidateClaimStatus.ResourceBlocked => HubCandidateResolution.ResourceBlocked,
                HubCandidateClaimStatus.AdmissionDeferred => HubCandidateResolution.AdmissionDeferred,
                HubCandidateClaimStatus.OwnerLost => HubCandidateResolution.OwnerLost,
                _ => HubCandidateResolution.Unavailable };
            _registry.Complete(r.Id,outcome,DateTimeOffset.UtcNow,blocked);
            if(hasLease && outcome is HubCandidateResolution.Leased or HubCandidateResolution.CommitOutcomeUnknown)
                _retained[(lease!.Id,lease.Epoch)]=new(r.Credit.Id,outcome==HubCandidateResolution.CommitOutcomeUnknown?r.Id:null,lease,request.WorkerId);
        }
        lock(demand.Gate)
        {
            demand.Admitting=false;
            if(status==HubCandidateClaimStatus.OwnerLost)
                demand.Result.TrySetException(new InvalidOperationException("Matching owner generation lost"));
            else if(status==HubCandidateClaimStatus.CommitOutcomeUnknown)
                demand.Result.TrySetException(new InvalidOperationException("Candidate commit outcome is uncertain"));
            else if(status==HubCandidateClaimStatus.Leased)
                demand.Result.TrySetResult(leases);
            else if(demand.CancelRequested)
                demand.Result.TrySetCanceled(demand.Cancellation);
        }
    }
    private async Task ReapAsync(CancellationToken ct)
    {
        var expired = _retained.ToArray().Where(x => DateTime.UtcNow >= x.Value.Lease.LeaseUntil).ToArray();
        foreach(var chunk in expired.Chunk(256))
        {
            var retired = await _tasks.RetirableCandidateLeasesAsync(chunk.Select(x => (x.Value.Lease,x.Value.Worker)).ToArray(), ct);
            foreach(var pair in chunk)
            {
                if (!retired.Contains(pair.Key) || !_retained.TryRemove(pair.Key,out var item)) continue;
                if(item.Reservation is Guid reservation)
                    _registry.ResolveUncertain(reservation,HubCandidateUncertainResolution.RetireExpiredCredit,DateTimeOffset.UtcNow);
                _registry.ReleaseGrantedCredit(item.Credit);
            }
        }
    }

    private async Task ReapWithLoggingAsync(CancellationToken ct)
    {
        try { await ReapAsync(ct); }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception ex) { _logger.LogWarning(ex, "Candidate lease reconciliation failed"); }
    }
}

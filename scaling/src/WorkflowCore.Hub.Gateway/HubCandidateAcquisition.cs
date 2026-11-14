using System.Collections.Concurrent;
using WorkflowCore.Hub;

namespace WorkflowCore.Hub.Gateway;

/// <summary>Transient in-memory demand saturation. No task lease or attempt was created.</summary>
public sealed class HubCandidateBackpressureException : Exception
{
    public HubCandidateBackpressureException() : base("Candidate demand capacity exceeded") { }
}

// Primary Hub matching coordinator (rollback remains configuration-gated).
// Workers never receive direct access to discovery or to PostgreSQL.
public sealed class HubCandidateAcquisition : BackgroundService
{
    private sealed class Demand(HubClaimRequest request)
    {
        public readonly HubClaimRequest Request = request;
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
    // Only ambiguous commits remain quarantined. Successful matching credits
    // are single-use tickets; worker-slot ownership moves to the transport.
    private readonly ConcurrentDictionary<(Guid Id,long Epoch),Retained> _retained = new();
    private readonly ConcurrentDictionary<long,Task> _activeAdmissions = new();
    private readonly SemaphoreSlim _admissionSlots;
    private long _nextAdmission;
    public bool Enabled => _ownership.CandidateTopology;

    public HubCandidateAcquisition(HubTaskStore tasks, HubMatchingOwnership ownership,
        HubReadinessBroadcaster readiness, ILogger<HubCandidateAcquisition> logger)
    {
        _tasks=tasks; _ownership=ownership; _logger=logger;
        _registry=new(new HubCandidateReader(tasks));
        var configured=Environment.GetEnvironmentVariable("WFC_HUB_MATCHING_ADMISSION_CONCURRENCY");
        var parallel=string.IsNullOrWhiteSpace(configured) ? 3 :
            int.TryParse(configured,out var count) && count is >= 1 and <= 32 ? count :
            throw new ArgumentException("WFC_HUB_MATCHING_ADMISSION_CONCURRENCY must be 1..32");
        _admissionSlots=new SemaphoreSlim(parallel,parallel);
        if (Enabled)
        {
            ownership.PartitionAcquired += Acquired;
            ownership.PartitionLost += Lost;
            readiness.TaskReady += Hint;
        }
    }
    private void Acquired(HubMatchingPartitionLease owner) => _registry.StartGeneration(owner,_ownership.PartitionCount);
    private void Lost(HubMatchingPartitionLease owner) => _registry.StopGeneration(owner.HubId,owner.Partition,owner.Epoch);
    private void Hint(string hub,string? type,int version,int? taskPartition)
    {
        foreach(var owner in _registry.ActiveOwners())
            if(owner.HubId==hub && (taskPartition is null || taskPartition.Value % _ownership.PartitionCount == owner.Partition))
                _registry.Notify(hub,owner.Partition,owner.Epoch);
    }
    internal int GenerationCount => _registry.Snapshot().Partitions;
    public int CandidateCredits => _registry.Snapshot().Credits;
    public int CandidateReservations => _registry.Snapshot().Reservations;
    public int CandidateUncertain => _registry.Snapshot().Uncertain;
    public int CandidateBuffered => _registry.Snapshot().Entries;
    public int AdmissionActive => _activeAdmissions.Count;

    public async Task<IReadOnlyList<HubLeasedTask>> AcquireAsync(HubClaimRequest request, CancellationToken ct)
    {
        if (!Enabled || request.Hubs.Length!=1 || request.MatchingPartition is null || request.MatchingPartitionCount!=_ownership.PartitionCount)
            throw new InvalidOperationException("Candidate matching is unavailable for this topology");
        var hub=request.Hubs[0]; var partition=request.MatchingPartition.Value;
        var owner=_registry.ActiveOwners().FirstOrDefault(x=>x.HubId==hub && x.Partition==partition);
        if(owner is null) throw new InvalidOperationException("Matching generation is unavailable");
        var demand=new Demand(request);
        try
        {
            for(var i=0;i<request.MaxTasks;i++)
            {
                var id=Guid.NewGuid().ToString("N");
                var credit=new HubCandidateCredit(id,request.WorkerId,request.Capabilities,request.Workflows);
                _demands[id]=demand;
                if(!_registry.AddCredit(hub,partition,owner.Epoch,credit)) { _demands.TryRemove(id,out _); break; }
                demand.Credits.Add(id);
            }
            if(demand.Credits.Count==0) throw new HubCandidateBackpressureException();
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
            foreach(var credit in demand.Credits)
            {
                _registry.CancelCredit(credit);
                _demands.TryRemove(credit,out _);
            }
        }
    }

    // Discovery, admission and cleanup have independent loops. A slow resource
    // lock must not freeze other partitions or lease-retirement bookkeeping.
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if(!Enabled) return;
        try
        {
            await Task.WhenAll(DiscoverAsync(stoppingToken),DispatchAsync(stoppingToken),
                ReapLoopAsync(stoppingToken));
        }
        finally
        {
            foreach(var demand in _demands.Values.Distinct()) demand.Result.TrySetCanceled(stoppingToken);
            await Task.WhenAll(_activeAdmissions.Values);
            _registry.Dispose();
        }
    }

    private async Task DiscoverAsync(CancellationToken ct)
    {
        using var timer=new PeriodicTimer(TimeSpan.FromMilliseconds(50));
        while(!ct.IsCancellationRequested)
        {
            try
            {
                if(!await timer.WaitForNextTickAsync(ct)) break;
                await _registry.RunOneTurnAsync(DateTimeOffset.UtcNow,ct);
            }
            catch(OperationCanceledException) when(ct.IsCancellationRequested) { break; }
            catch(Exception error) { _logger.LogWarning(error,"Candidate discovery turn failed"); }
        }
    }

    private async Task DispatchAsync(CancellationToken ct)
    {
        using var timer=new PeriodicTimer(TimeSpan.FromMilliseconds(20));
        while(!ct.IsCancellationRequested)
        {
            try
            {
                if(!await timer.WaitForNextTickAsync(ct)) break;
                DispatchReady(ct);
            }
            catch(OperationCanceledException) when(ct.IsCancellationRequested) { break; }
            catch(Exception error) { _logger.LogWarning(error,"Candidate dispatch turn failed"); }
        }
    }

    private async Task ReapLoopAsync(CancellationToken ct)
    {
        using var timer=new PeriodicTimer(TimeSpan.FromSeconds(1));
        while(!ct.IsCancellationRequested)
        {
            try
            {
                if(!await timer.WaitForNextTickAsync(ct)) break;
                await ReapAsync(ct);
            }
            catch(OperationCanceledException) when(ct.IsCancellationRequested) { break; }
            catch(Exception error) { _logger.LogWarning(error,"Candidate uncertainty reaper failed"); }
        }
    }

    private void DispatchReady(CancellationToken ct)
    {
        // Round-robin passes across owned partitions; only reserve DB admission
        // capacity when an actual reservation can be dispatched. The semaphore
        // bounds in-flight transactions independently of active SSE sessions.
        var owners=_registry.ActiveOwners();
        bool progressed;
        do
        {
            progressed=false;
            foreach(var owner in owners)
            {
                if(!_admissionSlots.Wait(0)) return;
                var picked=new List<HubCandidateReservation>();
                try
                {
                    for(var i=0;i<32;i++)
                    {
                        var r=_registry.TakeReservation(owner.HubId,owner.Partition,owner.Epoch);
                        if(r is null) break;
                        picked.Add(r);
                    }
                }
                catch
                {
                    _admissionSlots.Release();
                    throw;
                }
                if(picked.Count==0) { _admissionSlots.Release(); continue; }
                progressed=true;
                var id=Interlocked.Increment(ref _nextAdmission);
                var running=ProcessReservationsAsync(picked,ct);
                _activeAdmissions[id]=running;
                _=running.ContinueWith(done=>{ _activeAdmissions.TryRemove(id,out var ignored); },
                    CancellationToken.None,TaskContinuationOptions.ExecuteSynchronously,TaskScheduler.Default);
            }
        } while(progressed && !ct.IsCancellationRequested);
    }

    private async Task ProcessReservationsAsync(List<HubCandidateReservation> picked,CancellationToken ct)
    {
        try
        {
            foreach(var group in picked.GroupBy(x=>_demands.TryGetValue(x.Credit.Id,out var d)?d:null))
            {
                var reservations=group.ToList(); var demand=group.Key;
                if(demand is null || demand.Result.Task.IsCompleted)
                { foreach(var r in reservations) _registry.AbandonBeforeAdmission(r.Id); continue; }
                // Only metadata with a complete empty resource set may take the
                // atomic batch path. Other tasks use a single fenced claim.
                var first=reservations[0];
                var unconstrained=first.Candidate.ResourceKeysComplete && first.Candidate.ResourceKeys.Length==0;
                var batch=unconstrained
                    ? reservations.Where(r=>r.Candidate.ResourceKeysComplete && r.Candidate.ResourceKeys.Length==0).ToList()
                    : [first];
                foreach(var extra in reservations.Except(batch))
                { _registry.AbandonBeforeAdmission(extra.Id); _demands.TryRemove(extra.Credit.Id,out _); }
                await AdmitAsync(demand,batch,ct);
            }
        }
        catch(Exception error)
        {
            _logger.LogWarning(error,"Candidate reservation dispatch failed");
            foreach(var demand in picked.Select(x=>_demands.TryGetValue(x.Credit.Id,out var d)?d:null)
                         .Where(x=>x is not null).Distinct())
                demand!.Result.TrySetException(error);
        }
        finally { _admissionSlots.Release(); }
    }

    private async Task AdmitAsync(Demand demand,List<HubCandidateReservation> reservations,CancellationToken ct)
    {
        lock(demand.Gate)
        {
            if(demand.Result.Task.IsCompleted || demand.Admitting)
            { foreach(var r in reservations) _registry.AbandonBeforeAdmission(r.Id); return; }
            demand.Admitting=true;
        }
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
        var granted=new List<string>();
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
            if(hasLease && outcome==HubCandidateResolution.Leased) granted.Add(r.Credit.Id);
            // An uncertain COMMIT must not be delivered or recycled until the
            // database proves the old epoch has ceased to be live.
            else if(hasLease && outcome==HubCandidateResolution.CommitOutcomeUnknown)
                _retained[(lease!.Id,lease.Epoch)]=new(r.Credit.Id,r.Id,lease,request.WorkerId);
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
        // A successful candidate claim consumes one ephemeral matching ticket,
        // not a worker execution slot. The SSE session or long-poll request now
        // owns the leased task and is responsible for slot conservation.
        foreach(var credit in granted) _registry.ReleaseGrantedCredit(credit);
    }
    private async Task ReapAsync(CancellationToken ct)
    {
        foreach(var pair in _retained.ToArray())
        {
            var item=pair.Value;
            if(DateTime.UtcNow<item.Lease.LeaseUntil) continue;
            if(!await _tasks.CanRetireCandidateLeaseAsync(item.Lease,item.Worker,ct)) continue;
            if(!_retained.TryRemove(pair.Key,out _)) continue;
            if(item.Reservation is Guid reservation)
                _registry.ResolveUncertain(reservation,HubCandidateUncertainResolution.RetireExpiredCredit,DateTimeOffset.UtcNow);
            _registry.ReleaseGrantedCredit(item.Credit);
        }
    }
}

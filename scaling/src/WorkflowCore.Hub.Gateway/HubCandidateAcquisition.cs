using System.Collections.Concurrent;
using WorkflowCore.Hub;

namespace WorkflowCore.Hub.Gateway;

// Candidate mode is opt-in. One instance owns all generations and transports;
// workers never receive direct access to discovery or to PostgreSQL.
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
    private readonly ConcurrentDictionary<(Guid Id,long Epoch),Retained> _retained = new();
    public bool Enabled => _ownership.CandidateTopology;

    public HubCandidateAcquisition(HubTaskStore tasks, HubMatchingOwnership ownership,
        HubReadinessBroadcaster readiness, ILogger<HubCandidateAcquisition> logger)
    {
        _tasks=tasks; _ownership=ownership; _logger=logger;
        _registry=new(new HubCandidateReader(tasks));
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
            if(demand.Credits.Count==0) throw new InvalidOperationException("Candidate demand capacity exceeded");
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

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if(!Enabled) return;
        using var timer=new PeriodicTimer(TimeSpan.FromMilliseconds(50));
        var reaper=0;
        try
        {
            while(await timer.WaitForNextTickAsync(stoppingToken))
            {
                try
                {
                    await _registry.RunOneTurnAsync(DateTimeOffset.UtcNow,stoppingToken);
                    await DrainAsync(stoppingToken);
                    if(++reaper%20==0) await ReapAsync(stoppingToken);
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
            foreach(var demand in _demands.Values.Distinct()) demand.Result.TrySetCanceled(stoppingToken);
            _registry.Dispose();
        }
    }
    private async Task DrainAsync(CancellationToken ct)
    {
        foreach(var owner in _registry.ActiveOwners())
        {
            var picked=new List<HubCandidateReservation>();
            for(var i=0;i<32;i++)
            { var r=_registry.TakeReservation(owner.HubId,owner.Partition,owner.Epoch); if(r is null) break; picked.Add(r); }
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
    }
    private async Task AdmitAsync(Demand demand,List<HubCandidateReservation> reservations,CancellationToken ct)
    {
        lock(demand.Gate)
        {
            if(demand.Result.Task.IsCompleted)
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

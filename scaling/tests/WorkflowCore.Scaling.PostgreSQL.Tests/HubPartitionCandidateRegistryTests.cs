using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubPartitionCandidateRegistryTests
{
    private sealed class Clock : TimeProvider
    {
        public DateTimeOffset Now = new(2030,1,1,0,0,0,TimeSpan.Zero);
        public override DateTimeOffset GetUtcNow() => Now;
        public void Advance(TimeSpan value) => Now += value;
    }
    private sealed class Discovery : IHubCandidateDiscovery
    {
        public readonly Dictionary<(string, int, HubCandidateStream), List<HubTaskCandidate>> Data = new();
        public readonly List<(string Hub,int Bucket,HubCandidateStream Stream,long? Cursor)> Calls = [];
        public int Active, MaxActive, Captures;
        public TaskCompletionSource? Entered, Release, PageEntered, PageRelease;
        public async Task<HubCandidateHorizon> CaptureHorizonAsync(string hub, CancellationToken ct)
        {
            Captures++; var current = Interlocked.Increment(ref Active); MaxActive = Math.Max(MaxActive,current);
            try { if (Entered is not null) { Entered.TrySetResult(); await Release!.Task.WaitAsync(ct); } return new(10000,new DateTime(2030,1,1,0,0,0,DateTimeKind.Utc)); }
            finally { Interlocked.Decrement(ref Active); }
        }
        public async Task<HubCandidatePage> ReadPageAsync(string hub,int bucket,HubCandidateStream stream,HubCandidateHorizon horizon,HubCandidateCursor? cursor,int pageSize,int byteLimit,CancellationToken ct)
        {
            Calls.Add((hub,bucket,stream,cursor?.Sequence));
            if (PageEntered is not null) { PageEntered.TrySetResult(); await PageRelease!.Task.WaitAsync(ct); }
            var all = Data.GetValueOrDefault((hub,bucket,stream),[]).Where(x => x.Sequence > (cursor?.Sequence ?? 0) && x.Sequence <= horizon.UpperSequence).OrderBy(x => x.Sequence).Take(pageSize).ToArray();
            var last = all.LastOrDefault();
            return new HubCandidatePage(all,last is null ? cursor : new(last.Sequence,stream == HubCandidateStream.Ready ? null : last.Deadline),all.Length < pageSize,all.Length*160);
        }
        public void Add(string hub, int bucket, HubCandidateStream stream, params HubTaskCandidate[] items)
        { if (!Data.TryGetValue((hub,bucket,stream),out var list)) Data[(hub,bucket,stream)] = list = []; list.AddRange(items); }
    }
    private static HubMatchingPartitionLease Owner(string hub,int partition=0,long epoch=1) =>
        new(hub,partition,"owner"+epoch,"http://localhost",epoch,new DateTime(2035,1,1,0,0,0,DateTimeKind.Utc));
    private static HubTaskCandidate Candidate(string hub,long seq,string type="activity",string? definition=null,int? version=null,string[]? keys=null,int partition=0) =>
        new(Guid.NewGuid(),seq,hub,partition,type,1,definition is null ? null : Guid.NewGuid(),definition,version,0,new DateTime(2029,1,1,0,0,0,DateTimeKind.Utc),keys ?? []);
    private static HubCandidateCoordinatorOptions Options(int entries=4,int probe=8) =>
        new(PageSize:1,PageWireBytes:256,PartitionEntries:entries,ProcessEntries:entries*4,
            PartitionBytes:65536,ProcessBytes:262144,PartitionCredits:8,ProcessCredits:32,
            ProcessPartitions:4,SequenceProbeEverySweeps:probe,RecoveryCadence:TimeSpan.FromSeconds(10),ResourceCooldown:TimeSpan.FromSeconds(5));
    private static HubCandidateCredit Credit(string id,string hub,string type="activity",HubWorkflowCapability[]? pins=null) =>
        new(id,id,[new(hub,type,1)],pins);
    private static async Task Pump(HubPartitionCandidateRegistry registry, Clock clock, int turns, Action? perTurn=null)
    { for(var i=0;i<turns;i++) { perTurn?.Invoke(); await registry.RunOneTurnAsync(clock.Now,default); } }

    [Fact]
    public async Task FullBufferAndNotificationStormStillTraverseAndRediscover()
    {
        var clock = new Clock(); var source = new Discovery(); var hub = "fair";
        var early = Enumerable.Range(1,4).Select(i => Candidate(hub,i,"unsupported")).ToArray(); var target = Candidate(hub,5);
        source.Add(hub,0,HubCandidateStream.ReadyDue,[..early,target]);
        using var registry = new HubPartitionCandidateRegistry(source,Options(entries:2),clock); registry.StartGeneration(Owner(hub),64);
        Assert.True(registry.AddCredit(hub,0,1,Credit("credit",hub)));
        await Pump(registry,clock,14,() => registry.Notify(hub,0,1));
        var reservation = registry.TakeReservation(hub,0,1); Assert.NotNull(reservation); Assert.Equal(target.Id,reservation.Candidate.Id);
        Assert.Equal(new long?[] { null,1,2,3,4,5 }, source.Calls.Where(x => x.Stream==HubCandidateStream.ReadyDue).Take(6).Select(x => x.Cursor));
        var cursors = source.Calls.Where(x => x.Stream==HubCandidateStream.ReadyDue).Select(x => x.Cursor).ToArray();
        Assert.Contains(4L,cursors); Assert.True(registry.Snapshot().Entries<=2); Assert.True(registry.Snapshot().Bytes<=262144);
        registry.Complete(reservation.Id,HubCandidateResolution.Leased,clock.Now); Assert.True(registry.ReleaseGrantedCredit("credit"));
        Assert.True(registry.AddCredit(hub,0,1,Credit("new-credit",hub,"unsupported")));
        await Pump(registry,clock,15,() => registry.Notify(hub,0,1));
        var rediscovered = registry.TakeReservation(hub,0,1); Assert.NotNull(rediscovered); Assert.Contains(early,x => x.Id==rediscovered.Candidate.Id);
    }

    [Fact]
    public async Task DueStreamIsDefaultSequenceProbeIsPeriodicAndDuplicatesNeverReserveTwice()
    {
        var clock = new Clock(); var source = new Discovery(); var hub = "streams"; var task = Candidate(hub,1);
        source.Add(hub,0,HubCandidateStream.ReadyDue,task); source.Add(hub,0,HubCandidateStream.Ready,task);
        using var registry = new HubPartitionCandidateRegistry(source,Options(probe:2),clock); registry.StartGeneration(Owner(hub),64);
        Assert.True(registry.AddCredit(hub,0,1,Credit("one",hub))); Assert.True(registry.AddCredit(hub,0,1,Credit("two",hub)));
        await Pump(registry,clock,7); var first = registry.TakeReservation(hub,0,1); Assert.NotNull(first);
        Assert.DoesNotContain(source.Calls,x => x.Stream==HubCandidateStream.Ready);
        registry.Notify(hub,0,1); await Pump(registry,clock,8);
        Assert.Contains(source.Calls,x => x.Stream==HubCandidateStream.Ready);
        Assert.Null(registry.TakeReservation(hub,0,1)); Assert.Equal(1,registry.Snapshot().Reservations);
        registry.Complete(first.Id,HubCandidateResolution.CommitOutcomeUnknown,clock.Now);
        Assert.Equal(1,registry.Snapshot().Uncertain); Assert.False(registry.CancelCredit("one"));
        Assert.False(registry.AddCredit(hub,0,1,Credit("one",hub)));
        registry.ResolveUncertain(first.Id,HubCandidateUncertainResolution.LeaseConfirmed,clock.Now);
        Assert.False(registry.AddCredit(hub,0,1,Credit("one",hub)));
        Assert.True(registry.ReleaseGrantedCredit("one")); Assert.True(registry.AddCredit(hub,0,1,Credit("one",hub)));
    }

    [Fact]
    public async Task WorkflowPinsAndCapabilitiesReserveDistinctRealCredits()
    {
        var clock = new Clock(); var source = new Discovery(); var hub = "pins";
        var a = Candidate(hub,1,"__native_workflow__","flow",1); var b = Candidate(hub,2,"__native_workflow__","flow",2); var c = Candidate(hub,3);
        source.Add(hub,0,HubCandidateStream.ReadyDue,a,b,c);
        using var registry = new HubPartitionCandidateRegistry(source,Options(entries:6),clock); registry.StartGeneration(Owner(hub),64);
        Assert.True(registry.AddCredit(hub,0,1,Credit("p2",hub,"__native_workflow__",[new(hub,"flow",2)])));
        Assert.True(registry.AddCredit(hub,0,1,new HubCandidateCredit("p1","p1",[new(hub,"__native_workflow__",1),new(hub,"activity",1)],[new(hub,"flow",1)])));
        Assert.True(registry.AddCredit(hub,0,1,Credit("activity",hub)));
        await Pump(registry,clock,10);
        var reservations = Enumerable.Range(0,3).Select(_ => registry.TakeReservation(hub,0,1)).ToArray();
        Assert.All(reservations,Assert.NotNull); Assert.Equal(3,reservations.Select(x => x!.Candidate.Id).Distinct().Count());
        Assert.Equal(a.Id,reservations.Single(x => x!.Credit.Id=="p1")!.Candidate.Id);
        Assert.Equal(b.Id,reservations.Single(x => x!.Credit.Id=="p2")!.Candidate.Id);
        Assert.Equal(c.Id,reservations.Single(x => x!.Credit.Id=="activity")!.Candidate.Id);
        Assert.Null(registry.TakeReservation(hub,0,1));
    }

    [Fact]
    public async Task BlockedResourceDefersLaterPagesUntilWakeAndLeavesOtherWorkEligible()
    {
        var clock = new Clock(); var source = new Discovery(); var hub = "resources";
        var a = Candidate(hub,1,keys:["scope"]); var b = Candidate(hub,2,keys:["scope"]); var c = Candidate(hub,3,keys:["other"]);
        source.Add(hub,0,HubCandidateStream.ReadyDue,a,b,c);
        using var registry = new HubPartitionCandidateRegistry(source,Options(entries:5),clock); registry.StartGeneration(Owner(hub),64);
        Assert.True(registry.AddCredit(hub,0,1,Credit("one",hub)));
        await Pump(registry,clock,3); var first = registry.TakeReservation(hub,0,1); Assert.NotNull(first); Assert.Equal(a.Id,first.Candidate.Id);
        registry.Complete(first.Id,HubCandidateResolution.ResourceBlocked,clock.Now,"scope");
        await Pump(registry,clock,5); var other = registry.TakeReservation(hub,0,1); Assert.NotNull(other); Assert.Equal(c.Id,other.Candidate.Id);
        registry.Complete(other.Id,HubCandidateResolution.Leased,clock.Now); Assert.True(registry.ReleaseGrantedCredit("one"));
        Assert.True(registry.AddCredit(hub,0,1,Credit("two",hub))); Assert.Null(registry.TakeReservation(hub,0,1));
        registry.Notify(hub,0,1,"scope"); var unblocked = registry.TakeReservation(hub,0,1); Assert.NotNull(unblocked); Assert.Equal(a.Id,unblocked.Candidate.Id);
    }

    [Fact]
    public async Task OwnerCancellationRetainsDispatchedCreditAndCandidateUntilResolution()
    {
        var clock = new Clock(); var source = new Discovery(); var hub = "ownership"; var task = Candidate(hub,1);
        source.Add(hub,0,HubCandidateStream.ReadyDue,task);
        using var registry = new HubPartitionCandidateRegistry(source,Options(),clock); registry.StartGeneration(Owner(hub),64);
        Assert.True(registry.AddCredit(hub,0,1,Credit("credit",hub))); await Pump(registry,clock,3);
        var first = registry.TakeReservation(hub,0,1); Assert.NotNull(first);
        registry.StartGeneration(Owner(hub,epoch:2),64); Assert.True(first.OwnerStopped.IsCancellationRequested);
        Assert.False(registry.AddCredit(hub,0,2,Credit("credit",hub)));
        registry.Complete(first.Id,HubCandidateResolution.CommitOutcomeUnknown,clock.Now);
        Assert.True(registry.AddCredit(hub,0,2,Credit("other",hub))); await Pump(registry,clock,6);
        Assert.Null(registry.TakeReservation(hub,0,2)); Assert.Equal(1,registry.Snapshot().Uncertain);
        registry.ResolveUncertain(first.Id,HubCandidateUncertainResolution.AuthoritativeNoCommit,clock.Now);
        Assert.True(registry.AddCredit(hub,0,2,Credit("credit",hub)));
        registry.Notify(hub,0,2); await Pump(registry,clock,8);
        var again = registry.TakeReservation(hub,0,2); Assert.NotNull(again); Assert.Equal(task.Id,again.Candidate.Id);
    }

    [Fact]
    public async Task SchedulerSerializesSqlAndRotatesPartitionsUnderPressure()
    {
        var clock = new Clock(); var source = new Discovery { Entered = new(TaskCreationOptions.RunContinuationsAsynchronously), Release = new(TaskCreationOptions.RunContinuationsAsynchronously) };
        using var registry = new HubPartitionCandidateRegistry(source,Options(),clock); registry.StartGeneration(Owner("one"),64); registry.StartGeneration(Owner("two"),64);
        var first = registry.RunOneTurnAsync(clock.Now,default); await source.Entered.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.False(await registry.RunOneTurnAsync(clock.Now,default)); Assert.Equal(1,source.MaxActive);
        source.Release.SetResult(); Assert.True(await first); source.Entered=null;
        await Pump(registry,clock,6);
        Assert.Contains(source.Calls,x => x.Hub=="one"); Assert.Contains(source.Calls,x => x.Hub=="two");
        var snapshot=registry.Snapshot(); Assert.InRange(snapshot.Bytes,0,262144); Assert.InRange(snapshot.Entries,0,16); Assert.Equal(0,snapshot.InFlightPages);
    }
    [Fact]
    public async Task SameEpochRediscoveryRefreshesPreviouslyIncompatibleMetadata()
    {
        var clock = new Clock(); var source = new Discovery(); var hub = "refresh";
        var before = Candidate(hub,1,"unsupported");
        source.Add(hub,0,HubCandidateStream.ReadyDue,before);
        using var registry = new HubPartitionCandidateRegistry(source,Options(),clock); registry.StartGeneration(Owner(hub),64);
        Assert.True(registry.AddCredit(hub,0,1,Credit("c",hub))); await Pump(registry,clock,6);
        Assert.Null(registry.TakeReservation(hub,0,1));
        source.Data[(hub,0,HubCandidateStream.ReadyDue)][0] = before with { TaskType="activity" };
        registry.Notify(hub,0,1); await Pump(registry,clock,6);
        var reservation = registry.TakeReservation(hub,0,1); Assert.NotNull(reservation); Assert.Equal(before.Id,reservation.Candidate.Id);
    }

    [Fact]
    public async Task DemandLimitsAndUncertainReservationsRemainChargedUnderPressure()
    {
        var clock = new Clock(); var source = new Discovery(); var hub = "budget";
        var first = Candidate(hub,1); var second = Candidate(hub,2);
        source.Add(hub,0,HubCandidateStream.ReadyDue,first,second);
        var options = Options(entries:2) with { PartitionCredits=1, ProcessCredits=2 };
        using var registry = new HubPartitionCandidateRegistry(source,options,clock); registry.StartGeneration(Owner(hub),64);
        Assert.True(registry.AddCredit(hub,0,1,Credit("one",hub))); Assert.False(registry.AddCredit(hub,0,1,Credit("two",hub)));
        await Pump(registry,clock,4); var reservation = registry.TakeReservation(hub,0,1); Assert.NotNull(reservation);
        registry.Complete(reservation.Id,HubCandidateResolution.CommitOutcomeUnknown,clock.Now);
        await Pump(registry,clock,25,() => registry.Notify(hub,0,1));
        var snapshot = registry.Snapshot(); Assert.Equal(1,snapshot.Uncertain); Assert.Equal(1,snapshot.Credits);
        Assert.InRange(snapshot.Entries,1,options.PartitionEntries); Assert.InRange(snapshot.Bytes,1,options.PartitionBytes);
        Assert.False(registry.CancelCredit("one")); Assert.False(registry.AddCredit(hub,0,1,Credit("two",hub)));
        registry.StopGeneration(hub,0,1); registry.StartGeneration(Owner(hub,epoch:2),64);
        Assert.False(registry.AddCredit(hub,0,2,Credit("one",hub)));
        Assert.False(registry.AddCredit(hub,0,2,Credit("two",hub)));
        registry.ResolveUncertain(reservation.Id,HubCandidateUncertainResolution.AuthoritativeNoCommit,clock.Now);
        Assert.True(registry.AddCredit(hub,0,2,Credit("two",hub)));
    }

    [Fact]
    public async Task RecoverySweepsRevisitWorkWithoutNotifications()
    {
        var clock = new Clock(); var source = new Discovery(); var hub = "recovery";
        using var registry = new HubPartitionCandidateRegistry(source,Options(),clock); registry.StartGeneration(Owner(hub),64);
        Assert.True(registry.AddCredit(hub,0,1,Credit("c",hub))); await Pump(registry,clock,4);
        Assert.Equal(1,registry.Snapshot().CompletedSweeps); var captures = source.Captures;
        var task = Candidate(hub,1); source.Add(hub,0,HubCandidateStream.ReadyDue,task);
        await Pump(registry,clock,4); Assert.Equal(captures,source.Captures); Assert.Null(registry.TakeReservation(hub,0,1));
        clock.Advance(TimeSpan.FromSeconds(11)); await Pump(registry,clock,3);
        var reservation = registry.TakeReservation(hub,0,1); Assert.NotNull(reservation); Assert.Equal(task.Id,reservation.Candidate.Id);
    }

    [Fact]
    public async Task InFlightPageIsChargedAndOwnerCancellationStopsRead()
    {
        var clock = new Clock(); var source = new Discovery { PageEntered=new(TaskCreationOptions.RunContinuationsAsynchronously), PageRelease=new(TaskCreationOptions.RunContinuationsAsynchronously) };
        var options = Options(entries:2);
        using var registry = new HubPartitionCandidateRegistry(source,options,clock); registry.StartGeneration(Owner("busy"),64);
        Assert.True(await registry.RunOneTurnAsync(clock.Now,default));
        var page = registry.RunOneTurnAsync(clock.Now,default); await source.PageEntered.Task.WaitAsync(TimeSpan.FromSeconds(2));
        var snapshot = registry.Snapshot(); Assert.Equal(1,snapshot.InFlightPages); Assert.Equal(options.PageSize,snapshot.Entries);
        Assert.InRange(snapshot.Bytes,options.PageReservationBytes,options.PartitionBytes);
        Assert.False(await registry.RunOneTurnAsync(clock.Now,default));
        registry.StopGeneration("busy",0,1); Assert.True(await page.WaitAsync(TimeSpan.FromSeconds(2)));
        snapshot = registry.Snapshot(); Assert.Equal(0,snapshot.InFlightPages); Assert.Equal(0,snapshot.Entries); Assert.Equal(0,snapshot.Bytes);
    }

    [Fact]
    public async Task HeadCatchupIsRateLimitedAndDoesNotResetForwardCursor()
    {
        var clock = new Clock(); var source = new Discovery(); var hub = "head";
        source.Add(hub,0,HubCandidateStream.ReadyDue,Enumerable.Range(1,30).Select(i => Candidate(hub,i,"other")).ToArray());
        using var registry = new HubPartitionCandidateRegistry(source,Options(entries:3),clock); registry.StartGeneration(Owner(hub),64);
        await Pump(registry,clock,28,() => registry.Notify(hub,0,1));
        var ready = source.Calls.Where(x => x.Stream == HubCandidateStream.ReadyDue).ToArray();
        Assert.True(ready.Count(x => x.Cursor is null) >= 2);
        Assert.Contains(ready,x => x.Cursor >= 20);
        var forward = ready.Where(x => x.Cursor.HasValue).Select(x => x.Cursor!.Value).ToArray();
        Assert.Equal(Enumerable.Range(1,forward.Length).Select(x => (long)x),forward);
    }

    [Fact]
    public async Task BusyBucketDoesNotStarveOtherBucketInPartition()
    {
        var clock = new Clock(); var source = new Discovery(); var hub = "bucket-fairness";
        source.Add(hub,0,HubCandidateStream.ReadyDue,Enumerable.Range(1,20).Select(i => Candidate(hub,i,"unsupported")).ToArray());
        var target = Candidate(hub,100,partition:32); source.Add(hub,32,HubCandidateStream.ReadyDue,target);
        using var registry = new HubPartitionCandidateRegistry(source,Options(entries:3),clock); registry.StartGeneration(Owner(hub),32);
        Assert.True(registry.AddCredit(hub,0,1,Credit("c",hub)));
        await Pump(registry,clock,5);
        var reservation = registry.TakeReservation(hub,0,1); Assert.NotNull(reservation); Assert.Equal(target.Id,reservation.Candidate.Id);
        Assert.Equal(1,source.Calls.Count(x => x.Bucket == 0 && x.Stream == HubCandidateStream.ReadyDue));
    }

    [Fact]
    public async Task NewerEpochWaitsForReservationResolutionThenIsRediscovered()
    {
        var clock = new Clock(); var source = new Discovery(); var hub = "new-epoch"; var original = Candidate(hub,1);
        source.Add(hub,0,HubCandidateStream.ReadyDue,original);
        using var registry = new HubPartitionCandidateRegistry(source,Options(),clock); registry.StartGeneration(Owner(hub),64);
        Assert.True(registry.AddCredit(hub,0,1,Credit("c",hub))); await Pump(registry,clock,3);
        var first = registry.TakeReservation(hub,0,1); Assert.NotNull(first);
        source.Data[(hub,0,HubCandidateStream.ReadyDue)][0] = original with { Epoch=1 };
        registry.Notify(hub,0,1); await Pump(registry,clock,8);
        Assert.Null(registry.TakeReservation(hub,0,1)); Assert.Equal(1,registry.Snapshot().Reservations);
        registry.Complete(first.Id,HubCandidateResolution.Unavailable,clock.Now);
        registry.Notify(hub,0,1); await Pump(registry,clock,8);
        var later = registry.TakeReservation(hub,0,1); Assert.NotNull(later); Assert.Equal(original.Id,later.Candidate.Id); Assert.Equal(1,later.Candidate.Epoch);
    }

    [Fact]
    public async Task ProcessWideLimitsHoldWhilePartitionsEvictAndMakeProgress()
    {
        var clock = new Clock(); var source = new Discovery();
        var first = Candidate("p1",5); var second = Candidate("p2",5);
        foreach (var (hub, target) in new[] { ("p1",first), ("p2",second) })
            source.Add(hub,0,HubCandidateStream.ReadyDue,[..Enumerable.Range(1,4).Select(i => Candidate(hub,i,"unsupported")),target]);
        var options = Options(entries:2) with { ProcessEntries=2, PartitionBytes=32768, ProcessBytes=40000, ProcessPartitions=2 };
        using var registry = new HubPartitionCandidateRegistry(source,options,clock);
        registry.StartGeneration(Owner("p1"),64); registry.StartGeneration(Owner("p2"),64);
        Assert.True(registry.AddCredit("p1",0,1,Credit("one","p1"))); Assert.True(registry.AddCredit("p2",0,1,Credit("two","p2")));
        for (var i=0;i<40;i++)
        {
            await registry.RunOneTurnAsync(clock.Now,default);
            var state=registry.Snapshot(); Assert.InRange(state.Entries,0,options.ProcessEntries); Assert.InRange(state.Bytes,0,options.ProcessBytes);
        }
        var a=registry.TakeReservation("p1",0,1); var b=registry.TakeReservation("p2",0,1);
        Assert.NotNull(a); Assert.NotNull(b); Assert.Equal(first.Id,a.Candidate.Id); Assert.Equal(second.Id,b.Candidate.Id);
        Assert.Equal(2,registry.Snapshot().Reservations);
    }

    [Fact]
    public async Task CancellationRetiresOnlyUndispatchedReservations()
    {
        var clock=new Clock(); var source=new Discovery(); var hub="cancel-credit"; var task=Candidate(hub,1);
        source.Add(hub,0,HubCandidateStream.ReadyDue,task);
        using var registry=new HubPartitionCandidateRegistry(source,Options(),clock); registry.StartGeneration(Owner(hub),64);
        await Pump(registry,clock,3);
        Assert.True(registry.AddCredit(hub,0,1,Credit("abandoned",hub)));
        Assert.Equal(1,registry.Snapshot().Reservations); Assert.True(registry.CancelCredit("abandoned"));
        Assert.True(registry.AddCredit(hub,0,1,Credit("next",hub)));
        var claim=registry.TakeReservation(hub,0,1); Assert.NotNull(claim); Assert.Equal(task.Id,claim.Candidate.Id);
        Assert.False(registry.CancelCredit("next"));
        registry.Complete(claim.Id,HubCandidateResolution.CommitOutcomeUnknown,clock.Now);
        Assert.False(registry.CancelCredit("next"));
        registry.ResolveUncertain(claim.Id,HubCandidateUncertainResolution.LeaseConfirmed,clock.Now);
        Assert.False(registry.CancelCredit("next")); Assert.True(registry.ReleaseGrantedCredit("next"));
    }

}

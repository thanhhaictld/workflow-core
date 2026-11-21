using System.Diagnostics.Metrics;
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
        public bool AlwaysFailHydration;
        public int HydrationCalls;
        public TaskCompletionSource? Entered, Release, PageEntered, PageRelease;
        public TaskCompletionSource? HydrationEntered, HydrationRelease;
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
        public async Task<HubCandidateHydrationPage> HydrateAsync(string hub,int matchingPartition,int partitionCount,
            IReadOnlyList<Guid> ids,int byteLimit,CancellationToken ct)
        {
            Interlocked.Increment(ref HydrationCalls);
            if (AlwaysFailHydration) throw new InvalidOperationException("synthetic hydration failure");
            if (HydrationEntered is not null)
            {
                HydrationEntered.TrySetResult();
                await HydrationRelease!.Task.WaitAsync(ct);
            }
            var items = Data.Values.SelectMany(x => x).Where(x => ids.Contains(x.Id) && x.HubId == hub &&
                x.PartitionId % partitionCount == matchingPartition).DistinctBy(x => x.Id).ToArray();
            return new HubCandidateHydrationPage(items,0,items.Length*160);
        }
        public void Add(string hub, int bucket, HubCandidateStream stream, params HubTaskCandidate[] items)
        { if (!Data.TryGetValue((hub,bucket,stream),out var list)) Data[(hub,bucket,stream)] = list = []; list.AddRange(items); }
    }
    private static HubMatchingPartitionLease Owner(string hub,int partition=0,long epoch=1) =>
        new(hub,partition,"owner"+epoch,"http://localhost",epoch,new DateTime(2035,1,1,0,0,0,DateTimeKind.Utc));
    private static HubTaskCandidate Candidate(string hub,long seq,string type="activity",string? definition=null,int? version=null,int partition=0) =>
        new(Guid.NewGuid(),seq,hub,partition,type,1,definition is null ? null : Guid.NewGuid(),definition,version,0,new DateTime(2029,1,1,0,0,0,DateTimeKind.Utc));
    private static HubCandidateCoordinatorOptions Options(int entries=4,int probe=8) =>
        new(PageSize:1,PageWireBytes:256,PartitionEntries:entries,ProcessEntries:entries*4,
            PartitionBytes:65536,ProcessBytes:262144,PartitionCredits:8,ProcessCredits:32,
            ProcessPartitions:4,SequenceProbeEverySweeps:probe,RecoveryCadence:TimeSpan.FromSeconds(10),AdmissionCooldown:TimeSpan.FromSeconds(5));
    private static HubCandidateCredit Credit(string id,string hub,string type="activity",HubWorkflowCapability[]? pins=null) =>
        new(id,id,[new(hub,type,1)],pins);
    private static async Task Pump(HubPartitionCandidateRegistry registry, Clock clock, int turns, Action? perTurn=null)
    { for(var i=0;i<turns;i++) { perTurn?.Invoke(); await registry.RunOneTurnAsync(clock.Now,default); } }

    [Fact]
    public async Task DemandGatingStopsDiscoveryAndResumesWhenCreditArrives()
    {
        var clock = new Clock(); var source = new Discovery(); var hub = "demand-gate";
        var candidate = Candidate(hub, 1);
        source.Add(hub, 0, HubCandidateStream.ReadyDue, candidate);
        using var registry = new HubPartitionCandidateRegistry(source, Options() with { RequireDemand = true }, clock);
        registry.StartGeneration(Owner(hub), 64);
        registry.Notify(hub, 0, 1);
        await Pump(registry, clock, 10);
        Assert.Equal(0, source.Captures);
        Assert.Empty(source.Calls);
        Assert.True(registry.AddCredit(hub, 0, 1, Credit("first", hub)));
        await Pump(registry, clock, 4);
        var reservation = registry.TakeReservation(hub, 0, 1);
        Assert.NotNull(reservation);
        Assert.Equal(candidate.Id, reservation.Candidate.Id);
        registry.Complete(reservation.Id, HubCandidateResolution.Leased, clock.Now);
        var captures = source.Captures; var reads = source.Calls.Count;
        clock.Advance(TimeSpan.FromMinutes(1));
        registry.Notify(hub, 0, 1);
        await Pump(registry, clock, 10);
        Assert.Equal(captures, source.Captures);
        Assert.Equal(reads, source.Calls.Count);
    }

    [Fact]
    public async Task ExactHintHydratesByIdAndRetainsSameIdHintArrivingDuringHydration()
    {
        var clock = new Clock(); var source = new Discovery(); var hub = "exact-hint";
        var candidate = Candidate(hub, 1); source.Add(hub, 0, HubCandidateStream.ReadyDue, candidate);
        source.HydrationEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        source.HydrationRelease = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registry = new HubPartitionCandidateRegistry(source, Options(), clock);
        registry.StartGeneration(Owner(hub), 64);
        Assert.True(registry.AddCredit(hub, 0, 1, Credit("worker", hub)));
        Assert.True(registry.NotifyTask(hub, 0, 1, candidate.Id));
        var firstHydration = registry.RunOneTurnAsync(clock.Now, default);
        await source.HydrationEntered.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.True(registry.NotifyTask(hub, 0, 1, candidate.Id));
        source.HydrationRelease.SetResult();
        await firstHydration;
        Assert.Equal(0, source.Captures);
        Assert.Empty(source.Calls);
        Assert.Equal(1, source.HydrationCalls);
        await registry.RunOneTurnAsync(clock.Now, default);
        Assert.Equal(2, source.HydrationCalls);
        Assert.Equal(candidate.Id, registry.TakeReservation(hub, 0, 1)!.Candidate.Id);
    }

    [Fact]
    public async Task ExactHintForNoLongerEligibleTaskRemovesOnlyUnreservedCandidate()
    {
        var clock = new Clock(); var source = new Discovery(); var hub = "exact-terminal";
        var candidate = Candidate(hub, 1); source.Add(hub, 0, HubCandidateStream.ReadyDue, candidate);
        using var registry = new HubPartitionCandidateRegistry(source, Options(), clock);
        registry.StartGeneration(Owner(hub), 64);
        Assert.True(registry.NotifyTask(hub, 0, 1, candidate.Id));
        await registry.RunOneTurnAsync(clock.Now, default);
        Assert.Equal(1, registry.Snapshot().Entries);
        source.Data[(hub, 0, HubCandidateStream.ReadyDue)].Clear();
        Assert.True(registry.NotifyTask(hub, 0, 1, candidate.Id));
        await registry.RunOneTurnAsync(clock.Now, default);
        Assert.Equal(0, registry.Snapshot().Entries);
    }

    [Fact]
    public async Task RepeatedHydrationFailuresYieldToIndexedRecoveryAndLaterExactWork()
    {
        var clock = new Clock(); var source = new Discovery { AlwaysFailHydration = true }; var hub = "exact-fail-fairness";
        using var registry = new HubPartitionCandidateRegistry(source, Options(), clock);
        registry.StartGeneration(Owner(hub), 64);
        var first = Candidate(hub, 1);
        Assert.True(registry.NotifyTask(hub, 0, 1, first.Id));
        await Assert.ThrowsAsync<InvalidOperationException>(() => registry.RunOneTurnAsync(clock.Now, default));
        await registry.RunOneTurnAsync(clock.Now, default); // a failed hydration yields to horizon capture
        await registry.RunOneTurnAsync(clock.Now, default); // indexed page
        await Assert.ThrowsAsync<InvalidOperationException>(() => registry.RunOneTurnAsync(clock.Now, default));
        await registry.RunOneTurnAsync(clock.Now, default); // another failure cannot block forward recovery
        Assert.Equal(1, source.Captures);
        Assert.NotEmpty(source.Calls);

        // Reset the retry budget on every recovery page, so exact work keeps
        // receiving bounded service during a long paged sweep.
        source.AlwaysFailHydration = false;
        var second = Candidate(hub, 2); source.Add(hub, 0, HubCandidateStream.ReadyDue, second);
        Assert.True(registry.NotifyTask(hub, 0, 1, second.Id));
        for (var i = 0; i < 24; i++) await registry.RunOneTurnAsync(clock.Now, default);
        Assert.True(source.HydrationCalls > 2);
        Assert.True(source.Calls.Count > 1);
    }


    [Fact]
    public async Task ExactHintFloodIsBoundedAndFallsBackToIndexedRecovery()
    {
        var clock = new Clock(); var source = new Discovery(); var hub = "exact-flood";
        using var registry = new HubPartitionCandidateRegistry(source, Options(), clock);
        registry.StartGeneration(Owner(hub), 64);
        var accepted = 0;
        for (var i = 0; i < 20; i++)
            if (registry.NotifyTask(hub, 0, 1, Guid.NewGuid())) accepted++;
        Assert.Equal(4, accepted);
        Assert.True(registry.Snapshot().Bytes <= Options().ProcessBytes);
        await Pump(registry, clock, 20);
        Assert.True(source.HydrationCalls <= 8);
        Assert.True(source.Captures > 0);
    }

    [Fact]
    public async Task OwnerGenerationLossDuringHydrationCannotPopulateReplacement()
    {
        var clock = new Clock(); var source = new Discovery(); var hub = "exact-takeover";
        var candidate = Candidate(hub, 1); source.Add(hub, 0, HubCandidateStream.ReadyDue, candidate);
        source.HydrationEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        source.HydrationRelease = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registry = new HubPartitionCandidateRegistry(source, Options(), clock);
        registry.StartGeneration(Owner(hub, epoch: 1), 64);
        registry.NotifyTask(hub, 0, 1, candidate.Id);
        var hydration = registry.RunOneTurnAsync(clock.Now, default);
        await source.HydrationEntered.Task.WaitAsync(TimeSpan.FromSeconds(2));
        source.Data.Clear();
        registry.StartGeneration(Owner(hub, epoch: 2), 64);
        await hydration;
        source.HydrationEntered = null;
        source.HydrationRelease!.SetResult();
        await Pump(registry, clock, 3);
        Assert.Equal(0, registry.Snapshot().Entries);
        Assert.True(source.Captures > 0);
    }

    [Fact]
    public async Task CoalescedHintsPrioritizeTheirOwnedBucket()
    {
        var clock = new Clock(); var source = new Discovery(); var hub = "hint-priority";
        using var registry = new HubPartitionCandidateRegistry(source, Options() with { RequireDemand = true }, clock);
        registry.StartGeneration(Owner(hub), 1);
        registry.Notify(hub, 0, 1, bucket: 17);
        registry.Notify(hub, 0, 1, bucket: 17);
        Assert.True(registry.AddCredit(hub, 0, 1, Credit("credit", hub)));
        await Pump(registry, clock, 2);
        Assert.Equal(17, Assert.Single(source.Calls).Bucket);
    }

    [Fact]
    public async Task WaitingDemandConsumesNoCreditUntilCompatibleCandidateIsAvailable()
    {
        var clock = new Clock(); var source = new Discovery(); var hub = "lazy-demand";
        var task = Candidate(hub, 1); source.Add(hub, 0, HubCandidateStream.ReadyDue, task);
        using var registry = new HubPartitionCandidateRegistry(source, Options() with { RequireDemand = true }, clock);
        registry.StartGeneration(Owner(hub), 64);
        Assert.True(registry.AddWaitingDemand(hub, 0, 1));
        Assert.False(registry.AddMatchedCredit(hub, 0, 1, Credit("incompatible", hub, "other")));
        Assert.Equal(0, registry.Snapshot().Credits);
        await Pump(registry, clock, 2);
        Assert.False(registry.AddMatchedCredit(hub, 0, 1, Credit("incompatible", hub, "other")));
        Assert.Equal(0, registry.Snapshot().Credits);
        Assert.True(registry.AddMatchedCredit(hub, 0, 1, Credit("matched", hub)));
        Assert.Equal(1, registry.Snapshot().Credits);
        Assert.Equal(task.Id, registry.TakeReservation(hub, 0, 1)!.Candidate.Id);
        registry.RemoveWaitingDemand(hub, 0, 1);
    }

    [Fact]
    public async Task BackpressuredCreditSearchUsesCapabilityQueuesAndHonorsOlderCredit()
    {
        var clock = new Clock();
        var source = new Discovery();
        const string hub = "backpressured-capability";
        var first = Candidate(hub, 65);
        var second = Candidate(hub, 66);
        var backlog = Enumerable.Range(1, 64).Select(i => Candidate(hub, i, "unrelated"));
        source.Add(hub, 0, HubCandidateStream.ReadyDue, backlog.Append(first).Append(second).ToArray());
        using var registry = new HubPartitionCandidateRegistry(source,
            Options(entries: 80) with { PageSize = 80, PartitionBytes = 262144, ProcessBytes = 1048576 }, clock);
        registry.StartGeneration(Owner(hub), 64);
        await Pump(registry, clock, 2);
        Assert.Equal(66, registry.Snapshot().Entries);

        // Both compatible candidates are deferred while PostgreSQL admission is full.
        foreach (var id in new[] { "first-attempt", "second-attempt" })
        {
            Assert.True(registry.AddMatchedCredit(hub, 0, 1, Credit(id, hub)));
            var reservation = registry.TakeReservation(hub, 0, 1)!;
            registry.Complete(reservation.Id, HubCandidateResolution.AdmissionDeferred, clock.Now);
            Assert.True(registry.CancelCredit(id));
        }
        Assert.True(registry.AddCredit(hub, 0, 1, Credit("older", hub)));
        Assert.Null(registry.TakeReservation(hub, 0, 1));
        clock.Advance(TimeSpan.FromSeconds(6));
        var matchingThread = Environment.CurrentManagedThreadId;
        var checkedEntries = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (instrument.Name == HubTelemetry.CandidateMatcherEntriesChecked.Name)
                meterListener.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<int>((_, count, _, _) =>
        {
            if (Environment.CurrentManagedThreadId == matchingThread) checkedEntries += count;
        });
        listener.Start();
        Assert.True(registry.AddMatchedCredit(hub, 0, 1, Credit("newer", hub)));
        Assert.Equal(3, checkedEntries);

        var older = registry.TakeReservation(hub, 0, 1)!;
        var newer = registry.TakeReservation(hub, 0, 1)!;
        Assert.Equal(("older", first.Id), (older.Credit.Id, older.Candidate.Id));
        Assert.Equal(("newer", second.Id), (newer.Credit.Id, newer.Candidate.Id));
    }

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
    public async Task UnknownWorkflowMetadataRemainsAdvisoryAndCannotStrandPinnedWork()
    {
        var clock = new Clock(); var source = new Discovery(); var hub = "unknown-pinned-workflow";
        var unknown = Candidate(hub, 1, "__native_workflow__") with
        {
            WorkflowId = Guid.NewGuid(), WorkflowMetadataComplete = false
        };
        source.Add(hub, 0, HubCandidateStream.ReadyDue, unknown);
        using var registry = new HubPartitionCandidateRegistry(source, Options(entries: 4), clock);
        registry.StartGeneration(Owner(hub), 64);
        Assert.True(registry.AddCredit(hub, 0, 1, Credit("pinned", hub, "__native_workflow__",
            [new HubWorkflowCapability(hub, "expected-definition", 5)])));
        await Pump(registry, clock, 6);
        var reservation = registry.TakeReservation(hub, 0, 1);
        Assert.NotNull(reservation);
        Assert.Equal(unknown.Id, reservation.Candidate.Id);
        Assert.False(reservation.Candidate.WorkflowMetadataComplete);
        Assert.Null(reservation.Candidate.DefinitionId);
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

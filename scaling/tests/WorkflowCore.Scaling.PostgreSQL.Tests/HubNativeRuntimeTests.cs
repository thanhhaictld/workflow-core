using System.Text.Json;
using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubNativeRuntimeTests
{
    [Fact]
    public async Task WorkflowClaimsRespectRegisteredDefinitionVersionsDuringRollout()
    {
        var database=Environment.GetEnvironmentVariable("WFC_TEST_PG");if(database is null)return;
        await using var tasks=new HubTaskStore(database);
        var definitions=new HubDefinitionStore(tasks);var remote=new HubRemoteWorkflowStore(tasks,definitions);
        var hub="versions-"+Guid.NewGuid().ToString("N");
        var metadata=JsonSerializer.SerializeToElement(new{executionMode="native"});
        await definitions.RegisterAsync(new(hub,"native",2,4,1,metadata),default);
        await definitions.RegisterAsync(new(hub,"native",3,4,1,metadata),default);
        // The unsupported version is deliberately first in FIFO task order.
        var v3=await remote.StartAsync(new(hub,"native",3,JsonSerializer.SerializeToElement(new{})),default);
        var v2=await remote.StartAsync(new(hub,"native",2,JsonSerializer.SerializeToElement(new{})),default);
        var claim=Assert.Single(await tasks.ClaimAsync("v2-worker",[hub],[new(hub,"__native_workflow__",1)],1,
            TimeSpan.FromSeconds(30),default,[new(hub,"native",2)]));
        Assert.Equal(v2,claim.WorkflowId);
        Assert.Empty(await tasks.ClaimAsync("v2-worker",[hub],[new(hub,"__native_workflow__",1)],1,
            TimeSpan.FromSeconds(30),default,[new(hub,"native",2)]));
        claim=Assert.Single(await tasks.ClaimAsync("v3-worker",[hub],[new(hub,"__native_workflow__",1)],1,
            TimeSpan.FromSeconds(30),default,[new(hub,"native",3)]));
        Assert.Equal(v3,claim.WorkflowId);
    }

    [Fact]
    public async Task ExhaustedActivityTransportClosesAttemptAndDeliversNativeFailure()
    {
        var database=Environment.GetEnvironmentVariable("WFC_TEST_PG");if(database is null)return;
        await using var tasks=new HubTaskStore(database);
        var definitions=new HubDefinitionStore(tasks);var native=new HubNativeWorkflowStore(tasks,definitions);
        var hub="exhaust-"+Guid.NewGuid().ToString("N");
        await definitions.RegisterAsync(new(hub,"native",2,4,1,JsonSerializer.SerializeToElement(new{executionMode="native"})),default);
        await definitions.RegisterActivityAsync(new(hub,"act",2,4,10,10),default);
        var id=await new HubRemoteWorkflowStore(tasks,definitions).StartAsync(new(hub,"native",2,JsonSerializer.SerializeToElement(new{})),default);
        var workflow=Assert.Single(await tasks.ClaimAsync("worker",[hub],[new(hub,"__native_workflow__",1)],1,TimeSpan.FromSeconds(30),default));
        Assert.NotNull(await native.CommitAsync(id,new(1,"worker",workflow.Epoch,0,workflow.Id,JsonSerializer.SerializeToElement(new{}),
            "Runnable",false,null,[],[],[new("exhausted-dispatch","pointer",0,"act",2,JsonSerializer.SerializeToElement(new{}))],[],["exhausted-dispatch"]),default));
        var activity=Assert.Single(await tasks.ClaimAsync("worker",[hub],[new(hub,"act",2)],1,TimeSpan.FromSeconds(30),default));
        await using(var db=new NpgsqlConnection(database))
        {
            await db.OpenAsync();
            await using var limit=new NpgsqlCommand("UPDATE wfc_scale.hub_task SET max_attempts=1 WHERE id=@id",db);
            limit.Parameters.AddWithValue("id",activity.Id);await limit.ExecuteNonQueryAsync();
        }
        Assert.True(await tasks.FailAsync(activity.Id,"worker",activity.Epoch,"Transport unavailable",TimeSpan.Zero,default));
        await new HubTimerStore(tasks).FireDueAsync(100,default);
        var runtime=(await native.GetRuntimeAsync(id,default))!;
        var message=Assert.Single(runtime.Inbox);
        Assert.Equal("exhausted-dispatch",message.Payload.GetProperty("dispatchKey").GetString());
        Assert.Contains("transport attempts exhausted",message.Payload.GetProperty("error").GetString());
        var trace=Assert.Single(await new HubWorkflowStore(tasks,new HubPolicyRegistry(),new HubWorkflowRegistry()).TraceAsync(id,default));
        var attempt=Assert.Single(trace.Attempts);
        Assert.Equal("Failed",attempt.Outcome);Assert.NotNull(attempt.FinishedAt);
        Assert.Single(await tasks.ClaimAsync("worker",[hub],[new(hub,"__native_workflow__",1)],1,TimeSpan.FromSeconds(30),default));
    }

    [Fact]
    public async Task ConcurrentPublicationAndSubscriptionCommitCannotLoseWakeup()
    {
        var database=Environment.GetEnvironmentVariable("WFC_TEST_PG");if(database is null)return;
        await using var tasks=new HubTaskStore(database);
        var definitions=new HubDefinitionStore(tasks);var native=new HubNativeWorkflowStore(tasks,definitions);
        var hub="race-"+Guid.NewGuid().ToString("N");
        await definitions.RegisterAsync(new(hub,"native",2,4,1,JsonSerializer.SerializeToElement(new{executionMode="native"})),default);
        for(var iteration=0;iteration<20;iteration++)
        {
            var id=await new HubRemoteWorkflowStore(tasks,definitions).StartAsync(new(hub,"native",2,JsonSerializer.SerializeToElement(new{})),default);
            async Task<HubLeasedTask> Claim()=>Assert.Single(await tasks.ClaimAsync("worker",[hub],[new(hub,"__native_workflow__",1)],1,TimeSpan.FromSeconds(30),default));
            var lease=await Claim();
            var commit=new HubNativeCommit(1,"worker",lease.Epoch,0,lease.Id,JsonSerializer.SerializeToElement(new{}),"Runnable",false,null,
                [],[new("pointer","event",iteration.ToString(),DateTime.UnixEpoch)],[],[]);
            var pendingCommit=native.CommitAsync(id,commit,default);
            var publication=native.PublishEventAsync(hub,new("event",iteration.ToString(),JsonSerializer.SerializeToElement(iteration),iteration.ToString()),default);
            await Task.WhenAll(pendingCommit,publication);
            Assert.NotNull(await pendingCommit);
            var runtime=(await native.GetRuntimeAsync(id,default))!;
            Assert.Equal(iteration,Assert.Single(runtime.Inbox).Payload.GetInt32());
            var awakened=await Claim();
            Assert.NotNull(await native.CommitAsync(id,commit with{TaskId=awakened.Id,Epoch=awakened.Epoch,ExpectedRevision=1,
                Status="Complete",Subscriptions=[],ConsumedInbox=runtime.Inbox.Select(i=>i.Id).ToArray()},default));
        }
    }

    [Fact]
    public async Task CompetingNativeTimerScannersFireOnceAndReplayKeepsOriginalAcknowledgement()
    {
        var database=Environment.GetEnvironmentVariable("WFC_TEST_PG");if(database is null)return;
        await using var tasks=new HubTaskStore(database);
        var definitions=new HubDefinitionStore(tasks);var native=new HubNativeWorkflowStore(tasks,definitions);
        var hub="timer-"+Guid.NewGuid().ToString("N");
        await definitions.RegisterAsync(new(hub,"native",2,4,1,JsonSerializer.SerializeToElement(new{executionMode="native"})),default);
        var id=await new HubRemoteWorkflowStore(tasks,definitions).StartAsync(new(hub,"native",2,JsonSerializer.SerializeToElement(new{})),default);
        async Task<HubLeasedTask> Claim()=>Assert.Single(await tasks.ClaimAsync("worker",[hub],[new(hub,"__native_workflow__",1)],1,TimeSpan.FromSeconds(30),default));
        var lease=await Claim();
        var commit=new HubNativeCommit(1,"worker",lease.Epoch,0,lease.Id,JsonSerializer.SerializeToElement(new{}),"Runnable",false,DateTime.UnixEpoch,
            [],[],[],[],[],new("timer-pointer",0));
        Assert.NotNull(await native.CommitAsync(id,commit,default));
        var fired=await Task.WhenAll(new HubTimerStore(tasks).FireDueAsync(100,default),new HubTimerStore(tasks).FireDueAsync(100,default));
        Assert.Equal(1,fired.Sum());
        var awakened=await Claim();
        Assert.NotNull(await native.CommitAsync(id,commit with{TaskId=awakened.Id,Epoch=awakened.Epoch,ExpectedRevision=1,
            Status="Complete",WakeAt=null,Timer=null},default));
        var replay=await native.CommitAsync(id,commit,default);
        Assert.True(replay!.Replayed);Assert.Equal(1,replay.Revision);Assert.Equal("Runnable",replay.State);
    }

    [Fact]
    public async Task SuspendedDispatchResumesAndPointerCancellationFencesItsLease()
    {
        var database=Environment.GetEnvironmentVariable("WFC_TEST_PG");if(database is null)return;
        await using var tasks=new HubTaskStore(database);
        var definitions=new HubDefinitionStore(tasks);var native=new HubNativeWorkflowStore(tasks,definitions);
        var hub="suspend-"+Guid.NewGuid().ToString("N");
        await definitions.RegisterAsync(new(hub,"native",2,4,2,JsonSerializer.SerializeToElement(new{executionMode="native"})),default);
        await definitions.RegisterActivityAsync(new(hub,"act",2,4,10,10),default);
        var id=await new HubRemoteWorkflowStore(tasks,definitions).StartAsync(new(hub,"native",2,JsonSerializer.SerializeToElement(new{})),default);
        async Task<HubLeasedTask> Claim(string kind)=>Assert.Single(await tasks.ClaimAsync("worker",[hub],[new(hub,kind,kind=="act"?2:1)],1,TimeSpan.FromSeconds(30),default));
        var first=await Claim("__native_workflow__");
        var commit=new HubNativeCommit(1,"worker",first.Epoch,0,first.Id,JsonSerializer.SerializeToElement(new{}),
            "Suspended",false,null,[],[],[new("dispatch","pointer",0,"act",2,JsonSerializer.SerializeToElement(new{}))],[],["dispatch"]);
        Assert.NotNull(await native.CommitAsync(id,commit,default));
        Assert.Empty(await tasks.ClaimAsync("worker",[hub],[new(hub,"act",2)],1,TimeSpan.FromSeconds(30),default));
        Assert.True(await native.ChangeStatusAsync(id,new("Runnable","resume"),default));
        var activity=await Claim("act");
        var next=await Claim("__native_workflow__");
        var cancelled=commit with{TaskId=next.Id,Epoch=next.Epoch,ExpectedRevision=2,Status="Runnable",Activities=[],ActiveDispatches=[]};
        Assert.NotNull(await native.CommitAsync(id,cancelled,default));
        Assert.False(await tasks.HeartbeatAsync(activity.Id,"worker",activity.Epoch,TimeSpan.FromSeconds(30),default));
        Assert.False(await native.CompleteActivityAsync(activity.Id,"worker",activity.Epoch,JsonSerializer.SerializeToElement(new{}),default));
    }

    [Fact]
    public async Task RepeatedWaitSkipsConsumedEventAndFutureEventWaitsForScheduler()
    {
        var database=Environment.GetEnvironmentVariable("WFC_TEST_PG");if(database is null)return;
        await using var tasks=new HubTaskStore(database);
        var definitions=new HubDefinitionStore(tasks);var native=new HubNativeWorkflowStore(tasks,definitions);
        var hub="events-"+Guid.NewGuid().ToString("N");
        await definitions.RegisterAsync(new(hub,"native",2,4,1,JsonSerializer.SerializeToElement(new{executionMode="native"})),default);
        var id=await new HubRemoteWorkflowStore(tasks,definitions).StartAsync(new(hub,"native",2,JsonSerializer.SerializeToElement(new{})),default);
        async Task<HubLeasedTask> Claim()=>Assert.Single(await tasks.ClaimAsync("worker",[hub],[new(hub,"__native_workflow__",1)],1,TimeSpan.FromSeconds(30),default));
        var first=await Claim();
        var commit=new HubNativeCommit(1,"worker",first.Epoch,0,first.Id,JsonSerializer.SerializeToElement(new{}),"Runnable",false,null,
            [],[new("pointer","event","key",DateTime.UnixEpoch)],[],[]);
        await native.PublishEventAsync(hub,new("event","key",JsonSerializer.SerializeToElement(1),"first"),default);
        Assert.NotNull(await native.CommitAsync(id,commit,default));
        var second=await Claim();
        var before=(await native.GetRuntimeAsync(id,default))!;
        Assert.Equal(1,Assert.Single(before.Inbox).Payload.GetInt32());
        await native.PublishEventAsync(hub,new("event","key",JsonSerializer.SerializeToElement(2),"second",DateTime.UtcNow.AddSeconds(2)),default);
        Assert.NotNull(await native.CommitAsync(id,commit with{TaskId=second.Id,Epoch=second.Epoch,ExpectedRevision=1,
            ConsumedInbox=before.Inbox.Select(i=>i.Id).ToArray()},default));
        Assert.Empty((await native.GetRuntimeAsync(id,default))!.Inbox);
        for(var attempt=0;attempt<100 && (await native.GetRuntimeAsync(id,default))!.Inbox.Length==0;attempt++)
        { await Task.Delay(40);await new HubTimerStore(tasks).FireDueAsync(100,default); }
        Assert.Equal(2,Assert.Single((await native.GetRuntimeAsync(id,default))!.Inbox).Payload.GetInt32());
        await Claim();
    }

    [Fact]
    public async Task InboxSurvivesExecutionRaceAndCommitReplayIsFenced()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (database is null) return;
        await using var tasks = new HubTaskStore(database);
        var definitions = new HubDefinitionStore(tasks);
        var remote = new HubRemoteWorkflowStore(tasks, definitions);
        var native = new HubNativeWorkflowStore(tasks, definitions);
        var hub = "native-" + Guid.NewGuid().ToString("N");
        await definitions.RegisterAsync(new(hub, "native", 2, 4, 2,
            JsonSerializer.SerializeToElement(new { executionMode = "native" })), default);
        var id = await remote.StartAsync(new(hub, "native", 2,
            JsonSerializer.SerializeToElement(new { value = 1 })), default);
        async Task<HubLeasedTask> Claim() => Assert.Single(await tasks.ClaimAsync("processor", [hub],
            [new(hub, "__native_workflow__", 1)], 1, TimeSpan.FromSeconds(30), default));
        var first = await Claim();
        await native.PublishEventAsync(hub, new("approved", "tenant", JsonSerializer.SerializeToElement(42), "early"), default);
        var snapshot = JsonSerializer.SerializeToElement(new { data = "snapshot" });
        var commit = new HubNativeCommit(1, "processor", first.Epoch, 0, first.Id, snapshot,
            "Runnable", false, null, [], [new("pointer", "approved", "tenant", DateTime.UnixEpoch)], [], []);
        Assert.NotNull(await native.CommitAsync(id, commit, default));
        var replay = await native.CommitAsync(id, commit, default);
        Assert.True(replay!.Replayed);
        Assert.Null(await native.CommitAsync(id, commit with { Snapshot = JsonSerializer.SerializeToElement(new { changed = true }) }, default));
        var second = await Claim();
        var runtime = (await native.GetRuntimeAsync(id, default))!;
        Assert.Equal(42, Assert.Single(runtime.Inbox).Payload.GetInt32());
        // A result/event arriving after the runtime read cannot be erased by its commit.
        await native.PublishEventAsync(hub, new("other", "tenant", JsonSerializer.SerializeToElement(99), "during"), default);
        var finish = commit with { TaskId = second.Id, Epoch = second.Epoch, ExpectedRevision = 1,
            ConsumedInbox = runtime.Inbox.Select(x => x.Id).ToArray(),
            Subscriptions = [new("other-pointer", "other", "tenant", DateTime.UnixEpoch)] };
        Assert.NotNull(await native.CommitAsync(id, finish, default));
        Assert.Equal(99, Assert.Single((await native.GetRuntimeAsync(id, default))!.Inbox).Payload.GetInt32());
        Assert.Null(await native.CommitAsync(id, finish with { TaskId = first.Id, Epoch = first.Epoch }, default));
    }
}

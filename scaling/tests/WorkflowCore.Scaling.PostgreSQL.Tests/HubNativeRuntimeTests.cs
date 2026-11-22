using System.Text.Json;
using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubNativeRuntimeTests
{
    [Theory]
    [InlineData("pointer")]
    [InlineData("batch")]
    [InlineData("complete")]
    [InlineData("terminate")]
    public async Task CancellationClosesOnlyTheWorkflowsOpenAttempts(string mode)
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (database is null) return;
        await using var tasks = new HubTaskStore(database);
        var definitions = new HubDefinitionStore(tasks);
        var native = new HubNativeWorkflowStore(tasks, definitions);
        var remote = new HubRemoteWorkflowStore(tasks, definitions);
        var empty = JsonSerializer.SerializeToElement(new { });

        async Task<(string Hub, Guid Id, HubNativeCommit Commit, HubLeasedTask Finished,
            HubLeasedTask Pending, HubLeasedTask AlsoPending, HubLeasedTask Retained)> Start()
        {
            var hub = "cancel-attempt-" + Guid.NewGuid().ToString("N");
            await definitions.RegisterAsync(new(hub, "native", 2, 4, 2,
                JsonSerializer.SerializeToElement(new { executionMode = "native" })), default);
            await definitions.RegisterActivityAsync(new(hub, "act", 2, 4), default);
            var id = await remote.StartAsync(new(hub, "native", 2, empty), default);
            var workflow = Assert.Single(await tasks.ClaimAsync("worker", [hub],
                [new(hub, "__native_workflow__", 1)], 1, TimeSpan.FromMinutes(5), default));
            var keys = new[] { "finished", "pending", "also-pending", "retained" };
            var dispatches = keys.Select((key, index) => new HubNativeDispatch(key, key + "-pointer", index,
                "act", 2, JsonSerializer.SerializeToElement(new { key }))).ToArray();
            var commit = new HubNativeCommit(1, "worker", workflow.Epoch, 0, workflow.Id, empty,
                "Runnable", false, null, [], [], dispatches, [], keys);
            Assert.NotNull(await native.CommitAsync(id, commit, default));
            var activities = await tasks.ClaimAsync("worker", [hub], [new(hub, "act", 2)],
                4, TimeSpan.FromMinutes(5), default);
            Assert.Equal(4, activities.Count);
            var byKey = activities.ToDictionary(activity => activity.Payload.GetProperty("key").GetString()!);
            var finished = byKey["finished"];
            Assert.True(await native.CompleteActivityAsync(finished.Id, "worker", finished.Epoch, empty, default));
            return (hub, id, commit, finished, byKey["pending"], byKey["also-pending"], byKey["retained"]);
        }

        await using var db = new NpgsqlConnection(database);
        await db.OpenAsync();
        async Task<(string Outcome, DateTime? FinishedAt)> Attempt(HubLeasedTask task)
        {
            await using var command = new NpgsqlCommand("""
                SELECT outcome,finished_at FROM wfc_scale.hub_task_attempt
                WHERE task_id=@task AND epoch=@epoch
                """, db);
            command.Parameters.AddWithValue("task", task.Id);
            command.Parameters.AddWithValue("epoch", task.Epoch);
            await using var reader = await command.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            return (reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetDateTime(1));
        }

        var target = await Start();
        var other = await Start();
        var finishedBefore = await Attempt(target.Finished);
        var otherFinishedBefore = await Attempt(other.Finished);
        var otherPendingBefore = await Attempt(other.Pending);
        Assert.Equal("Completed", finishedBefore.Outcome);
        Assert.Equal(("Leased", (DateTime?)null), await Attempt(target.Pending));
        var pending = target.Pending;

        if (mode == "terminate")
        {
            Assert.True(await native.ChangeStatusAsync(target.Id, new("Terminated", "terminate"), default));
            Assert.True(await native.ChangeStatusAsync(target.Id, new("Terminated", "terminate"), default));
        }
        else
        {
            var workflow = Assert.Single(await tasks.ClaimAsync("worker", [target.Hub],
                [new(target.Hub, "__native_workflow__", 1)], 1, TimeSpan.FromMinutes(5), default));
            var runtime = (await native.GetRuntimeAsync(target.Id, default))!;
            var commit = target.Commit with
            {
                TaskId = workflow.Id, Epoch = workflow.Epoch, ExpectedRevision = 1,
                Status = mode == "complete" ? "Complete" : "Runnable", Activities = [],
                ActiveDispatches = mode switch { "pointer" => [], "batch" => ["retained"], _ => null },
                ObsoleteDispatches = mode == "batch" ? ["finished", "pending", "also-pending"] : null,
                ConsumedInbox = runtime.Inbox.Select(i => i.Id).ToArray()
            };
            if (mode == "batch")
            {
                // Let an in-flight admission commit a new attempt while cancellation waits.
                await using var admission = new NpgsqlConnection(database);
                await admission.OpenAsync();
                await using var claim = await admission.BeginTransactionAsync();
                await using (var reLease = new NpgsqlCommand("""
                    UPDATE wfc_scale.hub_task SET epoch=epoch+1,attempts=attempts+1 WHERE id=@id;
                    UPDATE wfc_scale.hub_task_attempt SET outcome='LeaseExpired',finished_at=clock_timestamp()
                    WHERE task_id=@id AND finished_at IS NULL;
                    INSERT INTO wfc_scale.hub_task_attempt(task_id,epoch,workflow_id,step_index,attempt_number,worker_id)
                    SELECT id,epoch,workflow_id,native_step_id,attempts,owner FROM wfc_scale.hub_task WHERE id=@id;
                    """, admission, claim))
                {
                    reLease.Parameters.AddWithValue("id", pending.Id);
                    await reLease.ExecuteNonQueryAsync();
                }
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
                var committing = native.CommitAsync(target.Id, commit, timeout.Token);
                await using (var blocked = new NpgsqlCommand("""
                    SELECT EXISTS(SELECT 1 FROM pg_stat_activity
                        WHERE wait_event_type='Lock' AND @blocker=ANY(pg_blocking_pids(pid)))
                    """, db))
                {
                    blocked.Parameters.AddWithValue("blocker", admission.ProcessID);
                    while (!(bool)(await blocked.ExecuteScalarAsync(timeout.Token))!)
                        await Task.Delay(10, timeout.Token);
                }
                await claim.CommitAsync(timeout.Token);
                Assert.NotNull(await committing);
                pending = pending with { Epoch = pending.Epoch + 1 };
                Assert.Equal("LeaseExpired", (await Attempt(target.Pending)).Outcome);
                // Older replicas compare receipts with the original commit shape.
                Assert.True((await native.CommitAsync(target.Id, commit with { ObsoleteDispatches = null }, default))!.Replayed);
                Assert.Equal(("Leased", (DateTime?)null), await Attempt(target.Retained));
                Assert.True(await tasks.HeartbeatAsync(target.Retained.Id, "worker", target.Retained.Epoch, TimeSpan.FromSeconds(30), default));
            }
            else
                Assert.NotNull(await native.CommitAsync(target.Id, commit, default));
            Assert.True((await native.CommitAsync(target.Id, commit, default))!.Replayed);
        }

        var cancelled = await Attempt(pending);
        Assert.Equal("Cancelled", cancelled.Outcome);
        Assert.NotNull(cancelled.FinishedAt);
        var alsoCancelled = await Attempt(target.AlsoPending);
        Assert.Equal("Cancelled", alsoCancelled.Outcome);
        Assert.NotNull(alsoCancelled.FinishedAt);
        Assert.Equal(finishedBefore, await Attempt(target.Finished));
        Assert.Equal(otherFinishedBefore, await Attempt(other.Finished));
        Assert.Equal(otherPendingBefore, await Attempt(other.Pending));
        Assert.False(await tasks.HeartbeatAsync(pending.Id, "worker", pending.Epoch, TimeSpan.FromSeconds(30), default));
        Assert.False(await native.CompleteActivityAsync(pending.Id, "worker", pending.Epoch, empty, default));
        Assert.True(await tasks.HeartbeatAsync(other.Pending.Id, "worker", other.Pending.Epoch, TimeSpan.FromSeconds(30), default));
    }

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
        await definitions.RegisterActivityAsync(new(hub,"act",2,4),default);
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
        var trace=Assert.Single(await new HubWorkflowStore(tasks,new HubTaskRouteRegistry(),new HubWorkflowRegistry()).TraceAsync(id,default));
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
        await definitions.RegisterActivityAsync(new(hub,"act",2,4),default);
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

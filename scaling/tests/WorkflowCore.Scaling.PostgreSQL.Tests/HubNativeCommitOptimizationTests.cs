using System.Text.Json;
using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubNativeCommitOptimizationTests
{
    [Fact]
    public async Task NativeDelayedDispatchPersistsFinalDueFieldsWithoutEarlyReadyHint()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (database is null) return;
        await using var tasks=new HubTaskStore(database);
        var definitions=new HubDefinitionStore(tasks);
        var native=new HubNativeWorkflowStore(tasks,definitions);
        var hub="native-due-"+Guid.NewGuid().ToString("N");
        await definitions.RegisterAsync(new(hub,"native",1,4,1,
            JsonSerializer.SerializeToElement(new{executionMode="native"})),default);
        await definitions.RegisterActivityAsync(new(hub,"delayed",1,4),default);
        await definitions.RegisterActivityAsync(new(hub,"immediate",1,4),default);
        var id=await new HubRemoteWorkflowStore(tasks,definitions).StartAsync(
            new(hub,"native",1,JsonSerializer.SerializeToElement(new{})),default);
        var lease=Assert.Single(await tasks.ClaimAsync("workflow-worker",[hub],
            [new(hub,"__native_workflow__",1)],1,TimeSpan.FromSeconds(30),default));

        await using var listener=new NpgsqlConnection(database);
        await listener.OpenAsync();
        var notifications=new List<NpgsqlNotificationEventArgs>();
        listener.Notification+=(_,notification)=>notifications.Add(notification);
        await using(var listen=new NpgsqlCommand("LISTEN wfc_hub_matching_ready",listener))
            await listen.ExecuteNonQueryAsync();
        var due=DateTime.UtcNow.AddSeconds(2);
        var dispatch=new HubNativeDispatch("delayed-key","pointer-7",7,"delayed",1,
            JsonSerializer.SerializeToElement(new{value=42}),due);
        var immediate=new HubNativeDispatch("immediate-key","pointer-8",8,"immediate",1,
            JsonSerializer.SerializeToElement(new{value=43}));
        var commit=new HubNativeCommit(1,"workflow-worker",lease.Epoch,0,lease.Id,
            JsonSerializer.SerializeToElement(new{}),"Runnable",false,null,[],[],[dispatch,immediate],[]);
        Assert.NotNull(await native.CommitWithRoutingAsync(id,commit,default));
        using(var timeout=new CancellationTokenSource(TimeSpan.FromMilliseconds(300)))
        {
            try { await listener.WaitAsync(timeout.Token); }
            catch(OperationCanceledException) { }
        }
        // The signal no longer contains task types. Exactly one notification
        // proves that the immediate dispatch woke the bucket, while the
        // future-due dispatch did not emit an early second notification.
        var notification = Assert.Single(notifications, IsForThisHub);
        using (var signal = JsonDocument.Parse(notification.Payload))
        {
            Assert.Equal(hub,signal.RootElement.GetProperty("hub").GetString());
            Assert.Equal("signal",signal.RootElement.GetProperty("kind").GetString());
            Assert.Equal(lease.PartitionId % 64,signal.RootElement.GetProperty("partition").GetInt32());
            Assert.False(signal.RootElement.TryGetProperty("taskId",out _));
        }

        bool IsForThisHub(NpgsqlNotificationEventArgs item)
        {
            using var signal = JsonDocument.Parse(item.Payload);
            return signal.RootElement.GetProperty("hub").GetString() == hub;
        }

        Guid activityTaskId;
        await using(var db=new NpgsqlConnection(database))
        {
            await db.OpenAsync();
            await using(var taskId=new NpgsqlCommand("SELECT id FROM wfc_scale.hub_task WHERE workflow_id=@id AND dispatch_key=@key",db))
            {
                taskId.Parameters.AddWithValue("id",id); taskId.Parameters.AddWithValue("key",dispatch.DispatchKey);
                activityTaskId=(Guid)(await taskId.ExecuteScalarAsync() ?? throw new InvalidOperationException("Native activity task was not persisted"));
            }
            await using var saved=new NpgsqlCommand("""
                SELECT execution_pointer_id,dispatch_key,native_step_id,workflow_step,due_at
                FROM wfc_scale.hub_task WHERE workflow_id=@id AND dispatch_key=@key
                """,db);
            saved.Parameters.AddWithValue("id",id); saved.Parameters.AddWithValue("key",dispatch.DispatchKey);
            await using var reader=await saved.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(dispatch.PointerId,reader.GetString(0));
            Assert.Equal(dispatch.DispatchKey,reader.GetString(1));
            Assert.Equal(dispatch.StepId,reader.GetInt32(2));
            Assert.True(reader.IsDBNull(3));
            Assert.InRange(Math.Abs((reader.GetDateTime(4)-due).TotalMilliseconds),0,20);
        }
        await using(var db=new NpgsqlConnection(database))
        {
            await db.OpenAsync();
            await using var saved=new NpgsqlCommand("""
                SELECT execution_pointer_id,dispatch_key,native_step_id,workflow_step,due_at<=clock_timestamp()
                FROM wfc_scale.hub_task WHERE workflow_id=@id AND dispatch_key=@key
                """,db);
            saved.Parameters.AddWithValue("id",id); saved.Parameters.AddWithValue("key",immediate.DispatchKey);
            await using var reader=await saved.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(immediate.PointerId,reader.GetString(0));
            Assert.Equal(immediate.DispatchKey,reader.GetString(1));
            Assert.Equal(immediate.StepId,reader.GetInt32(2));
            Assert.True(reader.IsDBNull(3));
            Assert.True(reader.GetBoolean(4));
        }

        var candidates=new HubCandidateReader(tasks);
        var partition=lease.PartitionId;
        var horizon=await candidates.CaptureHorizonAsync(hub, 0, 1, default);
        var before=await candidates.ReadPageAsync(hub,partition,HubCandidateStream.Ready,horizon,null,32,65536,default);
        Assert.DoesNotContain(before.Items,item=>item.Id==activityTaskId);
        Assert.Empty(await tasks.ClaimAsync("activity-worker",[hub],[new(hub,"delayed",1)],1,
            TimeSpan.FromSeconds(30),default));
        await Task.Delay(TimeSpan.FromMilliseconds(Math.Max(0,(due-DateTime.UtcNow).TotalMilliseconds)+1500));
        horizon=await candidates.CaptureHorizonAsync(hub, 0, 1, default);
        var after=await candidates.ReadPageAsync(hub,partition,HubCandidateStream.Ready,horizon,null,32,65536,default);
        Assert.Contains(after.Items,item=>item.Id==activityTaskId);
        Assert.Single(await tasks.ClaimAsync("activity-worker",[hub],[new(hub,"delayed",1)],1,
            TimeSpan.FromSeconds(30),default));
    }



    [Fact]
    public async Task DispatchBatchPreservesVersionsAndPayloadsAndRollsBackMissingRoute()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        await using var tasks = new HubTaskStore(database);
        var definitions = new HubDefinitionStore(tasks);
        var native = new HubNativeWorkflowStore(tasks, definitions);
        var hub = "native-routes-" + Guid.NewGuid().ToString("N");
        await definitions.RegisterAsync(new(hub, "native", 1, 4, 1,
            JsonSerializer.SerializeToElement(new { executionMode = "native" })), default);
        var registration = new HubActivityRegistration(hub, "act", 1, 4);
        Assert.True(await definitions.RegisterActivityAsync(registration, default));
        var id = await new HubRemoteWorkflowStore(tasks, definitions).StartAsync(
            new(hub, "native", 1, JsonSerializer.SerializeToElement(new { })), default);
        var lease = Assert.Single(await tasks.ClaimAsync("worker", [hub],
            [new(hub, "__native_workflow__", 1)], 1, TimeSpan.FromSeconds(30), default));
        HubNativeDispatch Dispatch(string key, int version, int input) =>
            new(key, key, 0, "act", version, JsonSerializer.SerializeToElement(new { input }));
        var activities = new[] { Dispatch("a", 1, 42), Dispatch("b", 2, 84) };
        var commit = new HubNativeCommit(1, "worker", lease.Epoch, 0, lease.Id,
            JsonSerializer.SerializeToElement(new { }), "Runnable", true, null, [], [], activities, []);

        await using var db = new NpgsqlConnection(database);
        await db.OpenAsync();
        await using var check = new NpgsqlCommand("""
            SELECT dispatch_key, task_type, task_version, partition_id, (payload->>'input')::int
            FROM wfc_scale.hub_task WHERE workflow_id=@id AND dispatch_key IS NOT NULL
            ORDER BY dispatch_key
            """, db);
        check.Parameters.AddWithValue("id", id);

        await Assert.ThrowsAsync<KeyNotFoundException>(() => native.CommitWithRoutingAsync(id, commit, default));
        await using (var rows = await check.ExecuteReaderAsync())
            Assert.False(await rows.ReadAsync());

        Assert.True(await definitions.RegisterActivityAsync(registration with { Version = 2 }, default));
        var outcome = await native.CommitWithRoutingAsync(id, commit, default);
        Assert.NotNull(outcome);
        Assert.Equal(hub, outcome.HubId);
        Assert.Equal(new HubTransitionResult(id, 1, "Runnable", false), outcome.Result);
        Assert.Equal(outcome.Result with { Replayed = true },
            (await native.CommitWithRoutingAsync(id, commit, default))!.Result);
        Assert.Null(await native.CommitWithRoutingAsync(id, commit with { Epoch = lease.Epoch + 1 }, default));

        await using var dispatched = await check.ExecuteReaderAsync();
        foreach (var activity in activities)
        {
            Assert.True(await dispatched.ReadAsync());
            Assert.Equal(activity.DispatchKey, dispatched.GetString(0));
            Assert.Equal(activity.ActivityType, dispatched.GetString(1));
            Assert.Equal(activity.Version, dispatched.GetInt32(2));
            Assert.Equal(lease.PartitionId, dispatched.GetInt32(3));
            Assert.Equal(activity.Input.GetProperty("input").GetInt32(), dispatched.GetInt32(4));
        }
        Assert.False(await dispatched.ReadAsync());
    }
}

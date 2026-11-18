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
        await using var fixture=await LegacyDatabaseFixture.CreateAsync(database);
        database=fixture.ConnectionString;
        await using var tasks=new HubTaskStore(database);
        var definitions=new HubDefinitionStore(tasks);
        var native=new HubNativeWorkflowStore(tasks,definitions);
        var hub="native-due-"+Guid.NewGuid().ToString("N");
        await definitions.RegisterAsync(new(hub,"native",1,4,1,
            JsonSerializer.SerializeToElement(new{executionMode="native"})),default);
        await fixture.SeedRegistrationAsync(new(hub,"delayed",1,4,10,10));
        await fixture.SeedRegistrationAsync(new(hub,"immediate",1,4,10,10));
        await fixture.ApplyAsync(33);
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
        Assert.Contains(notifications,item=>item.Payload.Contains("immediate",StringComparison.Ordinal));
        Assert.DoesNotContain(notifications,item=>item.Payload.Contains("delayed",StringComparison.Ordinal));

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
        var horizon=await candidates.CaptureHorizonAsync(hub,default);
        var before=await candidates.ReadPageAsync(hub,partition,HubCandidateStream.Ready,horizon,null,32,65536,default);
        Assert.DoesNotContain(before.Items,item=>item.Id==activityTaskId);
        Assert.Empty(await tasks.ClaimAsync("activity-worker",[hub],[new(hub,"delayed",1)],1,
            TimeSpan.FromSeconds(30),default));
        await Task.Delay(TimeSpan.FromMilliseconds(Math.Max(0,(due-DateTime.UtcNow).TotalMilliseconds)+1500));
        horizon=await candidates.CaptureHorizonAsync(hub,default);
        var after=await candidates.ReadPageAsync(hub,partition,HubCandidateStream.Ready,horizon,null,32,65536,default);
        Assert.Contains(after.Items,item=>item.Id==activityTaskId);
        Assert.Single(await tasks.ClaimAsync("activity-worker",[hub],[new(hub,"delayed",1)],1,
            TimeSpan.FromSeconds(30),default));
        fixture.MarkPassed();
    }

    [Fact]
    public async Task RetiredDispatchTerminatesFencedNativeWorkflowWithoutApplyingProposedEffects()
    {
        var database=Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub="retired-native-"+Guid.NewGuid().ToString("N");
        await using var tasks=new HubTaskStore(database);
        var definitions=new HubDefinitionStore(tasks);
        var native=new HubNativeWorkflowStore(tasks,definitions);
        await definitions.RegisterAsync(new(hub,"native",1,4,1,JsonSerializer.SerializeToElement(new{executionMode="native"})),default);
        await using var db=new NpgsqlConnection(database); await db.OpenAsync();
        await using(var seed=new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_activity_registration
              (hub_id,activity_type,version,partitions,hub_concurrency,activity_concurrency,resource_name,resource_field,execution_policy)
            VALUES (@hub,'old',1,4,10,10,'tenant','tenantId','KeyedFifo')
            """,db)) { seed.Parameters.AddWithValue("hub",hub); await seed.ExecuteNonQueryAsync(); }
        var id=await new HubRemoteWorkflowStore(tasks,definitions).StartAsync(new(hub,"native",1,JsonSerializer.SerializeToElement(new{})),default);
        var lease=Assert.Single(await tasks.ClaimAsync("worker",[hub],[new(hub,"__native_workflow__",1)],1,TimeSpan.FromSeconds(30),default));
        await using(var seed=new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_native_subscription(workflow_id,pointer_id,event_name,event_key,as_of)
              VALUES (@id,'existing','event','key',now());
            INSERT INTO wfc_scale.hub_timer(id,workflow_id,hub_id,partition_id,step_index,timer_key,generation,due_at,scheduled_revision)
              SELECT gen_random_uuid(),id,hub_id,partition_id,0,'old',1,now()+interval '1 hour',0 FROM wfc_scale.hub_workflow WHERE id=@id
            """,db)) { seed.Parameters.AddWithValue("id",id); await seed.ExecuteNonQueryAsync(); }
        var dispatch=new HubNativeDispatch("proposed","pointer",0,"old",1,JsonSerializer.SerializeToElement(new{tenantId="secret"}));
        var commit=new HubNativeCommit(1,"worker",lease.Epoch,0,lease.Id,JsonSerializer.SerializeToElement(new{proposed=true}),
            "Runnable",false,null,[],[],[dispatch],[]);
        Assert.Null(await native.CommitWithRoutingAsync(id,commit with { Epoch=lease.Epoch+1 },default));
        Assert.Null(await native.CommitWithRoutingAsync(id,commit with { ExpectedRevision=1 },default));
        var result=(await native.CommitWithRoutingAsync(id,commit,default))!.Result;
        Assert.True(result.PolicyRejected); Assert.Equal("Terminated",result.State); Assert.Equal(1,result.Revision);
        var replay=(await native.CommitWithRoutingAsync(id,commit,default))!.Result;
        Assert.Equal(result with { Replayed=true },replay);
        Assert.Null(await native.CommitWithRoutingAsync(id,commit with { Snapshot=JsonSerializer.SerializeToElement(new{different=true}) },default));
        await using var check=new NpgsqlCommand("""
            SELECT w.state,w.native_status,w.last_error,w.native_snapshot IS NULL,
              (SELECT count(*) FROM wfc_scale.hub_task t WHERE t.workflow_id=w.id AND t.dispatch_key IS NOT NULL),
              (SELECT count(*) FROM wfc_scale.hub_task t WHERE t.workflow_id=w.id AND t.state IN (0,1)),
              (SELECT count(*) FROM wfc_scale.hub_native_subscription s WHERE s.workflow_id=w.id),
              (SELECT count(*) FROM wfc_scale.hub_timer tm WHERE tm.workflow_id=w.id AND tm.state=0),
              (SELECT count(*) FROM wfc_scale.hub_workflow_event e WHERE e.workflow_id=w.id AND e.event_type='WorkflowPolicyRejected')
            FROM wfc_scale.hub_workflow w WHERE w.id=@id
            """,db);
        check.Parameters.AddWithValue("id",id); await using var reader=await check.ExecuteReaderAsync(); Assert.True(await reader.ReadAsync());
        Assert.Equal((short)3,reader.GetInt16(0)); Assert.Equal("Terminated",reader.GetString(1));
        Assert.Contains("retired",reader.GetString(2)); Assert.DoesNotContain("secret",reader.GetString(2)); Assert.True(reader.GetBoolean(3));
        for(var i=4;i<=7;i++) Assert.Equal(0L,reader.GetInt64(i)); Assert.Equal(1L,reader.GetInt64(8));
    }

    [Fact]
    public async Task BatchDispatchesKeepPerInputAndVersionRoutesAndRefreshOnNextCommit()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (database is null) return;
        await using var fixture=await LegacyDatabaseFixture.CreateAsync(database);
        database=fixture.ConnectionString;
        await using var tasks = new HubTaskStore(database);
        var definitions = new HubDefinitionStore(tasks);
        var native = new HubNativeWorkflowStore(tasks, definitions);
        var hub = "commit-cache-" + Guid.NewGuid().ToString("N");
        await definitions.RegisterAsync(new(hub, "native", 1, 4, 1,
            JsonSerializer.SerializeToElement(new { executionMode = "native" })), default);
        var registration = new HubActivityRegistration(hub, "act", 1, 4, 10, 10,
            "tenant", "tenantId", 2, Ordering: HubActivityOrdering.KeyedBounded);
        await fixture.SeedRegistrationAsync(registration);
        await fixture.SeedRegistrationAsync(registration with
            { Version = 2, ResourceName = "account", ResourceField = "accountId", Ordering = HubActivityOrdering.KeyedFifo });
        var id = await new HubRemoteWorkflowStore(tasks, definitions).StartAsync(
            new(hub, "native", 1, JsonSerializer.SerializeToElement(new { })), default);
        await fixture.ApplyAsync(33);
        async Task<HubLeasedTask> Claim() => Assert.Single(await tasks.ClaimAsync("worker", [hub],
            [new(hub, "__native_workflow__", 1)], 1, TimeSpan.FromSeconds(30), default));
        HubNativeDispatch Dispatch(string key, int version, string tenant, string account) =>
            new(key, key, 0, "act", version, JsonSerializer.SerializeToElement(new { tenantId = tenant, accountId = account }));
        var activities = new[] { Dispatch("a", 1, "t1", "a1"), Dispatch("b", 1, "t2", "a2"),
            Dispatch("c", 2, "t1", "a3"), Dispatch("d", 2, "t1", "a4") };
        var lease = await Claim();
        var commit = new HubNativeCommit(1, "worker", lease.Epoch, 0, lease.Id,
            JsonSerializer.SerializeToElement(new { }), "Runnable", true, null, [], [], activities, []);
        var outcome = await native.CommitWithRoutingAsync(id, commit, default);
        Assert.NotNull(outcome);
        Assert.Equal(hub, outcome.HubId);
        Assert.Equal(new HubTransitionResult(id, 1, "Runnable", false), outcome.Result);
        var replay = await native.CommitWithRoutingAsync(id, commit, default);
        Assert.NotNull(replay);
        Assert.Equal(hub, replay.HubId);
        Assert.Equal(outcome.Result with { Replayed = true }, replay.Result);
        Assert.Null(await native.CommitWithRoutingAsync(id, commit with { Epoch = lease.Epoch + 1 }, default));

        async Task AssertRoutes(IEnumerable<HubNativeDispatch> dispatches)
        {
            // The persisted workflow partition is stable across commits.
            var workflow = (await new HubWorkflowStore(tasks, new HubPolicyRegistry(), new HubWorkflowRegistry()).GetAsync(id, default))!;
            foreach (var dispatch in dispatches)
            {
                var route = (await definitions.ResolveAsync(hub, dispatch.ActivityType, dispatch.Version, dispatch.Input, default))!;
                await using var db = new NpgsqlConnection(database);
                await db.OpenAsync();
                await using var cmd = new NpgsqlCommand("""
                    SELECT c.resource_key,r.capacity,r.strict_fifo FROM wfc_scale.hub_task t
                    JOIN wfc_scale.hub_task_constraint c ON c.task_id=t.id
                    JOIN wfc_scale.hub_resource r ON r.resource_key=c.resource_key
                    WHERE t.workflow_id=@id AND t.dispatch_key=@key ORDER BY c.resource_key
                    """, db);
                cmd.Parameters.AddWithValue("id", id);
                cmd.Parameters.AddWithValue("key", dispatch.DispatchKey);
                var actual = new List<HubResourceRule>();
                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync()) actual.Add(new(reader.GetString(0), reader.GetInt32(1), reader.GetBoolean(2)));
                Assert.Equal(route.ResourcesForPartition(workflow.PartitionId), actual);
            }
        }
        await AssertRoutes(activities);

        // Capacity changes remain supported; selector drift is frozen for drain.
        var policy = (await definitions.GetActivityPolicyAsync(hub, "act", 1, default))!;
        await definitions.UpdateActivityCapacityAsync(new(hub, "act", 1, policy.Revision, 10, 10, 3), default);
        await Assert.ThrowsAsync<InvalidOperationException>(() => definitions.RegisterActivityAsync(registration with { ResourceField = "accountId" }, default));
        var next = await Claim();
        var fresh = new[] { Dispatch("e", 1, "unused", "a5"), Dispatch("f", 1, "unused", "a6") };
        Assert.NotNull(await native.CommitWithRoutingAsync(id, commit with
            { TaskId = next.Id, Epoch = next.Epoch, ExpectedRevision = 1, Activities = fresh, Runnable = false }, default));
        await AssertRoutes(fresh);
        fixture.MarkPassed();
    }

    [Fact]
    public async Task ReusedRegistrationStillValidatesEachInputAndRollsBackInvalidBatch()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (database is null) return;
        await using var fixture=await LegacyDatabaseFixture.CreateAsync(database);
        database=fixture.ConnectionString;
        await using var tasks = new HubTaskStore(database);
        var definitions = new HubDefinitionStore(tasks);
        var native = new HubNativeWorkflowStore(tasks, definitions);
        var hub = "commit-input-" + Guid.NewGuid().ToString("N");
        await definitions.RegisterAsync(new(hub, "native", 1, 4, 1,
            JsonSerializer.SerializeToElement(new { executionMode = "native" })), default);
        await fixture.SeedRegistrationAsync(new(hub, "act", 1, 4, 10, 10,
            "tenant", "tenantId", Ordering: HubActivityOrdering.KeyedExclusive));
        var id = await new HubRemoteWorkflowStore(tasks, definitions).StartAsync(
            new(hub, "native", 1, JsonSerializer.SerializeToElement(new { })), default);
        await fixture.ApplyAsync(33);
        var lease = Assert.Single(await tasks.ClaimAsync("worker", [hub], [new(hub, "__native_workflow__", 1)],
            1, TimeSpan.FromSeconds(30), default));
        var first = new HubNativeDispatch("a", "a", 0, "act", 1, JsonSerializer.SerializeToElement(new { tenantId = "t1" }));
        var second = first with { DispatchKey = "b", PointerId = "b", Input = JsonSerializer.SerializeToElement(new { }) };
        var commit = new HubNativeCommit(1, "worker", lease.Epoch, 0, lease.Id,
            JsonSerializer.SerializeToElement(new { }), "Runnable", false, null, [], [], [first, second], []);
        await Assert.ThrowsAsync<ArgumentException>(() => native.CommitWithRoutingAsync(id, commit, default));
        Assert.Equal(0, (await native.GetRuntimeAsync(id, default))!.Revision);
        await using var db = new NpgsqlConnection(database);
        await db.OpenAsync();
        await using var count = new NpgsqlCommand("SELECT count(*) FROM wfc_scale.hub_task WHERE workflow_id=@id AND dispatch_key IS NOT NULL", db);
        count.Parameters.AddWithValue("id", id);
        Assert.Equal(0L, await count.ExecuteScalarAsync());
        Assert.NotNull(await native.CommitWithRoutingAsync(id, commit with
            { Activities = [first, second with { Input = JsonSerializer.SerializeToElement(new { tenantId = "t2" }) }] }, default));
        fixture.MarkPassed();
    }
}

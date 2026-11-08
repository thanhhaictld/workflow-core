using System.Text.Json;
using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubNativeCommitOptimizationTests
{
    [Fact]
    public async Task BatchDispatchesKeepPerInputAndVersionRoutesAndRefreshOnNextCommit()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (database is null) return;
        await using var tasks = new HubTaskStore(database);
        var definitions = new HubDefinitionStore(tasks);
        var native = new HubNativeWorkflowStore(tasks, definitions);
        var hub = "commit-cache-" + Guid.NewGuid().ToString("N");
        await definitions.RegisterAsync(new(hub, "native", 1, 4, 1,
            JsonSerializer.SerializeToElement(new { executionMode = "native" })), default);
        var registration = new HubActivityRegistration(hub, "act", 1, 4, 10, 10,
            "tenant", "tenantId", 2, Ordering: HubActivityOrdering.KeyedBounded);
        await definitions.RegisterActivityAsync(registration, default);
        await definitions.RegisterActivityAsync(registration with
            { Version = 2, ResourceName = "account", ResourceField = "accountId", Ordering = HubActivityOrdering.KeyedFifo }, default);
        var id = await new HubRemoteWorkflowStore(tasks, definitions).StartAsync(
            new(hub, "native", 1, JsonSerializer.SerializeToElement(new { })), default);
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

        // A later transaction must observe both policy and selector updates.
        var policy = (await definitions.GetActivityPolicyAsync(hub, "act", 1, default))!;
        await definitions.UpdateActivityCapacityAsync(new(hub, "act", 1, policy.Revision, 10, 10, 3), default);
        await definitions.RegisterActivityAsync(registration with { ResourceField = "accountId" }, default);
        var next = await Claim();
        var fresh = new[] { Dispatch("e", 1, "unused", "a5"), Dispatch("f", 1, "unused", "a6") };
        Assert.NotNull(await native.CommitWithRoutingAsync(id, commit with
            { TaskId = next.Id, Epoch = next.Epoch, ExpectedRevision = 1, Activities = fresh, Runnable = false }, default));
        await AssertRoutes(fresh);
    }

    [Fact]
    public async Task ReusedRegistrationStillValidatesEachInputAndRollsBackInvalidBatch()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (database is null) return;
        await using var tasks = new HubTaskStore(database);
        var definitions = new HubDefinitionStore(tasks);
        var native = new HubNativeWorkflowStore(tasks, definitions);
        var hub = "commit-input-" + Guid.NewGuid().ToString("N");
        await definitions.RegisterAsync(new(hub, "native", 1, 4, 1,
            JsonSerializer.SerializeToElement(new { executionMode = "native" })), default);
        await definitions.RegisterActivityAsync(new(hub, "act", 1, 4, 10, 10,
            "tenant", "tenantId", Ordering: HubActivityOrdering.KeyedExclusive), default);
        var id = await new HubRemoteWorkflowStore(tasks, definitions).StartAsync(
            new(hub, "native", 1, JsonSerializer.SerializeToElement(new { })), default);
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
    }
}

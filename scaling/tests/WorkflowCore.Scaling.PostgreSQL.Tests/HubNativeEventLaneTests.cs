using System.Text.Json;
using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

/// <summary>
/// PostgreSQL-backed race tests for conditional native event-lane locking.
/// The unrelated Hub lock is deliberately held by an external transaction.
/// Event-free commits must not block; subscription mutations still must
/// synchronize with event publication, preventing the lost-wakeup race.
/// </summary>
public sealed class HubNativeEventLaneTests
{
    private static readonly JsonElement Empty = JsonSerializer.SerializeToElement(new { });
    private static string Hub() => "lane-" + Guid.NewGuid().ToString("N");

    private static async Task<(HubTaskStore Tasks, HubNativeWorkflowStore Native, HubRemoteWorkflowStore Remote, string Hub)> CreateAsync(string database)
    {
        var tasks = new HubTaskStore(database);
        var definitions = new HubDefinitionStore(tasks);
        var hub = Hub();
        await definitions.RegisterAsync(new(hub, "flow", 1, 8, 1,
            JsonSerializer.SerializeToElement(new { executionMode = "native" })), default);
        return (tasks, new HubNativeWorkflowStore(tasks, definitions),
            new HubRemoteWorkflowStore(tasks, definitions), hub);
    }

    private static Task<Guid> Start(HubRemoteWorkflowStore remote, string hub) =>
        remote.StartAsync(new(hub, "flow", 1, Empty), default);

    private static async Task<HubLeasedTask> Claim(HubTaskStore tasks, string hub) =>
        Assert.Single(await tasks.ClaimAsync("worker", [hub],
            [new(hub, "__native_workflow__", 1)], 1, TimeSpan.FromSeconds(30), default));

    private static async Task<NpgsqlTransaction> HoldHubEventLane(NpgsqlConnection db, string hub)
    {
        var tx = await db.BeginTransactionAsync();
        await using var cmd = new NpgsqlCommand(
            "SELECT pg_advisory_xact_lock(hashtextextended(@hub,12012))", db, tx);
        cmd.Parameters.AddWithValue("hub", hub);
        await cmd.ExecuteNonQueryAsync();
        return tx;
    }

    private static HubNativeCommit Commit(HubLeasedTask task, long revision,
        string status = "Complete", HubNativeSubscription[]? subs = null,
        long[]? consumed = null) =>
        new(1, "worker", task.Epoch, revision, task.Id, Empty,
            status, false, null, consumed ?? [],
            subs ?? [], [], [
                new("StepCompleted", "pointer-0", 0, null, [], 1)
            ]);

    [Fact]
    public async Task IndependentEventFreeWorkflowsCommitWhileHubEventLaneIsHeld()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (database is null) return;
        var setup = await CreateAsync(database);
        await using var ownedTasks = setup.Tasks;
        for (var i = 0; i < 8; i++) await Start(setup.Remote, setup.Hub);
        var leases = await setup.Tasks.ClaimAsync("worker", [setup.Hub],
            [new(setup.Hub, "__native_workflow__", 1)], 8,
            TimeSpan.FromSeconds(30), default);
        Assert.Equal(8, leases.Count);
        await using var holder = new NpgsqlConnection(database);
        await holder.OpenAsync();
        await using var held = await HoldHubEventLane(holder, setup.Hub);
        var pending = leases.Select(lease => setup.Native.CommitAsync(
            lease.WorkflowId!.Value, Commit(lease, 0), default)).ToArray();
        // A Hub-wide commit lock would deadlock this await until held is released.
        var results = await Task.WhenAll(pending).WaitAsync(TimeSpan.FromSeconds(15));
        Assert.All(results, result => Assert.NotNull(result));
        await held.CommitAsync();
        foreach (var lease in leases)
        {
            var runtime = await setup.Native.GetRuntimeAsync(lease.WorkflowId!.Value, default);
            Assert.Equal("Complete", runtime!.Status);
        }
    }

    [Fact]
    public async Task PublishingAndRegisteringSubscriptionConcurrentlyNeverLosesAnEvent()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (database is null) return;
        var setup = await CreateAsync(database);
        await using var ownedTasks = setup.Tasks;
        var workflow = await Start(setup.Remote, setup.Hub);
        var task = await Claim(setup.Tasks, setup.Hub);
        var subs = new[] { new HubNativeSubscription("pointer", "approved", "key", DateTime.UnixEpoch) };
        await using var holder = new NpgsqlConnection(database);
        await holder.OpenAsync();
        await using var held = await HoldHubEventLane(holder, setup.Hub);
        var register = setup.Native.CommitAsync(workflow,
            Commit(task, 0, "Runnable", subs), default);
        var publish = setup.Native.PublishEventAsync(setup.Hub,
            new("approved", "key", JsonSerializer.SerializeToElement(17), "event-once"), default);
        await Task.Delay(150);
        Assert.False(register.IsCompleted);
        Assert.False(publish.IsCompleted);
        await held.CommitAsync();
        Assert.NotNull(await register.WaitAsync(TimeSpan.FromSeconds(15)));
        await publish.WaitAsync(TimeSpan.FromSeconds(15));
        var runtime = (await setup.Native.GetRuntimeAsync(workflow, default))!;
        Assert.Equal(17, Assert.Single(runtime.Inbox).Payload.GetInt32());
    }

    [Fact]
    public async Task RemovingExistingSubscriptionAlsoRetainsEventLaneProtection()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (database is null) return;
        var setup = await CreateAsync(database);
        await using var ownedTasks = setup.Tasks;
        var workflow = await Start(setup.Remote, setup.Hub);
        var first = await Claim(setup.Tasks, setup.Hub);
        var subscription = new[] { new HubNativeSubscription("pointer", "approved", "key", DateTime.UnixEpoch) };
        Assert.NotNull(await setup.Native.CommitAsync(workflow,
            Commit(first, 0, "Runnable", subscription), default));
        await setup.Native.PublishEventAsync(setup.Hub,
            new("approved", "key", JsonSerializer.SerializeToElement(5), "first"), default);
        var inbox = Assert.Single((await setup.Native.GetRuntimeAsync(workflow, default))!.Inbox);
        var second = await Claim(setup.Tasks, setup.Hub);

        await using var holder = new NpgsqlConnection(database);
        await holder.OpenAsync();
        await using var held = await HoldHubEventLane(holder, setup.Hub);
        var remove = setup.Native.CommitAsync(workflow,
            Commit(second, 1, "Complete", [], [inbox.Id]), default);
        await Task.Delay(150);
        Assert.False(remove.IsCompleted); // old subscriptions require protection
        await held.CommitAsync();
        Assert.NotNull(await remove.WaitAsync(TimeSpan.FromSeconds(15)));
        Assert.Equal("Complete", (await setup.Native.GetRuntimeAsync(workflow, default))!.Status);
    }
}

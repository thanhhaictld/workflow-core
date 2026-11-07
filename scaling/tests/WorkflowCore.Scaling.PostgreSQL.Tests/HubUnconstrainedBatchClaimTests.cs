using System.Text.Json;
using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

/// <summary>
/// Opt-in batch-claim integration tests against the same PostgreSQL schema as
/// the Hub. No mock can establish row-lock, permit, or fencing correctness.
/// </summary>
public sealed class HubUnconstrainedBatchClaimTests
{
    private static string? Database => Environment.GetEnvironmentVariable("WFC_TEST_PG");
    private static string Hub() => "batch-claim-" + Guid.NewGuid().ToString("N")[..14];
    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(30);
    private static JsonElement Payload(string key) => JsonSerializer.SerializeToElement(new { resourceId = key });

    private static async Task<Guid> Enqueue(HubTaskStore store, HubPolicyRegistry policies,
        string hub, string type, string resource)
    {
        var payload = Payload(resource);
        return await store.EnqueueAsync(policies.Resolve(hub, type, 1, payload),
            payload.GetRawText(), null, null, CancellationToken.None);
    }

    private static Task<IReadOnlyList<HubLeasedTask>> Claim(
        HubTaskStore store, string hub, string owner, int max = 16,
        params string[] types) =>
        store.ClaimAsync(owner, new[] { hub },
            types.Select(type => new HubCapability(hub, type, 1)).ToArray(),
            max, Lease, CancellationToken.None);

    [Fact]
    public async Task BatchClaims_AreUniqueAcrossCompetingWorkers_AndFenced()
    {
        if (Database is null) return;
        var hub = Hub();
        var registry = new HubPolicyRegistry()
            .Register(hub, "unconstrained", 1, 16, _ => { });
        await using var store = new HubTaskStore(Database, 12, enableUnconstrainedBatchClaims: true);

        var expected = new HashSet<Guid>();
        for (var i = 0; i < 48; i++)
            expected.Add(await Enqueue(store, registry, hub, "unconstrained", i.ToString()));

        // Six independent claim calls race on one shared database; each row
        // must be leased at most once. The bulk path uses SKIP LOCKED.
        var batches = await Task.WhenAll(Enumerable.Range(0, 6)
            .Select(i => Claim(store, hub, "batch-worker-" + i, 16, "unconstrained")));
        var actual = batches.SelectMany(items => items).ToArray();
        Assert.Equal(48, actual.Length);
        Assert.True(expected.SetEquals(actual.Select(t => t.Id)));
        Assert.All(actual, t => Assert.Equal(1L, t.Epoch));

        await using (var db = new NpgsqlConnection(Database))
        {
            await db.OpenAsync();
            await using var cmd = new NpgsqlCommand("""
                SELECT count(*) FROM wfc_scale.hub_task_constraint c
                JOIN wfc_scale.hub_task t ON t.id=c.task_id
                WHERE t.hub_id=@hub
                """, db);
            cmd.Parameters.AddWithValue("hub", hub);
            Assert.Equal(0L, (long)(await cmd.ExecuteScalarAsync() ?? -1L));
        }

        foreach (var (owner, tasks) in batches.Select((tasks, index) =>
                     ("batch-worker-" + index, tasks)))
            foreach (var task in tasks)
                Assert.True(await store.CompleteAsync(task.Id, owner, task.Epoch,
                    "{\"ok\":true}", CancellationToken.None));
    }

    [Fact]
    public async Task MixedClaims_BatchOnlyUnconstrained_AndPreserveStrictFifo()
    {
        if (Database is null) return;
        var hub = Hub();
        var registry = new HubPolicyRegistry()
            .Register(hub, "free", 1, 8, _ => { })
            .Register(hub, "serialized", 1, 8,
                policy => policy.SequentialBy("resource",
                    value => value.GetProperty("resourceId").GetString()));
        await using var store = new HubTaskStore(Database, 12, enableUnconstrainedBatchClaims: true);

        var constrained = new List<Guid>();
        for (var i = 0; i < 3; i++)
            constrained.Add(await Enqueue(store, registry, hub, "serialized", "same-key"));
        var free = new List<Guid>();
        for (var i = 0; i < 8; i++)
            free.Add(await Enqueue(store, registry, hub, "free", i.ToString()));

        var leased = await Claim(store, hub, "A", 16, "serialized", "free");
        Assert.Equal(9, leased.Count);
        Assert.True(free.ToHashSet().SetEquals(leased.Where(t => t.TaskType == "free")
            .Select(t => t.Id)));
        Assert.Equal(constrained[0], Assert.Single(leased.Where(t =>
            t.TaskType == "serialized")).Id);
        Assert.Empty(await Claim(store, hub, "B", 16, "serialized", "free"));

        var first = Assert.Single(leased.Where(t => t.TaskType == "serialized"));
        Assert.True(await store.CompleteAsync(first.Id, "A", first.Epoch,
            "{\"ok\":true}", CancellationToken.None));
        var second = Assert.Single(await Claim(store, hub, "B", 16, "serialized", "free"));
        Assert.Equal(constrained[1], second.Id);
        Assert.True(await store.CompleteAsync(second.Id, "B", second.Epoch,
            "{\"ok\":true}", CancellationToken.None));
        var third = Assert.Single(await Claim(store, hub, "C", 16, "serialized", "free"));
        Assert.Equal(constrained[2], third.Id);
    }
}

using System.Text.Json;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

/// <summary>
/// Real PostgreSQL concurrency tests. Hub is the namespace, partition controls
/// placement, and optional resource permits alone control ordering/admission.
/// </summary>
public sealed class HubExplicitOrderingTests
{
    private static string NewHub() => "policy-" + Guid.NewGuid().ToString("N");
    private static JsonElement Input(string key) =>
        JsonSerializer.SerializeToElement(new { resourceId = key });

    private static async Task<(HubTaskStore Store, HubTaskRoute Route, string Hub)> SetupAsync(
        string connection, HubActivityOrdering policy, int partitions = 4,
        int capacity = 1)
    {
        var hub = NewHub();
        var store = new HubTaskStore(connection, 20);
        var defs = new HubDefinitionStore(store);
        var keyed = policy is HubActivityOrdering.KeyedExclusive or
            HubActivityOrdering.KeyedFifo or HubActivityOrdering.KeyedBounded;
        await defs.RegisterActivityAsync(new HubActivityRegistration(
            hub, "do-work", 1, partitions, 100, 100,
            keyed ? "resource" : null, keyed ? "resourceId" : null, capacity,
            false, Ordering: policy), default);
        var route = await defs.ResolveAsync(hub, "do-work", 1, Input("same"), default);
        return (store, route!, hub);
    }

    private static async Task<Guid> Enqueue(HubTaskStore store, HubTaskRoute route,
        string key, int iteration)
    {
        var input = Input(key);
        // Each task must resolve its permit from its own business payload.
        // Reusing a route already resolved for another resource would assign
        // the wrong policy key, which is invalid caller behavior.
        var actualRoute = route.Ordering is HubActivityOrdering.KeyedExclusive or
            HubActivityOrdering.KeyedFifo or HubActivityOrdering.KeyedBounded
            ? (await new HubDefinitionStore(store).ResolveAsync(
                route.HubId, route.TaskType, route.Version, input, default))!
            : route;
        return await store.EnqueueAsync(actualRoute, input.GetRawText(), null,
            "order-" + iteration, default);
    }

    private static Task<IReadOnlyList<HubLeasedTask>> Claim(HubTaskStore store, string hub,
        string worker, int count = 8) =>
        store.ClaimAsync(worker, [hub], [new HubCapability(hub, "do-work", 1)],
            count, TimeSpan.FromSeconds(60), default);

    [Fact]
    public async Task UnorderedNeverCreatesAnImplicitHubOrPartitionPermit()
    {
        var cs = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (cs is null) return;
        var setup = await SetupAsync(cs, HubActivityOrdering.Unordered, partitions: 1);
        await using var store = setup.Store;
        Assert.Empty(setup.Route.ResourcesForPartition(0));
        for (var i = 0; i < 8; i++) await Enqueue(store, setup.Route, "same", i);
        var claimed = await Claim(store, setup.Hub, "worker", 8);
        Assert.Equal(8, claimed.Count);
        Assert.All(claimed, t => Assert.Equal(0, t.PartitionId));
        foreach (var lease in claimed)
            Assert.True(await store.CompleteAsync(lease.Id, "worker", lease.Epoch, "{}", default));
    }

    [Fact]
    public async Task PartitionSerialAdmitsOnlyOnePerPartitionButDifferentPartitionsRunTogether()
    {
        var cs = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (cs is null) return;
        var setup = await SetupAsync(cs, HubActivityOrdering.PartitionSerial, partitions: 4);
        await using var store = setup.Store;
        Assert.Empty(setup.Route.Resources);
        for (var p = 0; p < 4; p++)
            Assert.Single(setup.Route.ResourcesForPartition(p));
        for (var i = 0; i < 36; i++) await Enqueue(store, setup.Route, "same", i);
        var claimed = await Claim(store, setup.Hub, "worker", 16);
        Assert.Equal(4, claimed.Count);
        Assert.Equal(4, claimed.Select(c => c.PartitionId).Distinct().Count());
        Assert.Empty(await Claim(store, setup.Hub, "other", 16));
        foreach (var task in claimed)
            Assert.True(await store.CompleteAsync(task.Id, "worker", task.Epoch, "{}", default));
        Assert.NotEmpty(await Claim(store, setup.Hub, "other", 16));
    }

    [Theory]
    [InlineData(HubActivityOrdering.KeyedExclusive)]
    [InlineData(HubActivityOrdering.KeyedFifo)]
    [InlineData(HubActivityOrdering.KeyedBounded)]
    public async Task ResourcePermitsSpanAllPartitions(HubActivityOrdering policy)
    {
        var cs = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (cs is null) return;
        var capacity = policy == HubActivityOrdering.KeyedBounded ? 2 : 1;
        var setup = await SetupAsync(cs, policy, partitions: 4, capacity);
        await using var store = setup.Store;
        for (var i = 0; i < 24; i++)
            await Enqueue(store, setup.Route, i == 23 ? "other" : "same", i);
        var first = await Claim(store, setup.Hub, "worker", 16);
        Assert.Equal(capacity + 1, first.Count);
        Assert.Equal(capacity, first.Count(t => t.Payload.GetProperty("resourceId").GetString() == "same"));
        Assert.Single(first, t => t.Payload.GetProperty("resourceId").GetString() == "other");
        Assert.Single(first, t => t.Payload.GetProperty("resourceId").GetString() == "other");
        Assert.Empty(await Claim(store, setup.Hub, "other", 16));
        foreach (var t in first)
            Assert.True(await store.CompleteAsync(t.Id, "worker", t.Epoch, "{}", default));
        Assert.NotEmpty(await Claim(store, setup.Hub, "other", 16));
    }

    [Fact]
    public async Task MatchingShardsClaimOnlyTheirOwnTaskPartitions()
    {
        var cs = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (cs is null) return;
        var setup = await SetupAsync(cs, HubActivityOrdering.Unordered, partitions: 4);
        await using var store = setup.Store;
        for (var i = 0; i < 32; i++)
            await Enqueue(store, setup.Route, "key-" + i, i);

        var all = new List<HubLeasedTask>();
        for (var shard = 0; shard < 4; shard++)
        {
            var claimed = await store.ClaimAsync(
                "worker-" + shard, [setup.Hub], [new HubCapability(setup.Hub, "do-work", 1)],
                32, TimeSpan.FromSeconds(60), default,
                matchingPartition: shard, matchingPartitionCount: 4);
            Assert.All(claimed, t => Assert.Equal(shard, t.PartitionId % 4));
            all.AddRange(claimed);
        }
        Assert.Equal(32, all.Count);
        Assert.Equal(32, all.Select(x => x.Id).Distinct().Count());
        foreach (var leased in all)
            Assert.True(await store.CompleteAsync(
                leased.Id, "worker-" + (leased.PartitionId % 4),
                leased.Epoch, "{}", default));
    }

    [Fact]
    public async Task MatchingShardsDoNotBypassCrossPartitionKeyedFifo()
    {
        var cs = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (cs is null) return;
        var setup = await SetupAsync(cs, HubActivityOrdering.KeyedFifo, partitions: 4);
        await using var store = setup.Store;
        for (var i = 0; i < 20; i++)
            await Enqueue(store, setup.Route, "same", i);

        var claimed = new List<HubLeasedTask>();
        for (var shard = 0; shard < 4; shard++)
            claimed.AddRange(await store.ClaimAsync(
                "worker-" + shard, [setup.Hub], [new HubCapability(setup.Hub, "do-work", 1)],
                16, TimeSpan.FromSeconds(60), default,
                matchingPartition: shard, matchingPartitionCount: 4));
        Assert.Single(claimed);
        var first = claimed[0];
        Assert.True(await store.CompleteAsync(
            first.Id, "worker-" + (first.PartitionId % 4), first.Epoch, "{}", default));

        var next = new List<HubLeasedTask>();
        for (var shard = 0; shard < 4; shard++)
            next.AddRange(await store.ClaimAsync(
                "next-" + shard, [setup.Hub], [new HubCapability(setup.Hub, "do-work", 1)],
                16, TimeSpan.FromSeconds(60), default,
                matchingPartition: shard, matchingPartitionCount: 4));
        Assert.Single(next);
    }

    [Fact]
    public async Task LegacyActivityPermitsStayUnchangedAfterMigration()
    {
        var cs = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (cs is null) return;
        var setup = await SetupAsync(cs, HubActivityOrdering.Legacy);
        await using var store = setup.Store;
        Assert.Equal(2, setup.Route.Resources.Count); // implicit Hub + activity limits
        Assert.Equal(HubActivityOrdering.Legacy, setup.Route.Ordering);
    }
}

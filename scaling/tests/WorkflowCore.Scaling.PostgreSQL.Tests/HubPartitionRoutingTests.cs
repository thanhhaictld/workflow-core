using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubPartitionRoutingTests
{
    [Fact]
    public async Task MatchingShardAllowsConcurrentLeasesAndExcludesOtherPartitions()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        await using var tasks = new HubTaskStore(database);
        var hub = "routing-" + Guid.NewGuid().ToString("N");
        var route = new HubTaskRoute(hub, "activity", 1, 4);
        Guid[] workflowIds = new Guid[4];
        for (var partition = 0; partition < workflowIds.Length; partition++)
        {
            do { workflowIds[partition] = Guid.NewGuid(); }
            while (HubPartitionRouter.ForWorkflow(workflowIds[partition], 4) != partition);
        }
        var first = await tasks.EnqueueAsync(route, "{}", workflowIds[1], "first", default);
        var second = await tasks.EnqueueAsync(route, "{}", workflowIds[1], "second", default);
        var other = await tasks.EnqueueAsync(route, "{}", workflowIds[2], "other", default);

        Task<IReadOnlyList<HubLeasedTask>> Claim(string worker, int partition) =>
            tasks.ClaimAsync(worker, [hub], [new(hub, "activity", 1)], 1,
                TimeSpan.FromSeconds(30), default, matchingPartition: partition, matchingPartitionCount: 4);

        var leases = await Task.WhenAll(Claim("one", 1), Claim("two", 1));
        Assert.Equal(new HashSet<Guid> { first, second }, leases.Select(x => Assert.Single(x).Id).ToHashSet());
        Assert.Equal(other, Assert.Single(await Claim("three", 2)).Id);
    }
}

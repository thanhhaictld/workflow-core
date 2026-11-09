using WorkflowCore.RemoteExecution;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

/// <summary>
/// No database required: tests the worker-side reservation invariant.
/// PostgreSQL claim/fencing/ordering is covered by the existing Hub suites.
/// </summary>
public sealed class HubShardCapacityPlannerTests
{
    [Fact]
    public void EveryPendingPollConsumesOneExecutionSlotAndOneUniqueShard()
    {
        var planner = new HubShardCapacityPlanner(capacity: 4, shards: 4,
            maxPolls: 4, initialShard: 0);
        var shards = new HashSet<int>();
        for (var i = 0; i < 4; i++)
        {
            Assert.True(planner.TryReserve(executing: 0, out var shard));
            Assert.True(shards.Add(shard));
        }
        Assert.False(planner.TryReserve(executing: 0, out _));
        Assert.Equal(4, planner.ReservedPolls);

        planner.Release(0);
        // One claimed lease now executes in the slot freed by shard 0.
        // Do not create another HTTP poll until a running task has completed.
        Assert.False(planner.TryReserve(executing: 1, out _));
        planner.Release(1);
        Assert.True(planner.TryReserve(executing: 1, out var next));
        Assert.DoesNotContain(next, new[] { 2, 3 });
    }

    [Fact]
    public void ASingleCreditRotatesFairlyAcrossAllShards()
    {
        var planner = new HubShardCapacityPlanner(capacity: 4, shards: 4,
            maxPolls: 1, initialShard: 2);
        var order = new List<int>();
        for (var i = 0; i < 8; i++)
        {
            Assert.True(planner.TryReserve(0, out var shard));
            order.Add(shard);
            Assert.False(planner.TryReserve(0, out _));
            planner.Release(shard);
        }
        Assert.Equal(new[] { 2, 3, 0, 1, 2, 3, 0, 1 }, order);
    }

    [Fact]
    public void NoPollIsReservedWhenAllWorkerSlotsAreAlreadyExecuting()
    {
        var planner = new HubShardCapacityPlanner(capacity: 8, shards: 4, maxPolls: 4);
        Assert.False(planner.TryReserve(executing: 8, out _));
        Assert.True(planner.TryReserve(executing: 7, out var shard));
        Assert.False(planner.TryReserve(executing: 7, out _));
        planner.Release(shard);
        Assert.True(planner.TryReserve(executing: 6, out _));
    }

    [Fact]
    public void InvalidOrDuplicateReleaseCannotProducePhantomCredits()
    {
        var planner = new HubShardCapacityPlanner(2, 4, 2);
        Assert.True(planner.TryReserve(0, out var shard));
        planner.Release(shard);
        Assert.Throws<InvalidOperationException>(() => planner.Release(shard));
        Assert.Throws<ArgumentOutOfRangeException>(() => planner.TryReserve(3, out _));
        Assert.Equal(0, planner.ReservedPolls);
    }

    [Theory]
    [InlineData(0, 4, 1)]
    [InlineData(2, 0, 1)]
    [InlineData(2, 65, 1)]
    [InlineData(2, 4, 0)]
    [InlineData(2, 4, 3)]
    public void InvalidSettingsAreRejected(int capacity, int shards, int maxPolls)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new HubShardCapacityPlanner(capacity, shards, maxPolls));
    }
}

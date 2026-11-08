using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

/// <summary>
/// Two independent HubTaskStore instances share PostgreSQL. Tests prove
/// per-shard owner fencing, independent takeover, and topology consistency.
/// </summary>
public sealed class HubMatchingPartitionTests
{
    private static string? Database => Environment.GetEnvironmentVariable("WFC_TEST_PG");
    private static string Hub() => "matching-shards-" + Guid.NewGuid().ToString("N");

    [Fact]
    public async Task DifferentMatchingPartitionsCanBeOwnedByDifferentHubReplicas()
    {
        if (Database is null) return;
        var hub = Hub();
        await using var a = new HubTaskStore(Database, 4);
        await using var b = new HubTaskStore(Database, 4);
        var first = new HubMatchingPartitionLeaseStore(a);
        var second = new HubMatchingPartitionLeaseStore(b);
        var p0 = await first.AcquireOrReadAsync(hub, 0, 4, "hub-a", "http://hub:5100",
            TimeSpan.FromSeconds(8), default);
        var p1 = await second.AcquireOrReadAsync(hub, 1, 4, "hub-b", "http://hub-2:5100",
            TimeSpan.FromSeconds(8), default);
        var p2 = await first.AcquireOrReadAsync(hub, 2, 4, "hub-a", "http://hub:5100",
            TimeSpan.FromSeconds(8), default);
        var p3 = await second.AcquireOrReadAsync(hub, 3, 4, "hub-b", "http://hub-2:5100",
            TimeSpan.FromSeconds(8), default);
        Assert.Equal(new[] { "hub-a", "hub-b", "hub-a", "hub-b" },
            (await first.ListAsync(hub, default)).Select(x => x.OwnerId).ToArray());
        Assert.Equal(p0.Epoch, (await second.AcquireOrReadAsync(
            hub, 0, 4, "hub-b", "http://hub-2:5100", TimeSpan.FromSeconds(8), default)).Epoch);
        Assert.True(await first.OwnsAsync(hub, 0, "hub-a", p0.Epoch, default));
        Assert.False(await second.OwnsAsync(hub, 0, "hub-b", p0.Epoch, default));
        Assert.True(await second.OwnsAsync(hub, 1, "hub-b", p1.Epoch, default));
        Assert.True(await first.OwnsAsync(hub, 2, "hub-a", p2.Epoch, default));
        Assert.True(await second.OwnsAsync(hub, 3, "hub-b", p3.Epoch, default));
    }

    [Fact]
    public async Task ExpiredPartitionTakeoverDoesNotAffectHealthyNeighbors()
    {
        if (Database is null) return;
        var hub = Hub();
        await using var a = new HubTaskStore(Database, 4);
        await using var b = new HubTaskStore(Database, 4);
        var first = new HubMatchingPartitionLeaseStore(a);
        var second = new HubMatchingPartitionLeaseStore(b);
        var failed = await first.AcquireOrReadAsync(hub, 0, 4, "hub-a", "http://hub:5100",
            TimeSpan.FromSeconds(8), default);
        var healthy = await second.AcquireOrReadAsync(hub, 1, 4, "hub-b", "http://hub-2:5100",
            TimeSpan.FromSeconds(8), default);
        await using (var db = new NpgsqlConnection(Database))
        {
            await db.OpenAsync();
            await using var command = new NpgsqlCommand("""
                UPDATE wfc_scale.hub_matching_partition_owner
                SET lease_until=clock_timestamp()-interval '1 second'
                WHERE hub_id=@hub AND matching_partition=0
                """, db);
            command.Parameters.AddWithValue("hub", hub);
            Assert.Equal(1, await command.ExecuteNonQueryAsync());
        }
        var newOwner = await second.AcquireOrReadAsync(hub, 0, 4,
            "hub-b", "http://hub-2:5100", TimeSpan.FromSeconds(8), default);
        Assert.Equal("hub-b", newOwner.OwnerId);
        Assert.True(newOwner.Epoch > failed.Epoch);
        Assert.False(await first.RenewAsync(
            hub, 0, "hub-a", failed.Epoch, TimeSpan.FromSeconds(8), default));
        Assert.False(await first.ReleaseAsync(hub, 0, "hub-a", failed.Epoch, default));
        Assert.True(await second.OwnsAsync(hub, 1, "hub-b", healthy.Epoch, default));
        Assert.True(await second.OwnsAsync(hub, 0, "hub-b", newOwner.Epoch, default));
    }

    [Fact]
    public async Task GracefulReleaseEnablesImmediateTakeoverWithNewEpoch()
    {
        if (Database is null) return;
        var hub = Hub();
        await using var a = new HubTaskStore(Database, 4);
        await using var b = new HubTaskStore(Database, 4);
        var first = new HubMatchingPartitionLeaseStore(a);
        var second = new HubMatchingPartitionLeaseStore(b);
        var oldOwner = await first.AcquireOrReadAsync(hub, 3, 4, "hub-a", "http://hub:5100",
            TimeSpan.FromSeconds(8), default);
        Assert.True(await first.ReleaseAsync(hub, 3, "hub-a", oldOwner.Epoch, default));
        var recovered = await second.AcquireOrReadAsync(hub, 3, 4, "hub-b",
            "http://hub-2:5100", TimeSpan.FromSeconds(8), default);
        Assert.Equal("hub-b", recovered.OwnerId);
        Assert.Equal(oldOwner.Epoch + 1, recovered.Epoch);
        Assert.False(await first.OwnsAsync(hub, 3, "hub-a", oldOwner.Epoch, default));
    }

    [Fact]
    public async Task PartitionCountMismatchFailsClosed()
    {
        if (Database is null) return;
        var hub = Hub();
        await using var a = new HubTaskStore(Database, 4);
        var leases = new HubMatchingPartitionLeaseStore(a);
        await leases.EnsureConfigAsync(hub, 4, default);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            leases.EnsureConfigAsync(hub, 8, default));
    }

    [Fact]
    public async Task ParallelElectionOfSameShardHasExactlyOneOwner()
    {
        if (Database is null) return;
        var hub = Hub();
        await using var a = new HubTaskStore(Database, 4);
        await using var b = new HubTaskStore(Database, 4);
        var first = new HubMatchingPartitionLeaseStore(a);
        var second = new HubMatchingPartitionLeaseStore(b);
        var elected = await Task.WhenAll(
            first.AcquireOrReadAsync(hub, 2, 4, "hub-a", "http://hub:5100",
                TimeSpan.FromSeconds(8), default),
            second.AcquireOrReadAsync(hub, 2, 4, "hub-b", "http://hub-2:5100",
                TimeSpan.FromSeconds(8), default));
        Assert.Equal(elected[0].OwnerId, elected[1].OwnerId);
        Assert.Equal(elected[0].Epoch, elected[1].Epoch);
    }
}

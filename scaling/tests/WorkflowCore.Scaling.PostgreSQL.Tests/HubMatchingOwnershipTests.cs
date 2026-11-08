using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

/// <summary>
/// Actual PostgreSQL leases shared by two independent Hub owners. Expiration
/// is manipulated in SQL to avoid slow, flaky wall-clock sleeps.
/// </summary>
public sealed class HubMatchingOwnershipTests
{
    private static string? Database => Environment.GetEnvironmentVariable("WFC_TEST_PG");

    [Fact]
    public async Task TwoReplicas_CannotBothOwnTheSameMatchingPartition()
    {
        if (Database is null) return;
        var hub = "matching-" + Guid.NewGuid().ToString("N");
        await using var one = new HubTaskStore(Database, 3);
        await using var two = new HubTaskStore(Database, 3);
        var storeOne = new HubMatchingLeaseStore(one);
        var storeTwo = new HubMatchingLeaseStore(two);
        var first = await storeOne.AcquireOrReadAsync(
            hub, "hub-a", "http://hub-a:5100", TimeSpan.FromSeconds(8), default);
        var second = await storeTwo.AcquireOrReadAsync(
            hub, "hub-b", "http://hub-b:5100", TimeSpan.FromSeconds(8), default);
        Assert.Equal("hub-a", first.OwnerId);
        Assert.Equal(first.OwnerId, second.OwnerId);
        Assert.Equal(first.Epoch, second.Epoch);
        Assert.True(await storeOne.OwnsAsync(hub, "hub-a", first.Epoch, default));
        Assert.False(await storeTwo.OwnsAsync(hub, "hub-b", first.Epoch, default));
    }

    [Fact]
    public async Task ExpiredOwner_CanBeTakenOver_AndOldEpochCannotRenew()
    {
        if (Database is null) return;
        var hub = "matching-" + Guid.NewGuid().ToString("N");
        await using var one = new HubTaskStore(Database, 3);
        await using var two = new HubTaskStore(Database, 3);
        var storeOne = new HubMatchingLeaseStore(one);
        var storeTwo = new HubMatchingLeaseStore(two);
        var oldOwner = await storeOne.AcquireOrReadAsync(
            hub, "hub-a", "http://hub-a:5100", TimeSpan.FromSeconds(8), default);

        await using (var connection = new NpgsqlConnection(Database))
        {
            await connection.OpenAsync();
            await using var command = new NpgsqlCommand("""
                UPDATE wfc_scale.hub_matching_owner
                SET lease_until=clock_timestamp()-interval '1 second'
                WHERE hub_id=@hub
                """, connection);
            command.Parameters.AddWithValue("hub", hub);
            Assert.Equal(1, await command.ExecuteNonQueryAsync());
        }

        var recovered = await storeTwo.AcquireOrReadAsync(
            hub, "hub-b", "http://hub-b:5100", TimeSpan.FromSeconds(8), default);
        Assert.Equal("hub-b", recovered.OwnerId);
        Assert.True(recovered.Epoch > oldOwner.Epoch);
        Assert.False(await storeOne.RenewAsync(
            hub, "hub-a", oldOwner.Epoch, TimeSpan.FromSeconds(8), default));
        Assert.False(await storeOne.OwnsAsync(hub, "hub-a", oldOwner.Epoch, default));
        Assert.True(await storeTwo.OwnsAsync(hub, "hub-b", recovered.Epoch, default));
    }

    [Fact]
    public async Task ParallelElection_ReturnsOneUnexpiredOwner()
    {
        if (Database is null) return;
        var hub = "matching-" + Guid.NewGuid().ToString("N");
        await using var one = new HubTaskStore(Database, 8);
        await using var two = new HubTaskStore(Database, 8);
        var first = new HubMatchingLeaseStore(one);
        var second = new HubMatchingLeaseStore(two);
        var results = await Task.WhenAll(
            first.AcquireOrReadAsync(hub, "hub-a", "http://hub-a:5100",
                TimeSpan.FromSeconds(8), default),
            second.AcquireOrReadAsync(hub, "hub-b", "http://hub-b:5100",
                TimeSpan.FromSeconds(8), default));
        Assert.Equal(results[0].OwnerId, results[1].OwnerId);
        Assert.Equal(results[0].Epoch, results[1].Epoch);
    }
}

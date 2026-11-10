using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

/// <summary>
/// Both SQL claim paths must use the same durable task-to-shard mapping.
/// The indexed path is optional; non-power-of-two counts fall back to the
/// legacy expression, and no new partition/ownership column is introduced.
/// </summary>
public sealed class HubIndexedMatchingBucketTests
{
    private static string? Database => Environment.GetEnvironmentVariable("WFC_TEST_PG");

    [Theory]
    [InlineData(false, false, 4)]
    [InlineData(true, false, 4)]
    [InlineData(false, true, 4)]
    [InlineData(true, true, 4)]
    [InlineData(false, true, 8)]
    [InlineData(true, true, 8)]
    [InlineData(false, true, 3)]
    [InlineData(true, true, 3)]
    public async Task ClaimsRespectMatchingShardAndCapability(
        bool indexed, bool batch, int matchingCount)
    {
        if (Database is null) return;
        var hub = "bucket-" + Guid.NewGuid().ToString("N");
        var inserted = new List<(Guid Id, int Partition, string Type)>();
        await using var db = new NpgsqlConnection(Database);
        await db.OpenAsync();

        // Interleave partitions, including unsupported tasks, to expose
        // a wrong matching bucket or an incorrect early LIMIT.
        for (var i = 0; i < 48; i++)
        {
            var id = Guid.NewGuid();
            var partition = i % 16;
            var type = i % 5 == 0 ? "unsupported" : "activity";
            await using var command = new NpgsqlCommand("""
                INSERT INTO wfc_scale.hub_task
                    (id, hub_id, partition_id, task_type, task_version, payload)
                VALUES (@id, @hub, @partition, @type, 1, '{}'::jsonb)
                """, db);
            command.Parameters.AddWithValue("id", id);
            command.Parameters.AddWithValue("hub", hub);
            command.Parameters.AddWithValue("partition", partition);
            command.Parameters.AddWithValue("type", type);
            await command.ExecuteNonQueryAsync();
            inserted.Add((id, partition, type));
        }

        await using var store = new HubTaskStore(Database, 12,
            enableUnconstrainedBatchClaims: batch,
            enableIndexedMatchingBucketClaims: indexed);
        const int shard = 1;
        var expected = inserted
            .Where(t => t.Partition % matchingCount == shard && t.Type == "activity")
            .Select(t => t.Id).ToHashSet();
        var leases = await store.ClaimAsync(
            "bucket-worker", [hub], [new(hub, "activity", 1)],
            32, TimeSpan.FromSeconds(30), default,
            matchingPartition: shard,
            matchingPartitionCount: matchingCount);
        Assert.True(expected.SetEquals(leases.Select(t => t.Id)));
        Assert.Equal(expected.Count, leases.Count);
        Assert.All(leases, t => Assert.Equal(shard, t.PartitionId % matchingCount));
        Assert.All(leases, t => Assert.Equal(1, t.Epoch));
        Assert.All(leases, t => Assert.Equal(1, t.Attempts));

        // A second owner cannot claim a task still leased by this owner.
        var again = await store.ClaimAsync(
            "other-owner", [hub], [new(hub, "activity", 1)],
            32, TimeSpan.FromSeconds(30), default,
            matchingPartition: shard,
            matchingPartitionCount: matchingCount);
        Assert.Empty(again);
    }

    [Fact]
    public async Task MigrationCreatesTheMatchingBucketIndex()
    {
        if (Database is null) return;
        await using var db = new NpgsqlConnection(Database);
        await db.OpenAsync();
        await using var cmd = new NpgsqlCommand("""
            SELECT EXISTS (
              SELECT 1 FROM pg_indexes
              WHERE schemaname='wfc_scale'
                AND tablename='hub_task'
                AND indexname='ix_hub_task_claim_capability_bucket_sequence'
                AND indexdef LIKE '%partition_id % 64%'
            )
            """, db);
        Assert.True((bool)(await cmd.ExecuteScalarAsync())!);
    }
}

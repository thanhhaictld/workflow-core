using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

/// <summary>
/// Both SQL claim paths must use the same durable task-to-shard mapping.
/// The indexed path is enabled by default; non-power-of-two counts fall back to the
/// legacy expression, and no new partition/ownership column is introduced.
/// </summary>
public sealed class HubIndexedMatchingBucketTests
{
    private static string? Database => Environment.GetEnvironmentVariable("WFC_TEST_PG");

    public static IEnumerable<object?[]> ClaimModes()
    {
        foreach (bool? indexed in new bool?[] { null, false, true })
        foreach (var batch in new[] { false, true })
        foreach (int? count in new int?[] { null, 1, 2, 3, 4, 8, 16, 32, 64 })
            yield return [indexed, batch, count];
    }

    [Theory]
    [MemberData(nameof(ClaimModes))]
    public async Task ClaimsRespectMatchingShardAndCapability(
        bool? indexed, bool batch, int? matchingCount)
    {
        if (Database is null) return;
        var hub = "bucket-" + Guid.NewGuid().ToString("N");
        var inserted = new List<(Guid Id, int Partition, string Type)>();
        await using var db = new NpgsqlConnection(Database);
        await db.OpenAsync();

        // Include persisted IDs above 64 and near Int32.MaxValue. Bucket IDs
        // must never be treated as a bound on persisted partition IDs.
        // Interleave unsupported tasks to expose an incorrect early LIMIT.
        for (var i = 0; i < 96; i++)
        {
            var id = Guid.NewGuid();
            var partition = i == 94 ? int.MaxValue : i * 67;
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
        var count = matchingCount ?? 1;
        var shard = count - 1;
        var expected = inserted
            .Where(t => t.Partition % count == shard && t.Type == "activity")
            .Select(t => t.Id).ToHashSet();
        var leases = new List<HubLeasedTask>();
        // Drain across LIMIT boundaries; a shard must neither lose later
        // candidates nor duplicate an earlier lease in the next batch.
        for (var batchNumber = 0; batchNumber < 5; batchNumber++)
        {
            var claimed = await store.ClaimAsync(
                "bucket-worker", [hub], [new(hub, "activity", 1)],
                32, TimeSpan.FromMinutes(5), default,
                matchingPartition: matchingCount.HasValue ? shard : null,
                matchingPartitionCount: matchingCount);
            leases.AddRange(claimed);
            if (claimed.Count == 0) break;
        }
        Assert.True(expected.SetEquals(leases.Select(t => t.Id)));
        Assert.Equal(expected.Count, leases.Count);
        Assert.All(leases, t => Assert.Equal(shard, t.PartitionId % count));
        Assert.All(leases, t => Assert.Equal(1, t.Epoch));
        Assert.All(leases, t => Assert.Equal(1, t.Attempts));

        // A second owner cannot claim a task still leased by this owner.
        var again = await store.ClaimAsync(
            "other-owner", [hub], [new(hub, "activity", 1)],
            32, TimeSpan.FromSeconds(30), default,
            matchingPartition: matchingCount.HasValue ? shard : null,
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

using System.Text.Json;
using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

/// <summary>
/// Migration 024 regression: a compact, indexed pending frontier is only a
/// candidate filter. Real task locks, resource locks, lease epochs and FIFO
/// must remain the final authorities under concurrency and across shards.
/// WFC_TEST_PG must point at a database with ordered migrations 002..024.
/// </summary>
public sealed class HubIndexedFifoFrontierTests
{
    private static readonly CancellationToken Ct = CancellationToken.None;

    [Fact]
    public async Task BlockedSuccessorsNeverHideIndependentWorkPastCandidateWindow()
    {
        var cs = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (cs is null) return;
        var hub = "frontier-" + Guid.NewGuid().ToString("N");
        var policy = new HubPolicyRegistry()
            .Register(hub, "serialized", 1, 4,
                p => p.SequentialBy("tenant", body => body.GetProperty("tenantId").GetString()))
            .Register(hub, "independent", 1, 4, _ => { });
        await using var store = new HubTaskStore(cs, 12,
            enableUnconstrainedBatchClaims: false, enableFifoHeadPrefilter: false,
            enableIndexedFifoFrontier: true);

        async Task<Guid> Enqueue(string type, string key)
        {
            var input = JsonSerializer.SerializeToElement(new { tenantId = key });
            return await store.EnqueueAsync(policy.Resolve(hub, type, 1, input),
                input.GetRawText(), null, null, Ct);
        }
        Task<IReadOnlyList<HubLeasedTask>> Claim(string worker, params string[] types) =>
            store.ClaimAsync(worker, [hub],
                types.Select(type => new HubCapability(hub, type, 1)).ToArray(),
                1, TimeSpan.FromSeconds(45), Ct);

        var headId = await Enqueue("serialized", "same-key");
        var head = Assert.Single(await Claim("first", "serialized"));
        Assert.Equal(headId, head.Id);
        var successors = new List<Guid>();
        for (var n = 0; n < 110; ++n)
            successors.Add(await Enqueue("serialized", "same-key"));
        var independent = await Enqueue("independent", "other-key");

        var lease = Assert.Single(await Claim("unblocked", "serialized", "independent"));
        Assert.Equal(independent, lease.Id);
        Assert.Empty(await Claim("blocked", "serialized"));
        Assert.True(await store.CompleteAsync(head.Id, "first", head.Epoch, "{}", Ct));
        Assert.True(await store.CompleteAsync(lease.Id, "unblocked", lease.Epoch, "{}", Ct));
        var next = Assert.Single(await Claim("next", "serialized"));
        Assert.Equal(successors[0], next.Id);

        await using var db = new NpgsqlConnection(cs);
        await db.OpenAsync(Ct);
        await using var check = new NpgsqlCommand("""
            SELECT c.fifo_pending FROM wfc_scale.hub_task_constraint c
            WHERE c.task_id=@id
            """, db);
        check.Parameters.AddWithValue("id", headId);
        Assert.False(Assert.IsType<bool>(await check.ExecuteScalarAsync(Ct)));
    }

    [Fact]
    public async Task ConcurrentWorkersCannotStealNextKeyedFifoTaskAcrossMatchingShards()
    {
        var cs = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (cs is null) return;
        var hub = "frontier-race-" + Guid.NewGuid().ToString("N");
        var policy = new HubPolicyRegistry()
            .Register(hub, "serialized", 1, 16,
                p => p.SequentialBy("tenant", body => body.GetProperty("tenantId").GetString()));
        await using var a = new HubTaskStore(cs, 12,
            enableIndexedFifoFrontier: true);
        await using var b = new HubTaskStore(cs, 12,
            enableIndexedFifoFrontier: true);

        var ids = new List<Guid>();
        for (var n = 0; n < 20; n++)
        {
            var input = JsonSerializer.SerializeToElement(new { tenantId = "shared" });
            ids.Add(await a.EnqueueAsync(policy.Resolve(hub, "serialized", 1, input),
                input.GetRawText(), null, null, Ct));
        }
        Task<IReadOnlyList<HubLeasedTask>> Claim(HubTaskStore store, string worker,
            int shard) => store.ClaimAsync(worker, [hub],
            [new HubCapability(hub, "serialized", 1)],
            20, TimeSpan.FromSeconds(45), Ct,
            matchingPartition: shard, matchingPartitionCount: 4);

        // Find the head's matching shard without assuming its partition hash.
        await using var db = new NpgsqlConnection(cs);
        await db.OpenAsync(Ct);
        await using var cmd = new NpgsqlCommand("""
            SELECT partition_id % 4 FROM wfc_scale.hub_task WHERE id=@id
            """, db);
        cmd.Parameters.AddWithValue("id", ids[0]);
        var headShard = Convert.ToInt32(await cmd.ExecuteScalarAsync(Ct));
        var competing = await Task.WhenAll(
            Claim(a, "worker-a", headShard),
            Claim(b, "worker-b", headShard));
        var winner = Assert.Single(competing.SelectMany(x => x));
        Assert.Equal(ids[0], winner.Id);
        Assert.Empty(await Claim(a, "worker-a-next", headShard));
        Assert.Empty(await Claim(b, "worker-b-next", headShard));
        var owner = competing[0].Count == 1 ? "worker-a" : "worker-b";
        Assert.True(await a.CompleteAsync(winner.Id, owner, winner.Epoch, "{}", Ct));

        // The next FIFO task may live on a different matching shard.
        var next = new List<HubLeasedTask>();
        for (var shard = 0; shard < 4; shard++)
            next.AddRange(await b.ClaimAsync("next-" + shard, [hub],
                [new HubCapability(hub, "serialized", 1)],
                10, TimeSpan.FromSeconds(45), Ct,
                matchingPartition: shard, matchingPartitionCount: 4));
        Assert.Equal(ids[1], Assert.Single(next).Id);
    }
}

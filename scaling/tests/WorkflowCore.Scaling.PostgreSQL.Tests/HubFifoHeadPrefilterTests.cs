using System.Text.Json;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

/// <summary>
/// PostgreSQL integration regression for constrained FIFO queues: a backlog
/// of blocked successors must not hide an unrelated runnable task behind
/// the legacy 96-row claim candidate window.
/// </summary>
public sealed class HubFifoHeadPrefilterTests
{
    [Fact]
    public async Task BlockedFifoBacklog_DoesNotStarveIndependentReadyTask()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (database is null) return;
        var hub = "fifo-head-" + Guid.NewGuid().ToString("N")[..14];
        var policies = new HubPolicyRegistry()
            .Register(hub, "serialized", 1, 16,
                p => p.SequentialBy("tenant", body => body.GetProperty("tenantId").GetString()))
            .Register(hub, "independent", 1, 16, _ => { });
        await using var store = new HubTaskStore(database, 12,
            enableUnconstrainedBatchClaims: false, enableFifoHeadPrefilter: true);

        async Task<Guid> Enqueue(string type, string tenant)
        {
            var json = JsonSerializer.SerializeToElement(new { tenantId = tenant });
            return await store.EnqueueAsync(
                policies.Resolve(hub, type, 1, json), json.GetRawText(),
                null, null, CancellationToken.None);
        }

        Task<IReadOnlyList<HubLeasedTask>> Claim(string owner, params string[] types) =>
            store.ClaimAsync(owner, [hub],
                types.Select(type => new HubCapability(hub, type, 1)).ToArray(),
                1, TimeSpan.FromSeconds(30), CancellationToken.None);

        var firstId = await Enqueue("serialized", "same-tenant");
        var first = Assert.Single(await Claim("worker-a", "serialized"));
        Assert.Equal(firstId, first.Id);

        // The 96-row candidate limit previously hid the unrelated task.
        var queued = new List<Guid>();
        for (var i = 0; i < 105; i++)
            queued.Add(await Enqueue("serialized", "same-tenant"));
        var independentId = await Enqueue("independent", "different-tenant");

        var unrelated = Assert.Single(await Claim("worker-b", "serialized", "independent"));
        Assert.Equal(independentId, unrelated.Id);
        Assert.True(await store.CompleteAsync(unrelated.Id, "worker-b", unrelated.Epoch,
            "{\"ok\":true}", CancellationToken.None));

        // The SQL prefilter is advisory. Task and resource locks still enforce
        // FIFO, including while the first task holds an unexpired lease.
        Assert.Empty(await Claim("worker-c", "serialized"));
        Assert.True(await store.CompleteAsync(first.Id, "worker-a", first.Epoch,
            "{\"ok\":true}", CancellationToken.None));
        var second = Assert.Single(await Claim("worker-c", "serialized"));
        Assert.Equal(queued[0], second.Id);
    }

    [Fact]
    public async Task DistinctFifoKeysRemainConcurrentlyClaimable()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (database is null) return;
        var hub = "fifo-keys-" + Guid.NewGuid().ToString("N")[..14];
        var policies = new HubPolicyRegistry()
            .Register(hub, "serialized", 1, 16,
                p => p.SequentialBy("tenant", body => body.GetProperty("tenantId").GetString()));
        await using var store = new HubTaskStore(database, 12,
            enableUnconstrainedBatchClaims: false, enableFifoHeadPrefilter: true);
        async Task<Guid> Enqueue(string tenant)
        {
            var payload = JsonSerializer.SerializeToElement(new { tenantId = tenant });
            return await store.EnqueueAsync(policies.Resolve(hub, "serialized", 1, payload),
                payload.GetRawText(), null, null, CancellationToken.None);
        }

        var tenantA = await Enqueue("A");
        var tenantB = await Enqueue("B");
        var capability = new[] { new HubCapability(hub, "serialized", 1) };
        var leased = await store.ClaimAsync("worker", [hub], capability, 2,
            TimeSpan.FromSeconds(30), CancellationToken.None);
        Assert.Equal(2, leased.Count);
        Assert.True(new[] { tenantA, tenantB }.ToHashSet()
            .SetEquals(leased.Select(l => l.Id)));
    }
}

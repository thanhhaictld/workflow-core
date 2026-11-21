using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

/// <summary>Real PostgreSQL acceptance tests for Hub-side admission and fencing.
/// CI provisions WFC_TEST_PG and applies scaling/sql/007_hub_gateway.sql.
/// </summary>
public sealed class HubGatewayTests
{

    private static string? Database => Environment.GetEnvironmentVariable("WFC_TEST_PG");
    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task CompletionRoute_ResolvesUnconstrainedTask_AndUnknownId()
    {
        if (Database is null) return;
        var hub = Key();
        var taskRoute = new HubTaskRoute(hub, "demo-activity", 1, 8);
        await using var store = new HubTaskStore(Database, 3);
        var taskId = await store.EnqueueAsync(taskRoute, "{}", null, null, default);
        var route = await store.GetCompletionRouteAsync(taskId, CancellationToken.None);
        Assert.NotNull(route);
        Assert.Equal(hub, route.HubId);
        Assert.Equal(HubCompletionKind.Standalone, route.Kind);
        Assert.Null(await store.GetCompletionRouteAsync(Guid.NewGuid(), CancellationToken.None));
    }

    [Fact]
    public async Task CompletionIsIdempotent_AndRejectsStaleEpoch()
    {
        if (Database is null) return;
        var hub = Key();
        var taskRoute = new HubTaskRoute(hub, "demo-activity", 1, 8);
        await using var store = new HubTaskStore(Database, 3);
        var id = await store.EnqueueAsync(taskRoute, "{}", null, "same-request", default);
        Assert.Equal(id, await store.EnqueueAsync(taskRoute, "{}", null, "same-request", default));
        var first = Assert.Single(await Claim(store, hub, "worker-A"));

        // Force the previous lease to expire as if worker A disconnected.
        await using (var db = new NpgsqlConnection(Database))
        {
            await db.OpenAsync();
            await using var cmd = new NpgsqlCommand("""
                UPDATE wfc_scale.hub_task SET lease_until = now() - interval '1 second'
                WHERE id = @id
                """, db);
            cmd.Parameters.AddWithValue("id", id);
            await cmd.ExecuteNonQueryAsync();
        }
        var second = Assert.Single(await Claim(store, hub, "worker-B"));
        Assert.True(second.Epoch > first.Epoch);
        Assert.False(await store.CompleteAsync(id, "worker-A", first.Epoch,
            "{\"ok\":true}", CancellationToken.None));
        Assert.True(await store.CompleteAsync(id, "worker-B", second.Epoch,
            "{\"ok\":true}", CancellationToken.None));
        Assert.True(await store.CompleteAsync(id, "worker-B", second.Epoch,
            "{\"ok\":true}", CancellationToken.None));
        Assert.Equal("Complete", (await store.GetAsync(id, CancellationToken.None))!.State);
    }

    private static string Key() => "hub-gateway-" + Guid.NewGuid().ToString("N");

    private static Task<IReadOnlyList<HubLeasedTask>> Claim(HubTaskStore store, string hub, string worker) =>
        store.ClaimAsync(worker, [hub], [new(hub, "demo-activity", 1)], 1, Lease, default);
}

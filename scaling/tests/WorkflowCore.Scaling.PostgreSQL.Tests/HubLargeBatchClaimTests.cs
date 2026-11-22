using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

/// <summary>
/// Real PostgreSQL coverage for both 64-task lease paths used by the
/// credit-driven SSE dispatcher. All tasks are independent / unconstrained;
/// ordered and resource-constrained eligibility stays authoritative in SQL.
/// </summary>
public sealed class HubLargeBatchClaimTests
{
    private static async Task<Guid[]> SeedAsync(string pg, string hub)
    {
        await using var db = new NpgsqlConnection(pg);
        await db.OpenAsync();
        await using var cmd = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task
                (id,hub_id,partition_id,task_type,task_version,workflow_id,payload,admission)
            SELECT gen_random_uuid(),@hub,0,'activity',1,NULL,'{}'::jsonb,1
            FROM generate_series(1,64)
            RETURNING id
            """, db);
        cmd.Parameters.AddWithValue("hub", hub);
        var ids = new List<Guid>(64);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) ids.Add(reader.GetGuid(0));
        Assert.Equal(64, ids.Count);
        return ids.ToArray();
    }

    [Fact]
    public async Task UnconstrainedPostgresClaimAcceptsSixtyFourIndependentActivities()
    {
        var pg = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (pg is null) return;
        var hub = "claim-64-pull-" + Guid.NewGuid().ToString("N");
        var ids = await SeedAsync(pg, hub);
        await using var tasks = new HubTaskStore(pg, enableUnconstrainedBatchClaims:true);
        var claimed = await tasks.ClaimAsync("worker-64",[hub],[new(hub,"activity",1)],
            64,TimeSpan.FromSeconds(45),default);
        Assert.Equal(64,claimed.Count);
        Assert.Equal(ids.Order(),claimed.Select(x=>x.Id).Order());
        Assert.All(claimed,x=>Assert.Equal(1,x.Epoch));
        Assert.Empty(await tasks.ClaimAsync("worker-other",[hub],[new(hub,"activity",1)],
            64,TimeSpan.FromSeconds(45),default));
    }

    [Fact]
    public async Task CandidatePostgresClaimAcceptsSixtyFourReservedActivityCredits()
    {
        var pg = Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if (pg is null) return;
        var hub = "claim-64-candidate-" + Guid.NewGuid().ToString("N");
        var ids = await SeedAsync(pg, hub);
        await using var tasks = new HubTaskStore(pg);
        var owners = new HubMatchingPartitionLeaseStore(tasks);
        var owner = await owners.AcquireOrReadAsync(hub,0,4,
            "candidate-owner-64","http://hub:5100",TimeSpan.FromSeconds(60),default);
        var caps = new HubCapability[]{ new(hub,"activity",1) };
        var items = ids.Select(x=>new HubCandidateBatchItem(x,0)).ToArray();
        var result = await tasks.ClaimCandidateBatchAsync(new(
            items,"worker-64",hub,0,4,caps,null,owner,TimeSpan.FromSeconds(45)),default);
        Assert.Equal(HubCandidateClaimStatus.Leased,result.Status);
        Assert.Equal(64,result.Leases.Count);
        Assert.Equal(ids.Order(),result.Leases.Select(x=>x.Id).Order());
        Assert.All(result.Leases,x=>Assert.True(x.Epoch > 0));
        Assert.Empty(await tasks.ClaimAsync("another",[hub],caps,
            64,TimeSpan.FromSeconds(45),default));
    }
}

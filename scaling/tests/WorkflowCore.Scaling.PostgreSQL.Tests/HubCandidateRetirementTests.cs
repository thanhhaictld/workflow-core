using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubCandidateRetirementTests
{
    [Fact]
    public async Task BatchRetirementKeepsLiveExtendedLeaseAndRetiresStaleEpoch()
    {
        var connection = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "retire-" + Guid.NewGuid().ToString("N"); var live = Guid.NewGuid(); var stale = Guid.NewGuid();
        await using var db = new NpgsqlConnection(connection); await db.OpenAsync();
        await using (var insert = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload,state,owner,epoch,lease_until)
            VALUES (@live,@hub,0,'work',1,'{}',1,'worker',1,clock_timestamp()+interval '5 minutes'),
                   (@stale,@hub,0,'work',1,'{}',1,'other',2,clock_timestamp()+interval '5 minutes')
            """, db))
        {
            insert.Parameters.AddWithValue("live", live); insert.Parameters.AddWithValue("stale", stale);
            insert.Parameters.AddWithValue("hub", hub); await insert.ExecuteNonQueryAsync();
        }
        var deadline = DateTime.UtcNow.AddMinutes(-1);
        HubLeasedTask Lease(Guid id) => new(id, hub, 0, "work", 1, null,
            System.Text.Json.JsonSerializer.SerializeToElement(new { }), 1, 1, deadline);
        await using var store = new HubTaskStore(connection);
        var retired = await store.RetirableCandidateLeasesAsync([(Lease(live), "worker"), (Lease(stale), "worker")], default);
        Assert.DoesNotContain((live, 1L), retired);
        Assert.Contains((stale, 1L), retired);
    }
}

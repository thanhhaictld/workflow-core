using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubCandidateCoordinatorPostgresTests
{
    [Fact]
    public async Task LongUtf8CandidateFitsTinyPageShareAndReceivesDatabaseLease()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "long-acquisition-" + new string('h', 54) + Guid.NewGuid().ToString("N")[..8];
        var taskType = new string('t', 80); var id = Guid.NewGuid();
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        await using (var insert = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
            VALUES (@id,@hub,0,@taskType,1,'{}')
            """, db))
        {
            insert.Parameters.AddWithValue("id", id); insert.Parameters.AddWithValue("hub", hub);
            insert.Parameters.AddWithValue("taskType", taskType); await insert.ExecuteNonQueryAsync();
        }
        await using var tasks = new HubTaskStore(database); var owners = new HubMatchingPartitionLeaseStore(tasks);
        var owner = await owners.AcquireOrReadAsync(hub,0,64,"owner","http://localhost:5100",TimeSpan.FromSeconds(60),default);
        var options = new HubCandidateCoordinatorOptions(PageSize:256,PageWireBytes:512,PartitionEntries:256,ProcessEntries:512,
            PartitionBytes:300_000,ProcessBytes:600_000,PartitionCredits:2,ProcessCredits:4,ProcessPartitions:2);
        using var registry = new HubPartitionCandidateRegistry(new HubCandidateReader(tasks, metadataEnrichmentEnabled: true),options);
        registry.StartGeneration(owner,64);
        var capabilities = new[] { new HubCapability(hub,taskType,1) };
        Assert.True(registry.AddCredit(hub,0,owner.Epoch,new("long-credit","worker",capabilities,null)));
        HubCandidateReservation? reservation = null;
        for (var turn=0; turn<16 && reservation is null; turn++)
        {
            await registry.RunOneTurnAsync(DateTimeOffset.UtcNow,default);
            reservation = registry.TakeReservation(hub,0,owner.Epoch);
        }
        Assert.NotNull(reservation);
        Assert.Equal(id,reservation.Candidate.Id);
        Assert.Equal(taskType,reservation.Candidate.TaskType);
        var claim = new HubCandidateClaim(id,reservation.Candidate.Epoch,"worker",hub,0,64,capabilities,null,owner,TimeSpan.FromSeconds(30));
        var leased = await tasks.ClaimCandidateAsync(claim,default);
        Assert.Equal(HubCandidateClaimStatus.Leased,leased.Status);
        registry.Complete(reservation.Id,HubCandidateResolution.Leased,DateTimeOffset.UtcNow);
    }


}

using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubCandidateCoordinatorPostgresTests
{
    [Fact]
    public async Task OversizedHeadAndUnknownResourcesProgressWithAuthoritativeSqlAdmission()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "coord-oversized-" + Guid.NewGuid().ToString("N"); var huge = Guid.NewGuid(); var blocked = Guid.NewGuid(); var regular = Guid.NewGuid(); var holder = Guid.NewGuid();
        var key = hub + new string('x',600);
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        await using (var cmd = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_resource(resource_key,capacity,strict_fifo) VALUES (@key,1,false);
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
            VALUES (@huge,@hub,0,@large,1,'{}'),(@blocked,@hub,0,'activity',1,'{}'),(@regular,@hub,0,'activity',1,'{}');
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload,state,owner,lease_until)
            VALUES (@holder,@hub,0,'activity',1,'{}',1,'other',now()+interval '1 hour');
            INSERT INTO wfc_scale.hub_task_constraint(task_id,resource_key) VALUES (@blocked,@key),(@holder,@key);
            """,db))
        { cmd.Parameters.AddWithValue("key",key); cmd.Parameters.AddWithValue("hub",hub); cmd.Parameters.AddWithValue("huge",huge); cmd.Parameters.AddWithValue("blocked",blocked); cmd.Parameters.AddWithValue("regular",regular); cmd.Parameters.AddWithValue("holder",holder); cmd.Parameters.AddWithValue("large",new string('z',3000)); await cmd.ExecuteNonQueryAsync(); }
        await using var tasks = new HubTaskStore(database); var owners = new HubMatchingPartitionLeaseStore(tasks);
        var owner = await owners.AcquireOrReadAsync(hub,0,64,"owner","http://localhost:5100",TimeSpan.FromSeconds(60),default);
        var options = new HubCandidateCoordinatorOptions(PageSize:1,PageWireBytes:256,PartitionEntries:4,ProcessEntries:8,
            PartitionBytes:32768,ProcessBytes:65536,PartitionCredits:2,ProcessCredits:4,ProcessPartitions:2);
        using var registry = new HubPartitionCandidateRegistry(new HubCandidateReader(tasks),options);
        registry.StartGeneration(owner,64); var caps = new[] { new HubCapability(hub,"activity",1) };
        Assert.True(registry.AddCredit(hub,0,owner.Epoch,new("credit","worker",caps,null)));
        HubCandidateReservation? reserve = null;
        for(var i=0;i<12 && reserve is null;i++) { await registry.RunOneTurnAsync(DateTimeOffset.UtcNow,default); reserve = registry.TakeReservation(hub,0,owner.Epoch); }
        Assert.NotNull(reserve); Assert.Equal(blocked,reserve.Candidate.Id); Assert.False(reserve.Candidate.ResourceKeysComplete);
        Assert.Equal(huge,registry.Snapshot().LastOversized!.Entry.Id);
        var claim = new HubCandidateClaim(reserve.Candidate.Id,reserve.Candidate.Epoch,"worker",hub,0,64,caps,null,owner,TimeSpan.FromSeconds(30));
        var rejected = await tasks.ClaimCandidateAsync(claim,default); Assert.Equal(HubCandidateClaimStatus.ResourceBlocked,rejected.Status);
        registry.Complete(reserve.Id,HubCandidateResolution.ResourceBlocked,DateTimeOffset.UtcNow,rejected.ResourceKey);
        HubCandidateReservation? later = null;
        for(var i=0;i<12 && later is null;i++) { await registry.RunOneTurnAsync(DateTimeOffset.UtcNow,default); later = registry.TakeReservation(hub,0,owner.Epoch); }
        Assert.NotNull(later); Assert.Equal(regular,later.Candidate.Id);
        var leased = await tasks.ClaimCandidateAsync(claim with { TaskId=regular },default); Assert.Equal(HubCandidateClaimStatus.Leased,leased.Status);
        registry.Complete(later.Id,HubCandidateResolution.Leased,DateTimeOffset.UtcNow);
        Assert.True(registry.ReleaseGrantedCredit("credit"));
        await using var check = new NpgsqlCommand("SELECT state,epoch,attempts FROM wfc_scale.hub_task WHERE id=@id",db);
        check.Parameters.AddWithValue("id",blocked); await using var rows = await check.ExecuteReaderAsync(); Assert.True(await rows.ReadAsync());
        Assert.Equal(0,rows.GetInt16(0)); Assert.Equal(0,rows.GetInt64(1)); Assert.Equal(0,rows.GetInt32(2));
    }
}

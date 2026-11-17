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
    [Fact]
    public async Task WarmLargeResourceCandidatesReuseInPlaceAcrossMixedRecoveryPages()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "warm-" + new string('h', 60) + Guid.NewGuid().ToString("N")[..10];
        var taskType = new string('t', 80); var keyPadding = new string('r', 5000);
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        await using (var cmd = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
            SELECT gen_random_uuid(),@hub,0,@taskType,1,'{}' FROM generate_series(1,64);
            INSERT INTO wfc_scale.hub_resource(resource_key,capacity,strict_fifo)
            SELECT 'warm-'||id::text||@padding,1,false FROM wfc_scale.hub_task WHERE hub_id=@hub;
            INSERT INTO wfc_scale.hub_task_constraint(task_id,resource_key)
            SELECT id,'warm-'||id::text||@padding FROM wfc_scale.hub_task WHERE hub_id=@hub;
            """, db))
        { cmd.Parameters.AddWithValue("hub", hub); cmd.Parameters.AddWithValue("taskType", taskType);
          cmd.Parameters.AddWithValue("padding", keyPadding); await cmd.ExecuteNonQueryAsync(); }

        await using var tasks = new HubTaskStore(database); var owners = new HubMatchingPartitionLeaseStore(tasks);
        var owner = await owners.AcquireOrReadAsync(hub,0,64,"owner","http://localhost:5100",TimeSpan.FromSeconds(60),default);
        var options = new HubCandidateCoordinatorOptions(PageSize:8,PageWireBytes:65536,PartitionEntries:80,ProcessEntries:160,
            PartitionBytes:1_500_000,ProcessBytes:3_000_000,PartitionCredits:16,ProcessCredits:64,ProcessPartitions:4,
            RecoveryCadence:TimeSpan.FromSeconds(10));
        using var registry = new HubPartitionCandidateRegistry(new HubCandidateReader(tasks),options);
        registry.StartGeneration(owner,64);
        for (var turn=0;turn<24;turn++) await registry.RunOneTurnAsync(DateTimeOffset.UtcNow,default);
        Assert.Equal(64,registry.Snapshot().Entries);
        var entryEstimate = 512L + (32 + 2L*hub.Length) + (32 + 2L*taskType.Length) +
            16 + 32 + 2L*(5_000 + 5 + 36);
        Assert.True(entryEstimate * 64 > options.PageReservationBytes);

        var coldTask = Guid.NewGuid(); var coldKey = "warm-" + coldTask.ToString("N") + keyPadding;
        await using (var cmd = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_resource(resource_key,capacity,strict_fifo) VALUES (@key,1,false);
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
            VALUES (@id,@hub,0,@taskType,1,'{}');
            INSERT INTO wfc_scale.hub_task_constraint(task_id,resource_key) VALUES (@id,@key);
            """, db))
        { cmd.Parameters.AddWithValue("id",coldTask); cmd.Parameters.AddWithValue("hub",hub);
          cmd.Parameters.AddWithValue("taskType",taskType); cmd.Parameters.AddWithValue("key",coldKey); await cmd.ExecuteNonQueryAsync(); }
        registry.Notify(hub,0,owner.Epoch);
        for (var turn=0;turn<28;turn++) await registry.RunOneTurnAsync(DateTimeOffset.UtcNow,default);
        var snapshot = registry.Snapshot();
        Assert.Equal(65,snapshot.Entries);
        Assert.InRange(snapshot.Bytes,0,options.ProcessBytes);

        var capabilities = new[] { new HubCapability(hub,taskType,1) };
        Assert.True(registry.AddCredit(hub,0,owner.Epoch,new("large-key-credit","worker",capabilities,null)));
        var reservation = registry.TakeReservation(hub,0,owner.Epoch);
        Assert.NotNull(reservation);
        Assert.True(reservation.Candidate.ResourceKeysComplete);
        Assert.True(Assert.Single(reservation.Candidate.ResourceKeys).Length > 5000);
    }
}

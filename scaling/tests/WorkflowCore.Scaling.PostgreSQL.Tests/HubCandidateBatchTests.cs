using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubCandidateBatchTests
{
    private static string Database => Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
    [Fact]
    public async Task BatchRevalidatesEachIdPinsEpochAndConstraintsAndJournalsOnlyAdmitted()
    {
        var hub="candidate-batch-"+Guid.NewGuid().ToString("N"); var ids=Enumerable.Range(0,4).Select(_=>Guid.NewGuid()).ToArray();
        await using var db=new NpgsqlConnection(Database); await db.OpenAsync();
        for(var i=0;i<ids.Length;i++)
        {
            var workflow=Guid.NewGuid(); await using var cmd=new NpgsqlCommand("""
                INSERT INTO wfc_scale.hub_workflow(id,hub_id,definition_id,definition_version,partition_id,input_json,step_count,execution_mode,native_status)
                VALUES (@w,@hub,'definition',@version,0,'{}',1,'native','Runnable');
                INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,workflow_id,dispatch_key,payload)
                VALUES (@id,@hub,0,'__native_workflow__',1,@w,'stable','{}');
                """,db);
            cmd.Parameters.AddWithValue("id",ids[i]); cmd.Parameters.AddWithValue("w",workflow); cmd.Parameters.AddWithValue("hub",hub); cmd.Parameters.AddWithValue("version",i==1?2:1); await cmd.ExecuteNonQueryAsync();
        }
        var key=hub+"-constraint";
        await using (var cmd=new NpgsqlCommand("INSERT INTO wfc_scale.hub_resource(resource_key,capacity,strict_fifo) VALUES (@key,1,false); INSERT INTO wfc_scale.hub_task_constraint(task_id,resource_key) VALUES (@id,@key)",db))
        { cmd.Parameters.AddWithValue("key",key); cmd.Parameters.AddWithValue("id",ids[2]); await cmd.ExecuteNonQueryAsync(); }
        await using var store=new HubTaskStore(Database); var owners=new HubMatchingPartitionLeaseStore(store);
        var owner=await owners.AcquireOrReadAsync(hub,0,4,"owner","http://localhost:5100",TimeSpan.FromSeconds(60),default);
        var request=new HubCandidateBatchClaim(ids.Select((id,i)=>new HubCandidateBatchItem(id,i==3?1:0)).ToArray(),"worker",hub,0,4,
            [new(hub,"__native_workflow__",1)],[new(hub,"definition",1)],owner,TimeSpan.FromSeconds(30));
        var result=await store.ClaimCandidateBatchAsync(request,default);
        Assert.Equal(HubCandidateClaimStatus.Leased,result.Status); Assert.Equal(ids[0],Assert.Single(result.Leases).Id);
        await using var check=new NpgsqlCommand("""
            SELECT t.id,t.state,t.epoch,t.attempts,t.dispatch_key,(SELECT count(*) FROM wfc_scale.hub_workflow_event e WHERE e.task_id=t.id AND e.event_type='WorkflowTaskClaimed')
            FROM wfc_scale.hub_task t WHERE t.id=ANY(@ids)
            """,db); check.Parameters.AddWithValue("ids",ids); await using var rows=await check.ExecuteReaderAsync(); var count=0;
        while(await rows.ReadAsync()) { var admitted=rows.GetGuid(0)==ids[0]; Assert.Equal(admitted?1:0,rows.GetInt16(1)); Assert.Equal(admitted?1:0,rows.GetInt64(2)); Assert.Equal(admitted?1:0,rows.GetInt32(3)); Assert.Equal("stable",rows.GetString(4)); Assert.Equal(admitted?1:0,rows.GetInt64(5)); count++; }
        Assert.Equal(4,count);
    }

    [Fact]
    public async Task BatchRollbackAndLostCommitResponseHaveDistinctOutcomes()
    {
        var hub="candidate-batch-fault-"+Guid.NewGuid().ToString("N"); var ids=new[]{Guid.NewGuid(),Guid.NewGuid()};
        await using var db=new NpgsqlConnection(Database); await db.OpenAsync();
        await using (var cmd=new NpgsqlCommand("INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload) SELECT unnest(@ids),@hub,0,'activity',1,'{}'",db))
        { cmd.Parameters.AddWithValue("ids",ids); cmd.Parameters.AddWithValue("hub",hub); await cmd.ExecuteNonQueryAsync(); }
        await using var store=new HubTaskStore(Database); var owners=new HubMatchingPartitionLeaseStore(store);
        var owner=await owners.AcquireOrReadAsync(hub,0,4,"owner","http://localhost:5100",TimeSpan.FromSeconds(60),default);
        var request=new HubCandidateBatchClaim(ids.Select(id=>new HubCandidateBatchItem(id,0)).ToArray(),"worker",hub,0,4,[new(hub,"activity",1)],null,owner,TimeSpan.FromSeconds(30));
        await Assert.ThrowsAsync<OperationCanceledException>(()=>store.ClaimCandidateBatchAsync(request,default,new(BeforeOwnerRecheck:_=>throw new OperationCanceledException())));
        await using (var check=new NpgsqlCommand("SELECT count(*) FROM wfc_scale.hub_task WHERE id=ANY(@ids) AND state=0 AND epoch=0 AND attempts=0",db))
        { check.Parameters.AddWithValue("ids",ids); Assert.Equal(2L,(long)(await check.ExecuteScalarAsync())!); }
        var result=await store.ClaimCandidateBatchAsync(request,default,new(Commit:async(tx,ct)=>{await tx.CommitAsync(ct); throw new IOException("lost response");}));
        Assert.Equal(HubCandidateClaimStatus.CommitOutcomeUnknown,result.Status); Assert.Equal(2,result.Leases.Count);
        foreach(var lease in result.Leases) Assert.True(await store.IsCandidateLeaseCurrentAsync(lease,"worker",default));
        var retry=await store.ClaimCandidateBatchAsync(request,default); Assert.Equal(HubCandidateClaimStatus.Unavailable,retry.Status);
    }
}

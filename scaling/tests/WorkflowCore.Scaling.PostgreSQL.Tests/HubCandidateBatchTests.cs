using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubCandidateBatchTests
{
    private static string Database => Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
    [Fact]
    public async Task MixedWorkerBatchRevalidatesEachWorkerAndDoesNotRejectOtherItems()
    {
        var hub = "mixed-worker-" + Guid.NewGuid().ToString("N");
        var ids = Enumerable.Range(0, 4).Select(_ => Guid.NewGuid()).ToArray();
        await using var db = new NpgsqlConnection(Database); await db.OpenAsync();
        await using (var insert = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
            VALUES (@a,@hub,0,'a',1,'{}'),(@b,@hub,0,'b',1,'{}'),(@bad,@hub,0,'a',1,'{}'),(@stale,@hub,0,'a',1,'{}')
            """, db))
        {
            insert.Parameters.AddWithValue("a", ids[0]); insert.Parameters.AddWithValue("b", ids[1]);
            insert.Parameters.AddWithValue("bad", ids[2]); insert.Parameters.AddWithValue("stale", ids[3]);
            insert.Parameters.AddWithValue("hub", hub); await insert.ExecuteNonQueryAsync();
        }
        await using var store = new HubTaskStore(Database);
        var owner = await new HubMatchingPartitionLeaseStore(store).AcquireOrReadAsync(hub, 0, 4,
            "owner", "http://localhost:5100", TimeSpan.FromSeconds(60), default);
        HubCapability[] a = [new(hub,"a",1)], b = [new(hub,"b",1)];
        HubCandidateBatchItem[] items = [new(ids[0],0,"worker-a",a), new(ids[1],0,"worker-b",b),
            new(ids[2],0,"worker-b",b), new(ids[3],1,"worker-a",a)];
        var result = await store.ClaimCandidateBatchAsync(new(items,"worker-a",hub,0,4,a,null,owner,TimeSpan.FromSeconds(30)),default);
        Assert.Equal(HubCandidateClaimStatus.Leased,result.Status);
        Assert.Equal(2,result.Leases.Count);
        Assert.True(await store.IsCandidateLeaseCurrentAsync(result.Leases.Single(x => x.Id == ids[0]),"worker-a",default));
        Assert.True(await store.IsCandidateLeaseCurrentAsync(result.Leases.Single(x => x.Id == ids[1]),"worker-b",default));
        await using var check = new NpgsqlCommand("SELECT count(*) FROM wfc_scale.hub_task WHERE id=ANY(@ids) AND state=0 AND epoch=0",db);
        check.Parameters.AddWithValue("ids",new[] { ids[2], ids[3] }); Assert.Equal(2L,(long)(await check.ExecuteScalarAsync())!);
    }
    [Fact]
    public async Task BatchRevalidatesEachIdPinsEpochAndConstraintsAndJournalsOnlyAdmitted()
    {
        var hub="candidate-batch-"+Guid.NewGuid().ToString("N"); var ids=Enumerable.Range(0,7).Select(_=>Guid.NewGuid()).ToArray();
        await using var db=new NpgsqlConnection(Database); await db.OpenAsync();
        for(var i=0;i<6;i++)
        {
            var workflow=Guid.NewGuid(); await using var cmd=new NpgsqlCommand("""
                INSERT INTO wfc_scale.hub_workflow(id,hub_id,definition_id,definition_version,partition_id,input_json,step_count,execution_mode,native_status)
                VALUES (@w,@hub,'definition',@version,0,'{}',1,'native',@status);
                INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,workflow_id,dispatch_key,payload)
                VALUES (@id,@hub,0,'__native_workflow__',1,@w,'stable','{}');
                """,db);
            cmd.Parameters.AddWithValue("id",ids[i]); cmd.Parameters.AddWithValue("w",workflow); cmd.Parameters.AddWithValue("hub",hub);
            cmd.Parameters.AddWithValue("version",i==1?2:1); cmd.Parameters.AddWithValue("status",i==4?"Suspended":i==5?"Completed":"Runnable"); await cmd.ExecuteNonQueryAsync();
        }
        await using(var missing=new NpgsqlCommand("INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload) VALUES (@id,@hub,0,'__native_workflow__',1,'{}')",db))
        { missing.Parameters.AddWithValue("id",ids[6]); missing.Parameters.AddWithValue("hub",hub); await missing.ExecuteNonQueryAsync(); }
        var key=hub+"-constraint";
        await using (var cmd=new NpgsqlCommand("INSERT INTO wfc_scale.hub_resource(resource_key,capacity,strict_fifo) VALUES (@key,1,false); INSERT INTO wfc_scale.hub_task_constraint(task_id,resource_key) VALUES (@id,@key)",db))
        { cmd.Parameters.AddWithValue("key",key); cmd.Parameters.AddWithValue("id",ids[2]); await cmd.ExecuteNonQueryAsync(); }
        await using var store=new HubTaskStore(Database); var owners=new HubMatchingPartitionLeaseStore(store);
        var owner=await owners.AcquireOrReadAsync(hub,0,4,"owner","http://localhost:5100",TimeSpan.FromSeconds(60),default);
        var request=new HubCandidateBatchClaim(ids.Select((id,i)=>new HubCandidateBatchItem(id,i==3?1:0,"worker",
            [new(hub,"__native_workflow__",1)],i==6?null:[new(hub,"definition",1)])).ToArray(),"worker",hub,0,4,
            [new(hub,"__native_workflow__",1)],[new(hub,"definition",1)],owner,TimeSpan.FromSeconds(30));
        var result=await store.ClaimCandidateBatchAsync(request,default);
        Assert.Equal(HubCandidateClaimStatus.Leased,result.Status); Assert.Equal(new HashSet<Guid>{ids[0],ids[6]},result.Leases.Select(x=>x.Id).ToHashSet());
        await using var check=new NpgsqlCommand("""
            SELECT t.id,t.state,t.epoch,t.attempts,t.dispatch_key,(SELECT count(*) FROM wfc_scale.hub_workflow_event e WHERE e.task_id=t.id AND e.event_type='WorkflowTaskClaimed')
            FROM wfc_scale.hub_task t WHERE t.id=ANY(@ids)
            """,db); check.Parameters.AddWithValue("ids",ids); await using var rows=await check.ExecuteReaderAsync(); var count=0;
        while(await rows.ReadAsync()) { var taskId=rows.GetGuid(0); var admitted=taskId==ids[0]||taskId==ids[6]; Assert.Equal(admitted?1:0,rows.GetInt16(1)); Assert.Equal(admitted?1L:0L,rows.GetInt64(2)); Assert.Equal(admitted?1:0,rows.GetInt32(3)); if(taskId!=ids[6]) Assert.Equal("stable",rows.GetString(4)); Assert.Equal(taskId==ids[0]?1L:0L,rows.GetInt64(5)); count++; }
        Assert.Equal(7,count);
    }

    [Fact]
    public async Task BatchRollbackAndLostCommitResponseHaveDistinctOutcomes()
    {
        var hub="candidate-batch-fault-"+Guid.NewGuid().ToString("N"); var ids=new[]{Guid.NewGuid(),Guid.NewGuid()};
        var workflows=new[]{Guid.NewGuid(),Guid.NewGuid()};
        await using var db=new NpgsqlConnection(Database); await db.OpenAsync();
        await using (var cmd=new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_workflow(id,hub_id,definition_id,definition_version,partition_id,input_json,step_count)
            SELECT unnest(@workflows),@hub,'definition',1,0,'{}',1;
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,workflow_id,workflow_step,payload,state,owner,epoch,attempts,lease_until)
            SELECT id,@hub,0,'__workflow__',1,workflows.workflow_id,0,'{}',1,'worker-old',1,1,now()-interval '1 second'
            FROM unnest(@ids,@workflows) AS workflows(id,workflow_id);
            INSERT INTO wfc_scale.hub_task_attempt(task_id,epoch,workflow_id,step_index,attempt_number,worker_id)
            SELECT id,1,workflow_id,0,1,'worker-old' FROM wfc_scale.hub_task WHERE id=ANY(@ids);
            """,db))
        { cmd.Parameters.AddWithValue("ids",ids); cmd.Parameters.AddWithValue("workflows",workflows); cmd.Parameters.AddWithValue("hub",hub); await cmd.ExecuteNonQueryAsync(); }
        await using var store=new HubTaskStore(Database); var owners=new HubMatchingPartitionLeaseStore(store);
        var owner=await owners.AcquireOrReadAsync(hub,0,4,"owner","http://localhost:5100",TimeSpan.FromSeconds(60),default);
        HubCapability[] capabilities = [new(hub,"__workflow__",1)];
        var request=new HubCandidateBatchClaim(ids.Select((id,i)=>new HubCandidateBatchItem(id,1,"worker-"+i,capabilities)).ToArray(),"worker-a",hub,0,4,capabilities,null,owner,TimeSpan.FromSeconds(30));
        var staleOwner=await store.ClaimCandidateBatchAsync(request with { Owner=owner with { Epoch=owner.Epoch+1 } },default);
        Assert.Equal(HubCandidateClaimStatus.OwnerLost,staleOwner.Status);
        await Assert.ThrowsAsync<ArgumentException>(()=>store.ClaimCandidateBatchAsync(request with { Partition=1 },default));
        await Assert.ThrowsAsync<OperationCanceledException>(()=>store.ClaimCandidateBatchAsync(request,default,new(BeforeOwnerRecheck:_=>throw new OperationCanceledException())));
        await using (var check=new NpgsqlCommand("""
            SELECT (SELECT count(*) FROM wfc_scale.hub_task WHERE id=ANY(@ids) AND state=1 AND epoch=1 AND attempts=1 AND owner='worker-old'),
                   (SELECT count(*) FROM wfc_scale.hub_task_attempt WHERE task_id=ANY(@ids)),
                   (SELECT count(*) FROM wfc_scale.hub_task_attempt WHERE task_id=ANY(@ids) AND outcome='Leased' AND finished_at IS NULL),
                   (SELECT count(*) FROM wfc_scale.hub_workflow_event WHERE task_id=ANY(@ids) AND event_type='WorkflowTaskClaimed')
            """,db))
        { check.Parameters.AddWithValue("ids",ids); await using var rows=await check.ExecuteReaderAsync(); Assert.True(await rows.ReadAsync()); Assert.Equal(2L,rows.GetInt64(0)); Assert.Equal(2L,rows.GetInt64(1)); Assert.Equal(2L,rows.GetInt64(2)); Assert.Equal(0L,rows.GetInt64(3)); }
        var result=await store.ClaimCandidateBatchAsync(request,default,new(Commit:async(tx,ct)=>{await tx.CommitAsync(ct); throw new IOException("lost response");}));
        Assert.Equal(HubCandidateClaimStatus.CommitOutcomeUnknown,result.Status); Assert.Equal(2,result.Leases.Count);
        foreach(var lease in result.Leases)
            Assert.True(await store.IsCandidateLeaseCurrentAsync(lease,"worker-"+Array.IndexOf(ids,lease.Id),default));
        await using (var verify=new NpgsqlCommand("""
            SELECT t.id,t.epoch,t.attempts,t.owner,
                   (SELECT count(*) FROM wfc_scale.hub_task_attempt a WHERE a.task_id=t.id) AS attempt_count,
                   (SELECT count(*) FROM wfc_scale.hub_task_attempt a WHERE a.task_id=t.id AND a.epoch=1 AND a.outcome='LeaseExpired' AND a.finished_at IS NOT NULL) AS expired_count,
                   (SELECT count(*) FROM wfc_scale.hub_task_attempt a WHERE a.task_id=t.id AND a.epoch=2 AND a.outcome='Leased' AND a.finished_at IS NULL AND a.worker_id=t.owner) AS active_count,
                   (SELECT count(*) FROM wfc_scale.hub_workflow_event e WHERE e.task_id=t.id AND e.event_type='WorkflowTaskClaimed' AND e.payload->>'worker'=t.owner AND e.payload->>'epoch'='2') AS journal_count
            FROM wfc_scale.hub_task t WHERE t.id=ANY(@ids)
            """,db))
        { verify.Parameters.AddWithValue("ids",ids); await using var rows=await verify.ExecuteReaderAsync(); var count=0; while(await rows.ReadAsync()) { Assert.Equal(2L,rows.GetInt64(1)); Assert.Equal(2,rows.GetInt32(2)); Assert.Equal("worker-"+Array.IndexOf(ids,rows.GetGuid(0)),rows.GetString(3)); Assert.Equal(2L,rows.GetInt64(4)); Assert.Equal(1L,rows.GetInt64(5)); Assert.Equal(1L,rows.GetInt64(6)); Assert.Equal(1L,rows.GetInt64(7)); count++; } Assert.Equal(2,count); }
        var retry=await store.ClaimCandidateBatchAsync(request,default); Assert.Equal(HubCandidateClaimStatus.Unavailable,retry.Status);
        await using (var stable=new NpgsqlCommand("SELECT (SELECT count(*) FROM wfc_scale.hub_task_attempt WHERE task_id=ANY(@ids)),(SELECT count(*) FROM wfc_scale.hub_workflow_event WHERE task_id=ANY(@ids) AND event_type='WorkflowTaskClaimed')",db))
        { stable.Parameters.AddWithValue("ids",ids); await using var rows=await stable.ExecuteReaderAsync(); Assert.True(await rows.ReadAsync()); Assert.Equal(4L,rows.GetInt64(0)); Assert.Equal(2L,rows.GetInt64(1)); }
    }

    [Fact]
    public async Task BatchKeepsDueExpiredAttemptAndShardEligibilityUnchanged()
    {
        var hub="candidate-batch-eligibility-"+Guid.NewGuid().ToString("N");
        var ids=Enumerable.Range(0,7).Select(_=>Guid.NewGuid()).ToArray();
        await using var db=new NpgsqlConnection(Database); await db.OpenAsync();
        await using (var cmd=new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload)
            VALUES (@due,@hub,0,'activity',1,'{}'),(@future,@hub,0,'activity',1,'{}'),
                   (@expired,@hub,0,'activity',1,'{}'),(@live,@hub,0,'activity',1,'{}'),
                   (@exhausted,@hub,0,'activity',1,'{}'),(@wrongPartition,@hub,1,'activity',1,'{}'),
                   (@wrongHub,'other-'||@hub,0,'activity',1,'{}');
            UPDATE wfc_scale.hub_task SET due_at=now()+interval '1 hour' WHERE id=@future;
            UPDATE wfc_scale.hub_task SET state=1,owner='old',lease_until=now()-interval '1 second',epoch=2 WHERE id=@expired;
            UPDATE wfc_scale.hub_task SET state=1,owner='live',lease_until=now()+interval '1 hour' WHERE id=@live;
            UPDATE wfc_scale.hub_task SET attempts=1,max_attempts=1 WHERE id=@exhausted;
            """,db))
        {
            cmd.Parameters.AddWithValue("due",ids[0]); cmd.Parameters.AddWithValue("future",ids[1]);
            cmd.Parameters.AddWithValue("expired",ids[2]); cmd.Parameters.AddWithValue("live",ids[3]);
            cmd.Parameters.AddWithValue("exhausted",ids[4]); cmd.Parameters.AddWithValue("wrongPartition",ids[5]);
            cmd.Parameters.AddWithValue("wrongHub",ids[6]); cmd.Parameters.AddWithValue("hub",hub);
            await cmd.ExecuteNonQueryAsync();
        }

        await using var store=new HubTaskStore(Database); var owners=new HubMatchingPartitionLeaseStore(store);
        var owner=await owners.AcquireOrReadAsync(hub,0,4,"owner","http://localhost:5100",TimeSpan.FromSeconds(60),default);
        var capability=new HubCapability(hub,"activity",1);
        var items=ids.Select((id,index)=>new HubCandidateBatchItem(id,index==2?2:0,"worker",[capability])).ToArray();
        var result=await store.ClaimCandidateBatchAsync(new(items,"worker",hub,0,4,[capability],null,owner,TimeSpan.FromSeconds(30)),default);

        Assert.Equal(HubCandidateClaimStatus.Leased,result.Status);
        Assert.Equal(new HashSet<Guid>{ids[0],ids[2]},result.Leases.Select(x=>x.Id).ToHashSet());
        await using var verify=new NpgsqlCommand("""
            SELECT id,state,epoch,attempts,owner FROM wfc_scale.hub_task WHERE id=ANY(@ids)
            """,db);
        verify.Parameters.AddWithValue("ids",ids); await using var rows=await verify.ExecuteReaderAsync();
        while(await rows.ReadAsync())
        {
            var index=Array.IndexOf(ids,rows.GetGuid(0));
            var admitted=index is 0 or 2;
            Assert.Equal(admitted?(short)1:(short)(index==3?1:0),rows.GetInt16(1));
            Assert.Equal(admitted?(index==2?3L:1L):(index==2?2L:0L),rows.GetInt64(2));
            Assert.Equal(admitted?1:(index==4?1:0),rows.GetInt32(3));
            if(admitted) Assert.Equal("worker",rows.GetString(4));
            else if(index==2) Assert.Equal("old",rows.GetString(4));
            else if(index==3) Assert.Equal("live",rows.GetString(4));
            else Assert.True(rows.IsDBNull(4));
        }
    }

    [Fact]
    public async Task OppositeOrderCompetingStoreBatchesSerializeOnOwnerAndLeaseEachIdOnce()
    {
        var hub="candidate-batch-contended-"+Guid.NewGuid().ToString("N");
        var ids=Enumerable.Range(0,12).Select(_=>Guid.NewGuid()).ToArray();
        await using var db=new NpgsqlConnection(Database); await db.OpenAsync();
        await using(var insert=new NpgsqlCommand("INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload) SELECT unnest(@ids),@hub,0,'activity',1,'{}'",db))
        { insert.Parameters.AddWithValue("ids",ids); insert.Parameters.AddWithValue("hub",hub); await insert.ExecuteNonQueryAsync(); }

        await using var firstStore=new HubTaskStore(Database); await using var secondStore=new HubTaskStore(Database);
        var owner=await new HubMatchingPartitionLeaseStore(firstStore).AcquireOrReadAsync(hub,0,4,"owner","http://localhost:5100",TimeSpan.FromSeconds(60),default);
        HubCapability[] capabilities=[new(hub,"activity",1)];
        HubCandidateBatchClaim Request(IEnumerable<Guid> selected)=>new(selected.Select(id=>new HubCandidateBatchItem(id,0,"worker",capabilities)).ToArray(),"worker",hub,0,4,capabilities,null,owner,TimeSpan.FromSeconds(30));
        var reached=new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release=new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstTask=firstStore.ClaimCandidateBatchAsync(Request(ids.Take(8)),default,new(BeforeOwnerRecheck:async ct=>{reached.TrySetResult(); await release.Task.WaitAsync(ct);}));
        await reached.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var secondTask=secondStore.ClaimCandidateBatchAsync(Request(ids.Skip(4).Reverse()),default);
        try
        {
            using var timeout=new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var blocked=false;
            while(!blocked)
            {
                await using var waitCheck=new NpgsqlCommand("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query ILIKE '%locked_owner%')",db);
                blocked=(bool)(await waitCheck.ExecuteScalarAsync(timeout.Token))!;
                if(!blocked) await Task.Delay(25,timeout.Token);
            }
        }
        finally { release.TrySetResult(); }

        var first=await firstTask.WaitAsync(TimeSpan.FromSeconds(10));
        var second=await secondTask.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(HubCandidateClaimStatus.Leased,first.Status); Assert.Equal(8,first.Leases.Count);
        Assert.Equal(HubCandidateClaimStatus.Leased,second.Status); Assert.Equal(4,second.Leases.Count);
        Assert.Equal(12,first.Leases.Concat(second.Leases).Select(x=>x.Id).Distinct().Count());
        foreach(var lease in first.Leases.Concat(second.Leases)) Assert.True(await firstStore.IsCandidateLeaseCurrentAsync(lease,"worker",default));
    }

    [Fact]
    public async Task ExpiredOwnerAtFinalBatchRecheckRollsBackHistoryBeforeTakeoverRecoversTask()
    {
        var hub="candidate-batch-owner-expiry-"+Guid.NewGuid().ToString("N"); var id=Guid.NewGuid(); var workflow=Guid.NewGuid();
        await using var store=new HubTaskStore(Database); var owners=new HubMatchingPartitionLeaseStore(store);
        var oldOwner=await owners.AcquireOrReadAsync(hub,0,4,"owner-old","http://localhost:5100",TimeSpan.FromSeconds(4),default);
        await using var db=new NpgsqlConnection(Database); await db.OpenAsync();
        await using(var seed=new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_workflow(id,hub_id,definition_id,definition_version,partition_id,input_json,step_count)
            VALUES (@workflow,@hub,'definition',1,0,'{}',1);
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,workflow_id,workflow_step,payload,state,owner,epoch,attempts,lease_until)
            VALUES (@id,@hub,0,'__workflow__',1,@workflow,0,'{}',1,'worker-old',1,1,now()-interval '1 second');
            INSERT INTO wfc_scale.hub_task_attempt(task_id,epoch,workflow_id,step_index,attempt_number,worker_id)
            VALUES (@id,1,@workflow,0,1,'worker-old');
            """,db))
        { seed.Parameters.AddWithValue("workflow",workflow); seed.Parameters.AddWithValue("hub",hub); seed.Parameters.AddWithValue("id",id); await seed.ExecuteNonQueryAsync(); }

        HubCapability[] caps=[new(hub,"__workflow__",1)];
        var request=new HubCandidateBatchClaim([new HubCandidateBatchItem(id,1,"worker-old",caps)],"worker-old",hub,0,4,caps,null,oldOwner,TimeSpan.FromSeconds(30));
        var reached=new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release=new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var claim=store.ClaimCandidateBatchAsync(request,default,new(BeforeOwnerRecheck:async ct=>{reached.TrySetResult(); await release.Task.WaitAsync(ct);}));
        await reached.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await using(var expire=new NpgsqlCommand("SELECT pg_sleep(GREATEST(0,extract(epoch from @until::timestamptz-clock_timestamp())+0.2))",db))
        { expire.Parameters.AddWithValue("until",oldOwner.LeaseUntil); await expire.ExecuteNonQueryAsync(); }
        var takeover=owners.AcquireOrReadAsync(hub,0,4,"owner-new","http://localhost:5200",TimeSpan.FromSeconds(30),default);
        try
        {
            using var timeout=new CancellationTokenSource(TimeSpan.FromSeconds(10)); var blocked=false;
            while(!blocked)
            {
                await using var wait=new NpgsqlCommand("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query ILIKE '%INSERT INTO wfc_scale.hub_matching_partition_owner%')",db);
                blocked=(bool)(await wait.ExecuteScalarAsync(timeout.Token))!;
                if(!blocked) await Task.Delay(25,timeout.Token);
            }
        }
        finally { release.TrySetResult(); }

        Assert.Equal(HubCandidateClaimStatus.OwnerLost,(await claim.WaitAsync(TimeSpan.FromSeconds(10))).Status);
        var newOwner=await takeover.WaitAsync(TimeSpan.FromSeconds(10)); Assert.True(newOwner.Epoch>oldOwner.Epoch);
        await using(var rolledBack=new NpgsqlCommand("""
            SELECT state,owner,epoch,attempts,
                   (SELECT count(*) FROM wfc_scale.hub_task_attempt WHERE task_id=@id) AS attempts_count,
                   (SELECT count(*) FROM wfc_scale.hub_task_attempt WHERE task_id=@id AND outcome='Leased' AND finished_at IS NULL) AS old_open,
                   (SELECT count(*) FROM wfc_scale.hub_workflow_event WHERE task_id=@id AND event_type='WorkflowTaskClaimed') AS journal_count
            FROM wfc_scale.hub_task WHERE id=@id
            """,db))
        { rolledBack.Parameters.AddWithValue("id",id); await using var rows=await rolledBack.ExecuteReaderAsync(); Assert.True(await rows.ReadAsync()); Assert.Equal((short)1,rows.GetInt16(0)); Assert.Equal("worker-old",rows.GetString(1)); Assert.Equal(1L,rows.GetInt64(2)); Assert.Equal(1,rows.GetInt32(3)); Assert.Equal(1L,rows.GetInt64(4)); Assert.Equal(1L,rows.GetInt64(5)); Assert.Equal(0L,rows.GetInt64(6)); }

        var recovered=await store.ClaimCandidateBatchAsync(request with { Owner=newOwner, Items=[new(id,1,"worker-new",caps)] },default);
        Assert.Equal(HubCandidateClaimStatus.Leased,recovered.Status); var lease=Assert.Single(recovered.Leases); Assert.Equal(2L,lease.Epoch); Assert.Equal(2,lease.Attempts);
        Assert.False(await store.CompleteAsync(id,"worker-old",1,"{}",default));
        Assert.True(await store.IsCandidateLeaseCurrentAsync(lease,"worker-new",default));
        await using(var verify=new NpgsqlCommand("""
            SELECT count(*) AS count,
                   count(*) FILTER (WHERE epoch=1 AND outcome='LeaseExpired' AND finished_at IS NOT NULL) AS expired,
                   count(*) FILTER (WHERE epoch=2 AND worker_id='worker-new' AND outcome='Leased' AND finished_at IS NULL) AS fresh,
                   (SELECT count(*) FROM wfc_scale.hub_workflow_event WHERE task_id=@id AND event_type='WorkflowTaskClaimed' AND payload->>'worker'='worker-new' AND payload->>'epoch'='2') AS journal
            FROM wfc_scale.hub_task_attempt WHERE task_id=@id
            """,db))
        { verify.Parameters.AddWithValue("id",id); await using var rows=await verify.ExecuteReaderAsync(); Assert.True(await rows.ReadAsync()); Assert.Equal(2L,rows.GetInt64(0)); Assert.Equal(1L,rows.GetInt64(1)); Assert.Equal(1L,rows.GetInt64(2)); Assert.Equal(1L,rows.GetInt64(3)); }
    }

    [Fact]
    public async Task BatchKeepsSequenceOrderWhenSelectedIdsArriveInReverseOrder()
    {
        var hub="candidate-batch-sequence-"+Guid.NewGuid().ToString("N");
        var ids=Enumerable.Range(0,4).Select(_=>Guid.NewGuid()).ToArray();
        await using var db=new NpgsqlConnection(Database); await db.OpenAsync();
        foreach(var id in ids)
        {
            await using var insert=new NpgsqlCommand("INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload) VALUES (@id,@hub,0,'activity',1,'{}')",db);
            insert.Parameters.AddWithValue("id",id); insert.Parameters.AddWithValue("hub",hub); await insert.ExecuteNonQueryAsync();
        }
        await using var store=new HubTaskStore(Database);
        var owner=await new HubMatchingPartitionLeaseStore(store).AcquireOrReadAsync(hub,0,4,
            "owner","http://localhost:5100",TimeSpan.FromSeconds(60),default);
        HubCapability[] capabilities=[new(hub,"activity",1)];
        var reverse=ids.Reverse().Select(id=>new HubCandidateBatchItem(id,0,"worker",capabilities)).ToArray();
        var result=await store.ClaimCandidateBatchAsync(new(reverse,"worker",hub,0,4,capabilities,null,owner,TimeSpan.FromSeconds(30)),default);
        Assert.Equal(HubCandidateClaimStatus.Leased,result.Status);
        Assert.Equal(ids,result.Leases.Select(x=>x.Id));
    }

    [Fact]
    public async Task BatchSkipsASelectedRowLockedByConcurrentStateTransition()
    {
        var hub="candidate-batch-row-lock-"+Guid.NewGuid().ToString("N"); var id=Guid.NewGuid();
        await using var store=new HubTaskStore(Database); var owners=new HubMatchingPartitionLeaseStore(store);
        var owner=await owners.AcquireOrReadAsync(hub,0,4,"owner","http://localhost:5100",TimeSpan.FromSeconds(60),default);
        await using var writer=new NpgsqlConnection(Database); await writer.OpenAsync();
        await using(var insert=new NpgsqlCommand("INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload) VALUES (@id,@hub,0,'activity',1,'{}')",writer))
        { insert.Parameters.AddWithValue("id",id); insert.Parameters.AddWithValue("hub",hub); await insert.ExecuteNonQueryAsync(); }
        await using var transition=await writer.BeginTransactionAsync();
        await using(var lockRow=new NpgsqlCommand("SELECT id FROM wfc_scale.hub_task WHERE id=@id FOR UPDATE",writer,transition))
        { lockRow.Parameters.AddWithValue("id",id); Assert.Equal(id,await lockRow.ExecuteScalarAsync()); }

        HubCapability[] capabilities=[new(hub,"activity",1)];
        var request=new HubCandidateBatchClaim([new(id,0,"worker",capabilities)],"worker",hub,0,4,capabilities,null,owner,TimeSpan.FromSeconds(30));
        var result=await store.ClaimCandidateBatchAsync(request,default).WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(HubCandidateClaimStatus.Unavailable,result.Status);
        Assert.Empty(result.Leases);

        await using(var completeTransition=new NpgsqlCommand("UPDATE wfc_scale.hub_task SET state=2 WHERE id=@id",writer,transition))
        { completeTransition.Parameters.AddWithValue("id",id); Assert.Equal(1,await completeTransition.ExecuteNonQueryAsync()); }
        await transition.CommitAsync();
        await using var verify=new NpgsqlCommand("SELECT state,epoch,attempts,(SELECT count(*) FROM wfc_scale.hub_task_attempt WHERE task_id=@id),(SELECT count(*) FROM wfc_scale.hub_workflow_event WHERE task_id=@id AND event_type='WorkflowTaskClaimed') FROM wfc_scale.hub_task WHERE id=@id",writer);
        verify.Parameters.AddWithValue("id",id); await using var row=await verify.ExecuteReaderAsync();
        Assert.True(await row.ReadAsync()); Assert.Equal((short)2,row.GetInt16(0)); Assert.Equal(0L,row.GetInt64(1));
        Assert.Equal(0,row.GetInt32(2)); Assert.Equal(0L,row.GetInt64(3)); Assert.Equal(0L,row.GetInt64(4));
    }
}

using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubCandidateAdmissionTests
{
    [Fact]
    public async Task CandidateAdmission_RevalidatesEpochCapabilityTopologyAndOwner()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG")
            ?? throw new InvalidOperationException("WFC_TEST_PG is required for candidate admission tests");
        var hub = "candidate-admission-" + Guid.NewGuid().ToString("N");
        var id = Guid.NewGuid();
        await using var store = new HubTaskStore(database);
        var owners = new HubMatchingPartitionLeaseStore(store);
        var owner = await owners.AcquireOrReadAsync(hub, 0, 4, "owner", "http://localhost:5100", TimeSpan.FromSeconds(60), default);
        await using var db = new NpgsqlConnection(database);
        await db.OpenAsync();
        await using (var insert = new NpgsqlCommand("INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload) VALUES (@id,@hub,0,'activity',1,'{}')", db))
        {
            insert.Parameters.AddWithValue("id", id);
            insert.Parameters.AddWithValue("hub", hub);
            await insert.ExecuteNonQueryAsync();
        }
        var request = new HubCandidateClaim(id, 0, "worker", hub, 0, 4,
            [new HubCapability(hub, "activity", 1)], null, owner, TimeSpan.FromSeconds(30));
        Assert.Equal(HubCandidateClaimStatus.Unavailable, (await store.ClaimCandidateAsync(request with { ObservedEpoch = 1 }, default)).Status);
        Assert.Equal(HubCandidateClaimStatus.Unavailable, (await store.ClaimCandidateAsync(request with { Capabilities = [new(hub, "other", 1)] }, default)).Status);
        Assert.Equal(HubCandidateClaimStatus.OwnerLost, (await store.ClaimCandidateAsync(request with { PartitionCount = 2 }, default)).Status);
        Assert.Equal(HubCandidateClaimStatus.Unavailable, (await store.ClaimCandidateAsync(request with { Capabilities = [new(hub, "activity", 2)] }, default)).Status);
        Assert.Equal(HubCandidateClaimStatus.Unavailable, (await store.ClaimCandidateAsync(request with { Capabilities = [new("another-hub", "activity", 1)] }, default)).Status);
        await using (var change = new NpgsqlCommand("UPDATE wfc_scale.hub_task SET partition_id=1 WHERE id=@id",db))
        { change.Parameters.AddWithValue("id",id); await change.ExecuteNonQueryAsync(); }
        Assert.Equal(HubCandidateClaimStatus.Unavailable,(await store.ClaimCandidateAsync(request,default)).Status);
        await using (var change = new NpgsqlCommand("UPDATE wfc_scale.hub_task SET partition_id=0,hub_id=@other WHERE id=@id",db))
        { change.Parameters.AddWithValue("id",id); change.Parameters.AddWithValue("other",hub+"-other"); await change.ExecuteNonQueryAsync(); }
        Assert.Equal(HubCandidateClaimStatus.Unavailable,(await store.ClaimCandidateAsync(request,default)).Status);
        await using (var change = new NpgsqlCommand("UPDATE wfc_scale.hub_task SET hub_id=@hub WHERE id=@id",db))
        { change.Parameters.AddWithValue("id",id); change.Parameters.AddWithValue("hub",hub); await change.ExecuteNonQueryAsync(); }
        var leased = await store.ClaimCandidateAsync(request, default);
        Assert.Equal(HubCandidateClaimStatus.Leased, leased.Status);
        Assert.NotNull(leased.Lease);
        Assert.Equal(id, leased.Lease.Id);
        Assert.Equal(1, leased.Lease.Epoch);
        Assert.Equal(1, leased.Lease.Attempts);
        Assert.Equal(HubCandidateClaimStatus.Unavailable, (await store.ClaimCandidateAsync(request, default)).Status);
        await owners.ReleaseAsync(hub, 0, owner.OwnerId, owner.Epoch, default);
        Assert.Equal(HubCandidateClaimStatus.OwnerLost, (await store.ClaimCandidateAsync(request, default)).Status);
    }
    [Fact]
    public async Task CandidateAdmissionChecksWorkflowPinsAndNativeStatus()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "candidate-pins-" + Guid.NewGuid().ToString("N"); var workflow = Guid.NewGuid(); var id = Guid.NewGuid();
        await using var store = new HubTaskStore(database); var owners = new HubMatchingPartitionLeaseStore(store);
        var owner = await owners.AcquireOrReadAsync(hub,0,4,"owner","http://localhost:5100",TimeSpan.FromSeconds(60),default);
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        await using (var cmd = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_workflow(id,hub_id,definition_id,definition_version,partition_id,input_json,step_count,execution_mode,native_status)
            VALUES (@workflow,@hub,'definition',2,0,'{}',1,'native','Suspended');
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,workflow_id,payload)
            VALUES (@id,@hub,0,'__native_workflow__',1,@workflow,'{}');
            """,db))
        { cmd.Parameters.AddWithValue("workflow",workflow); cmd.Parameters.AddWithValue("id",id); cmd.Parameters.AddWithValue("hub",hub); await cmd.ExecuteNonQueryAsync(); }
        var request = new HubCandidateClaim(id,0,"worker",hub,0,4,[new(hub,"__native_workflow__",1)],[new(hub,"definition",2)],owner,TimeSpan.FromSeconds(30));
        Assert.Equal(HubCandidateClaimStatus.Unavailable,(await store.ClaimCandidateAsync(request,default)).Status);
        await using (var cmd = new NpgsqlCommand("UPDATE wfc_scale.hub_workflow SET native_status='Runnable' WHERE id=@id",db))
        { cmd.Parameters.AddWithValue("id",workflow); await cmd.ExecuteNonQueryAsync(); }
        Assert.Equal(HubCandidateClaimStatus.Unavailable,(await store.ClaimCandidateAsync(request with { Workflows = [new(hub,"definition",1)] },default)).Status);
        await using (var rejected = new NpgsqlCommand("SELECT (SELECT count(*) FROM wfc_scale.hub_workflow_event WHERE task_id=@id), (SELECT count(*) FROM wfc_scale.hub_task_attempt WHERE task_id=@id), state, epoch, attempts FROM wfc_scale.hub_task WHERE id=@id",db))
        { rejected.Parameters.AddWithValue("id",id); await using var r = await rejected.ExecuteReaderAsync(); Assert.True(await r.ReadAsync()); Assert.Equal(0L,r.GetInt64(0)); Assert.Equal(0L,r.GetInt64(1)); Assert.Equal(0,r.GetInt16(2)); Assert.Equal(0,r.GetInt64(3)); Assert.Equal(0,r.GetInt32(4)); }
        Assert.Equal(HubCandidateClaimStatus.Leased,(await store.ClaimCandidateAsync(request,default)).Status);
        await using var check = new NpgsqlCommand("SELECT count(*) FROM wfc_scale.hub_workflow_event WHERE workflow_id=@id AND event_type='WorkflowTaskClaimed'",db);
        check.Parameters.AddWithValue("id",workflow); Assert.Equal  (1L, (long)(await check.ExecuteScalarAsync())!);
    }

    [Fact]
    public async Task CandidateAdmissionReportsBlockingResourceAndHonorsFifo()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "candidate-resource-" + Guid.NewGuid().ToString("N"); var first = Guid.NewGuid(); var second = Guid.NewGuid(); var key = hub + "-key";
        await using var store = new HubTaskStore(database, enableUnconstrainedBatchClaims: false); var owners = new HubMatchingPartitionLeaseStore(store);
        var owner = await owners.AcquireOrReadAsync(hub,0,4,"owner","http://localhost:5100",TimeSpan.FromSeconds(60),default);
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        await using (var cmd = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_resource(resource_key,capacity,strict_fifo) VALUES (@key,1,true);
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload) VALUES (@first,@hub,0,'activity',1,'{}'),(@second,@hub,0,'activity',1,'{}');
            INSERT INTO wfc_scale.hub_task_constraint(task_id,resource_key) VALUES (@first,@key),(@second,@key);
            """,db))
        { cmd.Parameters.AddWithValue("hub",hub); cmd.Parameters.AddWithValue("first",first); cmd.Parameters.AddWithValue("second",second); cmd.Parameters.AddWithValue("key",key); await cmd.ExecuteNonQueryAsync(); }
        var request = new HubCandidateClaim(second,0,"worker",hub,0,4,[new(hub,"activity",1)],null,owner,TimeSpan.FromSeconds(30));
        var blocked = await store.ClaimCandidateAsync(request,default);
        Assert.Equal(HubCandidateClaimStatus.ResourceBlocked,blocked.Status); Assert.Equal(key,blocked.ResourceKey);
        var old = await store.ClaimAsync("worker",[hub],[new(hub,"activity",1)],1,TimeSpan.FromSeconds(30),default,matchingPartition:0,matchingPartitionCount:4);
        Assert.Equal(first,Assert.Single(old).Id);
        blocked = await store.ClaimCandidateAsync(request,default);
        Assert.Equal(HubCandidateClaimStatus.ResourceBlocked,blocked.Status);
    }

    [Fact]
    public async Task CandidateAndLegacyClaimsPreserveAttemptHistoryOnTakeover()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "candidate-parity-" + Guid.NewGuid().ToString("N"); var oldId = Guid.NewGuid(); var newId = Guid.NewGuid();
        var w1 = Guid.NewGuid(); var w2 = Guid.NewGuid();
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        await using (var cmd = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_workflow(id,hub_id,definition_id,definition_version,partition_id,input_json,step_count)
            VALUES (@w1,@hub,'definition',1,0,'{}',1),(@w2,@hub,'definition',1,0,'{}',1);
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,workflow_id,workflow_step,dispatch_key,payload)
            VALUES (@old,@hub,0,'activity',1,@w1,0,'stable-key','{}'),(@new,@hub,0,'activity',1,@w2,0,'stable-key','{}');
            """,db))
        { cmd.Parameters.AddWithValue("hub",hub); cmd.Parameters.AddWithValue("old",oldId); cmd.Parameters.AddWithValue("new",newId); cmd.Parameters.AddWithValue("w1",w1); cmd.Parameters.AddWithValue("w2",w2); await cmd.ExecuteNonQueryAsync(); }
        await using var store = new HubTaskStore(database, enableUnconstrainedBatchClaims:false);
        var owners = new HubMatchingPartitionLeaseStore(store);
        var owner = await owners.AcquireOrReadAsync(hub,0,4,"owner","http://localhost:5100",TimeSpan.FromSeconds(60),default);
        var request = new HubCandidateClaim(newId,0,"worker",hub,0,4,[new(hub,"activity",1)],null,owner,TimeSpan.FromSeconds(30));
        var legacy = Assert.Single(await store.ClaimAsync("worker",[hub],[new(hub,"activity",1)],1,TimeSpan.FromSeconds(30),default,matchingPartition:0,matchingPartitionCount:4));
        var candidate = await store.ClaimCandidateAsync(request,default);
        Assert.Equal(oldId,legacy.Id); Assert.Equal(HubCandidateClaimStatus.Leased,candidate.Status);
        Assert.Equal(legacy.Epoch,candidate.Lease!.Epoch); Assert.Equal(legacy.Attempts,candidate.Lease.Attempts);
        await using (var cmd = new NpgsqlCommand("UPDATE wfc_scale.hub_task SET lease_until=now()-interval '1 minute' WHERE id IN (@old,@new)",db))
        { cmd.Parameters.AddWithValue("old",oldId); cmd.Parameters.AddWithValue("new",newId); await cmd.ExecuteNonQueryAsync(); }
        legacy = Assert.Single(await store.ClaimAsync("worker-2",[hub],[new(hub,"activity",1)],1,TimeSpan.FromSeconds(30),default,matchingPartition:0,matchingPartitionCount:4));
        candidate = await store.ClaimCandidateAsync(request with { ObservedEpoch=1, WorkerId="worker-2" },default);
        Assert.Equal(2,legacy.Epoch); Assert.Equal(legacy.Epoch,candidate.Lease!.Epoch); Assert.Equal(legacy.Attempts,candidate.Lease.Attempts);
        await using var check = new NpgsqlCommand("SELECT task_id, count(*), count(*) FILTER (WHERE outcome='LeaseExpired'), count(*) FILTER (WHERE finished_at IS NULL) FROM wfc_scale.hub_task_attempt WHERE task_id IN (@old,@new) GROUP BY task_id",db);
        check.Parameters.AddWithValue("old",oldId); check.Parameters.AddWithValue("new",newId);
        await using var rows = await check.ExecuteReaderAsync(); var count = 0;
        while (await rows.ReadAsync()) { Assert.Equal(2L,rows.GetInt64(1)); Assert.Equal(1L,rows.GetInt64(2)); Assert.Equal(1L,rows.GetInt64(3)); count++; }
        Assert.Equal(2,count);
        await rows.DisposeAsync();
        await using var keys = new NpgsqlCommand("SELECT count(*) FROM wfc_scale.hub_task WHERE id IN (@old,@new) AND dispatch_key='stable-key'",db);
        keys.Parameters.AddWithValue("old",oldId); keys.Parameters.AddWithValue("new",newId); Assert.Equal(2L,(long)(await keys.ExecuteScalarAsync())!);
    }

    [Fact]
    public async Task CommitFaultsPreserveTentativeLeaseForDurableReconciliation()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "candidate-fault-" + Guid.NewGuid().ToString("N"); var id = Guid.NewGuid();
        await using var store = new HubTaskStore(database); var owners = new HubMatchingPartitionLeaseStore(store);
        var owner = await owners.AcquireOrReadAsync(hub,0,4,"owner","http://localhost:5100",TimeSpan.FromSeconds(60),default);
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        await using (var cmd = new NpgsqlCommand("INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload) VALUES (@id,@hub,0,'activity',1,'{}')",db))
        { cmd.Parameters.AddWithValue("hub",hub); cmd.Parameters.AddWithValue("id",id); await cmd.ExecuteNonQueryAsync(); }
        var req = new HubCandidateClaim(id,0,"worker",hub,0,4,[new(hub,"activity",1)],null,owner,TimeSpan.FromSeconds(30));
        var precommit = new HubCandidateClaimTestHooks(BeforeOwnerRecheck: _ => throw new IOException("injected before commit"));
        await Assert.ThrowsAsync<IOException>(() => store.ClaimCandidateAsync(req,default,precommit));
        await using (var check = new NpgsqlCommand("SELECT state,epoch,attempts FROM wfc_scale.hub_task WHERE id=@id",db))
        { check.Parameters.AddWithValue("id",id); await using var r = await check.ExecuteReaderAsync(); Assert.True(await r.ReadAsync()); Assert.Equal(0,r.GetInt16(0)); Assert.Equal(0,r.GetInt64(1)); Assert.Equal(0,r.GetInt32(2)); }
        var unknown = await store.ClaimCandidateAsync(req,default,new HubCandidateClaimTestHooks(Commit: async (tx,ct) =>
        { await tx.CommitAsync(ct); throw new IOException("injected lost commit response"); }));
        Assert.Equal(HubCandidateClaimStatus.CommitOutcomeUnknown,unknown.Status); Assert.NotNull(unknown.Lease);
        Assert.True(await store.IsCandidateLeaseCurrentAsync(unknown.Lease!,"worker",default));
        Assert.False(await store.IsCandidateLeaseCurrentAsync(unknown.Lease!,"another-worker",default));
        Assert.Equal(HubCandidateClaimStatus.Unavailable,(await store.ClaimCandidateAsync(req,default)).Status);
        await using var verify = new NpgsqlCommand("SELECT attempts,epoch FROM wfc_scale.hub_task WHERE id=@id",db);
        verify.Parameters.AddWithValue("id",id); await using var rows = await verify.ExecuteReaderAsync(); Assert.True(await rows.ReadAsync());
        Assert.Equal(1,rows.GetInt32(0)); Assert.Equal(1,rows.GetInt64(1));
    }

    [Fact]
    public async Task ExpiredOwnerRollsBackBeforeCompetingGenerationAdmits()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "candidate-owner-race-" + Guid.NewGuid().ToString("N"); var id = Guid.NewGuid();
        await using var store = new HubTaskStore(database); var owners = new HubMatchingPartitionLeaseStore(store);
        var oldOwner = await owners.AcquireOrReadAsync(hub,0,4,"old","http://localhost:5100",TimeSpan.FromSeconds(4),default);
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        await using (var cmd = new NpgsqlCommand("INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload) VALUES (@id,@hub,0,'activity',1,'{}')",db))
        { cmd.Parameters.AddWithValue("hub",hub); cmd.Parameters.AddWithValue("id",id); await cmd.ExecuteNonQueryAsync(); }
        var req = new HubCandidateClaim(id,0,"worker",hub,0,4,[new(hub,"activity",1)],null,oldOwner,TimeSpan.FromSeconds(30));
        var reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var oldClaim = store.ClaimCandidateAsync(req,default,new HubCandidateClaimTestHooks(BeforeOwnerRecheck: async ct => { reached.SetResult(); await release.Task.WaitAsync(ct); }));
        await reached.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await using (var wait = new NpgsqlCommand("SELECT pg_sleep(GREATEST(0,extract(epoch from @until::timestamptz-clock_timestamp())+0.2))",db))
        { wait.Parameters.AddWithValue("until",oldOwner.LeaseUntil); await wait.ExecuteNonQueryAsync(); }
        var takeover = owners.AcquireOrReadAsync(hub,0,4,"new","http://localhost:5200",TimeSpan.FromSeconds(30),default);
        release.SetResult();
        Assert.Equal(HubCandidateClaimStatus.OwnerLost,(await oldClaim).Status);
        var newOwner = await takeover; Assert.True(newOwner.Epoch > oldOwner.Epoch);
        var leased = await store.ClaimCandidateAsync(req with { Owner=newOwner, WorkerId="worker-2" },default);
        Assert.Equal(HubCandidateClaimStatus.Leased,leased.Status); Assert.Equal(1,leased.Lease!.Attempts); Assert.Equal(1,leased.Lease.Epoch);
        Assert.Equal(HubCandidateClaimStatus.OwnerLost,(await store.ClaimCandidateAsync(req,default)).Status);
    }

    [Fact]
    public async Task ConcurrentPartitionsSerializeMultipleResourceAdmission()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "candidate-locks-" + Guid.NewGuid().ToString("N"); var first = Guid.NewGuid(); var second = Guid.NewGuid();
        var keyA = hub + "-a"; var keyB = hub + "-b";
        await using var store = new HubTaskStore(database,maxPoolSize:12); var owners = new HubMatchingPartitionLeaseStore(store);
        var o0 = await owners.AcquireOrReadAsync(hub,0,4,"owner0","http://localhost:5100",TimeSpan.FromSeconds(60),default);
        var o1 = await owners.AcquireOrReadAsync(hub,1,4,"owner1","http://localhost:5200",TimeSpan.FromSeconds(60),default);
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        await using (var cmd = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_resource(resource_key,capacity,strict_fifo) VALUES (@a,1,false),(@b,1,false);
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,payload) VALUES (@first,@hub,0,'activity',1,'{}'),(@second,@hub,1,'activity',1,'{}');
            INSERT INTO wfc_scale.hub_task_constraint(task_id,resource_key) VALUES (@first,@b),(@first,@a),(@second,@a),(@second,@b);
            """,db))
        { cmd.Parameters.AddWithValue("a",keyA); cmd.Parameters.AddWithValue("b",keyB); cmd.Parameters.AddWithValue("hub",hub); cmd.Parameters.AddWithValue("first",first); cmd.Parameters.AddWithValue("second",second); await cmd.ExecuteNonQueryAsync(); }
        var reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously); var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var r0 = new HubCandidateClaim(first,0,"w0",hub,0,4,[new(hub,"activity",1)],null,o0,TimeSpan.FromSeconds(30));
        var r1 = new HubCandidateClaim(second,0,"w1",hub,1,4,[new(hub,"activity",1)],null,o1,TimeSpan.FromSeconds(30));
        var c0 = store.ClaimCandidateAsync(r0,default,new HubCandidateClaimTestHooks(BeforeOwnerRecheck: async ct => { reached.SetResult(); await release.Task.WaitAsync(ct); }));
        await reached.Task.WaitAsync(TimeSpan.FromSeconds(10)); var c1 = store.ClaimCandidateAsync(r1,default);
        release.SetResult();
        Assert.Equal(HubCandidateClaimStatus.Leased,(await c0.WaitAsync(TimeSpan.FromSeconds(10))).Status);
        var blocked = await c1.WaitAsync(TimeSpan.FromSeconds(10)); Assert.Equal(HubCandidateClaimStatus.ResourceBlocked,blocked.Status); Assert.Equal(keyA,blocked.ResourceKey);
        await using var check = new NpgsqlCommand("SELECT count(*) FROM wfc_scale.hub_task WHERE id=@id AND state=0 AND epoch=0 AND attempts=0",db);
        check.Parameters.AddWithValue("id",second); Assert.Equal(1L,(long)(await check.ExecuteScalarAsync())!);
    }

    [Fact]
    public async Task CandidateAndLegacyWorkflowJournalAndDispatchKeysMatch()
    {
        var database = Environment.GetEnvironmentVariable("WFC_TEST_PG") ?? throw new InvalidOperationException("WFC_TEST_PG is required");
        var hub = "candidate-journal-" + Guid.NewGuid().ToString("N"); var oldId = Guid.NewGuid(); var newId = Guid.NewGuid(); var w1 = Guid.NewGuid(); var w2 = Guid.NewGuid();
        await using var db = new NpgsqlConnection(database); await db.OpenAsync();
        await using (var cmd = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_workflow(id,hub_id,definition_id,definition_version,partition_id,input_json,step_count,execution_mode,native_status)
            VALUES (@w1,@hub,'definition',1,0,'{}',1,'native','Runnable'),(@w2,@hub,'definition',1,0,'{}',1,'native','Runnable');
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,workflow_id,dispatch_key,payload)
            VALUES (@old,@hub,0,'__native_workflow__',1,@w1,'stable','{}'),(@new,@hub,0,'__native_workflow__',1,@w2,'stable','{}');
            """,db))
        { cmd.Parameters.AddWithValue("hub",hub); cmd.Parameters.AddWithValue("old",oldId); cmd.Parameters.AddWithValue("new",newId); cmd.Parameters.AddWithValue("w1",w1); cmd.Parameters.AddWithValue("w2",w2); await cmd.ExecuteNonQueryAsync(); }
        await using var store = new HubTaskStore(database,enableUnconstrainedBatchClaims:false); var owners = new HubMatchingPartitionLeaseStore(store);
        var owner = await owners.AcquireOrReadAsync(hub,0,4,"owner","http://localhost:5100",TimeSpan.FromSeconds(60),default);
        var caps = new[] { new HubCapability(hub,"__native_workflow__",1) }; var pins = new[] { new HubWorkflowCapability(hub,"definition",1) };
        Assert.Equal(oldId,Assert.Single(await store.ClaimAsync("worker",[hub],caps,1,TimeSpan.FromSeconds(30),default,pins,0,4)).Id);
        var result = await store.ClaimCandidateAsync(new(newId,0,"worker",hub,0,4,caps,pins,owner,TimeSpan.FromSeconds(30)),default);
        Assert.Equal(HubCandidateClaimStatus.Leased,result.Status);
        await using var check = new NpgsqlCommand("""
            SELECT t.id,t.dispatch_key,e.event_type,e.payload->>'worker',e.payload->>'epoch'
            FROM wfc_scale.hub_task t JOIN wfc_scale.hub_workflow_event e ON e.task_id=t.id
            WHERE t.id IN (@old,@new) ORDER BY t.sequence
            """,db);
        check.Parameters.AddWithValue("old",oldId); check.Parameters.AddWithValue("new",newId);
        await using var rows = await check.ExecuteReaderAsync(); var ids = new List<Guid>();
        while (await rows.ReadAsync()) { ids.Add(rows.GetGuid(0)); Assert.Equal("stable",rows.GetString(1)); Assert.Equal("WorkflowTaskClaimed",rows.GetString(2)); Assert.Equal("worker",rows.GetString(3)); Assert.Equal("1",rows.GetString(4)); }
        Assert.Equal(new[] {oldId,newId},ids);
    }

}

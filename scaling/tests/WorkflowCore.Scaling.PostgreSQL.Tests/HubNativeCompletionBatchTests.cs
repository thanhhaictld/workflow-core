using System.Text.Json;
using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubNativeCompletionBatchTests
{
    [Fact]
    public async Task MultipleIndependentResultsCommitTogetherAndReplayIsFenced()
    {
        var pg=Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if(pg is null)return;
        await using var tasks=new HubTaskStore(pg);
        var definitions=new HubDefinitionStore(tasks);
        var native=new HubNativeWorkflowStore(tasks,definitions);
        var store=new HubNativeCompletionBatchStore(tasks);
        var hub="batch-result-"+Guid.NewGuid().ToString("N");
        var value=JsonSerializer.SerializeToElement(new{result=42});
        await definitions.RegisterAsync(new(hub,"native",1,4,1,
            JsonSerializer.SerializeToElement(new{executionMode="native"})),default);
        await definitions.RegisterActivityAsync(new(hub,"act",1,4),default);
        var items=new List<(Guid Workflow,HubLeasedTask Activity)>();
        for(var i=0;i<4;i++)
        {
            var id=await new HubRemoteWorkflowStore(tasks,definitions).StartAsync(
                new(hub,"native",1,JsonSerializer.SerializeToElement(new{})),default);
            var turn=Assert.Single(await tasks.ClaimAsync("worker",[hub],
                [new(hub,"__native_workflow__",1)],1,TimeSpan.FromMinutes(2),default));
            var commit=new HubNativeCommit(1,"worker",turn.Epoch,0,turn.Id,
                JsonSerializer.SerializeToElement(new{}),"Runnable",false,null,[],[],
                [new HubNativeDispatch("dispatch-"+i,"pointer-"+i,i,"act",1,
                    JsonSerializer.SerializeToElement(new{}))],[]);
            Assert.NotNull(await native.CommitAsync(id,commit,default));
            var activity=Assert.Single(await tasks.ClaimAsync("worker",[hub],
                [new(hub,"act",1)],1,TimeSpan.FromMinutes(2),default));
            items.Add((id,activity));
        }

        var requests=items.Select(x=>new HubCompletionBatchItem(
            x.Activity.Id,"worker",x.Activity.Epoch,value)).ToArray();
        var result=await store.CompleteAsync(requests,default);
        Assert.Equal(4,result.Length);
        Assert.All(result,r=>Assert.Equal("completed",r.Status));
        Assert.All(await store.CompleteAsync(requests,default),
            r=>Assert.Equal("completed",r.Status));
        Assert.All(await store.CompleteAsync(requests.Select(r=>r with{Epoch=r.Epoch+1}).ToArray(),default),
            r=>Assert.Equal("fenced",r.Status));
        Assert.All(await store.CompleteAsync(requests.Select(r=>r with{
            Result=JsonSerializer.SerializeToElement(new{result=7})}).ToArray(),default),
            r=>Assert.Equal("fenced",r.Status));
        await using var db=new NpgsqlConnection(pg);
        await db.OpenAsync();
        foreach(var (workflow,activity) in items)
        {
            await using var check=new NpgsqlCommand("""
                SELECT
                    (SELECT count(*) FROM wfc_scale.hub_workflow_event
                     WHERE workflow_id=@workflow AND task_id=@task
                       AND event_type='ActivityResultReceived'),
                    (SELECT count(*) FROM wfc_scale.hub_native_inbox
                     WHERE workflow_id=@workflow AND kind='Activity'),
                    (SELECT count(*) FROM wfc_scale.hub_task
                     WHERE workflow_id=@workflow AND task_type='__native_workflow__' AND state=0),
                    (SELECT count(*) FROM wfc_scale.hub_task_attempt
                     WHERE task_id=@task AND epoch=@epoch
                       AND outcome='Completed' AND finished_at IS NOT NULL)
                """,db);
            check.Parameters.AddWithValue("workflow",workflow);
            check.Parameters.AddWithValue("task",activity.Id);
            check.Parameters.AddWithValue("epoch",activity.Epoch);
            await using var rows=await check.ExecuteReaderAsync();
            Assert.True(await rows.ReadAsync());
            Assert.Equal(1L,rows.GetInt64(0));
            Assert.Equal(1L,rows.GetInt64(1));
            Assert.Equal(1L,rows.GetInt64(2));
            Assert.Equal(1L,rows.GetInt64(3));
        }
    }

    [Fact]
    public async Task LockedWorkflowIsDeferredWhileUnrelatedCompletionCommits()
    {
        var pg=Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if(pg is null)return;
        await using var tasks=new HubTaskStore(pg);
        var definitions=new HubDefinitionStore(tasks);
        var native=new HubNativeWorkflowStore(tasks,definitions);
        var store=new HubNativeCompletionBatchStore(tasks);
        var hub="batch-skip-"+Guid.NewGuid().ToString("N");
        await definitions.RegisterAsync(new(hub,"native",1,4,1,
            JsonSerializer.SerializeToElement(new{executionMode="native"})),default);
        await definitions.RegisterActivityAsync(new(hub,"act",1,4),default);
        async Task<(Guid Workflow,HubLeasedTask Activity)> Prepare(int n)
        {
            var id=await new HubRemoteWorkflowStore(tasks,definitions).StartAsync(
                new(hub,"native",1,JsonSerializer.SerializeToElement(new{})),default);
            var turn=Assert.Single(await tasks.ClaimAsync("worker",[hub],
                [new(hub,"__native_workflow__",1)],1,TimeSpan.FromMinutes(2),default));
            Assert.NotNull(await native.CommitAsync(id,new HubNativeCommit(1,"worker",
                turn.Epoch,0,turn.Id,JsonSerializer.SerializeToElement(new{}),
                "Runnable",false,null,[],[],
                [new HubNativeDispatch("dispatch-"+n,"pointer-"+n,n,"act",1,
                    JsonSerializer.SerializeToElement(new{}))],[]),default));
            return (id,Assert.Single(await tasks.ClaimAsync("worker",[hub],
                [new(hub,"act",1)],1,TimeSpan.FromMinutes(2),default)));
        }
        var blocked=await Prepare(1);
        var free=await Prepare(2);
        await using var db=new NpgsqlConnection(pg);
        await db.OpenAsync();
        await using var tx=await db.BeginTransactionAsync();
        await using(var hold=new NpgsqlCommand("""
            SELECT id FROM wfc_scale.hub_workflow WHERE id=@id FOR UPDATE
            """,db,tx))
        {
            hold.Parameters.AddWithValue("id",blocked.Workflow);
            Assert.Equal(blocked.Workflow,await hold.ExecuteScalarAsync());
        }
        var payload=JsonSerializer.SerializeToElement(new{value=1});
        HubCompletionBatchItem Request(HubLeasedTask task)=>
            new(task.Id,"worker",task.Epoch,payload);
        using var timeout=new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var result=await store.CompleteAsync([Request(blocked.Activity),Request(free.Activity)],
            timeout.Token);
        Assert.Equal("retry",result[0].Status);
        Assert.Equal("completed",result[1].Status);
        await tx.CommitAsync();
        Assert.Equal("completed",(await store.CompleteAsync([Request(blocked.Activity)],
            default))[0].Status);
        Assert.Equal("completed",(await store.CompleteAsync([Request(free.Activity)],
            default))[0].Status);
    }

    [Fact]
    public async Task InvalidBatchIsRejectedBeforeDatabaseWrites()
    {
        var pg=Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if(pg is null)return;
        await using var tasks=new HubTaskStore(pg);
        var store=new HubNativeCompletionBatchStore(tasks);
        var data=JsonSerializer.SerializeToElement(new{value=1});
        var item=new HubCompletionBatchItem(Guid.NewGuid(),"worker",1,data);
        await Assert.ThrowsAsync<ArgumentException>(()=>
            store.CompleteAsync([item,item],default));
        await Assert.ThrowsAsync<ArgumentException>(()=>
            store.CompleteAsync([],default));
    }
}

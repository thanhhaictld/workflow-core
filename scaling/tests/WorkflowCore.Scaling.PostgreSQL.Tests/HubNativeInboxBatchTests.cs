using System.Text.Json;
using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubNativeInboxBatchTests
{
    [Fact]
    public async Task RejectingUnknownInboxIdRollsBackAllNativeCommitWrites()
    {
        var database=Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if(database is null)return;
        await using var tasks=new HubTaskStore(database);
        var definitions=new HubDefinitionStore(tasks);
        var remote=new HubRemoteWorkflowStore(tasks,definitions);
        var native=new HubNativeWorkflowStore(tasks,definitions);
        var hub="inbox-atomic-"+Guid.NewGuid().ToString("N");
        var empty=JsonSerializer.SerializeToElement(new {});
        await definitions.RegisterAsync(new(hub,"native",1,1,1,
            JsonSerializer.SerializeToElement(new {executionMode="native"})),default);
        var workflow=await remote.StartAsync(new(hub,"native",1,empty),default);
        var lease=Assert.Single(await tasks.ClaimAsync("worker",[hub],
            [new(hub,"__native_workflow__",1)],1,TimeSpan.FromMinutes(2),default));
        await using var db=new NpgsqlConnection(database);
        await db.OpenAsync();
        long message;
        await using(var insert=new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_native_inbox
                (workflow_id,pointer_id,kind,source_key,payload)
            VALUES(@id,'p','Event','source','{}'::jsonb) RETURNING id
            """,db))
        {
            insert.Parameters.AddWithValue("id",workflow);
            message=(long)(await insert.ExecuteScalarAsync()!);
        }
        var request=new HubNativeCommit(1,"worker",lease.Epoch,0,lease.Id,empty,
            "Runnable",false,null,[message,message+1000000],[],[],[]);
        await Assert.ThrowsAsync<ArgumentException>(
            ()=>native.CommitAsync(workflow,request,default));
        await using(var check=new NpgsqlCommand("""
            SELECT (SELECT consumed FROM wfc_scale.hub_native_inbox WHERE id=@message),
                   (SELECT state FROM wfc_scale.hub_task WHERE id=@task),
                   (SELECT revision FROM wfc_scale.hub_workflow WHERE id=@id)
            """,db))
        {
            check.Parameters.AddWithValue("message",message);
            check.Parameters.AddWithValue("task",lease.Id);
            check.Parameters.AddWithValue("id",workflow);
            await using var r=await check.ExecuteReaderAsync();
            Assert.True(await r.ReadAsync());
            Assert.False(r.GetBoolean(0));
            Assert.Equal(1,r.GetInt16(1));
            Assert.Equal(0,r.GetInt64(2));
        }
        var valid=request with {ConsumedInbox=[message]};
        Assert.NotNull(await native.CommitAsync(workflow,valid,default));
        Assert.True((await native.CommitAsync(workflow,valid,default))!.Replayed);
    }
}

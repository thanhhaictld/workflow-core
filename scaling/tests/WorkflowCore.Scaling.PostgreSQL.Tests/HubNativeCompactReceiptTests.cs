using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubNativeCompactReceiptTests
{
    [Fact]
    public async Task BothLegacyAndHashedReceiptsReplayAcrossFeatureModes()
    {
        var database=Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if(database is null)return;
        await using var tasks=new HubTaskStore(database);
        var definitions=new HubDefinitionStore(tasks);
        var remote=new HubRemoteWorkflowStore(tasks,definitions);
        var native=new HubNativeWorkflowStore(tasks,definitions);
        var hub="receipt-interop-"+Guid.NewGuid().ToString("N");
        var empty=JsonSerializer.SerializeToElement(new {});
        await definitions.RegisterAsync(new(hub,"native",1,1,1,
            JsonSerializer.SerializeToElement(new{executionMode="native"})),default);
        var id=await remote.StartAsync(new(hub,"native",1,empty),default);
        var lease=Assert.Single(await tasks.ClaimAsync("worker",[hub],
            [new(hub,"__native_workflow__",1)],1,TimeSpan.FromMinutes(2),default));
        var commit=new HubNativeCommit(1,"worker",lease.Epoch,0,lease.Id,empty,
            "Runnable",false,null,[],[],[],[]);
        Assert.NotNull(await native.CommitAsync(id,commit,default));
        await using var db=new NpgsqlConnection(database);
        await db.OpenAsync();
        async Task SetReceipt(string json)
        {
            await using var cmd=new NpgsqlCommand("""
                UPDATE wfc_scale.hub_task SET result_json=@value::jsonb WHERE id=@id
                """,db);
            cmd.Parameters.AddWithValue("value",json);
            cmd.Parameters.AddWithValue("id",lease.Id);
            Assert.Equal(1,await cmd.ExecuteNonQueryAsync());
        }
        var canonical=JsonSerializer.Serialize(commit);
        var hash=Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(canonical)))
            .ToLowerInvariant();
        var legacy=JsonSerializer.Serialize(new{commit,revision=1,status="Runnable"});
        var compact=JsonSerializer.Serialize(new{receiptVersion=2,commitHash=hash,
            revision=1,status="Runnable"});

        // Simulate both rolling-deployment receipt formats within one process.
        foreach(var receipt in new[]{legacy,compact})
        {
            await SetReceipt(receipt);
            var exact=await native.CommitAsync(id,commit,default);
            Assert.NotNull(exact);
            Assert.True(exact.Replayed);
            Assert.Equal(1,exact.Revision);
            Assert.Null(await native.CommitAsync(id,commit with {
                Snapshot=JsonSerializer.SerializeToElement(new{different=true})
            },default));
        }
    }
}

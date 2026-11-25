using System.Text.Json;
using Npgsql;
using WorkflowCore.Hub;
using Xunit;

namespace WorkflowCore.Scaling.PostgreSQL.Tests;

public sealed class HubNativeCompletionBatchTests
{
    [Fact]
    public async Task BatchAndSingleShareFencingReplayAndDurableHistory()
    {
        var connection=Environment.GetEnvironmentVariable("WFC_TEST_PG");
        if(connection is null)return;
        await using var tasks=new HubTaskStore(connection);
        var native=new HubNativeWorkflowStore(tasks,new HubDefinitionStore(tasks));
        var hub="test-batch-native-"+Guid.NewGuid().ToString("N");
        var ids=await Seed(connection,hub,3);
        var payload=JsonSerializer.SerializeToElement(new { result = "ok" });

        // A stale attempt must not prevent other, independent tasks from
        // committing within the same transaction.
        var first=await native.CompleteActivitiesBatchAsync([
            new(ids[0],"bench-worker",1,payload),
            new(ids[1],"bench-worker",1,payload),
            new(ids[2],"wrong-worker",1,payload)
        ],default);
        Assert.Equal([true,true,false],first);
        Assert.Equal((2L,2L,2L,2L,2L),await Counts(connection,hub));

        // Exact replay succeeds without duplicating attempt/history/wake rows.
        var replay=await native.CompleteActivitiesBatchAsync([
            new(ids[0],"bench-worker",1,payload),
            new(ids[1],"bench-worker",1,payload)
        ],default);
        Assert.Equal([true,true],replay);
        Assert.Equal((2L,2L,2L,2L,2L),await Counts(connection,hub));

        // The original single-completion path remains fully compatible.
        Assert.False(await native.CompleteActivityAsync(ids[2],"bench-worker",2,payload,default));
        Assert.True(await native.CompleteActivityAsync(ids[2],"bench-worker",1,payload,default));
        Assert.Equal((3L,3L,3L,3L,3L),await Counts(connection,hub));
        var wrongResult=JsonSerializer.SerializeToElement(new { result = "incorrect" });
        Assert.False(await native.CompleteActivityAsync(ids[2],"bench-worker",1,wrongResult,default));
        Assert.Equal((3L,3L,3L,3L,3L),await Counts(connection,hub));
        await Assert.ThrowsAsync<ArgumentException>(()=>native.CompleteActivitiesBatchAsync([
            new(ids[0],"bench-worker",1,payload),new(ids[0],"bench-worker",1,payload)
        ],default));
    }

    private static async Task<Guid[]> Seed(string connection,string hub,int count)
    {
        await using var db=new NpgsqlConnection(connection);
        await db.OpenAsync();
        await using(var cmd=new NpgsqlCommand("""
            WITH workflows AS (
                INSERT INTO wfc_scale.hub_workflow
                  (id,hub_id,definition_id,definition_version,partition_id,input_json,
                   step_count,state,execution_mode,native_status)
                SELECT gen_random_uuid(),@hub,'native-batch-test',1,0,'{}'::jsonb,1,1,'native','Runnable'
                FROM generate_series(1,@count)
                RETURNING id,partition_id
            ), tasks AS (
                INSERT INTO wfc_scale.hub_task
                  (id,hub_id,partition_id,task_type,task_version,workflow_id,payload,
                   state,owner,last_owner,epoch,lease_until,attempts,native_step_id,
                   dispatch_key,execution_pointer_id,admission)
                SELECT gen_random_uuid(),@hub,0,'test-activity',1,w.id,'{}'::jsonb,
                   1,'bench-worker','bench-worker',1,now()+interval '30 minutes',1,
                   0,w.id::text,'pointer-'||w.id::text,1
                FROM workflows w
                RETURNING id,workflow_id
            )
            INSERT INTO wfc_scale.hub_task_attempt
               (task_id,epoch,workflow_id,step_index,attempt_number,worker_id)
            SELECT id,1,workflow_id,0,1,'bench-worker' FROM tasks
            """,db))
        {
            cmd.Parameters.AddWithValue("hub",hub);
            cmd.Parameters.AddWithValue("count",count);
            await cmd.ExecuteNonQueryAsync();
        }
        await using var query=new NpgsqlCommand("""
            SELECT id FROM wfc_scale.hub_task WHERE hub_id=@hub AND task_type='test-activity' ORDER BY id
            """,db);
        query.Parameters.AddWithValue("hub",hub);
        await using var reader=await query.ExecuteReaderAsync();
        var ids=new List<Guid>();
        while(await reader.ReadAsync())ids.Add(reader.GetGuid(0));
        Assert.Equal(count,ids.Count);
        return ids.ToArray();
    }

    private static async Task<(long Completed,long Attempts,long Inbox,long History,long Wakes)> Counts(
        string connection,string hub)
    {
        await using var db=new NpgsqlConnection(connection);
        await db.OpenAsync();
        await using var cmd=new NpgsqlCommand("""
            SELECT
              (SELECT count(*) FROM wfc_scale.hub_task WHERE hub_id=@hub AND task_type='test-activity' AND state=2),
              (SELECT count(*) FROM wfc_scale.hub_task_attempt a JOIN wfc_scale.hub_task t ON t.id=a.task_id
               WHERE t.hub_id=@hub AND a.outcome='Completed' AND a.finished_at IS NOT NULL),
              (SELECT count(*) FROM wfc_scale.hub_native_inbox i JOIN wfc_scale.hub_workflow w ON w.id=i.workflow_id
               WHERE w.hub_id=@hub AND i.kind='Activity'),
              (SELECT count(*) FROM wfc_scale.hub_workflow_event e JOIN wfc_scale.hub_workflow w ON w.id=e.workflow_id
               WHERE w.hub_id=@hub AND e.event_type='ActivityResultReceived'),
              (SELECT count(*) FROM wfc_scale.hub_task WHERE hub_id=@hub AND task_type='__native_workflow__' AND state=0)
            """,db);
        cmd.Parameters.AddWithValue("hub",hub);
        await using var reader=await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        return(reader.GetInt64(0),reader.GetInt64(1),reader.GetInt64(2),reader.GetInt64(3),reader.GetInt64(4));
    }
}

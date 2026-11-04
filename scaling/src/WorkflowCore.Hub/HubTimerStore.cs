using Npgsql;
using System.Text.Json;

namespace WorkflowCore.Hub;

/// <summary>
/// PostgreSQL timer arbitration. The scheduler never runs user code.
/// Lock order is workflow -> timer -> task, also used by event dispatch.
/// </summary>
public interface IHubTimerStore
{
    Task<int> FireDueAsync(int batchSize,CancellationToken ct);
    Task<HubRemoteTimerMetrics> MetricsAsync(CancellationToken ct);
}

public sealed class HubTimerStore(HubTaskStore tasks) : IHubTimerStore
{
    private NpgsqlDataSource Source=>tasks.DataSource;

    public async Task<int> FireDueAsync(int batchSize,CancellationToken ct)
    {
        if(batchSize is < 1 or > 1000)
            throw new ArgumentOutOfRangeException(nameof(batchSize));
        await HubNativeWorkflowStore.SweepInboxAsync(tasks,batchSize,ct);
        var candidates=new List<(Guid Id,Guid Workflow)>();
        await using(var cmd=Source.CreateCommand("""
            SELECT id,workflow_id FROM wfc_scale.hub_timer
            WHERE state=0 AND due_at<=now()
            ORDER BY due_at,partition_id,id
            LIMIT @take
            """))
        {
            cmd.Parameters.AddWithValue("take",batchSize*2);
            await using var reader=await cmd.ExecuteReaderAsync(ct);
            while(await reader.ReadAsync(ct))
                candidates.Add((reader.GetGuid(0),reader.GetGuid(1)));
        }
        var fired=0;
        foreach(var candidate in candidates)
        {
            if(fired>=batchSize)break;
            if(await FireOneAsync(candidate.Id,candidate.Workflow,ct))fired++;
        }
        return fired;
    }

    private async Task<bool> FireOneAsync(Guid timerId,Guid workflowId,CancellationToken ct)
    {
        await using var db=await Source.OpenConnectionAsync(ct);
        await using var tx=await db.BeginTransactionAsync(ct);
        string hub,waitingFor,mode;int partition,step;long revision;short workflowState;

        // Skip a workflow claimed by another scanner or state transition.
        // Select workflow lock FIRST; timer commands/events take this same lock.
        await using(var q=new NpgsqlCommand("""
            SELECT hub_id,partition_id,next_step,revision,state,waiting_for,execution_mode
            FROM wfc_scale.hub_workflow
            WHERE id=@id AND execution_mode IN ('remote','native')
            FOR UPDATE SKIP LOCKED
            """,db,tx))
        {
            q.Parameters.AddWithValue("id",workflowId);
            await using var r=await q.ExecuteReaderAsync(ct);
            if(!await r.ReadAsync(ct))return false;
            hub=r.GetString(0);partition=r.GetInt32(1);step=r.GetInt32(2);
            revision=r.GetInt64(3);workflowState=r.GetInt16(4);
            waitingFor=r.IsDBNull(5)?"":r.GetString(5);
            mode=r.GetString(6);
        }
        int timerStep;long scheduledRevision;DateTime due;short timerState;
        string timerKey;string? pointerId;
        await using(var q=new NpgsqlCommand("""
            SELECT step_index,scheduled_revision,due_at,state,timer_key,execution_pointer_id,due_at<=clock_timestamp()
            FROM wfc_scale.hub_timer WHERE id=@id AND workflow_id=@workflow
            FOR UPDATE SKIP LOCKED
            """,db,tx))
        {
            q.Parameters.AddWithValue("id",timerId);
            q.Parameters.AddWithValue("workflow",workflowId);
            await using var r=await q.ExecuteReaderAsync(ct);
            if(!await r.ReadAsync(ct))return false;
            timerStep=r.GetInt32(0);scheduledRevision=r.GetInt64(1);
            due=r.GetDateTime(2);timerState=r.GetInt16(3);
            timerKey=r.GetString(4);
            pointerId=r.IsDBNull(5)?null:r.GetString(5);
            if(!r.GetBoolean(6))return false;
        }
        if(timerState!=0)return false;
        if(workflowState>=2 || revision!=scheduledRevision || (mode=="remote" &&
           (waitingFor!="timer:"+timerId.ToString("D") || step!=timerStep)))
        {
            await using var cancel=new NpgsqlCommand("""
                UPDATE wfc_scale.hub_timer
                SET state=2,cancelled_at=clock_timestamp()
                WHERE id=@id AND state=0
                """,db,tx);
            cancel.Parameters.AddWithValue("id",timerId);
            if(await cancel.ExecuteNonQueryAsync(ct)==1)
                await HubRemoteWorkflowStore.JournalAsync(db,tx,workflowId,timerStep,null,
                    timerKey,"TimerCancelled",JsonSerializer.Serialize(new{timerId}),ct);
            await tx.CommitAsync(ct);
            return false;
        }
        await using(var fire=new NpgsqlCommand("""
            UPDATE wfc_scale.hub_timer
            SET state=1,fired_at=clock_timestamp()
            WHERE id=@id AND state=0 AND due_at<=clock_timestamp()
            """,db,tx))
        {
            fire.Parameters.AddWithValue("id",timerId);
            if(await fire.ExecuteNonQueryAsync(ct)!=1)return false;
        }
        if(mode=="native")
        {
            await HubNativeWorkflowStore.Execute(db,tx,"""
                INSERT INTO wfc_scale.hub_workflow_event(workflow_id,step_index,activity_type,event_type,execution_pointer_id,payload)
                VALUES(@workflow,@step,'__native_workflow__','TimerFired',@pointer,jsonb_build_object('timerId',@timer))
                """,ct,("workflow",workflowId),("step",timerStep),("pointer",pointerId),("timer",timerId));
            await HubNativeWorkflowStore.WakeAsync(db,tx,workflowId,hub,partition,ct);
            await tx.CommitAsync(ct);
            return true;
        }
        await using(var update=new NpgsqlCommand("""
            UPDATE wfc_scale.hub_workflow
            SET next_step=next_step+1,revision=revision+1,
                waiting_for=NULL,updated_at=now(),
                results_json=jsonb_set(results_json,ARRAY[@key],
                    jsonb_build_object('timerFired',true),true)
            WHERE id=@id AND revision=@revision
            """,db,tx))
        {
            update.Parameters.AddWithValue("id",workflowId);
            update.Parameters.AddWithValue("revision",revision);
            update.Parameters.AddWithValue("key",step.ToString());
            if(await update.ExecuteNonQueryAsync(ct)!=1)
                throw new InvalidOperationException("Workflow changed during timer firing");
        }
        await HubRemoteWorkflowStore.JournalAsync(db,tx,workflowId,step,null,timerKey,
            "TimerFired",JsonSerializer.Serialize(new{timerId}),ct);
        await HubRemoteWorkflowStore.WakeProcessorAsync(db,tx,workflowId,hub,
            partition,revision+1,ct);
        await tx.CommitAsync(ct);
        HubTelemetry.TimerFired.Add(1,HubTelemetry.HubTag(hub));
        HubTelemetry.TimerFireLag.Record(Math.Max(0,
            (DateTime.UtcNow-due).TotalSeconds),HubTelemetry.HubTag(hub));
        return true;
    }

    public async Task<HubRemoteTimerMetrics> MetricsAsync(CancellationToken ct)
    {
        await using var cmd=Source.CreateCommand("""
            SELECT count(*) FILTER (WHERE state=0),
                   count(*) FILTER (WHERE state=0 AND due_at<=now()),
                   count(*) FILTER (WHERE state=1)
            FROM wfc_scale.hub_timer
            """);
        await using var reader=await cmd.ExecuteReaderAsync(ct);
        if(!await reader.ReadAsync(ct))return new(0,0,0);
        return new(reader.GetInt64(0),reader.GetInt64(1),reader.GetInt64(2));
    }
}

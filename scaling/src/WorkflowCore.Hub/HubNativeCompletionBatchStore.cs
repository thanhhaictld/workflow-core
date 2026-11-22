using System.Diagnostics;
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace WorkflowCore.Hub;

/// <summary>
/// Experimental PostgreSQL microbatch for independent native activity results.
/// The worker keeps each result/epoch immutable until a committed acknowledgement.
/// A row which cannot be locked immediately is deferred; it must be retried with
/// the exact same id, owner, epoch and result. The original single-task endpoint
/// remains the default and the recovery path.
/// </summary>
public sealed record HubCompletionBatchItem(Guid TaskId, string WorkerId, long Epoch, JsonElement Result);
public sealed record HubCompletionBatchRequest(HubCompletionBatchItem[] Items);
public sealed record HubCompletionBatchItemResult(Guid TaskId, string Status, string? HubId = null);

public sealed class HubNativeCompletionBatchStore(HubTaskStore tasks)
{
    private sealed record Metadata(Guid Task, Guid Workflow, string Hub, int Partition,
        string Status, string Pointer, string Dispatch, int Step);
    private sealed record LockedTask(Guid Task, Guid Workflow, short State, string? Owner,
        string? LastOwner, long Epoch, DateTime? LeaseUntil, string? Result);
    private sealed record Eligible(HubCompletionBatchItem Item, Metadata Metadata);
    private static JsonElement Json(string json) => JsonDocument.Parse(json).RootElement.Clone();

    // One transaction covers many INDEPENDENT workflow results. Skip-locked
    // selection avoids waiting on workflow/task row contention; SQL lock_timeout
    // limits less obvious index/unique/trigger contention to 100ms.
    private const string WritesSql = """
        WITH input AS MATERIALIZED (
          SELECT * FROM jsonb_to_recordset(@items::jsonb) AS x(
            task uuid, worker text, epoch bigint, result jsonb,
            workflow uuid, pointer text, dispatch text, step integer,
            hub text, partition integer, wake uuid, journal jsonb)
        ), finished AS (
          UPDATE wfc_scale.hub_task t SET state=2, owner=NULL, lease_until=NULL,
            result_json=i.result, updated_at=now()
          FROM input i
          WHERE t.id=i.task AND t.workflow_id=i.workflow
            AND t.owner=i.worker AND t.epoch=i.epoch
            AND CASE t.state WHEN 1 THEN t.lease_until>clock_timestamp() ELSE false END
          RETURNING t.id
        ), attempts AS (
          UPDATE wfc_scale.hub_task_attempt a
            SET outcome='Completed', finished_at=clock_timestamp()
          FROM finished f JOIN input i ON i.task=f.id
          WHERE a.task_id=f.id AND a.epoch=i.epoch AND a.finished_at IS NULL
          RETURNING a.task_id
        ), inbox AS (
          INSERT INTO wfc_scale.hub_native_inbox(workflow_id,pointer_id,kind,source_key,payload)
          SELECT i.workflow,i.pointer,'Activity',i.dispatch,i.result
          FROM finished f JOIN input i ON i.task=f.id
          ON CONFLICT DO NOTHING RETURNING workflow_id
        ), events AS (
          INSERT INTO wfc_scale.hub_workflow_event
            (workflow_id,step_index,task_id,activity_type,event_type,payload)
          SELECT i.workflow,i.step,f.id,'__native_activity__','ActivityResultReceived',i.journal
          FROM finished f JOIN input i ON i.task=f.id
          RETURNING task_id
        ), wake AS (
          INSERT INTO wfc_scale.hub_task
            (id,hub_id,partition_id,task_type,task_version,workflow_id,payload,admission)
          SELECT i.wake,i.hub,i.partition,'__native_workflow__',1,i.workflow,'{}'::jsonb,1
          FROM finished f JOIN input i ON i.task=f.id
          JOIN wfc_scale.hub_workflow w ON w.id=i.workflow AND w.native_status='Runnable'
          ON CONFLICT DO NOTHING RETURNING id
        )
        SELECT id FROM finished
        """;

    public async Task<HubCompletionBatchItemResult[]> CompleteAsync(
        IReadOnlyList<HubCompletionBatchItem> requests, CancellationToken ct)
    {
        if (requests.Count is < 1 or > 64 ||
            requests.Any(r => r.TaskId == Guid.Empty || r.Epoch < 1 ||
                string.IsNullOrWhiteSpace(r.WorkerId) || r.WorkerId.Length > 128 ||
                r.Result.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined ||
                r.Result.GetRawText().Length > 65536) ||
            requests.Select(r => r.TaskId).Distinct().Count() != requests.Count)
            throw new ArgumentException("Invalid completion microbatch");
        var timer=Stopwatch.GetTimestamp();
        var output=new Dictionary<Guid,HubCompletionBatchItemResult>();
        void Set(Guid id,string status,string? hub=null) =>
            output[id]=new HubCompletionBatchItemResult(id,status,hub);
        await using var db=await tasks.DataSource.OpenConnectionAsync(ct);
        await using var tx=await db.BeginTransactionAsync(ct);
        try
        {
            // Protect the whole batch from a single row/index/trigger blocker.
            // PostgreSQL will abort this transaction on timeout: report Retry
            // for every item, never ACK a result from an aborted transaction.
            await using(var timeout=new NpgsqlCommand("SET LOCAL lock_timeout = '100ms'",db,tx))
                await timeout.ExecuteNonQueryAsync(ct);
            var ids=requests.Select(r=>r.TaskId).ToArray();
            var metadata=new Dictionary<Guid,Metadata>();
            await using(var query=new NpgsqlCommand("""
                SELECT t.id,w.id,w.hub_id,w.partition_id,w.native_status,
                       t.execution_pointer_id,t.dispatch_key,t.native_step_id
                FROM wfc_scale.hub_task t
                JOIN wfc_scale.hub_workflow w ON w.id=t.workflow_id
                WHERE t.id=ANY(@tasks) AND w.execution_mode='native'
                  AND t.dispatch_key IS NOT NULL
                """,db,tx))
            {
                query.Parameters.AddWithValue("tasks",ids);
                await using var reader=await query.ExecuteReaderAsync(ct);
                while(await reader.ReadAsync(ct))
                {
                    var info=new Metadata(reader.GetGuid(0),reader.GetGuid(1),
                        reader.GetString(2),reader.GetInt32(3),reader.GetString(4),
                        reader.GetString(5),reader.GetString(6),reader.GetInt32(7));
                    metadata[info.Task]=info;
                }
            }

            // Never couple two concurrent activities from the same workflow.
            // One result per workflow may enter this transaction; the rest
            // are returned as Retry for a subsequent flush.
            var selected=new List<HubCompletionBatchItem>();
            var workflows=new HashSet<Guid>();
            foreach(var item in requests)
            {
                if(!metadata.TryGetValue(item.TaskId,out var m))
                    Set(item.TaskId,"fenced");
                else if(!workflows.Add(m.Workflow))
                    Set(item.TaskId,"retry");
                else selected.Add(item);
            }

            if(selected.Count > 0)
            {
                var workflowIds=selected.Select(x=>metadata[x.TaskId].Workflow).ToArray();
                var lockedWorkflows=new HashSet<Guid>();
                await using(var cmd=new NpgsqlCommand("""
                    SELECT id FROM wfc_scale.hub_workflow
                    WHERE id=ANY(@ids)
                    ORDER BY id FOR UPDATE SKIP LOCKED
                    """,db,tx))
                {
                    cmd.Parameters.AddWithValue("ids",workflowIds);
                    await using var reader=await cmd.ExecuteReaderAsync(ct);
                    while(await reader.ReadAsync(ct))lockedWorkflows.Add(reader.GetGuid(0));
                }
                var toLock=selected.Where(x=>lockedWorkflows.Contains(metadata[x.TaskId].Workflow))
                    .Select(x=>x.TaskId).ToArray();
                var locked=new Dictionary<Guid,LockedTask>();
                if(toLock.Length>0)
                {
                    await using var cmd=new NpgsqlCommand("""
                        SELECT t.id,t.workflow_id,t.state,t.owner,t.last_owner,t.epoch,
                               t.lease_until,t.result_json::text
                        FROM wfc_scale.hub_task t
                        WHERE t.id=ANY(@ids)
                        ORDER BY t.id FOR UPDATE SKIP LOCKED
                        """,db,tx);
                    cmd.Parameters.AddWithValue("ids",toLock);
                    await using var reader=await cmd.ExecuteReaderAsync(ct);
                    while(await reader.ReadAsync(ct))
                    {
                        var row=new LockedTask(reader.GetGuid(0),reader.GetGuid(1),
                            reader.GetInt16(2),reader.IsDBNull(3)?null:reader.GetString(3),
                            reader.IsDBNull(4)?null:reader.GetString(4),reader.GetInt64(5),
                            reader.IsDBNull(6)?null:reader.GetDateTime(6),
                            reader.IsDBNull(7)?null:reader.GetString(7));
                        locked[row.Task]=row;
                    }
                }
                var eligible=new List<Eligible>();
                foreach(var item in selected)
                {
                    var m=metadata[item.TaskId];
                    if(!lockedWorkflows.Contains(m.Workflow) || !locked.TryGetValue(item.TaskId,out var row))
                    {
                        Set(item.TaskId,"retry");
                        continue;
                    }
                    if(row.Workflow!=m.Workflow)
                    {
                        Set(item.TaskId,"retry");
                        continue;
                    }
                    if(row.State==2 && row.Epoch==item.Epoch && row.LastOwner==item.WorkerId
                        && row.Result is not null &&
                        JsonElement.DeepEquals(Json(row.Result),item.Result))
                    {
                        Set(item.TaskId,"completed",m.Hub);
                        continue;
                    }
                    if(row.State!=1 || row.Owner!=item.WorkerId || row.Epoch!=item.Epoch ||
                       row.LeaseUntil<=DateTime.UtcNow || m.Status is "Complete" or "Terminated")
                    {
                        Set(item.TaskId,"fenced");
                        continue;
                    }
                    eligible.Add(new Eligible(item,m));
                }
                if(eligible.Count>0)
                {
                    var json=JsonSerializer.Serialize(eligible.Select(e=>new
                    {
                        task=e.Item.TaskId,worker=e.Item.WorkerId,epoch=e.Item.Epoch,
                        result=e.Item.Result,workflow=e.Metadata.Workflow,
                        pointer=e.Metadata.Pointer,dispatch=e.Metadata.Dispatch,
                        step=e.Metadata.Step,hub=e.Metadata.Hub,partition=e.Metadata.Partition,
                        wake=Guid.NewGuid(),
                        journal=new{executionPointerId=e.Metadata.Pointer,dispatchKey=e.Metadata.Dispatch}
                    }));
                    var committed=new HashSet<Guid>();
                    await using(var cmd=new NpgsqlCommand(WritesSql,db,tx))
                    {
                        cmd.Parameters.Add("items",NpgsqlDbType.Jsonb).Value=json;
                        await using var reader=await cmd.ExecuteReaderAsync(ct);
                        while(await reader.ReadAsync(ct))committed.Add(reader.GetGuid(0));
                    }
                    foreach(var e in eligible)
                        Set(e.Item.TaskId,committed.Contains(e.Item.TaskId)?"completed":"retry",
                            committed.Contains(e.Item.TaskId)?e.Metadata.Hub:null);
                }
            }
            await tx.CommitAsync(ct);
        }
        catch(PostgresException ex) when(ex.SqlState is "55P03" or "40P01" or "40001")
        {
            // No result from a failed transaction can be acknowledged. The
            // caller retries the frozen result through the same fencing gate.
            foreach(var r in requests)Set(r.TaskId,"retry");
        }
        HubTelemetry.NativeCompletionBatchSize.Record(requests.Count);
        HubTelemetry.NativeCompletionBatchDuration.Record(
            Stopwatch.GetElapsedTime(timer).TotalSeconds);
        foreach(var group in output.Values.GroupBy(x=>x.Status))
            HubTelemetry.NativeCompletionBatchOutcomes.Add(group.Count(),
                new KeyValuePair<string,object?>("outcome",group.Key));
        return requests.Select(x=>output[x.TaskId]).ToArray();
    }
}

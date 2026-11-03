using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace WorkflowCore.Hub;

/// <summary>
/// Durable, generic remote workflow task state machine. C# workflow decisions
/// come only from independent processor hosts. All commits are revision- and
/// lease-fenced PostgreSQL transactions; there is no business logic here.
/// </summary>
public sealed class HubRemoteWorkflowStore(HubTaskStore tasks, HubDefinitionStore definitions)
{
    internal NpgsqlDataSource Source => tasks.DataSource;
    private static readonly string[] States = ["Ready", "Running", "Complete", "Failed"];

    public async Task<Guid> StartAsync(HubWorkflowStartRequest request, CancellationToken ct)
    {
        var definition = await definitions.GetAsync(request.HubId, request.DefinitionId,
            request.Version, ct) ?? throw new KeyNotFoundException("Unknown remote workflow definition");
        if (request.Input.ValueKind != JsonValueKind.Object ||
            request.Input.GetRawText().Length > 65536 ||
            request.IdempotencyKey?.Length > 128 || request.RunId?.Length > 100)
            throw new ArgumentException("Invalid workflow start request");

        var id = Guid.NewGuid();
        var partition = HubPartitionRouter.ForWorkflow(id, definition.Partitions);
        await using var db = await Source.OpenConnectionAsync(ct);
        await using var tx = await db.BeginTransactionAsync(ct);
        await using (var cmd = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_workflow
              (id,hub_id,definition_id,definition_version,partition_id,
               input_json,step_count,run_id,idempotency_key,execution_mode)
            VALUES (@id,@hub,@definition,@version,@partition,
                    @input,@steps,@run,@key,'remote')
            ON CONFLICT DO NOTHING RETURNING id
            """, db, tx))
        {
            cmd.Parameters.AddWithValue("id", id);
            cmd.Parameters.AddWithValue("hub", request.HubId);
            cmd.Parameters.AddWithValue("definition", request.DefinitionId);
            cmd.Parameters.AddWithValue("version", request.Version);
            cmd.Parameters.AddWithValue("partition", partition);
            cmd.Parameters.Add("input", NpgsqlDbType.Jsonb).Value = request.Input.GetRawText();
            cmd.Parameters.AddWithValue("steps", definition.StepCount);
            cmd.Parameters.Add("run", NpgsqlDbType.Text).Value = (object?)request.RunId ?? DBNull.Value;
            cmd.Parameters.Add("key", NpgsqlDbType.Text).Value =
                (object?)request.IdempotencyKey ?? DBNull.Value;
            var inserted = await cmd.ExecuteScalarAsync(ct);
            if (inserted is null)
            {
                if (request.IdempotencyKey is null)
                    throw new InvalidOperationException("Workflow identifier collision");
                await using var existing = new NpgsqlCommand("""
                    SELECT id FROM wfc_scale.hub_workflow
                    WHERE hub_id=@hub AND idempotency_key=@key
                      AND definition_id=@definition AND definition_version=@version
                      AND execution_mode='remote' AND input_json=@input
                      AND run_id IS NOT DISTINCT FROM @run
                    """, db, tx);
                existing.Parameters.AddWithValue("hub", request.HubId);
                existing.Parameters.AddWithValue("key", request.IdempotencyKey);
                existing.Parameters.AddWithValue("definition", request.DefinitionId);
                existing.Parameters.AddWithValue("version", request.Version);
                existing.Parameters.Add("input", NpgsqlDbType.Jsonb).Value = request.Input.GetRawText();
                existing.Parameters.Add("run", NpgsqlDbType.Text).Value =
                    (object?)request.RunId ?? DBNull.Value;
                var previous = await existing.ExecuteScalarAsync(ct);
                if (previous is null)
                    throw new InvalidOperationException("Workflow idempotency key reused with different request");
                await tx.CommitAsync(ct);
                return (Guid)previous;
            }
        }
        await WakeProcessorAsync(db, tx, id, request.HubId, partition, 0, ct);
        await JournalAsync(db, tx, id, 0, null, "__workflow__",
            "WorkflowStarted", JsonSerializer.Serialize(new { definition.Version }), ct);
        await tx.CommitAsync(ct);
        HubTelemetry.WorkflowsStarted.Add(1, HubTelemetry.HubTag(request.HubId));
        return id;
    }

    public async Task<HubWorkflowRuntime?> GetRuntimeAsync(Guid id, CancellationToken ct)
    {
        await using var cmd = Source.CreateCommand("""
            SELECT hub_id,definition_id,definition_version,partition_id,
                   revision,state,next_step,step_count,waiting_for,
                   input_json::text,results_json::text,processor_state::text
            FROM wfc_scale.hub_workflow WHERE id=@id AND execution_mode='remote'
            """);
        cmd.Parameters.AddWithValue("id", id);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return null;
        return new(id, reader.GetString(0),reader.GetString(1),
            reader.GetInt32(2),reader.GetInt32(3),reader.GetInt64(4),
            States[reader.GetInt16(5)],reader.GetInt32(6),reader.GetInt32(7),
            reader.IsDBNull(8)?null:reader.GetString(8),
            JsonDocument.Parse(reader.GetString(9)).RootElement.Clone(),
            JsonDocument.Parse(reader.GetString(10)).RootElement.Clone(),
            JsonDocument.Parse(reader.GetString(11)).RootElement.Clone());
    }

    public async Task<bool> IsRemoteActivityAsync(Guid taskId, CancellationToken ct)
    {
        await using var cmd = Source.CreateCommand("""
            SELECT 1 FROM wfc_scale.hub_task t
            JOIN wfc_scale.hub_workflow w ON w.id=t.workflow_id
            WHERE t.id=@id AND w.execution_mode='remote'
              AND t.workflow_step IS NOT NULL AND t.task_type<>'__workflow__'
            """);
        cmd.Parameters.AddWithValue("id",taskId);
        return await cmd.ExecuteScalarAsync(ct) is not null;
    }

    public async Task<HubTransitionResult?> CommitAsync(Guid id,
        HubTransitionRequest request, CancellationToken ct)
    {
        if (request.TaskId == Guid.Empty || string.IsNullOrWhiteSpace(request.WorkerId) ||
            request.WorkerId.Length > 200 || request.ExpectedRevision < 0 ||
            request.Epoch < 1 || request.Command is not
                ("ScheduleActivity" or "ScheduleTimer" or "CompleteWorkflow") ||
            request.ProcessorState?.GetRawText().Length > 65536)
            throw new ArgumentException("Invalid processor transition");

        var canonical = JsonSerializer.Serialize(request);
        HubTaskRoute? route = null;
        await using var db = await Source.OpenConnectionAsync(ct);
        await using var tx = await db.BeginTransactionAsync(ct);
        string hub, definition; int partition, step, steps; long revision;
        short workflowState; string? waitingFor; string input;
        await using (var cmd = new NpgsqlCommand("""
            SELECT hub_id,definition_id,partition_id,next_step,step_count,
                   revision,state,waiting_for,input_json::text
            FROM wfc_scale.hub_workflow
            WHERE id=@id AND execution_mode='remote' FOR UPDATE
            """, db, tx))
        {
            cmd.Parameters.AddWithValue("id", id);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct)) return null;
            hub=reader.GetString(0); definition=reader.GetString(1);
            partition=reader.GetInt32(2); step=reader.GetInt32(3);
            steps=reader.GetInt32(4); revision=reader.GetInt64(5);
            workflowState=reader.GetInt16(6);
            waitingFor=reader.IsDBNull(7)?null:reader.GetString(7);
            input=reader.GetString(8);
        }

        short taskState; string? owner,lastOwner,storedResult; long epoch;
        DateTime? lease;
        await using (var cmd = new NpgsqlCommand("""
            SELECT state,owner,last_owner,epoch,lease_until,result_json::text
            FROM wfc_scale.hub_task
            WHERE id=@task AND workflow_id=@workflow AND task_type='__workflow__'
            FOR UPDATE
            """, db, tx))
        {
            cmd.Parameters.AddWithValue("task",request.TaskId);
            cmd.Parameters.AddWithValue("workflow",id);
            await using var reader=await cmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct)) return null;
            taskState=reader.GetInt16(0);
            owner=reader.IsDBNull(1)?null:reader.GetString(1);
            lastOwner=reader.IsDBNull(2)?null:reader.GetString(2);
            epoch=reader.GetInt64(3);
            lease=reader.IsDBNull(4)?null:reader.GetDateTime(4);
            storedResult=reader.IsDBNull(5)?null:reader.GetString(5);
        }
        if (taskState==2 && epoch==request.Epoch && lastOwner==request.WorkerId &&
            storedResult is not null)
        {
            using var previous=JsonDocument.Parse(storedResult);
            using var current=JsonDocument.Parse(canonical);
            if (JsonElement.DeepEquals(previous.RootElement,current.RootElement))
                return new(id,revision,States[workflowState],true);
        }
        if (taskState!=1 || epoch!=request.Epoch || owner!=request.WorkerId ||
            lease<=DateTime.UtcNow || revision!=request.ExpectedRevision ||
            workflowState>=2 || waitingFor is not null)
            return null;

        string? nextWait=null;
        var updateState=1;
        var nextStep=step;
        if (request.Command=="ScheduleActivity")
        {
            if (string.IsNullOrWhiteSpace(request.ActivityType) ||
                request.ActivityType.Length>80 || request.ActivityVersion<1 || step>=steps)
                throw new ArgumentException("Invalid activity transition");
            var payload = request.ActivityInput is { ValueKind: JsonValueKind.Object } supplied
                ? supplied : JsonDocument.Parse(input).RootElement;
            if (payload.GetRawText().Length>65536)
                throw new ArgumentException("Activity payload exceeds limit");
            route=await definitions.ResolveAsync(hub,request.ActivityType,request.ActivityVersion,
                payload,ct,db,tx)
                ?? throw new KeyNotFoundException("Activity is not registered");
            if (route.PartitionCount<=partition)
                throw new InvalidOperationException("Activity partition registration mismatch");
            var activityId=Guid.NewGuid();
            await InsertActivityAsync(db,tx,activityId,id,hub,partition,step,route,
                payload.GetRawText(),ct);
            nextWait="activity:"+activityId.ToString("D");
            await JournalAsync(db,tx,id,step,activityId,request.ActivityType,
                "ActivityScheduled",null,ct);
        }
        else if (request.Command=="ScheduleTimer")
        {
            if (string.IsNullOrWhiteSpace(request.TimerKey) ||
                request.TimerKey.Length>100 || request.DelayMilliseconds is < 0 or > 31536000000L ||
                request.DelayMilliseconds is null || step>=steps)
                throw new ArgumentException("Invalid timer transition");
            var timerId=Guid.NewGuid();
            await using (var timer = new NpgsqlCommand("""
                INSERT INTO wfc_scale.hub_timer
                (id,workflow_id,hub_id,partition_id,step_index,timer_key,generation,
                 due_at,scheduled_revision)
                VALUES (@id,@workflow,@hub,@partition,@step,@key,@generation,
                        now()+@delay,@revision)
                """,db,tx))
            {
                timer.Parameters.AddWithValue("id",timerId);
                timer.Parameters.AddWithValue("workflow",id);
                timer.Parameters.AddWithValue("hub",hub);
                timer.Parameters.AddWithValue("partition",partition);
                timer.Parameters.AddWithValue("step",step);
                timer.Parameters.AddWithValue("key",request.TimerKey);
                timer.Parameters.AddWithValue("generation",revision+1);
                timer.Parameters.Add("delay",NpgsqlDbType.Interval).Value =
                    TimeSpan.FromMilliseconds(request.DelayMilliseconds.Value);
                timer.Parameters.AddWithValue("revision",revision+1);
                await timer.ExecuteNonQueryAsync(ct);
            }
            nextWait="timer:"+timerId.ToString("D");
            await JournalAsync(db,tx,id,step,null,request.TimerKey,
                "TimerScheduled",JsonSerializer.Serialize(new { timerId,
                    delayMilliseconds=request.DelayMilliseconds }),ct);
        }
        else if (request.Command=="CompleteWorkflow")
        {
            if (step!=steps) throw new InvalidOperationException("Workflow has incomplete steps");
            updateState=2;
            await JournalAsync(db,tx,id,Math.Max(0,step-1),request.TaskId,
                "__workflow__","WorkflowCompleted",null,ct);
        }

        await using (var workflow = new NpgsqlCommand("""
            UPDATE wfc_scale.hub_workflow
            SET state=@state,waiting_for=@wait,revision=revision+1,
                processor_state=COALESCE(@processor_state,processor_state),
                completed_at=CASE WHEN @state=2 THEN now() ELSE completed_at END,
                updated_at=now()
            WHERE id=@id AND revision=@revision
            """,db,tx))
        {
            workflow.Parameters.AddWithValue("state",(short)updateState);
            workflow.Parameters.Add("wait",NpgsqlDbType.Text).Value =
                (object?)nextWait??DBNull.Value;
            workflow.Parameters.Add("processor_state",NpgsqlDbType.Jsonb).Value =
                request.ProcessorState?.GetRawText() is string state ? state : DBNull.Value;
            workflow.Parameters.AddWithValue("id",id);
            workflow.Parameters.AddWithValue("revision",revision);
            if(await workflow.ExecuteNonQueryAsync(ct)!=1)
                throw new InvalidOperationException("Workflow revision changed");
        }
        await using (var task = new NpgsqlCommand("""
            UPDATE wfc_scale.hub_task
            SET state=2,owner=NULL,lease_until=NULL,result_json=@result,updated_at=now()
            WHERE id=@id AND state=1 AND owner=@owner AND epoch=@epoch
              AND lease_until>now()
            """,db,tx))
        {
            task.Parameters.AddWithValue("id",request.TaskId);
            task.Parameters.AddWithValue("owner",request.WorkerId);
            task.Parameters.AddWithValue("epoch",request.Epoch);
            task.Parameters.Add("result",NpgsqlDbType.Jsonb).Value=canonical;
            if(await task.ExecuteNonQueryAsync(ct)!=1) return null;
        }
        await JournalAsync(db,tx,id,step,request.TaskId,"__workflow__",
            "WorkflowTaskCommitted",JsonSerializer.Serialize(new{request.Command}),ct);
        await tx.CommitAsync(ct);
        if(updateState==2)
        {
            HubTelemetry.WorkflowsCompleted.Add(1,HubTelemetry.HubTag(hub));
        }
        return new(id,revision+1,States[updateState],false);
    }

    public async Task<bool> CompleteActivityAsync(Guid taskId, string worker,
        long epoch, string result, CancellationToken ct)
    {
        if(result.Length>65536)throw new ArgumentException("Activity result exceeds limit");
        await using var db=await Source.OpenConnectionAsync(ct);
        await using var tx=await db.BeginTransactionAsync(ct);
        Guid workflowId; string hub; int partition,step; long revision; string? waitingFor;
        short workflowState;
        // Lock the workflow row before the task row to avoid deadlocks with scheduler.
        await using(var q=new NpgsqlCommand("""
            SELECT w.id,w.hub_id,w.partition_id,t.workflow_step,w.revision,
                   w.waiting_for,w.state
            FROM wfc_scale.hub_task t
            JOIN wfc_scale.hub_workflow w ON w.id=t.workflow_id
            WHERE t.id=@task AND w.execution_mode='remote'
              AND t.workflow_step IS NOT NULL AND t.task_type<>'__workflow__'
            FOR UPDATE OF w
            """,db,tx))
        {
            q.Parameters.AddWithValue("task",taskId);
            await using var r=await q.ExecuteReaderAsync(ct);
            if(!await r.ReadAsync(ct))return false;
            workflowId=r.GetGuid(0);hub=r.GetString(1);partition=r.GetInt32(2);
            step=r.GetInt32(3);revision=r.GetInt64(4);
            waitingFor=r.IsDBNull(5)?null:r.GetString(5);
            workflowState=r.GetInt16(6);
        }
        await using(var q=new NpgsqlCommand("""
            SELECT state,owner,last_owner,epoch,lease_until,result_json::text
            FROM wfc_scale.hub_task WHERE id=@id FOR UPDATE
            """,db,tx))
        {
            q.Parameters.AddWithValue("id",taskId);
            await using var r=await q.ExecuteReaderAsync(ct);
            if(!await r.ReadAsync(ct))return false;
            var state=r.GetInt16(0);
            var currentOwner=r.IsDBNull(1)?null:r.GetString(1);
            var last=r.IsDBNull(2)?null:r.GetString(2);
            var currentEpoch=r.GetInt64(3);
            var until=r.IsDBNull(4)?null:r.GetDateTime(4);
            if(state==2 && currentEpoch==epoch && last==worker)
            {
                var stored=r.IsDBNull(5)?null:r.GetString(5);
                using var expected=JsonDocument.Parse(result);
                if(stored is not null)
                {
                    using var previous=JsonDocument.Parse(stored);
                    return JsonElement.DeepEquals(previous.RootElement,expected.RootElement);
                }
            }
            if(state!=1||currentOwner!=worker||currentEpoch!=epoch||
               until<=DateTime.UtcNow||workflowState>=2||
               waitingFor!="activity:"+taskId.ToString("D"))
                return false;
        }
        await using(var q=new NpgsqlCommand("""
            UPDATE wfc_scale.hub_task
            SET state=2,owner=NULL,lease_until=NULL,result_json=@result,updated_at=now()
            WHERE id=@id AND owner=@worker AND epoch=@epoch AND state=1
              AND lease_until>now()
            """,db,tx))
        {
            q.Parameters.AddWithValue("id",taskId);
            q.Parameters.AddWithValue("worker",worker);
            q.Parameters.AddWithValue("epoch",epoch);
            q.Parameters.Add("result",NpgsqlDbType.Jsonb).Value=result;
            if(await q.ExecuteNonQueryAsync(ct)!=1)return false;
        }
        await using(var q=new NpgsqlCommand("""
            UPDATE wfc_scale.hub_task_attempt
            SET outcome='Completed',finished_at=clock_timestamp()
            WHERE task_id=@id AND epoch=@epoch AND finished_at IS NULL
            """,db,tx))
        {
            q.Parameters.AddWithValue("id",taskId);
            q.Parameters.AddWithValue("epoch",epoch);
            await q.ExecuteNonQueryAsync(ct);
        }
        await using(var q=new NpgsqlCommand("""
            UPDATE wfc_scale.hub_workflow
            SET next_step=next_step+1, revision=revision+1,
                results_json=jsonb_set(results_json,ARRAY[@key],@result,true),
                waiting_for=NULL,updated_at=now()
            WHERE id=@id AND revision=@revision
            """,db,tx))
        {
            q.Parameters.AddWithValue("id",workflowId);
            q.Parameters.AddWithValue("revision",revision);
            q.Parameters.AddWithValue("key",step.ToString());
            q.Parameters.Add("result",NpgsqlDbType.Jsonb).Value=result;
            if(await q.ExecuteNonQueryAsync(ct)!=1)
                throw new InvalidOperationException("Lost workflow revision on activity completion");
        }
        await using(var q=new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_workflow_history
            (workflow_id,step_index,task_id,event_type,payload)
            VALUES (@workflow,@step,@task,'StepCompleted',@payload)
            ON CONFLICT DO NOTHING
            """,db,tx))
        {
            q.Parameters.AddWithValue("workflow",workflowId);
            q.Parameters.AddWithValue("step",step);
            q.Parameters.AddWithValue("task",taskId);
            q.Parameters.Add("payload",NpgsqlDbType.Jsonb).Value=result;
            await q.ExecuteNonQueryAsync(ct);
        }
        await WakeProcessorAsync(db,tx,workflowId,hub,partition,revision+1,ct);
        await tx.CommitAsync(ct);
        HubTelemetry.TasksCompleted.Add(1,HubTelemetry.HubTag(hub));
        return true;
    }

    public static async Task WakeProcessorAsync(NpgsqlConnection db,NpgsqlTransaction tx,
        Guid workflow,string hub,int partition,long revision,CancellationToken ct)
    {
        await using var q=new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task
            (id,hub_id,partition_id,task_type,task_version,workflow_id,
             payload,request_key)
            VALUES (@id,@hub,@partition,'__workflow__',1,@workflow,'{}'::jsonb,@key)
            ON CONFLICT DO NOTHING
            """,db,tx);
        q.Parameters.AddWithValue("id",Guid.NewGuid());
        q.Parameters.AddWithValue("hub",hub);
        q.Parameters.AddWithValue("partition",partition);
        q.Parameters.AddWithValue("workflow",workflow);
        q.Parameters.AddWithValue("key","wf:"+workflow.ToString("N")+":r:"+revision);
        await q.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertActivityAsync(NpgsqlConnection db,NpgsqlTransaction tx,
        Guid taskId,Guid workflow,string hub,int partition,int step,HubTaskRoute route,
        string payload,CancellationToken ct)
    {
        foreach(var resource in route.Resources)
        {
            await using(var upsert=new NpgsqlCommand("""
                INSERT INTO wfc_scale.hub_resource(resource_key,capacity,strict_fifo)
                VALUES (@key,@capacity,@fifo) ON CONFLICT DO NOTHING
                """,db,tx))
            {
                upsert.Parameters.AddWithValue("key",resource.Key);
                upsert.Parameters.AddWithValue("capacity",resource.Capacity);
                upsert.Parameters.AddWithValue("fifo",resource.StrictFifo);
                await upsert.ExecuteNonQueryAsync(ct);
            }
            await using(var verify=new NpgsqlCommand("""
                SELECT capacity,strict_fifo FROM wfc_scale.hub_resource
                WHERE resource_key=@key FOR UPDATE
                """,db,tx))
            {
                verify.Parameters.AddWithValue("key",resource.Key);
                await using var reader=await verify.ExecuteReaderAsync(ct);
                if(!await reader.ReadAsync(ct) ||
                   reader.GetInt32(0)!=resource.Capacity ||
                   reader.GetBoolean(1)!=resource.StrictFifo)
                    throw new InvalidOperationException("Concurrency policy version conflict");
            }
        }
        await using(var insert=new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task
            (id,hub_id,partition_id,task_type,task_version,workflow_id,
             workflow_step,payload)
            VALUES (@id,@hub,@partition,@type,@version,@workflow,@step,@payload)
            """,db,tx))
        {
            insert.Parameters.AddWithValue("id",taskId);
            insert.Parameters.AddWithValue("hub",hub);
            insert.Parameters.AddWithValue("partition",partition);
            insert.Parameters.AddWithValue("type",route.TaskType);
            insert.Parameters.AddWithValue("version",route.Version);
            insert.Parameters.AddWithValue("workflow",workflow);
            insert.Parameters.AddWithValue("step",step);
            insert.Parameters.Add("payload",NpgsqlDbType.Jsonb).Value=payload;
            await insert.ExecuteNonQueryAsync(ct);
        }
        foreach(var resource in route.Resources)
        {
            await using var attach=new NpgsqlCommand("""
                INSERT INTO wfc_scale.hub_task_constraint(task_id,resource_key)
                VALUES (@task,@key)
                """,db,tx);
            attach.Parameters.AddWithValue("task",taskId);
            attach.Parameters.AddWithValue("key",resource.Key);
            await attach.ExecuteNonQueryAsync(ct);
        }
    }

    public static async Task JournalAsync(NpgsqlConnection db,NpgsqlTransaction tx,
        Guid workflow,int step,Guid? task,string activity,string eventType,
        string? payload,CancellationToken ct)
    {
        await using var q=new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_workflow_event
            (workflow_id,step_index,task_id,activity_type,event_type,payload)
            VALUES (@workflow,@step,@task,@activity,@event,@payload)
            """,db,tx);
        q.Parameters.AddWithValue("workflow",workflow);
        q.Parameters.AddWithValue("step",step);
        q.Parameters.Add("task",NpgsqlDbType.Uuid).Value=(object?)task??DBNull.Value;
        q.Parameters.AddWithValue("activity",activity);
        q.Parameters.AddWithValue("event",eventType);
        q.Parameters.Add("payload",NpgsqlDbType.Jsonb).Value=(object?)payload??DBNull.Value;
        await q.ExecuteNonQueryAsync(ct);
    }
}

using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace WorkflowCore.Hub;

/// <summary>Opaque native snapshots plus a durable inbox. Lock order: event lane,
/// workflow, task. Inbox delivery never changes the snapshot revision.</summary>
public sealed class HubNativeWorkflowStore(HubTaskStore tasks, HubDefinitionStore definitions)
{
    private NpgsqlDataSource Source => tasks.DataSource;
    private static JsonElement Json(string value) => JsonDocument.Parse(value).RootElement.Clone();
    // Fixed per Hub process; false selects the original per-event insert for A/B tests.
    private static readonly bool BatchHistoryEnabled =
        !string.Equals(Environment.GetEnvironmentVariable("WFC_HUB_BATCH_HISTORY_ENABLED"), "false", StringComparison.OrdinalIgnoreCase)
        && Environment.GetEnvironmentVariable("WFC_HUB_BATCH_HISTORY_ENABLED") != "0";
    private const int HistoryBatchChunkSize = 256;
    private static KeyValuePair<string, object?> HistoryModeTag =>
        new("history_mode", BatchHistoryEnabled ? "batch" : "legacy");

    // Temporary benchmark/rollback switch. The default path only takes the
    // Hub event lane when a native commit installs or removes subscriptions.
    // It is NOT safe to remove the lane for subscription commits: doing so
    // races a concurrent PublishEvent and can permanently lose wakeups.
    private static readonly bool AlwaysLockHubForNativeCommit =
        string.Equals(Environment.GetEnvironmentVariable("WFC_HUB_NATIVE_EVENT_LANE_MODE"),
            "hub", StringComparison.OrdinalIgnoreCase);

    private static KeyValuePair<string, object?> EventLaneModeTag =>
        new("event_lane_mode", AlwaysLockHubForNativeCommit ? "hub" : "subscriptions");

    internal static NpgsqlCommand Command(NpgsqlConnection db, NpgsqlTransaction tx,
        string sql, params (string Name, object? Value)[] values)
    {
        var command = new NpgsqlCommand(sql, db, tx);
        foreach (var (name, value) in values)
            if (value is null) command.Parameters.Add(name, NpgsqlDbType.Text).Value = DBNull.Value;
            else command.Parameters.AddWithValue(name, value);
        return command;
    }

    internal static async Task Execute(NpgsqlConnection db, NpgsqlTransaction tx,
        string sql, CancellationToken ct, params (string Name, object? Value)[] values)
    {
        await using var cmd = Command(db, tx, sql, values);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task EventLane(NpgsqlConnection db, NpgsqlTransaction tx, string hub,
        string operation, CancellationToken ct)
    {
        var lockStarted = System.Diagnostics.Stopwatch.GetTimestamp();
        await using var cmd = Command(db, tx,
            "SELECT pg_advisory_xact_lock(hashtextextended(@hub,12012))", ("hub", hub));
        await cmd.ExecuteNonQueryAsync(ct);
        HubTelemetry.NativeEventLaneLockWait.Record(
            System.Diagnostics.Stopwatch.GetElapsedTime(lockStarted).TotalSeconds,
            new KeyValuePair<string, object?>("operation", operation),
            HistoryModeTag, EventLaneModeTag);
    }

    public async Task<HubNativeRuntime?> GetRuntimeAsync(Guid id, CancellationToken ct)
    {
        // One repeatable-read snapshot ensures inbox and workflow data agree.
        await using var db = await Source.OpenConnectionAsync(ct);
        await using var tx = await db.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, ct);
        string hub, definition, status, input; int version; long revision;
        JsonElement? snapshot; DateTime created;
        await using (var cmd = Command(db, tx, """
            SELECT hub_id,definition_id,definition_version,revision,native_status,
                   input_json::text,native_snapshot::text,created_at
            FROM wfc_scale.hub_workflow WHERE id=@id AND execution_mode='native'
            """, ("id", id)))
        {
            await using var r = await cmd.ExecuteReaderAsync(ct);
            if (!await r.ReadAsync(ct)) return null;
            hub=r.GetString(0); definition=r.GetString(1); version=r.GetInt32(2);
            revision=r.GetInt64(3); status=r.GetString(4); input=r.GetString(5);
            snapshot=r.IsDBNull(6)?null:Json(r.GetString(6)); created=r.GetDateTime(7);
        }
        var messages = new List<HubNativeMessage>();
        await using (var cmd = Command(db, tx, """
            SELECT id,pointer_id,kind,payload::text FROM wfc_scale.hub_native_inbox
            WHERE workflow_id=@id AND NOT consumed ORDER BY id
            """, ("id", id)))
        {
            await using var r=await cmd.ExecuteReaderAsync(ct);
            while(await r.ReadAsync(ct)) messages.Add(new(r.GetInt64(0),r.GetString(1),r.GetString(2),Json(r.GetString(3))));
        }
        await tx.CommitAsync(ct);
        return new(1,id,hub,definition,version,revision,status,Json(input),snapshot,messages.ToArray(),created);
    }

    public async Task<HubTransitionResult?> CommitAsync(Guid id, HubNativeCommit request, CancellationToken ct)
    {
        var commitStarted = System.Diagnostics.Stopwatch.GetTimestamp();
        if(request.ProtocolVersion!=1 || request.TaskId==Guid.Empty || request.Epoch<1 || request.ExpectedRevision<0 ||
           string.IsNullOrWhiteSpace(request.WorkerId) || request.WorkerId.Length>128 ||
           request.Status is not ("Runnable" or "Suspended" or "Complete" or "Terminated") ||
           request.Snapshot.ValueKind!=JsonValueKind.Object || request.Snapshot.GetRawText().Length>4*1024*1024 ||
           request.Subscriptions is null || request.Activities is null || request.History is null || request.ConsumedInbox is null ||
           request.Subscriptions.Length>10000 || request.Activities.Length>10000 || request.History.Length>20000 ||
           request.ConsumedInbox.Length>20000 || request.Subscriptions.Any(s=>s.PointerId.Length>100 ||
               s.EventName.Length>200 || s.EventKey.Length>200) ||
           request.Activities.Any(a=>string.IsNullOrWhiteSpace(a.DispatchKey) || a.DispatchKey.Length>200 ||
               a.PointerId.Length>100 || a.StepId<0 || a.Input.ValueKind!=JsonValueKind.Object || a.Input.GetRawText().Length>65536) ||
           request.Timer is { StepId: <0 } || request.Timer?.PointerId.Length>100)
            throw new ArgumentException("Invalid native commit");
        var canonical = JsonSerializer.Serialize(request);
        await using var db = await Source.OpenConnectionAsync(ct);
        await using var tx = await db.BeginTransactionAsync(ct);
        string hub;
        bool hasExistingSubscriptions;
        // A single query avoids an extra DB round trip on the hot path. New
        // subscriptions are declared by the commit request; old subscriptions
        // must be protected too, because this commit replaces/removes them.
        await using(var lookup=Command(db,tx,"""
            SELECT w.hub_id, EXISTS (
                SELECT 1 FROM wfc_scale.hub_native_subscription s
                WHERE s.workflow_id=w.id
            )
            FROM wfc_scale.hub_workflow w
            WHERE w.id=@id AND w.execution_mode='native'
            """,("id",id)))
        {
            await using var reader=await lookup.ExecuteReaderAsync(ct);
            if(!await reader.ReadAsync(ct))return null;
            hub=reader.GetString(0);
            hasExistingSubscriptions=reader.GetBoolean(1);
        }
        var requiresEventLane = AlwaysLockHubForNativeCommit ||
            hasExistingSubscriptions || request.Subscriptions.Length>0;
        // Lock order for event-sensitive commits is unchanged:
        // event lane -> workflow row -> task row. Without subscriptions,
        // the workflow row + fenced task lease alone serialize this workflow,
        // while other workflows in the same Hub may commit independently.
        if(requiresEventLane)
            await EventLane(db,tx,hub,"native_commit",ct);
        else
            HubTelemetry.NativeEventLaneBypass.Add(1,
                new KeyValuePair<string, object?>("operation","native_commit"),
                EventLaneModeTag);
        int partition;long revision;string status;
        var rowLockStarted=System.Diagnostics.Stopwatch.GetTimestamp();
        await using(var cmd=Command(db,tx,"""
            SELECT partition_id,revision,native_status FROM wfc_scale.hub_workflow
            WHERE id=@id FOR UPDATE
            """,("id",id)))
        {
            await using var r=await cmd.ExecuteReaderAsync(ct);
            if(!await r.ReadAsync(ct))return null;
            partition=r.GetInt32(0);revision=r.GetInt64(1);status=r.GetString(2);
        }
        HubTelemetry.NativeWorkflowRowLockDuration.Record(
            System.Diagnostics.Stopwatch.GetElapsedTime(rowLockStarted).TotalSeconds,
            EventLaneModeTag);
        await using(var cmd=Command(db,tx,"""
            SELECT state,owner,last_owner,epoch,lease_until,result_json::text
            FROM wfc_scale.hub_task WHERE id=@task AND workflow_id=@id
              AND task_type='__native_workflow__' FOR UPDATE
            """,("task",request.TaskId),("id",id)))
        {
            await using var r=await cmd.ExecuteReaderAsync(ct);
            if(!await r.ReadAsync(ct))return null;
            if(r.GetInt16(0)==2 && r.GetInt64(3)==request.Epoch && !r.IsDBNull(2) &&
               r.GetString(2)==request.WorkerId && !r.IsDBNull(5))
            {
                var stored=Json(r.GetString(5));
                if(stored.TryGetProperty("commit",out var previous) && JsonElement.DeepEquals(previous,Json(canonical)))
                    return new(id,stored.GetProperty("revision").GetInt64(),stored.GetProperty("status").GetString()!,true);
                return null;
            }
            if(r.GetInt16(0)!=1 || r.IsDBNull(1) || r.GetString(1)!=request.WorkerId ||
               r.GetInt64(3)!=request.Epoch || r.IsDBNull(4) || r.GetDateTime(4)<=DateTime.UtcNow ||
               revision!=request.ExpectedRevision)return null;
        }
        // A status change invalidates in-flight snapshots by incrementing revision.
        if(status is "Complete" or "Terminated")return null;
        foreach(var consumed in request.ConsumedInbox.Distinct())
        {
            await using var cmd=Command(db,tx,"""
                UPDATE wfc_scale.hub_native_inbox SET consumed=true
                WHERE id=@message AND workflow_id=@id AND NOT consumed
                """,("message",consumed),("id",id));
            if(await cmd.ExecuteNonQueryAsync(ct)!=1)throw new ArgumentException("Unknown or already consumed inbox message");
        }
        await Execute(db,tx,"DELETE FROM wfc_scale.hub_native_subscription WHERE workflow_id=@id",ct,("id",id));
        foreach(var subscription in request.Subscriptions)
            await Execute(db,tx,"""
                INSERT INTO wfc_scale.hub_native_subscription(workflow_id,pointer_id,event_name,event_key,as_of)
                VALUES(@id,@pointer,@name,@key,@asof)
                """,ct,("id",id),("pointer",subscription.PointerId),("name",subscription.EventName),
                ("key",subscription.EventKey),("asof",subscription.AsOf.ToUniversalTime()));
        // Retire cancelled pointer work before taking any activity resource locks.
        // Claim uses task -> resource, so reversing that order here can deadlock.
        if(request.ActiveDispatches is string[] active)
        {
            await Execute(db,tx,"""
                UPDATE wfc_scale.hub_task SET state=3,owner=NULL,lease_until=NULL,
                  last_error='Native pointer cancelled',updated_at=now()
                WHERE workflow_id=@id AND dispatch_key IS NOT NULL AND state IN (0,1)
                  AND NOT (dispatch_key=ANY(@active));
                UPDATE wfc_scale.hub_task_attempt a SET outcome='Cancelled',finished_at=clock_timestamp()
                FROM wfc_scale.hub_task t WHERE a.task_id=t.id AND t.workflow_id=@id
                  AND t.last_error='Native pointer cancelled' AND a.finished_at IS NULL
                """,ct,("id",id),("active",active));
        }
        if(request.Status is "Runnable" or "Suspended")
        {
            var dispatches=new List<(HubNativeDispatch Activity,HubTaskRoute Route)>();
            foreach(var activity in request.Activities)
            {
                // Dispatch keys are stable across transport retries, unique across native retries/iterations.
                await using(var existing=Command(db,tx,"""
                    SELECT 1 FROM wfc_scale.hub_task WHERE workflow_id=@id AND dispatch_key=@key
                    """,("id",id),("key",activity.DispatchKey)))
                    if(await existing.ExecuteScalarAsync(ct) is not null)continue;
                var route=await definitions.ResolveAsync(hub,activity.ActivityType,activity.Version,activity.Input,ct,db,tx)
                    ?? throw new KeyNotFoundException("Unregistered native activity");
                if(partition>=route.PartitionCount)throw new InvalidOperationException("Activity partition mismatch");
                dispatches.Add((activity,route));
            }
            // Lock the whole batch in canonical order. Locking per activity
            // could take a hub permit before another activity's permit while
            // a concurrent claim holds those resources in the opposite order.
            await HubRemoteWorkflowStore.LockActivityResourcesAsync(db,tx,
                dispatches.SelectMany(d=>d.Route.ResourcesForPartition(partition)),ct);
            foreach(var (activity,route) in dispatches)
            {
                var task=Guid.NewGuid();
                await HubRemoteWorkflowStore.InsertActivityAsync(db,tx,task,id,hub,partition,null,route,activity.Input.GetRawText(),ct);
                await Execute(db,tx,"""
                    UPDATE wfc_scale.hub_task SET execution_pointer_id=@pointer,dispatch_key=@key,native_step_id=@step,
                      due_at=COALESCE(@due::timestamptz,due_at) WHERE id=@task
                    """,ct,("pointer",activity.PointerId),("key",activity.DispatchKey),("task",task),("step",activity.StepId),
                    ("due",activity.NotBefore?.ToUniversalTime()));
                await HubRemoteWorkflowStore.JournalAsync(db,tx,id,activity.StepId,task,activity.ActivityType,"ActivityScheduled",
                    JsonSerializer.Serialize(new{executionPointerId=activity.PointerId,activity.DispatchKey}),ct);
            }
            await MatchEvents(db,tx,id,hub,ct);
        }
        await Execute(db,tx,"""
            WITH cancelled AS (
              UPDATE wfc_scale.hub_timer SET state=2,cancelled_at=clock_timestamp()
              WHERE workflow_id=@id AND state=0 RETURNING id,step_index,execution_pointer_id
            ) INSERT INTO wfc_scale.hub_workflow_event(workflow_id,step_index,activity_type,event_type,execution_pointer_id,payload)
              SELECT @id,step_index,'__native_workflow__','TimerCancelled',execution_pointer_id,jsonb_build_object('timerId',id)
              FROM cancelled
            """,ct,("id",id));
        if(request.Status=="Runnable" && request.WakeAt is DateTime deadline && !request.Runnable)
        {
            var timerId=Guid.NewGuid();
            await Execute(db,tx,"""
                INSERT INTO wfc_scale.hub_timer(id,workflow_id,hub_id,partition_id,step_index,timer_key,generation,due_at,scheduled_revision,execution_pointer_id)
                VALUES(@timer,@id,@hub,@partition,@step,'native-wakeup',@revision,@due,@revision,@pointer);
                INSERT INTO wfc_scale.hub_workflow_event(workflow_id,step_index,activity_type,event_type,execution_pointer_id,payload)
                VALUES(@id,@step,'__native_workflow__','TimerScheduled',@pointer,jsonb_build_object('timerId',@timer,'dueAt',@due))
                """,ct,("timer",timerId),("id",id),("hub",hub),("partition",partition),("step",request.Timer?.StepId??0),
                ("revision",revision+1),("due",deadline.ToUniversalTime()),("pointer",request.Timer?.PointerId));
        }
        var historyStarted = System.Diagnostics.Stopwatch.GetTimestamp();
        if (BatchHistoryEnabled)
        {
            // Ordering is preserved with WITH ORDINALITY (also across chunks).
            // All writes remain in the existing task-lease-fenced transaction.
            for (var offset = 0; offset < request.History.Length; offset += HistoryBatchChunkSize)
            {
                var values = JsonSerializer.Serialize(request.History.Skip(offset).Take(HistoryBatchChunkSize)
                    .Select(history => new
                    {
                        step = history.StepId,
                        task = request.TaskId,
                        activity = history.ActivityType,
                        eventType = history.EventType,
                        pointer = history.PointerId,
                        parent = history.ParentId,
                        scope = history.Scope,
                        attempt = history.Attempt,
                        payload = history.Payload
                    }));
                await Execute(db, tx, """
                    INSERT INTO wfc_scale.hub_workflow_event
                        (workflow_id,step_index,task_id,activity_type,event_type,
                         execution_pointer_id,parent_pointer_id,scope,attempt,payload)
                    SELECT @id,(item->>'step')::integer,
                           (item->>'task')::uuid,
                           COALESCE(item->>'activity','__native_workflow__'),
                           item->>'eventType',item->>'pointer',item->>'parent',
                           item->'scope',(item->>'attempt')::integer,
                           NULLIF(item->'payload','null'::jsonb)
                    FROM jsonb_array_elements(@history::jsonb) WITH ORDINALITY AS h(item, ordinal)
                    ORDER BY ordinal
                    """, ct, ("id", id), ("history", values));
            }
        }
        else
        {
            // Exact original path retained temporarily as a performance control.
            foreach (var history in request.History)
                await Execute(db,tx,"""
                    INSERT INTO wfc_scale.hub_workflow_event(workflow_id,step_index,task_id,activity_type,event_type,
                        execution_pointer_id,parent_pointer_id,scope,attempt,payload)
                    VALUES(@id,@step,@task,COALESCE(@activity,'__native_workflow__'),@event,@pointer,@parent,@scope::jsonb,@attempt,@payload::jsonb)
                    """,ct,("id",id),("step",history.StepId),("task",request.TaskId),("event",history.EventType),
                    ("pointer",history.PointerId),("parent",history.ParentId),("scope",JsonSerializer.Serialize(history.Scope)),
                    ("attempt",history.Attempt),("payload",history.Payload?.GetRawText()),("activity",history.ActivityType));
        }
        HubTelemetry.NativeHistoryWriteDuration.Record(
            System.Diagnostics.Stopwatch.GetElapsedTime(historyStarted).TotalSeconds, HistoryModeTag);
        HubTelemetry.NativeHistoryEventsPerCommit.Record(request.History.Length, HistoryModeTag);
        var nextStatus=request.Status;
        await Execute(db,tx,"""
            UPDATE wfc_scale.hub_workflow SET native_snapshot=@snapshot::jsonb,native_status=@status,
              waiting_for=CASE WHEN @waiting THEN 'native' ELSE NULL END,
              state=@state,revision=revision+1,updated_at=now(),
              completed_at=CASE WHEN @state>=2 THEN now() ELSE NULL END
            WHERE id=@id
            """,ct,("snapshot",request.Snapshot.GetRawText()),("status",nextStatus),
            ("state",(short)(nextStatus=="Complete"?2:nextStatus=="Terminated"?3:1)),("id",id),
            ("waiting",nextStatus is "Runnable" or "Suspended" && !request.Runnable));
        await using(var ack=Command(db,tx,"""
            UPDATE wfc_scale.hub_task SET state=2,owner=NULL,lease_until=NULL,result_json=@result::jsonb,updated_at=now()
            WHERE id=@task AND owner=@owner AND epoch=@epoch AND state=1 AND lease_until>clock_timestamp()
            """,("task",request.TaskId),("owner",request.WorkerId),("epoch",request.Epoch),
            ("result",JsonSerializer.Serialize(new{commit=request,revision=revision+1,status=nextStatus}))))
            if(await ack.ExecuteNonQueryAsync(ct)!=1)return null;
        if(nextStatus=="Runnable")
        {
            await using var pending=Command(db,tx,"SELECT 1 FROM wfc_scale.hub_native_inbox WHERE workflow_id=@id AND NOT consumed LIMIT 1",("id",id));
            if(request.Runnable || await pending.ExecuteScalarAsync(ct) is not null)
                await WakeAsync(db,tx,id,hub,partition,ct);
        }
        else if(nextStatus is "Complete" or "Terminated")
            await RetireAsync(db,tx,id,ct);
        await tx.CommitAsync(ct);
        HubTelemetry.NativeCommitDuration.Record(
            System.Diagnostics.Stopwatch.GetElapsedTime(commitStarted).TotalSeconds,
            HistoryModeTag, EventLaneModeTag);
        if(nextStatus=="Complete")HubTelemetry.WorkflowsCompleted.Add(1,HubTelemetry.HubTag(hub));
        if(nextStatus=="Terminated")HubTelemetry.WorkflowsFailed.Add(1,HubTelemetry.HubTag(hub));
        return new(id,revision+1,nextStatus,false);
    }

    internal static async Task WakeAsync(NpgsqlConnection db,NpgsqlTransaction tx,Guid id,string hub,int partition,CancellationToken ct)
    {
        await Execute(db,tx,"""
            INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,workflow_id,payload)
            SELECT @task,@hub,@partition,'__native_workflow__',1,@id,'{}'::jsonb
            FROM wfc_scale.hub_workflow WHERE id=@id AND native_status='Runnable'
            ON CONFLICT DO NOTHING
            """,ct,("task",Guid.NewGuid()),("hub",hub),("partition",partition),("id",id));
    }

    private static async Task MatchEvents(NpgsqlConnection db,NpgsqlTransaction tx,Guid id,string hub,CancellationToken ct)
    {
        await Execute(db,tx,"""
            INSERT INTO wfc_scale.hub_native_inbox(workflow_id,pointer_id,kind,source_key,payload)
            SELECT s.workflow_id,s.pointer_id,'Event',e.id::text,e.data
            FROM wfc_scale.hub_native_subscription s
            CROSS JOIN LATERAL (
              SELECT id,data FROM wfc_scale.hub_native_event e
              WHERE e.hub_id=@hub AND e.event_name=s.event_name AND e.event_key=s.event_key
                AND e.effective_at>=s.as_of AND e.effective_at<=clock_timestamp()
                AND NOT EXISTS(SELECT 1 FROM wfc_scale.hub_native_inbox i
                    WHERE i.workflow_id=s.workflow_id AND i.pointer_id=s.pointer_id
                      AND i.kind='Event' AND i.source_key=e.id::text)
              ORDER BY e.effective_at,e.id LIMIT 1
            ) e WHERE s.workflow_id=@id
              AND NOT EXISTS(SELECT 1 FROM wfc_scale.hub_native_inbox i
                  WHERE i.workflow_id=s.workflow_id AND i.pointer_id=s.pointer_id AND i.kind='Event' AND NOT i.consumed)
            ON CONFLICT DO NOTHING
            """,ct,("hub",hub),("id",id));
    }

    public async Task PublishEventAsync(string hub,HubNativeEvent request,CancellationToken ct)
    {
        if(string.IsNullOrWhiteSpace(hub)||hub.Length>80||string.IsNullOrWhiteSpace(request.EventName)||
           request.EventName.Length>200||request.EventKey.Length>200||request.EventName.StartsWith("__hub",StringComparison.Ordinal)||
           string.IsNullOrWhiteSpace(request.IdempotencyKey)||request.IdempotencyKey.Length>128||request.Data.GetRawText().Length>65536)
            throw new ArgumentException("Invalid native event");
        await using var db=await Source.OpenConnectionAsync(ct);
        await using var tx=await db.BeginTransactionAsync(ct);
        await EventLane(db,tx,hub,"publish_event",ct);
        await Execute(db,tx,"""
            INSERT INTO wfc_scale.hub_native_event(hub_id,event_name,event_key,data,effective_at,request_key)
            VALUES(@hub,@name,@key,@data::jsonb,COALESCE(@at::timestamptz,clock_timestamp()),@request)
            ON CONFLICT DO NOTHING
            """,ct,("hub",hub),("name",request.EventName),("key",request.EventKey),("data",request.Data.GetRawText()),
            ("at",request.EffectiveAt is DateTime at?at.ToUniversalTime():null),("request",request.IdempotencyKey));
        await using(var check=Command(db,tx,"""
            SELECT 1 FROM wfc_scale.hub_native_event WHERE hub_id=@hub AND request_key=@request
              AND event_name=@name AND event_key=@key AND data=@data::jsonb
              AND (@at::timestamptz IS NULL OR effective_at=@at::timestamptz)
            """,("hub",hub),("name",request.EventName),("key",request.EventKey),("data",request.Data.GetRawText()),
            ("at",request.EffectiveAt is DateTime date?date.ToUniversalTime():null),("request",request.IdempotencyKey)))
            if(await check.ExecuteScalarAsync(ct) is null)throw new InvalidOperationException("Event idempotency key reused");
        var workflows=new List<(Guid Id,int Partition)>();
        await using(var cmd=Command(db,tx,"""
            SELECT w.id,w.partition_id FROM wfc_scale.hub_workflow w
            WHERE w.hub_id=@hub AND w.execution_mode='native' AND w.native_status IN ('Runnable','Suspended')
              AND EXISTS(SELECT 1 FROM wfc_scale.hub_native_subscription s
                         WHERE s.workflow_id=w.id AND s.event_name=@name AND s.event_key=@key)
            ORDER BY w.id FOR UPDATE
            """,("hub",hub),("name",request.EventName),("key",request.EventKey)))
        {
            await using var r=await cmd.ExecuteReaderAsync(ct);
            while(await r.ReadAsync(ct))workflows.Add((r.GetGuid(0),r.GetInt32(1)));
        }
        foreach(var workflow in workflows)
        {
            await MatchEvents(db,tx,workflow.Id,hub,ct);
            await WakeAsync(db,tx,workflow.Id,hub,workflow.Partition,ct);
        }
        await tx.CommitAsync(ct);
    }

    public async Task<bool> IsActivityAsync(Guid task,CancellationToken ct)
    {
        await using var cmd=Source.CreateCommand("SELECT 1 FROM wfc_scale.hub_task WHERE id=@id AND dispatch_key IS NOT NULL");
        cmd.Parameters.AddWithValue("id",task);
        return await cmd.ExecuteScalarAsync(ct) is not null;
    }

    /// <summary>Bounded scheduler reconciliation for future-effective events and
    /// activity transport attempts exhausted without a result. All delivery is durable.</summary>
    internal static async Task SweepInboxAsync(HubTaskStore tasks,int take,CancellationToken ct)
    {
        var candidates=new List<(Guid Id,string Hub)>();
        await using(var cmd=tasks.DataSource.CreateCommand("""
            SELECT w.id,w.hub_id FROM wfc_scale.hub_workflow w
            WHERE w.execution_mode='native' AND w.native_status IN ('Runnable','Suspended') AND (
              EXISTS(SELECT 1 FROM wfc_scale.hub_native_subscription s
                JOIN wfc_scale.hub_native_event e ON e.hub_id=w.hub_id AND e.event_name=s.event_name AND e.event_key=s.event_key
                WHERE s.workflow_id=w.id AND e.effective_at>=s.as_of AND e.effective_at<=now()
                  AND NOT EXISTS(SELECT 1 FROM wfc_scale.hub_native_inbox i WHERE i.workflow_id=w.id
                    AND i.pointer_id=s.pointer_id AND i.kind='Event' AND (NOT i.consumed OR i.source_key=e.id::text)))
              OR EXISTS(SELECT 1 FROM wfc_scale.hub_task t WHERE t.workflow_id=w.id AND t.dispatch_key IS NOT NULL
                AND t.state=3 AND t.last_error IS DISTINCT FROM 'Native pointer cancelled'
                AND NOT EXISTS(SELECT 1 FROM wfc_scale.hub_native_inbox i WHERE i.workflow_id=w.id
                  AND i.pointer_id=t.execution_pointer_id AND i.kind='Activity' AND i.source_key=t.dispatch_key)))
            ORDER BY w.id LIMIT @take
            """))
        {
            cmd.Parameters.AddWithValue("take",take);
            await using var r=await cmd.ExecuteReaderAsync(ct);
            while(await r.ReadAsync(ct))candidates.Add((r.GetGuid(0),r.GetString(1)));
        }
        foreach(var candidate in candidates)
        {
            await using var db=await tasks.DataSource.OpenConnectionAsync(ct);
            await using var tx=await db.BeginTransactionAsync(ct);
            await EventLane(db,tx,candidate.Hub,"reconcile",ct);
            int partition;
            await using(var cmd=Command(db,tx,"""
                SELECT partition_id FROM wfc_scale.hub_workflow WHERE id=@id
                  AND native_status IN ('Runnable','Suspended') FOR UPDATE SKIP LOCKED
                """,("id",candidate.Id)))
            {
                var result=await cmd.ExecuteScalarAsync(ct);if(result is not int value)continue;partition=value;
            }
            await MatchEvents(db,tx,candidate.Id,candidate.Hub,ct);
            await Execute(db,tx,"""
                INSERT INTO wfc_scale.hub_native_inbox(workflow_id,pointer_id,kind,source_key,payload)
                SELECT workflow_id,execution_pointer_id,'Activity',dispatch_key,
                  jsonb_build_object('dispatchKey',dispatch_key,'outputs',null,'result',null,
                    'error','Activity transport attempts exhausted: '||COALESCE(last_error,'unknown outcome'))
                FROM wfc_scale.hub_task WHERE workflow_id=@id AND dispatch_key IS NOT NULL AND state=3
                  AND last_error IS DISTINCT FROM 'Native pointer cancelled'
                ON CONFLICT DO NOTHING
                """,ct,("id",candidate.Id));
            await WakeAsync(db,tx,candidate.Id,candidate.Hub,partition,ct);
            await tx.CommitAsync(ct);
        }
    }

    public async Task<bool> CompleteActivityAsync(Guid task,string worker,long epoch,JsonElement result,CancellationToken ct)
    {
        if(result.GetRawText().Length>65536)throw new ArgumentException("Native result exceeds limit");
        using var span=HubTelemetry.Traces.StartActivity("hub.activity.complete.transaction");
        span?.SetTag("task.id",task.ToString());
        span?.SetTag("task.epoch",epoch);
        var connStarted=System.Diagnostics.Stopwatch.GetTimestamp();
        await using var db=await Source.OpenConnectionAsync(ct);
        span?.AddEvent(new System.Diagnostics.ActivityEvent("db.connection.acquired",
            tags:new System.Diagnostics.ActivityTagsCollection {
                { "elapsed.seconds", System.Diagnostics.Stopwatch.GetElapsedTime(connStarted).TotalSeconds }}));
        await using var tx=await db.BeginTransactionAsync(ct);
        var lockStarted=System.Diagnostics.Stopwatch.GetTimestamp();
        Guid id;string hub,status,pointer,dispatch;int partition,step;
        await using(var cmd=Command(db,tx,"""
            SELECT w.id,w.hub_id,w.partition_id,w.native_status,t.execution_pointer_id,t.dispatch_key,t.native_step_id
            FROM wfc_scale.hub_workflow w JOIN wfc_scale.hub_task t ON t.workflow_id=w.id
            WHERE t.id=@task AND w.execution_mode='native' AND t.dispatch_key IS NOT NULL FOR UPDATE OF w
            """,("task",task)))
        {
            await using var r=await cmd.ExecuteReaderAsync(ct);
            if(!await r.ReadAsync(ct))return false;
            id=r.GetGuid(0);hub=r.GetString(1);partition=r.GetInt32(2);status=r.GetString(3);
            pointer=r.GetString(4);dispatch=r.GetString(5);step=r.GetInt32(6);
        }
        span?.SetTag("workflow.id",id.ToString());
        span?.AddEvent(new System.Diagnostics.ActivityEvent("db.workflow.row.locked",
            tags:new System.Diagnostics.ActivityTagsCollection {
                { "elapsed.seconds", System.Diagnostics.Stopwatch.GetElapsedTime(lockStarted).TotalSeconds }}));
        await using(var cmd=Command(db,tx,"""
            SELECT state,last_owner,epoch,result_json::text FROM wfc_scale.hub_task WHERE id=@task FOR UPDATE
            """,("task",task)))
        {
            await using var r=await cmd.ExecuteReaderAsync(ct);await r.ReadAsync(ct);
            if(r.GetInt16(0)==2 && !r.IsDBNull(1) && r.GetString(1)==worker && r.GetInt64(2)==epoch)
                return !r.IsDBNull(3) && JsonElement.DeepEquals(Json(r.GetString(3)),result);
        }
        if(status is "Complete" or "Terminated")return false;
        await using(var cmd=Command(db,tx,"""
            UPDATE wfc_scale.hub_task SET state=2,owner=NULL,lease_until=NULL,result_json=@result::jsonb,updated_at=now()
            WHERE id=@task AND owner=@worker AND epoch=@epoch AND state=1 AND lease_until>clock_timestamp()
            """,("result",result.GetRawText()),("task",task),("worker",worker),("epoch",epoch)))
            if(await cmd.ExecuteNonQueryAsync(ct)!=1)return false;
        await Execute(db,tx,"""
            UPDATE wfc_scale.hub_task_attempt SET outcome='Completed',finished_at=clock_timestamp()
            WHERE task_id=@task AND epoch=@epoch AND finished_at IS NULL
            """,ct,("task",task),("epoch",epoch));
        await Execute(db,tx,"""
            INSERT INTO wfc_scale.hub_native_inbox(workflow_id,pointer_id,kind,source_key,payload)
            VALUES(@id,@pointer,'Activity',@dispatch,@result::jsonb) ON CONFLICT DO NOTHING
            """,ct,("id",id),("pointer",pointer),("dispatch",dispatch),("result",result.GetRawText()));
        await HubRemoteWorkflowStore.JournalAsync(db,tx,id,step,task,"__native_activity__","ActivityResultReceived",
            JsonSerializer.Serialize(new{executionPointerId=pointer,dispatchKey=dispatch}),ct);
        await WakeAsync(db,tx,id,hub,partition,ct);
        var commitStarted=System.Diagnostics.Stopwatch.GetTimestamp();
        await tx.CommitAsync(ct);
        span?.AddEvent(new System.Diagnostics.ActivityEvent("db.completion.committed",
            tags:new System.Diagnostics.ActivityTagsCollection {
                { "elapsed.seconds", System.Diagnostics.Stopwatch.GetElapsedTime(commitStarted).TotalSeconds }}));
        HubTelemetry.TasksCompleted.Add(1,HubTelemetry.HubTag(hub));
        return true;
    }

    private static async Task RetireAsync(NpgsqlConnection db,NpgsqlTransaction tx,Guid id,CancellationToken ct)
    {
        await Execute(db,tx,"DELETE FROM wfc_scale.hub_native_subscription WHERE workflow_id=@id",ct,("id",id));
        await Execute(db,tx,"""
            UPDATE wfc_scale.hub_timer SET state=2,cancelled_at=clock_timestamp() WHERE workflow_id=@id AND state=0;
            UPDATE wfc_scale.hub_task SET state=3,owner=NULL,lease_until=NULL,last_error='Workflow ended',updated_at=now()
            WHERE workflow_id=@id AND state IN (0,1);
            UPDATE wfc_scale.hub_task_attempt a SET outcome='Cancelled',finished_at=clock_timestamp()
            FROM wfc_scale.hub_task t WHERE t.id=a.task_id AND t.workflow_id=@id
              AND t.last_error='Workflow ended' AND a.finished_at IS NULL
            """,ct,("id",id));
    }

    public async Task<bool> ChangeStatusAsync(Guid id,HubNativeStatusChange request,CancellationToken ct)
    {
        if(request.Status is not ("Runnable" or "Suspended" or "Terminated") ||
           string.IsNullOrWhiteSpace(request.IdempotencyKey)||request.IdempotencyKey.Length>128)
            throw new ArgumentException("Invalid native status change");
        await using var db=await Source.OpenConnectionAsync(ct);
        await using var tx=await db.BeginTransactionAsync(ct);
        string hub,status;int partition;
        await using(var cmd=Command(db,tx,"""
            SELECT hub_id,partition_id,native_status FROM wfc_scale.hub_workflow
            WHERE id=@id AND execution_mode='native' FOR UPDATE
            """,("id",id)))
        {
            await using var r=await cmd.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct))return false;
            hub=r.GetString(0);partition=r.GetInt32(1);status=r.GetString(2);
        }
        await using(var check=Command(db,tx,"SELECT status FROM wfc_scale.hub_native_status_request WHERE workflow_id=@id AND request_key=@key",
            ("id",id),("key",request.IdempotencyKey)))
        {
            if(await check.ExecuteScalarAsync(ct) is string previous)
            { if(previous!=request.Status)throw new InvalidOperationException("Status idempotency key reused");return true; }
        }
        if(status is "Complete" or "Terminated")return false;
        await Execute(db,tx,"""
            INSERT INTO wfc_scale.hub_native_status_request(workflow_id,request_key,status) VALUES(@id,@key,@status);
            UPDATE wfc_scale.hub_workflow SET native_status=@status,revision=revision+1,updated_at=now(),
              state=CASE WHEN @status='Terminated' THEN 3 ELSE 1 END,
              completed_at=CASE WHEN @status='Terminated' THEN now() ELSE NULL END WHERE id=@id;
            UPDATE wfc_scale.hub_task SET state=3,owner=NULL,lease_until=NULL,last_error='Workflow status changed'
            WHERE workflow_id=@id AND task_type='__native_workflow__' AND state IN (0,1)
            """,ct,("id",id),("key",request.IdempotencyKey),("status",request.Status));
        if(request.Status=="Runnable")await WakeAsync(db,tx,id,hub,partition,ct);
        if(request.Status=="Terminated")await RetireAsync(db,tx,id,ct);
        await HubRemoteWorkflowStore.JournalAsync(db,tx,id,0,null,"__native_workflow__","WorkflowStatusChanged",
            JsonSerializer.Serialize(request),ct);
        await tx.CommitAsync(ct);return true;
    }
}

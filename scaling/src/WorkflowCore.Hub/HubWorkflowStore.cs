using System.Text.Json;
using System.Diagnostics;
using Npgsql;
using NpgsqlTypes;

namespace WorkflowCore.Hub;

public sealed record HubWorkflowStartRequest(string HubId, string DefinitionId,
    int Version, JsonElement Input, string? IdempotencyKey = null,
    string? RunId = null);
public sealed record HubWorkflowStatus(Guid Id, string HubId, string DefinitionId,
    int Version, int PartitionId, string State, int NextStep, int StepCount,
    JsonElement Results, string? Error, DateTime CreatedAt, DateTime? CompletedAt,
    string ExecutionMode = "inline", string? NativeStatus = null);
public sealed record HubWorkflowRunSummary(string RunId, long Total, long Ready,
    long Running, long Completed, long Failed, DateTime? FirstCreatedAt,
    DateTime? LastCompletedAt, double? P50CompletionMs,
    double? P95CompletionMs, double? P99CompletionMs,
    double? MaxCompletionMs);
public sealed record HubWorkflowListItem(Guid Id, string HubId, string DefinitionId,
    int Version, string? RunId, string State, int NextStep, int StepCount,
    string? Error, DateTime CreatedAt, DateTime? CompletedAt);
public sealed record HubWorkflowListPage(IReadOnlyList<HubWorkflowListItem> Items,
    long Total, int Page, int Take);
public sealed record HubWorkflowOverview(long Total, long Ready, long Running,
    long Completed, long Failed, long Created24h, long Completed24h,
    long Suspended = 0, long Terminated = 0);
public sealed record HubRecentRun(string RunId, long Total, long Ready, long Running,
    long Completed, long Failed, DateTime LastCreatedAt);
public sealed record HubWorkflowTraceAttempt(
    long Epoch, int AttemptNumber, string WorkerId,
    DateTime ClaimedAt, DateTime? FinishedAt, string Outcome,
    string Source = "lease", string? TraceId = null, string? SpanId = null);
public sealed record HubWorkflowTraceStep(
    int StepIndex, Guid TaskId, string ActivityType, int Version,
    string State, DateTime? ReadyAt,
    IReadOnlyList<HubWorkflowTraceAttempt> Attempts, string? ExecutionPointerId = null, string? DispatchKey = null);
public sealed record HubWorkflowHistoryEntry(int StepIndex, Guid TaskId,
    string ActivityType, int Attempts, string EventType, DateTime OccurredAt,
    JsonElement? Payload, string? ExecutionPointerId = null, string? ParentPointerId = null,
    JsonElement? Scope = null, int? Attempt = null);

/// <summary>
/// Hub-managed durable sequential workflow execution. The gateway alone
/// accesses PostgreSQL; each activity completion and next-step release is
/// committed atomically. The same bounded DataSource is shared with HubTaskStore.
/// </summary>
public sealed class HubWorkflowStore(
    HubTaskStore tasks, HubPolicyRegistry policies, HubWorkflowRegistry definitions)
{
    private NpgsqlDataSource DataSource => tasks.DataSource;

    public async Task<Guid> StartAsync(HubWorkflowStartRequest request, CancellationToken ct)
    {
        using var span = HubTelemetry.Traces.StartActivity("hub.workflow.start");
        span?.SetTag("hub", request.HubId);
        span?.SetTag("workflow.definition", request.DefinitionId);
        var startedAt = Stopwatch.GetTimestamp();
        var definition = definitions.Get(request.HubId, request.DefinitionId, request.Version)
            ?? throw new KeyNotFoundException("Unknown code-first Hub workflow");
        if (request.Input.ValueKind != JsonValueKind.Object ||
            request.Input.GetRawText().Length > 65536 ||
            request.IdempotencyKey?.Length > 128 || request.RunId?.Length > 100)
            throw new ArgumentException("Invalid workflow input, run identifier or idempotency key");

        var routes = definition.Steps.Select(step =>
            policies.Resolve(definition.HubId, step.ActivityType, step.Version, request.Input))
            .ToArray();
        if (routes.Any(route => route.PartitionCount != definition.Partitions))
            throw new InvalidOperationException("Activity and workflow partition counts differ");

        var resources = routes.SelectMany(route => route.Resources)
            .GroupBy(resource => resource.Key, StringComparer.Ordinal)
            .Select(group =>
            {
                var first = group.First();
                if (group.Any(other => other.Capacity != first.Capacity ||
                                       other.StrictFifo != first.StrictFifo))
                    throw new InvalidOperationException("Inconsistent concurrency policy");
                return first;
            }).OrderBy(x => x.Key, StringComparer.Ordinal).ToArray();

        var id = Guid.NewGuid();
        var partition = HubPartitionRouter.ForWorkflow(id, definition.Partitions);
        var input = request.Input.GetRawText();

        await using var db = await DataSource.OpenConnectionAsync(ct);
        await using var tx = await db.BeginTransactionAsync(ct);
        await using (var insert = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_workflow
                (id, hub_id, definition_id, definition_version, partition_id,
                 input_json, step_count, run_id, idempotency_key)
            VALUES (@id, @hub, @definition, @version, @partition, @input,
                    @count, @run, @key)
            ON CONFLICT DO NOTHING RETURNING id
            """, db, tx))
        {
            insert.Parameters.AddWithValue("id", id);
            insert.Parameters.AddWithValue("hub", definition.HubId);
            insert.Parameters.AddWithValue("definition", definition.DefinitionId);
            insert.Parameters.AddWithValue("version", definition.Version);
            insert.Parameters.AddWithValue("partition", partition);
            insert.Parameters.Add("input", NpgsqlDbType.Jsonb).Value = input;
            insert.Parameters.AddWithValue("count", routes.Length);
            insert.Parameters.Add("run", NpgsqlDbType.Text).Value =
                (object?)request.RunId ?? DBNull.Value;
            insert.Parameters.Add("key", NpgsqlDbType.Text).Value =
                (object?)request.IdempotencyKey ?? DBNull.Value;
            var inserted = await insert.ExecuteScalarAsync(ct);
            if (inserted is null)
            {
                if (request.IdempotencyKey is null)
                    throw new InvalidOperationException("Workflow ID collision");
                await using var existing = new NpgsqlCommand("""
                    SELECT id FROM wfc_scale.hub_workflow
                    WHERE hub_id=@hub AND idempotency_key=@key
                      AND definition_id=@definition AND definition_version=@version
                      AND input_json=@input AND run_id IS NOT DISTINCT FROM @run
                    """, db, tx);
                existing.Parameters.AddWithValue("hub", definition.HubId);
                existing.Parameters.AddWithValue("key", request.IdempotencyKey);
                existing.Parameters.AddWithValue("definition", definition.DefinitionId);
                existing.Parameters.AddWithValue("version", definition.Version);
                existing.Parameters.Add("input", NpgsqlDbType.Jsonb).Value = input;
                existing.Parameters.Add("run", NpgsqlDbType.Text).Value =
                    (object?)request.RunId ?? DBNull.Value;
                var previous = await existing.ExecuteScalarAsync(ct);
                if (previous is null)
                    throw new InvalidOperationException("Workflow idempotency key payload mismatch");
                await tx.CommitAsync(ct);
                return (Guid)previous;
            }
        }

        // Register/verify resources under a deterministic lock order, including
        // strict FIFO keys, before allocating sequence numbers to future tasks.
        foreach (var resource in resources)
        {
            await using (var insert = new NpgsqlCommand("""
                INSERT INTO wfc_scale.hub_resource(resource_key,capacity,strict_fifo)
                VALUES (@key,@limit,@fifo)
                ON CONFLICT (resource_key) DO NOTHING
                """, db, tx))
            {
                insert.Parameters.AddWithValue("key", resource.Key);
                insert.Parameters.AddWithValue("limit", resource.Capacity);
                insert.Parameters.AddWithValue("fifo", resource.StrictFifo);
                await insert.ExecuteNonQueryAsync(ct);
            }
            await using var check = new NpgsqlCommand("""
                SELECT capacity,strict_fifo FROM wfc_scale.hub_resource
                WHERE resource_key=@key FOR UPDATE
                """, db, tx);
            check.Parameters.AddWithValue("key", resource.Key);
            await using var reader = await check.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct) ||
                reader.GetInt32(0) != resource.Capacity ||
                reader.GetBoolean(1) != resource.StrictFifo)
                throw new InvalidOperationException("Existing concurrency policy does not match C#");
        }

        for (var step = 0; step < routes.Length; step++)
        {
            var taskId = Guid.NewGuid();
            await using (var task = new NpgsqlCommand("""
                INSERT INTO wfc_scale.hub_task
                    (id,hub_id,partition_id,task_type,task_version,workflow_id,
                     workflow_step,payload,due_at,traceparent,tracestate)
                VALUES (@id,@hub,@partition,@type,@version,@workflow,@step,
                        @payload,CASE WHEN @first THEN now()
                                      ELSE 'infinity'::timestamptz END,@traceparent,@tracestate)
                """, db, tx))
            {
                task.Parameters.AddWithValue("id", taskId);
                task.Parameters.AddWithValue("hub", definition.HubId);
                task.Parameters.AddWithValue("partition", partition);
                task.Parameters.AddWithValue("type", routes[step].TaskType);
                task.Parameters.AddWithValue("version", routes[step].Version);
                task.Parameters.AddWithValue("workflow", id);
                task.Parameters.AddWithValue("step", step);
                task.Parameters.AddWithValue("first", step == 0);
                task.Parameters.Add("payload", NpgsqlDbType.Jsonb).Value = input;
                task.Parameters.Add("traceparent", NpgsqlDbType.Text).Value =
                    (object?)HubTraceCorrelation.TraceParent ?? DBNull.Value;
                task.Parameters.Add("tracestate", NpgsqlDbType.Text).Value =
                    (object?)HubTraceCorrelation.TraceState ?? DBNull.Value;
                await task.ExecuteNonQueryAsync(ct);
            }
            foreach (var resource in routes[step].Resources)
            {
                await using var relation = new NpgsqlCommand("""
                    INSERT INTO wfc_scale.hub_task_constraint(task_id,resource_key)
                    VALUES (@task,@resource)
                    """, db, tx);
                relation.Parameters.AddWithValue("task", taskId);
                relation.Parameters.AddWithValue("resource", resource.Key);
                await relation.ExecuteNonQueryAsync(ct);
            }
        }
        await tx.CommitAsync(ct);
        HubTelemetry.WorkflowsStarted.Add(1, HubTelemetry.HubTag(request.HubId));
        HubTelemetry.StoreOperationDuration.Record(Stopwatch.GetElapsedTime(startedAt).TotalSeconds,
            HubTelemetry.HubTag(request.HubId), new("operation", "start"));
        return id;
    }

    public async Task<bool> CompleteAsync(Guid taskId, string owner, long epoch,
        string result, CancellationToken ct)
    {
        await using var db = await DataSource.OpenConnectionAsync(ct);
        await using var tx = await db.BeginTransactionAsync(ct);
        var task = await LockManagedTaskAsync(db, tx, taskId, ct);
        if (task is null) return false;
        if (task.State == 2)
        {
            await using var repeat = new NpgsqlCommand("""
                SELECT 1 FROM wfc_scale.hub_task
                WHERE id=@id AND state=2 AND last_owner=@owner
                  AND epoch=@epoch AND result_json=@result
                """, db, tx);
            repeat.Parameters.AddWithValue("id", taskId);
            repeat.Parameters.AddWithValue("owner", owner);
            repeat.Parameters.AddWithValue("epoch", epoch);
            repeat.Parameters.Add("result", NpgsqlDbType.Jsonb).Value = result;
            return await repeat.ExecuteScalarAsync(ct) is not null;
        }
        if (task.State != 1 || task.Owner != owner || task.Epoch != epoch ||
            task.LeaseUntil <= DateTime.UtcNow || task.WorkflowState >= 2 ||
            task.NextStep != task.Step)
            return false;

        await using (var complete = new NpgsqlCommand("""
            UPDATE wfc_scale.hub_task
            SET state=2, owner=NULL, lease_until=NULL, updated_at=now(),
                result_json=@result
            WHERE id=@id AND state=1 AND owner=@owner AND epoch=@epoch
              AND lease_until>now()
            """, db, tx))
        {
            complete.Parameters.AddWithValue("id", taskId);
            complete.Parameters.AddWithValue("owner", owner);
            complete.Parameters.AddWithValue("epoch", epoch);
            complete.Parameters.Add("result", NpgsqlDbType.Jsonb).Value = result;
            if (await complete.ExecuteNonQueryAsync(ct) != 1) return false;
        }
        // Persist completion of this exact fenced lease in the same transaction
        // as step/result commit. Historic pre-migration tasks can have no row.
        await using (var attempt = new NpgsqlCommand("""
            UPDATE wfc_scale.hub_task_attempt
            SET finished_at=now(),outcome='Completed',trace_id=@traceId,span_id=@spanId
            WHERE task_id=@id AND epoch=@epoch AND worker_id=@owner
              AND finished_at IS NULL
            """, db, tx))
        {
            attempt.Parameters.AddWithValue("id", taskId);
            attempt.Parameters.AddWithValue("epoch", epoch);
            attempt.Parameters.AddWithValue("owner", owner);
            attempt.Parameters.Add("traceId", NpgsqlDbType.Text).Value =
                (object?)HubTraceCorrelation.TraceId ?? DBNull.Value;
            attempt.Parameters.Add("spanId", NpgsqlDbType.Text).Value =
                (object?)HubTraceCorrelation.SpanId ?? DBNull.Value;
            await attempt.ExecuteNonQueryAsync(ct);
        }
        await using (var progress = new NpgsqlCommand("""
            UPDATE wfc_scale.hub_workflow
            SET next_step=@next,
                state=CASE WHEN @next=step_count THEN 2 ELSE 1 END,
                results_json=jsonb_set(results_json, ARRAY[@index],
                                       @result::jsonb, true),
                completed_at=CASE WHEN @next=step_count THEN now() ELSE NULL END,
                updated_at=now()
            WHERE id=@workflow AND next_step=@step AND state IN (0,1)
            """, db, tx))
        {
            progress.Parameters.AddWithValue("next", task.Step + 1);
            progress.Parameters.AddWithValue("index", task.Step.ToString());
            progress.Parameters.Add("result", NpgsqlDbType.Jsonb).Value = result;
            progress.Parameters.AddWithValue("workflow", task.WorkflowId);
            progress.Parameters.AddWithValue("step", task.Step);
            if (await progress.ExecuteNonQueryAsync(ct) != 1) return false;
        }
        await using (var history = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_workflow_history
                (workflow_id,step_index,task_id,event_type,payload)
            VALUES (@workflow,@step,@task,'StepCompleted',@result)
            ON CONFLICT DO NOTHING
            """, db, tx))
        {
            history.Parameters.AddWithValue("workflow", task.WorkflowId);
            history.Parameters.AddWithValue("step", task.Step);
            history.Parameters.AddWithValue("task", taskId);
            history.Parameters.Add("result", NpgsqlDbType.Jsonb).Value = result;
            await history.ExecuteNonQueryAsync(ct);
        }
        await using (var next = new NpgsqlCommand("""
            UPDATE wfc_scale.hub_task
            SET due_at=now(), updated_at=now(),
                traceparent=@traceparent, tracestate=@tracestate
            WHERE workflow_id=@workflow AND workflow_step=@step AND state=0
              AND due_at='infinity'::timestamptz
            """, db, tx))
        {
            next.Parameters.AddWithValue("workflow", task.WorkflowId);
            next.Parameters.AddWithValue("step", task.Step + 1);
            next.Parameters.Add("traceparent", NpgsqlDbType.Text).Value =
                (object?)HubTraceCorrelation.TraceParent ?? DBNull.Value;
            next.Parameters.Add("tracestate", NpgsqlDbType.Text).Value =
                (object?)HubTraceCorrelation.TraceState ?? DBNull.Value;
            await next.ExecuteNonQueryAsync(ct);
        }
        await tx.CommitAsync(ct);
        HubTelemetry.TasksCompleted.Add(1, HubTelemetry.HubTag(task.HubId),
            HubTelemetry.ActivityTag(task.ActivityType));
        if (task.Step + 1 == task.StepCount)
        {
            HubTelemetry.WorkflowsCompleted.Add(1, HubTelemetry.HubTag(task.HubId));
            HubTelemetry.WorkflowDuration.Record((DateTime.UtcNow - task.CreatedAt).TotalSeconds,
                HubTelemetry.HubTag(task.HubId));
        }
        return true;
    }

    public async Task<bool> FailAsync(Guid taskId, string owner, long epoch,
        string error, TimeSpan retryDelay, CancellationToken ct)
    {
        if (retryDelay < TimeSpan.Zero || retryDelay > TimeSpan.FromHours(1))
            return false;
        await using var db = await DataSource.OpenConnectionAsync(ct);
        await using var tx = await db.BeginTransactionAsync(ct);
        var task = await LockManagedTaskAsync(db, tx, taskId, ct);
        if (task is null || task.State != 1 || task.Owner != owner ||
            task.Epoch != epoch || task.LeaseUntil <= DateTime.UtcNow ||
            task.WorkflowState >= 2)
            return false;
        var dead = task.Attempts >= task.MaxAttempts;
        await using (var fail = new NpgsqlCommand("""
            UPDATE wfc_scale.hub_task
            SET state=CASE WHEN @dead THEN 3 ELSE 0 END,
                owner=NULL,lease_until=NULL,due_at=now()+@delay,
                last_error=@error,updated_at=now()
            WHERE id=@id AND state=1 AND owner=@owner AND epoch=@epoch
              AND lease_until>now()
            """, db, tx))
        {
            fail.Parameters.AddWithValue("dead", dead);
            fail.Parameters.Add("delay", NpgsqlDbType.Interval).Value = retryDelay;
            fail.Parameters.AddWithValue("error", error.Length > 1000 ? error[..1000] : error);
            fail.Parameters.AddWithValue("id", taskId);
            fail.Parameters.AddWithValue("owner", owner);
            fail.Parameters.AddWithValue("epoch", epoch);
            if (await fail.ExecuteNonQueryAsync(ct) != 1) return false;
        }
        await using (var attempt = new NpgsqlCommand("""
            UPDATE wfc_scale.hub_task_attempt
            SET finished_at=now(),
                outcome=CASE WHEN @dead THEN 'Failed' ELSE 'RetryScheduled' END,
                trace_id=@traceId,span_id=@spanId
            WHERE task_id=@id AND epoch=@epoch AND worker_id=@owner
              AND finished_at IS NULL
            """, db, tx))
        {
            attempt.Parameters.AddWithValue("id", taskId);
            attempt.Parameters.AddWithValue("epoch", epoch);
            attempt.Parameters.AddWithValue("owner", owner);
            attempt.Parameters.AddWithValue("dead", dead);
            attempt.Parameters.Add("traceId", NpgsqlDbType.Text).Value =
                (object?)HubTraceCorrelation.TraceId ?? DBNull.Value;
            attempt.Parameters.Add("spanId", NpgsqlDbType.Text).Value =
                (object?)HubTraceCorrelation.SpanId ?? DBNull.Value;
            await attempt.ExecuteNonQueryAsync(ct);
        }
        if (dead)
        {
            await using var terminate = new NpgsqlCommand("""
                UPDATE wfc_scale.hub_workflow
                SET state=3,last_error=@error,completed_at=now(),updated_at=now()
                WHERE id=@id AND state IN (0,1)
                """, db, tx);
            terminate.Parameters.AddWithValue("id", task.WorkflowId);
            terminate.Parameters.AddWithValue("error", error.Length > 1000 ? error[..1000] : error);
            await terminate.ExecuteNonQueryAsync(ct);
        }
        await using (var history = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_workflow_history
                (workflow_id,step_index,task_id,event_type)
            VALUES (@workflow,@step,@task,@event)
            ON CONFLICT DO NOTHING
            """, db, tx))
        {
            history.Parameters.AddWithValue("workflow", task.WorkflowId);
            history.Parameters.AddWithValue("step", task.Step);
            history.Parameters.AddWithValue("task", taskId);
            history.Parameters.AddWithValue("event", dead ? "StepFailed" : "StepRetryScheduled");
            await history.ExecuteNonQueryAsync(ct);
        }
        await tx.CommitAsync(ct);
        HubTelemetry.TasksFailed.Add(1, HubTelemetry.HubTag(task.HubId),
            HubTelemetry.ActivityTag(task.ActivityType),
            new KeyValuePair<string, object?>("outcome", dead ? "dead" : "retry"));
        if (dead)
            HubTelemetry.WorkflowsFailed.Add(1, HubTelemetry.HubTag(task.HubId));
        return true;
    }

    public async Task ReconcileDeadAsync(CancellationToken ct)
    {
        await using var cmd = DataSource.CreateCommand("""
            UPDATE wfc_scale.hub_workflow w
            SET state=3,completed_at=now(),updated_at=now(),
                last_error='Task exhausted its lease/retry limit'
            WHERE w.state IN (0,1) AND EXISTS (
                SELECT 1 FROM wfc_scale.hub_task t
                WHERE t.workflow_id=w.id AND t.workflow_step IS NOT NULL
                  AND t.state=3
            )
            """);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<HubWorkflowStatus?> GetAsync(Guid id, CancellationToken ct)
    {
        await using var cmd = DataSource.CreateCommand("""
            SELECT id,hub_id,definition_id,definition_version,partition_id,
                   state,next_step,step_count,results_json::text,last_error,
                   created_at,completed_at,execution_mode,native_status
            FROM wfc_scale.hub_workflow WHERE id=@id
            """);
        cmd.Parameters.AddWithValue("id", id);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return null;
        var states = new[] { "Ready", "Running", "Complete", "Failed" };
        return new(reader.GetGuid(0),reader.GetString(1),reader.GetString(2),
            reader.GetInt32(3),reader.GetInt32(4),reader.GetString(12)=="native" && reader.GetString(13)!="Runnable"
                ?reader.GetString(13):states[reader.GetInt16(5)],
            reader.GetInt32(6),reader.GetInt32(7),
            JsonDocument.Parse(reader.GetString(8)).RootElement.Clone(),
            reader.IsDBNull(9) ? null : reader.GetString(9),
            reader.GetDateTime(10),reader.IsDBNull(11) ? null : reader.GetDateTime(11),reader.GetString(12),
            reader.GetString(12)=="native"?reader.GetString(13):null);
    }

    /// <summary>
    /// A durable lease-based trace waterfall. ClaimedAt is persisted by the Hub
    /// when it fences and assigns a task. It is NOT necessarily the precise
    /// remote handler start (which belongs to OpenTelemetry worker spans).
    /// Historic workflows without attempt rows have no fabricated bars.
    /// </summary>
    public async Task<IReadOnlyList<HubWorkflowTraceStep>> TraceAsync(Guid id,
        CancellationToken ct)
    {
        await using var cmd = DataSource.CreateCommand("""
            SELECT COALESCE(t.workflow_step,t.native_step_id),t.id,t.task_type,t.task_version,t.state,
                   CASE WHEN t.dispatch_key IS NOT NULL THEN t.created_at
                     WHEN t.workflow_step=0 THEN w.created_at
                     ELSE (SELECT max(h.occurred_at)
                           FROM wfc_scale.hub_workflow_history h
                           WHERE h.workflow_id=t.workflow_id
                             AND h.step_index=t.workflow_step-1
                             AND h.event_type='StepCompleted') END AS ready_at,
                   a.epoch,a.attempt_number,a.worker_id,
                   a.claimed_at,a.finished_at,a.outcome,t.execution_pointer_id,t.dispatch_key,
                   a.trace_id,a.span_id
            FROM wfc_scale.hub_task t
            JOIN wfc_scale.hub_workflow w ON w.id=t.workflow_id
            LEFT JOIN wfc_scale.hub_task_attempt a ON a.task_id=t.id
            WHERE t.workflow_id=@id AND (t.workflow_step IS NOT NULL OR t.native_step_id IS NOT NULL)
            ORDER BY COALESCE(t.workflow_step,t.native_step_id),t.created_at,t.id,a.epoch
            """);
        cmd.Parameters.AddWithValue("id", id);
        var grouped = new List<HubWorkflowTraceStep>();
        var attempts = new List<HubWorkflowTraceAttempt>();
        int previousIndex = -1;
        Guid taskId = default;
        string activity = "";
        int version = 0;
        string state = "";
        DateTime? readyAt = null;
        string? pointerId=null,dispatchKey=null;
        void Flush()
        {
            if (previousIndex < 0) return;
            grouped.Add(new(previousIndex, taskId, activity, version,
                state, readyAt, attempts.ToArray(),pointerId,dispatchKey));
            attempts.Clear();
        }
        string[] states = ["Ready", "Leased", "Completed", "Failed"];
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            int index = reader.GetInt32(0);
            if (reader.GetGuid(1) != taskId)
            {
                Flush();
                previousIndex = index;
                taskId = reader.GetGuid(1);
                activity = reader.GetString(2);
                version = reader.GetInt32(3);
                state = states[reader.GetInt16(4)];
                readyAt = reader.IsDBNull(5) ? null : reader.GetDateTime(5);
                pointerId=reader.IsDBNull(12)?null:reader.GetString(12);
                dispatchKey=reader.IsDBNull(13)?null:reader.GetString(13);
            }
            if (!reader.IsDBNull(6))
                attempts.Add(new(reader.GetInt64(6),reader.GetInt32(7),
                    reader.GetString(8),reader.GetDateTime(9),
                    reader.IsDBNull(10) ? null : reader.GetDateTime(10),
                    reader.GetString(11),"lease",
                    reader.IsDBNull(14) ? null : reader.GetString(14),
                    reader.IsDBNull(15) ? null : reader.GetString(15)));
        }
        Flush();

        // Native in-process steps never create a separate hub_task row. Their
        // timed spans are recorded by the remote workflow executor as
        // NativeStepSpan events with the *actual* worker-observed timestamps.
        // Earlier runs retain StepCompleted events without duration metadata:
        // render only an honest completion marker, never a made-up bar.
        await using var native = DataSource.CreateCommand("""
            SELECT e.step_index,COALESCE(e.task_id,'00000000-0000-0000-0000-000000000000'::uuid),
                   e.activity_type,e.execution_pointer_id,e.attempt,e.event_type,
                   e.payload::text,w.definition_version
            FROM wfc_scale.hub_workflow_event e
            JOIN wfc_scale.hub_workflow w ON w.id=e.workflow_id
            WHERE e.workflow_id=@id AND w.execution_mode='native'
              AND e.event_type IN ('NativeStepSpan','StepCompleted')
            ORDER BY e.occurred_at,e.id
            """);
        native.Parameters.AddWithValue("id",id);
        var nativeRows = new Dictionary<(int Step,string Pointer),
            (Guid TaskId,string Name,int Version,List<HubWorkflowTraceAttempt> Attempts)>();
        await using (var events = await native.ExecuteReaderAsync(ct))
        {
            while (await events.ReadAsync(ct))
            {
                var index=events.GetInt32(0);
                var task=events.GetGuid(1);
                var name=events.GetString(2);
                var pointer=events.IsDBNull(3)?task.ToString("N"):events.GetString(3);
                var key=(index,pointer);
                // Remote activity dispatches already have a persisted lease
                // trace. Do not create overlapping synthetic native spans.
                if(grouped.Any(t=>t.StepIndex==index &&
                    (t.DispatchKey is not null || t.ExecutionPointerId is null ||
                     t.ExecutionPointerId==pointer)))
                    continue;
                if(!nativeRows.TryGetValue(key,out var entry))
                    entry=(task,name,events.GetInt32(7),new List<HubWorkflowTraceAttempt>());
                if(events.GetString(5)=="NativeStepSpan" && !events.IsDBNull(6))
                {
                    using var payload=JsonDocument.Parse(events.GetString(6));
                    var json=payload.RootElement;
                    if(json.TryGetProperty("startedAt",out var started) &&
                       json.TryGetProperty("finishedAt",out var finished) &&
                       json.TryGetProperty("workerId",out var worker) &&
                       json.TryGetProperty("epoch",out var leaseEpoch) &&
                       started.TryGetDateTime(out var startAt) &&
                       finished.TryGetDateTime(out var finishAt) &&
                       leaseEpoch.TryGetInt64(out var epoch) &&
                       startAt<=finishAt && epoch>0)
                    {
                        var number=json.TryGetProperty("attemptNumber",out var attempt) &&
                            attempt.TryGetInt32(out var retry)?retry:
                            (events.IsDBNull(4)?1:events.GetInt32(4)+1);
                        entry.Attempts.Add(new(epoch,Math.Max(1,number),
                            worker.GetString()??"native",startAt.ToUniversalTime(),
                            finishAt.ToUniversalTime(),"Completed","native-step"));
                    }
                }
                nativeRows[key]=entry;
            }
        }
        foreach(var ((index,pointer),entry) in nativeRows)
            grouped.Add(new(index,entry.TaskId,entry.Name,entry.Version,
                "Completed",null,entry.Attempts.ToArray(),pointer));
        return grouped.OrderBy(s=>s.StepIndex).ThenBy(s=>s.ActivityType,StringComparer.Ordinal)
            .ToArray();
    }

    public async Task<IReadOnlyList<HubWorkflowHistoryEntry>> HistoryAsync(Guid id,
        CancellationToken ct)
    {
        await using var cmd = DataSource.CreateCommand("""
            SELECT step_index,task_id,activity_type,attempts,event_type,
                   occurred_at,payload,execution_pointer_id,parent_pointer_id,scope,attempt
            FROM (
                SELECT h.step_index,h.task_id,t.task_type AS activity_type,t.attempts,
                       h.event_type,h.occurred_at,h.payload::text AS payload,
                       t.execution_pointer_id,NULL::text AS parent_pointer_id,NULL::text AS scope,NULL::integer AS attempt
                FROM wfc_scale.hub_workflow_history h
                JOIN wfc_scale.hub_task t ON t.id=h.task_id
                WHERE h.workflow_id=@id
                UNION ALL
                SELECT e.step_index,COALESCE(e.task_id,'00000000-0000-0000-0000-000000000000'::uuid),
                       e.activity_type,0,e.event_type,e.occurred_at,e.payload::text,
                       e.execution_pointer_id,e.parent_pointer_id,e.scope::text,e.attempt
                FROM wfc_scale.hub_workflow_event e
                WHERE e.workflow_id=@id
            ) h ORDER BY occurred_at,step_index,task_id
            """);
        cmd.Parameters.AddWithValue("id", id);
        var items = new List<HubWorkflowHistoryEntry>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            items.Add(new(reader.GetInt32(0),reader.GetGuid(1),reader.GetString(2),
                reader.GetInt32(3),reader.GetString(4),reader.GetDateTime(5),
                reader.IsDBNull(6) ? null :
                JsonDocument.Parse(reader.GetString(6)).RootElement.Clone(),
                reader.IsDBNull(7)?null:reader.GetString(7),reader.IsDBNull(8)?null:reader.GetString(8),
                reader.IsDBNull(9)?null:JsonDocument.Parse(reader.GetString(9)).RootElement.Clone(),
                reader.IsDBNull(10)?null:reader.GetInt32(10)));
        return items;
    }

    public async Task<HubWorkflowListPage> ListAsync(string? hubId, string? search,
        string? status, int page, int take, CancellationToken ct)
    {
        if (page is < 1 or > 10000 || take is < 1 or > 100 ||
            hubId?.Length > 80 || search?.Length > 200 ||
            (status is not null && status is not ("Ready" or "Running" or "Runnable" or "Complete" or "Failed" or "Suspended" or "Terminated")))
            throw new ArgumentException("Invalid Hub workflow list filter or paging");
        var pattern = "%" + (search ?? "").Replace("\\", "\\\\")
            .Replace("%", "\\%").Replace("_", "\\_") + "%";
        const string where = """
            WHERE (@hub IS NULL OR hub_id = @hub)
              AND (@status IS NULL OR
                   (execution_mode='native' AND native_status=@status AND @status IN ('Suspended','Terminated')) OR
                   (@status = 'Runnable' AND state IN (0,1) AND (execution_mode<>'native' OR native_status='Runnable')) OR
                   ((execution_mode<>'native' OR native_status NOT IN ('Suspended','Terminated')) AND state = CASE @status
                   WHEN 'Ready' THEN 0 WHEN 'Running' THEN 1
                   WHEN 'Complete' THEN 2 WHEN 'Failed' THEN 3 ELSE -1 END))
              AND (@search IS NULL OR
                   id::text ILIKE @pattern ESCAPE '\' OR
                   definition_id ILIKE @pattern ESCAPE '\' OR
                   coalesce(run_id, '') ILIKE @pattern ESCAPE '\')
            """;
        await using var db = await DataSource.OpenConnectionAsync(ct);
        async Task<NpgsqlCommand> Command(string sql)
        {
            var command = new NpgsqlCommand(sql, db);
            command.Parameters.Add("hub", NpgsqlDbType.Text).Value =
                (object?)hubId ?? DBNull.Value;
            command.Parameters.Add("status", NpgsqlDbType.Text).Value =
                (object?)status ?? DBNull.Value;
            command.Parameters.Add("search", NpgsqlDbType.Text).Value =
                string.IsNullOrWhiteSpace(search) ? DBNull.Value : search;
            command.Parameters.AddWithValue("pattern", pattern);
            return await Task.FromResult(command);
        }
        long total;
        await using (var count = await Command("SELECT count(*) FROM wfc_scale.hub_workflow " + where))
            total = (long)(await count.ExecuteScalarAsync(ct) ?? 0L);

        await using var listing = await Command("""
            SELECT id,hub_id,definition_id,definition_version,run_id,state,
                   next_step,step_count,last_error,created_at,completed_at,execution_mode,native_status
            FROM wfc_scale.hub_workflow
            """ + " " + where + " ORDER BY created_at DESC, id DESC LIMIT @take OFFSET @offset");
        listing.Parameters.AddWithValue("take", take);
        listing.Parameters.AddWithValue("offset", (page - 1) * take);
        var records = new List<HubWorkflowListItem>();
        await using var reader = await listing.ExecuteReaderAsync(ct);
        string[] states = ["Ready", "Running", "Complete", "Failed"];
        while (await reader.ReadAsync(ct))
            records.Add(new(reader.GetGuid(0), reader.GetString(1), reader.GetString(2),
                reader.GetInt32(3), reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.GetString(11)=="native" && reader.GetString(12)!="Runnable"?reader.GetString(12):states[reader.GetInt16(5)], reader.GetInt32(6), reader.GetInt32(7),
                reader.IsDBNull(8) ? null : reader.GetString(8),
                reader.GetDateTime(9), reader.IsDBNull(10) ? null : reader.GetDateTime(10)));
        return new(records, total, page, take);
    }

    public async Task<HubWorkflowOverview> OverviewAsync(string? hubId, CancellationToken ct)
    {
        await using var cmd = DataSource.CreateCommand("""
            SELECT count(*), count(*) FILTER (WHERE state=0),
                   count(*) FILTER (WHERE state=1 AND (execution_mode<>'native' OR native_status='Runnable')), count(*) FILTER (WHERE state=2),
                   count(*) FILTER (WHERE state=3 AND (execution_mode<>'native' OR native_status<>'Terminated')),
                   count(*) FILTER (WHERE created_at>=now()-interval '24 hours'),
                   count(*) FILTER (WHERE state=2 AND
                       completed_at>=now()-interval '24 hours'),
                   count(*) FILTER (WHERE execution_mode='native' AND native_status='Suspended'),
                   count(*) FILTER (WHERE execution_mode='native' AND native_status='Terminated')
            FROM wfc_scale.hub_workflow WHERE (@hub IS NULL OR hub_id=@hub)
            """);
        cmd.Parameters.Add("hub", NpgsqlDbType.Text).Value = (object?)hubId ?? DBNull.Value;
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) throw new InvalidOperationException("Overview unavailable");
        return new(reader.GetInt64(0),reader.GetInt64(1),reader.GetInt64(2),
            reader.GetInt64(3),reader.GetInt64(4),reader.GetInt64(5),reader.GetInt64(6),reader.GetInt64(7),reader.GetInt64(8));
    }

    public async Task<IReadOnlyList<string>> HubIdsAsync(CancellationToken ct)
    {
        await using var cmd = DataSource.CreateCommand("""
            SELECT DISTINCT hub_id FROM wfc_scale.hub_workflow ORDER BY hub_id
            """);
        var ids = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) ids.Add(reader.GetString(0));
        return ids;
    }

    /// <summary>
    /// Read-only summary for the Next.js Hub Runs view. Never expose workflow
    /// payloads or tenant IDs; the caller must authenticate at the gateway.
    /// </summary>
    public async Task<IReadOnlyList<HubRecentRun>> RecentRunsAsync(int take, CancellationToken ct)
    {
        if (take is < 1 or > 100)
            throw new ArgumentOutOfRangeException(nameof(take));
        await using var command = DataSource.CreateCommand("""
            SELECT run_id,
                   count(*),
                   count(*) FILTER (WHERE state=0),
                   count(*) FILTER (WHERE state=1),
                   count(*) FILTER (WHERE state=2),
                   count(*) FILTER (WHERE state=3),
                   max(created_at)
            FROM wfc_scale.hub_workflow
            WHERE run_id IS NOT NULL
            GROUP BY run_id
            ORDER BY max(created_at) DESC
            LIMIT @take
            """);
        command.Parameters.AddWithValue("take", take);
        var runs = new List<HubRecentRun>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            runs.Add(new(reader.GetString(0), reader.GetInt64(1),
                reader.GetInt64(2), reader.GetInt64(3), reader.GetInt64(4),
                reader.GetInt64(5), reader.GetDateTime(6)));
        return runs;
    }

    public async Task<HubWorkflowRunSummary> SummaryAsync(string runId, CancellationToken ct)
    {
        await using var cmd = DataSource.CreateCommand("""
            SELECT count(*),
                   count(*) FILTER (WHERE state=0),
                   count(*) FILTER (WHERE state=1),
                   count(*) FILTER (WHERE state=2),
                   count(*) FILTER (WHERE state=3),
                   min(created_at),max(completed_at),
                   percentile_cont(0.50) WITHIN GROUP (
                       ORDER BY extract(epoch FROM (completed_at-created_at))*1000)
                       FILTER (WHERE state=2),
                   percentile_cont(0.95) WITHIN GROUP (
                       ORDER BY extract(epoch FROM (completed_at-created_at))*1000)
                       FILTER (WHERE state=2),
                   percentile_cont(0.99) WITHIN GROUP (
                       ORDER BY extract(epoch FROM (completed_at-created_at))*1000)
                       FILTER (WHERE state=2),
                   max(extract(epoch FROM (completed_at-created_at))*1000)
                       FILTER (WHERE state=2)
            FROM wfc_scale.hub_workflow
            WHERE run_id=@run
            """);
        cmd.Parameters.AddWithValue("run", runId);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) throw new InvalidOperationException("Summary unavailable");
        return new(runId, reader.GetInt64(0),reader.GetInt64(1),
            reader.GetInt64(2),reader.GetInt64(3),reader.GetInt64(4),
            reader.IsDBNull(5) ? null : reader.GetDateTime(5),
            reader.IsDBNull(6) ? null : reader.GetDateTime(6),
            reader.IsDBNull(7) ? null : reader.GetDouble(7),
            reader.IsDBNull(8) ? null : reader.GetDouble(8),
            reader.IsDBNull(9) ? null : reader.GetDouble(9),
            reader.IsDBNull(10) ? null : (double)reader.GetDecimal(10));
    }

    // Lock task and its workflow in the same transaction. The order prevents
    // concurrent completion/failure from changing the same pointer twice.
    private static async Task<ManagedTask?> LockManagedTaskAsync(
        NpgsqlConnection db, NpgsqlTransaction tx, Guid id, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("""
            SELECT t.workflow_id,t.workflow_step,t.state,t.owner,t.epoch,
                   t.lease_until,t.attempts,t.max_attempts,
                   w.state,w.next_step,t.hub_id,t.task_type,w.created_at,w.step_count
            FROM wfc_scale.hub_task t JOIN wfc_scale.hub_workflow w
              ON w.id=t.workflow_id
            WHERE t.id=@id AND t.workflow_step IS NOT NULL
            FOR UPDATE OF t,w
            """, db, tx);
        cmd.Parameters.AddWithValue("id", id);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return null;
        return new(reader.GetGuid(0),reader.GetInt32(1),reader.GetInt16(2),
            reader.IsDBNull(3) ? null : reader.GetString(3),reader.GetInt64(4),
            reader.IsDBNull(5) ? DateTime.MinValue : reader.GetDateTime(5),
            reader.GetInt32(6),reader.GetInt32(7),reader.GetInt16(8),
            reader.GetInt32(9),reader.GetString(10),reader.GetString(11),
            reader.GetDateTime(12),reader.GetInt32(13));
    }

    private sealed record ManagedTask(Guid WorkflowId, int Step, short State,
        string? Owner, long Epoch, DateTime LeaseUntil, int Attempts,
        int MaxAttempts, short WorkflowState, int NextStep,
        string HubId, string ActivityType, DateTime CreatedAt, int StepCount);
}

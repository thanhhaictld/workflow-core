using System.Text.Json;
using System.Security.Cryptography;
using System.Text;
using System.Diagnostics;
using Npgsql;
using NpgsqlTypes;

namespace WorkflowCore.Hub;

public sealed record HubCapability(string HubId, string TaskType, int Version);
public sealed record HubWorkflowCapability(string HubId, string DefinitionId, int Version);
public sealed record HubLeasedTask(
    Guid Id, string HubId, int PartitionId, string TaskType, int Version,
    Guid? WorkflowId, JsonElement Payload, long Epoch, int Attempts,
    DateTime LeaseUntil);
public sealed record HubTaskStatus(Guid Id, string HubId, int PartitionId,
    string TaskType, int Version, string State, int Attempts, string? Error,
    JsonElement? Result);
public sealed record HubClaimRequest(string WorkerId, string[] Hubs,
    HubCapability[] Capabilities, int MaxTasks = 8, HubWorkflowCapability[]? Workflows = null);
public sealed record HubEnqueueRequest(string HubId, string TaskType, int Version,
    JsonElement Payload, Guid? WorkflowId = null, string? IdempotencyKey = null);
public sealed record HubTaskResultRequest(string WorkerId, long Epoch,
    JsonElement Result);
public sealed record HubTaskHeartbeatRequest(string WorkerId, long Epoch);
public sealed record HubTaskFailureRequest(string WorkerId, long Epoch,
    string Error, int RetryDelaySeconds = 2);
public sealed record HubDatabaseDiagnostics(long DatabaseConnections, long ActiveDbConnections,
    long ReadyTasks, long LeasedTasks, long DeadTasks);

/// <summary>
/// PostgreSQL-authoritative Hub task queue. Resources are locked in stable
/// order before admission; task leases are durable concurrency permits.
/// Neither SSE nor a client-side semaphore conveys task ownership.
/// </summary>
public sealed class HubTaskStore : IAsyncDisposable
{
    private readonly NpgsqlDataSource _dataSource;
    internal NpgsqlDataSource DataSource => _dataSource;

    public HubTaskStore(string connectionString, int maxPoolSize = 12)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString)
        {
            MaxPoolSize = Math.Clamp(maxPoolSize, 1, 100)
        };
        _dataSource = NpgsqlDataSource.Create(builder.ConnectionString);
    }

    public async Task<Guid> EnqueueAsync(HubTaskRoute route, string payload,
        Guid? workflowId, string? requestKey, CancellationToken ct)
    {
        var id = Guid.NewGuid();
        var partition = HubPartitionRouter.ForWorkflow(workflowId ?? id, route.PartitionCount);
        await using var db = await _dataSource.OpenConnectionAsync(ct);
        await using var tx = await db.BeginTransactionAsync(ct);

        // Serialize against operator policy changes. Re-resolve declarative routes
        // inside the transaction after acquiring the registration lock.
        if (route.Resources.Any(r => r.Key.StartsWith("hub:" + route.HubId + ":", StringComparison.Ordinal) &&
                                     r.Key.EndsWith(":" + HubPolicyDigest(route.HubId), StringComparison.Ordinal)))
        {
            await using var lockRegistration=new NpgsqlCommand("""
                SELECT 1 FROM wfc_scale.hub_activity_registration
                WHERE hub_id=@hub AND activity_type=@type AND version=@version
                FOR SHARE
                """,db,tx);
            lockRegistration.Parameters.AddWithValue("hub",route.HubId);
            lockRegistration.Parameters.AddWithValue("type",route.TaskType);
            lockRegistration.Parameters.AddWithValue("version",route.Version);
            if(await lockRegistration.ExecuteScalarAsync(ct) is not null)
            {
                await using var checkPolicy=new NpgsqlCommand("""
                    SELECT hub_concurrency,activity_concurrency,resource_capacity
                    FROM wfc_scale.hub_activity_registration
                    WHERE hub_id=@hub AND activity_type=@type AND version=@version
                    """,db,tx);
                checkPolicy.Parameters.AddWithValue("hub",route.HubId);
                checkPolicy.Parameters.AddWithValue("type",route.TaskType);
                checkPolicy.Parameters.AddWithValue("version",route.Version);
                await using var reader=await checkPolicy.ExecuteReaderAsync(ct);
                if(await reader.ReadAsync(ct))
                {
                    var hubCapacity=reader.GetInt32(0);
                    var activityCapacity=reader.GetInt32(1);
                    var resourceCapacity=reader.GetInt32(2);
                    if(route.Resources.Any(r=>r.Key.StartsWith("hub:",StringComparison.Ordinal) && r.Capacity!=hubCapacity
                        || r.Key.StartsWith("activity:",StringComparison.Ordinal) && r.Capacity!=activityCapacity
                        || r.Key.StartsWith("resource:",StringComparison.Ordinal) && r.Capacity!=resourceCapacity))
                        throw new InvalidOperationException("Activity policy changed while enqueue was preparing; retry with the current policy");
                }
            }
        }

        // Also serialize enqueues for the same FIFO resource. Thus sequence
        // order is consistent with commit order for each ordering key.
        foreach (var resource in route.Resources)
        {
            await using (var upsert = new NpgsqlCommand("""
                INSERT INTO wfc_scale.hub_resource(resource_key, capacity, strict_fifo)
                VALUES (@key, @capacity, @fifo)
                ON CONFLICT (resource_key) DO NOTHING
                """, db, tx))
            {
                upsert.Parameters.AddWithValue("key", resource.Key);
                upsert.Parameters.AddWithValue("capacity", resource.Capacity);
                upsert.Parameters.AddWithValue("fifo", resource.StrictFifo);
                await upsert.ExecuteNonQueryAsync(ct);
            }
            await using var check = new NpgsqlCommand("""
                SELECT capacity, strict_fifo FROM wfc_scale.hub_resource
                WHERE resource_key = @key FOR UPDATE
                """, db, tx);
            check.Parameters.AddWithValue("key", resource.Key);
            await using var reader = await check.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct) ||
                reader.GetInt32(0) != resource.Capacity ||
                reader.GetBoolean(1) != resource.StrictFifo)
                throw new InvalidOperationException(
                    "Code-first concurrency policy drift for resource: " + resource.Key);
        }

        await using (var insert = new NpgsqlCommand("""
            INSERT INTO wfc_scale.hub_task
                (id,hub_id,partition_id,task_type,task_version,workflow_id,payload,request_key)
            VALUES (@id,@hub,@partition,@type,@version,@workflow,@payload,@request)
            ON CONFLICT DO NOTHING RETURNING id
            """, db, tx))
        {
            insert.Parameters.AddWithValue("id", id);
            insert.Parameters.AddWithValue("hub", route.HubId);
            insert.Parameters.AddWithValue("partition", partition);
            insert.Parameters.AddWithValue("type", route.TaskType);
            insert.Parameters.AddWithValue("version", route.Version);
            insert.Parameters.Add("workflow", NpgsqlDbType.Uuid).Value =
                (object?)workflowId ?? DBNull.Value;
            insert.Parameters.Add("payload", NpgsqlDbType.Jsonb).Value = payload;
            insert.Parameters.Add("request", NpgsqlDbType.Text).Value =
                (object?)requestKey ?? DBNull.Value;
            var inserted = await insert.ExecuteScalarAsync(ct);
            if (inserted is null)
            {
                if (requestKey is null)
                    throw new InvalidOperationException("Unexpected Hub task ID collision");
                await using var existing = new NpgsqlCommand("""
                    SELECT id FROM wfc_scale.hub_task
                    WHERE hub_id = @hub AND request_key = @request
                      AND task_type = @type AND task_version = @version
                      AND payload = @payload AND workflow_id IS NOT DISTINCT FROM @workflow
                    """, db, tx);
                existing.Parameters.AddWithValue("hub", route.HubId);
                existing.Parameters.AddWithValue("request", requestKey);
                existing.Parameters.AddWithValue("type", route.TaskType);
                existing.Parameters.AddWithValue("version", route.Version);
                existing.Parameters.Add("payload", NpgsqlDbType.Jsonb).Value = payload;
                existing.Parameters.Add("workflow", NpgsqlDbType.Uuid).Value =
                    (object?)workflowId ?? DBNull.Value;
                var previous = await existing.ExecuteScalarAsync(ct);
                if (previous is null)
                    throw new InvalidOperationException(
                        "Idempotency key reused for different Hub task");
                await tx.CommitAsync(ct);
                return (Guid)previous;
            }
        }
        foreach (var resource in route.Resources)
        {
            await using var constraint = new NpgsqlCommand("""
                INSERT INTO wfc_scale.hub_task_constraint(task_id,resource_key)
                VALUES (@task,@resource)
                """, db, tx);
            constraint.Parameters.AddWithValue("task", id);
            constraint.Parameters.AddWithValue("resource", resource.Key);
            await constraint.ExecuteNonQueryAsync(ct);
        }
        await tx.CommitAsync(ct);
        return id;
    }

    public async Task<IReadOnlyList<HubLeasedTask>> ClaimAsync(
        string workerId, IReadOnlyList<string> hubs, IReadOnlyList<HubCapability> capabilities,
        int maxTasks, TimeSpan lease, CancellationToken ct, IReadOnlyList<HubWorkflowCapability>? workflows = null)
    {
        using var span = HubTelemetry.Traces.StartActivity("hub.task.claim");
        var claimStarted = Stopwatch.GetTimestamp();
        if (string.IsNullOrWhiteSpace(workerId) || workerId.Length > 128 ||
            hubs.Count is < 1 or > 32 || capabilities.Count is < 1 or > 128 ||
            maxTasks is < 1 or > 32 || workflows?.Count>128 || lease < TimeSpan.FromSeconds(10) ||
            lease > TimeSpan.FromMinutes(5))
            throw new ArgumentException("Invalid worker claim parameters");

        var supported = capabilities.Select(c => $"{c.HubId}|{c.TaskType}|{c.Version}").ToArray();
        var pins=workflows is null?null:JsonSerializer.Serialize(workflows.Select(w=>new{hub=w.HubId,definition=w.DefinitionId,version=w.Version}));
        var results = new List<HubLeasedTask>(maxTasks);
        await using var db = await _dataSource.OpenConnectionAsync(ct);
        for (var batch = 0; batch < maxTasks; batch++)
        {
            var candidates = new List<Guid>();
            await using (var candidateQuery = new NpgsqlCommand("""
                SELECT id FROM wfc_scale.hub_task
                WHERE hub_id = ANY(@hubs)
                  AND NOT EXISTS (SELECT 1 FROM wfc_scale.hub_workflow w
                      WHERE w.id=workflow_id AND w.execution_mode='native' AND w.native_status<>'Runnable')
                  AND hub_id || '|' || task_type || '|' || task_version::text = ANY(@types)
                  AND (task_type NOT IN ('__workflow__','__native_workflow__') OR @pins IS NULL OR EXISTS(
                    SELECT 1 FROM wfc_scale.hub_workflow w,
                      jsonb_to_recordset(@pins) AS p(hub text,definition text,version integer)
                    WHERE w.id=workflow_id AND w.hub_id=p.hub
                      AND w.definition_id=p.definition AND w.definition_version=p.version))
                  AND attempts < max_attempts
                  AND ((state = 0 AND due_at <= now()) OR
                       (state = 1 AND lease_until < now()))
                ORDER BY sequence LIMIT 96
                """, db))
            {
                candidateQuery.Parameters.AddWithValue("hubs", hubs.ToArray());
                candidateQuery.Parameters.AddWithValue("types", supported);
                candidateQuery.Parameters.Add("pins",NpgsqlDbType.Jsonb).Value=(object?)pins??DBNull.Value;
                await using var reader = await candidateQuery.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct)) candidates.Add(reader.GetGuid(0));
            }

            HubLeasedTask? found = null;
            foreach (var taskId in candidates)
            {
                await using var tx = await db.BeginTransactionAsync(ct);
                Guid id; string hub; string taskType; int version, partition, attempts;
                int? workflowStep;
                long sequence; Guid? workflow; string payload;
                await using (var row = new NpgsqlCommand("""
                    SELECT id,hub_id,partition_id,task_type,task_version,workflow_id,
                           payload::text,sequence,attempts,COALESCE(workflow_step,native_step_id)
                    FROM wfc_scale.hub_task WHERE id = @id
                      AND (task_type NOT IN ('__workflow__','__native_workflow__') OR @pins IS NULL OR EXISTS(
                        SELECT 1 FROM wfc_scale.hub_workflow w,
                          jsonb_to_recordset(@pins) AS p(hub text,definition text,version integer)
                        WHERE w.id=workflow_id AND w.hub_id=p.hub
                          AND w.definition_id=p.definition AND w.definition_version=p.version))
                      AND NOT EXISTS (SELECT 1 FROM wfc_scale.hub_workflow w
                          WHERE w.id=workflow_id AND w.execution_mode='native' AND w.native_status<>'Runnable')
                      AND attempts < max_attempts
                      AND ((state = 0 AND due_at <= now()) OR
                           (state = 1 AND lease_until < now()))
                    FOR UPDATE SKIP LOCKED
                    """, db, tx))
                {
                    row.Parameters.AddWithValue("id", taskId);
                    row.Parameters.Add("pins",NpgsqlDbType.Jsonb).Value=(object?)pins??DBNull.Value;
                    await using var reader = await row.ExecuteReaderAsync(ct);
                    if (!await reader.ReadAsync(ct)) continue;
                    id = reader.GetGuid(0);
                    hub = reader.GetString(1);
                    partition = reader.GetInt32(2);
                    taskType = reader.GetString(3);
                    version = reader.GetInt32(4);
                    workflow = reader.IsDBNull(5) ? null : reader.GetGuid(5);
                    payload = reader.GetString(6);
                    sequence = reader.GetInt64(7);
                    attempts = reader.GetInt32(8);
                    workflowStep = reader.IsDBNull(9) ? null : reader.GetInt32(9);
                }

                var constraints = new List<(string Key, int Limit, bool Fifo)>();
                await using (var query = new NpgsqlCommand("""
                    SELECT c.resource_key, r.capacity, r.strict_fifo
                    FROM wfc_scale.hub_task_constraint c
                    JOIN wfc_scale.hub_resource r ON r.resource_key = c.resource_key
                    WHERE c.task_id = @id ORDER BY c.resource_key
                    """, db, tx))
                {
                    query.Parameters.AddWithValue("id", id);
                    await using var reader = await query.ExecuteReaderAsync(ct);
                    while (await reader.ReadAsync(ct))
                        constraints.Add((reader.GetString(0), reader.GetInt32(1),
                            reader.GetBoolean(2)));
                }

                // Canonical resource row lock ordering prevents deadlock when
                // tasks require multiple independent concurrency permits.
                var eligible = true;
                foreach (var constraint in constraints)
                {
                    await using (var resourceLock = new NpgsqlCommand("""
                        SELECT 1 FROM wfc_scale.hub_resource
                        WHERE resource_key = @key FOR UPDATE
                        """, db, tx))
                    {
                        resourceLock.Parameters.AddWithValue("key", constraint.Key);
                        await resourceLock.ExecuteScalarAsync(ct);
                    }

                    await using var check = new NpgsqlCommand("""
                        SELECT count(*) FILTER (WHERE t.state = 1 AND t.lease_until > now()),
                               count(*) FILTER (WHERE t.sequence < @seq AND t.state IN (0,1))
                        FROM wfc_scale.hub_task_constraint c
                        JOIN wfc_scale.hub_task t ON t.id = c.task_id
                        WHERE c.resource_key = @key
                        """, db, tx);
                    check.Parameters.AddWithValue("key", constraint.Key);
                    check.Parameters.AddWithValue("seq", sequence);
                    await using var reader = await check.ExecuteReaderAsync(ct);
                    if (!await reader.ReadAsync(ct)) throw new InvalidOperationException("Resource missing");
                    var active = reader.GetInt64(0);
                    var earlier = reader.GetInt64(1);
                    if (active >= constraint.Limit || (constraint.Fifo && earlier > 0))
                    {
                        eligible = false;
                        break;
                    }
                }
                if (!eligible) continue;

                await using var claim = new NpgsqlCommand("""
                    UPDATE wfc_scale.hub_task
                    SET state = 1, owner = @owner, last_owner = @owner,
                        epoch = epoch + 1, attempts = attempts + 1,
                        lease_until = now() + @lease, updated_at = now()
                    WHERE id = @id
                    RETURNING epoch, lease_until
                    """, db, tx);
                claim.Parameters.AddWithValue("id", id);
                claim.Parameters.AddWithValue("owner", workerId);
                claim.Parameters.Add("lease", NpgsqlDbType.Interval).Value = lease;
                await using (var reader = await claim.ExecuteReaderAsync(ct))
                {
                    if (!await reader.ReadAsync(ct))
                        throw new InvalidOperationException("Locked task disappeared");
                    var epoch = reader.GetInt64(0);
                    var until = reader.GetDateTime(1);
                    found = new(id, hub, partition, taskType, version, workflow,
                        JsonDocument.Parse(payload).RootElement.Clone(), epoch, attempts + 1, until);
                }
                // A lease is a durable fencing epoch. Keep one actual claim
                // timestamp per managed workflow activity attempt, including
                // retries and lease takeovers. This is NOT worker CPU time.
                if (workflowStep is not null && workflow is not null && found is not null)
                {
                    await using (var replaced = new NpgsqlCommand("""
                        UPDATE wfc_scale.hub_task_attempt
                        SET finished_at=clock_timestamp(), outcome='LeaseExpired'
                        WHERE task_id=@id AND finished_at IS NULL
                        """, db, tx))
                    {
                        replaced.Parameters.AddWithValue("id", id);
                        await replaced.ExecuteNonQueryAsync(ct);
                    }
                    await using var attempt = new NpgsqlCommand("""
                        INSERT INTO wfc_scale.hub_task_attempt
                            (task_id,epoch,workflow_id,step_index,attempt_number,worker_id)
                        VALUES (@id,@epoch,@workflow,@step,@number,@worker)
                        """, db, tx);
                    attempt.Parameters.AddWithValue("id", id);
                    attempt.Parameters.AddWithValue("epoch", found.Epoch);
                    attempt.Parameters.AddWithValue("workflow", workflow.Value);
                    attempt.Parameters.AddWithValue("step", workflowStep.Value);
                    attempt.Parameters.AddWithValue("number", attempts + 1);
                    attempt.Parameters.AddWithValue("worker", workerId);
                    await attempt.ExecuteNonQueryAsync(ct);
                }
                if (taskType is "__workflow__" or "__native_workflow__" && workflow is Guid processorWorkflow &&
                    found is not null)
                {
                    await using var log = new NpgsqlCommand("""
                        INSERT INTO wfc_scale.hub_workflow_event
                        (workflow_id,step_index,task_id,activity_type,event_type,payload)
                        SELECT w.id,w.next_step,@task,@type,
                               'WorkflowTaskClaimed',
                               jsonb_build_object('worker',@worker,'epoch',@epoch)
                        FROM wfc_scale.hub_workflow w WHERE w.id=@workflow
                        """, db, tx);
                    log.Parameters.AddWithValue("task", id);
                    log.Parameters.AddWithValue("type", taskType);
                    log.Parameters.AddWithValue("workflow", processorWorkflow);
                    log.Parameters.AddWithValue("worker", workerId);
                    log.Parameters.AddWithValue("epoch", found.Epoch);
                    await log.ExecuteNonQueryAsync(ct);
                }
                await tx.CommitAsync(ct);
                break;
            }

            if (found is null) break;
            results.Add(found);
            HubTelemetry.TasksClaimed.Add(1, HubTelemetry.HubTag(found.HubId),
                HubTelemetry.ActivityTag(found.TaskType));
        }
        HubTelemetry.ClaimDuration.Record(Stopwatch.GetElapsedTime(claimStarted).TotalSeconds);
        span?.SetTag("tasks.claimed", results.Count);
        return results;
    }

    public async Task<bool> IsManagedWorkflowStepAsync(Guid id, CancellationToken ct)
    {
        await using var cmd = _dataSource.CreateCommand("""
            SELECT workflow_step IS NOT NULL FROM wfc_scale.hub_task WHERE id=@id
            """);
        cmd.Parameters.AddWithValue("id", id);
        return await cmd.ExecuteScalarAsync(ct) is true;
    }

    public async Task<bool> HeartbeatAsync(Guid id, string worker, long epoch,
        TimeSpan lease, CancellationToken ct)
    {
        if (lease <= TimeSpan.Zero || lease > TimeSpan.FromMinutes(5)) return false;
        await using var command = _dataSource.CreateCommand("""
            UPDATE wfc_scale.hub_task SET lease_until = now() + @lease, updated_at=now()
            WHERE id=@id AND owner=@owner AND epoch=@epoch
              AND state=1 AND lease_until>now()
            """);
        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("owner", worker);
        command.Parameters.AddWithValue("epoch", epoch);
        command.Parameters.Add("lease", NpgsqlDbType.Interval).Value = lease;
        return await command.ExecuteNonQueryAsync(ct) == 1;
    }

    public async Task<bool> CompleteAsync(Guid id, string worker, long epoch,
        string resultJson, CancellationToken ct)
    {
        await using var cmd = _dataSource.CreateCommand("""
            UPDATE wfc_scale.hub_task
            SET state=2, owner=NULL, lease_until=NULL, result_json=@result, updated_at=now()
            WHERE id=@id AND owner=@owner AND epoch=@epoch
              AND state=1 AND lease_until>now()
            """);
        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("owner", worker);
        cmd.Parameters.AddWithValue("epoch", epoch);
        cmd.Parameters.Add("result", NpgsqlDbType.Jsonb).Value = resultJson;
        if (await cmd.ExecuteNonQueryAsync(ct) == 1) return true;

        // Client may retry after successful commit but a lost HTTP response.
        await using var check = _dataSource.CreateCommand("""
            SELECT 1 FROM wfc_scale.hub_task
            WHERE id=@id AND state=2 AND last_owner=@owner AND epoch=@epoch
              AND result_json=@result
            """);
        check.Parameters.AddWithValue("id", id);
        check.Parameters.AddWithValue("owner", worker);
        check.Parameters.AddWithValue("epoch", epoch);
        check.Parameters.Add("result", NpgsqlDbType.Jsonb).Value = resultJson;
        return await check.ExecuteScalarAsync(ct) is not null;
    }

    public async Task<bool> FailAsync(Guid id, string worker, long epoch,
        string error, TimeSpan retryDelay, CancellationToken ct)
    {
        if (retryDelay < TimeSpan.Zero || retryDelay > TimeSpan.FromHours(1))
            return false;
        await using var cmd = _dataSource.CreateCommand("""
            WITH failed AS (UPDATE wfc_scale.hub_task
            SET state=CASE WHEN attempts >= max_attempts THEN 3 ELSE 0 END,
                owner=NULL, lease_until=NULL, due_at=now()+@delay,
                last_error=@error, updated_at=now()
            WHERE id=@id AND state=1 AND owner=@owner AND epoch=@epoch
              AND lease_until>now() RETURNING id,state),
            closed AS (
              UPDATE wfc_scale.hub_task_attempt a SET finished_at=clock_timestamp(),
                outcome=CASE WHEN f.state=3 THEN 'Failed' ELSE 'RetryScheduled' END
              FROM failed f WHERE a.task_id=f.id AND a.epoch=@epoch AND a.finished_at IS NULL
              RETURNING a.task_id
            ) SELECT count(*) FROM failed
            """);
        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("owner", worker);
        cmd.Parameters.AddWithValue("epoch", epoch);
        cmd.Parameters.AddWithValue("error", error.Length > 1000 ? error[..1000] : error);
        cmd.Parameters.Add("delay", NpgsqlDbType.Interval).Value = retryDelay;
        return Convert.ToInt32(await cmd.ExecuteScalarAsync(ct)) == 1;
    }

    public async Task<HubTaskStatus?> GetAsync(Guid id, CancellationToken ct)
    {
        await using var cmd = _dataSource.CreateCommand("""
            SELECT id, hub_id, partition_id, task_type, task_version,
                   state, attempts, last_error, result_json::text
            FROM wfc_scale.hub_task WHERE id=@id
            """);
        cmd.Parameters.AddWithValue("id", id);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return null;
        var states = new[] { "Ready", "Leased", "Complete", "Dead" };
        return new(reader.GetGuid(0), reader.GetString(1), reader.GetInt32(2),
            reader.GetString(3), reader.GetInt32(4), states[reader.GetInt16(5)],
            reader.GetInt32(6), reader.IsDBNull(7) ? null : reader.GetString(7),
            reader.IsDBNull(8) ? null : JsonDocument.Parse(reader.GetString(8)).RootElement.Clone());
    }

    public async Task<string[]> ReadyHubsAsync(CancellationToken ct)
    {
        await using var cmd = _dataSource.CreateCommand("""
            SELECT DISTINCT hub_id FROM wfc_scale.hub_task
            WHERE (state=0 AND due_at<=now()) OR
                  (state=1 AND lease_until<now() AND attempts<max_attempts)
            """);
        var hubs = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) hubs.Add(reader.GetString(0));
        return hubs.ToArray();
    }

    public async Task<int> DeadLetterExpiredAsync(CancellationToken ct)
    {
        await using var cmd = _dataSource.CreateCommand("""
            WITH expired AS (UPDATE wfc_scale.hub_task SET state=3, owner=NULL, lease_until=NULL,
                updated_at=now(), last_error=coalesce(last_error,'Lease expired after max attempts')
            WHERE state=1 AND lease_until<now() AND attempts>=max_attempts RETURNING id),
            closed AS (
              UPDATE wfc_scale.hub_task_attempt a SET finished_at=clock_timestamp(),outcome='LeaseExpired'
              FROM expired e WHERE a.task_id=e.id AND a.finished_at IS NULL RETURNING a.task_id
            ) SELECT count(*) FROM expired
            """);
        return Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));
    }

    public async Task<HubDatabaseDiagnostics> DiagnosticsAsync(CancellationToken ct)
    {
        await using var command = _dataSource.CreateCommand("""
            SELECT (SELECT count(*) FROM pg_stat_activity
                    WHERE datname=current_database()),
                   (SELECT count(*) FROM pg_stat_activity
                    WHERE datname=current_database() AND state='active'),
                   (SELECT count(*) FROM wfc_scale.hub_task WHERE state=0 AND due_at<=now()),
                   (SELECT count(*) FROM wfc_scale.hub_task WHERE state=1 AND lease_until>now()),
                   (SELECT count(*) FROM wfc_scale.hub_task WHERE state=3)
            """);
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) throw new InvalidOperationException("Database metrics unavailable");
        return new(reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2),
            reader.GetInt64(3), reader.GetInt64(4));
    }

    public async Task PingAsync(CancellationToken ct)
    {
        await using var cmd = _dataSource.CreateCommand("SELECT 1");
        await cmd.ExecuteScalarAsync(ct);
    }

    private static string HubPolicyDigest(string value) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();

    public ValueTask DisposeAsync() => _dataSource.DisposeAsync();
}

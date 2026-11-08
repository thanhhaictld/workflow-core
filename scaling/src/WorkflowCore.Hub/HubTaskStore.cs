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

// Completion routing is immutable task metadata. Resolve once instead of making
// three independent classification queries and reloading task metadata after ACK.
public enum HubCompletionKind { Standalone, Managed, Remote, Native }
public sealed record HubCompletionRoute(string HubId, HubCompletionKind Kind);
public sealed record HubClaimRequest(string WorkerId, string[] Hubs,
    HubCapability[] Capabilities, int MaxTasks = 8, HubWorkflowCapability[]? Workflows = null,
    int? MatchingPartition = null, int? MatchingPartitionCount = null);
public sealed record HubEnqueueRequest(string HubId, string TaskType, int Version,
    JsonElement Payload, Guid? WorkflowId = null, string? IdempotencyKey = null);
public sealed record HubTaskResultRequest(string WorkerId, long Epoch,
    JsonElement Result);
public sealed record HubTaskHeartbeatRequest(string WorkerId, long Epoch);
public sealed record HubTaskFailureRequest(string WorkerId, long Epoch,
    string Error, int RetryDelaySeconds = 2);

/// <summary>
/// Transient backpressure signal for demand-matched claims only. The matching
/// dispatcher must retain the readiness hint and retry; no task is leased.
/// Never expose this as a permanent empty-queue result.
/// </summary>
public sealed class HubClaimAdmissionDeferredException : Exception
{
    public HubClaimAdmissionDeferredException() : base("Matching claim deferred while PostgreSQL claim capacity is occupied") { }
}
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
    // Admission control reserves connections for completion, timers and writes.
    // Unlike a global workflow lock, this only limits speculative claim SQL.
    private readonly SemaphoreSlim _claimGate;
    private readonly bool _claimAdmissionEnabled;
    // Demand-aware mode leaves only a small number of claims waiting behind
    // active SQL. Extra event-matched claims stay in memory, preserving hints.
    // This never throttles completion/heartbeat/failure database operations.
    private readonly bool _demandAwareAdmissionEnabled;
    private readonly SemaphoreSlim _claimWaitSlots;
    // Opt-in: atomically lease batches of tasks without any resource constraints.
    // Constrained tasks retain the original task -> resource row lock path.
    private readonly bool _unconstrainedBatchClaimsEnabled;
    // Optional SQL prefilter keeps blocked strict-FIFO successors out of the
    // bounded claim candidate window. Admission is still rechecked under locks.
    private readonly bool _fifoHeadPrefilterEnabled;
    internal NpgsqlDataSource DataSource => _dataSource;

    public HubTaskStore(string connectionString, int maxPoolSize = 12,
        bool? enableUnconstrainedBatchClaims = null,
        bool? enableFifoHeadPrefilter = null)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString)
        {
            MaxPoolSize = Math.Clamp(maxPoolSize, 1, 100)
        };
        _dataSource = NpgsqlDataSource.Create(builder.ConnectionString);
        var admissionMode = Environment.GetEnvironmentVariable("WFC_HUB_CLAIM_ADMISSION_MODE");
        _demandAwareAdmissionEnabled = string.Equals(admissionMode, "demand",
            StringComparison.OrdinalIgnoreCase);
        if (!string.IsNullOrWhiteSpace(admissionMode) &&
            admissionMode is not ("queue" or "QUEUE" or "Queue") &&
            !_demandAwareAdmissionEnabled)
            throw new ArgumentException("WFC_HUB_CLAIM_ADMISSION_MODE must be queue or demand");

        // A shared data source is used for claims and completions. Reserve at
        // least one, normally four, pooled connections for non-claim traffic.
        var effectivePoolSize = Math.Clamp(maxPoolSize, 1, 100);
        var reservedConnections = Math.Min(4, Math.Max(1, effectivePoolSize - 1));
        var maximumClaimConnections = Math.Max(1, effectivePoolSize - reservedConnections);
        var configuredLimit = int.TryParse(
            Environment.GetEnvironmentVariable("WFC_HUB_CLAIM_ADMISSION_LIMIT"),
            out var claimLimit) ? claimLimit : Math.Max(1, effectivePoolSize / 4);
        if (configuredLimit < 1)
            throw new ArgumentException("WFC_HUB_CLAIM_ADMISSION_LIMIT must be positive");
        _claimGate = new SemaphoreSlim(Math.Clamp(configuredLimit, 1, maximumClaimConnections));
        var maxQueued = int.TryParse(
            Environment.GetEnvironmentVariable("WFC_HUB_CLAIM_ADMISSION_MAX_WAITERS"),
            out var maxWaiting) ? maxWaiting : 1;
        if (maxQueued is < 0 or > 128)
            throw new ArgumentException("WFC_HUB_CLAIM_ADMISSION_MAX_WAITERS must be 0..128");
        _claimWaitSlots = new SemaphoreSlim(maxQueued, Math.Max(1, maxQueued));
        _claimAdmissionEnabled = _demandAwareAdmissionEnabled ||
            Environment.GetEnvironmentVariable("WFC_HUB_CLAIM_ADMISSION_ENABLED") is "true" or "1" or "True";
        _unconstrainedBatchClaimsEnabled = enableUnconstrainedBatchClaims ??
            (Environment.GetEnvironmentVariable("WFC_HUB_BATCH_UNCONSTRAINED_CLAIMS") is "true" or "1" or "True");
        _fifoHeadPrefilterEnabled = enableFifoHeadPrefilter ??
            (Environment.GetEnvironmentVariable("WFC_HUB_FIFO_HEAD_PREFILTER_ENABLED") is "true" or "1" or "True");
    }

    public async Task<Guid> EnqueueAsync(HubTaskRoute route, string payload,
        Guid? workflowId, string? requestKey, CancellationToken ct)
    {
        var id = Guid.NewGuid();
        var partition = HubPartitionRouter.ForWorkflow(workflowId ?? id, route.PartitionCount);
        var constraints = route.ResourcesForPartition(partition);
        await using var db = await _dataSource.OpenConnectionAsync(ct);
        await using var tx = await db.BeginTransactionAsync(ct);

        // Serialize against operator policy changes. Re-resolve declarative routes
        // inside the transaction after acquiring the registration lock.
        if (constraints.Any(r => r.Key.StartsWith("hub:" + route.HubId + ":", StringComparison.Ordinal) &&
                                     r.Key.EndsWith(":" + HubPolicyDigest(route.HubId), StringComparison.Ordinal)))
        {
            await HubDefinitionStore.LockPolicyHub(db,tx,route.HubId,ct,shared:true);
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
                    if(constraints.Any(r=>r.Key.StartsWith("hub:",StringComparison.Ordinal) && r.Capacity!=hubCapacity
                        || r.Key.StartsWith("activity:",StringComparison.Ordinal) && r.Capacity!=activityCapacity
                        || r.Key.StartsWith("resource:",StringComparison.Ordinal) && r.Capacity!=resourceCapacity))
                        throw new InvalidOperationException("Activity policy changed while enqueue was preparing; retry with the current policy");
                }
            }
        }

        // Also serialize enqueues for the same FIFO resource. Thus sequence
        // order is consistent with commit order for each ordering key.
        foreach (var resource in constraints)
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
        foreach (var resource in constraints)
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
        int maxTasks, TimeSpan lease, CancellationToken ct, IReadOnlyList<HubWorkflowCapability>? workflows = null,
        int? matchingPartition = null, int? matchingPartitionCount = null,
        bool deferWhenBusy = false)
    {
        using var span = HubTelemetry.Traces.StartActivity("hub.task.claim");
        var claimStarted = Stopwatch.GetTimestamp();
        if (string.IsNullOrWhiteSpace(workerId) || workerId.Length > 128 ||
            hubs.Count is < 1 or > 32 || capabilities.Count is < 1 or > 128 ||
            maxTasks is < 1 or > 32 || workflows?.Count>128 || lease < TimeSpan.FromSeconds(10) ||
            lease > TimeSpan.FromMinutes(5) ||
            (matchingPartition.HasValue != matchingPartitionCount.HasValue) ||
            (matchingPartitionCount.HasValue &&
                (matchingPartitionCount.Value is < 1 or > 64 ||
                 matchingPartition!.Value < 0 ||
                 matchingPartition.Value >= matchingPartitionCount.Value)))
            throw new ArgumentException("Invalid worker claim parameters");

        var matchPartition = matchingPartition ?? 0;
        var matchCount = matchingPartitionCount ?? 1;
        span?.SetTag("matching.partition", matchingPartition);
        span?.SetTag("matching.partition_count", matchCount);
        span?.SetTag("claim.fifo_head_prefilter", _fifoHeadPrefilterEnabled);

        var gateAcquired = false;
        var queuedSlotAcquired = false;
        try
        {
            if (_claimAdmissionEnabled)
            {
                var waitStarted = Stopwatch.GetTimestamp();
                using (var admission = HubTelemetry.Traces.StartActivity("hub.task.claim.admission_wait"))
                {
                    admission?.SetTag("matching.partition", matchPartition);
                    admission?.SetTag("matching.partition_count", matchCount);
                    if (_demandAwareAdmissionEnabled && deferWhenBusy)
                    {
                        if (_claimGate.Wait(0))
                            gateAcquired = true;
                        else if (_claimWaitSlots.Wait(0))
                        {
                            queuedSlotAcquired = true;
                            HubTelemetry.ClaimAdmissionQueued.Add(1);
                            try
                            {
                                await _claimGate.WaitAsync(ct);
                                gateAcquired = true;
                            }
                            finally
                            {
                                HubTelemetry.ClaimAdmissionQueued.Add(-1);
                                _claimWaitSlots.Release();
                                queuedSlotAcquired = false;
                            }
                        }
                        else
                        {
                            HubTelemetry.ClaimAdmissionDeferred.Add(1);
                            admission?.SetTag("admission.deferred", true);
                            throw new HubClaimAdmissionDeferredException();
                        }
                    }
                    else
                    {
                        await _claimGate.WaitAsync(ct);
                        gateAcquired = true;
                    }
                    HubTelemetry.ClaimAdmissionActive.Add(1);
                    var waitSeconds = Stopwatch.GetElapsedTime(waitStarted).TotalSeconds;
                    admission?.SetTag("admission.wait_seconds", waitSeconds);
                    span?.SetTag("claim.admission_wait_seconds", waitSeconds);
                    span?.AddEvent(new ActivityEvent("claim.admission.acquired",
                        tags: new ActivityTagsCollection {
                            { "wait.seconds", waitSeconds },
                            { "matching.partition", matchPartition },
                            { "matching.partition_count", matchCount }
                        }));
                    HubTelemetry.ClaimAdmissionWait.Record(waitSeconds,
                        new KeyValuePair<string, object?>("claim_path",
                            matchingPartitionCount is > 1 ? "matching" : "legacy"));
                }
            }

        var supported = capabilities.Select(c => $"{c.HubId}|{c.TaskType}|{c.Version}").ToArray();
        var pins=workflows is null?null:JsonSerializer.Serialize(workflows.Select(w=>new{hub=w.HubId,definition=w.DefinitionId,version=w.Version}));
        var results = new List<HubLeasedTask>(maxTasks);
        var scannedCandidates = 0;
        var connectionStarted = Stopwatch.GetTimestamp();
        await using var db = await _dataSource.OpenConnectionAsync(ct);
        var connectionWaitSeconds = Stopwatch.GetElapsedTime(connectionStarted).TotalSeconds;
        HubTelemetry.ClaimConnectionWait.Record(connectionWaitSeconds);
        span?.SetTag("db.connection_acquire_seconds", connectionWaitSeconds);
        if (_unconstrainedBatchClaimsEnabled)
        {
            // Keep all existing filtering, lease epochs, attempt closure and
            // native/workflow claim events in one fenced PostgreSQL transaction.
            // The legacy path then fills any remaining slots, including all
            // resource-constrained work (FIFO / capacity permits).
            var batchLeases = await ClaimUnconstrainedBatchAsync(db, workerId,
                hubs, supported, pins, maxTasks, lease, matchPartition, matchCount, ct);
            results.AddRange(batchLeases);
            span?.SetTag("tasks.unconstrained_batch_claimed", batchLeases.Count);
        }
        var initialBatchCount = results.Count;
        if (_unconstrainedBatchClaimsEnabled)
        {
            var batchOutcome = initialBatchCount == maxTasks ? "full" :
                initialBatchCount == 0 ? "empty" : "partial";
            HubTelemetry.ClaimBatchOutcome.Add(1,
                new KeyValuePair<string, object?>("outcome", batchOutcome));
        }
        var fallbackStarted = Stopwatch.GetTimestamp();
        for (var batch = results.Count; batch < maxTasks; batch++)
        {
            var candidates = new List<Guid>();
            var queryStarted = Stopwatch.GetTimestamp();
            await using (var candidateQuery = new NpgsqlCommand("""
                -- For a single hub, scan its queue in sequence order. Multi-hub
                -- requests retain the global ordered scan and outer hub filter.
                SELECT t.id FROM (
                  SELECT * FROM wfc_scale.hub_task
                  WHERE state IN (0,1) AND attempts < max_attempts
                    AND /*hub_filter*/
                  ORDER BY sequence OFFSET 0
                ) t
                WHERE t.hub_id = ANY(@hubs)
                  AND (@matchCount=1 OR partition_id % @matchCount=@matchPartition)
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
                  AND (NOT @fifoHeadFilter OR NOT EXISTS (
                    SELECT 1 FROM wfc_scale.hub_task_constraint c
                    JOIN wfc_scale.hub_resource r
                      ON r.resource_key = c.resource_key AND r.strict_fifo
                    JOIN wfc_scale.hub_task_constraint previous
                      ON previous.resource_key = c.resource_key
                    JOIN wfc_scale.hub_task earlier ON earlier.id = previous.task_id
                    WHERE c.task_id = t.id AND earlier.sequence < t.sequence
                      AND earlier.state IN (0, 1)
                  ))
                ORDER BY sequence LIMIT 96
                """.Replace("/*hub_filter*/", hubs.Count == 1
                    ? "hub_id = @hub" : "TRUE"), db))
            {
                candidateQuery.Parameters.AddWithValue("hubs", hubs.ToArray());
                candidateQuery.Parameters.AddWithValue("hub", hubs[0]);
                candidateQuery.Parameters.AddWithValue("matchCount", matchCount);
                candidateQuery.Parameters.AddWithValue("matchPartition", matchPartition);
                candidateQuery.Parameters.AddWithValue("fifoHeadFilter", _fifoHeadPrefilterEnabled);
                candidateQuery.Parameters.AddWithValue("types", supported);
                candidateQuery.Parameters.Add("pins",NpgsqlDbType.Jsonb).Value=(object?)pins??DBNull.Value;
                await using var reader = await candidateQuery.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct)) candidates.Add(reader.GetGuid(0));
            }
            var querySeconds = Stopwatch.GetElapsedTime(queryStarted).TotalSeconds;
            HubTelemetry.ClaimCandidateQuery.Record(querySeconds);
            span?.AddEvent(new ActivityEvent("claim.candidates.selected",
                tags: new ActivityTagsCollection { { "candidates", candidates.Count }, { "query.seconds", querySeconds } }));
            scannedCandidates += candidates.Count;

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
                      AND (@matchCount=1 OR partition_id % @matchCount=@matchPartition)
                      AND (task_type NOT IN ('__workflow__','__native_workflow__') OR @pins IS NULL OR EXISTS(
                        SELECT 1 FROM wfc_scale.hub_workflow w,
                          jsonb_to_recordset(@pins) AS p(hub text,definition text,version integer)
                        WHERE w.id=workflow_id AND w.hub_id=p.hub
                          AND w.definition_id=p.definition AND w.definition_version=p.version))
                      AND NOT EXISTS (SELECT 1 FROM wfc_scale.hub_workflow w
                          WHERE w.id=workflow_id AND w.execution_mode='native' AND w.native_status<>'Runnable')
                      -- Keep the eligibility check, but do not let stale statistics
                      -- turn this primary-key lookup into an active-queue scan.
                      AND attempts::bigint < max_attempts
                      AND ((state = 0 AND due_at <= now()) OR
                           (state = 1 AND lease_until < now()))
                    FOR UPDATE SKIP LOCKED
                    """, db, tx))
                {
                    row.Parameters.AddWithValue("id", taskId);
                    row.Parameters.AddWithValue("matchCount", matchCount);
                    row.Parameters.AddWithValue("matchPartition", matchPartition);
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
                var permitStarted = Stopwatch.GetTimestamp();
                foreach (var constraint in constraints)
                {
                    // Fetch current values *under* the permit lock, never from a stale candidate snapshot.
                    int currentCapacity;
                    bool currentFifo;
                    await using (var resourceLock = new NpgsqlCommand("""
                        SELECT capacity,strict_fifo FROM wfc_scale.hub_resource
                        WHERE resource_key = @key FOR UPDATE
                        """, db, tx))
                    {
                        resourceLock.Parameters.AddWithValue("key", constraint.Key);
                        await using var resourceReader=await resourceLock.ExecuteReaderAsync(ct);
                        if(!await resourceReader.ReadAsync(ct))
                            throw new InvalidOperationException("Resource missing during claim");
                        currentCapacity=resourceReader.GetInt32(0);
                        currentFifo=resourceReader.GetBoolean(1);
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
                    if (active >= currentCapacity || (currentFifo && earlier > 0))
                    {
                        eligible = false;
                        break;
                    }
                }
                if (constraints.Count > 0)
                {
                    var permitSeconds = Stopwatch.GetElapsedTime(permitStarted).TotalSeconds;
                    HubTelemetry.ClaimPermitCheck.Record(permitSeconds);
                    span?.AddEvent(new ActivityEvent("claim.permits.checked",
                        tags: new ActivityTagsCollection { { "resources", constraints.Count }, { "admitted", eligible }, { "permit.seconds", permitSeconds } }));
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
        if (_unconstrainedBatchClaimsEnabled && initialBatchCount < maxTasks)
            HubTelemetry.ClaimLegacyFallbackDuration.Record(
                Stopwatch.GetElapsedTime(fallbackStarted).TotalSeconds,
                new KeyValuePair<string, object?>("batch_result",
                    initialBatchCount == 0 ? "empty" : "partial"));
        var outcome = results.Count == 0 ? "empty" : "claimed";
        // Stable process-level label separates the two benchmark modes without
        // cardinality from hubs, workflow IDs or worker IDs.
        var mode = string.Equals(Environment.GetEnvironmentVariable("WFC_HUB_BATCH_HISTORY_ENABLED"),
            "false", StringComparison.OrdinalIgnoreCase) ||
            Environment.GetEnvironmentVariable("WFC_HUB_BATCH_HISTORY_ENABLED") == "0" ? "legacy" : "batch";
        var modeTag = new KeyValuePair<string, object?>("history_mode", mode);
        var outcomeTag = new KeyValuePair<string, object?>("outcome", outcome);
        var claimStrategyTag = new KeyValuePair<string, object?>("claim_strategy",
            _unconstrainedBatchClaimsEnabled ? "batch" : "legacy");
        HubTelemetry.ClaimDuration.Record(Stopwatch.GetElapsedTime(claimStarted).TotalSeconds,
            outcomeTag, modeTag, claimStrategyTag);
        HubTelemetry.ClaimCandidateCount.Record(scannedCandidates, outcomeTag, modeTag, claimStrategyTag,
            new KeyValuePair<string, object?>("fifo_head_prefilter", _fifoHeadPrefilterEnabled));
        HubTelemetry.ClaimReturnedCount.Record(results.Count, outcomeTag, modeTag, claimStrategyTag);
        span?.SetTag("tasks.claimed", results.Count);
        span?.SetTag("tasks.candidates_scanned", scannedCandidates);
        span?.SetTag("tasks.claim_amplification", results.Count == 0 ? scannedCandidates : (double)scannedCandidates / results.Count);
        return results;
        }
        finally
        {
            if (queuedSlotAcquired)
            {
                HubTelemetry.ClaimAdmissionQueued.Add(-1);
                _claimWaitSlots.Release();
            }
            if (gateAcquired)
            {
                HubTelemetry.ClaimAdmissionActive.Add(-1);
                _claimGate.Release();
            }
        }
    }

    /// <summary>
    /// Single-round-trip row selection and lease update for tasks with NO
    /// hub_task_constraint rows. The SQL must exclude constrained tasks before
    /// SKIP LOCKED, otherwise resource FIFO/capacity could be bypassed.
    /// Nothing about this path changes PostgreSQL fencing or task eligibility.
    /// </summary>
    private async Task<IReadOnlyList<HubLeasedTask>> ClaimUnconstrainedBatchAsync(
        NpgsqlConnection db, string workerId, IReadOnlyList<string> hubs,
        string[] supported, string? pins, int maxTasks, TimeSpan lease,
        int matchingPartition, int matchingCount, CancellationToken ct)
    {
        var started = Stopwatch.GetTimestamp();
        await using var tx = await db.BeginTransactionAsync(ct);
        var claimed = new List<(HubLeasedTask Lease, bool HasStep)>();
        await using (var command = new NpgsqlCommand("""
            WITH candidates AS MATERIALIZED (
                SELECT t.id
                FROM wfc_scale.hub_task t
                WHERE t.hub_id = ANY(@hubs)
                  AND (@matchCount=1 OR t.partition_id % @matchCount=@matchPartition)
                  AND t.hub_id || '|' || t.task_type || '|' || t.task_version::text = ANY(@types)
                  AND t.attempts < t.max_attempts
                  AND ((t.state=0 AND t.due_at<=now()) OR
                       (t.state=1 AND t.lease_until<now()))
                  AND NOT EXISTS (
                      SELECT 1 FROM wfc_scale.hub_task_constraint c WHERE c.task_id=t.id)
                  AND NOT EXISTS (
                      SELECT 1 FROM wfc_scale.hub_workflow w
                      WHERE w.id=t.workflow_id AND w.execution_mode='native'
                        AND w.native_status<>'Runnable')
                  AND (t.task_type NOT IN ('__workflow__','__native_workflow__')
                       OR @pins IS NULL OR EXISTS (
                           SELECT 1 FROM wfc_scale.hub_workflow w,
                               jsonb_to_recordset(@pins) AS p(hub text,definition text,version integer)
                           WHERE w.id=t.workflow_id AND w.hub_id=p.hub
                             AND w.definition_id=p.definition AND w.definition_version=p.version))
                ORDER BY t.sequence
                LIMIT @limit
                FOR UPDATE OF t SKIP LOCKED
            )
            UPDATE wfc_scale.hub_task t
            SET state=1, owner=@owner, last_owner=@owner, epoch=t.epoch+1,
                attempts=t.attempts+1, lease_until=now()+@lease, updated_at=now()
            FROM candidates c WHERE t.id=c.id
            RETURNING t.id,t.hub_id,t.partition_id,t.task_type,t.task_version,
                      t.workflow_id,t.payload::text,t.epoch,t.attempts,t.lease_until,
                      COALESCE(t.workflow_step,t.native_step_id)
            """, db, tx))
        {
            command.Parameters.AddWithValue("hubs", hubs.ToArray());
            command.Parameters.AddWithValue("matchCount", matchingCount);
            command.Parameters.AddWithValue("matchPartition", matchingPartition);
            command.Parameters.AddWithValue("types", supported);
            command.Parameters.Add("pins", NpgsqlDbType.Jsonb).Value =
                (object?)pins ?? DBNull.Value;
            command.Parameters.AddWithValue("limit", maxTasks);
            command.Parameters.AddWithValue("owner", workerId);
            command.Parameters.Add("lease", NpgsqlDbType.Interval).Value = lease;
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var workflow = reader.IsDBNull(5) ? (Guid?)null : reader.GetGuid(5);
                var leased = new HubLeasedTask(
                    reader.GetGuid(0), reader.GetString(1), reader.GetInt32(2),
                    reader.GetString(3), reader.GetInt32(4), workflow,
                    JsonDocument.Parse(reader.GetString(6)).RootElement.Clone(),
                    reader.GetInt64(7), reader.GetInt32(8), reader.GetDateTime(9));
                claimed.Add((leased, workflow.HasValue && !reader.IsDBNull(10)));
            }
        }

        if (claimed.Count == 0)
        {
            await tx.CommitAsync(ct);
            HubTelemetry.ClaimUnconstrainedBatchCount.Record(0);
            HubTelemetry.ClaimUnconstrainedBatchDuration.Record(
                Stopwatch.GetElapsedTime(started).TotalSeconds);
            return Array.Empty<HubLeasedTask>();
        }

        // Preserve exactly the same attempt lifecycle as the legacy claim:
        // close the previous expired attempt, then append the new epoch.
        var stepTaskIds = claimed.Where(c => c.HasStep).Select(c => c.Lease.Id).ToArray();
        if (stepTaskIds.Length > 0)
        {
            await using (var close = new NpgsqlCommand("""
                UPDATE wfc_scale.hub_task_attempt
                SET finished_at=clock_timestamp(), outcome='LeaseExpired'
                WHERE task_id=ANY(@ids) AND finished_at IS NULL
                """, db, tx))
            {
                close.Parameters.AddWithValue("ids", stepTaskIds);
                await close.ExecuteNonQueryAsync(ct);
            }
            await using (var attempts = new NpgsqlCommand("""
                INSERT INTO wfc_scale.hub_task_attempt
                    (task_id,epoch,workflow_id,step_index,attempt_number,worker_id)
                SELECT t.id,t.epoch,t.workflow_id,
                       COALESCE(t.workflow_step,t.native_step_id),t.attempts,@worker
                FROM wfc_scale.hub_task t WHERE t.id=ANY(@ids)
                """, db, tx))
            {
                attempts.Parameters.AddWithValue("ids", stepTaskIds);
                attempts.Parameters.AddWithValue("worker", workerId);
                await attempts.ExecuteNonQueryAsync(ct);
            }
        }

        var workflowTaskIds = claimed.Where(c =>
            c.Lease.WorkflowId.HasValue &&
            c.Lease.TaskType is "__workflow__" or "__native_workflow__")
            .Select(c => c.Lease.Id).ToArray();
        if (workflowTaskIds.Length > 0)
        {
            await using var journal = new NpgsqlCommand("""
                INSERT INTO wfc_scale.hub_workflow_event
                    (workflow_id,step_index,task_id,activity_type,event_type,payload)
                SELECT w.id,w.next_step,t.id,t.task_type,'WorkflowTaskClaimed',
                       jsonb_build_object('worker',@worker,'epoch',t.epoch)
                FROM wfc_scale.hub_task t
                JOIN wfc_scale.hub_workflow w ON w.id=t.workflow_id
                WHERE t.id=ANY(@ids)
                """, db, tx);
            journal.Parameters.AddWithValue("ids", workflowTaskIds);
            journal.Parameters.AddWithValue("worker", workerId);
            await journal.ExecuteNonQueryAsync(ct);
        }

        await tx.CommitAsync(ct);
        HubTelemetry.ClaimUnconstrainedBatchCount.Record(claimed.Count);
        HubTelemetry.ClaimUnconstrainedBatchDuration.Record(
            Stopwatch.GetElapsedTime(started).TotalSeconds);
        foreach (var item in claimed)
            HubTelemetry.TasksClaimed.Add(1, HubTelemetry.HubTag(item.Lease.HubId),
                HubTelemetry.ActivityTag(item.Lease.TaskType));
        return claimed.Select(item => item.Lease).ToArray();
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

    public async Task<HubCompletionRoute?> GetCompletionRouteAsync(Guid id, CancellationToken ct)
    {
        await using var cmd = _dataSource.CreateCommand("""
            SELECT t.hub_id,
                   CASE WHEN t.dispatch_key IS NOT NULL THEN 3
                        WHEN w.execution_mode='remote' AND t.workflow_step IS NOT NULL
                             AND t.task_type<>'__workflow__' THEN 2
                        WHEN t.workflow_step IS NOT NULL THEN 1
                        ELSE 0 END
            FROM wfc_scale.hub_task t
            LEFT JOIN wfc_scale.hub_workflow w ON w.id=t.workflow_id
            WHERE t.id=@id
            """);
        cmd.Parameters.AddWithValue("id", id);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return null;
        return new(reader.GetString(0), (HubCompletionKind)reader.GetInt32(1));
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

    /// <summary>
    /// Low-frequency recovery scan for missed PostgreSQL notifications.
    /// Returns shard-targetable hints, not a one-query-per-worker claim.
    /// The dispatcher deduplicates these before touching the claim store.
    /// </summary>
    public async Task<IReadOnlyList<(string Hub, string TaskType, int Version, int Partition)>> ReadyMatchingHintsAsync(
        CancellationToken ct)
    {
        await using var cmd = _dataSource.CreateCommand("""
            SELECT hub_id,task_type,task_version,partition_id
            FROM wfc_scale.hub_task
            WHERE attempts < max_attempts AND
                  ((state=0 AND due_at<=now()) OR
                   (state=1 AND lease_until<now()))
            GROUP BY hub_id,task_type,task_version,partition_id
            ORDER BY hub_id,task_type,task_version,partition_id
            LIMIT 512
            """);
        var hints = new List<(string Hub, string TaskType, int Version, int Partition)>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            hints.Add((reader.GetString(0), reader.GetString(1), reader.GetInt32(2),
                reader.GetInt32(3)));
        return hints;
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

    public async ValueTask DisposeAsync()
    {
        await _dataSource.DisposeAsync();
        _claimGate.Dispose();
        _claimWaitSlots.Dispose();
    }
}

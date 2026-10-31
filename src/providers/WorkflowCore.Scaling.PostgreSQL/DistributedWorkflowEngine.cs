using System.Security.Cryptography;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using WorkflowCore.Interface;
using WorkflowCore.Models;
using WorkflowCore.Persistence.EntityFramework;
using WorkflowCore.Persistence.EntityFramework.Models;
using WorkflowCore.Persistence.PostgreSQL;

namespace WorkflowCore.Scaling.PostgreSQL;

/// <summary>
/// PostgreSQL-authoritative workflow runtime. Do NOT start the legacy polling,
/// event, or workflow consumers alongside this engine: they bypass fencing.
/// </summary>
public sealed class DistributedWorkflowEngine
{
    private readonly string _connectionString;
    private readonly FencedWorkQueue _queue;
    private readonly IWorkflowRegistry _registry;
    private readonly IExecutionPointerFactory _pointers;
    private readonly IServiceScopeFactory _scopes;
    private readonly JsonSerializerSettings _serializer;
    private readonly string _schema;

    public DistributedWorkflowEngine(string connectionString, FencedWorkQueue queue,
        IWorkflowRegistry registry, IExecutionPointerFactory pointers,
        IServiceScopeFactory scopes, string schema = "wfc")
    {
        _connectionString = connectionString;
        _queue = queue;
        _registry = registry;
        _pointers = pointers;
        _scopes = scopes;
        _schema = schema;
        _serializer = ExtensionMethods.CreateSerializerSettings();
    }

    private PostgresContext Context() => new(_connectionString, _schema);

    /// <summary>
    /// Creates workflow state and the first execution intent in one transaction.
    /// This is the only supported way to create workflows using this runtime.
    /// </summary>
    public async Task<Guid> StartAsync(string definitionId, object? data = null,
        int? definitionVersion = null, string? reference = null,
        string? idempotencyKey = null, CancellationToken ct = default)
    {
        var definition = _registry.GetDefinition(definitionId, definitionVersion)
            ?? throw new InvalidOperationException($"Workflow definition not registered: {definitionId}");
        var id = Guid.NewGuid();
        var instance = new WorkflowInstance
        {
            Id = id.ToString(), WorkflowDefinitionId = definition.Id,
            Version = definition.Version, Data = data ??
                (definition.DataType is null ? new object() : Activator.CreateInstance(definition.DataType)),
            Description = definition.Description, Reference = reference,
            NextExecution = 0, Status = WorkflowStatus.Runnable,
            CreateTime = DateTime.UtcNow
        };
        instance.ExecutionPointers.Add(_pointers.BuildGenesisPointer(definition));
        await using var db = Context();
        await using var tx = await db.Database.BeginTransactionAsync(ct);
        if (idempotencyKey is not null)
        {
            var fingerprint = Fingerprint(definitionId, definitionVersion?.ToString(),
                reference, JsonConvert.SerializeObject(data, _serializer));
            var duplicate = await ClaimRequestAsync(db, "workflow", idempotencyKey, id, fingerprint, ct);
            if (duplicate.HasValue) return duplicate.Value;
        }
        db.Set<PersistedWorkflow>().Add(instance.ToPersistable(_serializer));
        await db.SaveChangesAsync(ct);
        await InsertWorkAsync(db, "workflow", id, $"wf:{id}:0", DateTime.UtcNow, ct);
        await ExecutionHistory.AppendAsync(db, id, 0, 0, "WorkflowStarted", ct,
            details: new Dictionary<string, string?>
            {
                ["definitionId"] = definition.Id, ["version"] = definition.Version.ToString()
            });
        var genesis = instance.ExecutionPointers.Single();
        await ExecutionHistory.AppendAsync(db, id, 0, 1, "StepScheduled", ct,
            genesis.Id, genesis.StepId);
        await tx.CommitAsync(ct);
        return id;
    }

    /// <summary>Record an event and its durable dispatch intent atomically.</summary>
    public async Task<Guid> PublishEventAsync(string name, string key, object? data,
        DateTime? effectiveAt = null, string? idempotencyKey = null,
        CancellationToken ct = default)
    {
        var id = Guid.NewGuid();
        var due = effectiveAt?.ToUniversalTime() ?? DateTime.UtcNow;
        var payload = JsonConvert.SerializeObject(data, _serializer);
        await using var db = Context();
        await using var tx = await db.Database.BeginTransactionAsync(ct);
        if (idempotencyKey is not null)
        {
            var fingerprint = Fingerprint(name, key, payload, effectiveAt?.ToUniversalTime().Ticks.ToString());
            var duplicate = await ClaimRequestAsync(db, "event", idempotencyKey, id, fingerprint, ct);
            if (duplicate.HasValue) return duplicate.Value;
        }
        await db.Database.ExecuteSqlInterpolatedAsync($"""
            INSERT INTO wfc_scale.signal_event(id,event_name,event_key,payload,effective_at)
            VALUES ({id},{name},{key},{payload},{due})
            """, ct);
        await InsertWorkAsync(db, "event", id, $"event:{id}:initial", due, ct);
        await tx.CommitAsync(ct);
        return id;
    }

    public async Task<bool> ChangeStatusAsync(Guid workflowId, WorkflowStatus target,
        CancellationToken ct = default)
    {
        var owner = $"control:{Guid.NewGuid():N}";
        var fence = await _queue.AcquireWorkflowAsync(workflowId, owner, TimeSpan.FromSeconds(30), ct);
        if (fence is null) return false;
        try
        {
            await using var db = Context();
            await using var tx = await db.Database.BeginTransactionAsync(ct);
            if (!await AdvanceFenceAsync(db, fence, ct))
                return false;
            var stored = await LoadTrackedAsync(db, workflowId, ct);
            if (stored is null) return false;
            var previousStatus = stored.Status;
            stored.Status = target;
            if (target == WorkflowStatus.Runnable)
            {
                stored.NextExecution = 0;
                await InsertWorkAsync(db, "workflow", workflowId,
                    $"wf:{workflowId}:{fence.Revision + 1}", DateTime.UtcNow, ct);
            }
            if (target is WorkflowStatus.Complete or WorkflowStatus.Terminated)
                stored.CompleteTime = DateTime.UtcNow;
            await db.SaveChangesAsync(ct);
            if (previousStatus != target)
                await ExecutionHistory.AppendAsync(db, workflowId, fence.Revision + 1,
                    0, "WorkflowStatusChanged", ct, worker: owner,
                    details: new Dictionary<string, string?>
                    {
                        ["from"] = previousStatus.ToString(), ["to"] = target.ToString()
                    });
            await tx.CommitAsync(ct);
            return true;
        }
        finally { await _queue.ReleaseGateAsync(fence, CancellationToken.None); }
    }

    /// <summary>
    /// Called only by a worker that owns BOTH the work-item lease and the workflow gate.
    /// External effects remain at-least-once and require application idempotency keys.
    /// </summary>
    public async Task ExecuteWorkflowAsync(LeasedWork work, WorkflowFence fence,
        string owner, CancellationToken ct)
    {
        // Coalescing wakeups is safe only if stale generations are discarded.
        // The durable work key denotes the expected state revision to execute.
        if (work.ItemKey != $"wf:{work.ItemId}:{fence.Revision}")
        {
            await AckEventAsync(work, owner, ct);
            return;
        }

        WorkflowInstance instance;
        WorkflowInstance before;
        await using (var read = Context())
        {
            var stored = await LoadTrackedAsync(read, work.ItemId, ct);
            if (stored is null) throw new InvalidOperationException($"Workflow {work.ItemId} not found");
            // Independent snapshots: execution mutates instance in memory.
            before = stored.ToWorkflowInstance(_serializer);
            instance = stored.ToWorkflowInstance(_serializer);
        }

        // Durable dispatch evidence is committed before invoking step bodies.
        // The outcome remains unknown if this worker crashes before state commit.
        IReadOnlyList<ExecutionPointer> dispatched;
        await using (var dispatch = Context())
            dispatched = await ExecutionHistory.DispatchAsync(
                dispatch, instance, work, fence, owner, ct);

        WorkflowExecutorResult? result = null;
        if (instance.Status == WorkflowStatus.Runnable)
        {
            using var scope = _scopes.CreateScope();
            var executor = scope.ServiceProvider.GetRequiredService<IWorkflowExecutor>();
            result = await executor.Execute(instance, ct);
        }

        ct.ThrowIfCancellationRequested();
        await using var db = Context();
        await using var tx = await db.Database.BeginTransactionAsync(ct);
        if (!await AdvanceFenceAsync(db, fence, ct))
            throw new InvalidOperationException("Workflow owner lost fencing generation");
        if (!await AckAsync(db, work, owner, ct))
            throw new InvalidOperationException("Queue lease expired during execution");

        if (result is not null)
        {
            var existing = await LoadTrackedAsync(db, work.ItemId, ct);
            if (existing is null) throw new InvalidOperationException("Workflow disappeared");
            instance.ToPersistable(_serializer, existing);
            foreach (var sub in result.Subscriptions)
            {
                sub.Id = Guid.NewGuid().ToString();
                db.Set<PersistedSubscription>().Add(sub.ToPersistable(_serializer));
            }
            foreach (var error in result.Errors)
                db.Set<PersistedExecutionError>().Add(error.ToPersistable());

            if (instance.Status == WorkflowStatus.Runnable && instance.NextExecution.HasValue)
            {
                var ticks = instance.NextExecution.Value;
                var due = ticks <= DateTime.UtcNow.Ticks
                    ? DateTime.UtcNow : new DateTime(ticks, DateTimeKind.Utc);
                await InsertWorkAsync(db, "workflow", work.ItemId,
                    $"wf:{work.ItemId}:{fence.Revision + 1}", due, ct);
            }
            // Backfill ALL pending signals, even those arriving before WaitFor
            // was registered. Their effective time remains a lower bound.
            foreach (var sub in result.Subscriptions)
            {
                await db.Database.ExecuteSqlInterpolatedAsync($"""
                    INSERT INTO wfc_scale.work_item
                        (id, queue_name, item_key, item_id, due_at)
                    SELECT gen_random_uuid(), 'event',
                           'event:' || e.id || ':subscription:' || {sub.Id},
                           e.id, greatest(e.effective_at, now())
                    FROM wfc_scale.signal_event e
                    WHERE e.processed = false
                      AND e.event_name = {sub.EventName}
                      AND e.event_key = {sub.EventKey}
                      AND e.effective_at >= {sub.SubscribeAsOf.ToUniversalTime()}
                    ON CONFLICT (queue_name, item_key) DO NOTHING
                    """, ct);
            }
        }
        await db.SaveChangesAsync(ct);
        await ExecutionHistory.AppendOutcomesAsync(db, work.ItemId,
            fence.Revision + 1, before, instance, result, dispatched, work, owner, ct);
        await tx.CommitAsync(ct);
    }

    public async Task ExecuteEventAsync(LeasedWork work, string owner, CancellationToken ct)
    {
        string name, key, payload;
        DateTime effective;
        await using (var db = Context())
        {
            // Event claimed by a single queue work item; delivery markers protect
            // against redelivery and partially completed fanout.
            await using var cmd = db.Database.GetDbConnection().CreateCommand();
            cmd.CommandText = "SELECT event_name,event_key,payload,effective_at FROM wfc_scale.signal_event WHERE id = @id AND NOT processed";
            var param = cmd.CreateParameter(); param.ParameterName = "id"; param.Value = work.ItemId;
            cmd.Parameters.Add(param);
            await db.Database.OpenConnectionAsync(ct);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
            {
                await AckEventAsync(work, owner, ct);
                return;
            }
            name = reader.GetString(0); key = reader.GetString(1);
            payload = reader.GetString(2); effective = reader.GetDateTime(3);
        }

        // Find all matching subscriptions, including multiple workflows.
        List<PersistedSubscription> subscriptions;
        await using (var db = Context())
            subscriptions = await db.Set<PersistedSubscription>().AsNoTracking()
                .Where(s => s.EventName == name && s.EventKey == key && s.SubscribeAsOf <= effective)
                .OrderBy(s => s.WorkflowId).ThenBy(s => s.SubscriptionId)
                .ToListAsync(ct);

        // Event ordering: an earlier pending signal eligible for any active
        // subscription must be consumed first, even across worker replicas.
        if (subscriptions.Count > 0)
        {
            await using var orderDb = Context();
            var older = await orderDb.Database.SqlQuery<Guid>($"""
                SELECT e.id AS "Value" FROM wfc_scale.signal_event e
                WHERE e.processed = false
                  AND e.event_name = {name} AND e.event_key = {key}
                  AND (e.effective_at < {effective}
                    OR (e.effective_at = {effective} AND e.id < {work.ItemId}))
                  AND EXISTS (
                     SELECT 1 FROM wfc."Subscription" s
                     WHERE s."EventName" = e.event_name AND s."EventKey" = e.event_key
                       AND s."SubscribeAsOf" <= e.effective_at
                  )
                ORDER BY e.effective_at, e.id LIMIT 1
                """).FirstOrDefaultAsync(ct);
            if (older != Guid.Empty)
            {
                await InsertWorkAsync(orderDb, "event", older,
                    $"event:{older}:precedes:{work.ItemId}", DateTime.UtcNow, ct);
                throw new DeferredWorkException("Earlier matching signal must be processed first");
            }
        }

        foreach (var sub in subscriptions)
        {
            ct.ThrowIfCancellationRequested();
            var id = Guid.Parse(sub.WorkflowId);
            var subOwner = $"signal:{work.Id:N}:{sub.SubscriptionId:N}";
            var fence = await _queue.AcquireWorkflowAsync(id, subOwner, TimeSpan.FromSeconds(30), ct);
            if (fence is null) throw new DeferredWorkException("Workflow busy during event delivery");
            try
            {
                await using var db = Context();
                await using var tx = await db.Database.BeginTransactionAsync(ct);
                var already = await db.Database.SqlQuery<int>($"""
                    SELECT 1 AS "Value" FROM wfc_scale.signal_delivery
                    WHERE event_id = {work.ItemId} AND subscription_id = {sub.SubscriptionId}
                    """).AnyAsync(ct);
                if (already) continue;
                if (!await AdvanceFenceAsync(db, fence, ct))
                    throw new InvalidOperationException("Event delivery lost workflow fence");
                var stored = await LoadTrackedAsync(db, id, ct);
                if (stored is null) throw new InvalidOperationException("Workflow missing on event delivery");
                var workflow = stored.ToWorkflowInstance(_serializer);
                var data = JsonConvert.DeserializeObject(payload, _serializer);
                foreach (var pointer in workflow.ExecutionPointers.Where(p =>
                    (p.Id == sub.ExecutionPointerId || (p.EventName == name && p.EventKey == key))
                    && !p.EventPublished && p.EndTime == null))
                {
                    pointer.EventData = data;
                    pointer.EventPublished = true;
                    pointer.Active = true;
                }
                workflow.NextExecution = 0;
                workflow.ToPersistable(_serializer, stored);
                var subscription = await db.Set<PersistedSubscription>().AsTracking()
                    .SingleOrDefaultAsync(s => s.SubscriptionId == sub.SubscriptionId, ct);
                if (subscription is not null)
                    db.Set<PersistedSubscription>().Remove(subscription);
                await db.Database.ExecuteSqlInterpolatedAsync($"""
                    INSERT INTO wfc_scale.signal_delivery(event_id,subscription_id)
                    VALUES ({work.ItemId},{sub.SubscriptionId})
                    ON CONFLICT DO NOTHING
                    """, ct);
                await InsertWorkAsync(db, "workflow", id,
                    $"wf:{id}:{fence.Revision + 1}", DateTime.UtcNow, ct);
                await db.SaveChangesAsync(ct);
                await ExecutionHistory.AppendAsync(db, id, fence.Revision + 1,
                    0, "SignalDelivered", ct, sub.ExecutionPointerId, sub.StepId,
                    worker: subOwner,
                    eventKey: $"signal:{work.ItemId:N}:{sub.SubscriptionId:N}",
                    details: new Dictionary<string, string?>
                    {
                        ["signalId"] = work.ItemId.ToString("N"),
                        ["subscriptionId"] = sub.SubscriptionId.ToString("N")
                    });
                await tx.CommitAsync(ct);
            }
            finally { await _queue.ReleaseGateAsync(fence, CancellationToken.None); }
        }
        await using (var db = Context())
        {
            await using var tx = await db.Database.BeginTransactionAsync(ct);
            // If delivery finished before an earlier attempt crashed, the
            // subscription rows may already be gone. Delivery markers preserve
            // evidence that at least one subscriber consumed this event.
            await db.Database.ExecuteSqlInterpolatedAsync($"""
                UPDATE wfc_scale.signal_event
                SET processed = true
                WHERE id = {work.ItemId}
                  AND (EXISTS (SELECT 1 FROM wfc_scale.signal_delivery WHERE event_id = {work.ItemId})
                       OR {subscriptions.Count} > 0)
                """, ct);
            if (!await AckAsync(db, work, owner, ct))
                throw new InvalidOperationException("Event queue lease expired");
            await tx.CommitAsync(ct);
        }
    }

    private static string Fingerprint(params string?[] fields)
    {
        var canonical = JsonConvert.SerializeObject(fields);
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(canonical)));
    }

    private static async Task<Guid?> ClaimRequestAsync(PostgresContext db, string kind,
        string requestKey, Guid candidateId, string fingerprint, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(requestKey))
            throw new ArgumentException("Idempotency key cannot be empty", nameof(requestKey));
        var inserted = await db.Database.ExecuteSqlInterpolatedAsync($"""
            INSERT INTO wfc_scale.request_dedupe(kind,request_key,resource_id,payload_hash)
            VALUES ({kind},{requestKey},{candidateId},decode({fingerprint}, 'hex'))
            ON CONFLICT (kind,request_key) DO NOTHING
            """, ct);
        if (inserted == 1) return null;
        var saved = await db.Database.SqlQuery<string>($"""
            SELECT resource_id::text || ':' || encode(payload_hash,'hex') AS "Value"
            FROM wfc_scale.request_dedupe
            WHERE kind = {kind} AND request_key = {requestKey}
            """).SingleAsync(ct);
        var split = saved.LastIndexOf(':');
        if (!string.Equals(saved[(split + 1)..], fingerprint, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("Idempotency key reused with different request content");
        return Guid.Parse(saved[..split]);
    }

    private async Task AckEventAsync(LeasedWork work, string owner, CancellationToken ct)
    {
        await using var db = Context();
        if (!await AckAsync(db, work, owner, ct))
            throw new InvalidOperationException("Event acknowledgement lost ownership");
    }

    internal static Task<int> InsertWorkAsync(PostgresContext db, string queue,
        Guid itemId, string key, DateTime due, CancellationToken ct) =>
        db.Database.ExecuteSqlInterpolatedAsync($"""
            INSERT INTO wfc_scale.work_item(id,queue_name,item_key,item_id,due_at)
            VALUES ({Guid.NewGuid()},{queue},{key},{itemId},{due})
            ON CONFLICT (queue_name,item_key) DO NOTHING
            """, ct);

    private static async Task<bool> AdvanceFenceAsync(PostgresContext db,
        WorkflowFence fence, CancellationToken ct) =>
        await db.Database.ExecuteSqlInterpolatedAsync($"""
            UPDATE wfc_scale.workflow_gate SET revision = revision + 1
            WHERE workflow_id = {fence.WorkflowId} AND owner = {fence.Owner}
              AND epoch = {fence.Epoch} AND revision = {fence.Revision}
              AND lease_until > now()
            """, ct) == 1;

    private static async Task<bool> AckAsync(PostgresContext db, LeasedWork work,
        string owner, CancellationToken ct) =>
        await db.Database.ExecuteSqlInterpolatedAsync($"""
            UPDATE wfc_scale.work_item SET state = 2, owner = NULL,
                lease_until = NULL, updated_at = now()
            WHERE id = {work.Id} AND state = 1 AND owner = {owner}
              AND epoch = {work.Epoch} AND lease_until > now()
            """, ct) == 1;

    private static Task<PersistedWorkflow?> LoadTrackedAsync(PostgresContext db,
        Guid id, CancellationToken ct) => db.Set<PersistedWorkflow>().AsTracking()
        .Include(w => w.ExecutionPointers).ThenInclude(ep => ep.ExtensionAttributes)
        .SingleOrDefaultAsync(w => w.InstanceId == id, ct);
}

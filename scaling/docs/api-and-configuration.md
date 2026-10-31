# API, hosting and configuration reference

## Target and registration

New runtime project: `src/providers/WorkflowCore.Scaling.PostgreSQL`,
target framework **net10.0**. PostgreSQL provider and EF Core projects are
referenced rather than duplicated.

```csharp
builder.Services.AddDistributedWorkflowCore(connectionString, options =>
{
    options.Concurrency = 16;
    options.MaxAttempts = 12;
    options.LeaseDuration = TimeSpan.FromSeconds(60);
    options.IdleDelay = TimeSpan.FromMilliseconds(200);
    options.RetryDelay = TimeSpan.FromSeconds(2);
});
builder.Services.AddTransient<MyStep>();

var app = builder.Build();
app.Services.GetRequiredService<IWorkflowHost>()
    .RegisterWorkflow<MyWorkflow>();
// Do not call IWorkflowHost.Start(): the generic host starts the new pump.
await app.RunAsync();
```

The extension disables `EnableWorkflows`, `EnableEvents`,
`EnablePolling`, `EnableIndexes` and
`EnableLifeCycleEventsPublisher` for the legacy runtime, registers
`FencedWorkQueue`, `DistributedWorkflowEngine` and the hosted
`DistributedWorkPump`.

**Important:** `AddDistributedWorkflowCore` itself does not create/migrate
the database. Apply old EF migrations and new SQL in deployment.

## Engine methods

| Method | Meaning |
|---|---|
| `StartAsync(definitionId, data?, definitionVersion?, reference?, idempotencyKey?, ct)` | Validate registered definition and transactionally create workflow + initial work |
| `PublishEventAsync(name,key,data,effectiveAt?,idempotencyKey?,ct)` | Persist signal + durable event work |
| `ChangeStatusAsync(workflowId,target,ct)` | Acquire workflow gate, change status, optionally enqueue resume work |
| `ExecuteWorkflowAsync(work,fence,owner,ct)` | Worker-only fenced workflow execution |
| `ExecuteEventAsync(work,owner,ct)` | Worker-only matching event delivery |

Callers should use `DistributedWorkflowEngine` for commands. Calling original
`IWorkflowHost.StartWorkflow()` or `IWorkflowHost.PublishEvent()` bypasses
the distributed ledger and is **not supported**. The worker methods are
internal operational interfaces in practice; do not expose them as public HTTP
operations.

`ChangeStatusAsync` supports the `WorkflowStatus` enum, but transitions and
concurrent callers need further state-machine testing before production.

## Work pump options

| Name | Default | Effect / constraint |
|---|---:|---|
| `Concurrency` | 8 | Worker slots per process, validated >=1 |
| `MaxAttempts` | 12 | Maximum claimed attempts before dead-letter path, validated >=1 |
| `LeaseDuration` | 60 seconds | Work/gate lease, validated >=10 seconds |
| `IdleDelay` | 200 ms | Delay after an empty queue poll |
| `RetryDelay` | 2 seconds | Base exponential retry delay; jitter applied |

Heartbeat interval is approximately `LeaseDuration / 3`; worker IDs include
machine name, a process GUID and slot index. Retries grow exponentially, with
factor capped at 64 and jitter. `DeferAsync` returns contended work to ready
and reverses the claim's attempt increment.

The sample API takes `WORKERS` from the environment to override concurrency
(default 16 in that sample). No tenant limit, dynamic reload or autoscaler is
implemented.

## Sample HTTP API (development only)

File: `src/samples/WorkflowCore.Scaling.Api/Program.cs`.

| Route | Behavior |
|---|---|
| `POST /api/workflows` | Body `{"tenantId":"tenant-1"}`; returns 202 with workflowId |
| `POST /api/events` | Body `{"name":"X","key":"K","value":"payload"}`; returns 202 with eventId |
| `GET /api/workflows/{id}` | Returns ID, definition ID, status and next execution |
| `GET /health/ready` | `SELECT 1` PostgreSQL connectivity check |

Both POST handlers accept an optional `Idempotency-Key` header. Sample
definition `benchmark` has a one-step no-op; it is **not** a production
provisioning API.

The sample has **no authentication, authorization, rate limiting or tenant
ownership enforcement**. Do not expose it beyond a trusted test network.

## Configuration boundaries

- The new `wfc_scale` schema is hard-coded in raw SQL.
- The event ordering query explicitly references `wfc."Subscription"`.
  Other EF schema names are not end-to-end supported.
- Npgsql owns a data source and pool per `FencedWorkQueue` instance; treat it
  as a singleton and dispose it with the service provider.
- All replicas must share compatible workflow definitions and serializer
  types. Default serialization uses Newtonsoft type metadata; do not trust
  arbitrary input types.

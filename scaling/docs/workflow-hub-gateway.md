# Workflow Hub Gateway and Code-First Concurrency

**Status: functional remote-activity prototype, not a complete remote Workflow Core runtime.**

## Architecture

~~~mermaid
flowchart LR
  API["REST submissions"] --> H["Standalone Hub Gateway"]
  H --> P["Code-first concurrency admission"]
  P --> DB[("PostgreSQL durable queue + leases")]
  H -. "SSE readiness notification" .-> W["Remote activity worker"]
  W -->|"REST claim / heartbeat / complete"| H
~~~

The standalone Hub host is under scaling/src/WorkflowCore.Hub.Server.
The API/broadcaster project is scaling/src/WorkflowCore.Hub.Gateway.
The bounded Npgsql store and code-first policy registry are in
scaling/src/WorkflowCore.Hub.

Business hubs are namespaces (onboarding, billing, backup).
Partition IDs are computed deterministically and stored with each task.
Worker fleets can be scaled independently of the Hub process; remote
workers do not initialize PostgreSQL connections or need its credentials.

## Start locally

~~~powershell
pwsh ./scripts/dev.ps1 bootstrap

# Terminal A: standalone Hub, port 5100.
$env:WFC_HUB_AUTH_TOKEN = 'local-development-secret-change-me-2026'
pwsh ./scripts/dev.ps1 hub

# Terminal B: worker with no DB credential.
$env:WFC_HUB_AUTH_TOKEN = 'local-development-secret-change-me-2026'
$env:WFC_HUB_URL = 'http://127.0.0.1:5100'
$env:WORKERS = '4'
pwsh ./scripts/dev.ps1 remote-worker

# Terminal C: add a remote onboarding activity.
$headers = @{ Authorization = 'Bearer local-development-secret-change-me-2026' }
$body = @{
  hubId = 'onboarding'
  taskType = 'provision-tenant'
  version = 1
  payload = @{ tenantId = 'acme' }
  idempotencyKey = [guid]::NewGuid().ToString()
} | ConvertTo-Json -Depth 10
$result = Invoke-RestMethod 'http://127.0.0.1:5100/api/hub/tasks' -Method Post -Headers $headers -ContentType 'application/json' -Body $body
Invoke-RestMethod "http://127.0.0.1:5100/api/hub/tasks/$($result.taskId)" -Headers $headers
~~~

For E2E: pwsh ./scripts/test-hub-gateway.ps1 -ApiUrl http://127.0.0.1:5100

## REST + SSE operations

| Method | Path | Purpose |
| --- | --- | --- |
| GET | /api/hub/policies | Read-only effective code-first policy list |
| POST | /api/hub/tasks | Validate registered task, enqueue durably |
| GET | /api/hub/tasks/{id} | Task state/result |
| GET | /api/hub/workers/events?hub=onboarding,billing | SSE readiness |
| POST | /api/hub/workers/tasks:claim | Atomic admission + fenced lease |
| POST | /api/hub/tasks/{id}/heartbeat | Renew active lease |
| POST | /api/hub/tasks/{id}/complete | Idempotent, fenced completion |
| POST | /api/hub/tasks/{id}/fail | Durable retry / dead-letter |

Claims specify workerId, hubs, exact capabilities [(hubId,taskType,version)],
and maxTasks. SSE is an advisory wakeup, never proof of ownership. A new
subscriber receives initial hints, with replay-by-claim on reconnect and
periodic reconciliation. Hub replicas listen for PostgreSQL `wfc_hub_task_ready` notifications after
migration 014 and immediately fan out advisory SSE hints for their subscribed hubs.
A dedicated, non-pooled LISTEN connection per Hub pod is required. The existing
2-second DB sweep remains the loss-recovery and timer/lease-expiry fallback.
Keepalive comments protect idle connections. See
[notification plan](hub-postgres-notify-sse.md).

## Code-first policy configuration

~~~csharp
registry.Register("onboarding", "provision-tenant", 1, 32, policy => policy
    .MaxHubConcurrency(100)
    .MaxActivityConcurrency(20)
    .SequentialBy("tenant", body => body.GetProperty("tenantId").GetString()));
~~~

Available rules: MaxHubConcurrency, MaxActivityConcurrency,
MaxWorkflowTaskConcurrency, LimitBy, ExclusiveBy, SequentialBy,
ExclusiveGloballyBy, SequentialGloballyBy. The workflow-task cap applies
to dispatched workflow tasks; it does NOT constrain the entire lifetime
of old Workflow Core instances waiting for external signals.

Concurrency constraints are resolved server-side from code, never accepted
from untrusted worker-provided JSON policy. Resource values are SHA256-hashed
into scope-specific resource keys. Named resource policies are persisted and
immutable; conflicting changes require explicit policy migration.

The queue stores a constraint row for each applicable resource. A claim
locks task and resource rows in stable order, checks active live leases
against all capacities, checks all strict FIFO predecessors, then commits
the task lease and its incremented epoch. No worker waits while occupying a
slot for a blocked activity; failed tasks use persisted retry deadlines.
Strict FIFO blocks later tasks during earlier retries. Concurrent enqueue
transactions are serialized for the same FIFO key so sequence order is
meaningful. A task that exhausts retries enters Dead state.

Completion and heartbeat require the matching workerId and epoch with
an unexpired lease. Duplicate completion with the same epoch and JSON is
idempotent after a lost HTTP response. Leases only guarantee Hub admission:
a zombie worker can continue an external side effect after losing a lease,
so downstream operations must use fencing or idempotency.

## PostgreSQL connection budget

The standalone Hub uses one NpgsqlDataSource with a maximum pool size of
WFC_HUB_DB_POOL (default 12 per process). For a deployment with 8 Hub
replicas, that means at most 96 connections from these Hub stores, plus
one dedicated PostgreSQL LISTEN session per replica (when enabled), and any
connections from any other services. REST/SSE worker processes use zero
workflow-state PostgreSQL connections.

This protects connection count, not database CPU, row-lock contention or
transaction throughput. Load-test queue admission and hot keys.

## Security and production limitations

The prototype uses a single bearer token, WFC_HUB_AUTH_TOKEN (24+ chars),
for all Hub routes. **This is for a private lab only.** Add TLS, service
identity, scoped hub authorization, audit, rate limiting and payload access
controls before public deployment.

The following are not yet implemented:

- Migration of the existing DistributedWorkflowEngine execution loop to
  remote Hub-managed state load/commit, timers, workflow-gate fencing and
  event subscriptions. Existing Workflow Core processors still use PostgreSQL.
- Automatic remote execution of arbitrary legacy StepBody implementations.
  This slice implements separately registered IHubActivityHandler handlers.
- Transactional completion outbox to wake waiting Workflow Core pointers.
- Partition ownership assignment/rebalancing and multi-region failover.
  Partition IDs exist but do not currently limit claim ownership.
- Full policy administration UI or versioned policy migrations.

The next milestone is a remote execution adapter and transactional
completion outbox; only then can all orchestrator workers be DB-free.

## Verification

PostgreSQL integration tests cover bounded concurrency, distinct resource
keys, strict FIFO, retry blocking, idempotency, lease takeover and stale
epoch rejection. CI also boots a separate Hub gateway and DB-free worker,
submits activities via REST, observes per-key serialization and verifies
completion across the SSE dispatch path.

## 1,000 concurrent workflow benchmark

The Hub now contains an experimental, code-first multi-step remote workflow
chain with atomic step progression, durable results and history. It is **not**
a full adapter for the legacy `IWorkflow<T>` runtime.

Two GitHub CI runs staged 1,000 workflows before starting workers and
completed 3,000 remote steps with zero workflow failures. See
[full reproducible performance results](workflow-hub-1000-performance.md).

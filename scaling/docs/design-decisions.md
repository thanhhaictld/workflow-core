# Architecture decision records (ADRs)

Each decision reflects **implemented behavior** at the documented baseline,
not a claim that the option is optimal for every workload.

## ADR-001 — PostgreSQL is both durable execution state and queue

**Status:** accepted (current implementation).  
**Decision:** use `wfc` EF records plus `wfc_scale.work_item` to atomically
commit execution state and follow-up work. Workers select rows with
`FOR UPDATE SKIP LOCKED`.  
**Alternatives:** RabbitMQ / Azure Service Bus with transactional outbox, Redis
streams, Temporal matching service.  
**Rationale:** avoids dual-write gaps and a separate broker deployment while
scaling beyond single-node in-memory dispatch.  
**Tradeoff:** one PostgreSQL primary is both database and work broker;
long-lived history and heavy polling can stress connections, autovacuum,
WAL and indexes. Dedicated broker is a future measured optimization, not
present today.

## ADR-002 — Lease + epoch + state revision

**Status:** accepted.  
**Decision:** work item and workflow gate own independent leases; workflow
gate `epoch` fences stale owners, `revision` fences stale state commits.  
**Alternatives:** process-local mutex, Redis lock, advisory lock alone,
optimistic concurrency only.  
**Tradeoff:** two claims and a heartbeat, more round trips, and non-cooperative
external steps may continue running after lease loss. State writes are
conditional; remote effects still need idempotency.

## ADR-003 — At-least-once with application-level idempotency

**Status:** accepted.  
**Decision:** work can be retried and reclaimed; optional request keys dedupe
workflow/event creation.  
**Alternatives:** pretend exactly-once execution; distributed transactions
with every third-party API.  
**Consequence:** external provisioning commands need stable per-operation
idempotency keys and read/reconcile logic after uncertain timeouts.

## ADR-004 — Reuse Workflow Core executor, replace host scheduling

**Status:** accepted.  
**Decision:** keep workflow definitions, step DSL and execution pointer
semantics; launch through `DistributedWorkflowEngine`, not old
`IWorkflowHost.StartWorkflow`/pollers.  
**Consequence:** legacy host compatibility and mixed-runtime operation are
not supported; consumer side effects such as legacy index updates and
lifecycle hub notifications are not part of the new path.

## ADR-005 — Combined worker/scheduler loop for initial version

**Status:** accepted as implementation detail, may be revisited.  
**Decision:** every replica runs multiple slots that directly claim due rows.  
**Alternatives:** centrally partitioned scheduler plus independent broker
workers.  
**Tradeoff:** low operational complexity but query pressure grows with
replicas; no virtual partitions, leader election or partition rebalancer.

## ADR-006 — Per-subscription event delivery markers

**Status:** accepted, additional verification required.  
**Decision:** on delivery, atomically update workflow pointers, delete
subscriptions, write `signal_delivery`, and enqueue workflow wakeup;
use replay intents for late `WaitFor` subscriptions.  
**Tradeoff:** fan-out is several transactions; older-signal checks and
concurrent subscriber registration require deeper chaos tests. Event
ordering and subscriber semantics are **not yet production-certified**.

## ADR-007 — .NET 10 only, no backwards-compatibility promise

**Status:** accepted by requirement.  
**Decision:** the new runtime targets net10.0; the old projects remain as
dependencies but the new public workflow submission surface is different.
Retaining the old workflows API as a shim would risk bypassing fencing.  
**Tradeoff:** consumers must explicitly migrate to the new API.

## Reconsideration triggers

Revisit the decisions if the measured target fails under PostgreSQL CPU/WAL
pressure, the workload requires independent scheduler isolation, strict
cross-region RPO/RTO, tenant isolation, external-broker operations, or the
supported ordering guarantee cannot be demonstrated in failure testing.

See [Architecture](architecture.md), [Persistence](persistence.md), and
[Production readiness](production-readiness.md).

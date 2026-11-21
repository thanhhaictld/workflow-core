# Parallel candidate discovery and fenced admission (MVP)

See [multi-Hub matching and async SSE delivery](hub-multi-hub-sse-admission-mvp.md) for an end-to-end architecture diagram, current REST/SSE endpoints, and failover sequence.


Base: `thanhhaictld/hub-memory-candidates-luna`.
This branch deliberately breaks compatibility with older task writers/policy registrations.
It **does not claim** that 1,000 activity starts/s have been demonstrated.

## Design

1. A Hub instance owns matching shards and receives worker credits as before.
2. Each shard has at most one active discovery turn, but the gateway now runs up
   to four independent turns concurrently. Global candidate/credit memory limits
   and reservations remain guarded by the registry lock.
3. An admission takes `FOR SHARE OF o` on its matching-owner generation row.
   Claims against independent task IDs can commit concurrently on the same shard.
   Renewal, release and takeover UPDATE the owner row, so they wait for any
   outstanding admission transaction's shared locks. The final live-owner
   deadline/epoch check is still required *before commit*.
4. The gateway defaults to bounded concurrent admission, two admission slots
   per shard (and two total per Hub unless separately configured).
   Each worker demand has one in-flight admission at a time, so a confirmed
   batch can return all of its leases in the demand's single response.
   Claims still observe the PostgreSQL claim-admission gate and connection pool.
5. Only tasks with persisted `admission=1` and no
   `hub_task_constraint` row are eligible for the fast batch-by-ID path.
   This is checked again under PostgreSQL task-row locks, not only in the matcher.
   Every supported MVP writer inserts `admission=1` explicitly.
6. The candidate reader excludes other admission classes to avoid filling the
   hot cache with tasks the MVP matcher cannot currently serve.
   Durable notification-loss recovery and lease reconciliation remain in place.

## Supported workload and breaking changes

The MVP supports *unordered, unconstrained* task leases and native/remote
workflow tasks that use the new writer contract. The previous branch had already
retired new Hub-managed business-key policies. This experiment additionally
treats legacy constrained/unknown work as **unsupported**: it is not allowed to
enter an unconstrained lease transition.

Migration `034_hub_task_admission.sql` is updated in-place for the MVP:
default classification is 1; existing rows carrying persisted constraints are
marked 0; existing unknown rows without constraints become 1. Adding a
constraint to an unconstrained row invalidates the descriptor via trigger.
There is no backwards-compatible mixed-writer rollout. Drain/reset older
policy-dependent queues and deploy only the new writers. This classification
is **not** a replacement for resource-aware permitting if partition-serial or
quota semantics are reintroduced later.

Do not remove epoch checks, owner-generation locking, ambiguous-commit
reconciliation, or durable lease history to improve a benchmark.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `WFC_HUB_CANDIDATE_DISCOVERY_PARALLELISM` | 4 | Independent discovery turns per gateway activation (1–16) |
| `WFC_HUB_CANDIDATE_CONCURRENT_ADMISSION` | true | Parallel admission on/off comparison |
| `WFC_HUB_CANDIDATE_ADMISSION_PER_PARTITION` | 2 | Max in-flight admissions per matching shard (1–8) |
| `WFC_HUB_CANDIDATE_ADMISSION_CONCURRENCY` | 2 | Total admission slots per Hub (1–32) |
| `WFC_HUB_CANDIDATE_BATCH_SIZE` | 32 | Maximum selected IDs per drain |
| `WFC_HUB_CANDIDATE_CROSS_WORKER_BATCH` | false | Optional cross-worker batch transaction |

For a controlled performance test, explicitly set cross-worker batching and
backpressure consistently in both arms; do not conflate toggle effects.
Exact-task-ID hints remain opt-in until their measured regressions are resolved.

## Correctness gates

- Two distinct task IDs can be claimed concurrently under one owner generation.
- The same task ID is claimed at most once across racing batches.
- Takeover/release cannot overtake a transaction holding the owner fence.
- Task lease history and epoch updates are atomic with the claim.
- A task with a persisted resource constraint never enters the fast path.
- A stopped or superseded discovery generation cannot publish candidates.
- Under concurrent discovery, per-shard cursors and memory budgets remain bounded.
- SSE worker-credit reservations survive ambiguous commits and are reconciled.

## Required controlled A/B

Run the existing candidate + SSE failover tests and 1,000-workflow/16-processor
stress workflow before adopting this branch. Do not interpret raw workflow
completion rate as independent activity starts/s.

Suggested controlled matrix (same workload, PostgreSQL, resource limits and
worker topology in every run):

| Arm | Discovery | Owner fence | Admission per shard | Cross-worker batch |
| --- | --- | --- | --- | --- |
| Baseline | global sequential | FOR UPDATE | 1 | same as candidate |
| New conservative | parallel 4 | FOR SHARE | 1 | same |
| New parallel | parallel 4 | FOR SHARE | 2 | same |
| Scalability | parallel 4–8 | FOR SHARE | 2–4 | same |

Record throughput, *activity schedule-to-start* p50/p95/p99 (measured at
actual worker start), workflow completion p95/p99, matching-owner lock wait,
admission duration, batch fill, DB connection wait, attempts not delivered via
SSE, and PostgreSQL write/commit/lock pressure. The current lease-to-delivery
proxy is not the same as actual worker-start latency.

**Known risk:** continuous shared owner locks can delay owner renewal. The
bounded admission limits and short transactions mitigate, but do not prove
absence of renewal starvation. Require owner failover/renewal tests at load.

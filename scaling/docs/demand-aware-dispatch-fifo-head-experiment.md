# Demand-aware dispatch and FIFO-head claim experiment

**Status:** Experimental, feature flags default OFF.  
**Base:** `perf/linear-processor-scaling-from-pr22` (PR #26).  
**Branch:** `perf/demand-aware-claim-admission-from-pr26`.

## Decision and scope

Optimize throughput without changing the correctness authority or introducing processor-owned workflow partitions. PostgreSQL remains the source of truth for durable tasks, lease epochs, workflow revisions, resource capacities, FIFO predecessors and execution history. Remote processors remain DB-free and keep the existing REST long-poll, heartbeat and completion protocol. The Hub scheduler continues to own durable timer wakeups.

**This increment does not implement a push dispatcher, worker capacity credits, processor partition ownership, or a new database claim budget.** Those remain separate candidates that require measurements and crash/recovery design. Existing admission throttling (`WFC_HUB_CLAIM_ADMISSION_MODE=demand`) is exercised in the new benchmark on both control and experiment arms, not silently enabled in production.

## Implemented isolated changes

### 1. Capability-scoped event-gated claims

Flag: `WFC_HUB_MATCHING_SCOPED_CLAIMS=true` on every Hub matching replica (default OFF).

Previously, a committed typed matching hint woke a compatible waiter but its SQL claim used every capability in the worker's request. On a mixed-capability processor that could scan and lease unrelated tasks while the hinted capability stayed pending.

With this flag the event-gated dispatcher narrows the claim request to the signaled `(task_type,version)`. Generic hints (permit release / recovery) and the legacy SSE/adaptive path continue to claim using the full capability set. This changes SQL filtering only; the response still contains PostgreSQL-committed leases. The dispatcher retains hints on admission deferral, keeps single-flight per matching shard, and does not assume NOTIFY guarantees ownership or delivery.

Safety: no change to `HubTaskStore.ClaimAsync` lease checks; task payload, workflow version pinning, resource permits and task fencing remain server-side. Tests verify typed-versus-generic claim scope.

### 2. PostgreSQL strict-FIFO head prefilter

Flag: `WFC_HUB_FIFO_HEAD_PREFILTER_ENABLED=true` on every Hub (default OFF).

The legacy constrained claim path queries at most 96 ordered candidate tasks. In a hot FIFO resource, 96 blocked successors can hide a runnable task for another tenant/type. The new optional candidate SQL excludes strict-FIFO successors whose resource still has an earlier task in state READY or LEASED, **before** applying the candidate limit. The original task-row lock, resource-row locks, capacity check and FIFO predecessor recheck continue to decide the actual lease.

This is a performance experiment, not a promise of improvement. The additional correlated SQL joins can increase candidate-query CPU under large backlogs; inspect `EXPLAIN (ANALYZE, BUFFERS)`, `pg_stat_statements` and the existing `workflow_hub_claim_candidate_query_seconds` histogram. A transactional snapshot can reject an otherwise newly available successor until a later claim; that is safe because the locked eligibility check remains authoritative. The existing 2-second readiness and 10-second event-gated recovery scans remain in place.

Tests cover a 105-task same-tenant blocked backlog followed by a runnable independent task, continued FIFO order after completion, and concurrency across different resource keys.

## Benchmark and acceptance

`.github/workflows/workflow-hub-dispatch-admission-ab.yml` uses the existing two-Hub, four-matching-partition topology, 12 pooled DB connections per Hub, matching long polls, demand-mode admission and **the same enabled unconstrained batch fast path in all variants**.

| Variant | Scoped hints | FIFO-head SQL |
|---|---|---|
| baseline | OFF | OFF |
| scoped | ON | OFF |
| fifo | OFF | ON |
| combined | ON | ON |

Each tests 1, 2, 4, 8 and 16 processor instances, with unordered, partition-FIFO and keyed-FIFO workloads at the same workflow count. The 1-processor cohort is mandatory for the scaling summarizer; the original benchmark failed at report generation when it ran only 8 and 16 processors (all 24 k6 workload cohorts had completed successfully). It archives k6 workflow summaries, order verifications, PostgreSQL wait samples and the scaling report. GitHub shared runners are useful for regression comparison but **not** evidence of linear scaling or production capacity. The first 8/16-only run showed mixed results and occasional regressions; do not infer an improvement before repeated controlled comparisons.

Promote each flag only after multiple controlled runs confirm no meaningful throughput/p95/p99 regression, fewer expensive unsuccessful scans or claims, no unexpected retries, ordering verification, stable database waits and acceptable connection/WAL pressure. Run a 1/2/4/8/16/32 scaling curve on dedicated resources before concluding processor scaling is linear.

### Safety and rollback

- No schema migration, lease-contract or processing-order change.
- Rollback: set both flags OFF on **all** Hub replicas and restart them.
- Keep `WFC_HUB_BATCH_UNCONSTRAINED_CLAIMS` and claim-admission settings fixed across A/B comparisons; do not mix old/new flag values on Hub replicas during a performance run.
- Verify the existing 1,000-workflow runtime suite, PostgreSQL integration tests, and Hub matching HA crash/takeover tests.
- Activity side effects are at-least-once; external systems still need idempotency/fencing.
- No benchmark result or CI validation is claimed by this document until the runs complete.

## Deferred alternatives and go/no-go criteria

1. Capacity-credit matching across shards (no rotating single long-poll) may reduce idle processor slots; pursue if idle capacity persists while runnable backlog exists.
2. Shard-level concurrency expansion and per-capability claim lanes may remove the single-flight ceiling; pursue only after quantifying SQL service time, admission wait and completion starvation.
3. Processor-affine ownership is justified only if state locality dominates the optimized matching path and a safe rebalance/failover protocol is designed.
4. For persistently hot FIFO keys, consider an indexed durable resource-head structure after measuring the cost of the correlated prefilter. Never substitute in-memory eligibility for transactionally enforced FIFO.

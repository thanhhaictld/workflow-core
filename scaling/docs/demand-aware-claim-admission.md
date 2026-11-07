# Demand-aware claim admission and bounded remote workers

Status: **experimental / opt-in** (PR #26, based on PR #22).

## Motivation

The previous four-core shared-runner benchmark with 16 processor replicas
recorded Prometheus histogram estimates of **2.46–4.78 seconds for end-to-end
claim p99**, mostly from waiting for the Hub's claim-admission semaphore.
These are **not PostgreSQL query p99** and do not establish a production SLO.

`HubTaskStore` shares one Npgsql pool with task completions, failure reporting,
heartbeats and workflow transitions. A large backlog of speculative SQL claims
should not exhaust that pool or prevent already-executed work from committing.

## Changes

The existing claim SQL, PostgreSQL fencing epochs, lease state transitions,
resource capacities, FIFO order, attempt history and work-ready notifications
are **unchanged**.

1. **Configurable admission capacity**: active speculative claims are bounded
   by `WFC_HUB_CLAIM_ADMISSION_LIMIT` and always clamped below the pool size
   so normally **at least four Npgsql connections remain unreserved** for
   completions and other operations. This is not a strict pool partition:
   unrelated operations may also acquire the remaining slots.
2. **Demand-aware admission** (opt-in): only the event-gated matching
   dispatcher can request a deferrable claim. If an active permit is not
   immediately available, at most
   `WFC_HUB_CLAIM_ADMISSION_MAX_WAITERS` such requests queue for it.
   Additional requests throw a private transient deferral signal *before*
   opening any PostgreSQL connection or leasing a task.
3. **Never discard readiness on defer/cancel**: the dispatcher retains the
   claimed hint on unsuccessful/incomplete claims, releases its single-flight
   shard slot, waits a jittered 60–99 ms when deferred, and wakes a compatible
   waiter. A poll timeout returns no work **without** claiming or dropping the
   hint; a later poll or periodic capability-scoped catch-up can drain backlog.
   A completed empty query still consumes its hint.
4. **Bounded worker demand** (independent opt-in): a processor can cap both
   workflow and activity concurrency *per lane*, **before** posting a claim.
   A limit of two allows up to two inflight workflow operations and two
   activity operations per processor, not two globally. No lease is acquired
   and then left waiting for a local semaphore.
5. **Tail-latency guardrails**: preserve histogram buckets for claim p99,
   admission wait p99, worker-observed HTTP poll p99 (which includes deferral),
   matching long-poll p99, and completion HTTP p99. Do not declare a speedup
   based solely on internal claim p99: latency might have moved upstream.

### Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `WFC_HUB_CLAIM_ADMISSION_ENABLED` | `false` in base Compose | Legacy semaphore admission switch; demand mode enables it regardless |
| `WFC_HUB_CLAIM_ADMISSION_MODE` | `queue` | `queue`: previous wait-for-permit path. `demand`: bounded queue and defer for event-matched claims |
| `WFC_HUB_CLAIM_ADMISSION_LIMIT` | `max(1, pool/4)` in library; `3` in Compose | Max simultaneous SQL claim calls per Hub process. Clamped to `pool - min(4, max(1,pool - 1))` |
| `WFC_HUB_CLAIM_ADMISSION_MAX_WAITERS` | `1` | Max matching requests waiting for active claim SQL (0–128). Others defer in memory |
| `WFC_HUB_WORKER_MAX_INFLIGHT_PER_LANE` | `0` | No extra cap when zero. Otherwise clamp the already-configured workflow/activity lane limits to this per-processor value |
| `WFC_HUB_DB_POOL` | `12` in Compose | Connection pool shared by claims and completion operations |

**Legacy compatibility:** direct `/workers/tasks:claim` and the older non-event-
gated matching path preserve their normal claim behavior, even when demand
mode is set. This experiment optimizes only `WFC_HUB_MATCHING_EVENT_GATED=true`
matching. No REST schema changes.

## A/B benchmark and verification

The workflow `.github/workflows/workflow-hub-admission-ab.yml` compares:

- Admission: `queue` vs `demand`
- Processor worker cap: unlimited (`0`) vs `2` per lane
- Unconstrained SQL batching: OFF vs ON
- 16 processor replicas, two Hub replicas, four matching shards, PostgreSQL
  with pg_stat_statements, and **two 300-workflow trials** per matrix cell

The full factorial design has **8 cells × 2 runs = 16 verified scenarios**;
the optimized demand+cap=2 variants also verify keyed FIFO, no overlap,
all attempts closed, and no unwanted replays.

Benchmark output includes PostgreSQL statement times, lock/wait samples,
Docker CPU, completed-workflow throughput, workflow p95/p99, claim p99,
admission p99, worker HTTP poll p99, matching poll p99, completion HTTP p99
and claim deferral totals. Per-trial metrics come from Prometheus histogram
**bucket differences**; percentiles are interpolated estimates, not exact
samples. Claims/worker p99 are different latency distributions and must not
be added together.

CI is hosted on a **shared four-core runner**: it can identify regressions
and hypotheses, but throughput improvements require repeated runs on
**isolated worker, Hub and PostgreSQL CPU pools** under steady-state load.
Only enable the optimization outside the CI environment after completion
p99, correctness and throughput have been evaluated together.

## Rollout and rollback

Start with `WFC_HUB_CLAIM_ADMISSION_MODE=queue` and
`WFC_HUB_WORKER_MAX_INFLIGHT_PER_LANE=0` (current behavior). Test `demand`
with `WFC_HUB_CLAIM_ADMISSION_MAX_WAITERS=1` and a nonzero worker cap.
Watch:

- `workflow_hub_claim_admission_deferred_total`: frequent spikes indicate
  demand above the DB claim budget
- `workflow_hub_claim_admission_wait_seconds_bucket`: in-Hub SQL permit queue
- `workflow_hub_worker_claim_http_duration_seconds_bucket`: **end-to-end**
  worker claim latency, including time spent deferring
- `workflow_hub_worker_completion_http_duration_seconds_bucket`: latency of
  committing already-executed activity results
- `workflow_hub_matching_poll_duration_seconds_bucket`: server matching
  queue/wakeup latency
- `workflow_hub_claim_db_connection_acquire_seconds_bucket`: Npgsql pool wait
- `workflow_hub_tasks_completed_total` and durable verifier failure count:
  completion progress and correctness

Rollback is immediate using only configuration: `MODE=queue` and
`WORKER_MAX_INFLIGHT_PER_LANE=0`. No migration or durable state rollback
is necessary. Do not change lease/fencing or resource ordering to tune this
experiment.

## Known limitations

- The matching dispatcher still permits one SQL drain at a time per Hub/shard.
- The capped worker budget is **per process and per lane**, not a coordinated
  cluster-wide execution limit.
- A PostgreSQL pool-reservation bound does not guarantee the application will
  have free connections when other non-claim services consume them.
- Deferring SQL can make internal claim p99 look better while user-observed
  worker poll p99 grows. Both metrics must be evaluated before enabling.
- The benchmark's short burst workload and shared CI runner are unsuitable
  for production-capacity claims.

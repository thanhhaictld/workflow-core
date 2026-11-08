# Workflow Hub p99: OTLP/Tempo root-cause investigation (2026-11-06)

**Source:** [Passing 100-workflow CI run 42202860247](https://github.com/thanhhaictld/workflow-core/actions/runs/42202860247), [raw telemetry artifact 13495606331](https://github.com/thanhhaictld/workflow-core/actions/runs/42202860247/artifacts/13495606331), [PR #17](https://github.com/thanhhaictld/workflow-core/pull/17).

## Controlled experiment

- **600 actual workflows:** 100 per policy `unordered`, `partition FIFO`, and `keyed FIFO` at 8 and 16 processor containers.
- 75ms synthetic activity; eight independent resource-permit scopes for constrained modes.
- All Hub and processor services share one **4-vCPU** GitHub runner, Hub's Npgsql pool defaults to **12 connections**.
- **600 completed / 0 failures / 0 activity retries / 0 FIFO lease overlap violations**.
- Archived actual OTLP JSON traces from the Collector alongside Tempo, with 60/60 selected slow workflows correlated by `workflow.id`, and PostgreSQL wait snapshots. This is not a 16-host horizontal scale test.
- Source code changes are **only timing instrumentation and benchmark plumbing**. No task-claim / FIFO / fencing behavior changes.

## Main discovery: excessive claim pressure

From the **final archived OTLP JSON file** (the in-job summary was collected before the last OTLP flush, so it contains slightly fewer spans):

| Instrument | Observed |
|---|---:|
| Hub `hub.task.claim` spans | **13,263** |
| Claimed task leases across six runs | **2,400** |
| Empty claims (`tasks.claimed=0`) | **11,562 = 87.2%** |
| Permit-check span events | **38,637** |
| Candidate-list selection events | **14,659** |
| `hub.task.claim` p99 | ~**1.05 seconds** (in-job aggregate) |
| DB connection-acquire p50 / p95 / p99, all claim calls | **32 ms / 402 ms / 652 ms** |
| Candidate-query p95 / p99 | **23 ms / 40 ms** |
| Permit-check event p95 / p99 | **15 ms / 32 ms** |

A large majority of claim requests return no work while still competing for DB connections. The existing `ClaimAsync` executes an initial candidate scan of up to 96 and checks individual candidates in transactions. This observed amplification directly supports prioritizing **poll frequency, claim fast paths, work hints, DB pool connection acquire timing and admission efficiency**.

**Causality caveat:** the trace data demonstrates high claim frequency and expensive connection acquisition, but cannot yet distinguish client-side pool queue time from network/physical connection setup, or quantify how much each contributes to end-to-end workflow p99 without an A/B intervention.

## Activity execution vs HTTP completion

Per-policy duration percentiles are computed from the raw OTLP trace archive. Each stage has **100 activity executions**, matched by `workflow.id` and `activity.type`; the two 100-workflow stages are sequential and ordered by activity start time. `body` includes worker activity setup and async step execution; `completion HTTP` includes Hub transport and server-side processing.

| Policy (processors) | Worker body p50 | Completion HTTP p50 | Completion HTTP p99 | Hub completion DB acquire p50 | Hub workflow-row lock p99 |
|---|---:|---:|---:|---:|---:|
| Unordered (8) | 158ms | 1,119ms | 1,656ms | 186ms | 33ms |
| Unordered (16) | 134ms | **3,330ms** | **3,671ms** | 617ms | 56ms |
| Partition FIFO (8) | 97ms | 58ms | 1,311ms | 0ms | 8ms |
| Partition FIFO (16) | 97ms | **1,004ms** | **2,825ms** | 168ms | **12ms** |
| Keyed FIFO (8) | 97ms | 96ms | 865ms | 7ms | 12ms |
| Keyed FIFO (16) | 98ms | **957ms** | **2,396ms** | 151ms | **11ms** |

**Most useful observation:** the synthetic activity body is near 100ms in FIFO runs regardless of processor count, but completion ACK latency rises from ~60–100ms to ~1s (median) at 16 processors. FIFO permit occupancy lasts until the completion transaction, so longer ACK directly reduces the maximum permitted activity rate.

PostgreSQL *workflow row lock* p99 is only ~11–12ms for FIFO in this run. Simple pessimistic workflow-row lock contention is **not** enough to account for the multi-second HTTP completion tail. Hub connection acquisition alone rises to p50 ~150–170ms in FIFO, and there is substantial **additional HTTP/dispatch/server overhead** not yet stage-attributed. Sparse `pg_stat_activity` samples did record transaction-ID and tuple waits; they do not prove these cause the p99.

## Workflow-level p99 (this OTLP-instrumented run)

| Policy | 8 processors | 16 processors | 16 processor activity-queue p99 |
|---|---:|---:|---:|
| Unordered | 11.964s | 12.899s | 0.514s |
| Partition FIFO | 15.243s | 19.768s | **14.475s** |
| Keyed FIFO | 10.240s | 17.345s | **12.565s** |

This instrumented run has higher absolute overhead than the previous uninstrumented 100-workflow benchmark. Compare **within** this run; tracing overhead and single-run noise prevent exact cross-run attribution. Each p99 stage comes from its own distribution and must not be summed across workflows.

## Root-cause assessment

1. **Confirmed:** FIFO queue p99 is inflated by long admission waits. Existing lease permits remain active through worker completion/Hub ACK, not only the 75ms business body.
2. **Confirmed:** At 16 processors, completion HTTP median rises to ~1 second for FIFO, despite a ~100ms body.
3. **Confirmed:** 87.2% of claim RPCs return no tasks; claim DB acquisition p99 ~652ms, revealing substantial claim-path overhead and contention.
4. **Confirmed:** Hub workflow-row lock duration is much smaller than the overall completion delay in FIFO; changing pessimistic row locking to optimistic alone is **not supported** as first fix.
5. **Not proven:** exact source of extra HTTP completion time (Hub request scheduling, database pool waiting, ASP.NET request backlog, 4-vCPU CPU starvation, or their combination). No separate physical hosts were tested.
6. **Not supported:** task fencing lease timeout as the primary source. No retries or takeover observed in the diagnostic tests.

## Recommended A/B experiments

**First**, introduce diagnostic load-shedding and measurement around empty claims: bounded poll frequency/jitter, claim request counters (empty, candidate scan, admitted), and explicit Npgsql data-source pool waiting. Compare current vs throttled claims with *identical* 100-workflow/8-16 processor loads.

**Second**, separate completion processing into HTTP receive → handler entry → pool-acquire → SQL/commit → HTTP response finish and benchmark with both 12 and 24 pool slots. Avoid blindly enlarging the pool; higher DB CPU/lock contention may worsen tails.

**Third**, run a proper horizontal scaling experiment with independent worker compute resources and dedicated Hub/PostgreSQL capacity, controlling input arrival, CPU, and memory.

Preserve resource permits, FIFO ordering and lease fencing during all of the above. A batched unordered `SKIP LOCKED` claim optimization is reasonable only after isolation A/B proves it improves throughput.

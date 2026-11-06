# Hub p95/p99 optimization roadmap after PR #18

## Baseline, scope and caution

Base branch: `perf/p95-adaptive-claim-completion` (PR #18, commit `8bb2a58`).
All experiments in this series MUST keep the three PR #18 flags enabled:
`WFC_HUB_ADAPTIVE_CLAIMS=true`, `WFC_HUB_CLAIM_ADMISSION_ENABLED=true`,
`WFC_HUB_COMPLETION_FAST_PATH_ENABLED=true`. The pre-PR #18 baseline is no longer the relevant control.

PR #18's one-run A/B evidence: empty claim RPCs 11,142 -> 823; completion HTTP
p95 1.824 s -> 0.165 s; 16-processor partition FIFO workflow p95
12.787 s -> 7.408 s, keyed FIFO 10.710 s -> 5.497 s. Those are
measurements from separate shared-CPU CI runners, not production SLOs.

## Phase 1 (this PR): one candidate scan per claim RPC

The previous `HubTaskStore.ClaimAsync` query selected up to 96 ordered task IDs
**for every successfully leased task**, then attempted per-task row/permit locks.
When a client requests 16 tasks, this can repeat the same initial candidate
query up to 16 times. That is an unnecessary database read multiplier.

Behind `WFC_HUB_CLAIM_SINGLE_SCAN_ENABLED=true`, cache the first candidate
result set for one claim RPC, advance an in-request cursor across it and
never revisit an ID in that RPC. Keep all existing fenced lease transactions,
attempt journal entries, resource-lock ordering and strict FIFO permit checks.
The default remains **off** until A/B measurements demonstrate improvement.

Benefits to test: fewer candidate queries per claim, shorter claim connection
occupancy, less pressure on Hub completion transactions, improved p95/p99 at 16
processors. The single-scan path **is not** a batch SQL claim; it still
uses a per-candidate transaction. Do not claim a performance win without results.

Caveats:
- Only the first 96 candidates are considered in a single RPC. Existing
  head-of-line blocking at the front of that set remains; it must be addressed
  by a separately tested paged/partition-aware selector, not by silently
  bypassing FIFO constraints.
- A fresh candidate appearing after the snapshot will be picked up by the
  next claim/reconciliation cycle. The original repeated-query path is
  preserved as a rollback.
- Candidate scan count now represents one snapshot rather than repeated reads.
  Compare returned leases, database-acquire p95/p99 and completion latency, not
  only this new count.
- Shared GitHub-hosted runner tests show contention, not independent host
  scaling or production durability proof.

## Controlled A/B

`.github/workflows/workflow-hub-p99-diagnostic.yml` compares:
- `optimized`: all PR #18 flags on; single scan off
- `single-scan`: all PR #18 flags on; single scan on

Each job runs 100 actual workflows for unordered / partition FIFO / keyed FIFO,
at both 8 and 16 processor containers, with the same OTLP tracing and persisted
workflow timeline. Compare workflow completion p50/p95/p99, activity queue
p95/p99, completion HTTP p95/p99, admitted tasks per claim, claim query duration,
claim admission wait, DB connection acquisition wait and throughput.

Gate on 600/600 complete, zero unexpected retries, valid attempt history,
no FIFO overlap and no stale lease completing after takeover. Repeat each
variant at least three times for median and spread before promoting the flag.
A reduction in claim latency at the cost of FIFO workflow p99, starvation,
or completion correctness is a regression.

## Remaining roadmap (not implemented in this PR)

1. **Attribution:** isolate adaptive polling, claim admission and fast completion
   independently if the PR #18 trace distributions remain noisy. Measure
   admission queue wait separately from SQL elapsed and real connection-pool
   waiting. Compare at constant work arrival and workload size.
2. **Unconstrained fast path:** implement feature-flagged, atomic batch
   `FOR UPDATE SKIP LOCKED` claims only where no applicable
   `hub_task_constraint` exists. Preserve workflow task attempt journals and
   fencing epochs within the same transaction. Keep resource-permit/FIFO
   claims on their current admission path until correctness tests pass.
3. **Avoid head-of-line scans:** keyset page candidate discovery or
   partition-aware ready queues. Never remove a strict FIFO predecessor test.
   Bound DB work per RPC and ensure new tasks and lease expiries remain visible.
4. **Completion isolation:** if connection-acquire and ASP.NET request backlog
   remain high, test separate budgeted Npgsql data sources for claim and
   completion on the same PostgreSQL primary. Benchmark 12 total pooled
   connections vs controlled budgets, not larger pools by default.
5. **Topology:** repeat 1/4/8/16 worker process tests with Hub/PostgreSQL
   on dedicated compute; report offered rate, DB CPU/WAL, queue age and
   scaling efficiency. Do not compare heterogeneous Temporal/ELSA datasets
   as a claim of engine superiority.

## Release decision

Do not merge the optimization solely because claim RPC latency falls.
Promote it only when workflow p95/p99 and throughput are stable or better
under identical loads, and FIFO/fencing correctness and crash recovery hold.

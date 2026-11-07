# Workflow Hub throughput investigation — summary

**Status:** Investigation complete through FIFO prefilter A/B; root cause of keyed-FIFO regression **not yet confirmed**.  
**Scope:** [PR #27](https://github.com/thanhhaictld/workflow-core/pull/27) · two Hubs, four matching shards, PostgreSQL 17, 75 ms synthetic activity, 200 workflows/cohort, shared **4-vCPU** GitHub runners.  
**Decision:** Keep PR in **draft** and experimental flags **OFF**. Passing correctness tests does **not** prove performance gains.

## Findings

| Question | Evidence | Assessment |
|---|---|---|
| Does adding processors improve throughput? | Mixed-Hub unordered 8→16→8: **9.15→5.22→9.87 WF/s**; 16-processor host CPU ~99.6%; DB commits/workflow **45.5→60.7→47.3**. [Run](https://github.com/thanhhaictld/workflow-core/actions/runs/42376571948) | **No** for this workload/runner. Resource contention and coordination overhead are credible; not evidence against multi-host scaling. |
| Is matching ownership SQL the bottleneck? | At 16 processors, ownership statements totaled **0.10–0.19 s** versus task queue **1.7–11.0 s** and policy/resource SQL **7.2–17.2 s** per 200-workflow cohort. [SQL/CPU profile](https://github.com/thanhhaictld/workflow-core/actions/runs/42383367632) | **Unlikely** dominant SQL cost in this test. Deferred Gateway ownership check did not show a reliable throughput gain. |
| Does the sample test the real matching path? | Mixed-Hub workflow lane uses adaptive SSE/claim fallback; opt-in **policy-only** processor exercises matching on both workflow and activity lanes. [Run](https://github.com/thanhhaictld/workflow-core/actions/runs/42376571948) | Tested both. Policy-only matching **did not** solve scaling. |
| Does FIFO-head prefilter help? | **Keyed FIFO** 8 processors, OFF→ON→OFF: **9.57→5.17→9.55 WF/s**; p95 **20.36→34.14→20.34 s**. Policy/resource SQL calls/workflow fell **~69→14**. [FIFO A/B](https://github.com/thanhhaictld/workflow-core/actions/runs/42387790626) | **Regression despite fewer SQL calls**: −46% throughput and ~68% worse p95 with stable OFF controls (−0.2% drift). Do **not** enable globally. |
| Does partition FIFO benefit? | OFF→ON→OFF: **6.98→11.79→10.37 WF/s**; OFF control drift **+48.6%**. [FIFO A/B](https://github.com/thanhhaictld/workflow-core/actions/runs/42387790626) | **Inconclusive**; apparent ON improvement cannot be separated reliably from drift. |

**What the measurements suggest (not proven):** Keyed FIFO prefilter ON lowered sampled processor CPU (~0.66→0.26 cores) and PostgreSQL CPU (~0.60→0.30 cores), while empty matching drains increased (~360→479). The system may be **waiting to hand off newly eligible keyed tasks** rather than consuming excessive DB CPU. Candidate-window queries also became costlier. We have not measured the exact critical-path gap.

## Implementation and safety

- Opt-in flags: `WFC_HUB_MATCHING_SCOPED_CLAIMS`, `WFC_HUB_FIFO_HEAD_PREFILTER_ENABLED`, `WFC_HUB_MATCHING_DEFER_OWNER_CHECK` — **all default OFF**.
- Added single-Hub policy test mode, 8→16→8 capacity tests, per-container CPU / `pg_stat_statements` profiling, and FIFO OFF→ON→OFF benchmarking. PostgreSQL query profiling uses a **diagnostic-only** Compose overlay.
- Correctness / matching HA / ordering CI passed for the reviewed runs; persisted PostgreSQL task lease epochs, resource permits, and FIFO authority were **not changed**.
- Limitations: shared 4-vCPU runners; tests have runner/cache/history drift; SQL statement execution wall time is **not CPU time**; 75 ms synthetic activity stresses orchestration, not real provisioning.

## Next investigation — one focused question

**Measure keyed-FIFO handoff latency** from completion of the previous leased activity to claim of the next ready activity on the same resource key, using persisted task attempts and constraints. Compare **OFF→ON→OFF** p50/p95/p99 and fraction of gaps >1 s; correlate with committed release notifications, matching hint processing, and empty claim drains. Distinguish **(a) task not yet eligible, (b) readiness signal not reaching a waiter, (c) SQL claim deferred/empty, (d) worker unavailable**.

If handoff lag explains the regression, optimize readiness/dispatch **without bypassing PostgreSQL FIFO/lease fencing**. Otherwise inspect parameterized `EXPLAIN (ANALYZE, BUFFERS)` for expensive candidate queries. Only then repeat scaling on isolated Hub/Postgres/processor resources before promoting flags.

**Evidence and details:** [Processor scaling](processor-scaling-root-cause-diagnostics.md) · [SQL attribution and FIFO A/B](sql-cpu-statement-investigation.md) · [Design and flags](demand-aware-dispatch-fifo-head-experiment.md).

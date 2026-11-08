# Workflow throughput bottleneck: PostgreSQL statement and container CPU attribution

**Status:** Experimental investigation; production defaults unchanged.  
**PR:** [#27](https://github.com/thanhhaictld/workflow-core/pull/27).  
**Workflow:** `.github/workflows/workflow-hub-sql-cost-attribution.yml`.

## Reason for this investigation

Earlier single-run benchmarks on GitHub-hosted **4-vCPU** runners showed that unordered workflow throughput often regressed when the processor fleet increased from 8 to 16. The bracketed 8→16→8 experiment reproduced a throughput collapse for mixed-Hub workers (9.15→5.22→9.87 workflows/second), with host CPU near 100% and more PostgreSQL transactions per workflow at 16 processors. This supports increased coordination overhead, but not whether CPU is spent by PostgreSQL, the Hub gateway, processors or the load driver.

A separate policy-only processor mode proved that the workflow lane normally falls back to adaptive SSE/claim when its registrations include two Hubs, and matching on both lanes alone did not solve throughput scaling.

The next question is: **Which SQL statements and which service consume the resources as processors are added?**

## Diagnostic-only additions

- `scaling/observability/compose.pg-statement-profile.yaml` enables PostgreSQL 17 `pg_stat_statements`, query IDs and I/O timing, and sets distinct Npgsql `Application Name` for the two Hubs. The overlay is **not enabled** in the normal Compose deployment.
- `.github/scripts/profile_workflow_bottleneck.py` snapshots normalized query fingerprints and cumulative execution/wait/WAL counters before and after each workload, samples container CPU cores via Docker, and pulls selected Hub claim/admission Prometheus timeseries.
- `scaling/observability/benchmark/summarize_statement_attribution.py` aggregates CPU by service and SQL statement family with cohort throughput and p95/p99. It marks high 8-control drift and never treats cumulative SQL execution time as CPU time.
- `.github/workflows/workflow-hub-sql-cost-attribution.yml` runs a **single policy-only matching baseline** with both speculative optimization flags and deferred owner check OFF, fixed Hub/DB pool settings, 8→16→8 processors, and all three ordering policies. Each cohort verifies complete workflow persistence, no extra attempts, and activity policy ordering.

### Source and security

Query fingerprints and broad table-family categories are recorded; **raw SQL text, task payloads, tenant IDs and database credentials are not exported**. Source-visible PostgreSQL 17 documentation: https://www.postgresql.org/docs/17/pgstatstatements.html .

The extension requires `shared_preload_libraries` (PostgreSQL restart) and `CREATE EXTENSION` in the benchmark database. CI enables it only in the opt-in overlay. Profiling incurs measurable overhead and should be compared against an identically profiled control, not against unprofiled production numbers.

## How to interpret the report

| Observation | Plausible next investigation |
|---|---|
| PostgreSQL CPU cores increase and SQL exec ms/workflow rise at 16 | Break down top query fingerprints and investigate `EXPLAIN (ANALYZE, BUFFERS)` / indexes / query cadence |
| Hub CPU cores dominate while PostgreSQL CPU stays low | Test matcher routing, serialization and request forwarding; inspect OTel spans |
| Processor CPU dominates | Worker execution concurrency and cross-container oversubscription, rather than SQL claim architecture |
| Matching owner SQL calls/workflow rises with idle processors | Consider eliminating repeated ownership-check requests or coalescing poll demand, while preserving lease fencing |
| SQL attempt/claim calls high but WAL bytes/workflow stay steady | Likely excess read/empty claim workload; measure claim occupancy and failures |
| p95 rises with low CPU consumption | Check DB waits, pool acquisition and FIFO permit contention, not just CPU |
| Eight-processor controls drift by at least 20% | Treat absolute throughput comparisons as unstable and repeat on isolated hardware |

The PostgreSQL counters are **per database** and include background traffic from scheduler, monitoring and matchmaking; SQL execution elapsed time across concurrent sessions is **not** CPU time. Docker stats are sampled and may perturb CPU-limited tests. Prometheus scrape intervals may miss short transients. No results from this CI should be represented as distributed linear-scaling validation.

## Safety and next gate

- No schema changes, leases, FIFO rules, retry semantics, or runtime feature-flag defaults changed.
- The experiment creates a PostgreSQL extension only in the disposable CI database.
- Keep PR #27 in draft until all mandatory correctness/HA gates pass, SQL cost evidence is collected, and repeated 8→16→8 comparisons are stable.
- A real multi-host capacity assessment still requires separate CPU reservations or hosts for PostgreSQL, Hub and workers. GitHub-hosted Compose containers **do not** provide that environment.


## Completed SQL/CPU profile: run 42383367632

The corrected baseline [run](https://github.com/thanhhaictld/workflow-core/actions/runs/42383367632) completed successfully: all 9 workload cohorts passed durable workflow completion and ordering validation. The remaining CI checks at that commit also passed.

### Throughput at 8 → 16 → 8 processors

| Policy | 8a WF/s | 16 WF/s | 8b WF/s | 16 / mean 8 | 8-control drift |
|---|---:|---:|---:|---:|---:|
| Unordered | 5.90 | 5.15 | 9.68 | 0.66x | +64.1% |
| Partition FIFO | 3.77 | 7.27 | 5.77 | 1.52x | +52.9% |
| Keyed FIFO | 5.43 | 10.46 | 5.91 | 1.84x | +8.8% |

These are single-host four-vCPU results with one shared PostgreSQL and two Hub containers. Unordered and partition controls drift >20%; no claim of throughput speedup or regression should be generalized from this run.

### SQL families at 16 processors (200 workflows per cohort)

| SQL group | Unordered wall seconds | Partition FIFO wall seconds | Keyed FIFO wall seconds |
|---|---:|---:|---:|
| Task queue | 10.97 | 1.73 | 1.91 |
| Policy / resource admission | 7.23 | 17.21 | 9.10 |
| Matching ownership | 0.19 | 0.13 | 0.10 |
| Bounded candidate window | 0.98 | 0.54 | 0.35 |

All times are cumulative PostgreSQL *statement execution wall time*, not CPU time or workflow latency. The `policy_admission` heuristic also matches statements involving constraint tables and should not automatically be interpreted as resource-lock waits. At 16-processor partition FIFO, fingerprint `-4541374265085359344` ran 4,384 times and accounted for 14.58 s in this broad group. Its exact SQL shape must be verified before optimizing or indexing. The repeated no-row task queue fingerprint `-1435339812113414142` is another suspect, especially for unordered work.

Average container CPU cores at 16 processors (sampled via Docker, subject to overhead): PostgreSQL 0.22 unordered / 0.38 partition / 0.60 keyed; Hub pair 0.25 / 0.28 / 0.32; processors 1.68 / 0.71 / 0.66. PostgreSQL **CPU saturation is not evidenced**. Claim/admission Prometheus series are collected but alone do not distinguish queue wait from end-to-end dispatch latency.

### Next falsifiable experiment

Run [workflow-hub-fifo-sql-ab.yml](../../.github/workflows/workflow-hub-fifo-sql-ab.yml) on the same runner with **8 processors** and feature-flag sequence OFF → ON → OFF. Verify partition and keyed FIFO ordering each time. Record per-policy throughput/p95, resource/permit SQL calls per workflow and SQL execution time, and compare the ON value to the average OFF control. Reuse the existing `WFC_HUB_FIFO_HEAD_PREFILTER_ENABLED` implementation; do **not** enable it by default yet. Watch for the previously observed 4-processor partition regression and query-plan overhead.

This experiment is intended to determine if filtering blocked FIFO successors reduces speculative resource-row checks; it does not address unordered no-row scans. A future plan/index experiment should capture `EXPLAIN (ANALYZE, BUFFERS)` for the expensive matching SQL under representative backlog and parameter values, rather than guessing an index.


## FIFO-head prefilter OFF → ON → OFF — completed

The dedicated [FIFO SQL A/B run 42387790626](https://github.com/thanhhaictld/workflow-core/actions/runs/42387790626) passed all six 200-workflow cohorts with durable ordering checks. The eight-processor topology and PostgreSQL database remained the same between cohorts; both Hubs were recreated between stages so the feature flag was applied consistently.

| Policy | OFF1 WF/s | ON WF/s | OFF2 WF/s | ON / mean OFF | OFF drift |
|---|---:|---:|---:|---:|---:|
| Partition FIFO | 6.98 | 11.79 | 10.37 | **1.36x** | **+48.6%** |
| Keyed FIFO | 9.57 | 5.17 | 9.55 | **0.54x** | **−0.2%** |

| Metric | Partition OFF1 / ON / OFF2 | Keyed OFF1 / ON / OFF2 |
|---|---|---|
| Workflow p95 (s) | 26.24 / 13.21 / 15.15 | **20.36 / 34.14 / 20.34** |
| Policy SQL calls per workflow | 64.57 / 13.57 / 64.16 | 69.19 / 13.64 / 68.61 |
| Policy SQL execution wall ms/workflow | 237.33 / 100.50 / 59.00 | 35.43 / 26.46 / 44.24 |
| Candidate-window SQL calls/workflow | 3.79 / 3.90 / 3.40 | 3.27 / 4.00 / 3.33 |
| Candidate-window query mean execution ms | 0.69 / 1.40 / 0.65 | **0.54 / 1.02 / 0.54** |
| PostgreSQL sampled CPU cores | 0.39 / 0.52 / 0.54 | 0.53 / 0.30 / 0.67 |
| Processor fleet sampled CPU cores | 0.87 / 0.72 / 0.62 | 0.68 / **0.26** / 0.63 |

All SQL execution times are cumulative statement wall time, not CPU. Categories are a heuristic based on SQL text inspected inside PostgreSQL; exported artifacts contain fingerprints, not SQL text.

**Decision: reject promotion of the global prefilter flag.** Keyed FIFO ON nearly halves throughput and increases p95 by ~68% against two very stable OFF controls. Although policy-admission SQL calls decrease by approximately 80%, keyed ON candidate-window query cache-block hits rise from ~132k–97k (OFF controls) to ~641k, and CPU usage *decreases* while latency rises. These observations favor an **underutilization/readiness-gap hypothesis** over a saturated PostgreSQL CPU explanation. The cause is not yet proven; no SQL text, execution-trace critical path or per-resource handoff gap was captured. The partition-FIFO improvement is ambiguous because OFF controls drift ~49%.

For keyed FIFO, event-gated matching empty SQL drain counts were approximately 360 (OFF1), **479 (ON)**, 363 (OFF2), despite 442 / 433 / 459 successful drains. This is consistent with more unsuccessful checks per useful lease and an active policy-head wakeup issue, but counters from restarted or reset Hub processes should not be treated as fully reliable unless the restart boundaries are verified.

**Next falsifiable question:** How much time passes between completion of the previous activity for a specific resource key and claiming the next activity for that same key, when the resource still has pending ready work? Using `hub_task_attempt` and each activity's resource key, measure the p50/p95/p99 FIFO handoff gap and number of handoffs >1 s by benchmark stage. Correlate those gaps with generic permit-release NOTIFY, match-hint receipt/forward and event-gated dispatcher empty-claim counts. A higher ON handoff gap would explain lower processor utilization despite fewer SQL resource checks. Keep all lease/fencing and ordering checks intact; do not globally disable generic completion wakeups or bypass strict FIFO.

The head prefilter remains default OFF; it should not be enabled only for keyed or partition policy based on these single-run results. No change to live PostgreSQL connection pool or processor partition ownership is justified.

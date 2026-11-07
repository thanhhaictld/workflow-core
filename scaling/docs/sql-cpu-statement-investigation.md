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

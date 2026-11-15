# 4k benchmark after the stale-statistics CASE fix

**Verdict:** the three exact `make obs-test-4k` repetitions all passed. The new selected-ID statement fingerprint ran in every cohort and its old OR-form fingerprint had **zero new calls**; the earlier selected-ID 50.9m shared-hit outlier did not recur. PostgreSQL process CPU was **137.13 s, 129.27 s, and 159.25 s** in the durable workflow windows (median **137.13 s**, range **129.27–159.25 s**). The last two runs also show that the broad claim-sequence index still receives millions of tuple reads in runs 1–2 outside the selected-ID fingerprint; the counters point to discovery/recovery work, but do not attribute those index reads to a specific SQL plan.

## Per-run durable outcomes

The workload was unordered policy mode, 4,000 workflows at 100 VUs. Throughput below is calculated from first durable workflow creation through last durable completion; persisted latency percentiles are per-workflow `completed_at - created_at`. k6 driver time is separate. All audit counts are durable rows, not inferred from the k6 iteration count.

| Run / unique cohort ID | Durable WF / tasks | Workflow interval (s) | WF/s / tasks/s | Persisted p50 / p95 / p99 (s) | k6 driver (s) | Audit: failed / extra attempts / dead tasks | Make exit |
|---|---:|---:|---:|---:|---:|---|---:|
| 1 `k6-unordered-20261115t032252053-e2b37831` | 4,000/4,000; 16,000/16,000 | 55.343 | 72.277 / 289.108 | 40.911 / 49.475 / 50.250 | 56.553 | 0 / 0 / 0 | 0 |
| 2 `k6-unordered-20261115t032516285-04efd26b` | 4,000/4,000; 16,000/16,000 | 52.295 | 76.489 / 305.954 | 37.121 / 44.311 / 45.881 | 53.178 | 0 / 0 / 0 | 0 |
| 3 `k6-unordered-20261115t032740651-8e4deebf` | 4,000/4,000; 16,000/16,000 | 56.077 | 71.331 / 285.324 | 29.901 / 49.188 / 49.495 | 57.047 | 0 / 0 / 0 | 0 |
| **Median (range)** | **4,000/4,000; 16,000/16,000** | — | **72.277 (71.331–76.489) / 289.108 (285.324–305.954)** | **37.121 / 49.188 / 49.495** | **56.553** | **0 / 0 / 0 in every run** | **0** |

Each k6 summary logged 4,000 accepted/completed iterations and zero failed iterations. Durable audits recorded 4,000 submitted and completed workflows, 4,000 activity attempts, 4,000 durable completions/results, 16,000 completed task rows (state 2), no extra task attempts/retries, and no dead tasks. FIFO/audit-order assertions do not apply to this unordered workload. No failed run was hidden or omitted.

Exact boundaries are UTC. The workflow window is taken from persisted workflow rows; the command window is from immediately before spawning Make through its return after the audit.

| Run | Durable window: first create → last completion | Command + durable audit window |
|---|---|---|
| 1 | 2026-11-15 03:22:53.192 → 03:23:48.534 | estimated Make spawn 2026-11-15 03:22:48.919 → 03:24:00.012 |
| 2 | 2026-11-15 03:25:17.237 → 03:26:09.533 | estimated Make spawn 2026-11-15 03:25:12.984 → 03:26:19.192 |
| 3 | 2026-11-15 03:27:42.316 → 03:28:38.393 | estimated Make spawn 2026-11-15 03:27:37.092 → 03:28:53.789 |

Inter-run readiness checks found no active k6 container, no pending or leased `policy-benchmark` tasks, and zero active sessions at the sampled idle points; the drains/idle gaps were 31.5 s and 36.7 s. The 16 processors and two Hub replicas remained running throughout the three repetitions.

## PostgreSQL CPU

The single healthy Prometheus source was `namedprocess_namegroup_cpu_seconds_total{job="postgres-process",groupname="postgres"}` from `postgres-process-exporter:9256`. The exporter supplied one `postgres` process-group series for each of `user` and `system`; both modes were summed once. PostgreSQL container ID `b2643dea82c378f2755753541cc8a331ddfdd1f93ac44de62e792eda7f40ce94` and its start time remained unchanged through the runs. `process_start_time_seconds` was stable at `1794676365.38`; counter samples were monotonic with no resets.

CPU seconds use cumulative counter deltas, with linear interpolation at the durable and command boundaries. Peak is the maximum of `sum(rate(namedprocess_namegroup_cpu_seconds_total{job="postgres-process",groupname="postgres"}[1m]))`, the one-minute rate definition, evaluated every 5 seconds within the window. It is reported as cores; 1.0 core is 100% of one core. PostgreSQL process CPU includes its foreground/background processes and any automatic maintenance, not Hub or processor CPU. SQL elapsed time and shared-buffer counts are not CPU measurements.

| Run | Workflow CPU-s / mean cores / peak 1m cores | Workflow CPU-ms / durable WF / task | Command + audit CPU-s / mean cores / peak 1m cores | Command + audit CPU-ms / WF / task |
|---|---|---|---|---|
| 1 | 137.13 / 2.478 / 2.728 (273%) | 34.28 / 8.57 | 146.98 / 2.067 / 2.728 (273%) | 36.74 / 9.19 |
| 2 | 129.27 / 2.472 / 2.551 (255%) | 32.32 / 8.08 | 142.07 / 2.146 / 2.569 (257%) | 35.52 / 8.88 |
| 3 | 159.25 / 2.840 / 3.152 (315%) | 39.81 / 9.95 | 172.48 / 2.249 / 3.152 (315%) | 43.12 / 10.78 |
| **Median (range)** | **137.13 (129.27–159.25) / 2.478 (2.472–2.840) / 2.728 (2.551–3.152)** | **34.28 (32.32–39.81) / 8.57 (8.08–9.95)** | **146.98 (142.07–172.48) / 2.146 (2.067–2.249) / 2.728 (2.569–3.152)** | **36.74 (35.52–43.12) / 9.19 (8.88–10.78)** |

The third run remains included: it had the highest workflow CPU, peak rate, and slowest workflow throughput, although its `ix_hub_task_claim_sequence` tuple-read count was the lowest. That divergence is a reason not to explain total PostgreSQL CPU with one index counter. A separate 60.134-second post-refresh idle interval measured about 4.10 CPU-s, or 0.068 mean cores. The idle figure also uses the 15-second source counter.

The separate database-stat snapshot intervals were 76.577 s (run 1), 71.926 s (run 2), and 82.702 s (run 3), with CPU 147.69 s, 145.80 s, and 173.45 s respectively. Those brackets exactly match the PSS/table/index snapshot deltas and begin 5.2–5.8 seconds before the estimated Make spawn, so they include the short idle/readiness preflight. The matching statement and index deltas below therefore use these snapshot brackets, not exact Make spawn boundaries. The exporter target scrape interval was **15 seconds** (timeout 5 seconds). Prometheus was polled every 5 seconds, but those polls are not treated as new scrape samples: the raw range results for `timestamp(namedprocess_namegroup_cpu_seconds_total{...})` were used to deduplicate underlying samples. The maximum observed source-sample gap per run was 15.54–15.58 seconds, with 16 distinct scrape timestamps in each range and no collection errors. CPU totals therefore have scrape-boundary uncertainty of up to about one source interval. Raw counters, timestamp series, and one-minute-rate ranges are retained, with pre/post context.

## Fixed query and other `hub_task` activity

The rebuilt Hub image contains the CASE branch in the compiled `CandidateBatchAdmissionSql` string; PostgreSQL recorded that normalized statement as query ID **`-1905154372977884721`**. The old OR-form query ID **`-9210391527002916596`** was present in every before/after snapshot at the same cumulative 7,271 calls and 79,562 rows (delta **0** in each run). Thus the current workload did not execute the old selected-ID SQL. This confirms the integrated code path was active; there was no mutating `EXPLAIN ANALYZE` against the shared database.

`pg_stat_statements` deltas below span the same before/after snapshot interval as the `pg_stat_user_tables`/`pg_stat_user_indexes` deltas. These intervals start 5.2–5.8 seconds before estimated Make spawn, so include short idle/readiness preflight and command/audit work. Calls/rows describe operations; shared blocks describe buffer access and are not unique rows or CPU.

| Statement fingerprint / observed path | Run 1: calls / rows / hits / reads / elapsed ms | Run 2 | Run 3 |
|---|---|---|---|
| **Selected-ID batch claim** `-1905154372977884721` (CASE form) | 1,548 / 15,888 / 927,821 / 5,819 / 2,697.6; **1.743 ms/call** | 1,591 / 15,771 / 931,711 / 6,727 / 2,553.7; **1.605 ms/call** | 1,728 / 15,781 / 938,502 / 4,750 / 3,344.9; **1.936 ms/call** |
| CandidateReader ready page `3737985904634580822` | 1,847 / 50,409 / 3,430,885 / 2,672 / 5,345.6 | 3,474 / 51,256 / 2,923,657 / 3,080 / 4,451.2 | 4,625 / 66,581 / 444,300 / 3,081 / 1,285.3 |
| CandidateReader expired page `-5021478638737671992` | 1,376 / 0 / 2,098,805 / 0 / 2,941.2 | 2,752 / 0 / 1,723,737 / 0 / 2,305.9 | 3,584 / 0 / 18,421 / 17 / 86.8 |
| `ReadyHubsAsync` periodic readiness sweep `-3370666674386915353` | 75 / 55 / 351,513 / 34,739 / 1,038.5 | 71 / 51 / 354,775 / 49,279 / 1,021.7 | 82 / 54 / 417,969 / 61,758 / 1,364.0 |
| Durable completion `UPDATE hub_task ... result_json` `702976769898827252` | 16,000 / 16,000 / 1,329,911 / 2,993 / 7,980.6 | 16,000 / 16,000 / 1,386,942 / 3,009 / 7,333.1 | 16,000 / 16,000 / 420,476 / 2,999 / 7,950.0 |
| Durable next-task inserts `325984884542718125` | 12,000 / 12,000 / 612,358 / 4,175 / 5,685.2 | 12,000 / 12,000 / 633,469 / 4,625 / 4,733.0 | 12,000 / 12,000 / 653,004 / 3,822 / 5,636.7 |

The existing two-Hub `HubReadinessBroadcaster` calls `ReadyHubsAsync` from a two-second safety sweep; that is expected coordination/polling activity. The `WITH page AS MATERIALIZED` rows are the separate `HubCandidateReader` discovery streams, also expected work while processors search for candidates. Their high call counts and buffer activity are visible in `pg_stat_statements`, including expired pages that returned no rows in runs 1–2. Completion updates, next-task inserts, and history writes are necessary durable workflow operations. The command window also includes a run-ID-scoped durable audit query; its fingerprint `-5665257445861637134` made 44–46 calls and read workflow rows for verification. It is audit cost, not business throughput.

The independent `pg_stat_user_indexes` counters do not attribute an index access to a statement. On the same command/audit windows:

| Run (same before/after stats snapshot interval, including preflight) | All `hub_task` index heap fetches | `ix_hub_task_claim_sequence`: scans / index tuples read / heap tuples fetched | `ix_hub_task_ready`: scans / tuples read | `ix_hub_task_expired`: scans / tuples read |
|---|---:|---:|---:|---:|
| 1 | 6,165,749 (385.4 per completed task) | 3,375 / 5,679,499 / 4,833,546 | 75 / 859,621 | 18,635 / 1,127,892 |
| 2 | 5,241,476 (327.6 per task) | 1,737 / 4,711,980 / 4,081,601 | 91 / 837,843 | 15,220 / 1,161,352 |
| 3 | 482,682 (30.2 per task) | 23 / 67,751 / 29,517 | 212 / 1,061,636 | 3,750 / 2,788 |

This is substantially below the prior integrated selected-ID outlier's **56.064m** claim-sequence heap fetches and **56.817m** table-wide `hub_task` heap fetches, and the fixed query's own shared hits stayed around 0.93m/run. Yet the first two rows still have millions of claim-sequence index tuple reads. They cannot be charged to query `-1905154372977884721` from aggregate counters alone. PSS shows high-work CandidateReader page statements and the readiness sweep; those are the leading observed discovery/recovery paths, but no per-statement plan-to-index evidence was collected for them. Prometheus 5-second index-counter history is archived separately; it confirms the distinct during-workflow changes but is not used to fabricate statement attribution.

**Interpretation:** the prior exact per-ID `LATERAL ... FOR UPDATE` broad scan is not observed in these runs: the OR fingerprint did not run, the CASE fingerprint did, and its hits/elapsed stayed near the normal-sized runs in the previous report rather than the outlier. High aggregate index activity still exists in runs 1–2, most plausibly in candidate discovery/readiness work given the actual SQL call shapes and source paths; whether the current candidate page planner itself walks `ix_hub_task_claim_sequence` under a stale estimate remains unproven. The smallest next diagnostic is an isolated PG17 plan reproduction for the exact CandidateReader ready/expired page SQL and `ReadyHubsAsync` against the current schema/history with the pre-run stale statistics state. Do not infer a fix from table-wide fetch counts.

## Statistics and database boundaries

The source commit was `e9c6c4e3e60f17e2cc29ee8d04b9e964bad7380c`; the CASE fix and related tests were uncommitted worktree changes (`git diff --binary` SHA-256 `21da23e3bc941b0e8e6d07ad5c8707de671251a444097d32fb2b1fa9674561af`). `HubTaskStore.cs` SHA-256 was `780402ae50dbbd5f9e81731976fd3301e2f7d41cd551d16a3c1f928d590999a5`; the extracted normalized `CandidateBatchAdmissionSql` SHA-256 is `cc0c148a07455727e088983ececcdb599ff48423bdea734c6a9834820d76e063`. Both Hub containers carried identical `WorkflowCore.Hub.dll` (SHA-256 `485f629bb04dd2b04846e7557c65052488e7b59e5a551f188e245ca837e29aa7`); the CASE fragment is present in the binary, and the new query fingerprint is present in database-scoped PSS deltas. Hub image IDs were `sha256:f8592fc5f0c54551191f6ef30d020550052df37819febb8a9b0f5a7d63c2feec` and `sha256:a2e1893be0b078ad72c4dc436bd4f1af177af53267860a1e4384d3f0f5be3882`; processor image ID was `sha256:64d9c3d2824fdfd076b361701ed916133cfea8f5541a865fb8cb87bb63715a96`.

The Docker project label was `workflow-hub-observability-ad84b937db5895e0`. Every checked container's project working directory was this checkout's `scaling/observability`, with base `compose.yaml` plus `compose.matching-ha.yaml`, `compose.matching-shards.yaml`, and `compose.owner-recovery.yaml`. Before refresh the project had two Hubs and 16 processors. `make ups-16 OBS_PROCESSORS=16` rebuilt Hub/Hub-2/processor/monitor images and recreated only changed runtime services; both Hubs and all 16 processors became healthy/running. PostgreSQL was not restarted: the full container ID, image ID, and `StartedAt=2026-11-14T16:49:07.163Z` were unchanged. The normal startup migration container completed; the existing `hub_task` index set remained 21 indexes. No volume removal, reset, truncate, index operation, manual ANALYZE/VACUUM, database setting change, or statistics reset was performed.

Observed effective preset/settings included: two Hubs; 16 processors, each with 8 workflow and 8 activity workers; Hub DB pool 12; demand claim admission limit 3/max waiter 1; matching partitions 4; candidate batch size 32, candidate admission concurrency 2, cross-worker batching/concurrent admission/backpressure enabled; indexed bucket claims, split discovery, completion fast path, and batch history enabled; scheduler enabled; native event lane `subscriptions`; push enabled; event gating, long polling, task SSE push, unconstrained batch claims, outbox, FIFO-head prefilter, scoped claims, deferred owner check, ordered capability merge, and worker-capacity fanout disabled; Hub SQL metrics sampler disabled. PostgreSQL was 17.11, `max_connections=100`, with the 21 retained indexes listed in the evidence artifact. The host exposed 16 CPUs and 31.2 GiB memory; PostgreSQL and application containers had no Docker CPU or memory limits. No k6 workload was running before the first measurement; all three queue checks were clear.

Database history was preserved and grew only with the three cohorts: before run 1 there were 36,000 workflows/144,000 tasks/36,000 attempts/504,000 workflow events; after run 3 there were 48,000/192,000/48,000/672,000. `hub_task` heap/total bytes grew from 257,261,568/416,022,528 to 337,575,936/539,901,952; corresponding workflow, attempt, and event sizes are in each before/after JSON snapshot. `pg_stat_statements_info.stats_reset` stayed `2026-11-14T16:49:08.098849Z`; database `stats_reset` was null. The table-level `reltuples`, page count, `pg_stats.state` distribution, `n_live_tup/n_dead_tup`, and `last_autoanalyze` were preserved before/after every run. Natural autovacuum analyze occurred during the measurement series: `last_autoanalyze` advanced from 2026-11-15 01:43:54.966Z before run 1 to 03:24:00.227Z after it, 03:25:56.375Z after run 2, and 03:28:07.739Z after run 3. Before run 1, state MCV showed only state 2 at frequency 1.0; later sampled distributions included states 0/1 while work was active. This is run-to-run context, not a controlled statistics treatment. The snapshots do not include the partial index's own `pg_class.reltuples` or per-boundary `last_autovacuum`; the exact partial-index estimate and autovacuum timestamp at each boundary therefore remain unavailable.

WAL is also not reported per run: this PostgreSQL 17 build exposes WAL byte counters through `pg_stat_wal`, not `pg_stat_database`; the per-run read-only snapshots omitted `pg_stat_wal`. The derived summary marks this as unavailable rather than reporting a false zero. Transaction commits/rollbacks, table counters, index counters, relation sizes, and statement stats are retained in the raw per-run snapshots. These limits do not affect the durable cohort audit or process-exporter CPU calculation.

## Commands, refresh and retained artifacts

Prerequisites were GNU Make 4.4.1, PowerShell 7.6.1, Docker Engine 29.9.3, and Python available to the owner-recovery audit. Before workload, the supported refresh was `make ups-16 OBS_PROCESSORS=16`; its exit code was 0. The actual three repetitions were identical apart from their generated evidence/run IDs:

```text
make obs-test-4k OBS_PROCESSORS=16 OWNER_RESULTS="C:/Users/haido-ws/AppData/Roaming/orca/chat-scratch/740b1a1da96a083620bbe9d96f86cc40/post-stale-fix-4k-luna/runs"
```

The target expansion is saved in `make-target-expansion.txt`: `owner-recovery.ps1 test-4k`, unordered, hard-coded 4,000 workflows/100 VUs, 600-second timeout, and `-Processors 16`. Each `make.log` contains the run-specific k6 audit result; each `exit-code.txt` is 0. `make ups-16` built from the current worktree, and the copied Hub DLL and PSS fingerprint prove the measured Hub included and executed the CASE SQL.

Raw artifacts are under:

`C:/Users/haido-ws/AppData/Roaming/orca/chat-scratch/740b1a1da96a083620bbe9d96f86cc40/post-stale-fix-4k-luna/`

- `refresh-ups-16.log`, `refresh-ups-16.meta.txt`, `stack-source-identity.json`, `hub-1-WorkflowCore.Hub.dll`, `hub-2-WorkflowCore.Hub.dll`, `database-settings-before.txt`, `make-target-expansion.txt`: safe build/stack/source identity and expansion evidence. No raw container `Env` or credential-bearing Compose config was saved.
- `runs/measurement-run-1/`, `runs/measurement-run-2/`, `runs/measurement-run-3/`: Make logs/exit codes, per-run before/after database-scoped PSS and table/index/statistics snapshots, 15-second live table/index snapshots, audit results, workflow timestamp/latency windows, and k6 summaries.
- Each run directory has `workflow-cpu-counter-prom-range.json`, `workflow-cpu-timestamp-prom-range.json`, `workflow-cpu_rate_1m-prom-range.json`, command/audit equivalents, and raw Prometheus ranges for per-index scans, table index fetches/scans, and sequential reads. `postgres-cpu-instant-polls.jsonl` contains the timestamped poll responses plus tail; `cpu-tail-meta.json` defines the post-command metric-tail capture.
- `prometheus-process-target.json`, `postgres-process-start-identity.json`, `idle-post-refresh-cpu.jsonl` and its raw counter/timestamp range responses, `hub-task-index-set.txt`, `old-selected-query-check.json`, `three-run-summary.csv`, and `three-run-summary.json` preserve compact source, target, idle, and result evidence. The temporary collector exited after the third tail; no sampler was left running.

The latest report is observational. The previous post-integration report recorded CPU 124.14/107.53/187.48 s and the selected-ID outlier with 56.8m table fetches; its flags, accumulated database history, run windows, and cache/autovacuum state differ. Current median CPU and p95 are not evidence of causal gain or regression, and SQL elapsed time is not CPU. Current workload throughput is lower than that earlier observation; this result remains visible. The refreshed development stack is left running with two Hubs and 16 processors, and all benchmark rows/volumes remain intact.


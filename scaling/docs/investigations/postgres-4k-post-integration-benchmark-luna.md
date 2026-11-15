# Selected-ID batch admission: post-integration 4k benchmark

**Result:** all three required 4,000-workflow make obs-test-4k runs passed. Each persisted 4,000/4,000 completed workflows and 16,000/16,000 completed tasks, with zero workflow failures, dead tasks, or extra task attempts. PostgreSQL process CPU for the workflow windows was **124.14 s, 107.53 s, and 187.48 s** (median **124.14 CPU-s/run**, range **107.53–187.48**); command/audit-inclusive estimates were **126.21 s, 112.33 s, and 187.48 s** (median **126.21 CPU-s/run**, range **112.33–187.48**). These are observational estimates from the existing PostgreSQL process exporter; its actual scrape interval was 15 seconds, so sub-15-second boundary CPU is not resolved exactly.

## Per-run results

Workflow duration is from the first persisted creation to the last persisted completion. Throughput uses that same interval. Persisted p95/p99 are the per-workflow created-to-completed durations. k6 driver duration is reported separately.

| Run ID | Workflows completed | Tasks completed | Workflow interval | WF/s | Tasks/s | Persisted p50 / p95 / p99 | k6 driver | Audit result |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| k6-unordered-20261115t010504605-57015f28 | 4,000/4,000 | 16,000/16,000 | 48.34 s | 82.75 | 331.00 | 37.23 / 42.49 / 43.40 s | 49.48 s | pass; 0 failed, 0 extra attempts, 0 dead |
| k6-unordered-20261115t010851570-50724699 | 4,000/4,000 | 16,000/16,000 | 49.95 s | 80.07 | 320.29 | 34.69 / 40.49 / 43.72 s | 50.94 s | pass; 0 failed, 0 extra attempts, 0 dead |
| k6-unordered-20261115t011013617-651ab7da | 4,000/4,000 | 16,000/16,000 | 52.45 s | 76.26 | 305.04 | 31.67 / 42.84 / 46.88 s | 53.34 s | pass; 0 failed, 0 extra attempts, 0 dead |

Across the three runs, median throughput was **80.07 workflows/s** and **320.29 tasks/s**; median persisted p95/p99 were **42.49/43.72 s**. All runs were unordered, so keyed/FIFO ordering assertions do not apply. Each audit recorded 4,000 activity-attempt rows, zero extra attempts/retries, zero dead tasks, exactly one completion and one activity result per workflow, and no duplicate records. There were no audit failures.

Exact UTC boundaries from durable workflow rows and the Make exit markers:

| Run ID suffix | Workflow start → last completion (UTC) | Make command start → audit return (UTC) |
|---|---|---|
| 010504605 | 2026-11-15 01:05:05.759 → 01:05:54.098 | 2026-11-15 01:05:01.548 → 01:06:02.518 |
| 010851570 | 2026-11-15 01:08:52.613 → 01:09:42.567 | 2026-11-15 01:08:48.450 → 01:09:50.425 |
| 011013617 | 2026-11-15 01:10:14.961 → 01:11:07.413 | 2026-11-15 01:10:10.595 → 01:11:15.123 |

| Run ID suffix | PostgreSQL CPU s (workflow / command+audit) | Mean cores (workflow / command+audit) | Peak one-minute rate (cores; percent of one core) | CPU ms / workflow (workflow / command+audit) | CPU ms / task (workflow / command+audit) |
|---|---:|---:|---:|---:|---:|
| 010504605 | 124.14 / 126.21 | 2.57 / 2.07 | 2.76 (276%) | 31.04 / 31.55 | 7.76 / 7.89 |
| 010851570 | 107.53 / 112.33 | 2.15 / 1.81 | 2.39 (239%) | 26.88 / 28.08 | 6.72 / 7.02 |
| 011013617 | 187.48 / 187.48 | 3.57 / 2.91 | 3.83 (383%) | 46.87 / 46.87 | 11.72 / 11.72 |

Median workflow-window mean CPU was **2.57 cores** and median one-minute peak was **2.76 cores**. The third run is visibly slower and more CPU-intensive; it remains in the three-run summary. Its command/audit counter delta equals its workflow delta at the available 15-second resolution. That does not show that the audit used zero CPU; the sampling cadence cannot resolve the short tail precisely.

## SQL path and database access

The current Hub image contains the compiled SQL constant beginning WITH selected AS MATERIALIZED, and PostgreSQL pg_stat_statements recorded its normalized statement as query ID **-9210391527002916596** during every run. The older cross-worker batch shape, query ID **-6050224743415277628**, had zero call delta in all three measured windows. This establishes that the selected-ID batch path ran; it is not a runtime EXPLAIN of the measured plans.

The per-run snapshots were taken through the workflow database connection. A post-run join on pg_stat_statements.dbid confirmed both fingerprints existed only in database workflow (OID 16384); the other databases on that PostgreSQL instance were template0, template1, and postgres. This rules out same-query-ID rows from another database affecting those deltas.

| Run | Selected-ID calls | Returned rows | Mean elapsed / call | Shared hits / reads | Shared hits / returned row | All hub_task index heap fetches / completed task |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 1,425 | 15,960 | 2.06 ms | 921,792 / 1,320 | 57.8 | 80.1 |
| 2 | 1,469 | 15,968 | 1.51 ms | 942,697 / 1,392 | 59.0 | 19.8 |
| 3 | 1,336 | 15,902 | 37.34 ms | 50,917,492 / 2,531 | 3,202.0 | 3,551.0 |

The selected-ID statement returned about 15.9k leased rows per 16k-task run. Run 3's statement elapsed time and cached-block hits rose sharply. The table-wide hub_task index-fetch delta rose to 56,816,712 in that run, of which 56,064,036 were attributed by pg_stat_user_indexes to ix_hub_task_claim_sequence. These counters are access counts across PostgreSQL statements, not unique tasks or CPU time; the snapshots do not attribute that index delta to one statement. Do not read the selected-ID query's SQL elapsed time as CPU time.

The two earlier runs' access counters were much lower. Run 3 is a real observed outlier, not excluded as warm-up. The benchmark confirms integration and successful throughput, while its run-to-run SQL and index-access variability needs further investigation before making an efficiency claim.

## CPU collection and boundaries

- CPU source: namedprocess_namegroup_cpu_seconds_total{job="postgres-process",groupname="postgres"}, summing its user and system modes for the single postgres-process-exporter:9256 target. No double counting of the mode series was done. All observed counters were monotonic with no reset.
- Prometheus target health was up; target metadata reports a **15-second scrape interval**. The collector polled the existing Prometheus instant endpoint every 5 seconds, archiving timestamped raw counter responses. Thus Prometheus range queries were evaluated at 5-second steps over 15-second source samples. CPU totals use interpolation across those cumulative samples; peak uses the maximum of sum(rate(namedprocess_namegroup_cpu_seconds_total{job="postgres-process",groupname="postgres"}[1m])) evaluated over each workflow window. Short bursts and exact boundary attribution are limited by the 15-second scrape.
- Each workflow window begins at the minimum persisted hub_workflow.created_at for that run and ends at the maximum completed_at. Each command/audit window begins immediately before invoking Make and ends at the timestamp of the recorded Make exit-code file after the durable audit. Command windows were 60.97, 61.98, and 64.53 seconds; workflow windows were 48.34, 49.95, and 52.45 seconds.
- A separate 65-second idle PostgreSQL sample immediately before run 1 measured approximately **1.36 CPU-seconds / 0.021 cores** (about 2.1% of one core). This is also a 15-second-counter estimate.
- PostgreSQL process CPU includes its foreground and background processes, including any maintenance occurring in the window; it does not include Hub/worker CPU. Database transaction and WAL deltas include background service activity. The three runs were sequential, with drained task queues and idle intervals between them. At run boundaries no k6 container or unrelated Docker workload was running; PostgreSQL snapshots showed only the expected Hub/processor connections and the diagnostic session. Other containers share the host's CPU resources.

The earlier review's screenshot values are context only: 69–71 workflows/s, 49–51 s p95, 3.29-core peak, and roughly 385 CPU-s across **two** runs in a five-minute audit-inclusive interval (8,000 workflows / 32,000 tasks, about 48.1 ms/workflow and 12.0 ms/task). Its service flags, history, and measurement interval differ. The totals are not compared one-run to two-run, and none of these observations establishes a controlled optimization gain or causal CPU reduction.

## Stack, source, and refresh

- Repository HEAD: cf523d06561025287c13f507e995ed51553278bc; source was uncommitted. HubTaskStore.cs SHA-256: D210A93DE78A596220A90376A7FAC7390468BA84E79A110D5990A595E318B9E5; git hash-object: 9ed1bd58befc95d8a3728da65d97e41b108e71e6. The CandidateBatchAdmissionSql literal SHA-256 is 81adb2b434f4ec97fec87076a10dd2aec7834b71357fa327fc92d45b13b3079b. No engine source was changed during this benchmark.
- make obs-up OBS_PROCESSORS=16 rebuilt and refreshed this worktree's local development images/services using the owner-recovery overlays and the Make preset. It preserved the 16 processor replicas. Both Hub containers ran the same rebuilt Hub DLL; the copied Hub DLL contains the selected SQL string. Hub image IDs were sha256:7e84fea290521ed8f6bada6a79d97fd853a273816067558ed446103f009991fd and sha256:5935a8929cc9b4a5d1eba4fa8c2fdcc9993418f177f16ec1cf1cf206f5c40521.
- Compose project label was workflow-hub-observability-ad84b937db5895e0; the PostgreSQL container's project working directory and Compose files resolved to this worktree's scaling/observability base plus the matching HA, matching shards, and owner-recovery overlays. PostgreSQL remained the same container/image (PostgreSQL 17.11) with its original start time; it was not restarted. No volume removal, data reset, statistics reset, truncate, index change, or database tuning change was performed. The idempotent migration step in the normal owner-recovery startup completed during refresh.
- Topology: two Hubs, 16 healthy processor containers, PostgreSQL, Prometheus, process exporter, Grafana, and the existing observability services. Each processor had 8 workflow and 8 activity worker slots. Docker host availability was 16 CPUs and 31.2 GiB RAM; PostgreSQL had no CPU quota or memory limit. PostgreSQL max_connections was 100; both Hubs used DB pool 12.
- Effective Hub settings included matching/candidates/push enabled; four matching partitions; candidate backpressure, concurrent admission, and cross-worker batches enabled; candidate batch size 32 and candidate admission concurrency 2; demand claim admission enabled with limit 3 and max waiter 1; indexed bucket claims and split batch discovery enabled; unconstrained batch claims disabled; completion fast path and batched history enabled; SQL metrics sampler disabled; embedded scheduler enabled. Ordered capability merge, FIFO-head prefilter, scoped claims, deferred owner check, worker capacity fanout, and outbox were disabled. Native event lane mode was subscriptions. Processor workflow/activity concurrency was 8/8; push was enabled, while long polling and task-SSE push flags were false under this preset.
- hub_task retained 21 indexes: hub_task_pkey, hub_task_sequence_key, ix_hub_task_active_workflow_lookup, ix_hub_task_candidate_expired_bucket_deadline, ix_hub_task_candidate_ready_bucket_due, ix_hub_task_candidate_ready_bucket_sequence, ix_hub_task_claim_capability_bucket_sequence, ix_hub_task_claim_capability_sequence, ix_hub_task_claim_hub_sequence, ix_hub_task_claim_sequence, ix_hub_task_dead_workflow_step, ix_hub_task_expired, ix_hub_task_native_reconcile, ix_hub_task_operations, ix_hub_task_ready, ix_hub_task_ready_capability_bucket_sequence, ix_hub_task_ready_capability_sequence, ux_hub_native_dispatch, ux_hub_native_work, ux_hub_task_request, and ux_hub_task_workflow_step.

Before the three runs, table statistics showed approximately 16,000 workflows, 64,000 tasks, 16,000 task attempts, and 224,000 workflow events; after, exact counts were 28,000 workflows, 112,000 tasks, 28,000 attempts, and 392,000 events. All 112,000 tasks were completed.

| Table | Pre-run total / heap bytes | Final total / heap bytes |
|---|---:|---:|
| hub_workflow | 53,641,216 / 42,598,400 | 87,064,576 / 70,615,040 |
| hub_task | 213,221,376 / 120,930,304 | 328,605,696 / 197,623,808 |
| hub_task_attempt | 5,046,272 / 2,957,312 | 8,454,144 / 5,038,080 |
| hub_workflow_event | 83,525,632 / 64,086,016 | 146,079,744 / 112,197,632 |

| Run | DB transaction commits / rollbacks | WAL bytes written | DB connections at boundary (active / idle, before → after) |
|---|---:|---:|---|
| 1 | 144,375 / 1 | 345,483,433 | 1 / 12 → 2 / 21 |
| 2 | 101,986 / 1 | 221,259,128 | 1 / 22 → 2 / 29 |
| 3 | 149,705 / 0 | 322,215,504 | 1 / 29 → 1 / 29 |

These database-wide transaction and WAL deltas include background Hub, processor, and PostgreSQL activity; they are not business-workflow operation counts. The connection values are boundary snapshots, not sampled peaks. hub_task total size grew from 213.2 MB to 328.6 MB (heap 120.9 MB to 197.6 MB); workflow, attempt, and event data also grew with the benchmark. pg_stat_statements_info.stats_reset remained at 2026-11-14 16:49:08 UTC throughout. No measured interval reset statistics. The PostgreSQL database stats_reset field was null in the captured snapshots.

## Commands and preserved evidence

Prerequisites observed: GNU Make 4.4.1, PowerShell 7.6.1, Docker Engine 29.9.3, Python available to the owner-recovery script. The actual benchmark command for each repetition was:

    make obs-test-4k OBS_PROCESSORS=16 OWNER_RESULTS="C:/Users/haido-ws/AppData/Roaming/orca/chat-scratch/740b1a1da96a083620bbe9d96f86cc40/postgres-4k-batch-luna/runs"

Make expanded this to owner-recovery.ps1 test-4k, stress mode unordered, 4,000 workflows, 100 VUs, 600-second timeout, and -Processors 16; all three exit codes were 0. k6 reported 4,000 accepted and completed iterations on every run. Durable cohort audit results and original k6 outputs are retained.

Large/raw artifacts are in C:/Users/haido-ws/AppData/Roaming/orca/chat-scratch/740b1a1da96a083620bbe9d96f86cc40/postgres-4k-batch-luna/:

- run-summary.json and report-metrics.py: computed per-run summary and reproduction from the preserved snapshots.
- postgres-process-cpu-samples.jsonl: timestamped 5-second Prometheus polling responses, including idle and pre/post context; run1-prom-counter-range.json through run3-prom-counter-range.json and matching run*-prom-rate-1m-range.json: raw Prometheus range API responses.
- run1-make.log through run3-make.log, run1-exit-code.txt through run3-exit-code.txt, run1-before.json through run3-after.json, run1-queue-state.txt, and run3-precondition.txt are in the artifact root; they preserve command output, exit codes, database snapshots, and queue checks.
- Durable source/audit folders are runs/20261115t010504605-57015f28, runs/20261115t010851570-50724699, and runs/20261115t011013617-651ab7da. Each has source.json, analysis.json, and a k6-unordered-<run-id> child directory containing durable workflow/task/attempt/event exports and the k6 summary.
- run-identity.json, hub-image-WorkflowCore.Hub.dll, queryid-database-scope.txt, running-services.txt, final-dataset-indexes.txt, and make-target-expansion.txt: image/source identity, query database scope, final service inventory, database index set, and expanded target.

The refreshed development stack remains running with 16 processors. The temporary CPU sampling loop was stopped after post-run context collection; its output and scripts remain in the scratch artifact directory. No production, external, or other-worktree services were targeted.


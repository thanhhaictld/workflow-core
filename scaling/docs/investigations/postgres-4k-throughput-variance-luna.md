# 4k throughput variance diagnosis

## Finding

The reported **47.973135 workflows/s** is a real low durable-completion result, not a k6 display conversion. Cohort `k6-unordered-20261115t040011728-5b6fdfa4` has 4,000 durable workflows and 16,000 completed task rows; first workflow creation to last completion is **83.380 s**, or **47.9731 workflows/s**. Its k6 iteration rate is 47.169/s because k6 includes teardown time; the durable rate is the appropriate completed-workflow measure.

The strongest evidence points to variable queue/hand-off delay in the workflow execution path. PostgreSQL did not reproduce the selected-ID broad-scan spike, and its process CPU was lower on average during the slow run than in the faster reference runs. The specific source of the queue delay remains unproven: per-worker/partition claim waits and per-stage queue depth were not captured at sufficient resolution.

## Durable run comparison

All cohorts are unordered 4k runs. Their durable audits report 4,000 submitted and completed workflows, 4,000 activity attempts, 16,000 tasks in completed state, zero failed workflows, zero extra attempts, and zero dead tasks. The time intervals below are UTC; there is no overlapping cohort. The prior cohort's audit artifact was written before the next k6 start.

| Cohort | Durable create → complete | WF/s | Persisted latency p50 / p95 / p99 | k6 submit p95 | Audit artifact written |
|---|---|---:|---:|---:|---|
| Reference 1 `032252053-e2b37831` | 03:22:53.192–03:23:48.534 (55.343 s) | 72.277 | 40.911 / 49.475 / 50.250 s | 373 ms | 03:24:35.381 |
| Reference 2 `032516285-04efd26b` | 03:25:17.237–03:26:09.533 (52.295 s) | 76.489 | 37.121 / 44.311 / 45.881 s | 260 ms | 03:26:54.542 |
| Reference 3 `032740651-8e4deebf` | 03:27:42.316–03:28:38.393 (56.077 s) | 71.331 | 29.901 / 49.188 / 49.495 s | 304 ms | 03:29:29.112 |
| Later `033806912-233f1c96` | 03:38:08.354–03:39:12.473 (64.119 s) | 62.384 | 33.982 / 58.998 / 59.786 s | 501 ms | 03:39:21.689 |
| **Reported slow** `040011728-5b6fdfa4` | **04:00:13.181–04:01:36.561 (83.380 s)** | **47.973** | **42.150 / 73.148 / 75.737 s** | **500 ms** | 04:01:45.257 |
| Adjacent repeat `040153933-a8aef46e` | 04:01:55.217–04:03:01.288 (66.071 s) | 60.541 | 36.617 / 61.419 / 61.841 s | 417 ms | 04:03:09.258 |

The slow run is **33.7% below** the earlier three-run median of 72.277 WF/s, and its persisted p95 is 23.96 s above that median (49.188 s). The next run, with the same Hub images and observed feature flags, recovered to 60.541 WF/s, but remained 16.2% below the old median. The 03:38 run is not a perfect control: its captured Hub setting had `WFC_HUB_OUTBOX_ENABLED=true`, while the reference and the two 04:00 runs had it `false`.

The slow and next runs did not overlap: the slow audit result was written at 04:01:45.257, before the next run's k6 setup at 04:01:55.204 and first workflow at 04:01:55.217. The 03:38 audit was written at 03:39:21.689, over 20 minutes before the reported slow run. This rules out an overlapping audited 4k cohort; it does not prove there was no unrelated background activity.

## PostgreSQL CPU and SQL/index evidence

CPU is from the existing `postgres-process` exporter counter, summing its `user` and `system` modes once. The PostgreSQL container ID `b2643dea82c378f2755753541cc8a331ddfdd1f93ac44de62e792eda7f40ce94` and its 2026-11-14 16:49:07Z start time were unchanged in the historical cohort metadata. Counter boundaries were linearly interpolated using deduplicated actual source timestamps from `timestamp(namedprocess_namegroup_cpu_seconds_total{job="postgres-process",groupname="postgres"})`; source cadence is 15 s, maximum gap in the captured range was 15.767 s, both mode counters were monotonic, and there was one healthy target. Peak is the maximum of the same one-minute `sum(rate(...[1m]))` over the workflow interval. CPU includes PostgreSQL background processes and concurrent database work; it is not SQL elapsed time.

| Run | Workflow CPU-s / mean cores / max 1m cores | CPU ms / workflow / task | Command + audit CPU-s / mean cores / max 1m cores |
|---|---:|---:|---:|
| Reference 1 | 137.13 / 2.478 / 2.728 | 34.28 / 8.57 | 146.98 / 2.067 / 2.728 |
| Reference 2 | 129.27 / 2.472 / 2.551 | 32.32 / 8.08 | 142.07 / 2.146 / 2.569 |
| Reference 3 | 159.25 / 2.840 / 3.152 | 39.81 / 9.95 | 172.48 / 2.249 / 3.152 |
| 03:38 run | 156.75 / 2.445 / 2.755 | 39.19 / 9.80 | 163.43 / 2.186 / 2.755* |
| **Reported slow** | **156.43 / 1.876 / 2.715** | **39.11 / 9.78** | **159.76 / 1.708 / 2.715*** |
| Adjacent repeat | 149.15 / 2.257 / 2.217 | 37.29 / 9.32 | 159.63 / 2.119 / 2.345* |

The reference command/audit windows came from the prior benchmark collector's estimated Make spawn and exit boundaries. For the three later runs, command/audit CPU is a bounded proxy from `source.json` UTC through the durable-audit `analysis.json` file-write time; that start precedes k6 setup by about 1.3 s and the file is written after durable audit. These are not exact shell process start/exit timestamps. `*` denotes this proxy window. CPU per completed workflow is only modestly higher in the slow run than the reference median (39.11 vs 34.28 ms); average DB load was 1.876 cores, with a 2.715-core one-minute peak. This is not evidence of PostgreSQL CPU saturation or a CPU-based explanation for the slow cohort’s **50.7% longer workflow window relative to the 55.342639 s reference median** (83.380066 s vs 55.342639 s).

The exact CPU interval boundaries are:

| Run | Command/audit CPU window (UTC) | Boundary source |
|---|---|---|
| Reference 1 | 03:22:48.919–03:24:00.012 | Prior collector's estimated Make spawn to Make return |
| Reference 2 | 03:25:12.984–03:26:19.192 | Prior collector's estimated Make spawn to Make return |
| Reference 3 | 03:27:37.092–03:28:53.789 | Prior collector's estimated Make spawn to Make return |
| 03:38 run | 03:38:06.920–03:39:21.689 | `source.json` time to audit analysis file write |
| **Reported slow** | **04:00:11.736–04:01:45.257** | **`source.json` time to audit analysis file write** |
| Adjacent repeat | 04:01:53.939–04:03:09.258 | `source.json` time to audit analysis file write |

Historical SQL timing and table counters come from the existing 15 s SQL receiver (Prometheus evaluates at 5 s). The historical Prometheus exposition has per-query elapsed and shared-block read/write series, but no per-query call, row, or shared-hit series; those cannot be reconstructed for these runs. PSS timing and buffer-read counters below are cumulative-series deltas bracketed by the nearest Prometheus samples; boundary uncertainty is up to the collector cadence. “Index fetches” and “index scans” are `pg_stat_user_tables` relation-wide counters for `hub_task`, not per-index figures and not SQL call counts.

| Run | `hub_task` index heap fetches | Table `idx_scan` | Sequential scans / rows | Selected-ID CASE elapsed / shared-read blocks | Old OR-form elapsed |
|---|---:|---:|---:|---:|---:|
| Reference 1 | 5,550,702 | 176,155 | 0 / 0 | 2.328 s / 4,983 | 0 |
| Reference 2 | 5,185,847 | 172,562 | 0 / 0 | 2.121 s / 5,450 | 0 |
| Reference 3 | 450,692 | 195,846 | 0 / 0 | 2.927 s / 4,358 | 0 |
| 03:38 run | 9,172,338 | 206,899 | 0 / 0 | 2.525 s / 7,135 | 0 |
| **Reported slow** | **550,118** | **250,082** | **0 / 0** | **2.760 s / 5,994** | **0** |
| Adjacent repeat | 502,481 | 192,005 | 0 / 0 | 2.116 s / 4,040 | 0 |

The CASE-form query ID `-1905154372977884721` is present in the slow and adjacent historical PSS series; the old OR-form ID `-9210391527002916596` has zero elapsed delta in every compared interval. The CASE statement consumed about 2.76 s total measured execution time across the slow window, with 5,994 shared blocks read—not CPU and not a call count. The old broad-scan reference was tens of millions of shared hits and 56.8m table index heap fetches; the reported slow run had 0.55m table-wide index fetches. The selected-ID broad scan therefore did not recur. Table-wide `idx_scan` remained high, but row-fetch and SQL counters do not identify which statement caused each scan. The controlled stale-statistics diagnosis is documented in the [stale-statistics fix report](postgres-4k-stale-statistics-fix-luna.md); this follow-up does not claim a causal CPU or end-to-end throughput gain.

Other leading database statements during the slow interval were task-result updates (query ID `702976769898827252`, 6.31 s elapsed / 2,774 shared blocks read), next-task inserts (`325984884542718125`, 5.34 s / 4,485 blocks), workflow-event batch inserts (`-821587332231162083`, 3.01 s / 225 blocks), and workflow-event inserts (`147205577398578030`, 2.13 s / 4,668 blocks). These represent durable runtime operations. The CandidateReader ready-page query (`3737985904634580822`) accumulated 1.67 s, its expired-page query (`-5021478638737671992`) 0.39 s, and the periodic `ReadyHubsAsync` query (`-3370666674386915353`) 0.91 s. These are bounded discovery/recovery and readiness costs; no historical PSS call counts or per-query plans are available, so they cannot be charged by call or attributed from table-wide index counters.

Other available historical context is weak evidence of a hard database resource cap: the database-wide backend gauge ranged 18–31 during the slow cohort (max connections was 100); its database-size gauge grew about 73 MB across the sample bracket, consistent with appended history but not a per-table attribution. The checkpoint counter advanced once in the slow bracket and zero times in the adjacent bracket, while checkpoint-duration metrics had no usable delta. `hub_task`'s exported manual-vacuum counter stayed at zero, but there is no historical auto-analyze counter here. Sampled Hub ThreadPool pending work peaked at 3; the claim-admission/database-connection wait histograms were absent from the historical Prometheus series. These sampling gaps mean a brief resource queue or storage event cannot be ruled out.

## Where the extra wall time appears

Durable workflow exports show that the producer created all 4,000 workflows in about **10.66 s** in the slow cohort, a creation rate of about 375/s. Its 83.38 s result is the time until all workflows completed, not a slow stream of API submissions. By contrast, the adjacent repeat created all workflows in 7.20 s and completed the cohort in 66.07 s. Audits and k6 logs show zero HTTP failures and zero workflow failures in both runs.

Per-workflow event timestamps isolate long waits in the workflow/processor hand-off. The event intervals name what was observed, not a claim that a particular service owns all of the delay:

| Cohort | Created → first native claim p50 / p95 | Policy step start → activity result p50 / p95 | Activity result → resume claim p50 / p95 | Workflow completion p95 |
|---|---:|---:|---:|---:|
| Reference 1 `032252053-e2b37831` | 8.42 / 21.88 s | 10.25 / 18.60 s | 8.29 / 19.75 s | 49.47 s |
| Reference 2 `032516285-04efd26b` | 5.49 / 21.18 s | 8.74 / 13.17 s | 7.43 / 18.85 s | 44.31 s |
| Reference 3 `032740651-8e4deebf` | 5.77 / 26.03 s | 5.32 / 31.31 s | 5.02 / 10.47 s | 49.19 s |
| 03:38 run | 9.42 / 46.77 s | 5.24 / 9.53 s | 3.58 / 6.43 s | 59.00 s |
| **Reported slow** | **12.65 / 27.62 s** | **6.95 / 23.27 s** | **3.98 / 25.14 s** | **73.15 s** |
| Adjacent repeat | 15.23 / 49.78 s | 4.62 / 15.27 s | 2.40 / 7.50 s | 61.42 s |

All stage percentiles use the same per-workflow event definitions, cohort-scoped workflow IDs, and nearest-rank method. In the three reference exports, each of the 4,000 workflows has exactly three native workflow claim events, two policy-step starts, and one native activity-result event; claim rows have no attempt/task correlation key, but the observed result is paired to the nearest subsequent claim in the same workflow. Each cohort has 4,000 valid nonnegative pairs per displayed stage, with zero missing pairs, negative intervals, or ambiguous pairs. In all 4,000 workflows per cohort, the native claims are ordered claim 1 < claim 2 < result < claim 3, and policy starts are ordered start 1 < result < start 2. Claim/result attempt fields are null and both policy-start rows have attempt 0; thus these captured event pairs do not cross attempts, although claims lack an explicit correlation key. The old min/max selection produced the same displayed values for the later cohorts, but the revised derivation makes temporal pairing explicit and applies the same definitions to the three reference exports.

Against the reference-run median, the slow cohort's created-to-first-claim p50/p95 are higher (12.65/27.62 s vs 5.77/21.88 s). Its start-to-result p95 is 23.27 s vs reference median 18.60 s, while its p50 (6.95 s) falls within the reference p50 range (5.32–10.25 s). Its result-to-resume p95 is higher than each reference p95 (25.14 s vs 10.47–19.75 s), even though its p50 (3.98 s) is below the reference median 7.43 s. These are observed interval differences, not proof that a particular service owns the delay. The independent stage percentiles are not additive and must not be summed to explain workflow duration.

The Prometheus `workflow_hub_worker_execution_duration_seconds_count` series has **16 distinct processor service-instance IDs with positive activity counts during the slow interval**; they sum to 4,000 activity executions. The adjacent interval has 16 active IDs as well. This supports 16 active processor instances and does not support blaming a 4-replica stack. Per-instance counts were uneven (slow run min 102, max 1,120; next min 5, max 795), but the faster adjacent run was also skewed, so skew alone is not established as the cause. The ownership-lab container evidence file lists only `worker-1` through `worker-4` by design (`owner_recovery_lab.py`); it is not a full replica inventory.

## Runtime identity, target semantics, limits, and next step

At the slow cohort's capture, the Hub containers had started at 03:59:59.540Z and 03:59:59.773Z, about **13.6 s before** the first workflow. They used image IDs `sha256:9641d658…` and `sha256:9fc6e623…`; the next cohort used the same images. The prior reference report captured Hubs started at 03:14:43Z, over eight minutes before its first cohort. This changed warm-up context is a cold-start hypothesis only: it could contribute to the slow first run and faster adjacent result, but there was no controlled warm-vs-cold run. The slow and adjacent Hub snapshots both show outbox disabled and the same listed admission/batch/sharding settings. The 03:38 cohort differs on outbox enablement and had Hubs started about 21 s before its first workflow; that cohort completed at 62.38 WF/s, so Hub restart age alone does not explain the 47.97 result.

The source commit recorded for the later runs is `f47dfd36e910097b0ae21d98ba5e915338138b71`, a child of the reference commit `e9c6c4e3e60f17e2cc29ee8d04b9e964bad7380c`; the commit delta does not touch Hub SQL or the observability Make/Compose files. The Hub worktree source was modified at both captures. Historical PSS confirms the same CASE-form query fingerprint, but the later container artifact does not include a Hub DLL hash, so a binary byte-for-byte comparison is unavailable.

`make obs-test-4k` in the [Makefile](../../../Makefile) dispatches `owner-recovery.ps1 test-4k`. That [test-4k branch](../../../scripts/owner-recovery.ps1) waits for the existing policy definitions and runs k6; it does **not** call `Start-Lab` or scale/restart processors. `OBS_PROCESSORS ?= 4` is only used when an up/start action scales the stack. Thus the actual topology is inherited from whatever service startup ran previously; passing `OBS_PROCESSORS=16` to the test target does not by itself establish topology. The 16 active processor metric instances are the best historical replica evidence in these artifacts. The source JSON did not preserve the exact Make command-line arguments, and the [lab's container snapshot](../../observability/benchmark/owner_recovery_lab.py) intentionally inspects only `worker-1` through `worker-4`, so it is not a full replica inventory.

The runs are real and varied: 47.97, 60.54, and 62.38 WF/s after the 72.28–76.49 WF/s reference set. The database-size gauge and durable rows confirm that more workflow history accumulated between the reference and later cohorts, but no per-run relation-size/auto-analyze snapshot exists for the later runs. No general CPU gain is claimed, and the earlier 124.14/107.53/187.48 CPU-second observations remain unmatched by dataset, cache, service-age, and measurement window. PostgreSQL CPU results here are observational and shared with any background database work.

**Smallest next measurement:** keep the desired 16-replica stack and flags fixed, capture a complete `docker compose ps` plus nonsecret service identity immediately before k6, and leave the same Hubs up for a fixed warm interval before starting one repeat. During that run, retain per-instance candidate/claim admission, inflight, worker completion, and ownership-partition counters alongside the existing Prometheus/PostgreSQL series. This will test the cold-start and queue skew hypotheses without changing engine logic; no additional load or setting change was performed for this diagnosis.

Raw evidence and reproduction metadata are in `C:/Users/haido-ws/AppData/Roaming/orca/chat-scratch/740b1a1da96a083620bbe9d96f86cc40/throughput-variance-luna/`:

- `capture_prom.py` and `prometheus-process-target.json` record the exact read-only Prometheus queries and scrape target configuration.
- `pg_cpu_counter.json`, `pg_cpu_timestamp.json`, `pg_cpu_rate_1m.json`, and `postgres-process-cpu-dedup.csv` preserve raw process counters/rates plus deduplicated source samples.
- `hub_task_table_*.json`, `pg_statement_elapsed.json`, `pg_statement_shared_reads.json`, `pg_connections.json`, `pg_db_size.json`, `postgresql_bgwriter_checkpoint.json`, `pg_hub_task_vacuum.json`, and the per-fingerprint PSS range files preserve the raw table and SQL telemetry.
- `cohort-summary.csv/json`, `cohort-prom-summary.json`, `command-audit-cpu-summary.json`, `event-stage-summary.json`, `event-stage-summary-all-cohorts-v2.json`, `event_stages_all_cohorts_v2.py`, `worker-instance-execution-deltas.json`, and `safe-container-identity.json` are compact derived artifacts without workflow payloads or credentials. The v2 stage summary covers all six cohorts; its extraction script records the exact scope, temporal pairing, nearest-rank percentile method, and missing/negative/ambiguous pair counts. Reproduce with `python C:/Users/haido-ws/AppData/Roaming/orca/chat-scratch/740b1a1da96a083620bbe9d96f86cc40/throughput-variance-luna/event_stages_all_cohorts_v2.py`.
- The three reference cohort workflow exports used for the added rows are `C:/Users/haido-ws/AppData/Roaming/orca/chat-scratch/740b1a1da96a083620bbe9d96f86cc40/post-stale-fix-4k-luna/runs/<cohort-id>/k6-unordered-<cohort-id>/{hub_workflow.json,hub_workflow_event.json}`. The later cohorts are under `artifacts/owner-recovery/<cohort-id>` with `source.json`, `analysis.json` (audit), and `k6-unordered-<cohort-id>/{k6-policy-summary.json,cohort.json,hub_workflow.json,hub_workflow_event.json,hub_task.json,hub_task_attempt.json}`. The prior 16-processor baseline and CPU-window evidence is under `C:/Users/haido-ws/AppData/Roaming/orca/chat-scratch/740b1a1da96a083620bbe9d96f86cc40/post-stale-fix-4k-luna/`.

No services, database state, statistics, settings, or source code were changed during this read-only diagnosis.

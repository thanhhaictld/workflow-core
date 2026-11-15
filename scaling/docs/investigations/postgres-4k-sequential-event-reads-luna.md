# Why the Hub event table shows sequential rows read

## Finding

The orange `hub_workflow_event` line is a **post-completion owner-recovery audit export**, not a SQL call rate and not 16,000 application queries per second. The one-minute rate formula comes from the [PostgreSQL dashboard](../../observability/grafana/dashboards/postgresql.json#L62). The benchmark's [`export_run`](../../observability/benchmark/owner_recovery_lab.py#L199) runs one `SELECT * ... WHERE workflow_id IN (SELECT id ... WHERE run_id=...)` per exported table, then `json_agg`s the selected cohort to JSON files. For the cohort visible in this screenshot, PostgreSQL scanned nearly the entire 898,501-row event history to return 56,000 cohort events. The timestamped counters show one event-table sequential scan in the captured bracket.

The Hub runtime's [event reads](../../src/WorkflowCore.Hub/HubWorkflowStore.cs#L498) found in source are workflow-scoped (`WHERE e.workflow_id=@id`) and have the existing [`(workflow_id, occurred_at, id)` index](../../sql/010_hub_remote_scheduler.sql#L65) available; they are not this broad exporter query. All 4,000 workflows had completed before the audit exports were written, so this read spike cannot explain that cohort's measured workflow throughput. The root cause of any separate runtime `hub_workflow` sequential reads remains unassigned.

## Screenshot and matched cohort

The screenshot axis is approximately **2026-11-15 11:52–11:56 Asia/Saigon (UTC+07:00)**, or **04:52–04:56 UTC**. The pictured maximum matches the one-minute Prometheus rate query in the current PostgreSQL dashboard:

`sum by (relname) (rate(workflow_postgres_table_seq_rows_read_total{schemaname="wfc_scale"}[1m]))`

The event-table peak was **16,078.38 rows/s at 04:55:30Z (11:55:30 local)**. The other larger peaks in that same one-minute rate chart were task 4,593.82, workflow 2,296.91, and attempt 1,148.46 rows/s, also near 04:55:30Z. These are rolling rates of tuple visits, not SQL call rates, and they do not mean a query ran for a minute. A single scan of roughly 0.9 million tuples in the rolling window accounts for an event rate of this order.

The matching cohort is `k6-unordered-20261115t045400755-9963152b`. Durable workflow exports show first creation at **04:54:02.069Z**, last completion at **04:55:09.878Z**, and 4,000 completed workflows. Its durable audit records 16,000 tasks, 4,000 activity attempts, zero failed workflows, and zero extra attempts. Export files were written after completion: workflows at 04:55:13Z, tasks and attempts at 04:55:15Z, events at 04:55:16Z, and the audit analysis at 04:55:17Z. The event JSON has **56,000 rows and 27,820,003 bytes**.

## Counter and statement evidence

The historical Prometheus requests cover 04:50–04:58Z at 5-second evaluations. I deduplicated held evaluations using `timestamp(metric)`: the SQL collector's source samples were about 5 seconds apart; PostgreSQL process-exporter samples were about 15 seconds apart. The same source-sample bracket for the PostgreSQL table and statement counters was **04:53:55.530–04:57:04.341Z (188.811 s)**, slightly wider than the screenshot labels.

| Table | Sequential scans in bracket | `seq_tup_read` delta | Index scans / index tuple fetches delta | Audit output rows | PSS audit elapsed / shared blocks read delta |
|---|---:|---:|---:|---:|---:|
| `hub_workflow_event` | 1 | **896,000** | 2 / 0 | 56,000 | **355.279 ms / 30,964** |
| `hub_task` | 1 | 256,000 | 244,092 / 8,630,477 | 16,000 | **616.802 ms / 50,416** |
| `hub_task_attempt` | 1 | 64,000 | 56,307 / 44,358 | 4,000 | **29.560 ms / 1,288** |
| `hub_workflow` | 2 | 128,000 | 324,754 / 1,025,475 | 4,000 | **63.142 ms / 0** |

PSS query IDs, in table order, are `8606891824003119563`, `-3429427022871835021`, `4577757376092003997`, and `382766534141941589`. The first three are the full-row JSON export query shapes. During the same source-sample bracket, event/task/attempt export statements read **82,668 shared blocks total (about 646 MiB at 8 KiB per block)**. Shared-buffer reads are not a count of physical disk I/O; the PSS elapsed values include waits and are not CPU. In the same bracket, runtime event-write fingerprints accumulated 2,905.515 ms for batched history inserts (query ID `-821587332231162083`, 304 shared blocks read), 2,007.090 ms for single event inserts (`147205577398578030`, 5,158 blocks), and 957.983 ms for task-claim event inserts (`7314323819383311026`, 7,471 blocks). These are durable writes; they are separate from the exporter statement that accounts for the event sequential scan.

The selected-ID event export returned 56,000 rows while the table counter advanced 896,000 rows: **about 16 history rows visited per exported row**. Current nearby catalog context, captured later at 05:01:23Z, estimates 898,501 live event rows, 895,969 `reltuples`, 31,249 heap pages, and a 256.4 MB heap (334.0 MB including indexes). The PSS event-export delta of 30,964 shared blocks read is 99.1% of those heap pages. These independent measurements match one near-full event-table scan. The current PSS entry has 16 calls and 16 aggregate result rows at that 05:01 snapshot; a later read-only snapshot at 05:28 showed 18 cumulative calls. Prometheus did not expose a historical PSS call-count series, so those cumulative values are not presented as an exact call count for the 04:54–04:57 bracket.

The same one-scan pattern appears in task and attempt history: their counter deltas are essentially one current-table pass, while the owner audit exports only the run's 16,000 tasks and 4,000 attempts. The workflow audit query differs: it uses `ix_hub_workflow_run` on `run_id`, with zero PSS shared-block reads in the bracket. Its separate two-scan / 128,000-row table-counter delta therefore came from another query or runtime work; the available evidence does not identify which. The event rate, in contrast, is directly explained by the event-export fingerprint and the one-scan counter delta.

The screenshot also shows many small-table scans. In the same bracket, `hub_definition` had 8,002 sequential scans and 24,006 rows read; `hub_activity_registration` had 4,000 scans and 12,000 rows read. At the later read-only snapshot each table had about three live rows. Their screenshot-window one-minute peaks were 473 rows/s (about 158 scans/s) and 212 rows/s (about 71 scans/s), respectively: roughly three rows per scan, not a broad history walk. The matching registration/definition lookup statements accumulated about 232 ms of PSS elapsed time in the bracket and at most one shared block read. These are frequent tiny lookups; changing their cadence is not justified by this evidence.

Nearby PostgreSQL process CPU was 174.36 CPU-seconds across a 199.572-second source-sample bracket (0.874 mean cores), with a **2.794-core one-minute peak**. This includes workflow execution, the audit, all other PostgreSQL processes, and background activity; no per-query CPU measurement exists. The audit's 355 ms PSS elapsed is not 355 ms of PostgreSQL CPU, and this diagnosis makes no CPU-gain claim.

## Current plan evidence (cost-only)

I ran cost-only `EXPLAIN (COSTS TRUE, FORMAT JSON)` for the exact cohort export SQL against the shared local database. The event query's current plan is a workflow `run_id` index scan followed by a hash join whose event side is a full sequential scan. The task and attempt export plans also scan their full tables; the workflow export uses `ix_hub_workflow_run`. These are estimates from the later 05:01-era database state, not historical actual plans, and no `EXPLAIN ANALYZE`, data change, `ANALYZE`, or other live mutation was performed.

The event trace index already exists as `ix_hub_workflow_event_trace(workflow_id, occurred_at, id)`. A narrow per-workflow lookup can use its leading key, but the baseline `IN (SELECT ...)` lets the planner choose a hash join and sequential scan when the cohort is a small fraction of history. The screenshot is therefore explained by audit SQL shape, not a missing event index.

## Isolated SQL comparison

I tested one prototype only, without changing runtime or audit source: materialize cohort workflow IDs, then retrieve each workflow's events through a LATERAL subquery with `OFFSET 0`. The saved actual plan, rather than the SQL syntax, is the evidence that this prototype selected the trace index: for the large fixture it used `ix_hub_workflow_event_trace` in 4,000 bitmap-index probes with `workflow_id = w.id`, followed by 4,000 bitmap heap visits averaging 14 event rows each. The baseline scanned all 898,501 rows once and returned 56,000 after the hash join. Both complete JSON exports had 56,000 rows, exactly 29,068,893 bytes, and matching sorted-by-event-ID SHA-256 row-multiset hashes.

PostgreSQL 17.11 ran in a labeled, isolated container with no network, 1 CPU and 1 GiB memory. The large synthetic fixture matched the live row counts (66,472 workflows, 898,501 events, 4,000 selected workflows and 56,000 selected events, 6.2% selectivity); its event heap was 286.9 MB versus the live 256.4 MB, and its JSON output was 4.5% larger. A smaller fixture used 7,000 workflows / 98,000 events with the same 56,000-event cohort (57.1% selectivity). Both shapes ran on the same data in each fixture, with three alternating `EXPLAIN ANALYZE BUFFERS SERIALIZE TEXT` measurements per shape in the repeat pass.

| Synthetic fixture | Baseline median total time / shared blocks | LATERAL prototype median total time / shared blocks | Observation |
|---|---:|---:|---|
| Large history, 6.2% selected | 389.845 ms / 35,066 | **287.445 ms / 18,442** | 26.3% lower elapsed and 47.4% fewer shared-block touches in the repeat pass; candidate median had zero block reads because its selected pages were already cached. |
| Small history, 57.1% selected | **233.934 ms / 3,776** | 276.033 ms / 18,442 | Candidate was 18.0% slower and touched 4.9× as many blocks; a sequential scan is better when much of the table is returned. |

The first isolated pass also had a cold candidate result on the large fixture: **1,586.838 ms total and 5,281 shared blocks read**, versus 393.231 ms for that pass's baseline median. A second pass's first candidate was 383.653 ms with the same 5,281 read blocks, then subsequent candidate runs had no shared reads. This variability is retained, not averaged away. The prototype reduces rows visited in the large fixture, but its cold-start elapsed cost is not reliably lower; the small fixture is a measured regression. These synthetic elapsed times are not CPU or production-throughput measurements.

## Next step and limits

The most justified follow-up is to test a cohort-aware export plan in the benchmark audit only: compare its selected workflow count with table history, use existing leading workflow-ID indexes when the cohort is small, and retain a sequential-scan path when the cohort is large. Validate both paths on the live-like row widths and cold/warm cache cases before changing `owner_recovery_lab.py`. The current data does not establish a safe selectivity threshold, does not test task/attempt rewrite shapes, and does not justify any runtime Hub change or index migration. The audit query runs after durable workflow completion, so changing it can reduce diagnostic read overhead but cannot improve the just-finished workflow rate.

The event/task/attempt output reads are benchmark instrumentation. The 2,296 rows/s peak for `hub_workflow` is separate and unassigned, and other sequential scans may exist outside this cohort. At the captured workload time, the measured table/statement counters include both workload activity and the audit, while exporter files establish that the full-table JSON reads occurred after workflow completion. No shared load, service refresh, runtime/source/config change, database mutation, reset, or tuning was performed. The isolated PostgreSQL container and its anonymous ephemeral data volume were removed at exit; no shared containers or named volumes were touched.

## Evidence and reproduction

The harness and synthetic SQL are in [`scaling/benchmarks/hub-audit-export-experiment`](../../benchmarks/hub-audit-export-experiment/README.md); reproduce with its documented Python command. The harness recorded container labels, image digest, resource limits, SQL hashes, fixture counts, plans, full synthetic exports, and output-equivalence hashes. Its first invocation failed before plan collection due to a missing SQL quote in the fixture-size snapshot; the harness was corrected and two successful isolated passes were retained. That failure did not touch the shared database or leave a container/volume behind.

Raw Prometheus ranges, source timestamps, read-only database snapshot, cost-only live plans, synthetic actual plans and exports, plus the derived JSON summary are under:

`C:/Users/haido-ws/AppData/Roaming/orca/chat-scratch/740b1a1da96a083620bbe9d96f86cc40/sequential-event-reads-luna/`

Key files are `prom-range-manifest.json`, `prom-counter-manifest.json`, `seq_rows_rate_by_table.json`, `seq_scans_rate_by_table.json`, the `*_counter_samples.json` / `*_counter_source_timestamp.json` pairs, `live-db-readonly-snapshot.txt`, `live-event-pg-stats.txt`, `live-plans/`, `live-export-pss-current.txt`, `isolated/`, `isolated-repeat/`, and `historical-and-isolated-summary.json`. The read-only current snapshot is later context; `timestamp(metric)`-deduplicated Prometheus ranges and their recorded boundaries are the historical evidence.

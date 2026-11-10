# Performance tuning harness instructions

Use this as an execution brief for an agent investigating Hub throughput. Creating
this document does not execute the workload. When the user invokes the harness,
carry out the sequence below and produce an evidence-linked bottleneck report.

## Objective and completion criteria

Bring up the existing recovery stack, establish idle and loaded baselines, locate
the constraint that limits **durably completed workflows per second**, and test
one targeted improvement at a time. Observe PostgreSQL, both Hubs, processors,
the load balancer, and the load generator throughout each experiment.

Completion requires a report identifying the strongest measured bottleneck,
its query or code path, evidence supporting causality, tested changes, correctness
results, remaining uncertainty, and the next constraint after any improvement.
An unsuccessful tuning experiment is a valid result; unsupported claims of a
speedup or a database bottleneck are not.

Read `AGENTS.md`, [current runtime](current-runtime.md),
[scheduler runbook](hub-remote-scheduler-implementation.md), and
[owner recovery instructions](../observability/owner-recovery.md) first. Inspect
current source and launcher arguments; historical reports are investigation leads.

## Constraints

- Preserve the existing PostgreSQL volume and history. Do not reset the database,
  truncate tables, prune Docker volumes, or reset statistics to simplify comparison.
- Preserve lease epochs, ownership fencing, retries, atomic state/history changes,
  resource capacity, FIFO ordering, and idempotency. Keep processors DB-free.
- Use one observability project for a series. Keep the matching partition count
  unchanged for an existing namespace; it is not a live tuning control.
- Keep monitoring configuration identical between control and candidate runs.
  Leave the monitor stopped for the initial measurements. Do not open browser
  dashboards during a measured window; collect metrics through their APIs.
- Do not run overlapping cohorts, DB integration tests, image builds, or unrelated
  load during measurement. Drain each cohort before scaling down its processors.
- Fault injection is a separate, explicitly selected phase. The normal performance
  suite does not kill owners or relax correctness checks.
- Keep tokens, passwords, connection strings, and workflow payloads out of reports.
  Inspect resolved configuration in memory and export only an allowlist of settings.
- Do not disable observability, increase policy capacity, or reduce business work
  and then describe the result as an equivalent throughput improvement.

## 1. Prepare and start the services

Run from the repository root with PowerShell 7, Docker Compose, Python 3, and
.NET SDK 10 when building/testing on the host. Reuse the existing `.env`.

```powershell
pwsh -NoProfile -File scripts/owner-recovery.ps1 setup
pwsh -NoProfile -File scripts/owner-recovery.ps1 up
pwsh -NoProfile -File scripts/owner-recovery.ps1 ps
```

The `up` task builds current application images, starts observability, applies
ordered migrations, waits for both Hubs/LB, starts processors, and checks benchmark
registration. At the time of writing it explicitly scales to **eight processors**;
the overlay's replica declaration is not proof of the running count. This task
also starts the monitor. Stop it before measuring using the helper below.

If Prometheus or a service is unavailable, inspect health/logs and restore the
stack before drawing conclusions. Preserve the actual startup error when recovery
fails. Do not report missing metrics as zero or substitute historical measurements.

Use this helper for later commands so that every operation uses the same layers
and worktree-derived project identity:

```powershell
$perfFiles = @(
    '-f', 'scaling/observability/compose.yaml',
    '-f', 'scaling/observability/compose.matching-ha.yaml',
    '-f', 'scaling/observability/compose.matching-shards.yaml',
    '-f', 'scaling/observability/compose.owner-recovery.yaml',
    '--env-file', 'scaling/observability/.env',
    '--profile', 'offboarding-load',
    '--profile', 'policy-k6',
    '--profile', 'owner-tests'
)
function Invoke-PerfCompose {
    & pwsh -NoProfile -File scaling/observability/compose.ps1 @perfFiles @args
    if ($LASTEXITCODE) { throw "Compose failed: $args" }
}
Invoke-PerfCompose stop monitor
Invoke-PerfCompose ps
```

If using a custom env file, project, or port base, pass the same choices to **all**
launcher and Compose commands; adjust the helper accordingly. Discover ports from
resolved Compose configuration instead of assuming defaults. Default endpoints:
Hub LB `5110`, Hubs `5100/5101`, Prometheus `9090`, collector `9464`, Grafana `3001`.

Create `artifacts/performance-harness/<UTC timestamp>/`. Record:

- UTC timestamps, Git commit, dirty state and relevant patch, image IDs/digests;
- actual service/replica counts, CPU/memory allocations and Docker host limits;
- worker slots, Hub pool/admission limits, scheduler cadence, matching count,
  transport/claim/history flags, policy mode, workload count, VUs and timeout;
- retained table sizes, row estimates, cache/warmup state, and active backlog;
- monitor state, collector/scrape intervals, instrumentation configuration.

Never export raw `docker inspect` environments or unredacted Compose configuration.

## 2. Verify correctness and establish idle cost

Before load, run a smoke cohort and the applicable database tests:

```powershell
pwsh -NoProfile -File scripts/owner-recovery.ps1 smoke-test
pwsh -NoProfile -File scripts/owner-recovery.ps1 db-test `
    -Filter 'FullyQualifiedName~HubNativeRuntimeTests|FullyQualifiedName~HubUnconstrainedBatchClaimTests|FullyQualifiedName~HubExplicitOrderingTests'
```

The launcher uses an isolated PostgreSQL test project and verifies a nonempty TRX.
Save the TRX and report its actual executed count. For a SQL/lease/ordering change,
include the tests covering that change, not just the example filter. Tests that
return early without `WFC_TEST_PG` are not database validation.

Wait for submitted work to finish, then collect **at least two 60-second idle
windows**. Keep the same processors, Hubs and collectors running. Verify no ready
or leased work before the window and verify the state afterwards; otherwise label
the window as recovery/drain rather than idle. Perform expensive verification
queries outside the timed interval so their reads do not contaminate it.

Take PostgreSQL statistics snapshots at each boundary. Collect service CPU/memory
and metrics concurrently. Do not infer idle cost from a one-minute rate immediately
after load: its window may still include execution traffic.

Known investigation leads, to remeasure rather than assume:

- `HubMetricsSampler` samples diagnostics every five seconds per Hub and invokes
  `HubOperationsStore.RecordSampleAsync` once per minute per Hub.
- `RecordSampleAsync` currently aggregates retained task history. Two Hubs scanning
  66,880 rows once each per minute can average about 2,229 sequential rows/second,
  despite zero new workflow executions and no monitor UI.
- Timer/native reconciliation, readiness recovery, owner renewal, and empty claims
  continue while workers are idle. Their costs must be separated from useful work.
- High row counts alone do not establish a throughput bottleneck. Measure elapsed
  time, CPU, I/O, waits, and contention during load before prioritizing a change.

## 3. Capture evidence for every measured cohort

Start collection **before** submission; stop only after durable completion/drain.
Use UTC boundary timestamps for all sources, and store raw samples plus derived
deltas in each cohort directory. Poll cheap metrics about every five seconds;
sample PostgreSQL waits every one to five seconds. Do not run heavy plans in a
timed workload. Keep collection overhead consistent between comparisons.

### PostgreSQL snapshots

Discover the runtime PostgreSQL container through Compose labels/service identity;
do not use the isolated test database. Use `psql -X -v ON_ERROR_STOP=1`, the resolved
user/database, and a statement timeout. Save these query results before and after:

```sql
SELECT clock_timestamp(), pg_postmaster_start_time();
SELECT * FROM pg_stat_statements_info;

SELECT datname,xact_commit,xact_rollback,blks_read,blks_hit,
       tup_returned,tup_fetched,tup_inserted,tup_updated,tup_deleted,
       temp_files,temp_bytes,deadlocks,stats_reset
FROM pg_stat_database WHERE datname=current_database();

SELECT relname,seq_scan,seq_tup_read,idx_scan,idx_tup_fetch,
       n_live_tup,n_dead_tup,last_autovacuum,last_autoanalyze
FROM pg_stat_user_tables WHERE schemaname='wfc_scale';

SELECT relname,indexrelname,idx_scan,idx_tup_read,idx_tup_fetch
FROM pg_stat_user_indexes WHERE schemaname='wfc_scale';

SELECT queryid::text AS query_id,calls,rows,total_exec_time,
       shared_blks_hit,shared_blks_read,shared_blks_dirtied,
       temp_blks_read,temp_blks_written,wal_bytes,query
FROM pg_stat_statements
WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database());

SELECT wal_records,wal_fpi,wal_bytes,stats_reset FROM pg_stat_wal;
```

Keep normalized SQL in private local evidence only after checking it contains no
sensitive literals. Use fingerprints and source links in shared reports. Preserve
query IDs as strings to avoid integer precision loss in JSON tooling.

At intervals, sample active backends and blockers without exporting payloads:

```sql
SELECT clock_timestamp(),pid,application_name,state,wait_event_type,wait_event,
       clock_timestamp()-query_start AS query_age,
       clock_timestamp()-xact_start AS transaction_age,
       pg_blocking_pids(pid) AS blocking_pids,query_id::text
FROM pg_stat_activity
WHERE datname=current_database() AND pid<>pg_backend_pid();
```

Compute **deltas**, not rankings by lifetime totals. Rank statements by execution
time delta, call delta, shared-buffer reads/hits, temp I/O and WAL; calculate
calls and SQL execution ms per durably completed workflow. SQL elapsed execution
time includes waits and concurrent calls; it is not CPU time. Mean-time deltas
must be `delta(total_exec_time)/delta(calls)`, not a subtraction of lifetime means.
Buffer hits are logical accesses, not unique pages. Buffer reads are not proof of
physical device reads because the operating system may cache them.

Record restarts, statistics resets, entry eviction and missing/new fingerprints;
invalidate or qualify affected deltas. `pg_stat_wal` is cluster-wide; do not call
its changes database-specific. Measure startup/registration separately from idle.

### Prometheus and service metrics

Discover emitted metric names and resource labels through `/api/v1/label/__name__/values`
and `/api/v1/series`. Export time series with `/api/v1/query_range` for the exact
cohort UTC interval; retain the PromQL, step, labels and raw JSON. Missing series,
insufficient histogram samples, resets and collection gaps must remain visible.

Useful queries, subject to verifying current names/labels:

```promql
sum(rate(workflow_hub_workflows_completed_total[1m]))
histogram_quantile(0.95, sum by(le)(rate(workflow_hub_workflow_duration_seconds_bucket[1m])))
max(workflow_hub_queue_ready)
max(workflow_hub_queue_leased)
sum by(relname)(rate(workflow_postgres_table_seq_rows_read_total[1m]))
sum by(relname)(rate(workflow_postgres_table_index_rows_fetched_total[1m]))
sum by(relname)(rate(workflow_postgres_table_index_scans_total[1m]))
sum by(query_id)(rate(workflow_postgres_statement_execution_time_ms_total[1m]))
sum(rate(namedprocess_namegroup_cpu_seconds_total{job="postgres-process",groupname="postgres"}[1m]))
histogram_quantile(0.95, sum by(le)(rate(workflow_hub_claim_admission_wait_seconds_bucket[1m])))
histogram_quantile(0.95, sum by(le)(rate(workflow_hub_claim_db_connection_acquire_seconds_bucket[1m])))
histogram_quantile(0.95, sum by(le)(rate(workflow_hub_native_event_lane_lock_wait_seconds_bucket[1m])))
```

The queue gauges are database-wide snapshots duplicated by Hubs: do not sum them
as independent queues. Count only the intended database/process/exporter series;
avoid duplicates. Application completion counters need durable cohort validation.
Per-table row rates measure rows read/fetched, not SQL calls or task executions.
Index scans can return zero rows, and index-only scans can have zero heap fetches.

Also capture per-service CPU, memory, network/block I/O, HTTP latency/errors,
worker inflight counts, execution/body/completion latency, claim amplification,
batch sizes, empty claims, pool/admission waits, scheduler lag, GC pauses and
thread-pool queueing where available. `docker stats --no-stream` can supply
service resource samples when no corresponding metric exists; timestamp samples
and include the load generator/observability containers. Capture host CPU pressure,
Docker VM CPU/memory limits, disk pressure and throttling where accessible.
Normalize CPU conventions: roughly 100% Docker CPU represents one core.

Capture bounded logs for both Hubs, processors, LB, PostgreSQL and collector.
Correlate representative slow traces across claim, execution, completion, and
native commit. Resource identifiers belong in private traces, not metric labels.

## 4. Run controlled throughput experiments

Use `stress-test` with explicit arguments instead of relying on stale help text
or the fixed `test-4k` shortcut. Each invocation generates a unique cohort/run ID,
saves k6 evidence, exports durable rows, and audits completion and ordering.

```powershell
pwsh -NoProfile -File scripts/owner-recovery.ps1 stress-test `
    -StressMode unordered -Count 1000 -Vus 100 -TimeoutSeconds 600 `
    -Results artifacts/performance-harness/owner-recovery
```

Run unordered mode first to expose infrastructure limits. Then run `partition`
and `keyed` separately to distinguish policy serialization from runtime overhead.
Keep count, VUs, policy keys, activity implementation/duration, lane sizes, and
Hub/DB resources identical within each comparison. Keyed mode currently uses
eight stable resource keys; an ordering/capacity ceiling can be intentional.

Calibrate cohort size before the comparison: aim for at least 60 seconds of useful
load and several telemetry samples. If 1,000 workflows finish too quickly, raise
count for **all** compared cases (for example 4,000). Submission plus drain is a
finite-cohort experiment, not proof of sustainable open-loop capacity. If the
load driver cannot maintain backlog, report the submission limit and do not claim
the runtime has reached its execution ceiling.

The current policy workload also polls run summaries during drain. Include this
observer traffic consistently; stopping the monitor does not remove all monitoring.

For the processor sweep, use 1, 2, 4, 8, and 16 replicas where host capacity permits:

```powershell
$processorCount = 8
Invoke-PerfCompose up -d --no-deps --wait --scale "processor=$processorCount" processor
```

Verify the actual healthy count and registrations after each change. `up` in the
recovery launcher resets the scale to its own default; reapply the selected scale
after any rebuild/start. Do not change matching partitions between these points.
Use at least three measured repetitions per point, a separate warmup, and a
bracketed control (for example 8 -> 16 -> 8). Record growing history and vacuum/cache
state; do not erase history to make later runs appear comparable. If needed, use
an explicitly prepared isolated benchmark database with the same starting dataset.

Report both:

- Cohort throughput: durable completed count divided by the interval from the
  cohort's first creation to its last completion, with drain included.
- The completion-rate curve and queue depth through submission, steady backlog,
  and drain; include a steady-backlog rate only when that phase actually exists.

Save completion p50/p95/p99, queue/claim latency, failures, retries/lease expiry,
and end-to-end time. k6 submission latency and HTTP request rate are not workflow
completion latency or throughput. Compute `S(N)=T(N)/T(1)` and `E(N)=S(N)/N` only
for comparable measurements. Shared-host container scaling is not multi-host
linear-scaling evidence.

## 5. Identify and test the limiting mechanism

For each hypothesis, state the observation, causal mechanism, predicted effect,
one controlled experiment, and a condition that would disprove it.

| Candidate constraint | Evidence to seek | Experiment to distinguish it |
| --- | --- | --- |
| PostgreSQL CPU/query work | PostgreSQL cores busy, high SQL calls/reads per workflow, costly plans | Reduce one proven query/scan cost; hold worker capacity fixed |
| DB I/O or sort spill | Read latency, temp blocks, disk pressure correlated with stalled completion | Compare one indexed/query rewrite under matched cache conditions |
| DB locks/serialization | Repeated blocker chains, wait time, event-lane/workflow/resource lock spans | Shorten the relevant transaction or avoid proven redundant work; preserve fencing |
| Hub CPU/memory | Hub CPU/GC/thread-pool pressure while DB and workers have spare capacity | Reduce one measured serialization/allocation/routing cost |
| Pool/admission limits | High client pool/admission wait with backend capacity available | Change one bounded limit; verify completion/heartbeat capacity remains available |
| Matching/readiness gaps | Ready backlog plus free worker slots, uneven shard drain, dispatch gaps | Trace hint -> claim -> SSE delivery; test one routing/batching change |
| Processor capacity | Worker slots consistently occupied; business body dominates | Increase processors without changing workload or Hub/DB resources |
| Policy/resource ceiling | Hot resource keys serialize work; FIFO/capacity is fully occupied | Compare unordered with constrained mode as a separate diagnostic workload |
| Load generator/shared host | Submission cannot sustain backlog, k6/host CPU saturated | Increase driver headroom or separate resources while preserving the workload |
| Idle/background work | Periodic history scans or no-row updates consume resources under load | Optimize the measured maintenance path and compare total/per-workflow cost |

Inspect the exact SQL/source for expensive fingerprints. Use plain `EXPLAIN` on
writes. Run `EXPLAIN (ANALYZE, BUFFERS)` on safe read-only equivalents outside
measurement, or on writes in an isolated disposable test database. A rollback
does not make arbitrary side effects safe on the retained runtime database.
Report estimated versus actual rows, scan/index conditions versus post-filters,
sort spills, loops and heap fetches. Do not force an index merely to improve the
appearance of a plan; verify end-to-end work and latency.

Change one mechanism at a time. Run relevant correctness tests, rebuild/recreate
the affected services, verify the running setting/image, restore the selected
replica count and monitor state, then repeat the identical measured workload.
Use control A -> candidate B -> control A where safe, at least three repetitions
per treatment. Keep the profiling setup identical; added instrumentation is itself
an experimental change.

Default promotion guardrail: median throughput improves by at least 10% and more
than control drift, completion p95 does not regress by more than 10%, and durable
correctness/error/retry checks show no regression. These are harness defaults,
not product SLOs. If control drift exceeds 10%, investigate and repeat before
making a causal claim. Preserve counterexamples and rejected experiments.

After a successful improvement, remeasure the critical path: the next bottleneck
may differ. Stop when the invoked scope is complete, measured constraints require
external resources, or further candidates lack evidence. Report that limit instead
of making speculative changes or claiming unmeasured maximum capacity.

## 6. Required report

Write `REPORT.md` in the run directory and link the raw evidence. Use this shape:

1. **Finding:** primary throughput constraint, affected service/query/source,
   confidence (`confirmed`, `strong evidence`, or `unproven`), tested throughput
   ceiling, and the concrete evidence linking the constraint to that ceiling.
2. **Environment:** images/source, host limits, retained data, topology, worker
   slots, flags, policy, observer configuration, run IDs and exact UTC windows.
3. **Correctness:** executed test counts, durable completion/audit results,
   retries/expiry/duplicates/FIFO checks, failures and excluded runs.
4. **Scaling results:** rows for every replica count/repetition and treatment;
   completed/failed counts, WF/s, completion p95/p99, queue latency, service CPU,
   DB waits/I/O, and efficiency. Include medians, spread and control drift.
5. **Database attribution:** idle versus loaded table/index deltas; top statement
   fingerprints with calls/ms/blocks per workflow; plans and source links. Label
   cluster-wide counters and explain the difference between CPU and SQL wall time.
6. **Service attribution:** Hub, worker, LB and driver utilization/latency, queue
   or shard imbalance, and representative critical-path traces.
7. **Experiments:** hypothesis, single change, predicted result, observed A/B/A
   result, correctness/latency/cost tradeoffs, and keep/reject decision.
8. **Recommendation:** highest-value next action and why it increases completed
   throughput, next bottleneck, uncertainty, required resources, and rollback.
9. **Final state:** running services/replicas, monitor state, active code/config,
   backlog, and saved artifacts. Preserve data and leave the recovered stack
   running unless the user requested shutdown.

The report must answer: **What prevents adding processors from increasing useful
throughput, and which measured change removes or reduces that constraint?** If the
experiment cannot answer it, name the missing observation and mark the hypothesis
unproven. Do not present an idle scan, a busy CPU, or one faster run as sufficient
causal evidence by itself.

## Invocation prompt

> Follow `scaling/docs/performance-tuning-harness.md`. Restore the existing recovery
> stack, keep the monitor stopped, run correctness/smoke checks, measure idle cost,
> and establish an unordered throughput baseline. Collect PostgreSQL and per-service
> evidence throughout controlled processor scaling. Test targeted changes one at
> a time with matched controls, then run the policy regressions. Produce the required
> bottleneck report with raw evidence, measured gains or rejected hypotheses, and
> the remaining throughput constraint. Preserve existing data and running services.

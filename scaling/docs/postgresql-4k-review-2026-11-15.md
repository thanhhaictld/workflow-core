# PostgreSQL review of two 4,000-workflow bursts

Read-only inspection on 15 November 2026, Asia/Saigon. The screenshot window
00:15–00:20 corresponds to 14 November 17:15–17:20 UTC. Evidence came from the
running worktree's PostgreSQL 17.11, Prometheus history, Docker resource settings,
and source. No services were restarted, workloads submitted, statistics reset,
or database settings/indexes changed. Observability changes below are source
changes awaiting deployment; no runtime performance improvement is claimed.

**Current implementation status:** the selected-ID batch query is now applied
in `HubTaskStore` source and qualified through the real store transaction.
The SDK 10 Release build and the full fresh-database PostgreSQL suite passed
(312 passed, 0 failed, 0 skipped). Fencing, attempt/history rollback,
lost-commit-response reconciliation, owner expiry/takeover and concurrent
claims have dedicated coverage. See the
[implementation report](investigations/postgres-4k-batch-implementation-luna.md).
The worktree-local benchmark stack has subsequently been rebuilt/refreshed with
the implementation and measured with three `make obs-test-4k` runs; see the
[post-integration benchmark report](investigations/postgres-4k-post-integration-benchmark-luna.md).
All three completed 4,000 workflows and 16,000 tasks without extra attempts or
dead tasks. Median throughput was 80.07 workflows/s and median workflow-window
PostgreSQL CPU was approximately 124.14 CPU-seconds (7.76 CPU-ms/task). Run 3
reached 187.48 CPU-seconds and a 3.83-core one-minute peak, with a large
batch-statement buffer-hit and broad-index-fetch spike. These observations do
not establish a consistent or controlled CPU gain. CPU window boundaries are
estimates from 15-second source scrapes. This was a local development refresh,
not an external/production deployment. The initial review and experiment below
record the earlier evidence and qualification boundaries at those stages.

## Consolidated Luna investigation decisions

Three supervised GPT-6 Luna investigations and their coordinator-requested
corrections are complete. Their initial findings established the investigation
order below. The subsequent isolated experiment reproduced SQL scan amplification
and measured a faster prototype, as recorded in the follow-up section; it does
not establish the historical CPU root cause.

| Priority | Investigator / evidence | Coordinator decision |
| --- | --- | --- |
| 1 | [Batch admission](investigations/postgres-4k-batch-admission-luna.md): cost-only custom/generic plans scanned broad task indexes before matching selected JSON IDs. | Strongest query-shape hypothesis. The probe manually reconstructed SQL, omitted `RETURNING`, and used an empty queue. Reproduce the exact source SQL under active backlog before implementing the proposed selected-ID lookup. A lateral join alone does not guarantee bounded probes. |
| 2 | [Discovery and recovery](investigations/postgres-4k-discovery-recovery-luna.md): demand/hint-driven bucket sweeps, same-horizon head rechecks, and nominal 500 ms inbox sweeps can repeat work. | Add per-stream/page-outcome and activation-reason measurements in an isolated baseline first. Retain the existing recovery cadence and head rechecks. Proposed 250 ms–5 s candidate and 1–5 s inbox schedules were withdrawn: they could increase SQL calls or recovery latency. |
| 3 | [Indexes and settings](investigations/postgres-4k-index-config-luna.md): 21 task indexes and a later cumulative snapshot of 144,000 updates with zero HOT updates are consistent with state-transition write costs. | Investigate index overlap only after query-plan work. Keep uniqueness, recovery and compatibility paths covered. No index removal or PostgreSQL memory/pool change is supported yet. |

The first optimization experiment should use a disposable PostgreSQL 17 instance
with the deployed migration set and the exact batch SQL, including `RETURNING`
and the original literal predicates. Seed an active backlog and compare batches
of 1/8/16/32 selected IDs at early and late sequence positions, under custom,
generic and actual Npgsql preparation behavior. Compare queue rows visited and
buffer hits per lease before proposing a rewrite. Require equivalent lease,
epoch, capability/pin, resource exclusion, history, concurrent-claim and ambiguous
commit behavior. Performance acceptance requires repeated lower CPU/buffer cost
per completed task without worse throughput or p95 latency. The SQL-only portion
has now run as documented below, and subsequent full-store qualification passed;
end-to-end performance acceptance remains open.

The investigators changed only their reports. The coordinator checked their
evidence boundaries and requested corrections to generic-plan wording, the
PostgreSQL 17 cost-only `EXPLAIN` recipe, and unmeasured backoff proposals.
Their read-only probes do not constitute a new workload benchmark. All services,
database settings, indexes and durable data remain as they were before this
investigation; the observability source changes described below remain pending
deployment.

## Isolated experiment follow-up: corrected deployment index set

The [Luna high experiment](investigations/postgres-4k-batch-experiment-luna-high.md)
completed 48 paired cells with five measured samples per arm/cell: batches
1/8/16/32, early/late selected IDs, custom/generic/unprepared execution, and
analyzed/stale-active statistics. The corrected run used migrations 002–027,
029 and 030, excluding optional 028 to match the observed deployed index set.
Earlier 028-inclusive results remain archived separately and are superseded for
this comparison. The extracted baseline SQL matches the source after whitespace
normalization and includes the full `RETURNING` projection.

The baseline scanned 50,000 active index entries to select 2,500 eligible rows
before matching a one-item JSON request in the analyzed fixture. The selected-ID
prototype instead used primary-key probes, and its median execution time was
lower in all 48 comparisons. Representative unprepared Npgsql results:

| Fixture / selected batch | Baseline median SQL ms | Prototype median SQL ms | Shared-buffer hits per lease, baseline → prototype |
| --- | ---: | ---: | ---: |
| Analyzed / 1 early ID | 8.436 | 0.251 | 1,552 → 45 |
| Analyzed / 32 late IDs | 9.988 | 1.969 | 87.66 → 46.34 |
| Stale active / 1 early ID | 11.690 | 0.232 | 2,299 → 47 |
| Stale active / 32 late IDs | 13.855 | 1.899 | 114.28 → 47.50 |

Actual baseline-versus-prototype checks used distinct SQL hashes and passed
per-item metadata/eligibility equality, unchanged rejected rows, and overlapping
opposite-order claims with `SKIP LOCKED`. Seventeen focused existing integration
tests passed against the **unchanged baseline store**. They did not qualify the
prototype's owner fencing, attempt/history transaction or ambiguous-commit flow.
An earlier baseline-only equality invocation was corrected and rerun rather than
counted as prototype coverage. The corrected result verifier retains read-bearing
and nonwinning samples instead of treating them as invalid trials.

This supports a selected-ID SQL rewrite on the synthetic workload. It does not
measure CPU reduction or workflow throughput: the prototype remains outside the
full store transaction, and rollback trials retain MVCC/cache drift limitations.
The subsequent implementation completed full-store test integration, including
fencing, journal, rollback/commit ambiguity and concurrent claims, as linked in
the current status above. Deployment and end-to-end measurement remain outside
the completed implementation. The experiment container is stopped with synthetic data retained; shared
services and database resources were not changed.

## Measured behavior

| Metric | First burst | Second burst |
| --- | ---: | ---: |
| UTC run ID prefix | `k6-unordered-20261114t171517030` | `k6-unordered-20261114t171732781` |
| Persisted/completed workflows | 4,000/4,000 | 4,000/4,000 |
| First creation to last completion | 58.06 s | 55.97 s |
| Workflows per second over that interval | 68.89 | 71.46 |
| Persisted workflow duration p95 | 50.71 s | 49.13 s |
| Completed tasks | 16,000 | 16,000 |
| Tasks with extra attempts / dead tasks | 0 / 0 | 0 / 0 |

Each workflow produced four durable tasks, so 4,000 workflows is not 4,000
database operations. Task attempts are not a count of SQL operations.

The five-minute window had an estimated **385.15 PostgreSQL CPU-seconds**,
approximately **48.14 ms per completed workflow / 12.04 ms per completed task**.
This includes background activity and the post-run audit. It is not an isolated
business-execution CPU measurement. Prometheus `increase` extrapolates between
samples; these totals are estimates.

The one-minute CPU rate peaked at **328.72%**, or **3.29 cores**. Docker reports
16 CPUs and about 31.2 GiB memory, with no PostgreSQL CPU/memory quota. This is
about 20.5% of that CPU count, not evidence that the entire VM is saturated.
Other containers share those CPUs. Idle PostgreSQL was about 2.4% of one core
when inspected, so the screenshots do not show a persistent idle CPU problem.

## What the scan panels mean

- `hub_task` is the dominant index-fetch series: about **20.82 million** live
  heap rows fetched through indexes in the window, about **651 per completed
  task**. A five-second evaluation of the one-minute rate peaks near 200k/s;
  the screenshot's coarser plot shows about 185k/s. This motivates investigation
  of repeated task access; table counters alone do not identify the statement.
- `hub_definition` and `hub_activity_registration` each contained only three
  live rows, and matching ownership only four. Their sequential scans alone
  do not justify more indexes.
- The later sequential-read peaks include `hub_workflow_event`, `hub_workflow`
  and `hub_task`. The benchmark verification queries are present in statement
  statistics, including full task JSON aggregation. Their timing is consistent
  with post-run auditing; a trace would be needed to attribute each scan.
- These are heap/index access counters, not unique rows, successful leases,
  physical disk IOPS or CPU time. PostgreSQL documents those distinctions in
  [cumulative statistics](https://www.postgresql.org/docs/17/monitoring-stats.html).

## Expensive statement families in this window

Ranked by increases in sampled `pg_stat_statements.total_exec_time`:

| Statement family | Query ID | Execution elapsed time |
| --- | --- | ---: |
| Cross-worker candidate batch admission | `-6050224743415277628` | 21.31 s |
| Fenced task completion update | `702976769898827252` | 19.91 s |
| Insert next native workflow task | `325984884542718125` | 14.35 s |
| Due-ready candidate discovery | `3737985904634580822` | 11.58 s |
| Batched workflow history insert | `-821587332231162083` | 8.06 s |
| Expired candidate discovery | `-5021478638737671992` | 6.47 s |
| Native event/exhausted-task reconciliation | `-5706908197263325917` | 3.78 s |

All sampled statements total about 129.27 execution seconds in this window.
**Execution elapsed time is not CPU attribution.** Planning and background
processes also consume CPU, and waits/concurrent execution complicate comparisons.
No conclusion about the unaccounted CPU follows from subtracting these totals.
See [pg_stat_statements](https://www.postgresql.org/docs/17/pgstatstatements.html).

A separate cumulative snapshot since 16:49:08 UTC spans **four** 4,000-workflow
runs; do not attribute its totals to these two bursts. It showed:

- Batch admission: 8,159 calls, 62,039 returned task rows, 5.018 ms mean
  execution, 21.85 million shared-buffer hits and 1,152 shared blocks read.
- Expired discovery: over 27k calls and **zero returned rows** across that
  snapshot, about 4.28 million shared-buffer hits.
- Inbox reconciliation: over 8k calls, **zero returned rows**, about 8.74 million
  shared-buffer hits.
- The broad `ix_hub_task_claim_sequence` and `ix_hub_task_claim_hub_sequence`
  indexes accounted for about 20.58m and 8.26m cumulative heap fetches. Which
  statements used them during backlog remains to be established with plans.
- `hub_task` had **21 indexes**, including several compatibility claim indexes.
  Zero scans in this short window do not establish that an index is disposable.
- Autovacuum/autoanalyze had run on the large task/workflow tables during the
  second burst. At inspection task dead rows were about 842 versus 64,000 live
  rows. This does not prove absence of transient backlog-time bloat.

## Tuning order

1. **Inspect batch admission plans under a representative active backlog.**
   [ClaimCandidateBatchAsync](../src/WorkflowCore.Hub/HubTaskStore.cs) joins selected
   task IDs/epochs and per-worker JSON capabilities/pins, then locks and updates
   rows. Compare custom and generic plans with representative batches of 1, 8,
   16 and 32. Look for queue-wide scans versus primary-key lookups driven by
   selected IDs, repeated workflow checks and estimated/actual row divergence.
   A bounded selected-ID lookup is a candidate optimization, not a proven fix.
   Keep eligibility, owner/task fencing, lock ordering, `SKIP LOCKED`, resource
   exclusion and journal writes intact. Use an isolated workload for
   `EXPLAIN ANALYZE` on mutating SQL; it executes the statement.
2. **Reduce empty and repeated discovery work.** The screenshot window's
   candidate telemetry shows about 93.69k discoveries versus 32.16k grants,
   roughly **2.91 discoveries per grant**, and mean recorded mixed-batch fill
   about **12.19 of 32**. Those counters are advisory, not distinct IDs or worker
   utilization. Investigate rescans and fairness before increasing batch size.
   Measure demand/hint coalescing and expired-stream cadence independently,
   retaining periodic recovery for missed notifications and expired leases.
3. **Measure planning overhead and the cost of empty inbox sweeps.** Capture
   planning timing in an isolated profiled A/B and plans for the event/exhausted
   task union before adjusting cadence. Do not skip durable event or dead-task
   recovery merely because this activity-only cohort returned no rows.
4. **Audit write amplification after query fixes.** Completion and task creation
   are significant costs, and task state transitions maintain many indexes.
   Review compatibility/rollback query coverage before proposing index removal.
   Preserve history and unique indexes. The notification outbox is currently off
   and remains experimental; its extra writes are not a demonstrated win here.
5. **Test capacity settings one at a time.** Current Hubs each have pool 12,
   claim gate 3, candidate admission concurrency 2, batch cap 32, four matching
   partitions and enabled candidate backpressure/cross-worker batching. Keep
   these as the measured control. An admission-concurrency 1/2 comparison is
   a useful CPU/latency tradeoff experiment; adding sessions or partitions has
   no measured justification yet. The SQL metrics sampler is already off.

PostgreSQL uses default `shared_buffers=128MB`, `work_mem=4MB` and
`effective_cache_size=4GB`. Most observed accesses are cache hits, and leading
statements reported no temporary blocks written. Larger buffers may merit a
later controlled test, but neither a memory shortage nor a disk bottleneck was
established. Keep autovacuum enabled; no `VACUUM FULL`, database reset, disabling
sequential scans or durability reduction is part of this review.

For every experiment, compare CPU-seconds/completed task, buffer hits/completed
task, acquisition calls/grant, discoveries/grant, batch fill, workflow throughput,
schedule-to-start p95 and persisted workflow p95. Use equal history size, workload,
workers and resource policy, with repeated bracketed controls. A lower CPU peak
alone can mean less throughput. The earlier ~38 WF/s runs used an unverified
earlier service configuration, so they are not a controlled speedup comparison.

## Observability corrections prepared with this review

- Candidate admission/capacity-wait histograms now use the existing submillisecond
  to minute latency buckets. Previously the first positive bucket ended at five
  seconds: **every recorded admission landed there**, producing an artificial
  p95 of 4.75 s while the sum/count mean was 55.5 ms. Historical p95 cannot be
  recovered. Use a range wholly after deployment to assess the new buckets.
- Candidate batch fill now has explicit 1–32 task boundaries.
- Both normal and p99 collectors export statement calls, rows and shared-buffer
  hits alongside elapsed execution and block I/O. This enables interval
  calls/grant, hits/returned-row and mean statement time without cumulative
  cross-run confusion. Returned rows are not grants for arbitrary statements.
- The PostgreSQL dashboard includes a table ranked by cached buffer accesses;
  its previous block-I/O table did not show this source of repeated work.

The new statement metrics add three series per existing query label set. They
retain the existing 15-second cadence and query-text truncation. They cannot
backfill the historical screenshot window. Deployment is required for these
changes to appear; the running services were left intact.

Validation: scaling solution Release build passed with seven existing warnings
and zero errors; both configs passed the installed collector 0.140.0 `validate`
command in disposable network-disabled containers; both statement queries ran
with PostgreSQL read-only transactions; all 19 dashboard PromQL expressions were
accepted by the running Prometheus; `git diff --check` passed. No new load A/B or
database integration test suite was run for these instrumentation-only changes.

Useful interval expressions after deployment:

```promql
# CPU-seconds per candidate grant, including background PostgreSQL work.
sum(increase(namedprocess_namegroup_cpu_seconds_total{job="postgres-process",groupname="postgres"}[5m]))
/ sum(increase(workflow_hub_candidate_leased_total[5m]))

# Per-statement cached buffer accesses per returned row; exclude zero-row SQL.
(sum by(query_id)(increase(workflow_postgres_statement_shared_blocks_hit_total[5m])))
/ (sum by(query_id)(increase(workflow_postgres_statement_rows_total[5m])) > 0)

# Mean elapsed execution milliseconds per statement call.
sum by(query_id)(increase(workflow_postgres_statement_execution_time_ms_total[5m]))
/ (sum by(query_id)(increase(workflow_postgres_statement_calls_total[5m])) > 0)
```

The grant denominator covers the instrumented candidate path; use durable task
counts or a verified all-path completion counter when fallbacks, retries or
unfinished work are present. Collect after new series have at least two samples.

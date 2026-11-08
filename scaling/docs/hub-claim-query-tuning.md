# Hub claim candidate query review

Reviewed 8 November 2026 against `HubTaskStore.ClaimAsync`. This is an
experimental local SQL comparison, not an end-to-end throughput result.

## Observed query

The supplied statement is the legacy candidate-selection query, including the
fallback when unconstrained batch claiming is enabled. A local PostgreSQL 17.11
`pg_stat_statements` snapshot for query ID `3217150368439748392` showed:

| Counter | Value |
| --- | ---: |
| Calls | 144,420 |
| Cumulative execution time | 1,287,737.1 ms |
| Mean execution time | 8.917 ms |
| Maximum execution time | 264.6 ms |
| Returned candidates | 12,104,356 |
| Shared buffer hits | 192,809,822 |
| Shared buffer reads | 484 |

These are cumulative counters from the existing local database, without a
known workload/reset interval. Execution time is elapsed database statement
time, not CPU time. Returned rows are speculative candidates, not acquired
leases. At inspection the database contained 150,092 completed tasks and no
active tasks; its current empty-queue plan took 0.063 ms. Consequently the
historical 8.917 ms average cannot be reproduced or attributed completely from
that snapshot.

The `$8`, `$9`, and `$10` placeholders in the normalized statement represent
source literals `0`, `1`, and `OFFSET 0`. This is not deep OFFSET pagination.
The source keeps `state IN (0,1)` and `attempts < max_attempts` literal so the
partial claim indexes remain eligible. PostgreSQL requires the query to imply
the partial-index predicate at planning time; see
[partial indexes](https://www.postgresql.org/docs/17/indexes-partial.html).

## Applied change

The candidate query previously checked `hub_workflow` twice: once to reject
non-runnable native workflows and again to match workflow definition pins.
It now shares a `LEFT JOIN` by the workflow primary key:

```sql
LEFT JOIN wfc_scale.hub_workflow w ON w.id = t.workflow_id
-- Status filter:
AND (w.id IS NULL OR w.execution_mode <> 'native' OR w.native_status = 'Runnable')
-- Workflow-task pin filter:
AND (t.task_type NOT IN ('__workflow__', '__native_workflow__')
     OR @pins IS NULL
     OR EXISTS (
       SELECT 1
       FROM jsonb_to_recordset(@pins) AS p(hub text, definition text, version integer)
       WHERE w.hub_id = p.hub
         AND w.definition_id = p.definition
         AND w.definition_version = p.version))
```

`execution_mode` and `native_status` are non-null schema columns. The explicit
missing-row branch preserves standalone tasks and references without a matching
workflow. Definition pins still apply only to workflow tasks; activity tasks
still obey native status checks. All filters remain before `LIMIT 96`.

The ordered inner query and `OFFSET 0`, single-hub equality, multi-hub global
sequence order, partition filtering, and optional FIFO prefilter remain in
place. A subsequent change described below moves task-local filters inside
that ordered boundary. The task/resource locking and eligibility rechecks
inside the lease transaction are unchanged. No migration or feature-flag
change is required.

## Isolated plan comparison

The comparison used a separate disposable PostgreSQL 17.11 container with
migrations 002–023. Existing stack data and statistics were not reset or
modified. Synthetic data comprised 38,500 workflows: 37,500 completed native
workflows plus 1,000 runnable native workflows, each with one ready workflow
task. Claims selected one hub, one task capability, one matching workflow pin,
no matching sharding, and FIFO prefilter disabled.

Each query was run six times per scenario with
`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON, TIMING OFF)`. Times below are medians;
buffer hits are from the last execution. Each query returned 96 candidates.

| Scenario | Before ms | After ms | Before buffer hits | After buffer hits |
| --- | ---: | ---: | ---: | ---: |
| All 1,000 tasks ready | 0.981 | 0.697 | 580 | 292 |
| Oldest 900 tasks hold unexpired leases | 1.832 | 1.407 | 620 | 332 |
| Oldest 900 tasks belong to suspended workflows | 6.380 | 3.232 | 6,156 | 3,110 |

The ready-work baseline performed 96 workflow primary-key scans for pins and
another 96 for native status. The rewrite performed 96 shared workflow scans.
This accounts for the 288-buffer reduction in that plan.

These short, sequential, mostly cached executions do not measure concurrent
claim contention, processor utilization, or workflow latency. Their timing
improvement must not be applied to the historical live-query average as a
prediction. Buffer access and plan shape provide the stronger evidence. See
[Using EXPLAIN](https://www.postgresql.org/docs/17/using-explain.html).

The original and actual rewritten source queries also returned identical
ordered candidate IDs for ready, leased, suspended, wrong-version pins, null
pins, and eight-way matching-partition cases. Integration regressions cover
status/pin filtering ahead of the candidate limit, missing workflow references,
standalone tasks, inline execution, FIFO, hub filtering, partition ownership,
competing workers, retry and stale lease fencing.

Verification used .NET SDK 10.0.100 and `WFC_TEST_PG` pointing at the isolated
container, so the database-dependent tests actually ran:

- `dotnet build scaling/WorkflowCore.Scaling.slnx -c Release --no-restore` passed
  with seven existing warnings and no errors.
- 30 tests passed across `HubClaimCandidateTests`, `HubUnconstrainedBatchClaimTests`,
  `HubFifoHeadPrefilterTests`, `HubExplicitOrderingTests`, and `HubGatewayTests`.
- 20 tests passed across `HubNativeRuntimeTests`, `HubNativeRoutedTests`,
  `HubRemoteSchedulerTests`, and `HubWorkflowE2ETests`.
- `git diff --check` passed.

The disposable container was stopped after verification. No running Hub or
processor was redeployed.

## Follow-up: filter task rows inside the ordered query

A later read-only snapshot from a different local stack contained the shared
workflow lookup query as ID `8118641777474683971`: 4,879 calls, 2.159 ms mean,
47.8 ms maximum, 380,497 candidates and 3,001,630 shared buffer hits. All 9,256
tasks were already complete. These counters cover a different database and
workload from the first snapshot and are not a controlled before/after result.

The next source change pushes hub membership, matching partition, capability
and due/expired-lease predicates into the ordered task subquery. This makes
them available when PostgreSQL chooses the task scan, instead of filtering
after `OFFSET 0` has established a query boundary. The inner projection now
contains only `id`, `sequence`, `hub_id`, `task_type`, and `workflow_id`.
The redundant outer hub/attempt checks are removed. A single-hub request
retains equality; multiple hubs use `ANY(@hubs)` inside the subquery, with a
global `ORDER BY sequence`. Unsharded requests generate a constant `TRUE`
predicate; sharded requests generate the modulo comparison directly.

The outer workflow status, pins, FIFO checks and `LIMIT 96` are retained.
Moving filters ahead of `OFFSET 0` preserves results because the offset is
zero and the subquery has no row limit. It does not move `LIMIT 96` ahead of
any eligibility check. Literal state and attempt predicates still match the
existing partial indexes.

The follow-up comparison used another disposable PostgreSQL 17.11 container
with migrations 002–023, 40,000 older unrelated activity tasks, 10,000 native
workflow tasks for a target hub, and 100 native workflow tasks for a second
target hub. Workflow tasks used matching definition pins and no FIFO
prefilter. Each result below is the median of five `EXPLAIN (ANALYZE, BUFFERS,
FORMAT JSON, TIMING OFF)` executions; buffer hits are from the final run.
"Before" includes the shared workflow lookup from the initial change.

| Scenario | Before ms | After ms | Before buffer hits | After buffer hits |
| --- | ---: | ---: | ---: | ---: |
| Target hub: 10,000 ready tasks | 8.416 | 7.091 | 1,091 | 586 |
| Same hub, one of eight matching shards | 8.158 | 3.895 | 1,107 | 586 |
| Both target hubs, global sequence order | 15.691 | 8.875 | 1,091 | 589 |
| Target hub: all leases unexpired | 15.983 | 4.688 | 14,906 | 521 |
| Target hub: all ready tasks due in the future | 21.585 | 15.351 | 12,297 | 12,297 |
| Dense majority hub: 40,000 ready activities | 0.145 | 0.155 | 4 | 4 |

Both query forms returned identical ordered candidate IDs in the first five
scenarios. Ready scenarios returned 96 rows and unexpired/future scenarios
returned zero. The dense-majority case separately checked the common short
ordered scan and returned 96 rows with four buffer hits for both forms.
These are short, sequential synthetic measurements on a shared host, not
concurrent-load or live-deployment guarantees. The future-due case reduced no
buffer access, so its timing difference alone is weak evidence.

The rewritten target-hub plans often used the existing hub-selective index
via a bitmap heap scan followed by sorting eligible rows. Dense-majority
plans retained an ordered index scan. PostgreSQL can choose between those
strategies based on selectivity; bitmap scans require a separate sort to
restore order. See [indexes and ordering](https://www.postgresql.org/docs/17/indexes-ordering.html)
and [combining indexes](https://www.postgresql.org/docs/17/indexes-bitmap-scans.html).
No additional index was introduced.

New integration cases place 110 rows for each exclusion (wrong hub, type,
version, matching partition, future due time, exhausted attempts, live lease,
or completed state) ahead of runnable tasks. Single- and multi-hub requests
must still return exactly the eligible IDs in global sequence order.

The follow-up passed 52 PostgreSQL integration tests across the same nine test
classes listed above, with `WFC_TEST_PG` set to the second disposable container.
The actual source SQL matched the measured proposed SQL after removing
whitespace and comments. The scaling solution Release build and
`git diff --check` also passed. The second disposable container was stopped;
the running stack was not redeployed.

## Remaining tuning candidates

- The fallback selects up to 96 candidates again for each requested lease.
  This can repeat up to 32 times per claim. Candidate-list reuse or batching
  needs a separate contention/fairness experiment with authoritative lock
  rechecks intact.
- Capability string concatenation and dynamic partition modulo remain row
  filters. A typed capability join or different index needs plans from a
  representative backlog and shard count before adding write/index overhead.
- Migration 023 already supplies `(hub_id, sequence)` with the claim predicate.
  Do not add a duplicate index. Neither `now()`-dependent eligibility nor
  workflow status from another table belongs in that partial-index predicate.
- The FIFO-head prefilter remains opt-in. Its earlier policy-specific
  regressions are documented in [SQL CPU investigation](sql-cpu-statement-investigation.md).

Validate deployment with representative claim parameters and compare
`workflow_hub_claim_candidate_query_seconds`, candidate-to-lease amplification,
SQL buffer access, throughput, and workflow latency under equivalent load.

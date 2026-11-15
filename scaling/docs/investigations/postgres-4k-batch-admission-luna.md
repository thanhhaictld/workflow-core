# PostgreSQL batch-admission investigation (Luna)

**Scope.** Investigate `ClaimCandidateBatchAsync` and query ID
`-6050224743415277628` for the two 4,000-workflow runs on 14 November 2026.
This is a read-only investigation; no runtime code, database objects, or live
service settings were changed. Findings below distinguish recorded facts from
plan-based hypotheses. The baseline review is
[`postgresql-4k-review-2026-11-15.md`](../postgresql-4k-review-2026-11-15.md).

## Ranked findings

### 1. The batch statement can scan the active task queue before matching the small selected-ID set — plausible, not proven during the bursts

**Measured facts:** the historical interval statement sample records
21,305.31 ms execution in the selected five-minute window. The read-only live
`pg_stat_statements` row showed 8,159 calls, 62,039 rows, 40,944.279 ms total,
5.018 ms mean, 21,851,598 shared-buffer hits and 1,152 reads since the
16:49:08 UTC stats reset. That cumulative sample spans four 4,000-workflow
runs, as the baseline warns; it must not be assigned to only the selected two.
The interval included both completed runs (4,000 workflows and 16,000 tasks
each), CPU peak 3.29 cores, and estimated PostgreSQL 385.15 CPU-seconds over
five minutes; none of those figures isolates this statement's CPU cost.

**Read-only plan evidence:** I ran `EXPLAIN (COSTS, VERBOSE)` for the statement
shape under a `READ ONLY` transaction with a 3-second timeout, forcing both
custom and generic plans. At inspection all 64,000 `hub_task` rows had
`state=2` (completed); current live/dead estimates were 64,000/842, so this
was not an active-backlog reproduction. The custom plan starts from
`ix_hub_task_claim_hub_sequence`; the generic plan starts from the broader
`ix_hub_task_claim_sequence`. In both plans the task scan is on the hash-build
side of a hash join to the selected JSON recordset, so the task scan can walk
eligible queue entries before filtering to selected IDs. The chosen generic
plan uses the global sequence index and applies hub equality as a filter. Both plans
estimate `jsonb_to_recordset` at 100 rows, independent of the supplied one-row
sample. These are cost-only plans on an empty queue, not evidence of actual
rows scanned, runtime selection under backlog, or causation for historical
buffer hits.

**Hypothesis / likely causes:** source lists `hub_task t` before
`jsonb_to_recordset(@selected) s`, but SQL order alone does not force a join
order. The planner may prefer a queue index scan plus hash join when it
estimates the eligible task set as cheap relative to the function scan. The
generic plan loses the hub-specific index bound; a custom plan has the hub
bound but can still scan that hub's eligible queue. The function row estimate
of 100 does not describe actual batches (admission is bounded to at most 32
items). Capability and pin `jsonb_to_recordset` checks, task-constraint
anti-join, native status lookup, and sequence sorting/row locking add work per
candidate. `MATERIALIZED picked` separates the update but does not force the
selection join to start from the supplied IDs. These explain why broad scans
are plausible; the historical plan was not saved, so they do not establish
that this statement caused the aggregate broad-index fetch totals.

The query ID's normalized `pg_stat_statements` text is consistent with the
source structure in
[`HubTaskStore.cs`](../../src/WorkflowCore.Hub/HubTaskStore.cs#L645), but is not
byte-for-byte source SQL: PostgreSQL normalizes literals and parameter names.
The source joins task ID and observed epoch, then checks hub/partition, each
selected worker's capability JSON, attempts/due-or-expired eligibility,
absence of resource constraints, native workflow runnable status, and
workflow pins before `ORDER BY sequence FOR UPDATE ... SKIP LOCKED`. The outer
update is by task ID.

### 2. Custom/generic plan selection changes the breadth of the queue access

This claim path sends SQL with Npgsql parameters; prepared-plan choice can
therefore matter depending on server/driver preparation behavior. I did not
observe the application's exact prepared-statement lifecycle in this
read-only probe, so do not assume the generic plan was used by every call. The
cost-only probe demonstrated a concrete plan difference for the query shape:
custom planning used `(hub_id, sequence)`, while generic planning used the
global `(sequence)` partial claim index and applied hub/partition as filters.
The observed generic plan had no `hub_id` index condition because it selected
the global sequence index and applied `t.hub_id = $2` as a filter. A
parameterized equality can normally be an index condition when the planner
chooses a compatible index; this one plan does not establish that generic
plans cannot use such an index condition. It makes generic-plan behavior a
specific backlog benchmark dimension, not a confirmed explanation for the
historical query's entire execution time.

### 3. Existing tests cover key safety filters, but not plan shape or batch lock-order contention

`HubCandidateBatchTests` already checks per-worker capability behavior with
mixed workers, stale observed epochs, pin mismatch, resource-constraint
exclusion, and that only the admitted workflow task gets its claim journal
entry ([test lines 11–68](../../tests/WorkflowCore.Scaling.PostgreSQL.Tests/HubCandidateBatchTests.cs#L11)).
It also checks rollback and the ambiguous-commit path: after a commit whose
response is lost, the claim reports `CommitOutcomeUnknown`, the exact lease
epoch remains current, and retry does not mint another lease (lines 71–90).
These are important compatibility constraints for any rewrite. There is no
batch-specific representative-backlog plan assertion or test that makes two
competing batches submit the same IDs in opposite orders and checks for lock
order/deadlock behavior.

## Proposed smallest change (not implemented)

First prototype the simpler selected-ID lookup below in an isolated database.
Make the bounded JSON set the outer relation and try a parameterized lateral
primary-key lookup to collect candidate IDs and sequence values, then lock the
base rows in sequence order in a second CTE. Carry each observed epoch and
repeat the task eligibility predicates in the locking step, including the
explicit `t.epoch = c.epoch` comparison. The lateral form is only an
experiment: PostgreSQL can flatten/reorder parts of it, so it does **not yet
guarantee** bounded primary-key probes. Verify both plan shapes before
considering it a production change.

```sql
WITH selected AS MATERIALIZED (
  SELECT * FROM jsonb_to_recordset(@selected)
    AS s(id uuid, epoch bigint, worker text, capabilities jsonb, pins jsonb)
),
candidate AS MATERIALIZED (
  SELECT s.id, s.epoch, s.worker, t.sequence
  FROM selected s
  CROSS JOIN LATERAL (
    SELECT t.sequence
    FROM wfc_scale.hub_task t
    WHERE t.id = s.id AND t.epoch = s.epoch
      AND /* current eligibility/capability/pin/constraint/status checks */
  ) t
),
picked AS MATERIALIZED (
  SELECT t.id, t.sequence, c.worker
  FROM candidate c
  JOIN wfc_scale.hub_task t ON t.id = c.id
  WHERE t.epoch = c.epoch
    AND /* repeat current eligibility/capability/pin/constraint/status checks */
  ORDER BY t.sequence
  FOR UPDATE OF t SKIP LOCKED
)
UPDATE ... FROM picked p WHERE t.id = p.id;
```

This is a plan-shaping proposal, not drop-in validated SQL. The sketch omits
the repeated full predicates for readability; production state/attempt
literals must remain literal (`state = 0/1`, `attempts < max_attempts`) where
needed for partial-index implication. Keep capability and pins per selected
row. Before adopting, confirm PostgreSQL retains a selected-row outer loop
with primary-key parameterized lookups in both custom and generic plans and
that `LockRows` remains above sequence ordering. The final locked step must
include `t.epoch = c.epoch` and recheck all eligibility; a preliminary match
cannot authorize a lease after a concurrent change. Retain the existing final
update by task ID. If the planner still pulls the task relation outward, stop
and revise the rewrite rather than adding a planner toggle or disabling
sequential scans globally.

No configuration change is recommended from current evidence. In particular,
do not adjust batch cap or admission concurrency based on empty-queue plans.

### Correctness constraints and risks

- Keep the partition-owner admission lock and the later owner recheck in the
  same transaction. The owner fence must continue to enclose task claim,
  attempts/history writes and commit.
- Preserve each item's own worker, capabilities and workflow pins; a batch may
  contain workers with different capability/pin sets.
- Preserve observed task epoch, attempts/max-attempts, due-ready or expired
  lease eligibility, native workflow `Runnable` status, workflow definition
  version pins, and constrained-task exclusion. Constrained rows must remain
  off this unconstrained batch path so FIFO/resource permits cannot be
  bypassed.
- Keep sequence-ordered row locking, `SKIP LOCKED`, and task-ID outer update.
  A selected-ID-driven plan must not accidentally lock candidates in JSON input
  order or let competing batches deadlock.
- Keep attempt closure/insertion and `WorkflowTaskClaimed` journal writes in
  the claim transaction. Do not change the `CommitOutcomeUnknown` contract or
  treat an uncertain commit as a safe retry; the current exact-epoch lease
  reconciliation remains required.
- A two-phase probe followed by lock can race with task mutation. The final
  locked step must recheck all eligibility and epoch predicates, and a skipped
  lock must still omit the row rather than wait or lease it twice.

## Regression and representative-backlog experiment

**Precise regression:** add a PostgreSQL integration test with 32 selected
IDs from a 50,000-row active eligible queue spread across multiple hubs and
partitions. Mix in wrong/stale epochs, one item whose own worker lacks the
task capability, one workflow task with a wrong pin, a non-runnable native
workflow, a constrained task, an exhausted task, a future-due task, and a
live lease. Assert only the exactly eligible selected tasks lease; each lease
retains that item's worker; epoch and attempts increment once; excluded rows
remain unchanged; attempts and `WorkflowTaskClaimed` rows match admitted IDs;
and a repeated claim after a simulated lost commit response cannot mint a new
epoch. Add a two-transaction competing-batch case that submits common IDs in
opposite JSON order and confirms both complete without deadlock/duplicate
lease, with `SKIP LOCKED` behavior preserved.

**Plan/performance experiment (unverified):** use a disposable PostgreSQL 17
database with the deployed migration/index set, not the shared live database.
Seed a representative backlog (for example 50,000 ready rows over several
hubs and four matching partitions, including tasks outside the target hub and
stale/noneligible rows); use representative worker capability/pin JSON and
batch sizes 1, 8, 16 and 32. Compare original and prototype with
`EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON, TIMING OFF)` on the isolated
database only, under forced custom and generic plans as well as the normal app
prepare path. Place selected eligible IDs both early and late in sequence to
surface queue scanning. Record selected-ID versus task scan row counts, rows
removed by filters, buffer hits/reads, plan shape, execution latency, lock
wait/deadlock outcomes, and results/order. Then run repeated equal-load A/B
bursts with identical task history/worker count/policies and compare statement
buffers per granted lease, CPU-seconds per completed task, throughput,
schedule-to-start p95, persisted workflow p95, and attempts/history equality.
Acceptance requires unchanged results and safety assertions, bounded
primary-key probes proportional to batch size in both generic and custom
plans, no new deadlocks or unfair starvation, and a repeatable reduction in
queue rows/buffers without worsening throughput or latency. This proposed
benchmark is not run and no performance improvement is claimed.

## Evidence limits and command record

- Historical metrics are archived in
  `C:/Users/haido-ws/AppData/Roaming/orca/chat-scratch/514ab8f3001b3c7ccca67da01bb393fb/postgres-review/evidence.json`;
  the five-minute execution total and cumulative post-reset counters have
  different windows. `pg_stat_statements` elapsed execution is not CPU
  attribution, and aggregate index counters cannot identify which statement
  fetched a heap tuple.
- The only live DB commands were `docker exec ... psql` read-only queries for
  the statement-statistics row, task state/table/index counters, and
  cost-only `EXPLAIN` of the batch SQL in `BEGIN READ ONLY` transactions with
  `statement_timeout` of 2 or 3 seconds. Both exited 0. The container's
  `POSTGRES_USER`/`POSTGRES_DB` were read inside the container; no credentials
  were displayed. No integration suite, load, mutating SQL, service change,
  or `EXPLAIN ANALYZE` was run.
- The active-queue scan hypothesis is not confirmed by live evidence because
  the inspected queue contained no ready or leased tasks. Do not infer backlog
  performance from these empty-queue estimates. Only the isolated experiment
  above can verify the candidate fix.
- Relevant architecture constraints: current runtime says authoritative
  admission revalidates and commits task lease, attempts and history before
  delivery ([lines 56–82](../current-runtime.md#L56)); the remote-scheduler
  runbook describes owner-fenced bounded admission and ambiguous commit
  quarantine ([lines 27–28](../hub-remote-scheduler-implementation.md#L27));
  the final plan requires transactional fenced state/history and explicitly
  calls for `SKIP LOCKED` tests ([lines 141–142, 302](../hub-embedded-scheduler-final-plan.md#L141)).

## Appendix: exact plan probe and output

### What was prepared

The plan probe was a **manual reconstruction** of the production query shape
from [`HubTaskStore.cs`](../../src/WorkflowCore.Hub/HubTaskStore.cs#L645)
and the normalized `pg_stat_statements` text. It was not a byte-for-byte copy
of the C# SQL: I reconstructed the statement manually, used SQL literals for
the source constants, and omitted the production `RETURNING` projection. The
probe changes the parsed query identifier and cannot stand in for an archived
application plan. The predicates listed below match the relevant batch
selection semantics.

The production Npgsql parameters map to the PREPARE signature as follows:

| Production parameter | PREPARE position/type | Production source literals kept literal |
| --- | --- | --- |
| `@selected` (`NpgsqlDbType.Jsonb`) | `$1 jsonb` | n/a |
| `@hub` (string/text) | `$2 text` | n/a |
| `@count` (int) | `$3 integer` | n/a |
| `@partition` (int) | `$4 integer` | n/a |
| `@lease` (`NpgsqlDbType.Interval`) | `$5 interval` | n/a |
| source predicates | n/a | `state = 0`, `state = 1`, `execution_mode = 'native'`, `native_status <> 'Runnable'`, task-type constants `('__workflow__','__native_workflow__')`, and `attempts < max_attempts` |

The literal state/attempt predicates are intentional: they stay literal in
source SQL so PostgreSQL can prove the partial claim-index predicates. The
custom plan used a one-item JSON sample (not a representative batch); the
generic plan used the same PREPARE statement but retained `$1`–`$5` as
parameters. Both were explained inside `BEGIN READ ONLY` with a 3-second
statement timeout. `EXPLAIN` did not execute the UPDATE.

### Exact SQL passed to psql

The PowerShell wrapper piped this here-string to the container-local psql
process (`psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"`):

```sql
BEGIN READ ONLY;
SET LOCAL statement_timeout = '3s';
PREPARE batch_claim(jsonb,text,integer,integer,interval) AS
WITH picked AS MATERIALIZED (
 SELECT t.id,t.sequence,s.worker FROM wfc_scale.hub_task t
 JOIN jsonb_to_recordset($1) AS s(id uuid,epoch bigint,worker text,capabilities jsonb,pins jsonb) ON s.id=t.id AND s.epoch=t.epoch
 WHERE t.hub_id=$2 AND t.partition_id % $3=$4
 AND EXISTS (SELECT 1 FROM jsonb_to_recordset(s.capabilities) AS c(hub text,task_type text,version integer) WHERE c.hub=t.hub_id AND c.task_type=t.task_type AND c.version=t.task_version)
 AND t.attempts<t.max_attempts AND ((t.state=0 AND t.due_at<=now()) OR (t.state=1 AND t.lease_until<now()))
 AND NOT EXISTS (SELECT 1 FROM wfc_scale.hub_task_constraint c WHERE c.task_id=t.id)
 AND NOT EXISTS (SELECT 1 FROM wfc_scale.hub_workflow w WHERE w.id=t.workflow_id AND w.execution_mode='native' AND w.native_status<>'Runnable')
 AND (t.task_type NOT IN ('__workflow__','__native_workflow__') OR s.pins IS NULL OR s.pins='null'::jsonb OR EXISTS (SELECT 1 FROM wfc_scale.hub_workflow w,jsonb_to_recordset(s.pins) AS p(hub text,definition text,version integer) WHERE w.id=t.workflow_id AND w.hub_id=p.hub AND w.definition_id=p.definition AND w.definition_version=p.version))
 ORDER BY t.sequence FOR UPDATE OF t SKIP LOCKED
)
UPDATE wfc_scale.hub_task t SET state=1,owner=p.worker,last_owner=p.worker,epoch=t.epoch+1,attempts=t.attempts+1,lease_until=now()+$5,updated_at=now()
FROM picked p WHERE t.id=p.id;
SET LOCAL plan_cache_mode='force_custom_plan';
EXPLAIN (COSTS, VERBOSE, FORMAT TEXT) EXECUTE batch_claim('[{"id":"00000000-0000-0000-0000-000000000001","epoch":0,"worker":"worker","capabilities":[{"hub":"workflow","task_type":"activity","version":1}],"pins":null}]'::jsonb,'workflow',4,0,'30 seconds');
SET LOCAL plan_cache_mode='force_generic_plan';
EXPLAIN (COSTS, VERBOSE, FORMAT TEXT) EXECUTE batch_claim('[{"id":"00000000-0000-0000-0000-000000000001","epoch":0,"worker":"worker","capabilities":[{"hub":"workflow","task_type":"activity","version":1}],"pins":null}]'::jsonb,'workflow',4,0,'30 seconds');
DEALLOCATE batch_claim;
COMMIT;
```

### Full relevant plan output

The custom plan's selected-task and base-task access nodes were:

```text
Update on wfc_scale.hub_task t  (cost=30.72..38.77 rows=0 width=0)
  CTE picked
    ->  LockRows  (cost=30.29..30.31 rows=1 width=218)
          ->  Sort  (cost=30.29..30.30 rows=1 width=218)
                Sort Key: t_1.sequence
                ->  Nested Loop Anti Join  (cost=10.56..30.28 rows=1 width=218)
                      ->  Nested Loop Anti Join  (cost=10.27..21.96 rows=1 width=228)
                            ->  Hash Join  (cost=6.08..7.61 rows=1 width=222)
                                  Hash Cond: ((s.id = t_1.id) AND (s.epoch = t_1.epoch))
                                  Join Filter: (EXISTS(SubPlan 1) AND ((t_1.task_type <> ALL ('{__workflow__,__native_workflow__}'::text[])) OR (s.pins IS NULL) OR (s.pins = 'null'::jsonb) OR EXISTS(SubPlan 2)))
                                  ->  Function Scan on pg_catalog.jsonb_to_recordset s  (cost=0.00..1.00 rows=100 width=264)
                                        Function Call: jsonb_to_recordset('[{"id": "00000000-0000-0000-0000-000000000001", "pins": null, "epoch": 0, "worker": "worker", "capabilities": [{"hub": "workflow", "version": 1, "task_type": "activity"}]}]'::jsonb)
                                  ->  Hash  (cost=6.06..6.06 rows=1 width=95)
                                        ->  Index Scan using ix_hub_task_claim_hub_sequence on wfc_scale.hub_task t_1  (cost=0.27..6.06 rows=1 width=95)
                                              Index Cond: (t_1.hub_id = 'workflow'::text)
                                              Filter: ((t_1.attempts < t_1.max_attempts) AND ((t_1.partition_id % 4) = 0) AND (((t_1.state = 0) AND (t_1.due_at <= now())) OR ((t_1.state = 1) AND (t_1.lease_until < now()))))
                                  SubPlan 1
                                    ->  Function Scan on pg_catalog.jsonb_to_recordset c  (cost=0.00..1.75 rows=1 width=0)
                                          Function Call: jsonb_to_recordset(s.capabilities)
                                          Filter: ((c.hub = t_1.hub_id) AND (c.task_type = t_1.task_type) AND (c.version = t_1.task_version))
                                  SubPlan 2
                                    ->  Hash Join  (cost=8.32..10.11 rows=1 width=0)
                                          Hash Cond: ((p_1.hub = w.hub_id) AND (p_1.definition = w.definition_id) AND (p_1.version = w.definition_version))
                                          ->  Function Scan on pg_catalog.jsonb_to_recordset p_1  (cost=0.00..1.00 rows=100 width=68)
                                                Function Call: jsonb_to_recordset(s.pins)
                                          ->  Hash  (cost=8.30..8.30 rows=1 width=38)
                                                ->  Index Scan using hub_workflow_pkey on wfc_scale.hub_workflow w  (cost=0.29..8.30 rows=1 width=38)
                                                      Index Cond: (w.id = t_1.workflow_id)
                            ->  Bitmap Heap Scan on wfc_scale.hub_task_constraint c_1  (cost=4.19..12.66 rows=5 width=22)
                                  Recheck Cond: (c_1.task_id = t_1.id)
                                  ->  Bitmap Index Scan on hub_task_constraint_pkey  (cost=0.00..4.19 rows=5 width=0)
                                        Index Cond: (c_1.task_id = t_1.id)
                      ->  Index Scan using hub_workflow_pkey on wfc_scale.hub_workflow w_1  (cost=0.29..8.31 rows=1 width=22)
                            Index Cond: (w_1.id = t_1.workflow_id)
                            Filter: ((w_1.native_status <> 'Runnable'::text) AND (w_1.execution_mode = 'native'::text))
  ->  Nested Loop  (cost=0.41..8.47 rows=1 width=172)
        ->  CTE Scan on picked p  (cost=0.00..0.02 rows=1 width=120)
        ->  Index Scan using hub_task_pkey on wfc_scale.hub_task t  (cost=0.41..8.43 rows=1 width=34)
              Index Cond: (t.id = p.id)
```

The generic plan differed at the task scan and retained parameters:

```text
Update on wfc_scale.hub_task t  (cost=281.08..289.13 rows=0 width=0)
  CTE picked
    ->  LockRows  (cost=280.65..280.66 rows=1 width=218)
          ->  Sort  (cost=280.65..280.65 rows=1 width=218)
                Sort Key: t_1.sequence
                ->  Nested Loop Anti Join  (cost=260.91..280.64 rows=1 width=218)
                      ->  Nested Loop Anti Join  (cost=260.63..272.32 rows=1 width=228)
                            ->  Hash Join  (cost=256.43..257.96 rows=1 width=222)
                                  Hash Cond: ((s.id = t_1.id) AND (s.epoch = t_1.epoch))
                                  Join Filter: (EXISTS(SubPlan 1) AND ((t_1.task_type <> ALL ('{__workflow__,__native_workflow__}'::text[])) OR (s.pins IS NULL) OR (s.pins = 'null'::jsonb) OR EXISTS(SubPlan 2)))
                                  ->  Function Scan on pg_catalog.jsonb_to_recordset s  (cost=0.00..1.00 rows=100 width=264)
                                        Function Call: jsonb_to_recordset($1)
                                  ->  Hash  (cost=256.42..256.42 rows=1 width=95)
                                        ->  Index Scan using ix_hub_task_claim_sequence on wfc_scale.hub_task t_1  (cost=0.27..256.42 rows=1 width=95)
                                              Filter: ((t_1.attempts < t_1.max_attempts) AND (t_1.hub_id = $2) AND ((t_1.partition_id % $3) = $4) AND (((t_1.state = 0) AND (t_1.due_at <= now())) OR ((t_1.state = 1) AND (t_1.lease_until < now()))))
                                  SubPlan 1
                                    ->  Function Scan on pg_catalog.jsonb_to_recordset c  (cost=0.00..1.75 rows=1 width=0)
                                          Function Call: jsonb_to_recordset(s.capabilities)
                                          Filter: ((c.hub = t_1.hub_id) AND (c.task_type = t_1.task_type) AND (c.version = t_1.task_version))
                                  SubPlan 2
                                    ->  Hash Join  (cost=8.32..10.11 rows=1 width=0)
                                          Hash Cond: ((p_1.hub = w.hub_id) AND (p_1.definition = w.definition_id) AND (p_1.version = w.definition_version))
                                          ->  Function Scan on pg_catalog.jsonb_to_recordset p_1  (cost=0.00..1.00 rows=100 width=68)
                                                Function Call: jsonb_to_recordset(s.pins)
                                          ->  Hash  (cost=8.30..8.30 rows=1 width=38)
                                                ->  Index Scan using hub_workflow_pkey on wfc_scale.hub_workflow w  (cost=0.29..8.30 rows=1 width=38)
                                                      Index Cond: (w.id = t_1.workflow_id)
                            ->  Bitmap Heap Scan on wfc_scale.hub_task_constraint c_1  (cost=4.19..12.66 rows=5 width=22)
                                  Recheck Cond: (c_1.task_id = t_1.id)
                                  ->  Bitmap Index Scan on hub_task_constraint_pkey  (cost=0.00..4.19 rows=5 width=0)
                                        Index Cond: (c_1.task_id = t_1.id)
                      ->  Index Scan using hub_workflow_pkey on wfc_scale.hub_workflow w_1  (cost=0.29..8.31 rows=1 width=22)
                            Index Cond: (w_1.id = t_1.workflow_id)
                            Filter: ((w_1.native_status <> 'Runnable'::text) AND (w_1.execution_mode = 'native'::text))
  ->  Nested Loop  (cost=0.41..8.47 rows=1 width=172)
        ->  CTE Scan on picked p  (cost=0.00..0.02 rows=1 width=120)
        ->  Index Scan using hub_task_pkey on wfc_scale.hub_task t  (cost=0.41..8.43 rows=1 width=34)
              Index Cond: (t.id = p.id)
```

In both full outputs the omitted `Output:`/width annotations do not change the
nodes summarized above: `LockRows` sits above sequence sort; the selected JSON
recordset is estimated at 100 rows; the task scan is on the hash side; the
constraint and two workflow checks use point/index lookups; and the final
UPDATE reaches the selected task by primary key. The plan probe proves only
these chosen plan shapes for this reconstructed SQL and current empty queue.
It does not show actual rows visited or confirm the application used either
forced plan in the bursts.


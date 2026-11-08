# Workflow Hub throughput: 1,000 real executions and 1,000,000 capacity assessment

**Measured:** November 6, 2026 (UTC CI began Nov 5).  
**Base:** `feat/unconstrained-offboarding-k6`; eight native-only offboarding
processors, one Hub (12-connection PostgreSQL pool), one PostgreSQL server.
**Warning:** No one-million-workflow run was completed. A million-workflow
end-to-end run exceeds the available GitHub-hosted job window at the currently
measured throughput. The projections below are illustrative, not a capacity SLA.

## Observed CI results

| Metric | Onboarding (routed, 1k) | Offboarding (local, 1k) |
| --- | ---: | ---: |
| Real completed workflows | 1,000 | 1,000 |
| Failed workflows | 0 | 0 |
| Processor fleet | Separate routed workers | 8 native processors |
| Policies | Routed activity resource / FIFO policies | None |
| Processing throughput | 6.17 workflows/s | 11.75 workflows/s server window |
| Full test throughput | Not directly comparable | 11.64 workflows/s |
| Workflow elapsed time p95 | 162.985 seconds | 76.752 seconds |
| Workflow elapsed time p99 | 163.434 seconds | 77.062 seconds |
| Completion/processing window | 162.073 seconds | approx. 85.1 seconds server window |
| Included artificial delays | 15ms + 400ms + 15ms and remote hops | None |
| Peak PostgreSQL sessions | 14 (12 active) | Not independently sampled |

CI: [1k onboarding](https://github.com/thanhhaictld/workflow-core/actions/runs/42024270613),
[1k offboarding](https://github.com/thanhhaictld/workflow-core/actions/runs/42132852192).

The workflows were created in a burst. Measured `p95` and `p99` are
`completed_at - created_at`, therefore **include time spent in queue**.
Do not interpret them as native step body runtime, CPU time or Hub commit latency.

1,000,000 / 11.75 ≈ 85,106 seconds ≈ **23.6 hours** at unchanged native
throughput. 1,000,000 / 6.17 ≈ 162,075 seconds ≈ **45 hours** at unchanged
onboarding throughput. These figures assume perfect sustained rates,
no WAL/disk/index growth and no degradations, which is unrealistic at this scale.
There are **no measured million-workflow p95/p99 values**.

## Why removing FIFO does not solve the throughput limit

1. **All native commits for one business Hub share one exclusive advisory
   transaction lock.** `HubNativeWorkflowStore.EventLane` uses
   `pg_advisory_xact_lock(hashtextextended(@hub,12012))`.
   This is acquired before validating/committing snapshots, writing
   native history and acknowledging tasks. Eight processors and 64 logical
   partitions cannot concurrently commit through that lock.
2. **Task claims suffer repeated ordered-candidate scans.**
   `HubTaskStore.ClaimAsync` scans `ORDER BY sequence LIMIT 96`,
   then starts an individual transaction per candidate, uses
   `FOR UPDATE SKIP LOCKED`, checks constraints and updates one task.
   Multiple processors can race on the same leading 96 candidates.
   The predicate `hub_id || '|' || task_type || ...` can also restrict
   usable indexes. A policy-free workload still uses this slow path.
3. **The native commit persists individual history events one-by-one.**
   `foreach (var history in request.History) await Execute(... INSERT ...)`
   sends an extra SQL command for each lifecycle event. Three native steps
   produce multiple start/finish/span/pointer-transition records per
   workflow, inside the Hub-wide lock.
4. **All worker traffic is funneled through one small Hub DB pool** (12
   configured sessions in the current Compose baseline). Increasing
   processor count can increase REST calls and DB queueing without
   increasing committed workflows per second.
5. **Routed onboarding adds deliberately slow remote activity work**:
   `ProvisionTenant` waits 400ms, `ValidateTenant`/ `SendWelcome`
   wait 15ms each; offboarding does not. Comparing the two workloads
   cannot isolate FIFO or policy overhead alone.

The exact fraction of time spent on each contributor is not yet measured.
Instrument `workflow_hub_native_event_lane_lock_wait_seconds`,
`workflow_hub_claim_duration_seconds` and queue age during a controlled
run before prioritizing a rewrite. PostgreSQL `pg_stat_activity` can
corroborate advisory-lock and client/connection waits.

## Why a naive one-million k6 run would give misleading results

- GitHub-hosted Actions jobs have a finite runtime; a 23-45h projected
  workload is not a valid hosted-CI test.
- The original k6 script had a **60-second default teardown timeout**
  despite a longer application deadline. A 1k run proved this: 1,000
  accepted in 7.8s, then k6 aborted while waiting. The script now
  explicitly sets `teardownTimeout` to `TIMEOUT + 30` and exposes
  `K6_OFFBOARD_MAX_DURATION`.
- `GET /api/hub/runs/{runId}/summary` currently recomputes three
  `percentile_cont` ordered-set aggregates every time it is polled.
  At one million rows, polling this every second introduces substantial
  **measurement-induced database load**. Introduce a cheap counts-only
  run-progress endpoint or poll much less frequently; query exact percentiles
  once only after the completed run.
- Plan PostgreSQL disk/WAL, indexes, vacuum, data retention, Hub pool,
  separate k6 generator CPU, and telemetry sampling/retention.
- Unique run IDs and idempotency keys are required. A test does not imply
  safe execution of a *real* tenant-offboarding workflow; this is the
  current synthetic no-op sample.

## Reproducible 1,000-workflow run (validated in CI)

```bash
make obs-setup
make obs-offboarding-up
docker compose -f scaling/observability/compose.yaml \
  --env-file scaling/observability/.env \
  --profile offboarding-k6 run --rm \
  -e K6_OFFBOARD_COUNT=1000 -e K6_OFFBOARD_VUS=100 \
  -e K6_OFFBOARD_TIMEOUT_SECONDS=900 k6-offboarding
```

This test writes `scaling/observability/results/k6-offboarding-summary.json`
and logs the run ID, overall workflows/s, server-window workflows/s,
server p95/p99 latency and persisted validation sample.

## How to calculate exact million-workflow numbers after a dedicated run

With one unique `runId` for the entire test, run this SQL **once the run
is quiescent**. Change `your-run-id` to the k6 output's run ID:

```sql
WITH samples AS (
    SELECT created_at, completed_at, state,
           extract(epoch FROM (completed_at-created_at)) * 1000 AS latency_ms
    FROM wfc_scale.hub_workflow
    WHERE hub_id='offboarding'
      AND definition_id='tenant-offboarding-fast'
      AND run_id='your-run-id'
), totals AS (
    SELECT count(*) AS accepted,
           count(*) FILTER (WHERE state=2) AS completed,
           count(*) FILTER (WHERE state=3) AS failed,
           min(created_at) AS first_created,
           max(completed_at) FILTER (WHERE state=2) AS last_completed,
           percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms)
              FILTER (WHERE state=2) AS p95_ms,
           percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ms)
              FILTER (WHERE state=2) AS p99_ms
    FROM samples
)
SELECT accepted, completed, failed,
       round(extract(epoch FROM (last_completed-first_created))::numeric,2)
           AS elapsed_seconds,
       round((completed / nullif(extract(epoch FROM
           (last_completed-first_created)),0))::numeric,2)
           AS completed_workflows_per_second,
       round(p95_ms::numeric,2) AS workflow_p95_ms,
       round(p99_ms::numeric,2) AS workflow_p99_ms
FROM totals;
```

The query reports time from the **first workflow creation to last completion**;
it includes submission overlap. Separate measured request-start throughput,
submit latency, workflow throughput and full workflow latency.

## Priority experiments

1. Instrument mean/p95/p99 Hub event-lane advisory-lock wait and claim
   duration; compare with empty-queue latency.
2. Batch `HubNativeHistory` inserts into a single statement per commit,
   while preserving fenced exactly-once committed outcomes.
3. Narrow the Hub-wide event lock to event-publish/match critical sections;
   per-workflow commits should rely on workflow row/revision fencing, but
   test timer wakeup, event fanout, duplicate/late events and crash recovery
   before changing lock scope.
4. Implement a set-based fast claim for **unconstrained** native tasks using
   row-level `SKIP LOCKED` and `UPDATE ... RETURNING`; retain the
   correctness-preserving policy-aware path for constrained tasks.
5. Re-run identical offboarding code at 1/4/8 workers, then vary Hub DB pool
   and Hub replicas independently. Progress from 1k -> 10k -> 100k ->
   1,000,000 with adequate dedicated infrastructure and data retention.

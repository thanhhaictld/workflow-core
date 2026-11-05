# Durable execution history (experimental)

This branch extends the PostgreSQL-distributed runtime with an **append-only**
execution journal at `wfc_scale.execution_history`. The existing
`wfc."ExecutionPointer"` rows remain the current-state snapshot and are not
an immutable audit trail.

## Storage and API

Apply `scaling/sql/005_execution_history.sql` **after** migrations 002–004.
Each journal row has a database-generated `history_id`, `workflow_id`,
`recorded_at` (database insertion time), `occurred_at` (event occurrence
time), `event_type`, optional work-item/pointer/step/attempt IDs,
the fenced workflow revision when available, and optional JSONB metadata.
Payloads and step input/output values are **not recorded by default**.

`ExecutionHistoryStore.ReadAsync(workflowId, afterHistoryId, take)` returns
history in ID order with a bounded page size (1–1000). The development-only
sample also exposes `GET /api/workflows/{id}/history?after=0&take=100`.
It is intentionally unauthenticated like the rest of the benchmark API:
add **tenant authorization**, rate limits and redaction before production use.

## Execution semantics

```mermaid
sequenceDiagram
  participant W as Worker / Engine
  participant H as History journal
  participant X as WorkflowCore executor
  participant DB as PostgreSQL workflow state
  W->>H: WorkAttemptStarted (autocommit)
  W->>X: Execute workflow
  X->>H: StepAttemptStarted (autocommit BEFORE body)
  X->>X: Run step body / external API
  X-->>W: Return, yield or throw (captured in memory)
  W->>DB: BEGIN; verify gate revision + ACK lease
  W->>DB: Update execution pointers, errors, subscriptions, follow-up work
  W->>H: StepBodyReturned/Yielded/Threw, StepCompleted/Failed/etc (same transaction)
  W->>H: WorkAttemptCommitted (same transaction)
  W->>DB: COMMIT
```

- `WorkflowStarted`: inserted with genesis pointer and initial queue work in
  one transaction. `WorkflowStatusChanged` is inserted with control-plane
  status transitions; `WorkflowCompleted` and step-state transitions are
  recorded with successful fenced execution.
- `WorkAttemptStarted`: inserted before running workflow executor. Contains
  only the work attempt count, work lease epoch and gate epoch.
- `StepAttemptStarted`: written independently **before** the step middleware
  invokes the next delegate. If it cannot be saved, the body is not invoked.
  Attempts carry a unique `step_attempt_id` even when the same pointer reruns.
- `StepBodyReturned`, `StepBodyYielded` and `StepBodyThrew`: captured by
  middleware but inserted only in the same PostgreSQL transaction as the
  successfully fenced workflow commit. Exceptions record their **type**, not
  the exception message.
- `StepCompleted`, `StepFailed`, `StepSleeping`,
  `StepWaitingForEvent`, `StepScheduled` and `StepStatusChanged` describe
  committed pointer transitions. `StepErrorRecorded` marks stored execution
  errors, including errors outside the step-body middleware.
- `SignalDelivered` is recorded in the same per-subscription transaction
  as the delivery marker and workflow wake-up. The underlying signal event
  remains in `signal_event`.

**Crash interpretation:** a started attempt with no committed outcome is an
**unknown outcome**, not evidence the external service call failed. Worker
loss can occur after the remote side effect and before the workflow commit;
retries still require stable idempotency keys or reconciliation. Append-only
history is not a Temporal deterministic replay engine.

**Fencing:** successful outcome records, pointer snapshots and work ACKs
commit together, so losing a fence does not leave a false committed success.
The independent `*Started` rows intentionally survive rolled-back work.
Status changes invoked by the control plane are independently fenced.

## Ordering, security and operations

- `occurred_at` records when an event occurred in the application and supports
  timeline display; worker clocks should be synchronized. It is not a global
  commit-order token, nor is `recorded_at`.
- `history_id` is an insertion sequence, **not** a strict commit order across
  concurrent PostgreSQL transactions. For an active workflow, a high-watermark
  cursor can miss an older row committed late. For audit exports, perform a
  final scan once a workflow is quiescent; streaming clients should overlap
  pages or use CDC rather than treating IDs as commit-order tokens.
- Keep permissions for `INSERT` and `SELECT` on the journal but deny
  `UPDATE` and `DELETE` to the worker principal. A separate maintenance
  principal should manage retention and archival.
- Each step body incurs an extra PostgreSQL commit for its start record,
  plus journal inserts in the workflow state transaction. Benchmark WAL, table
  growth, index bloat, checkpoints and write latency at your target throughput.
- There is **no implemented retention / partitioning / archival process**.
  Plan time-based partitioning and retention before high-volume production.
- Business payloads, output values and full exceptions are deliberately
  omitted to reduce PII/secret exposure. History is nonetheless sensitive
  workflow metadata; authorize by tenant and workflow ownership.
- A step deferred by `InitForExecution` or `BeforeExecute` may have no
  `StepAttemptStarted` because no step body is invoked. The corresponding
  committed pointer transition and any execution error are still captured.
- Journal storage is PostgreSQL-specific and active only when
  `AddDistributedWorkflowCore` is registered; legacy Workflow Core does not
  use the new middleware or journal.

## Diagnostics

```sql
SELECT history_id, occurred_at, recorded_at, event_type, work_item_id,
       execution_pointer_id, step_attempt_id, revision, details
FROM wfc_scale.execution_history
WHERE workflow_id = @workflow_id
ORDER BY history_id
LIMIT 500;

-- Attempts not associated with a committed StepBody... outcome.
SELECT h.*
FROM wfc_scale.execution_history h
WHERE h.event_type = 'StepAttemptStarted'
  AND NOT EXISTS (
      SELECT 1 FROM wfc_scale.execution_history outcome
      WHERE outcome.workflow_id = h.workflow_id
        AND outcome.step_attempt_id = h.step_attempt_id
        AND outcome.event_type IN ('StepBodyReturned', 'StepBodyYielded', 'StepBodyThrew')
  )
ORDER BY h.recorded_at;
```

Such unmatched attempts need investigation/reconciliation and are not
automatically safe to replay.

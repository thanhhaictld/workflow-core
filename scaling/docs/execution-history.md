# Distributed execution history

## Implementation

The PostgreSQL migration is `scaling/sql/005_execution_history.sql`.
`wfc_scale.execution_history` is append-only by runtime convention: rows
are never updated by the engine. Restrict UPDATE and DELETE privileges for the
application DB role in production. The integer `id` is the cursor; individual
event rows are ordered by insert assignment, not guaranteed wall-clock commit
order across unrelated workflows.

`ExecutionHistoryReader.ReadAsync(workflowId, afterId, limit)` reads at
most 500 rows ordered by ascending `id`. The reader is registered by
`AddDistributedWorkflowCore`.

## Recorded events

| Event | Transaction |
| --- | --- |
| `WorkflowStarted`, `StepScheduled` (genesis) | Start transaction, with workflow and initial work |
| `StepAttemptDispatched` | Independent pre-execution transaction, requires live queue lease and workflow fence |
| `StepCompleted`, `StepEnded`, `StepStateChanged`, `StepRetryScheduled`, `StepScheduled` | Execution state transaction, with pointers and work ACK |
| `StepFailed` | Execution transaction, for errors emitted by the workflow executor |
| `StepAttemptCommitted` | Execution transaction; marks successful commit, not necessarily successful step completion |
| `WorkflowStatusChanged` | Execution or control-plane status transaction |
| `SignalDelivered` | Per-subscription delivery transaction with signal marker and wakeup |
| `WorkRetryScheduled`, `WorkDeadLettered` | Conditional queue state update |

The attempt key uses work-item ID, lease epoch and pointer ID to distinguish
reclaims and to avoid duplicate dispatch entries if a request is retried.
A later `StepAttemptCommitted` has the same work ID, epoch and pointer ID in
its deduplication key. Journal rows contain only IDs, status labels, and small
metadata. They deliberately omit `Data`, `Outcome`, `PersistenceData`,
exception messages and event payloads.

## Crash and retry semantics

- A dispatch entry can exist without an outcome if a worker loses its lease,
  is killed, or throws before the state transaction commits. This is **unknown
  outcome**, not proof of failed external effects.
- The engine remains **at-least-once**: consumers must use external
  idempotency keys, reconciliation, or outbox patterns.
- The journal is **not** deterministic replay. Pointer snapshots remain
  authoritative, and time of execution or a remote side effect cannot be
  reproduced from these entries alone.
- Some eligible pointers may be deferred by workflow pipeline initialization
  rather than actually entering the step body. Therefore the start marker
  is called **dispatched**, not **executed**.
- A crash during a wholly in-memory execution may leave a dispatch without
  any terminal record; a later fenced attempt receives its own dispatch.
- Event fanout can commit per subscriber, so each delivered subscription gets
  its own journal record, not one atomic event for all subscribers.
- History is a troubleshooting and audit aid, not a substitute for
  security logging or external transactional semantics.

## Deployment

Apply migrations 002, 003, 004 and 005 in order. Running 005 against the
earlier prototype history schema drops its over-restrictive revision/event
uniqueness constraint and adds the event deduplication key.

For high-volume workflows, monitor journal insert volume, WAL, indexes,
vacuum and table growth. No archive or retention policy has been implemented.
Use database privileges to prevent runtime UPDATE/DELETE and define an
explicit archival horizon before production. The stored metadata is bounded
to 8 KiB by SQL constraint, but application owners must also avoid
embedding sensitive values in workflow definition/step names and IDs.

## Validation

PostgreSQL integration tests in `ExecutionHistoryTests.cs` exercise
committed lifecycle rows, pagination, redaction, retry transitions and stale
fencing. Run:

```bash
dotnet test tests/WorkflowCore.Scaling.PostgreSQL.Tests/WorkflowCore.Scaling.PostgreSQL.Tests.csproj
```

CI applies migration 005 before tests. Local builds require a .NET 10 SDK and
`WFC_TEST_PG` pointing to a prepared PostgreSQL instance.

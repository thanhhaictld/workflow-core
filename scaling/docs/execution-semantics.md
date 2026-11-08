# Execution, dispatch, and event semantics

> Scope: **direct PostgreSQL runtime** (`WorkflowCore.Scaling.PostgreSQL`).
> For the Hub native runtime, task transport and matching, start with the
> [current runtime guide](current-runtime.md). Historical branch/baseline notes
> below refer to this document's original review.

## Workflow creation and execution

```mermaid
sequenceDiagram
    autonumber
    participant C as Caller
    participant E as DistributedWorkflowEngine
    participant DB as PostgreSQL
    participant P as WorkPump
    participant Q as FencedWorkQueue
    participant X as IWorkflowExecutor
    C->>E: StartAsync(definition, data, idempotencyKey?)
    E->>DB: BEGIN
    E->>DB: INSERT request_dedupe (optional)
    E->>DB: INSERT wfc.Workflow + genesis pointer
    E->>DB: INSERT wfc_scale.work_item (workflow)
    E->>DB: COMMIT
    E-->>C: workflowId
    loop Each worker slot
      P->>Q: ClaimAsync(workflow, owner, lease)
      Q->>DB: SELECT ... FOR UPDATE SKIP LOCKED; UPDATE lease + epoch
      Q-->>P: LeasedWork
      P->>Q: AcquireWorkflowAsync(workflowId)
      Q->>DB: UPSERT workflow_gate + epoch
      Q-->>P: WorkflowFence
      P->>E: ExecuteWorkflowAsync(work, fence)
      E->>DB: Load workflow + pointers
      E->>X: Execute(workflow, cancellation token)
      X-->>E: Updated pointers, subscriptions, errors
      E->>DB: BEGIN
      E->>DB: Conditional UPDATE workflow_gate revision
      E->>DB: Conditional ACK work_item
      E->>DB: Update workflow; add subscriptions/errors
      E->>DB: Insert follow-up work and event replay intents
      E->>DB: COMMIT
      P->>Q: ReleaseGateAsync
    end
```

`ExecuteWorkflowAsync` first tests that `work.ItemKey` equals
`wf:{workflowId}:{fence.Revision}`; mismatched stale work items are
acknowledged without running steps. This is generation-based coalescing,
not arbitrary duplicate-message equivalence.

The successful state transition uses one EF transaction for the revision
advance, work ACK, workflow pointer updates, subscriptions, errors and next
work intents. Transaction rollback undoes them together. A workflow gate
lease is renewed by the worker heartbeat; unsuccessful lease renewal cancels
execution. Cancellation is best-effort for steps that ignore it.

**External service effects are outside this transaction.** Step authors must
use stable idempotency keys, reconcile uncertain outcomes, and avoid
irreversible non-idempotent work where possible.

## Event publication, delivery, and WaitFor

```mermaid
sequenceDiagram
    autonumber
    participant C as Caller
    participant E as Engine
    participant DB as PostgreSQL
    participant P as WorkPump
    C->>E: PublishEventAsync(name, key, payload, effectiveAt?, key?)
    E->>DB: Atomic INSERT signal_event + initial event work
    P->>E: ExecuteEventAsync(leased event)
    E->>DB: Read unprocessed signal and matching EF subscriptions
    alt No subscription yet
      E->>DB: ACK event work, retain signal_event.processed=false
    else Subscriptions exist
      loop For each subscription
        E->>DB: Acquire workflow gate
        E->>DB: In transaction: delivery marker + pointer activation
        E->>DB: Remove subscription + insert workflow work
      end
      E->>DB: Mark signal processed (if eligible); ACK event work
    end
    Note over E,DB: When WaitFor is created later, engine schedules replay intents for retained signals
```

On creation of a `WaitFor` subscription, `ExecuteWorkflowAsync` scans
unprocessed stored signals matching event name/key and subscription start time,
and inserts replay work with a per-event/per-subscription dedupe key.
`signal_delivery` deduplicates actual application to each subscription.

The event consumer implements an earlier-signal check and defers when an
older eligible signal is still pending. This includes a **hard-coded
`wfc."Subscription"` table reference**; the runtime currently assumes the
default EF schema `wfc`, regardless of the constructor's `schema` argument.
End-to-end concurrent event ordering is **not formally verified**; new
reconciliation/integration tests are needed for concurrent events, restart
during fan-out, and missing subscribers.

## State machines

| Object | States | Transition logic |
|---|---|---|
| Work item | 0 ready, 1 leased, 2 complete, 3 dead | Claim, ACK, retry, defer, expiration recovery, operator replay |
| Workflow gate | available (owner null or expired), owned | Acquire increments epoch; renewal extends lease; successful transaction advances revision |
| Workflow | Workflow Core `Runnable`, `Suspended`, `Complete`, `Terminated` | Existing executor and `ChangeStatusAsync` |
| Signal | unprocessed/processed | Remains unprocessed when no eligible subscriber yet |

## Delivery guarantees

- **Workflow creation:** state + first work item committed together.
- **Client retries:** optional `request_dedupe` key and request fingerprint;
  same key/same content returns same ID, mismatched content throws.
- **Work dispatch:** at least once via lease expiration/retry; `SKIP LOCKED`
  prevents simultaneous successful claim of the same row under normal DB behavior.
- **State writes:** conditional fence + revision + queue lease checks.
- **External APIs:** at least once unless the called service participates in
  idempotency/reconciliation.
- **Events:** matching subscriptions receive per-subscription delivery markers;
  not a globally exactly-once event log.

## Operational caution

If a workflow has been manually changed or an event needs reprocessing, do not
blindly insert new `work_item` rows without reviewing its gate revision and
the relevant signal delivery markers. Manual updates bypass engine
invariants. Use the documented operator procedure or a dedicated repair tool.

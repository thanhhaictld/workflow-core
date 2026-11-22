# Native activity completion: single-round-trip SQL experiment

## Baseline and question

This branch starts exactly from PR #42 head `40c97f96581cdba7d6f8261c976d12548e176c9e` (the optimistic matching-owner experiment). The only intended performance variable is SQL round trips for **native activity completion**.

`HubNativeWorkflowStore.CompleteActivityAsync` previously performed two required row-lock/validation reads, followed by five separate SQL write commands, then committed. The five writes were:

1. Fenced task completion (`hub_task`, requiring the current worker and epoch).
2. Mark the matching `hub_task_attempt` complete.
3. Insert the native result into `hub_native_inbox` with conflict protection.
4. Append the `ActivityResultReceived` workflow event.
5. Wake the native workflow by creating a runnable task when it is still runnable.

The experimental path executes these five operations in **one data-modifying CTE command**. Both modes retain the initial workflow-then-task locking order and one synchronous PostgreSQL transaction and commit. The command only inserts the inbox, journal and wake if the `completed` task CTE returned exactly one row. This prevents losing the lease check while consolidating writes.

## Switch

| Setting | Meaning |
| --- | --- |
| `WFC_HUB_COMBINED_ACTIVITY_COMPLETION=false` (default) | Existing five separate write commands, one transaction |
| `WFC_HUB_COMBINED_ACTIVITY_COMPLETION=true` | One PostgreSQL command with task ACK + attempt + inbox + history + wake CTEs, one transaction |

Both Hub replicas receive this environment variable in the HA Compose overlays.

## Preserved correctness

- No in-memory queue, asynchronous completion ACK or deferred PostgreSQL commit.
- The same task lease, worker and epoch CAS decides success.
- Same-workflow activity completion serializes on the workflow row; task lock order remains unchanged.
- Attempt completion, inbox, workflow event and runnable wake are conditional on successful task ACK.
- On SQL error, all writes roll back with the transaction; no partial success.
- Existing complete-task replay checks still return success only for the exact previous worker, epoch and matching JSON result.
- A stale or expired task cannot append a result, event or wake.

`HubNativeActivityCompletionRoundtripTests` exercises both modes for accepted completion, stale epoch, replay, worker mismatch, mismatched repeated result, durable history/attempt/inbox/wake, and expired lease.

## Measurement

The bounded-label histogram `workflow_hub_native_activity_completion_writes_seconds` measures the write phase (excluding initial validation/row locking and commit), labeled `mode=legacy|combined`. The A/B benchmark is `.github/workflows/workflow-hub-native-completion-sql-ab.yml`, which keeps ownership mode **strict** in both arms, uses two Hubs/four shards and varies only the completion SQL mode at 4/8/16 processors. Each job must durably complete 1,000 `policy-unordered` native workflows with one **routed activity per workflow** and zero failures. The CI verifies exactly 1,000 persisted `ActivityResultReceived` history events; the offboarding-fast workflow is intentionally not used because its steps execute inside the processor and do not exercise this SQL path. The benchmark reports throughput, workflow p95/p99, CPU, PostgreSQL commit count and sampled waits.

This experiment reduces commands per native activity completion, **not** transaction commits per workflow. If PostgreSQL WAL flushes or other operations dominate, throughput may not improve.

## Limits and next decisions

- Remote workflow activity completion is unchanged and should be a separate optimization if this experiment succeeds.
- Native workflow **snapshot commit** is unchanged; it already has other batching optimizations.
- The GitHub runner shares CPU among PostgreSQL, processors, two Hubs and k6. Do not claim a database scaling limit based on one run. Compare repeat warmed A/B runs on isolated hosts; inspect SQL duration, WAL bytes and PostgreSQL CPU per completed activity.
- Do not merge on performance grounds unless CI correctness is green and p95/throughput improvements repeat without extra retries or durable history regressions.

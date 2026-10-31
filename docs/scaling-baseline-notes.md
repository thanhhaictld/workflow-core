# Durable execution baseline: initial inventory

Status: incomplete S01 investigation, not a performance baseline. Checkout:
`76a882b533f73ebdb4e4bbc10c664864bfd6140a`. The task tracker remains the
authority for acceptance and dependencies.

## Initial writer and notification inventory

| Path | Existing behavior | Required durable treatment |
| --- | --- | --- |
| `WorkflowController` | Creates workflows and events; locks and persists suspend, resume and terminate. | Atomic creation with work/idempotency; fenced control mutations. |
| `WorkflowConsumer` | Acquires a workflow lock, executes and persists workflow with subscriptions. | Lease, version and delivery validation in the same transaction as state and next work. |
| `EventConsumer` | Locks event and workflow; persists workflow and terminates subscription through separate calls. | Deduplicated event delivery and subscription change in fenced workflow transaction. |
| `ActivityController` | Locks subscription and publishes result as an event. | Integrate token ownership and idempotent event delivery; prevent legacy writes on migrated shards. |
| `SyncWorkflowRunner` | Creates or locks and persists workflow directly. | Participate in durable transaction protocol or reject use on durable shards. |
| `WorkflowExecutor`, `ExecutionResultProcessor`, error handlers | Emit lifecycle notifications during execution. | Buffer and commit via outbox; an aborted transition must not publish committed notifications. |
| `RunnablePoller` | Uses global locks to poll runnable workflows, events and commands. | Use bounded durable claims and deterministic promotion. |
| EF persistence provider and purgers | Provide persistence operations outside any durable fencing contract. | Integrate transaction and ownership validation or exclude from durable ownership scopes. |
| Custom persistence and middleware | Extensible code may add writers or effects. | Inventory integrations and reject unsupported writers; external effects require idempotency. |

This inventory is preliminary: inspect all provider overrides, administrative and
purge paths, activity races and extension hooks before declaring S01 complete.
No query plans, workload sizes, throughput, lag, or database utilization have been
measured in this worktree. PostgreSQL provider and distributed failure tests have
not been run. Past results from another checkout are not worktree validation.

## Worktree test observation

The existing unit test project passed on net6.0, net8.0 and net10.0: 68 tests
per target, no failures or skips. Command: from `/workspace/.dotnet-run`, run
`dotnet test /workspace/workflow-core-durable/test/WorkflowCore.UnitTests/WorkflowCore.UnitTests.csproj`
with the prepared SDK and package-cache environment. Restore required execution
outside the filesystem sandbox because NuGet could not create its named mutex.
Warnings include the DiagnosticSource net6.0 support warning. This verifies the
legacy unit suite only; it does not verify the proposed durable implementation.

# Implementation inventory and history audit

**Scope:** files present in `feat/distributed-postgres-runtime` at baseline
commit `b475e877141f0d8626c0a2ca5281302227928d4a`, plus this
documentation. A commit message is not proof a change survives in the branch
head; inspect the final tree.

## New runtime files

| Path | Why it exists |
|---|---|
| `src/providers/WorkflowCore.Scaling.PostgreSQL/WorkflowCore.Scaling.PostgreSQL.csproj` | net10.0 library referencing Core, EF and PostgreSQL |
| `FencedWorkQueue.cs` | Claim, lease renewal, workflow gate, retry/defer and dead-letter exhaustion |
| `DistributedWorkflowEngine.cs` | Transactional workflow/event control plane and executor adapter |
| `DistributedWorkPump.cs` | Hosted polling worker slots, heartbeat, retry, OpenTelemetry instrumentation |
| `DistributedWorkflowServiceCollection.cs` | Registers new runtime and disables legacy background consumers |
| `DeferredWorkException.cs` | Signal/ownership contention deferral |
| `WorkQueueAdministration.cs` | Queue statistics and manual dead-letter replay |
| `src/samples/WorkflowCore.Scaling.Api/` | Unauthenticated development HTTP benchmark |
| `tests/WorkflowCore.Scaling.PostgreSQL.Tests/` | PostgreSQL integration suite |
| `.github/workflows/distributed-runtime.yml` | PostgreSQL 17 + .NET 10 CI |

## SQL and benchmark files

- `scaling/sql/002_distributed_runtime.sql`: `work_item`,
  `workflow_gate`, `signal_event`, indexes.
- `scaling/sql/003_signal_delivery.sql`: per-subscription delivery marker.
- `scaling/sql/004_request_dedupe.sql`: idempotent request creation keys.
- `scaling/benchmarks/k6-start-workflows.js`: offered API requests/sec
  benchmark, **not** completion throughput.

## Existing source modifications that remain at HEAD

- `src/providers/WorkflowCore.Persistence.EntityFramework/ExtensionMethods.cs`:
  mapping/serializer extension methods made publicly callable to bridge the
  new runtime and existing EF models.
- The new project references upstream Core and EF providers; it does not
  replace those projects with a wholly new engine.

## Important Git history caveat

An early intermediate commit in this branch
(`f1698a8f0739bfe6e59c7816bd51442d9bdf6b0a`) changed legacy
`RunnablePoller.cs`, `QueueConsumer.cs`, `WorkflowConsumer.cs` and
`EntityFrameworkPersistenceProvider.cs`, and introduced
`scaling/sql/001_legacy_indexes.sql`.

A later commit constructed a tree from the upstream base rather than that
earlier commit's tree. **Those legacy fixes and SQL 001 are not present in
the current branch HEAD.** The new distributed runtime does not enable
legacy polling/consumers, so those fixes are not part of its active
execution path. Never advertise those historical changes as shipped.

Future source maintenance should create trees from the **current branch
head tree** and use compare-and-swap ref updates so prior edits are not
silently lost.

## Prior test evidence

- Run [41207213239](https://github.com/thanhhaictld/workflow-core/actions/runs/41207213239):
  sample API built, 8/8 integration tests passed.
- This tests one PostgreSQL 17 instance; no measured throughput, multi-zone
  failover or disaster recovery guarantee.
- Source and documentation are versioned in the same Git branch. CI does
  not validate Markdown link integrity by itself.

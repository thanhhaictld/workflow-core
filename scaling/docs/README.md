# Distributed Workflow Core documentation

Start at the root [README](../../README.md), then read the
**[current runtime guide](current-runtime.md)**. It consolidates implemented
architecture, task delivery, scaling modes, configuration and source evidence.
Reviewed against the working tree on 8 November 2026; all distributed paths
remain experimental. Configuration in the repository does not prove a feature
is enabled in a live deployment.

## Current Hub guides

| Guide | Scope |
| --- | --- |
| [Current runtime](current-runtime.md) | Runtime choices, Hub components, assignment, SSE/long polling, replica ownership and migration baseline |
| [Native fluent runtime](hub-native-fluent-runtime.md) | Application registration, mappings, snapshots/inboxes, supported constructs and version compatibility |
| [Scheduler and remote execution runbook](hub-remote-scheduler-implementation.md) | Hub boundaries, endpoints, local execution and verification |
| [Activity policy updates](activity-policy-update.md) | Revision-checked operator concurrency changes |
| [Explicit activity ordering](explicit-partition-ordering.md) | Ordering policies and their relationship to partitioning |
| [Native commit event lane](native-commit-event-lane.md) | Event/subscription transaction serialization |
| [Operations dashboard](operations-dashboard.md) | Monitor operational data and limitations |
| [Monitor setup](../monitor/README.md) | Next.js monitor, development API and local stack |
| [Observability stack](../observability/README.md) | Compose setup, telemetry and workload commands |

## Matching and performance

The [batch matching and SSE task delivery review/plan](hub-batch-sse-dispatch-plan.md)
is historical design context. Default SSE carries readiness hints; experimental
opt-in [task distribution](sse-task-distributor.md) also pushes committed leases
under fenced sessions and explicit worker credits.

These documents describe implemented options and specific experiments. Their
branch names, measurements and comparisons refer to the recorded investigation,
not a guarantee for today's deployment. The current default transport is adaptive
claiming plus SSE hints; matching long polling is opt-in in code and enabled by
the HA Compose overlay.

- [Single-owner matching and HA](hub-distributed-matching-ha.md)
- [Ownership recovery and scaling design](cadence-inspired-scaling-design.md)
- [Ownership recovery validation and evidence](cadence-inspired-scaling-validation.md)
- [Multiple matching partitions](hub-matching-multi-partition.md)
- [Event-gated matching drain](hub-matching-event-gated-drain.md)
- [Matching and claim flow diagrams](hub-matching-claim-flow.md)
- [Experimental Hub push matching](hub-push-matching-design.md)
- [Hub push local benchmark](hub-push-benchmark-2026-11-14.md)
- [PostgreSQL notification design and rollout](hub-postgres-notify-sse.md)
- [Demand-aware claim admission](demand-aware-claim-admission.md)
- [Dispatch and FIFO-head experiment](demand-aware-dispatch-fifo-head-experiment.md)
- [Offboarding benchmark](offboarding-unconstrained-benchmark.md)
- [Processor scaling investigation](linear-processor-scaling-pr22.md)
- [Processor scaling diagnostics](processor-scaling-root-cause-diagnostics.md)
- [Performance tuning harness instructions](performance-tuning-harness.md)
- [Throughput investigation](throughput-investigation-report.md)
- [Claim/completion A/B](workflow-p95-claim-completion-ab.md)
- [SQL CPU investigation](sql-cpu-statement-investigation.md)
- [Claim candidate query review and local tuning](hub-claim-query-tuning.md)
- [Snapshot contract reuse and controlled throughput trials](snapshot-contract-reuse-throughput.md)
- [Capacity assessment](throughput-1000-to-million-assessment.md)
- [Historical 1,000-workflow activity benchmark](workflow-hub-1000-performance.md)

## Direct PostgreSQL runtime

The following documents describe `WorkflowCore.Scaling.PostgreSQL`, whose workers
access the database directly. Their statements about polling, host APIs and
absent matching components do not describe the Hub native runtime.

| Guide | Scope |
| --- | --- |
| [Architecture](architecture.md) | DistributedWorkPump, workflow gates and direct database execution |
| [Execution semantics](execution-semantics.md) | Transactions, retry and event delivery |
| [Execution history](execution-history.md) | Direct runtime durable attempts and audit |
| [Persistence](persistence.md) | Direct runtime schema and queries |
| [API and configuration](api-and-configuration.md) | Direct runtime DI and control-plane APIs |
| [Deployment](deployment.md) | Direct runtime migration and cutover |
| [Operations](operations.md) | Direct runtime leases, recovery and inspection |
| [Testing and benchmarks](testing-and-benchmarks.md) | Direct runtime validation and historical results |
| [Design decisions](design-decisions.md) | Direct runtime rationale |
| [Security](security.md) | Direct runtime trust boundaries |
| [Implementation inventory](implementation-inventory.md) | Direct runtime source inventory |
| [Production readiness](production-readiness.md) | Direct runtime readiness checklist |
| [Onboarding sample](onboarding-sample.md) | Direct runtime sample and signal flow |
| [Definition metadata](definition-version-management.md) | Separate legacy definition archive and schema diffs |

## Historical designs and issue reports

Retained for design context and existing links. Read their scope/status notices
before using examples; the current runtime guide takes precedence for present
architecture and deployment scope.

- [Original embedded scheduler plan](hub-embedded-scheduler-final-plan.md)
- [Original activity-only Gateway runbook](workflow-hub-gateway.md)
- [Native runtime implementation progress](native-runtime-progress.md)
- [Activity policy issue history](bug-activity-policy-changes.md)

## Sources of truth

- [Hub host](../src/WorkflowCore.Hub.Server/), [Gateway](../src/WorkflowCore.Hub.Gateway/),
  [durable stores](../src/WorkflowCore.Hub/), [scheduler](../src/WorkflowCore.Hub.Scheduling/)
- [Remote native execution](../src/WorkflowCore.RemoteExecution/)
- [Direct PostgreSQL runtime](../src/WorkflowCore.Scaling.PostgreSQL/)
- [Ordered migrations](../sql/) and [integration tests](../tests/WorkflowCore.Scaling.PostgreSQL.Tests/)
- [Compose configuration](../observability/compose.yaml) and [development script](../../scripts/dev.ps1)

Source and tests determine implemented behavior. Documentation proposals and
historical passing runs do not establish current verification results.

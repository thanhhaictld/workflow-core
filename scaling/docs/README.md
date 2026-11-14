# Distributed workflow documentation

## Start here — primary architecture

**[Hub-native architecture: partition candidate matching + fenced SSE push](current-runtime.md)** is the canonical description of the current distributed runtime, its configuration, failure recovery and tests. PostgreSQL is the sole lease and resource/FIFO authority; in-memory matching is advisory.

This is one selected architecture, not a menu of recommended polling/streaming alternatives. Adaptive HTTP claims and historical stream adapters remain implementation-level compatibility and emergency rollback paths. The development preset uses two Hub replicas, matching partitions and SSE push.

## Build and operate the Hub runtime

| Document | Purpose |
| --- | --- |
| [Native workflow contracts](hub-native-fluent-runtime.md) | C# workflow definitions, activity registration, supported constructs and version compatibility |
| [Scheduler and remote hosts](hub-remote-scheduler-implementation.md) | Generic Gateway, internal scheduling module, and separate processor/provisioner hosts |
| [Activity policy updates](activity-policy-update.md) | Controlled updates to concurrency and resource policy |
| [Ordering](explicit-partition-ordering.md) | Business ordering and strict FIFO, independently of matching partition routing |
| [Observability and dashboard](operations-dashboard.md) | Backlogs, task/activity latency, owner health and workflow trace |
| [Monitoring service](../monitor/README.md) | Next.js monitor and development API |
| [Compose and local operation](../observability/README.md) | The two-Hub stack, observability pipeline and load generator |

## Verification and investigation

Run the [three-window candidate/SSE failover suite](../benchmarks/hub-matching-failover-suite.ps1), the [PostgreSQL integration tests](../tests/WorkflowCore.Scaling.PostgreSQL.Tests/) and controlled multi-processor benchmarks. Use the [14 November 2026 local push benchmark](hub-push-benchmark-2026-11-14.md) **only** as evidence about the push transport; it did not enable candidate matching.

For performance investigations, these are retained as reference evidence rather than current architecture recommendations:

- [Throughput investigation](throughput-investigation-report.md) and [claim/completion A/B](workflow-p95-claim-completion-ab.md)
- [Claim SQL analysis](hub-claim-query-tuning.md) and [database CPU profiling](sql-cpu-statement-investigation.md)
- [Processor scaling](linear-processor-scaling-pr22.md) and [offboarding benchmark](offboarding-unconstrained-benchmark.md)
- [Historical matching owner HA experiments](hub-distributed-matching-ha.md) and [multiple matching partitions](hub-matching-multi-partition.md)
- [Original push protocol design](hub-push-matching-design.md) and [candidate implementation plan](hub-partition-candidate-matching-plan.md)

## Other engines and archived plans

The direct-PostgreSQL `DistributedWorkPump` path is a **different runtime** whose workers have database credentials. Its [architecture](architecture.md), [persistence](persistence.md), [operations](operations.md), [execution semantics](execution-semantics.md), [testing](testing-and-benchmarks.md) and [deployment](deployment.md) are retained for historical/reference use. It must not run against Hub-managed workflow state.

Older alternative designs—including [event-gated drain](hub-matching-event-gated-drain.md), [task SSE distributor](sse-task-distributor.md), [batch matching draft](hub-batch-sse-dispatch-plan.md), [PostgreSQL notification rollout](hub-postgres-notify-sse.md) and the [original embedded scheduler plan](hub-embedded-scheduler-final-plan.md)—remain accessible for decision history, but do **not** override the canonical Hub architecture.

Implementation and tests are the ultimate evidence: [Gateway](../src/WorkflowCore.Hub.Gateway/), [candidate registry](../src/WorkflowCore.Hub/HubPartitionCandidateRegistry.cs), [durable store](../src/WorkflowCore.Hub/HubTaskStore.cs), [processor](../src/WorkflowCore.RemoteExecution/) and [SQL migrations](../sql/). A passing earlier benchmark does not imply the current branch has passed all acceptance checks.

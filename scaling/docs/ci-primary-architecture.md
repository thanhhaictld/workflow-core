# CI contract: primary Hub runtime

This repository has **one supported distributed-runtime design**:
partition-owned in-memory candidate matching, bounded PostgreSQL-fenced
lease admission and SSE push delivery. See [current runtime](current-runtime.md).

## Continuous gates

| GitHub Actions file | Role |
| --- | --- |
| `distributed-runtime.yml` | Compile .NET / PostgreSQL runtime, integration tests, Next.js monitor and 1,000 native workflow end-to-end |
| `workflow-hub-candidate-sse-failover.yml` | Crash owner before claim, after COMMIT before send and during execution; assert fencing, no duplicates, credit cleanup and recovery |
| `workflow-hub-sse-push-e2e.yml` | Two Hubs / four matching partitions, eight workers, both routed workflow types and post-crash worker reuse using the **primary** SSE transport |
| `workflow-hub-observability.yml` | Candidate registry Prometheus metrics, Grafana, Next.js live proxy and SSE worker registrations |

The upstream `dotnet.yml` validates legacy Workflow Core libraries on
`master` PRs; `nuget-publish.yml` is retained for releases.

## Deliberate performance run

`workflow-hub-sse-stress-1000-16.yml` is **manual (`workflow_dispatch`)**:
1,000 native workflows with two Hubs, four matching shards and **16 processor
replicas**, including CPU/PostgreSQL evidence and workflow p95/p99. Run it
after correctness gates pass. GitHub runner throughput is not a reliable
production capacity prediction; compare identical runner configurations.

## Superseded CI

Deleted CI workflows for polling/event-gated matching, old adaptive SQL
admission A/B switches, artificial indexed bucket variants, direct database
scheduler baselines, historical two-Hub ownership experiments and older
processor-scaling permutations. Their benchmark scripts, SQL and investigation
documents remain in the repository as historical artifacts. We removed the
**automatic workflow invocations**, not the underlying domain tests.

A change to the supported runtime must preserve candidate matching and
owner-fenced SSE. The retained task-SSE distributor and capacity-fanout worker
modes are independently controlled by `WFC_HUB_TASK_SSE_PUSH` and
`WFC_HUB_WORKER_CAPACITY_FANOUT`; task SSE wins if both are enabled. CI checks `candidateEnabled`,
actual connected SSE sessions, durable completions and fencing rather than
searching for legacy startup log messages.

Correctness and performance must be validated on the **same commit** before
merging. Passing an older PR head is not acceptance of newer changes.

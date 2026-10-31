# Distributed Workflow Core — engineering documentation

**Status:** implemented experimental runtime; **not production-certified**.  
**Branch:** `feat/distributed-postgres-runtime`  
**Reviewed source baseline:** `b475e877141f0d8626c0a2ca5281302227928d4a` (31 October 2026).  
**Compatibility:** deliberate break with legacy scheduling, queueing and host APIs.

## Documentation map

| Document | Audience | Contents |
|---|---|---|
| [Architecture](architecture.md) | Architects/engineers | Context, boundaries, component diagram, scaling model |
| [Execution and event semantics](execution-semantics.md) | Runtime developers | Sequence diagrams, transaction boundaries, retries, `WaitFor` |
| [Execution history](execution-history.md) | Runtime developers and SREs | Durable step attempts, append-only events, audit guarantees, query API |
| [Workflow monitoring UI](../../monitor/README.md) | Operators / UI developers | Free AdminCN-inspired monitoring dashboard and Temporal-style step trace |
| [PostgreSQL data model](persistence.md) | Database engineers | Schema, indexes, queries, retention, reconciliation |
| [Public API and configuration](api-and-configuration.md) | Application developers | DI setup, APIs, runtime knobs, endpoint contract |
| [Design decisions](design-decisions.md) | Reviewers | Accepted choices, alternatives, consequences |
| [Deployment and migration](deployment.md) | Platform engineers | Fresh deployment, cutover, Kubernetes considerations, rollback |
| [Operations and recovery](operations.md) | SRE/on-call | Alerts, SQL inspection, stuck leases, dead letters, DR |
| [Observability and benchmarks](testing-and-benchmarks.md) | Performance engineers | Existing CI proof, test matrix, k6 and acceptance criteria |
| [Security model](security.md) | Security reviewers | Trust boundaries, permissions, secrets, data safeguards |
| [Implementation inventory](implementation-inventory.md) | Maintainers | Source map, commit ancestry caveat, precise implemented scope |
| [Production readiness gaps](production-readiness.md) | Owners | Known issues, unmet requirements, launch blockers |

## Quick orientation

The new .NET 10 assembly `WorkflowCore.Scaling.PostgreSQL` **reuses**
`IWorkflowExecutor` and existing PostgreSQL EF persistence models but provides
a **separate runtime control plane**: durable work items, PostgreSQL work claims,
fenced workflow gates and a hosted execution pump. There is no separate
scheduler/broker deployment today; each worker directly polls PostgreSQL using
`FOR UPDATE SKIP LOCKED`.

The API sample is for development benchmarks only and does **not** implement
authentication. CI builds the sample and executes PostgreSQL integration tests;
CI is **not** a proof of sustained horizontal throughput, availability or
crash consistency under all failure points.

## Sources of truth

- [New runtime source](../../src/providers/WorkflowCore.Scaling.PostgreSQL/)
- [Database migrations](../sql/)
- [Runtime integration tests](../../tests/WorkflowCore.Scaling.PostgreSQL.Tests/)
- [CI job](../../.github/workflows/distributed-runtime.yml)
- [Benchmark sample](../../src/samples/WorkflowCore.Scaling.Api/)
- [k6 script](../benchmarks/k6-start-workflows.js)

All guaranteed/implemented statements in this documentation refer to the
**current branch tree**, not just the historical commit messages.

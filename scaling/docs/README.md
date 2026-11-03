# Distributed Workflow Core — engineering documentation

**Status:** implemented experimental runtime; **not production-certified**.  
**Branch:** `feat/distributed-postgres-runtime`  
**Reviewed source baseline:** `b475e877141f0d8626c0a2ca5281302227928d4a` (31 October 2026).  
**Compatibility:** deliberate break with legacy scheduling, queueing and host APIs.

## Documentation map

| Document | Audience | Contents |
|---|---|---|
| [Architecture](architecture.md) | Architects/engineers | Context, boundaries, component diagram, scaling model |
| [Workflow Hub Gateway](workflow-hub-gateway.md) | API developers / SREs | Standalone REST+SSE Hub, code-first concurrency, remote activity workers and operational limitations |
| [1,000-workflow Hub performance](workflow-hub-1000-performance.md) | Performance engineers / SREs | Two measured passing CI simulations, throughput, p95, DB sessions, reproduction |
| [Execution and event semantics](execution-semantics.md) | Runtime developers | Sequence diagrams, transaction boundaries, retries, `WaitFor` |
| [Execution history](execution-history.md) | Runtime developers and SREs | Durable step attempts, append-only events, audit guarantees, query API |
| [Workflow monitoring UI](../../monitor/README.md) | Operators / UI developers | Free AdminCN-inspired monitoring dashboard and Temporal-style step trace |
| [Onboarding demo](onboarding-sample.md) | Testers and developers | Independent worker, synthetic tenant onboarding, retries, approval signals and trace review |
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

The legacy API sample is for development benchmarks only and does **not** implement
production authentication. The separate Hub gateway requires a development
bearer token, but also needs production authorization and identity hardening. CI builds the sample and executes PostgreSQL integration tests;
CI is **not** a proof of sustained horizontal throughput, availability or
crash consistency under all failure points.

## Sources of truth

- [New runtime source](../../scaling/src/WorkflowCore.Scaling.PostgreSQL/)
- [Database migrations](../sql/)
- [Runtime integration tests](../../scaling/tests/WorkflowCore.Scaling.PostgreSQL.Tests/)
- [CI job](../../.github/workflows/distributed-runtime.yml)
- [Control-plane sample](../../scaling/sample/onboarding/WorkflowCore.Scaling.Api/)
- [Standalone worker](../../scaling/sample/onboarding/WorkflowCore.Scaling.Worker/)
- [Sample workflows](../../scaling/sample/onboarding/WorkflowCore.Scaling.Samples/)
- [k6 script](../benchmarks/k6-start-workflows.js)

All guaranteed/implemented statements in this documentation refer to the
**current branch tree**, not just the historical commit messages.

- [Workflow definition metadata review](definition-version-management.md) — code-first executable definitions, immutable metadata snapshots and read-only schema diffs.
- [Workflow Hub Gateway](workflow-hub-gateway.md) — standalone, bounded-connection REST+SSE remote activity transport and PostgreSQL concurrency/FIFO admission. Existing workflow instances have not yet migrated to remote execution.

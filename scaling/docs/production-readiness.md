# Production readiness: gaps, risks and release checklist

> Scope: **direct PostgreSQL runtime** (`WorkflowCore.Scaling.PostgreSQL`).
> For the Hub native runtime, task transport and matching, start with the
> [current runtime guide](current-runtime.md). Historical branch/baseline notes
> below refer to this document's original review.

**Current verdict:** experimental implementation validated with a small
PostgreSQL integration suite; **not production-certified**.

## P0: launch blockers

| ID | Gap or risk | Why it matters | Completion evidence |
|---|---|---|---|
| P0-01 | API authentication, authorization and tenant isolation | Sample endpoints are unauthenticated | Authz integration tests, least-privilege access |
| P0-02 | Trusted serializer allowlist | Newtonsoft type-metadata deserialization can be dangerous with untrusted input | Binder/security tests and input policy |
| P0-03 | Concurrency/event-ordering race validation | `WaitFor` fan-out and earlier-signal checks have limited test coverage | Multi-replica fault/race matrix |
| P0-04 | Fault injection around commit and side effects | At-least-once execution may repeat external operations | Kill-point test suite plus downstream reconciliation |
| P0-05 | Operator recovery and audit | DLQ replay is callable but has no secured audited API | RBAC, replay approval, audit events |
| P0-06 | Data retention / archival | Completed work/events/dedupe records grow indefinitely | Backfill-safe cleanup with test coverage |
| P0-07 | Schema compatibility validation | SQL event-order check assumes `wfc."Subscription"` | Configurable schema or explicit schema guard |
| P0-08 | Deployment/rollback validation | Live legacy workflow migration is unsupported | Tested greenfield/cutover + snapshots + rollback runbook |

## P1: scaling and operability hardening

| ID | Gap | Expected deliverable |
|---|---|---|
| P1-01 | Sustained horizontal load tests | Concurrency vs throughput curve, P99 latency, DB saturation data |
| P1-02 | Outbox/dispatcher separation (if needed) | Decision informed by database bottleneck measurements |
| P1-03 | Tenant fairness and quotas | Per-tenant concurrency, capacity limiting, backpressure |
| P1-04 | Observability dashboards and alerts | OTEL exporter config, SLO dashboards, dead-letter alerts |
| P1-05 | Busy-queue dead-letter sweep | Dedicated periodic exhausted-lease reconciler |
| P1-06 | More complete status semantics | Suspend/resume/terminate races and state transition tests |
| P1-07 | HA PostgreSQL failover | Reproducible promotion/fencing/RPO-RTO tests |
| P1-08 | Work-item and signal lifecycle | Configurable retention windows and index maintenance |
| P1-09 | Error categorization | Transient vs permanent errors, per-workflow retry policy |
| P1-10 | Runtime upgrades | Mixed-version/safe rollout policy for workflow definition versions |

## P2: optional architecture evolution

- Independent scheduler service and broker adapter if PostgreSQL claim
  pressure dominates.
- Partition-aware worker placement for heavy tenant skew.
- Read models and lifecycle event stream for operator UI.
- Multi-region DR and eventually active/active **only after** conflict
  ownership, data consistency and failover protocols are designed.

## Acceptance gate (proposed)

1. Same commit and deployment image pass CI, 48-hour soak and deterministic
   retry/crash matrix, including multi-node event fan-out.
2. Concurrency scaling meets the team's stated workflow-completions/sec
   target with P99 scheduler lag and DB utilization inside budget.
3. No lost committed workflow transitions and no unauthorized state changes in
   failure/security tests; all external operations idempotent or reconcilable.
4. One zone outage, DB failover and restoration exercises have measured
   recovery times and a reviewed runbook.
5. Retention and replay windows, monitoring, alerts and on-call ownership are
   approved.
6. Migration and rollback procedures have been executed successfully in a
   representative staging environment.

**Do not infer “production complete” from passing CI.** The available
integration tests are valuable evidence of base functionality, not coverage
of all failure/race scenarios.

# Abrupt Hub and worker failure scenarios

This document records what the `hub-matching-failover-suite.ps1` integration runner tests. Each scenario runs against its own cloned PostgreSQL schema, unique test workflow run, and nginx proxy. It uses the primary partition-owned candidate matcher, multiplexed SSE push, and PostgreSQL-fenced leases. The suite is exercised by `.github/workflows/workflow-hub-candidate-sse-failover.yml`.

## Automated scenarios

| Scenario | Fault injected | Required evidence |
| --- | --- | --- |
| `before-claim` | Hard-kill Hub A before workflows are submitted | Hub B acquires the owner epoch; worker reconnects; both workflows finish |
| `after-claim` | Delay SSE transmission and hard-kill Hub A after a durable task lease is committed | Lost delivery is recovered via lease expiry; owner takeover and completion are durable |
| `during-execution` | Hard-kill Hub A while the surviving worker executes an activity | Worker reconnects to Hub B; running work is completed without bypassing the task fence |
| `worker-before-claim` | Hard-kill an idle worker with no graceful drain; start a replacement with its original credit journal | Replacement reconnects to the original Hub and executes both newly submitted workflows |
| `worker-during-execution` | Hard-kill the worker after a `provision-tenant` activity begins; start a replacement with its original credit journal | Orphaned lease expires; replacement claims remaining work; both workflows complete |

The worker cases deliberately reuse the same `WFC_HUB_PUSH_CREDIT_JOURNAL` file after a process hard-kill, approximating a same-identity restart on a persistent mount. Each test checks durable task completion, no duplicate same-epoch execution starts, no duplicate *observed* side-effect dispatch keys in the sample, a bounded candidate reservation count, and rejection of a stale heartbeat when an old lease was captured. Hub takeover tests also record owner-takeover and SSE-reconnect intervals.

The test sends two onboarding v5 workflows, with three sample activity executions per workflow. It is a **correctness smoke test**, not a sustained-load or disaster-recovery benchmark. Its sample side-effect log is not an independent external-service exactly-once guarantee.

## Run

After building the Release scaling solution and preparing the local PostgreSQL schema (see `scaling/observability/README.md`):

```powershell
pwsh -NoProfile -File scaling/benchmarks/hub-matching-failover-suite.ps1
```

Or run one scenario:

```powershell
pwsh -NoProfile -File scaling/benchmarks/hub-push-failover.ps1 -Scenario worker-during-execution
```

Each scenario writes an event timeline, old task lease, owner observations, side-effect counts, and outcome JSON to `scaling/benchmarks/results/push-local/failover-suite/`. CI uploads these artifacts.

## Not yet covered

- Simultaneous Hub and worker hard-kill under live workload.
- SIGTERM with a shortened Kubernetes termination grace period, preStop hooks, or an eviction with partial drain.
- Replacement on a different node **without** the original persisted journal (and the resulting lost local credit state).
- Network partitions where a process is alive but separated from PostgreSQL or peer Hubs.
- Sustained chaos at 16+ processors, multi-Hub owner churn, and measured recovery p95/p99.
- Real external activity side effects in the window after effect execution and before durable completion; idempotency must be exercised against a downstream service.

Do not interpret a passing suite as proof of exactly-once side effects or a production recovery-time objective.

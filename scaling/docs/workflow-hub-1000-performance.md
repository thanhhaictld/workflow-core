# Workflow Hub — 1,000 Concurrent Workflows: Measured E2E Benchmark

## Status

**PASS — 3 November 2026.** This is a reproducible GitHub Actions
performance and correctness scenario, **not** a production SLA, sustained
load test, or benchmark of the legacy IWorkflow<T> execution engine.

The Hub code-first *remote activity-chain* definition used by this
benchmark has three sequential steps:

1. validate-tenant (simulated 15 ms)
2. provision-tenant (simulated 400 ms, policy max 20 concurrent)
3. send-welcome (simulated 15 ms)

The Hub owns the durable workflow state, scheduling, task leases, result
commit, history and release of next tasks. Remote workers execute the C#
handlers and never receive WFC_PG.

This is the new HubWorkflowRegistry path, **not** a fully migrated adapter
for arbitrary legacy Workflow Core fluent workflow definitions. Existing
IWorkflow<T> instances still use their earlier PostgreSQL-backed runtime.

## System under test

| Component | CI configuration |
| --- | --- |
| Workflows submitted | 1,000 |
| Simultaneously pending before workers start | 1,000 |
| Activity steps | 3,000 |
| Hub gateway | 1 ASP.NET Core .NET 10 process |
| Durable state | PostgreSQL 17 service container |
| Hub PostgreSQL connection pool | 12 max |
| Remote worker processes | 4 |
| Execution slots | 32 per worker (128 total) |
| Hub activity policy | cap 100; provision activity cap 20 |
| Submission HTTP concurrency | 200 |
| Source of load | .NET 10 HttpClient runner on a GitHub-hosted runner |

The load runner **first submits all workflows with workers stopped**.
It requires all 1,000 instances to be durably pending. It then starts
four DB-free worker processes and waits for all workflows to reach a
terminal state. This distinction is essential: 1,000 activity tasks by
themselves would not verify 1,000 multi-step workflows.

Every step completion uses Hub-side atomic commit + next-step wakeup.
The benchmark verifies the complete ordered history and result shape
for 10 sampled workflow instances.

## Observed results

| Metric | Run 1 | Run 2 |
| --- | ---: | ---: |
| Accepted workflows | 1,000 | 1,000 |
| Completed workflows | 1,000 | 1,000 |
| Failed workflows | 0 | 0 |
| Simultaneously pending before workers | 1,000 | 1,000 |
| Submission duration | 8.967 s | 9.901 s |
| Submission p95 HTTP latency | 2,319.86 ms | 2,821.03 ms |
| Processing duration | 58.263 s | 47.188 s |
| Throughput during processing | 17.16 workflows/s | 21.19 workflows/s |
| Workflow age at completion p50 | 42,580.36 ms | 33,742.09 ms |
| Workflow age at completion p95 | 59,214.60 ms | 48,119.42 ms |
| Workflow age at completion p99 | 60,006.36 ms | 49,014.91 ms |
| Peak PostgreSQL sessions | 12 | 12 |
| Peak active DB sessions | — | 8 |
| Peak leased activities | — | 52 |
| Complete workflow histories inspected | 10 | 10 |

Source runs, both passed:
- Run 1: https://github.com/thanhhaictld/workflow-core/actions/runs/41567989259
- Run 2: https://github.com/thanhhaictld/workflow-core/actions/runs/41568076193

The GitHub Actions artifact **workflow-hub-1000-performance** contains the
machine-readable result JSON and Hub/worker logs. The most recent result
is not a guarantee of future throughput: GitHub-hosted runner CPU and
scheduling conditions vary from run to run.

**Latency interpretation:** workflow age is measured from database
created_at to completed_at, so its p50/p95/p99 includes *intentional*
pre-execution staging. The processing throughput is counted from the
verifier's start after workers launch; it is a different interval.
Do not treat age percentiles as isolated per-activity service latencies.

## Reproduce

Prerequisites: PowerShell 7, .NET SDK 10, PostgreSQL 17, and the branch.

~~~powershell
pwsh ./scripts/dev.ps1 bootstrap

# All terminals use the same private-lab token and Hub URL.
$env:WFC_HUB_AUTH_TOKEN = 'local-development-secret-change-me-2026'
$env:WFC_HUB_URL = 'http://127.0.0.1:5100'
$env:WFC_LOAD_RUN_ID = "manual-$([guid]::NewGuid().ToString('N'))"
$env:WFC_LOAD_COUNT = '1000'
$env:WFC_LOAD_SUBMISSION_CONCURRENCY = '200'
$env:WFC_LOAD_REPORT = './workflow-hub-1000.json'

# Terminal 1: Hub; do NOT start remote workers yet.
pwsh ./scripts/dev.ps1 hub

# Terminal 2: submit 1000 workflows, assert all are pending.
$env:WFC_LOAD_PHASE = 'submit'
dotnet run -c Release --project src/samples/WorkflowCore.Hub.LoadTest

# Terminals 3-6: start four remote workers (with no WFC_PG).
$env:WORKERS = '32'
pwsh ./scripts/dev.ps1 remote-worker

# Terminal 2, after workers are running:
$env:WFC_LOAD_PHASE = 'verify'
dotnet run -c Release --project src/samples/WorkflowCore.Hub.LoadTest
~~~

In manual runs, WFC_LOAD_RUN_ID and WFC_LOAD_REPORT must remain unchanged
between the submit and verify phases. Start four instances of the remote
worker command to match CI.

The exact CI harness is in
.github/workflows/distributed-runtime.yml, job hub-load-1000.
It stages pending instances before worker processes start and checks:

- Accepted == completed == 1000, with failed == 0.
- Peak simultaneous pending == 1000, prior to starting workers.
- 10 sampled workflow histories contain 0, 1, 2 in order and correct results.
- Peak PostgreSQL connections <= 24 (the Hub pool itself is capped at 12).
- All activities ran through REST + SSE, with worker WFC_PG unset.

## Interpretation and next performance work

Connection isolation works in this test: only the Hub uses the workflow
state database, with observed peak of 12 connections. Throughput is
**17–21 workflows/s** across the two runs, not thousands per second.

The present HubTaskStore claim path evaluates candidates one at a time,
opens transactions and checks concurrency resources for each claim.
The provision-tenant activity also has an explicit limit of 20 parallel
attempts, while the benchmark deploys 128 worker slots. These are
potential bottlenecks, but a profile is needed before attributing the
measured throughput to any single cause.

Before setting an SLO or shipping production, run several controlled
configurations (8/12/24 DB connections, 20/50 activity concurrency,
1/4/8 worker processes), add bulk claim/permit admission, measure
PostgreSQL CPU/IO/locks, prove recovery under worker/Hub restarts,
and migrate the legacy Workflow Core state machine to the remote Hub
protocol. Recheck end-to-end behavior with real external side effects
and idempotency/fencing at the downstream service.

## Correctness tests

HubWorkflowE2ETests covers durable multi-step transitions,
idempotent starts/completions, same-tenant FIFO across workflows,
retry with higher epoch, and stale completion rejection.
HubGatewayTests covers global resource exclusivity, limited concurrency
and simultaneous worker claims. The older onboarding tests remain
enabled. All are run by the distributed-tests CI job.

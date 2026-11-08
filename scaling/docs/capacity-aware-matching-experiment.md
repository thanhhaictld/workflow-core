# Capacity-aware matching — worker-side shard reservations (Phase 1)

**Branch:** `feat/hub-capacity-aware-matching-from-pr27`  
**Base:** [PR #27](https://github.com/thanhhaictld/workflow-core/pull/27) at `818ec2183a3e4b72a0ea8b8d76d7b287d6254326`. **The existing PostgreSQL optimization migrations 020–022, including `ix_hub_task_claim_sequence`, are inherited unchanged.**  
**Status:** Experimental, default OFF; not Microsoft Durable Task-style push.

**Diagrams:** [High-level topology, task-dispatch sequence and failover](capacity-aware-matching-diagrams.md).

## Motivation and scope

Current native worker lanes use one `/workers/tasks:poll` at a time and **rotate** across the four matching shards. The worker already communicates free execution slots through `HubClaimRequest.MaxTasks`, but only one shard gets attention during each up-to-four-second matching wait. This may inflate ready-to-claim latency when tasks are ready on other shards.

**Phase 1 tests the narrow hypothesis:** If a worker reserves one free execution slot for each of several concurrent shard polls, does it fill idle worker capacity sooner without expensive SQL claim churn?

The experiment changes **only the processor's request scheduling**:

- Opt in with `WFC_HUB_WORKER_CAPACITY_FANOUT=true` on remote workers; default `false`.
- `WFC_HUB_WORKER_MAX_SHARD_POLLS=4` sets a per-lane maximum, clamped to 1..min(4, available concurrency, configured matching shards). Default 4.
- Requires a single-Hub worker lane and matching partitions > 1. Existing multi-Hub SSE/adaptive requests, and original single-poll behavior, remain untouched.
- Each pending long poll consumes **one reservation** and uses `MaxTasks=1`. An active task also consumes one slot. Invariant: **executing tasks + pending polls <= lane concurrency**. A particular worker never has two simultaneous polls for the same matching shard.
- Polling remains authenticated REST via the existing gateway and matcher, which check current matching ownership before PostgreSQL claims.
- The existing PostgreSQL task leases, resource permits, FIFO eligibility checks, lease epochs, completion APIs, worker heartbeats and execution-history commits are unchanged.
- Instrument `workflow_hub_worker_shard_poll_credits` (worker-side up/down credits); compare existing `worker_claim_requests`, `worker_claim_http_duration`, Hub claim/admission metrics and PostgreSQL statement cost.

**This is not a durable capacity registry or push transport.** No persisted worker session, stream delivery ACK, offer-before-lease, replay-safe credit generation or replacement of the existing dispatcher is introduced.

## Tradeoffs and failure modes

| Benefit to test | Possible cost/regression |
|---|---|
| Avoid 4-second sequential shard rotation when a ready task is elsewhere | Up to four open HTTP requests per idle lane, more parked waiters and possibly more speculative SQL probes |
| Worker slots cannot be overbooked by concurrent replies | Each poll claims at most one task; less effective batch amortization than the original up-to-16-task claim |
| All matching shards may have active worker demand | More concurrent requests may oversubscribe a 4-vCPU CI runner and contend with workflow completion |
| No new distributed ownership protocol | Still vulnerable to missed hints, slow candidate scans and slow FIFO permit release; periodic recovery remains essential |
| Easy rollback | Stale/failed HTTP polls may consume a slot until timeout/cancellation; 1-second bounded network retry backoff |

On Hub failover, outstanding REST requests may return errors or time out. Worker retries; already-issued PostgreSQL leases remain fenced and are not instantly reassigned just because HTTP delivery failed. The one-credit-per-lease invariant is local and applies *before* tasks are issued, not a global resource fairness guarantee.

## Validation and decision gates

- Unit tests: distinct shards per pending poll, no oversubscription, fair rotation, invalid release/over-advertisement rejected.
- Existing distributed PostgreSQL, matching HA/failover and FIFO correctness suites remain mandatory.
- [.github/workflows/workflow-hub-worker-capacity-ab.yml](../../.github/workflows/workflow-hub-worker-capacity-ab.yml) executes OFF → ON → OFF with 8 processor replicas, two Hub replicas, four shards, and 200 workflows per stage *per policy* (unordered / partition FIFO / keyed FIFO); checks durable completion, zero extra activity attempts and FIFO non-overlap. It samples PostgreSQL transaction volume and shared-runner CPU.
- Keep `WFC_HUB_FIFO_HEAD_PREFILTER_ENABLED=false` in both arms: previous evidence found a ~46% keyed-FIFO throughput regression with that flag ON.
- Do not promote on a single CI pass: compare p95/p99 **and** throughput, workload drift, empty claims, worker demand idle time, resource admission cost and DB connections. Repeat with 4/8/16 processors on dedicated resources, separated from PostgreSQL/Hub CPU, before an improvement claim.

## Follow-on design choice

Only if measurable matching gaps remain after this experiment, consider true Hub-side session/credit registration and gRPC push. It requires additional protocol semantics for stream ownership, ACK loss, fencing during failover, bounded credits, fair scheduling and worker reconnection. The gateway would remain a transport/auth boundary, and PostgreSQL would remain the authority for task admission.

For the existing bottleneck evidence and scope limitations see [throughput investigation report](throughput-investigation-report.md).


## First completed CI outcome (2026-11-08)

[Capacity-aware OFF→ON→OFF run 42447369071](https://github.com/thanhhaictld/workflow-core/actions/runs/42447369071) **passed** all 9 cohorts with 200 workflows each, durable history and FIFO/partition ordering checks. This tests **eight worker replicas**, two Hubs and four matching shards on shared GitHub-hosted infrastructure with migrations 020–022.

| Policy | OFF1 WF/s | ON WF/s | OFF2 WF/s | ON / mean OFF | OFF drift |
|---|---:|---:|---:|---:|---:|
| Unordered | 5.85 | **8.54** | 5.97 | **1.45x** | +2.1% |
| Partition FIFO | 4.35 | 5.00 | 6.50 | 0.92x | **+49.3%** |
| Keyed FIFO | 7.21 | **14.36** | 7.26 | **1.99x** | +0.6% |

| Policy | OFF1 p95 / ON p95 / OFF2 p95 (s) | OFF1 p99 / ON p99 / OFF2 p99 (s) | PostgreSQL commits/WF OFF1 / ON / OFF2 | Host CPU busy OFF1 / ON / OFF2 |
|---|---|---|---|---|
| Unordered | 30.08 / **22.25** / 25.34 | 33.40 / **22.43** / 32.43 | 60.8 / **75.8** / 61.1 | 73.7% / **99.7%** / 76.5% |
| Partition FIFO | 40.74 / 17.47 / 29.91 | 45.24 / 39.27 / 30.22 | 73.8 / 94.7 / 67.8 | 37.7% / 45.6% / 56.3% |
| Keyed FIFO | 26.74 / **13.29** / 23.44 | 27.29 / **13.41** / 27.09 | 65.8 / **76.1** / 64.2 | 51.2% / **97.8%** / 59.6% |

**Assessment:** Promising unordered (+45% throughput) and keyed FIFO (+99%) single-run gains with unusually stable OFF controls; cannot interpret partition FIFO because of 49% OFF drift. **Important costs:** extra HTTP polls claim `MaxTasks=1`, giving ~17–24% more PostgreSQL commits/workflow on unordered and keyed cases, and saturating shared runner CPU when enabled. The measured result shows dispatch parallelism can improve utilization, but is not yet a production scaling or efficiency win. OTel metrics need to break down empty match claims and connection usage, including DB transaction cost per completed activity, before promotion.

**CI integration caveat:** [OTEL Observability Smoke run 42447369037](https://github.com/thanhhaictld/workflow-core/actions/runs/42447369037) failed while waiting for the *removed* legacy monitor service on port 5000. The head inherited a PR #27 change removing the legacy monitor API from Compose; the smoke workflow still asserted its existence. That check runs with capacity fanout OFF and therefore is **not evidence of a feature regression**. Commit `64876e7` updates the smoke test to validate the Hub-only UI and REST proxy. Confirm the replacement CI succeeds before declaring all checks green.

**Next gate:** Re-run the experiment on dedicated Hub/PostgreSQL/processor compute with 4/8/16 worker replicas and repeat each treatment. Monitor resource head handoff latency, ready-to-claim p95/p99, Hub waiter counts, empty SQL claims, PostgreSQL transactions/WAL per completed activity and CPU headroom. If throughput advantage disappears after CPU isolation, keep existing single-poll mode. No gRPC push or durable worker credits are warranted solely from this benchmark.

## Local PowerShell / Make commands

The root `Makefile` uses **PowerShell 7 (`pwsh`) as its recipe shell** on Windows, macOS and Linux. GNU Make and Docker Compose v2 are also required. Commands below work from the repository root and automatically combine the base Compose file, the two-Hub matching HA overlay and the four-shard overlay. They preserve the PostgreSQL migrations and database flags already configured in `scaling/observability/.env`.

```powershell
# One-time setup; edit the local .env token and DB password before starting.
make obs-setup

# 2 Hubs, 4 matching shards, 8 native-only processor replicas, fanout ON.
make obs-offboarding-fanout-up

# Inspect services and confirm "Capacity-aware shard polling enabled" in logs.
make obs-offboarding-fanout-ps
make obs-offboarding-fanout-logs

# Run the native offboarding k6 workload with the same HA topology.
make obs-offboarding-fanout-test K6_OFFBOARD_COUNT=1000 K6_OFFBOARD_VUS=200

# Stop services without removing PostgreSQL or Grafana volumes.
make obs-offboarding-fanout-down
```

Optional overrides: `OFFBOARD_PROCESSORS=16`, `FANOUT_MATCHING_SHARDS=4`, `FANOUT_MAX_SHARD_POLLS=4`, and `OBS_ENV_FILE=scaling/observability/.env`. The `obs-offboarding-fanout-*` commands set `WFC_HUB_WORKER_CAPACITY_FANOUT=true`, `WFC_HUB_LONG_POLL_ENABLED=true`, and `WFC_HUB_MATCHING_EVENT_GATED=true` only for those targets. The existing `obs-offboarding-*` commands continue to use their normal Compose configuration.

**Note:** `obs-offboarding-fanout-test` measures the three-step native offboarding workload; it is not the keyed-FIFO benchmark from CI. For a controlled OFF/ON comparison, use the dedicated `workflow-hub-worker-capacity-ab.yml` benchmark.

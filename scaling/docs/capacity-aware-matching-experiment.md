# Capacity-aware matching — worker-side shard reservations (Phase 1)

**Branch:** `feat/hub-capacity-aware-matching-from-pr27`  
**Base:** [PR #27](https://github.com/thanhhaictld/workflow-core/pull/27) at `818ec2183a3e4b72a0ea8b8d76d7b287d6254326`. **The existing PostgreSQL optimization migrations 020–022, including `ix_hub_task_claim_sequence`, are inherited unchanged.**  
**Status:** Experimental, default OFF; not Microsoft Durable Task-style push.

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

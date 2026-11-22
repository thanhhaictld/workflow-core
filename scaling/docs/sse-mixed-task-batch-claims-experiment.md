# Experiment: PostgreSQL batch claiming for activity and non-native task SSE lanes

**Branch**: `experiment/sse-mixed-task-batch-claims`, based on **PR #44** `b992d2f649511aaecb88eaccfad5ebd5c6040d7b`. This deliberately preserves the opt-in completion microbatch in PR #44 but **disables completion microbatching in the claim-only A/B**.

## Current path and change

The existing `HubTaskStore.ClaimUnconstrainedBatchAsync` already accepts batches of eligible activities, standalone tasks, `__workflow__` and `__native_workflow__`. The retained task SSE dispatcher (`HubSseTaskDistributor`) artificially restricts every lane other than an exclusively `__native_workflow__` lane to **one** reserved credit and SQL claim.

The experiment removes that transport restriction behind `WFC_HUB_SSE_CLAIM_BATCH_SIZE` (1,4,8,16,32), preserving **32** as the existing upper bound for native-only workflow claims. The dispatcher:

1. Accepts one-shot **actual free worker execution credits**, never creating speculative lease capacity. All credits remain associated with worker identity, Hub, matching shard, session ID and matching-owner epoch.
2. Optionally collects multiple already-reserved credits for **at most 0–5 ms** on the Hub; the first credit immediately occupies a `_claiming` slot to prevent overbooking during the collection window. Nothing waits for a target batch to fill after the deadline.
3. Calls `HubTaskMatcher.LongPollAsync` once with `MaxTasks = credit count` (1–32), using existing PostgreSQL-authoritative filters and `FOR UPDATE SKIP LOCKED` claim SQL. Candidate matching can also atomically reserve its own selected task candidates. Matching and activity ordering policies stay in the database.
4. Emits one individually fenced `task.leased {creditId,task}` SSE event for every actually leased task, and `credit.unused {creditId}` for unused reservations. Batch **claiming** does not change the SSE wire format to a combined frame. The Hub rechecks matching-owner epoch before each dispatch; worker verifies the credit/session ID, then runs each task with its own task epoch and heartbeat.
5. Optionally posts HTTP credits concurrently (bounded 1–32 per worker lane) to avoid serial HTTP grant latency preventing any actual multi-credit batches. Worker coordinator already enforces `executing + offered <= lane capacity`; concurrent sends do not add new slots.

## Flags

| Scope | Setting | Default | Experiment |
|---|---|---:|---|
| Hub (both replicas) | `WFC_HUB_SSE_CLAIM_BATCH_SIZE` | 1 | 1 / 4 / 8 / 16 / 32 |
| Hub (both replicas) | `WFC_HUB_SSE_CREDIT_COALESCE_MS` | 0 | 3 |
| Worker | `WFC_HUB_SSE_CREDIT_SEND_CONCURRENCY` | 1 | 16 |
| Worker | `WFC_HUB_TASK_SSE_PUSH` | false | true |
| Worker | `WFC_HUB_PUSH_ENABLED` | true | **false** |
| Hub | `WFC_HUB_BATCH_UNCONSTRAINED_CLAIMS` | unchanged | true |
| Worker | `WFC_HUB_COMPLETION_MICROBATCH_ENABLED` | false | false |

**Important:** Golden `WFC_HUB_PUSH_ENABLED=true` takes precedence over retained task SSE in `HubRuntimeWorker`. The A/B forces it off so the measured code path is genuinely `tasks:stream` + `tasks:credit`.

## Instrumentation and correctness

The claim result is independently measured on each Hub replica:

- OTel histograms `workflow_hub_sse_claim_requested_credits` and `workflow_hub_sse_claim_leased_tasks`, with bounded label `lane=native_workflow|mixed`, distinguish desired batches from actual leases.
- Authenticated `/api/hub/workers/transport-diagnostics` includes `sseMixedClaimCalls`, `sseMixedClaimedTasks`, `sseMixedMultiTaskClaims`, `sseMixedMaxTasksClaimed`, and `sseActiveSessions`. Aggregate **both** Hub replicas for CI; the counters are process-local, not durable.
- `HubSseMixedBatchClaimTests` prove credit-reserved SQL claim sizes for non-native workflow, activity and standalone tasks; no extra grants beyond capacity; one task ID per credit; partial batch unused-credit recovery; baseline one-credit behavior; invalid option rejection. Existing owner-loss/failover SSE and PostgreSQL tests continue running.

### A/B

`.github/workflows/workflow-hub-sse-mixed-claim-benchmark.yml` executes **10 jobs**: batch limits 1,4,8,16,32 × 8/16 processor replicas. Each job starts two Hubs, four matching shards and 1,000 native `policy-unordered` workflows that **each produce one routed activity task**. The benchmark verifies **1,000 durable ActivityResultReceived records** and that the new task SSE transport claimed at least 1,000 mixed-lane tasks, not merely produced SSE readiness hints. Experimental modes must demonstrate at least one **actual** multi-task claim. The single-claim baseline must have zero multi-task claims. The workflow measures throughput, workflow p95/p99, PG commits/workflow, actual mixed claim batch fill and host CPU/waits.

The experiment changes **PostgreSQL claim batch size**, not completion microbatching or SSE frame counts. Matching policy/ordering remains authoritative. The jobs run on noisy GitHub-hosted CPUs and are screening evidence; repeat multiple matched A/B runs before treating results as a production capacity measurement.

**Safety / rollout:** keep the PR draft and flags default-off until real multi-task claims are observed, ordered-keyed/FIFO scenarios pass on the retained SSE transport, and no unacceptable p99 or owner-takeover regressions are observed. Burst HTTP credit sending is optional and should be rolled back independently if it harms fairness or dispatch latency.

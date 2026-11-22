# Experiment: Four processors, 32 vs 64 PostgreSQL claims via SSE

## Baseline and scope

Branch `experiment/sse-4-processors-64-claim` derives from the PR #45 `experiment/sse-mixed-task-batch-claims` baseline. This is a **4-processor** follow-up to the earlier 8/16-processor experiments. The retained one-shot-credit `tasks:stream` SSE path must be enabled; golden push sessions must be **OFF** or the new code path is bypassed.

Extend the claim limit from 32 to 64 in three distinct boundaries: the SSE distributor (`WFC_HUB_SSE_CLAIM_BATCH_SIZE`), `HubTaskStore.ClaimAsync`, and `HubTaskStore.ClaimCandidateBatchAsync`. The candidate acquisition drain can accept a maximum 64 credits with `WFC_HUB_CANDIDATE_BATCH_SIZE=64`. Existing public REST `tasks:poll` / `tasks:claim` gateway validation remains capped at 32; the 64-task increase applies to the **internally invoked SSE path** and its PostgreSQL store. The owner epoch, task lease epoch, worker credit cap, eligibility, resource permits and ordering policy are unchanged.

## Controlled A/B

`.github/workflows/workflow-hub-sse-4p-claim-ab.yml` runs four independent jobs:

| Processor replicas | PostgreSQL claim limit | Actual activity slots per processor | Credits offered concurrently | Hub coalescing |
|---:|---:|---:|---:|---:|
| 4 | 1 (baseline) | 64 | 32 | 0ms |
| 4 | 16 | 64 | 32 | 3ms |
| 4 | 32 | 64 | 32 | 3ms |
| 4 | 64 | 64 | 32 | 3ms |

All runs use two Hub replicas, four matching shards, one Hub-owner generation per shard, 1,000 unordered native workflows with one **real routed activity** each, 200 k6 VUs, unbatched completion persistence, and `WFC_HUB_CANDIDATE_BATCH_SIZE=64` for matched owner capacity. No processor-local completed-result batching is enabled.

Each arm **must** verify 1,000 completed workflows, zero failed, exactly 1,000 durable `ActivityResultReceived` events, >=1,000 actual mixed-lane SSE leases, and (for batched modes) at least one multi-task PostgreSQL claim. The claim limit **does not guarantee** an actual batch of that size; record maximum leased tasks/claim, mixed multi-task claims, and average leases/claim including empty requests. Record workflows/sec, workflow p95/p99, PostgreSQL commits/workflow, CPU and sampled PostgreSQL wait events.

## Additional correctness gates

`HubSseMixedBatchClaimTests` checks delivery of exactly 64 distinct individually fenced SSE tasks from 64 one-shot reserved worker credits, including session-capacity enforcement and rejecting an invalid limit of 65. `HubLargeBatchClaimTests` verifies real PostgreSQL claims for 64 independent tasks using **both** unconstrained batch leasing and the candidate admission SQL with owner lease. The existing SSE owner-failover and FIFO tests remain required.

## Interpretation and limitations

Claims are performed **per matching shard and per registered worker session**, not across all partitions or all processors in one transaction. A configured limit of 64 may be underfilled. Per-task fencing, POST-COMMIT ACK and SSE credit/session checks remain mandatory. The candidate owner and PostgreSQL are authoritative for task eligibility.

Because GitHub-hosted test jobs run on independent shared runners, one observation cannot establish stable p95/p99 or reliable capacity. This screening A/B also has **fixed 64 activity execution slots** per processor unlike the earlier PR #45 benchmark (32), so it should be compared internally within this run rather than interpreted as a pure cross-run scaling comparison. For production recommendation, repeat on a stable dedicated runner with warmups, controlled CPU, and monitor empty-claim ratio, lease-renewal pressure, maximum batch occupancy and per-task dispatch p99. Feature flags default to earlier safe settings; keep PR draft.

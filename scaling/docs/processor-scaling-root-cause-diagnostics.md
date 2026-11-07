# Processor scaling diagnosis: 8 → 16 → 8 bracket

**Branch:** `perf/demand-aware-claim-admission-from-pr26`  
**Experimental, no production default changes.**  
**Measurement run:** [Capacity attribution 42376571948](https://github.com/thanhhaictld/workflow-core/actions/runs/42376571948).  
**Benchmark source:** `.github/workflows/workflow-hub-capacity-attribution.yml`.

## What was actually measured

Two Hub replicas, four matching shards, 12 pooled PostgreSQL connections per Hub, 200 native policy workflows per cohort, 75 ms asynchronous activity body, workflow/queue state persisted in a shared PostgreSQL instance. Shared GitHub runner reports only **4 logical CPUs** for all Hub, DB, k6, and processor containers.

Each processor-mode job runs 8a → 16 → 8b processor instances *on the same runner* and verifies unordered, partition-FIFO, and keyed-FIFO workloads after each stage. The second 8-stage control estimates load drift. Host CPU uses `/proc/stat`; PostgreSQL cumulative transaction and WAL deltas use `pg_stat_database` and `pg_stat_wal`; sampled wait events are not attribution of client pool waits. Do not infer multi-host linear scaling from these jobs.

### Mixed-Hub processor (original sample, matching only on activity lane)

| Policy | 8a WF/s | 16 WF/s | 8b WF/s | 16 / mean(8a,8b) | 8-run drift |
|---|---:|---:|---:|---:|---:|
| Unordered | 9.15 | 5.22 | 9.87 | 0.55× | +7.9% |
| Partition FIFO | 7.95 | 9.20 | 8.09 | 1.15× | +1.7% |
| Keyed FIFO | 13.50 | 11.70 | 10.22 | 0.99× | -24.3% |

Unordered CPU busy: 94.3% at 8a, 99.6% at 16, 99.2% at 8b. PostgreSQL commits/workflow: 45.5 → 60.7 → 47.3, while WAL KB/workflow is roughly 39–40. Extra DB transactions at 16 without a proportional WAL increase are consistent with read/empty claim overhead, but `pg_stat_database` includes unrelated/background work and does not prove the statement-level culprit. Sampled `Lock:object` and WAL-related waits warrant follow-up but are too sparse for a lock diagnosis.

### Policy-only processor (matching on workflow **and** activity lanes)

| Policy | 8a WF/s | 16 WF/s | 8b WF/s | 16 / mean(8a,8b) | 8-run drift |
|---|---:|---:|---:|---:|---:|
| Unordered | 6.63 | 4.59 | 4.61 | 0.82× | -30.5% |
| Partition FIFO | 4.90 | 6.22 | 5.69 | 1.18× | +16.1% |
| Keyed FIFO | 11.67 | 12.32 | 7.99 | 1.25× | -31.5% |

Policy-only mode does **not** demonstrate better throughput. It sees ~61–65 DB commits per unordered workflow at 8a/16 and as many as ~83 by 8b. Several controls drift >20%, meaning no stable speedup conclusion. Two jobs ran on different shared runners; direct policy-only vs mixed numerical comparisons are not causal.

## Code-path findings

1. **Different worker paths by Hub capability set:** `HubRuntimeWorker.ExecuteAsync` chooses matching long-poll only if all capabilities in the *worker lane* belong to one Hub. The offboarding sample originally registers both `offboarding` and `policy-benchmark`, so workflow workers use the adaptive SSE/REST claim path even when `WFC_HUB_LONG_POLL_ENABLED=true`. Activity workers remain single-Hub and do use matching. The new opt-in `POLICY_BENCHMARK_ONLY=true` registers only policy workloads to isolate actual matching on both lanes.
2. **Unnecessary owner-verification query at poll entry:** The Gateway already reads the current matching partition owner and checks that the returned owner/lease is local and valid. It then issues `StillOwnsPartitionAsync` before parking the worker. `HubEventGatedClaimDispatcher` executes another `StillOwnsPartitionAsync` immediately before any SQL claim; the legacy matching path also checks ownership before claiming. The new flag `WFC_HUB_MATCHING_DEFER_OWNER_CHECK=true` skips **only** the duplicate Gateway entry check. The matcher still executes the authoritative DB/epoch validation before claiming; task leases and completion fencing remain unchanged. Default OFF.
3. **Fixed DB claim admission and hot matching shards:** Event-gated matching retains one active SQL drain per matching shard; both Hubs share PostgreSQL. Additional processor capacity does not linearly expand claim drains and can increase network/poll/ownership checks without accelerating short business activities.
4. **Benchmark capacity limit:** A 75 ms `Task.Delay` is deliberately lightweight; it stresses scheduling/coordination rather than substantial provisioning work. 16 processor containers on four vCPUs are *not* evidence of a genuine multi-host processor scale-out test.

## Reproduction and guardrails

`workflow-hub-capacity-attribution.yml` measures three variants: original mixed-Hub worker lanes, policy-only worker lanes with original Gateway owner check, and policy-only with only that extra Gateway check deferred. All variants retain the same Postgres admission limits and 8→16→8 workload bracket.

- `POLICY_BENCHMARK_ONLY` is disabled by default, and changes only sample registration.
- `WFC_HUB_MATCHING_DEFER_OWNER_CHECK` is disabled by default and changes no task lease/fencing contract. The dispatcher verifies ownership before claiming, and an ownership-loss unit test asserts that a stale owner cannot claim a task.
- Hub matching HA/crash-takeover, native history, keyed FIFO, and 1000-workflow CI remain mandatory.
- A flag is not promoted merely because the job passes; evaluate repeated results, warmup, drift, p95/p99, error/replay counts, DB transaction cost, pool waits and dedicated-host CPU.

## Follow-up decision

If deferred owner-check improves claims/throughput without breaking HA, it is a low-complexity candidate. If host CPU is saturated, run the same workload on separate Hub/PostgreSQL hardware with processor count scaled independently. If PostgreSQL transaction volume scales with idle processor requests, prioritize capacity registration and claim batching, but do not implement processor partition ownership without a demonstrated workload locality benefit.

**Do not merge PR #27 by default solely on green CI.** The original scoped/FIFO changes have mixed A/B results, and the new owner-check experiment requires its own acceptance evidence.

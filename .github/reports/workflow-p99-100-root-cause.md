# 100-workflow P99 forensic benchmark (2026-11-06)

**Source:** [GitHub Actions run 42178755081](https://github.com/thanhhaictld/workflow-core/actions/runs/42178755081), [artifact 13483834863](https://github.com/thanhhaictld/workflow-core/actions/runs/42178755081/artifacts/13483834863). Branch: `bench/100-workflow-p99-root-cause`.

## Experiment
- 100 workflows per execution policy per processor count; 8 and 16 worker containers, **600 workflows total**.
- One Hub, PostgreSQL, 4-vCPU 15-GiB CI runner; 60 k6 VUs, 75ms simulated activity body.
- Modes: Unordered, Partition FIFO (8 partitions), Keyed FIFO (8 account resource keys).
- All 600 succeeded with complete activity attempt timelines, 0 activity retries, 0 ordering conflicts.
- 300 native workflow task claim journal events per 100 workflows in every run (three per workflow).
- All six run reports had 100/100 workflow native-first-claim, activity creation, activity attempt and native-resume timestamps.

### Workflow completion p99 and activity queue p99 (seconds)

| Mode | 8 workers workflow p99 | 16 workers workflow p99 | 8 workers activity queue p99 | 16 workers activity queue p99 |
|---|---:|---:|---:|---:|
| Unordered | 8.992 | 9.778 | 0.382 | 0.679 |
| Partition FIFO | 10.726 | 14.821 | 6.263 | 10.304 |
| Keyed FIFO | 7.897 | 13.245 | 4.711 | 9.299 |

### Six-stage percentile decomposition at 16 workers (seconds, p99)

| Stage | Unordered | Partition FIFO | Keyed FIFO |
|---|---:|---:|---:|
| Creation → first native claim | 1.646 | 1.433 | 0.899 |
| First native claim → activity task created | 5.136 | 3.458 | 2.949 |
| **Activity task created → activity claimed** | 0.679 | **10.304** | **9.299** |
| Activity claimed → activity completed (lease held) | 2.840 | 2.088 | 1.979 |
| Activity completed → native claim to resume | 2.748 | 1.172 | 1.284 |
| Native resume claim → workflow completed | 1.412 | 1.096 | 1.078 |

**Caution:** These are individual segment p99 values drawn from different workflows; they must **not** be added to explain end-to-end p99. Use a complete individual trace for an additive breakdown.

### Actual slowest workflow timelines at 16 workers

| Phase (s) | Unordered (9.827 s total) | Partition FIFO (14.859 s total) | Keyed FIFO (13.248 s total) |
|---|---:|---:|---:|
| Initial queue | 0.460 | 1.438 | 0.847 |
| Initial processing + commit | **5.266** | 2.942 | 2.642 |
| Activity admission queue | 0.484 | **10.283** | **9.350** |
| Activity lease (includes HTTP ACK) | 2.285 | 0.123 | 0.231 |
| Native resume queue | 0.318 | 0.025 | 0.068 |
| Final processing | 1.014 | 0.049 | 0.111 |

For the slowest FIFO workflow, activity queue wait is ~69–71% of total lifetime.

### Why the FIFO queue accumulates despite a 75ms activity body

Leases are the **resource permits** and remain occupied until activity result completion is acknowledged by Hub, not merely until the 75ms `Task.Delay` ends. Observed **mean** task lease duration:

| Mode | 8 workers mean lease | 16 workers mean lease |
|---|---:|---:|
| Unordered | 1.01s | 2.31s |
| Partition FIFO | **0.61s** | **1.00s** |
| Keyed FIFO | **0.48s** | **0.94s** |

There are 8 independent capacity-one FIFO permits. As a rough ceiling (assuming full, balanced permit utilization), 8 / 1.00s ≈ **8 activities/s** for partition FIFO; observed workflows throughput was **6.305/s**. For keyed FIFO, 8 / 0.94s ≈ **8.5 activities/s**, vs observed **7.112/s**. These are approximate capacity illustrations, not fitted queue models.

Thus the confirmed explanation for **where p99 grows** is: *long-lived lease permits plus queued tasks*, with additional time in native workflow scheduling. The mechanism that makes leases take 0.94–1s for a 75ms body needs more granular HTTP/DB measurements; it may include Hub completion transaction, .NET scheduling delays, HTTP transport and shared CPU/connection contention.

### PostgreSQL wait samples

Every 2 seconds, sampled active `pg_stat_activity` backends (one fixed shared CI runner). During 16-worker partition FIFO: 8 transactionid-lock backend observations and 4 tuple-lock observations; keyed FIFO: 8 transactionid-lock and 7 tuple-lock observations. Both show some row-level database waiting under load. The samples are sparse, are not SQL-statement-attributed, and are insufficient to prove how much latency each lock contributes. Npgsql pool acquisition waits and CPU scheduling are not represented.

### Root-cause finding and next diagnostic

**Established:** Activity admission queue is the dominant long segment for the slowest FIFO workflows, worsens greatly with 16 workers, and a capacity-one FIFO permit is held for the *entire activity lease/ACK interval* rather than the 75ms body only.

**Not yet proved:** Which share of the lease/ACK time or candidate admission delay is attributable to (a) Hub HTTP handler, (b) PostgreSQL row/transaction locks, (c) Npgsql connection-pool waiting, (d) 96-candidate scan amplification, (e) scheduler thread-pool/CPU contention.

**Next:** Before changing pessimistic locks to optimistic or adding a batch fast path, instrument activity lease receipt → body start/end → HTTP completion started/accepted, Hub completion handler → pool acquisition → SQL transaction/commit, and claim candidate scans/lock wait; run the same 100-workflow 8/16 test with these timings, preferably also on separate CPU hosts. Preserve FIFO safety and epoch fencing.

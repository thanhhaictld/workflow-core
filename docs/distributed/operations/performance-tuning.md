# Throughput tuning without hiding tail latency

Complete [Day 2: batching and latency](../tutorials/day-2-batching.md) first.

## Tune distinct layers independently

| Layer | Controls | Watch for |
| --- | --- | --- |
| Worker throughput | Processor replicas; `WORKFLOW_WORKERS`; `ACTIVITY_WORKERS` | Worker saturation, idle credits, external API rate limits |
| Candidate matching | `WFC_HUB_CANDIDATE_BATCH_SIZE`; `WFC_HUB_CANDIDATE_CROSS_WORKER_BATCH` | Batch fill, waiting worker credit, assignment fairness |
| PostgreSQL claim admission | `WFC_HUB_CLAIM_ADMISSION_LIMIT`; `WFC_HUB_CLAIM_ADMISSION_MODE`; `WFC_HUB_CANDIDATE_ADMISSION_CONCURRENCY` | Npgsql pool waits, excessive speculative claims, completion starvation |
| Unconstrained SQL claims | `WFC_HUB_BATCH_UNCONSTRAINED_CLAIMS` | PostgreSQL statement time and lock wait; differs from candidate batching |
| Native workflow commit | `WFC_HUB_NATIVE_SKIP_EMPTY_SUBS_SQL`; `WFC_HUB_NATIVE_BULK_EVENT_SQL`; `WFC_HUB_NATIVE_BULK_ACTIVITIES`; `WFC_HUB_NATIVE_COMPACT_RECEIPT` | WAL per workflow, p95/p99 completion, exact replay correctness |

The repo's `make ups-16` golden preset enables cross-worker candidate batching with a cap of 32 and demand-aware admission, but leaves the unconstrained SQL batch path disabled. A larger cap is an upper bound, **not a fixed waiting period**; actual fill, backlog and fairness determine whether it helps.

## The commit batching caveat

A separate PostgreSQL microbenchmark of **600 completed native activity tasks**, eight concurrent callers and a batch of 16 found persistence throughput of approximately **1,226 → 1,536 tasks/s** (+25.3%), but activity completion ACK p95 changed from **9.4 ms → 103.5 ms**. This batching API is a **microbenchmark**, not enabled in the live Gateway completion route. Do not enable imaginary batch-completion settings in deployment instructions.

The integrated A–D native *workflow* commit optimizations were measured separately on the fast offboarding sample. Two completed 1,000-workflow-per-arm A/B runs found +3.6–4.9% workflow throughput and −1.3–3.3% workflow p95, with −10.9–11.2% PostgreSQL CPU/workflow. These are shared-CI, workload-specific measurements—not a guarantee for routed parallel activities.

## A defensible experiment

1. Fix the workload, workflow definition/version, Hub count, worker count, CPU/DB resources and offered load.
2. Warm the deployment; use unique run IDs and verify 100% durable completion.
3. Run OFF → ON → ON → OFF and retain every failed run as evidence.
4. Record **workflows/s**, workflow p95/p99, schedule-to-start, completion ACK, PostgreSQL CPU-ms/workflow, WAL bytes/workflow, connections, waits, retry/duplicate effects.
5. Roll back on any fencing/idempotency regression even if throughput rises.

Use the [benchmark worksheet](../tutorials/benchmark-worksheet.md) and [configuration reference](../reference/configuration.md).

**More evidence:** [native commit A–D report](https://github.com/thanhhaictld/workflow-core/blob/thanhhaictld/hub-memory-candidates-luna/scaling/docs/native-commit-phased-optimization.md) · [complete vs batch transaction experiment](https://github.com/thanhhaictld/workflow-core/blob/thanhhaictld/hub-memory-candidates-luna/scaling/docs/native-task-completion-batch-commit-experiment.md).

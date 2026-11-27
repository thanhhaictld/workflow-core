> [Distributed Hub learning path](../learning-path.md) · [Source code](https://github.com/thanhhaictld/workflow-core/tree/thanhhaictld/hub-memory-candidates-luna/scaling) · Development lab, not a production runbook.

# Benchmark worksheet

Use the [Day 2 batching tutorial](day-2-batching.md) for the measurement procedure.

## Trial results

For repeatable performance decisions, fill this out **per trial**, not from a single GitHub Actions screenshot:

| Setting / result | Baseline | Optimized |
| --- | --- | --- |
| Commit SHA and Compose flags |  |  |
| Hub replicas, matching partitions, processor replicas |  |  |
| PostgreSQL topology and CPU/memory limits |  |  |
| Workload: workflow type, steps, number of workflows, VUs |  |  |
| Completed / failed / retried workflows |  |  |
| Durable workflows/second |  |  |
| Workflow completion p50 / p95 / p99 |  |  |
| Ready → lease p95 / p99 |  |  |
| Activity completion ACK p95 / p99 |  |  |
| PostgreSQL CPU-ms/workflow, WAL bytes/workflow |  |  |
| PostgreSQL lock and connection-pool waits |  |  |

Run **OFF → ON → ON → OFF**, take more repetitions on isolated hardware, keep raw logs, and treat any failed durability or fencing checks as a failed experiment regardless of throughput.


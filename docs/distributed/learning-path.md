# Learn distributed workflows in two days

A practical route from the first durable workflow to a measured two-Hub deployment. All commands are scoped to the [Hub-native runtime](index.md) in this fork.

## Day 1: A working application

1. **[First durable workflow](tutorials/day-1-first-workflow.md)** — install PostgreSQL migrations, start one Hub, run the existing .NET Provisioner, register a native fluent workflow, and submit it through REST.
2. Change a workflow step or add a new definition in the sample application. Keep the definition ID/version and the registration contract consistent.
3. Inspect the workflow status, durable snapshot, events and history. Restart the application host to see that state survives.

**Checkpoint:** A single Hub and a single application host can resume and complete a real workflow.

## Day 2: Distributed execution and measurement

1. **[Scale to two Hubs](tutorials/day-2-multi-hub.md)** — add load balancing, owner URLs, four matching partitions, two processor replicas, then increase to 16.
2. **[Batch claims and latency](tutorials/day-2-batching.md)** — run the same durable workload with different candidate batch caps and admission limits. Compare throughput and p95/p99, not only task-claim query time.
3. **[PostgreSQL topologies](tutorials/day-2-postgresql.md)** — understand a shared writable primary, a primary+standby HA setup, and why two writable shards need a new ownership/routing design.
4. **[Benchmark worksheet](tutorials/benchmark-worksheet.md)** — record control flags, counts, PostgreSQL CPU/WAL, schedule-to-start p95/p99 and durable completions.

**Checkpoint:** You can explain how Hub scale-out and PostgreSQL lease authority interact, reproduce a 1,000-workflow benchmark and recognize a throughput/latency trade-off.

## After Day 2

- [Architecture and ownership](architecture/index.md)
- [Operations and monitoring](operations/deployment.md)
- [Performance tuning strategy](operations/performance-tuning.md)
- [Configuration flags](reference/configuration.md)
- [Troubleshooting](reference/troubleshooting.md)

!!! note "Do not confuse the two batch optimizations"
    **Claim batching** groups lease admissions for selected tasks. **Completion batching** groups already-finished results into a database transaction. They affect different latency distributions and have different implementation status. The independent completion-batch microbenchmark has **not** been wired into the production Gateway REST completion path.

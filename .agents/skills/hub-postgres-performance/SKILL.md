---
name: hub-postgres-performance
description: Diagnose Workflow Core Hub throughput, latency and scaling bottlenecks by correlating PostgreSQL query, wait and index statistics with Prometheus and durable workflow evidence. Use for benchmark reports, PostgreSQL load, slow Hub queues, index reviews and capacity decisions.
---

# Diagnose Hub performance

Find the stage limiting workflow completions and the smallest change or experiment that can improve it. Use the Hub's durable outcomes to validate throughput and retries. Use Prometheus to locate when and where work slowed, and PostgreSQL evidence to attribute database work.

1. **Identify the workload.** Work from the repo root and follow `AGENTS.md`. For a latest-report request, identify the latest completed relevant cohort by the run manifest, UTC timestamps and durable audit. Start with the available artifacts. For a live investigation, verify the target environment, database, deployed build, active Hub and processor instances, and effective topology before collecting. Read the [runtime architecture](../../../scaling/docs/current-runtime.md) and the scheduler sections named in `AGENTS.md` for the path under investigation.
2. **Align the evidence.** Use one named UTC workload window and record the actual Prometheus sample and SQL snapshot bounds. Compare submitted/completed/failed workflows, retries, server completions/s and workflow p50/p95/p99 with CPU, queueing and stage timings. Keep source, cohort, sampling and coverage explicit. Use [metrics and experiments](references/metrics-and-experiments.md) for collection, durable timestamps and reproducible comparisons.
3. **Localize the limit.** Match the onset of backlog or the completion-rate plateau to the signals below. Check per-instance and per-shard distributions, including the drain tail. For a database hypothesis, rank interval statements, check waits and maintenance, and trace the leading fingerprint to the current SQL and plan. Use [PostgreSQL diagnosis](references/postgres-diagnosis.md) for interval stats, plan safety and index retirement checks. Require a second independent signal or an isolated experiment before calling a cause confirmed.
4. **Report what to scale or change.** Lead with the slow stage, measured impact and confidence. Show the key time-aligned numbers and link the raw run evidence. Separate observations, the leading hypothesis and open alternatives. Recommend one next lever or controlled comparison, with success measured in workflow completions/s, tail latency, PostgreSQL CPU per completion and durable correctness. Scope database changes, service changes and load generation to what the user requested.

## Match signals to a scaling decision

| Evidence in the same interval | What to investigate |
| --- | --- |
| Ready work builds while workers hit slot or CPU limits and database admission stays responsive | Processor capacity. Test more workers or slots with spare CPU and sufficient Hub and database headroom. Verify the ready queue actually drains faster. |
| Ready work builds while workers have spare capacity, and claim or admission wait rises | Hub discovery, owner/shard skew, compatible worker credit, batch fill, admission limits or connection-pool waits. A transport p95 alone cannot locate the wait. Compare candidate, admission and database signals before raising concurrency. |
| PostgreSQL CPU nears its effective budget as completions flatten, and statement calls or logical work per workflow rise | Rank statements by aggregate cost and amplification. Check plans for excess scanning and empty polls, then measure CPU and throughput after reducing the identified work. |
| Completions flatten with CPU headroom, but row/advisory lock waits, I/O waits or WAL activity rise | Check the wait type and blocking chain, hot workflow or shard keys, checkpoint/full-page-image activity and transaction duration. Connections or CPU alone cannot distinguish contention from storage delay. |
| Throughput falls while median completion improves or activity duration stays steady | Compare enqueue-to-claim p95/p99 and the time of the last completions. Inspect where late workflows queue and whether one shard or worker pool owns the tail. |

When the user asks for a change, preserve owner and lease fencing, transactional admission, idempotency, retries, timers and durable history. A query improvement, a disk-space reduction and a workflow throughput improvement each need their own evidence.

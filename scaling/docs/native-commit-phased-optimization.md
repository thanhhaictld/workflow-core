# Four-phase native commit write optimization

These PRs are stacked to ensure the benchmark in each stage isolates **only one additional SQL-path change**:

| Stage | Branch / PR | Flag (default false) | Paired workload |
|---|---|---|---|
| A | `opt/native-commit-a-noop-sql` / #51 | `WFC_HUB_NATIVE_SKIP_EMPTY_SUBS_SQL` | no subscriptions, no activities |
| B | `opt/native-commit-b-bulk-event-state` / #52 | `WFC_HUB_NATIVE_BULK_EVENT_SQL` | 8 consumed inbox messages + 12 replacement subscriptions |
| C | `opt/native-commit-c-bulk-activities` / #53 | `WFC_HUB_NATIVE_BULK_ACTIVITIES` | 16 native activity dispatches + corresponding history per commit |
| D | `opt/native-commit-d-compact-receipt` / #54 | `WFC_HUB_NATIVE_COMPACT_RECEIPT` | 80 KiB low-compression snapshot + 64 history events |

All optimizations are optional and keep the event-lane acquisition, workflow/task lock order, owner/epoch fencing and original synchronous PostgreSQL transaction boundary. No Hub buffering or worker API changes are part of this stack.

## Benchmark method

`scaling/benchmarks/native-commit-phases/Program.cs` creates valid leased native workflow fixtures in PostgreSQL and performs real `HubNativeWorkflowStore.CommitAsync` calls. Each phase's dedicated CI does 128 independent workflows per trial, eight concurrent callers, OFF → ON → ON → OFF, on the same shared runner and PostgreSQL 17 database. Fixture setup occurs outside the measured interval. Every trial verifies completed workflow tasks and phase-specific durable inbox/subscription/activity/history records. It collects transaction/commit p50/p95/p99, throughput, PostgreSQL container CPU-ms/workflow, `pg_stat_wal` bytes/workflow, `pg_stat_io`, PostgreSQL buffer reads/hits and cgroup physical disk bytes. D also measures stored `result_json` receipt bytes.

**Important:** this is native workflow persistence latency, not end-to-end workflow completion time. Do not extrapolate these results to 1,000-workflow performance or claim PostgreSQL's absolute limit.

## Results (first completed comparison)

| Phase | Throughput OFF→ON | PostgreSQL CPU ms/workflow OFF→ON | Native commit p95 OFF→ON | Native commit p99 OFF→ON |
|---|---|---|---|---|
| A | 487.638→577.474/s (+18.4%) | 3.099→2.537 (−18.2%) | 59.728→57.615 ms (−3.5%) | 101.924→102.498 ms (+0.6%) |
| B | 187.785→278.218/s (+48.2%) | 8.056→5.618 (−30.3%) | 102.391→97.513 ms (−4.8%) | 159.817→155.498 ms (−2.7%) |

A: [CI 45646515862](https://github.com/thanhhaictld/workflow-core/actions/runs/45646515862). B: [CI 45646758109](https://github.com/thanhhaictld/workflow-core/actions/runs/45646758109). These are first-pass two-sample means and require replication for production promotion. C/D: pending final validation and results at document creation.

## Regression checks

- Legacy and bulk mode both run `HubNativeRuntimeTests`, including concurrent event publication, timer behavior, dispatch fencing/cancellation and lost-response exact replay.
- B and later: `HubNativeInboxBatchTests` verifies an invalid inbox ID rejects the commit and rolls back other valid inbox changes.
- D: `HubNativeCompactReceiptTests` verifies both v1 full-JSON and v2 SHA-256 receipts can acknowledge exact replay and reject conflicting replay in either flag mode.
- D's retry receipt hash uses exactly the same canonical serialization as the v1 structural equality check (including normalization of optional obsolete dispatches) and does not remove authoritative workflow snapshots or journal entries.
- Changes in the child PRs must be reconciled with their parent before merging; do not merge D directly against master while parents are draft.

## Production roll-out

Roll out A, B, C, D sequentially only after each respective code+benchmark CI passes and repeated end-to-end workload tests show improved workflow throughput and no p95/p99 regression. Enable one flag at a time with mixed Hub versions, observe replay correctness, native event wake-ups and PostgreSQL WAL/CPU/lock waits, and retain ability to disable each independently. Keep all flags OFF until verified.

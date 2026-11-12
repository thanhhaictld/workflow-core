# Four-shard ordered capability merge experiment

Status: **opt-in experiment; one local A/B pair, not a production recommendation**.
This is a SQL candidate-selection experiment, not the proposed in-memory cache.

With unconstrained batching and split claims enabled, setting
`WFC_HUB_ORDERED_CAPABILITY_MERGE=true` enables a ready-task scan per distinct
capability for a single Hub, four matching shards, and at most eight distinct
capabilities. Each scan uses capability and `partition_id % 4` equality and
sequence order; its candidates are merged with each other and the existing
expired-task branch before leasing. Migration 028 adds the corresponding ready
index. All other requests retain the existing split or legacy path. The flag
defaults off. Compose and `scripts/dev.ps1 db-schema` install migration 028 only
with `WFC_HUB_INSTALL_SHARD4_READY_INDEX=true`. Install it before enabling the
merge query. The installation flag defaults off and does not remove existing
indexes. An installed index has write and storage overhead even with the query
disabled, and index creation can block writes. For a large deployment, schedule
an operational index build; do not drop existing indexes as a routine rollback.
Direct invocation of the SQL file still installs the index. Other migration
runners must choose whether to include this optional file explicitly.

Each capability scan may lock up to the requested batch size; the expired branch
can lock another batch. Unselected rows remain locked until the transaction
commits and may temporarily be skipped by competitors. The resulting maximum is
`(distinct capabilities + 1) * batch size` for this path. Task eligibility,
attempts, journal, task lease fencing and constrained-task admission remain
in PostgreSQL. A merge is ordered across the candidates available to that
transaction; `SKIP LOCKED` is not a global FIFO guarantee during concurrency.

## Local results, 2026-11-12

An isolated Compose project used two Hubs, four matching shards, eight processor
containers, 100 k6 virtual users, unordered policy workflows, split batch claims
and the SQL metrics sampler disabled. Both runs had migration 028's index. The
baseline ran first on a fresh database; the enabled run followed it on the same
database with 4,000 additional completed workflows. Thus data age, cache state,
time and background activity differ. These are single runs, not repeatable
estimates or an index-on/index-off comparison.

| Measurement | Merge off | Merge on |
| --- | ---: | ---: |
| Completed / failed | 4,000 / 0 | 4,000 / 0 |
| Server completion time | 126.713 s | 117.622 s |
| Server workflows/s | 31.57 | 34.01 |
| Workflow p95 / p99 | 117.934 / 118.144 s | 110.753 / 111.096 s |
| PostgreSQL CPU, mean of sampled 30 s rates | 2.58 cores | 2.45 cores |
| Claim SQL calls | 13,054 | 13,340 |
| Claim SQL cumulative execution time | 126.59 s | 42.97 s |
| Claim SQL shared-buffer hits | 46.27 million | 14.35 million |

Statement counters were sampled after each run and include nearby background
activity; SQL elapsed times overlap and do not represent CPU time. CPU values
are sampled process-wide, and the prior cohort and shared host affect the second
run. The durable audits recorded 4,000 activity attempts and completions per
cohort, zero extra attempts, and 16,000 completed tasks in each.

A separate 85-second busy-owner fault scenario killed Hub A with the flag on.
The audit reported all 120 workflows completed, zero extra activity attempts,
and Hub B observed taking over two affected shards about 9.21 seconds after
the fault. This is one scenario, not a 4,000-workflow fault injection or a
recovery SLO.

PostgreSQL integration tests exercised mixed capabilities, duplicate advertised
capabilities, ready/expired merge, locked-head skipping, competing claims,
workflow pins and native status, constrained FIFO admission, and fallback to
other shard counts. The local Release build and 28 focused PostgreSQL tests
passed. The test database was configured explicitly and had migration 028.

Before any wider use, repeat randomized A/B trials with equivalent data and
resource conditions; compare the index's write overhead separately. Include
large mixed-capability and constrained backlogs, varied shard counts, sparse
work, expired leases and many competing workers. Measure claim and completion
latency, oldest eligible task age, extra locks and skipped candidates, memory,
database CPU and total SQL work. Test both owners, idle and busy failure modes,
and uncertain delivery. Roll back query selection with the flag; retaining the
index avoids destructive migration rollback but retains its write cost.

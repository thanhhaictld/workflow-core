# Native completion and wakeup query optimization — 15 November 2026

The completion query has a reproduced plan instability and is fixed in source.
The wakeup insert already probes the workflow primary key; its observed cost
does not justify a query rewrite or another index. Nothing was deployed and no
live data, statistics, indexes, settings or services were changed.

## Live evidence

Read-only inspection of
`workflow-hub-observability-ad84b937db5895e0-postgres-1`, database `workflow`,
PostgreSQL 17.11, starting at 11:54 UTC; the retained statistics snapshot is
11:57:12 UTC. Statement statistics reset at
10:24:36 UTC. These are cumulative measurements across multiple cohorts,
not one benchmark and not CPU attribution.

| Statement | Calls / rows | Total execution | Mean | Max | Buffer accesses/call | WAL bytes/call |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Fenced completion, `702976769898827252` | 128,000 / 128,000 | 57.40 s | 0.448 ms | 21.67 ms | 96.01 | 2,816 |
| Native wakeup, `325984884542718125` | 96,000 / 96,000 | 36.09 s | 0.376 ms | 48.74 ms | 46.98 | 2,434 |

All observed wakeup calls inserted a row. An extra `NOT EXISTS` precheck would
add a read without eliminating observed inserts. Preserve the runnable-status
check and `ON CONFLICT DO NOTHING`: the unique active-native-task index is part
of durable wakeup deduplication.

The task table has 16 valid indexes, about 464k estimated live rows, and a
770 MB heap. Task statistics show 288k updates and zero HOT updates in this
window. State changes affect many partial-index predicates, so reducing
fillfactor alone cannot make these completions HOT. `ix_hub_task_operations`
is 79 MB and had two scans; a short usage window is insufficient to establish
that monitor/compatibility coverage permits removal. No current blocker was
seen in the idle activity snapshot; workload-time lock attribution is unknown.

Native workflow task results average 3,286 text bytes versus 953 for activity
results in the latest 16k tasks. The workflow result contains the original
commit, including its snapshot, for exact replay comparison. Removing that
copy would change replay/storage behavior and is outside this fix.

## Completion plan defect and fix

Both custom and generic live cost-only plans chose `ix_hub_task_expired`
**without an index condition**, filtering by ID, owner, epoch and lease after
the scan. Estimated cost was approximately 6 versus a primary-key probe's
higher estimate. A small/empty leased partial index can appear cheap after
ANALYZE; new backlog can then make this plan expensive before statistics catch
up. The captured live plan establishes exposure; it does not prove how much
of historical execution time came from this plan during prior bursts.

`HubNativeWorkflowStore.CompleteTaskSql` now uses:

```sql
UPDATE wfc_scale.hub_task
SET state=2, owner=NULL, lease_until=NULL,
    result_json=@result::jsonb, updated_at=now()
WHERE id=@task AND owner=@owner AND epoch=@epoch
  AND CASE state WHEN 1 THEN lease_until>clock_timestamp() ELSE false END;
```

The CASE retains the exact eligibility test while preventing matching the
broad leased partial-index predicate. Both native workflow acknowledgement
and native activity completion share the query. Live cost-only EXPLAIN of
this SQL uses `hub_task_pkey` with `Index Cond: id=...` in both plan modes.
Transactions, row-lock order, timestamps, history, inbox, next-task insertion
and replay checks remain in their existing paths. No migration is needed.

The isolated reproduction cloned the live schema without user data, seeded
12,160 synthetic tasks, analyzed an empty leased index, then made all tasks
leased without refreshing statistics. Five warmed samples per variant/mode
ran in alternating order against the same fixture with transaction rollback.

| Statistics / mode | Original median | Fixed median | Rows visited, original → fixed | Shared buffers, original → fixed |
| --- | ---: | ---: | ---: | ---: |
| Stale / custom | 1.619 ms | 0.147 ms | 12,160 → 1 | 410 → 18 |
| Stale / generic | 1.822 ms | 0.149 ms | 12,160 → 1 | 410 → 18 |
| Fresh / custom | 0.162 ms | 0.160 ms | 1 → 1 | 18 → 18 |
| Fresh / generic | 0.170 ms | 0.144 ms | 1 → 1 | 18 → 18 |

This proves bounded probes in the exercised fixtures and a large improvement
under the reproduced bad plan. It is not an end-to-end throughput or CPU claim.
Rollback samples can have MVCC/cache drift; both arms share the same fixture.

## Wakeup/write attribution and next priorities

A separate synthetic matrix used the exact schema, 12k completed-history rows,
160 leased targets, five repetitions per arm with alternating arm order, and
fresh fixture clones. Each target was completed and followed by a native
wakeup insertion. All 3,200 pairs completed/inserted one row each. Medians of
per-repetition means were:

| Arm | Completion ms | Wakeup ms | Completion WAL bytes | Wakeup WAL bytes |
| --- | ---: | ---: | ---: | ---: |
| Baseline | 0.104 | 0.087 | 987 | 979 |
| Without operations index | 0.094 | 0.080 | 882 | 847 |
| Without notification trigger | 0.078 | 0.070 | 988 | 979 |
| LZ4 result compression | 0.102 | 0.096 | 1,035 | 979 |

Keep the operations-index change as a candidate for a larger controlled
experiment: wakeup WAL fell about 13%, but one repetition was slower than
baseline, and monitor/recovery/query-plan qualification remains necessary.
No index was removed from the live database or repository migrations.

The baseline notification trigger accounted for approximately 0.024 ms per
completion and 0.017 ms per wakeup in this fixture. Completion's release hint
can unblock constrained work; wakeup's ready hint drives discovery. Disabling
notifications is not a valid optimization. The existing outbox adds writes and
requires a separate concurrency/recovery comparison; this experiment does not
qualify it. LZ4 did not demonstrate a useful improvement here.

These single-session synthetic results do not explain the full live average:
history size, JSON shape, cache pressure, concurrent writers, commit waits and
TOAST behavior differ. SQL EXPLAIN execution times exclude COMMIT latency.
The first deployment comparison should isolate the completion SQL fix with
the same topology and flags, then collect statement deltas, task throughput,
CPU/task, completion p95/p99, and query plans during an active backlog. New
SQL receives a new query ID; find it by text rather than tracking only the old
ID. Investigate write/index costs separately after that comparison.

## Validation and evidence

- .NET SDK 10 Release scaling solution build passed.
- 47 native-runtime tests passed, zero failed/skipped, with `WFC_TEST_PG`
  pointing to a fresh, separately created PostgreSQL 17 database containing
  the live schema. This includes four new custom/generic × stale/fresh plan
  cases, each checking successful completion and rejection for wrong owner,
  epoch, expired lease, ready/completed/dead state and missing ID. Existing
  native tests cover replay, cancellation, inbox/timer races and completion.
- The isolated SQL matrix verified 40 warmed before/after plans. All 20 fixed
  plans visited one row via the primary key. `git diff --check` passed.
- The full PostgreSQL suite and a live 4k workload comparison were not run.
  Throughput qualification requires a controlled benchmark after rebuilding
  the Hub images; the running Hubs retain their original binaries.
- All containers created for this investigation were stopped with synthetic
  data retained. The existing efficiency experiment and observability stack
  were left running.

The [collector and reproducer](../../benchmarks/hub-completion-write-cost/README.md)
includes safety boundaries and commands. Compact measurements and plan
summaries are retained in its `evidence/` directory. Local ignored raw folders
retain full plan JSON and schema dumps. PostgreSQL documents
[statement elapsed-time counters](https://www.postgresql.org/docs/17/pgstatstatements.html)
and [EXPLAIN trigger/buffer accounting](https://www.postgresql.org/docs/17/using-explain.html).

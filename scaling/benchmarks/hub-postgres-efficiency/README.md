# Hub PostgreSQL efficiency check (2026-11-18)

This run is a bounded SQL investigation against the isolated local PostgreSQL
container `wfc-luna-admission-correctness-20261118`; it is not the 16-processor
application benchmark. The dedicated scratch database
`wfc_hub_efficiency_20261118` is retained for review. No shared services were
restarted, and no statistics were reset. Integration fixtures removed only
their own successful scratch databases; no pre-existing database was reset or
cleaned. Two failed fixture databases remain retained for diagnosis.

The observed server was PostgreSQL 17.12. Current index definitions and table
statistics are included in `candidate-plans.txt`. The reproducible setup for a
fresh migrated database is `seed-workload.sql`, followed by
`candidate-plans.sql`; `stale-expiry-plan.sql` is a one-time follow-up that
adds the second expiry wave without refreshing statistics.

## Dispatch phases

The native task now receives its pointer ID, dispatch key, native step and UTC
due time in the shared `InsertActivityAsync` statement. Remote workflow dispatch
continues to pass its workflow step and uses the transaction's `now()` default.
The first patch removes the follow-up `UPDATE`, so the task trigger evaluates
the final due time. The second patch loads existing keys for the locked workflow
once, reuses that set for both policy preflight and dispatch, and marks already
canonically locked native resources for the shared insert helper to skip its
second lock pass. Remote callers still acquire resource locks in the shared
helper. Preflight remains before inbox, subscription, snapshot and cancellation
writes. A duplicate in the request itself is still sent through to the unique
index, preserving its durable conflict behavior.

The saved November 17 paired result recorded 4,000 calls each for native task
insert (2.132 s, 94,115 shared hits) and follow-up metadata update (1.505 s,
133,626 hits). That is useful prioritization evidence, not validation of this
candidate.

An isolated SQL-shape microbenchmark ran on PostgreSQL 17.12, container
`16ace1028ae95ee970916f7d2a517f7e5f244ef95709cdea7bb8d7ae33e01821`, image
`sha256:0d938e975177bef9f1f61f3f9dbcf2e0aa0a139568f66d3415ed8892eb97f6ac`.
The SQL files in this directory reproduce the old default-due INSERT plus
metadata UPDATE and the final-value single INSERT. Each got a 100-transaction
warm-up and three measured 2,000-transaction single-client runs, alternating
order across the first two repetitions. The old pair averaged 1.901 ms and
526 transactions/s; the single INSERT averaged 0.986 ms and 1,017
transactions/s. All measured runs had zero failed transactions. The separately
bracketed WAL LSN deltas were 14,691,424 and 8,898,688 bytes for 2,000 calls,
but LSN is cluster-wide and includes background WAL, so this is not a per-task
WAL result. `dispatch-sql-plans.txt` shows one warm EXPLAIN ANALYZE comparison:
the old insert/update reported 1,007 + 1,053 WAL bytes and 351 + 43 shared
hits, while the final-value insert reported 1,111 WAL bytes and 30 shared
hits. Its trigger time fell from about 0.96 ms on the immediately-ready old
insert to 0.02 ms on the future-due insert. These one-plan measurements are
diagnostic, not an application-level buffer/CPU measure. The isolated instance
did not preload `pg_stat_statements` and had I/O timing off, so no interval
statement-buffer ranking or process CPU was available. These measurements omit
Npgsql/Hub transaction work, policy lookup, resource locking, journaling,
attempts and workflow completion. They support the expected reduction in SQL
round trips and SQL-shape work, but do not validate application throughput,
CPU/task, durable workload WAL or completion-tail latency. The 16-processor 4K
application run was not started because the active benchmark stacks belong to
other work.

The pgbench invocation was
`docker exec -e PGPASSWORD=<redacted> wfc-luna-admission-correctness-20261118 pgbench -h 127.0.0.1 -U workflow -d wfc_hub_efficiency_20261118 -n -M simple -c 1 -j 1 -t <100-or-2000> -f /tmp/dispatch-<before-or-after>.sql`.
The `-t 100` runs were warm-ups; `-t 2000` runs supplied the reported samples.

## Expiry discovery evidence

The saved November 17 pair reported the expired candidate statement 944 times,
always empty, with 1,375,560 shared hits. It is historical. The November 18
recorded image builds differ from this checkout, so neither capture is treated
as a current plan. Current SQL text comes from
`HubCandidateReader.BuildPageSql`; plans and setup are in `candidate-plans.sql`
and `candidate-plans.txt`. The database contained 155,000 synthetic task rows:
50,000 future-ready, 50,000 due-ready, 50,000 nonexpired leased and 5,000
expired leased. One in twenty rows had a resource association. Statistics were
analyzed after loading; the table's autovacuum was disabled for controlled
repeatability. The captured run used bucket 0, 128 rows and sequence high-water
255,000; an aborted seed attempt consumed 100,000 identity values before the
successful load. The reproducible query script captures the current maximum
dynamically. Plans use the same bounded resource aggregation as the application
query. The sparse case
uses an empty Hub; the nonexpired expiry case uses a cutoff two minutes behind
database time; the expired case uses database time. `EXPLAIN ANALYZE, BUFFERS`
reports actual rows, loops, and shared/temp buffers per node in the saved JSON.

The analyzed plans used the existing ready bucket indexes for the ready page
(128 page rows; the index node visited 781 rows before ordering/limit) and the
expired bucket/deadline index for expiry (78 rows, one index loop). The empty
expiry page returned zero rows with three root shared hits when the lease cutoff
was passed as a stable timestamp, as it is by the application. The empty ready
page used six root shared hits. The active ready page used 1,659 root shared
hits, including bounded resource enrichment. A one-workflow metadata hydration
lookup returned one row with one shared hit in `metadata-hydration-plan.txt`;
the small synthetic workflow table selected a sequential scan. The stale-stat
run inserted another 5,000 expired tasks without ANALYZE: estimated table rows
remained 155,000 versus 160,000 actual, and the expiry page plan estimated two
rows while returning 128. See `stale-expiry-plan.txt`.

`stream-sweep.txt` executes one full enriched page probe for each of 64 buckets
per stream. Sparse ready returned 0 rows on 64 empty pages. Active due-ready
returned 8,192 rows and 409 resource keys on 64 nonempty pages. With leases made
nonexpired, expiry returned 0 rows on 64 empty pages. The expired workload
returned 5,000 rows and 250 resource keys on 64 nonempty pages. The active
due-ready stream has 50,000 rows total, which would require 448 pages at size
128 to drain fully. These isolated SQL page counts do not establish live
registry invocation frequency. This workload does not establish live candidate
activation frequency, metadata hydration frequency, process CPU, or reclaim
lag. The current plans and saved rows do not justify an expiry index or
registry behavior change. Retain the existing expiry scheduling until an
isolated live-runtime capture includes calls, empty pages, metadata lookups,
plan buffers and reclaim lag.

The source suggests a frequency mechanism worth measuring: ready hints set the
registry dirty and request a head pass, then horizon capture clears the current
stream list and rebuilds ready and expiry cursors. This can repeat the expiry
walk during ready activity, but the synthetic plan cannot establish how often
that occurs in a live owner. The current evidence therefore supports a
frequency hypothesis plus bounded per-page enrichment cost; it does not
identify a current runtime dominant cause.

## Verification

- `dotnet build scaling/WorkflowCore.Scaling.slnx -c Release -v:q` — exit 0.
- `make obs-ps` — exit 0; `make obs-config` — exit 0. Both were used only to
  inventory the current stack and resolved configuration.
- Focused delayed-dispatch PostgreSQL test — exit 0; verifies persisted fields,
  immediate versus future ready notification, no pre-due claim, candidate
  discovery after due time, and claim after due time.
- The initial native/remote focused test attempt against the supplied base
  database failed three cases because it lacked `wfc_scale.hub_definition`;
  the schema prerequisite was corrected by using the fully migrated isolated
  test database below.
- `$env:WFC_TEST_PG='Host=127.0.0.1;Port=55564;Database=wfc_hub_efficiency_20261118;Username=workflow;Password=<redacted>'; dotnet test scaling/tests/WorkflowCore.Scaling.PostgreSQL.Tests/WorkflowCore.Scaling.PostgreSQL.Tests.csproj -c Release -v:q` — exit 0; 359 passed, 0 failed, 0 skipped. This run covers native routing/replay, activity completion/history, remote dispatch, and existing constrained/FIFO compatibility. The two earlier failed test-created compatibility databases remain retained by the test fixture for diagnosis.
- `ups-16` application benchmark was not run because the active benchmark stacks belong to other work. Application throughput, CPU/task, WAL, completion-tail and expiry reclaim lag remain unverified.

## Independent follow-up stages

- **Scheduler:** split timer firing and inbox reconciliation into independent
  bounded loops with separate same-kind overlap limits. Measure timer lag under
  inbox backlog and distinguish event versus exhausted-activity query costs;
  consider sharding only if broad reads dominate after decoupling and ownership
  failover is qualified.
- **Constrained admission:** under canonical resource locks, fetch capacity/FIFO
  once, count live leases only to capacity, and use early-exit predecessor
  existence checks only for FIFO resources. Preserve lock order and time/expiry
  semantics; benchmark hot shared quota and strict FIFO with growing retained
  associations before index changes.
- **Monitoring:** preserve active-eligible task counts, global distinct counts,
  idle Hub zero rows and sample contracts. If duplicate sampling remains
  material, serialize each minute with a transaction-scoped claim and durable
  success check; test idle/active retained history and concurrent samplers.

None of these stages was changed or bundled with dispatch or expiry.

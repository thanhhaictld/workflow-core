# Snapshot contract reuse: throughput investigation

Status: opt-in experiment, 12 November 2026. The matching candidate cache is
**not implemented**. This improvement reuses serialization type metadata in
remote workers; it stores no tasks, candidates, workflow values or leases.

## Implementation and rollout

Set `WFC_HUB_REUSE_SNAPSHOT_CONTRACTS=true` on remote worker processes to reuse
one private Json.NET settings/resolver set per `HubSnapshotCodec` and catalog.
The default is false. Observability Compose forwards the flag to `processor`.
Other application hosts can set the same process environment variable. Restart
workers to change it. No Hub SQL flag or migration is required for this feature.

`JsonConvert` still creates a serializer for each operation. The resolver caches
contracts by CLR type; it does not cache deserialized instances or task state.
The boxed-value format, registered-assembly binder, pointer snapshots, REST
contracts, workflow initialization commit and all durable admission logic remain
the same. The catalog is constructed at startup; different hosts/catalogs get
different caches. Application converters attached through attributes must support
concurrent reuse when opting in. The repository's snapshot converters carry no
per-operation mutable state. Memory retains metadata for encountered types for
the life of the catalog.

This addresses an observed allocation/CPU amplification: the previous `Settings`
property constructs a new `DefaultContractResolver` subclass for every encode and
decode, repeatedly discarding its reflection metadata. This follows Json.NET's
[documented resolver reuse guidance](https://www.newtonsoft.com/json/help/html/Performance.htm).
Roll back by setting the flag false and restarting workers; persisted data is
compatible in both directions.

## Controlled workload and evidence

Evidence is under `artifacts/throughput-audit/` in this worktree. The retained
local scripts are `setup.ps1`, `rebuild.ps1`, `repeats.ps1`, `more.ps1`,
`trial.py`, `compare.py`, `audit.py`, and `plans.py`. They identify the isolated
project explicitly. Reruns must choose new trial/database names and unused ports;
the scripts retain previous databases, containers and volumes rather than reset
them. `config-*.json` records effective Compose configuration per trial,
`source.json` records source state, and `resources.json` / `docker-host.json`
record container resource limits and Docker host capacity.

The primary comparison used fresh, separately named databases in the isolated
`throughput-audit-20261112` Compose project. Each trial restarted the two Hubs
and eight processors with the same images, four matching shards, 8 workflow and
8 activity slots per processor, SSE task push, event-gated demand admission,
12 connections per Hub, admission limit 3, split/batch/ordered merge enabled,
index 028 present, and SQL metric sampler disabled. PostgreSQL 17 ran on the
same Docker host (16 CPUs, about 31 GiB) without container CPU quotas. The user's
existing observability project was left running and was not reconfigured.

Each cohort is exactly 4,000 `policy-unordered` workflows submitted with 100 k6
VUs. Each workflow has one simulated 75 ms routed activity, three native workflow
tasks and one activity task. The definition uses eight durable task partitions,
mapped onto four matching shards. This is the supplied recent benchmark workload;
these results do not generalize to arbitrary application workflows. The fixed
trial order was reuse on, off, off, on (ABBA). No build/test or fault experiment
ran concurrently with these trials. The earlier pilot included live plan probes
and is excluded from the comparison.

| Trial | Reuse | Completion seconds | Workflows/s | Workflow p95 seconds | Processor CPU seconds | PostgreSQL CPU seconds |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `codec_on_1` | on | 82.721 | 48.36 | 76.690 | 155.3 | 346.4 |
| `codec_off_1` | off | 126.542 | 31.61 | 117.595 | 724.3 | 351.3 |
| `codec_off_2` | off | 127.701 | 31.32 | 116.361 | 731.3 | 349.3 |
| `codec_on_2` | on | 93.772 | 42.66 | 86.206 | 158.4 | 333.1 |

The two enabled trials show substantial variation, but both improve completion
time and worker CPU. Pooled throughput across the two equal-size cohorts is
45.33 versus 31.47 workflows/s, about 44% higher. Worker CPU averages 156.9
versus 727.8 CPU-seconds, about 78% lower. SQL execution-time sums remain in
roughly the same range; the optimization reduces worker cost rather than durable
database work. Sampled processor allocation counters fell from approximately
10.4–12.1 GB to 1.46–1.47 GB. Metric exports omit portions of startup/tail and
can include stale series from prior restarted instances, so these allocation
figures are supporting observations, not exact per-cohort allocation totals.
The final analysis computes deltas within each unique process series, rather
than subtracting an aggregate that can lose old process series at collector expiry.

Completion time comes from first persisted creation to last persisted completion.
CPU uses cgroup cumulative `usage_usec` deltas, including the short sampler and
snapshot tail. These CPU windows exceed server completion windows; SQL elapsed
time is cumulative concurrent execution, not CPU time. Statement deltas are
filtered to the current database; nearby scheduler/reconciliation and sampling
SQL are included. No statement statistics were reset. Raw samples record database
activity/waits and task backlogs at five-second intervals; they are point samples,
not wait-duration profiles. This is a local two-trial-per-mode experiment on a
shared host, not a confidence interval, linear-scaling claim or production SLO.

### Default split path, without index 028

A second fresh-database ABBA comparison held ordered merge **off** and index 028
**absent**, retaining split/unconstrained batching, pool 12 and the same workers
and workload. It ran later in the day, with metadata reuse off, on, on, off:

| Trial | Reuse | Completion seconds | Workflows/s | p95 seconds | Processor CPU seconds | PostgreSQL CPU seconds |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `split_off_1` | off | 131.602 | 30.39 | 120.308 | 738.4 | 412.3 |
| `split_on_3` | on | 79.827 | 50.11 | 70.242 | 158.3 | 352.0 |
| `split_on_4` | on | 95.319 | 41.96 | 87.343 | 160.8 | 358.5 |
| `split_off_2` | off | 142.764 | 28.02 | 132.357 | 749.7 | 423.5 |

Pooled throughput is 45.68 versus 29.16 workflows/s, about 57% higher. Mean
processor CPU is 159.6 versus 744.1 CPU-seconds, about 79% lower. The two enabled
runs again vary substantially; do not turn the best run into a throughput promise.
The worker change's benefit therefore does not require the experimental merge,
its extra task locks, or index 028. Keep the SQL experiment disabled by default;
the live plan improvement alone does not justify enabling it for this workload.

The final strict audit found one native workflow task at epoch/attempt 2 in
`split_on_4`, despite no injected failure. Its initial claim at 13:01:57.971804
UTC produced no persisted initialization; it was reclaimed at 13:02:56.763795
and initialized at epoch 2. All activity tasks in that cohort had one attempt,
and all 4,000 workflows had exactly one durable result and completion. This run
is retained in the timing comparison. `split-on-4-retry.txt`, the Tempo search
and four exported traces preserve the investigation. The trace search found
execution at epoch 2 and subsequent turns, not an epoch-1 execution, but absence
of an exported span cannot prove nondelivery. The precise cause remains unknown.
An uncertain delivery/commit followed by lease recovery is consistent with the
evidence; no causal link to metadata reuse is established. The initial strict
audit failure is retained as `durable-audit-initial-strict.txt`; the final audit
records retry counts separately from durable safety assertions. These normal
runs must not all be described as retry-free.

## Query-path audit

The pilot captured actual backlog `EXPLAIN (ANALYZE, BUFFERS)` plans for full
ready/expired candidate selection with row locks, inside transactions rolled back
after the read. `live-SplitBatchClaimSql.json` and
`live-OrderedCapabilityMergeSql.json` preserve both plans. These are live-data
probes, unlike the earlier synthetic plan report. The probe used one capability,
shard zero, a batch limit of eight and null version pins; it does not capture
every production parameter combination or the lease/history writes.

The split ready branch visited 953 eligible rows and sorted to select eight,
reporting 8,145 shared hits and 18.7 ms. The merge ready branch visited eight
through the capability/shard4/sequence index, reporting 98 shared hits and
1.128 ms. Expired work was empty in both probes. This supports keeping the merge
as a useful opt-in SQL experiment, but does not alone establish completion
throughput. The worker change targets the substantial cost that remains after
that query improvement.

The existing split and merge selection paths retain transactional attempts and
journals, execution epochs and constraints exclusion. Unselected locked rows in
the merge remain locked through commit, up to `(distinct capabilities + 1) *
batch size`. Existing constrained FIFO/permit admission is unchanged. A matching
owner generation is different from a worker execution epoch. The experimental
`matchingOwner` API remains unused by the gateway, and is not an end-to-end
matching-owner admission guarantee.

During review, owner admission's expiry predicate was found before `LockRows`.
A waiter could therefore pass it, block behind an unchanged row until expiry,
and then admit a stale owner. A materialized locking CTE now acquires the row
before the outer database-clock expiry check. A regression test waits for an
observed PostgreSQL blocking relationship, lets the lease expire without a row
update, then checks rejection. This lock orders admission against takeover;
it does not promise expiry-at-commit or replace task lease fencing.

## Index operations

Migration 028 adds write/storage cost even with the merge query disabled.
Compose and `scripts/dev.ps1 db-schema` now install it only with
`WFC_HUB_INSTALL_SHARD4_READY_INDEX=true` (default false). An existing index is
retained regardless of that flag. Install it before opting into ordered merge;
plan an operational build for large tables because the SQL uses ordinary index
creation. The flag is independent of `WFC_HUB_ORDERED_CAPABILITY_MERGE`.
The SQL file itself still creates the index when invoked directly; other
migration runners that enumerate every SQL file must explicitly exclude optional
028 when it is not requested. The installation flag controls the Compose/dev
launchers, not arbitrary SQL execution.

With metadata reuse on, the split query, pool 12, and otherwise equivalent
fresh-database ABBA trials (index present, absent, absent, present), installing
028 did not show a completion-throughput benefit:

| Trial | Index 028 | Completion seconds | Workflows/s | p95 seconds | PostgreSQL CPU seconds |
| --- | --- | ---: | ---: | ---: | ---: |
| `index_present_1` | present | 84.308 | 47.44 | 75.494 | 356.1 |
| `index_absent_1` | absent | 80.704 | 49.56 | 72.381 | 349.4 |
| `index_absent_2` | absent | 79.047 | 50.60 | 70.198 | 345.2 |
| `index_present_2` | present | 81.229 | 49.24 | 71.409 | 348.4 |

The optional index occupied about 2.03–2.05 MiB after each 4,000-workflow cohort.
Its use by the split planner varied (4,144 index scans in the first present trial,
zero in the second). Cluster WAL deltas were 182.50/182.24 MB with it and
179.40/205.57 MB without it; checkpoint/full-page-image differences make those
WAL totals unsuitable for attributing write cost to this index. The small
completion-time differences are not a precise write-overhead estimate. A separate
table-write probe is retained to isolate maintenance cost from queue selection.

The write probe (`index-write-probe/summary.json`) cloned the actual task table
and all its indexes into four new tables in an isolated database, varying only
028 in with/without/without/with order. It inserted the same 16,000 task payloads
and partitions, then updated every row ready → leased → complete, in one short
transaction per table. It copied no business triggers or foreign keys and ran
no matching/history/REST code; it is a write-amplification probe, not a runtime
benchmark. All reported write plans had zero WAL full-page images.

Insertion with 028 emitted 28,826,154 WAL bytes versus 26,086,356 without it,
an extra 2,739,798 bytes (10.5%) for the identical insert set. The three-phase
cycle emitted 63,529,658 versus 60,789,860 WAL bytes (4.5% extra); this excludes
index vacuum/cleanup and retry-to-ready costs. Timing varied and did not establish
a reliable write-latency penalty. This confirms a real maintenance cost without
using the noisy end-to-end WAL totals as a causal estimate.

Increasing the Hub pool to 24, while retaining admission limit 3, metadata reuse
and ordered merge, yielded 48.65/51.81 workflows/s (82.215/77.203 seconds) with
p95 72.530/68.003 seconds. These two later trials are not an interleaved pool
comparison and overlap the result range of split/index-absent trials. No pool
default or claim-admission limit was changed. The tested cgroups reported zero
CPU throttling; connection-pool sizing is not a substitute for reducing worker
CPU and request amplification.

## Validation and remaining limits

The Release scaling solution build passed on SDK 10 (zero errors, seven existing
warnings). The full PostgreSQL test suite passed 255/255 with reuse and ordered
merge enabled, and 255/255 in default mode on a separately provisioned database.
`WFC_TEST_PG` was explicitly configured by the test container. New tests cover
concurrent metadata reuse, byte-compatible snapshot round trips, catalog
allow-list isolation after warmup, and owner expiry while blocked on a row lock.
Existing coverage includes matching selection, locked heads, expired leases,
retry/fencing/history, native execution/transport, pins/status, and permits/FIFO
across partitions.

After preserving the original public codec constructor through a delegating
overload, the final source was rebuilt and all 255 tests passed again with reuse
on and merge off. This constructor-only compatibility change runs once at worker
startup; the benchmarked serialization hot path is unchanged. The final source
hashes are recorded in `source-final.json` alongside the original image/resource
identities and effective per-trial flags.

The final audit covers 60,740 workflows across 21 cohorts: 56,000 controlled
performance workflows, a 4,000-workflow pilot, 300 keyed-FIFO workflows and 440
fault-cohort workflows. All completed, all task states ended complete, and each
workflow has exactly one durable activity result and completion. Performance
cohorts have zero extra activity attempts and one extra workflow-task attempt
in `split_on_4`. Faults add one activity retry and ten workflow-task retries;
the audit records those instead of assuming retries are impossible. The 300
keyed workflows span all eight resource keys across multiple durable partitions,
with zero sequence inversions or overlapping attempt intervals.

Operational checks confirmed that Compose skips 028 with installation disabled,
creates it when enabled, and retains it when subsequently disabled. The
development script help command and `git diff --check` passed. Staged content's
hash remained unchanged. No commit or staging operation was performed.
The isolated project was brought down after verification, retaining all four
named data volumes. The original observability project's 19 containers remained
running; no reset, deployment or reconfiguration of that project was performed.

An initial default-mode repeat against the enabled suite's database passed
254/255: `GlobalExclusiveResource_SerializesAcrossBusinessHubs` reused the fixed
global `shared-database` / `database-one` resource while the previous invocation
had left older unfinished tasks. This is test-data interference under durable
FIFO semantics. The unmodified suite passed 255/255 in a new database. The failed
run's TRX/log and old database are retained; nothing was reset to make it pass.

Five separate 85-second fault windows ran with metadata reuse on, ordered merge
off, index 028 absent, pool 12 and eight processors. These are small recovery
cohorts, **not faults injected into the 4,000-workflow throughput trials**:

| Scenario | Completed / submitted | Extra activity attempts | Extra workflow-task attempts | Affected-shard takeover observation range |
| --- | ---: | ---: | ---: | ---: |
| Kill Hub A, busy | 120 / 120 | 0 | 3 | 8.42 s |
| Kill Hub B, busy | 120 / 120 | 1 | 6 | 11.38–13.28 s |
| Kill Hub A, idle | 40 / 40 | 0 | 0 | 11.25–14.79 s |
| Kill Hub B, idle | 40 / 40 | 0 | 0 | 5.64–13.33 s |
| Pause Hub A for 16 seconds, busy | 120 / 120 | 0 | 1 | 8.95–11.97 s |

`recovery-summary.json`, each scenario's timeline, and full cohort exports retain
owner generations, database clocks, task epochs, attempts, results and completion
events. All 440 workflows completed, with exactly one durable result and one
workflow completion each. Matching ownership recovery does not release or replace
worker execution leases; a paused Hub resumes into a newer matching generation.

The busy Hub B kill had one task close epoch 1 as `LeaseExpired` and complete at
epoch 2, about 45 seconds after its first claim. Its first claim was persisted
just before the kill request. This is consistent with a committed but undelivered
lease; retained evidence does not prove whether the first body ran. The case had
46-second completion tails, and the paused-owner case had a maximum completion
of about 50 seconds despite zero extra activity attempts. These observations
must not be reduced to the owner-takeover time or presented as exactly-once
external execution. They exercise at-least-once recovery and durable fencing,
not a recovery SLO or a zero-retry guarantee.

A separate 300-workflow keyed-FIFO cohort validates the unchanged constrained
path across matching partitions. The final durable audit records its key,
sequence-inversion and overlapping-attempt counts along with all performance
cohorts. No candidate-cache implementation or production-readiness claim is
part of this change.

The next throughput boundary is still durable protocol amplification: this
one-activity workload requires three separately committed native workflow turns,
an activity lease/result, runtime fetches, worker credits, and transactional
journals. Metadata reuse reduces worker reflection/allocation cost without
removing those guarantees. Further batching would need new evidence and a
separate correctness review; a faster candidate scan or larger pool alone has
not demonstrated a reliable end-to-end advantage here. Broad mixed-capability
backlogs, long-duration/large snapshots, custom converters, remote network
latency, real external side effects and high-volume timer failures remain outside
these performance trials.

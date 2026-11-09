# Ownership recovery validation

Status: required recovery correction implemented and validated; experimental
runtime. Phases A, B and E are complete within this branch. Phases C and D are
closed with the findings and conditional plans below. No production capacity,
recovery SLO, deployment or source-branch merge is claimed.

## Provenance and isolation

Prerequisite merge `dcdab7bdf0c4c6b9566463d652c06da687316a64` combines the starting
master with the authorized committed history through `fb3c79ea`. Diagnostic-only
commit `d6f041f5` exposes opt-in owner-route exceptions. Runtime correction
`e194c25e` is separate; `0f3e87d5` adds the uncertain-delivery test and a decisive
database-clock expiry fixture. No source worktree or Terra uncommitted changes
were imported. No other worktree, agent or terminal was created or controlled.

[Raw evidence](cadence-inspired-evidence/) includes the original build logs,
image/container identities, resolved lab configuration, source status and dirty
diffs, raw HTTP/stream/log/ownership timelines, SQL snapshots, statements, waits,
metrics, CPU counters and test outputs. `historical-evidence.zip` preserves 168
historical files with relative paths; `historical-manifest.json` lists SHA-256
and byte counts. Historical data and source checkout were never modified.

The first image builds used this tree immediately after prerequisite integration
and before source edits. Subsequent exact-source builds use `git archive` piped
to Docker without switching branches. `baseline-exact-source-build.json` pins
the prerequisite archive hash and image IDs; `source-modified-exact-source-build.json`
pins `e194c25e`. The source commit captured in a later container identity means
the inspection-time HEAD; the exact-build record identifies image source.
Diagnostics and runtime builds, including failed tooling attempts, are retained.

Only project resources `cadence-owner-20261109-*`, network
`cadence-owner-20261109` and volume `cadence-owner-20261109-pg` were manipulated.
Loopback ports: Hubs 25200/25201, LB 25210, PostgreSQL 25440, metrics 25289.
Database `workflow` holds the lab; separate `owner_tests`, `owner_linux_tests`
and `owner_linux_final` databases hold configured tests. Migrations 002–024 were
applied in order. No database or existing schema was reset. Other running stacks
share host CPU, so these are controlled functional comparisons on a shared host,
not isolated machine capacity tests. Docker and SDK identities accompany builds.

The evidence [index](cadence-inspired-evidence/README.md) gives commands,
archive extraction/verification, source hashes and the retained resource state.
Raw runtime records are compressed with a per-file SHA-256 manifest; failed
and excluded runs remain in that archive. No result was discarded to improve
the comparison. The source brief remains an untracked user-owned file.

## Cause and correction

The fresh baseline reproduced expired ownership with repeated 503/504 and
unfinished workflows. The diagnostic repeat captured
`TaskCanceledException -> TimeoutException: A connection could not be established
within the configured ConnectTimeout`, originating in `ForwardSseToAsync`.
The preferred-peer branch caught `HttpRequestException` only. Timeout reached
the outer 503 handler and bypassed local SQL election on every reconnect. A
stopped container's network can time out rather than return connection refused.
The original historical incident is consistent with this reproduced mechanism;
its absent continuous exception trace still prevents proving its exact cause.

The correction handles pre-header transport timeout and HTTP rejection, bounds
the header wait to three seconds, preserves accepted stream bodies, propagates
caller cancellation, and elects only through PostgreSQL's conditional UPSERT.
A live remote owner remains authoritative even if unreachable. Acquisition
stripes and short jittered negative preferred-peer hints limit redundant work;
no positive ownership cache, new release protocol, background election service,
broker, etcd or schema migration was added. SQL task admission, resources,
FIFO, workflow revisions and transactional completion/history are unchanged.

## Fault method and results

The lab has two Hubs, four matching shards, four compatible processors and
independent 8-slot workflow/activity lanes. Readiness and admission settings
match between image variants. Each fault continuously records DB UTC plus
monotonic elapsed time, owner ID/epoch/expiry, task counts and DB waits; both Hub,
worker and LB logs are retained. Independent zero-credit observer streams record
per-shard session epochs and keepalives without executing tasks. Sampling uses
a nominal one-second interval; Docker subprocess and shared-host scheduling
can stretch intervals. DB access is unavailable while PostgreSQL is paused.

Hard-kill runs retain the dead process through the full observation window.
Restart occurs only after `end-before-restart`. A precondition records that the
victim owns live shards. Idle runs submit 40 workflows after a 12-second wait;
busy runs submit 120 immediately before the kill. The 75 ms policy activity is
synthetic. A lost delivery can consume its independent 45-second execution lease
before retry; matching takeover never authorizes immediate reuse of that task.

The baseline idle-A run completed 23/40; diagnostic idle-B completed 22/40.
Baseline busy-A completed 65/120 and busy-B completed 62/120 by window end.
Expired dead-preferred shards were not acquired during the roughly 85-second
windows. Diagnostic B initially owned all four shards: the survivor acquired its
own preferred shards but stranded the two requiring failed-peer fallback. The
first idle-A run overlapped the initial configured suite, so its DB-cost numbers
are unsuitable for performance attribution; the separate repeats reproduce
the ownership failure without those tests.

Eight valid modified hard-kill runs cover either owner twice with idle and busy
lanes: idle A `3/4`, idle B `1/2`, busy A `1/2`, busy B `1/2`. Each idle cohort
completed 40/40 and each busy cohort 120/120 before restart. New owner epochs
were observed about 9.3–12.5 seconds after fault completion in these runs.
These are sampled local observations, not eight-second guarantees. The
[analysis](cadence-inspired-evidence/analysis.json) preserves each shard's last
observed expiry, first expired sample, first new owner and first new observer
session. Logs and durable attempt/event timestamps support more detailed
dispatch/completion correlation; missing samples are not invented timestamps.

Two early modified runs are explicitly excluded: `modified-idle-a-1` lacked
workers after an image-build race; `modified-idle-a-2` killed a Hub owning no
shards. Their raw records remain. The harness now verifies images before
replacement and validates victim ownership before injecting a fault.

In `modified-resume-a-1`, Hub A was paused for 16 seconds and resumed with its
old process memory. The survivor acquired shard 3 under a newer epoch; resumed
Hub A reacquired shard 1 under a newer epoch. Old session generations did not
regain authority. All 120 workflows completed with one durable result each.
`modified-database-1` paused PostgreSQL for 16 seconds: all four matching epochs
advanced after access returned, and all 120 workflows completed without
restarting either Hub or any worker. These runs have single repetitions and
do not establish an outage-duration SLO.

`analyze_owner_recovery.py` audits every completed workflow's persisted
`Completed=true` business snapshot, one `WorkflowCompleted` history event and
one durable `ActivityResultReceived`. It rejects duplicate logical completions
and records retries separately. Lease/attempt counts alone are not success.
This proves the tested durable outcomes, not exactly-once external effects.

## Tests and measurement interpretation

Windows .NET SDK 10.0.100 Release builds passed. The configured baseline full
suite ran 139 tests: 135 passed, four direct-runtime tests failed on immediate
queue claims. Retained output reports exact failures. The separately configured
Hub/runtime baseline suite passed 128/128. The modified Hub/runtime selection
passed 134/134; the focused ownership/SSE/session/credit selection passed 21/21.
Selections overlap and must not be summed as distinct tests.

A fresh Linux SDK 10 image, exact runtime commit `e194c25e` and a separate
configured database first ran the full suite: 145 passed, zero failed/skipped.
The final run binds the final test file read only into that image, rebuilds
tests, and uses another fresh database: **146 passed, zero failed/skipped**.
The Windows routing selection also passed **7/7**, including the new real-DB
after-claim/before-delivery case. Full outputs and TRX are retained. The final
schema application stopped at migration 014 on its first invocation; the original
stderr was not captured. The failure was recorded, 014 was rerun successfully,
and 014–024 completed in place with retry output retained. No schema or database
was dropped or reset.

The routing tests use real Kestrel and PostgreSQL for HTTP rejection, stalled
headers, live-owner non-steal, caller cancellation, concurrent acquisition,
topology mismatch, stale owner epoch rejection, preserved execution heartbeat,
idempotent durable activity result, and suppressed uncertain delivery.
Existing configured suites cover resource FIFO/capacity, multi-resource
admission, session replacement, global lane credits, rollback notifications,
missed-hint catch-up, native revisions/history and timer/event races.

Windows/Docker clock samples observed PostgreSQL ahead of the host's entire
request interval by approximately 0.7–1.0 seconds during one collection. A
near-expiry fixture consequently produced two intermediate routing-test
failures. The fixture now expires five seconds in the past with DB time, matching
existing ownership test practice. The Linux full-suite pass and clock samples
are recorded separately; they do not prove the cause of every original Windows
direct-runtime failure. Clock synchronization remains an operational concern;
SQL fencing uses database time and local routing expiry checks are advisory.

New metrics use no task, worker, tenant, resource or credit ID labels. Ownership
change `outcome` and resolution failure `reason` are bounded. Session-to-first
successful flush includes idle wait and excludes never-dispatched sessions; it
must not be presented as exact eligibility or failover latency. Reconnects count
stream attempts after the initial attempt. Stale exporter samples from a dead
instance can outlive the instance; do not sum them as exact live session counts.
Ownership counters record local tracking observations; concurrent resolution can
repeat an acquisition observation. SQL rows and epoch-bearing logs identify
unique authoritative generations.

## Normal path, overhead and missed hints

Normal-path trials use the same two Hubs, four shards, four processors, lane
limits, 75 ms synthetic activity, admission flags and retained lab database.
Each cohort has 200 workflows, preceded by a separate 40-workflow warmup.
Stages alternate baseline A → modified A → modified B → baseline B; workload
order reverses between stages. Baseline is the exact archive of `dcdab7bd`;
modified is the exact archive of `e194c25e`. Owner diagnostics are disabled in
these trials. Runs reuse durable data without resetting the database, so later
stages have larger tables. Shared-host load, short windows and second-level
metrics export/scraping prevent a precise speedup or capacity claim.

| Stage/workload | DB-span workflows/s | Workflow p95 seconds | CPU seconds¹ | SQL calls/workflow² | Activity created→lease p95 seconds³ |
| --- | ---: | ---: | ---: | ---: | ---: |
| Baseline A, unordered | 14.71 | 12.90 | 113.27 | 199.28 | 5.82 |
| Baseline A, 8 keys | 14.99 | 12.50 | 94.54 | 275.81 | 3.89 |
| Modified A, unordered | 21.63 | 8.56 | 76.97 | 184.28 | 0.87 |
| Modified A, 8 keys | 16.05 | 11.83 | 112.91 | 221.80 | 2.29 |
| Modified B, unordered | 14.29 | 13.48 | 106.81 | 197.55 | 1.17 |
| Modified B, 8 keys | 15.54 | 12.25 | 88.47 | 236.82 | 2.20 |
| Baseline B, unordered | 16.55 | 11.25 | 71.44 | 182.46 | 5.46 |
| Baseline B, 8 keys | 10.89 | 17.49 | 152.71 | 304.32 | 9.19 |

All eight cohorts completed 200/200, with zero failed workflows, extra activity
attempts or duplicate durable results/completions. Every keyed cohort used eight
keys spanning multiple workflow partitions, with zero sequence inversions and
zero overlapping attempt intervals on each capacity-one resource. This is a
normal-path correctness regression check; variations cannot establish that the
routing correction improves scheduler throughput.

¹ CPU sums PostgreSQL, both Hubs, four workers and LB cgroup deltas, including
the two-second export tail and sampling cost. Collector and driver CPU are not
included: the collector's scratch image did not expose `cat` for this probe.
Neither elapsed seconds nor CPU seconds represent an isolated service cost.

² `pg_stat_statements` snapshots include all databases/users and background
traffic. The analyzer aggregates repeated query IDs before subtracting counters;
all final windows have zero counter decreases. Earlier analysis overwrote rows
sharing a query ID and gave invalid negative deltas; that output is retained as
superseded, while `analysis.json` is recomputed from untouched raw snapshots.
These values are a lab-wide DB-work proxy, not task-specific SQL attribution.
Wait samples are mostly idle `ClientRead`, with occasional object/transaction
locks and WAL waits; all eight normal windows have zero sampling errors. These
are point samples rather than wait-duration histograms or proof of no contention.

³ Created-to-lease includes policy-blocked/FIFO wait; it is not exact eligibility
time. DB-span throughput divides completions by first-created→last-completed DB
timestamps. Workflow p95 comes from persisted per-workflow durations. Host
monotonic duration is retained separately; Windows/VM clocks differ, so do not
subtract a host timestamp from a database timestamp.

Candidate-query histogram means range 2.81–4.03 ms; total claim p95 bucket
ceilings range 25–250 ms across these cohorts. Histogram bounds are not exact
quantiles. Active SSE session samples stay at 32. Active inter-Hub HTTP connection
samples range 12–23 depending on stage and include SSE plus credit/control
traffic; they are not a pure relay count. Dispatch-loop and completion-HTTP
histograms, errors and database waits remain in the raw metrics. The existing
dispatch timer measures a matching callback, not network delivery. There is no
per-task claim-to-worker receipt distribution in this lab. Zero-credit observer
session timing, first successful flush, and durable first progress are different
boundaries; none is relabeled as exact end-to-end delivery latency. No measured
connection or relay saturation justifies a queue/deployment migration.

Instrumentation-off trials keep the same modified runtime/workload and sampling
driver, disable SDK setup with an empty OTLP endpoint plus `OTEL_SDK_DISABLED`,
and run a 40-workflow warmup then two unordered 200-workflow cohorts. They
complete 200/200 each, at 15.62/18.52 DB-span workflows/s, workflow p95
12.28/8.56 seconds and 88.44/70.27 CPU seconds. Instrumented unordered runs span
14.29–21.63 workflows/s and 76.97–106.81 CPU seconds. These overlapping ranges
do not resolve stable telemetry overhead, and switching off the entire SDK does
not isolate this patch's counters. Off-run collector samples are stale/empty
and must not be interpreted as live gauges. A dedicated-host, sustained,
interleaved experiment remains the gate for a numeric overhead budget.

Finally, `missed-notifications` disables `WFC_HUB_PG_NOTIFY_ENABLED` on both
Hubs, leaving durable recovery scans enabled. All 80 workflows complete with
one result and completion each, zero extra activity attempts, and eight keys
spanning partitions without FIFO inversions or capacity overlap. Host elapsed
time is 32.18 seconds and workflow p95 is 26.44 seconds. This demonstrates
recovery without LISTEN hints, not a scan-latency SLO. The independent configured
tests also exercise rollback notifications and missed-hint catch-up.

## Scaling gates and Terra integration

Phase C is closed without another matching fast path. The measured owner stall
is a routing exception defect. Historical candidate means do not prove avoidable
task rediscovery, and Terra owns the demonstrated generic-release-hint capability
routing change. A direct-task queue or eligible-head cache here would duplicate
that work without causal evidence. Existing committed broad release hints and
scan recovery are retained.

Phase D remains a conditional architecture decision, not implemented queues or
independent deployments. No connection/relay/CPU saturation limit has been
established. The [design](cadence-inspired-scaling-design.md) specifies routing
identity, version compatibility, nullable legacy defaults, dual readers, backlog
drain, rollback of persisted data and trigger measurements. It is migration-ready
planning, not a configuration switch pretending to migrate existing work.

Terra was inspected read only at committed HEAD `cfd4fa77` with uncommitted
handoff work. Shared files requiring a deliberate additive merge are
`HubSseTaskDistributor.cs`, `HubRuntimeWorkers.cs`, `HubTelemetry.cs`, and
`HubOpenTelemetryExtensions.cs`. Preserve this branch's first-flush/reconnect
instrumentation and Terra's gated diagnostics; preserve all existing fine
histogram views. This branch does not change the dispatcher, matcher, task SQL,
notification listener or resource key policy. Terra's eventual committed patch
must run with this correction against the combined fault/FIFO/multi-resource/
rollback/missed-hint/session/credit tests before joint validation is claimed.
No uncommitted changes were imported and no completion was assumed.

## Rollout, rollback and remaining gates

No deployment is requested or performed. For a later canary, preserve namespace
topology, configure mutually reachable trusted peer advertise URLs and unique
node identities, synchronize clocks, and retain SQL authority and execution
leases. Test a hard owner failure and an idle zero-credit stream before expanding.
Monitor bounded resolution/fallback/reconnect counters, owner epochs, first
durable progress, error rates and database cost. Enable owner exception logs only
for the investigation window; payloads and credentials are excluded.

Rollback requires only the previous Hub/worker binaries: drain or reconnect
streams, retain every task/attempt/history/lease row, and allow normal fencing
and expiry. No migration reversal or database reset. Reverting the timeout fix
also restores the reproduced failure path; account for that operational risk.
Terra's generic-hint flag has its own rollout/rollback and must remain separate.

Remaining external gates are Terra's committed output plus combined validation,
dedicated-host sustained capacity/overhead measurement, production identity/key
decisions owned by the application, and any later queue/deployment migration
trigger. None authorizes an architectural rewrite or production-readiness claim.

All nine running containers carrying the exact owned lab label were stopped
after evidence collection. The two test containers had already exited
successfully. Before/after inventory and stop receipts are archived. Containers,
images, network, PostgreSQL databases and the volume remain available for review;
no other project container was included in any stop command. No deployment,
publishing or source-branch merge occurred.

# Hub matching scale: implementation and evidence report

**Status: implemented experimentally; benchmark and combined failover qualification remain open.** Baseline HEAD is `308fef892d6c4d74a543e07c960ae4d7e624918e`. Changes are uncommitted in the child worktree. No deployment, merge, PR publication, database reset, or modification of other running containers was performed. `IMPLEMENTATION.md` contains the full plan, exact commands, chronological milestones and latest evidence audit.

The implementation adds lazy bounded demand/backpressure, demand-gated and hint-prioritized discovery, per-item cross-worker unconstrained batches, bounded concurrent admissions, batched authoritative lease reconciliation, migration 030 and a transactional notification outbox, metrics, tests and benchmark tools. `WFC_HUB_CANDIDATE_BACKPRESSURE`, `WFC_HUB_CANDIDATE_CROSS_WORKER_BATCH`, `WFC_HUB_CANDIDATE_CONCURRENT_ADMISSION` and `WFC_HUB_OUTBOX_ENABLED` default off in the application and direct Compose, while the Makefile golden lab preset explicitly enables the first three. Outbox writes also require the PostgreSQL session setting `wfc.hub_notification_outbox=on`; migration 030 preserves direct notifications by default. See `hub-remote-scheduler-implementation.md` for validated numeric defaults and migration/rollback ordering.

## Verified build and database tests

The previously reported **306 passed, 0 failed, 0 skipped** was a real PostgreSQL run, with `WFC_TEST_PG` set to fresh isolated `workflow_verify3`; it preceded the final 128-demand-per-pass bound. A later full run after that bound and an additional fairness test covered the final runtime code:

```powershell
dotnet build scaling/WorkflowCore.Scaling.slnx -c Release
$env:WFC_TEST_PG='Host=127.0.0.1;Port=53596;Database=workflow_verify4;Username=postgres;Password=isolated-test'
$env:WFC_HUB_CANDIDATE_BACKPRESSURE='true'
$env:WFC_HUB_CANDIDATE_CROSS_WORKER_BATCH='true'
$env:WFC_HUB_CANDIDATE_CONCURRENT_ADMISSION='true'
dotnet test scaling/tests/WorkflowCore.Scaling.PostgreSQL.Tests/WorkflowCore.Scaling.PostgreSQL.Tests.csproj -c Release --no-build
```

Build: **passed**, 0 errors, 4 existing analyzer warnings. Test: **307 passed, 0 failed, 0 skipped**, 1m07s, with PostgreSQL 17, EF and migrations 002–027/029/030 applied to a fresh isolated database. WFC_TEST_PG was set, so tests that otherwise return early for missing database configuration did not take that path; not every test is itself a database test. An independent query during the run observed 48,872 task rows and 534 workflow rows. Only documentation changed after that build/test. An earlier run on a repeatedly reused isolated database had 302 passed and 1 failed, plausibly due to 440 accumulated exhausted tasks ahead of a bounded sweep; both fresh full runs passed that test.

## Failure injection: covered and still unverified

- **Covered separately:** store-level pre-commit rollback and post-commit lost response for single and mixed-worker claims; per-item eligibility, stale epoch, resource constraint/FIFO, journal and attempt checks; registry retention of uncertain credits across cancellation and owner loss; and PostgreSQL owner-fencing/takeover and extended-lease retirement.
- **Covered separately:** push send failure with lease takeover and stale completion fencing; a four-delivery single-session mid-batch SSE failure that preserves all leases and delivery records while attempting remaining sends; bounded demand, cancellation, takeover and progress with 129 incompatible waiters; and two partition admissions held concurrently while a third waits.
- **Covered separately:** outbox transaction rollback and injected dispatcher pre-commit failure followed by successful dispatch. The full suite did not globally enable outbox writing; the dedicated outbox test enables it locally.
- **Unverified:** one combined end-to-end multi-session cross-worker batch that loses a commit response, crashes the Hub, undergoes takeover/reconciliation and exercises stale completion; real multi-partition admission stress with ownership loss during commit; outbox process-crash/connection-loss after commit and reconnect stress; and large-scale saturation/fairness/memory behavior. The 8192 waiting-demand cap is shared across candidate acquisitions and exceeding it raises an exception, so a saturated mixture of push and pull sessions requires further end-to-end testing.

## Performance and remaining work

No benchmark matrix, acceptance targets or performance gains have been measured. An unrelated observability/k6 workload with 16 processor containers was active on this 16-logical-processor host; a 4000-lane matrix would contend with and potentially disrupt it, so dedicated/idle resources and five measured repetitions per cell remain necessary. `hub-matching-scale-benchmark.md` documents the workload and counter protocol, and `scaling/benchmarks/hub-matching-scale-analyze.py` fails closed on incomplete evidence. Fairness, CPU cost of scanning waiting demand and overall memory under thousands of workers are still unmeasured, so the optimizations remain opt-in outside the explicitly configured golden lab preset.

The task-owned PostgreSQL container was stopped after the final audit; its data was retained, and its dynamic host port may change on restart. The internal SQL used by existing same-worker candidate batches was also refactored to carry per-item metadata, so its default-path query cost remains unmeasured even when the new cross-worker switch is off.


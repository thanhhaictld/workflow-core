# Hub selected-ID batch admission implementation — Luna

**Status (2026-11-15): applied in source; not deployed.** This is a narrow SQL
integration into `HubTaskStore.ClaimCandidateBatchAsync`. No migration, index,
runtime flag, pool change, or cadence change was made. The matching owner lock,
the under-lock owner check, task eligibility checks, admission gate, attempts,
workflow journal, commit ambiguity result, return mapping, and transaction
boundary remain in the store path.

## Query change

`HubTaskStore.CandidateBatchAdmissionSql` is the exact query used by the store
and by the plan regression. It materializes the selected JSON items, resolves
each selected task's observed epoch and sequence through its primary key,
orders by sequence, then uses a per-ID lateral primary-key probe with
`FOR UPDATE ... SKIP LOCKED` and `OFFSET 0` barriers before updating. The
measured barriers are intentional: a plain LATERAL form was flattenable and
returned to queue scanning. The original predicates remain present, including
partial-index-friendly literal states, attempts/max-attempts, due and expired
lease checks, hub/partition, each item's worker/capabilities/pins, constraints,
native Runnable filtering, and the full RETURNING projection. No `ctid` is
used.

The old benchmark `baseline.sql` and its archived hashes/results are unchanged.
Its extractor was tied to the original source literal, so it is historical
baseline evidence rather than a source-current extraction after this patch. The
production SQL's normalized whitespace matches the validated Luna
`prototype.sql`. The existing Luna experiment remains the plan/performance
comparison. Do not
interpret its timings as whole-store, CPU, workflow-throughput, or deployed
performance evidence.

## Verification performed

- `dotnet build scaling/WorkflowCore.Scaling.slnx -c Release` passed after the
  implementation and focused regression additions: 0 errors and 7 warnings
  (one obsolete upstream API warning, two upstream naming warnings, and four
  existing xUnit analyzer warnings).
- Focused tests ran with `WFC_TEST_PG` pointing to an isolated PostgreSQL 17
  database with ordered migrations 002–027, 029, and 030. Command:

  ```powershell
  dotnet test scaling/tests/WorkflowCore.Scaling.PostgreSQL.Tests/WorkflowCore.Scaling.PostgreSQL.Tests.csproj -c Release --filter 'FullyQualifiedName~HubCandidateBatchTests|FullyQualifiedName~HubCandidateBatchPlanTests'
  ```

  Result after final changes: **8 passed, 0 failed, 0 skipped**, including the
  single-credit acquisition regression. Real-store coverage includes mixed
  per-worker capabilities, workflow pins and null pins, observed-epoch mismatch,
  constrained-task exclusion, due/future/expired/live/exhausted eligibility,
  wrong task hub/partition, native Runnable/Suspended/Completed and null
  workflow cases, valid/stale owner and partition fences, precommit rollback,
  postcommit lost-response reconciliation, exact per-item attempt and journal
  rows, owner expiry at the final recheck, stale completion rejection, and
  overlapping reversed-order store claims.
- The plan regression executes the exact production SQL under
  `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` on 25,000 active rows, for batch
  sizes 1/8/16/32 in forced custom and generic prepared modes, using the
  deployed-matching index set (migrations 002–027, 029, 030; no 028). All 8
  plans updated exactly the selected batch count, retained `LockRows`, `Sort`,
  and `ModifyTable`, and used primary-key probes whose total loops were between
  two and three times the selected batch size. Every `hub_task` relation scan
  used `hub_task_pkey`. The test deletes only its uniquely named hub in `finally`
  so the 25,000-row backlog cannot leak into later suite discovery. Each mutating
  EXPLAIN runs in its own transaction and rolls back immediately.
- The competing-store regression pauses the first store after it has locked its
  selected rows, starts an opposite-order overlapping second store call, waits
  until PostgreSQL reports the second transaction blocked on the owner row, then
  releases the first. The owner row's `FOR UPDATE` intentionally serializes
  store batches for one matching partition, so this integration test verifies
  real competing-store serialization and unique final leases; it does not claim
  the second store transaction reaches task `SKIP LOCKED` concurrently. The
  isolated SQL experiment separately tested overlapping `SKIP LOCKED` claims.
- The initial isolated run of
  `HubCandidateAcquisitionRuntimeTests.ConfirmedMatchingCreditsAreRetiredWithoutWaitingForFortyFiveSecondLeases`
  failed deterministically at line 75 (`Expected: 0; Actual: 1`) in 1.86 seconds
  on a new PostgreSQL database. Diagnosis: the test called
  `HubTaskStore.CompleteAsync` directly, which updates durable task state but
  intentionally does not notify candidate transport accounting. The actual
  gateway completion routes call `HubCandidateAcquisition.ReleaseCompletedLease`
  after durable completion; `HubPartitionCandidateRegistry` retains granted
  credit until that acknowledgement. The test had `MaxTasks=1`, so its claim
  followed `AdmitAsync`'s single-candidate `ClaimCandidateAsync` branch and
  never invoked the optimized batch SQL. The test now calls the same
  post-completion acknowledgement hook as the gateway. This fixes the test
  wiring; no runtime matching semantics or timeout was changed, and the failure
  was not caused by the optimized SQL.
- The full PostgreSQL suite ran against a separate fresh isolated database with
  `WFC_TEST_PG` set to that database:

  ```powershell
  dotnet test scaling/tests/WorkflowCore.Scaling.PostgreSQL.Tests/WorkflowCore.Scaling.PostgreSQL.Tests.csproj -c Release
  ```

  Final result: **312 passed, 0 failed, 0 skipped** in 1 minute 10 seconds.
  The Release solution build passed with 7 warnings and 0 errors. `git diff
  --check` passed; the only output was Git's line-ending notice for a pre-existing
  modified review document.

## Isolated database

The earlier implementation-check database also remains stopped and retained:
`wfc-pg17-batch-impl-luna-20261115` (ID
`53264df0d566748c16b8eb327d5cb758548159449d8c588b2feea36310963f59`). The
focused diagnosis/qualification database ran in
`wfc-pg17-batch-final-luna-20261115` (ID
`a395e03c0932129fcd384ef486eea75ab470dce56682d76eed6c211af585995a`); the final
full-suite database ran in `wfc-pg17-batch-suitefinal-luna-20261115` (ID
`60f36a14e5afd663485d8e77987c40e7f5c8ed98c38b69deba01b92376121011`). Both
focused/full-suite databases used `postgres:17`, at most 2 CPUs and 2 GiB, had
no published host port, and applied ordered migrations 002–027, 029, and 030.
Each database was attached to its own private Docker network; tests ran in
temporary .NET 10 SDK containers on those networks. All three PostgreSQL
containers are stopped and retained as exited containers with their data; all
private networks were removed. No shared observability resource was touched.

## Remaining qualification limits

The workflow-step batch tests start with expired attempts and verify one
`LeaseExpired` closure, exactly one new attempt with the selected worker and
epoch, and one matching workflow claim journal row per admitted workflow task.
Injected precommit cancellation leaves task, old attempt, new attempt, and
journal unchanged. A simulated successful COMMIT followed by a lost response
returns `CommitOutcomeUnknown`; exact-epoch reconciliation succeeds, and stale
replay adds no attempt or journal rows. The owner-expiry regression waits past a
short valid lease while the transaction holds the owner row, observes the
takeover blocked on that row, then releases the admission; the final recheck
returns `OwnerLost`, its task/attempt/journal writes roll back, and the waiting
takeover claims under a new epoch. The old worker's completion is rejected.

One boundary remains: a same-partition store batch first serializes on the
owner-row `FOR UPDATE`, so the competing-store test proves actual contention and
unique leases but does not make two store transactions simultaneously reach
task-row `SKIP LOCKED`. The SQL-only overlapping claim experiment separately
verified `SKIP LOCKED`; all plan tests also retain `LockRows`. CPU cost,
whole-workflow throughput, end-to-end HA, and external side-effect behavior were
not measured. No deployment or production performance claim is made.

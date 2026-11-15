# Hub selected-ID claim scan: stale partial-index fix

**Result:** the batch claim query could make a broad ordered walk over `ix_hub_task_claim_sequence` when PostgreSQL estimated its partial active index as empty. The smallest tested correction changes only the state/attempt eligibility expression inside the existing locking probe to a `CASE`; custom and generic plans then use `hub_task_pkey` for each selected ID even with the same stale-zero estimate. This is a SQL plan-shape result, not evidence of end-to-end throughput or CPU improvement.

## Change and safety boundary

`HubTaskStore.CandidateBatchAdmissionSql` still materializes selected IDs, probes task IDs/epochs, and performs authoritative eligibility checks in the same per-candidate subquery that acquires `FOR UPDATE SKIP LOCKED`. `ORDER BY t.sequence`, owner lock and final epoch checks, partition/work eligibility, constraints, native status and pin checks, attempt/history atomicity, and final primary-key update are unchanged. The old OR form of the state/due/lease predicate was replaced with mutually exclusive state branches:

```sql
AND CASE t.state
    WHEN 0 THEN t.attempts<t.max_attempts AND t.due_at<=now()
    WHEN 1 THEN t.attempts<t.max_attempts AND t.lease_until<now()
    ELSE false
END
```

This prevents the planner from proving the claim-sequence partial-index predicate from the eligibility condition, so it cannot select that broad index as the per-ID probe. It does not depend on CTE/LATERAL/OFFSET barriers forcing a plan. No lock boundary moved; no physical tuple identity, index, migration, GUC, or shared-database statistics change was introduced. Real-store tests exercise eligibility, owner expiry/takeover and rollback, replay/ambiguous commit handling, attempt/history atomicity, overlapping batches, sequence ordering, and a competing state transition while the selected row is locked.

## Isolated plan experiment

The reproduction is `scaling/benchmarks/hub-task-spike-diagnosis/reproduce-stale-partial-index.py`; run instructions and boundaries are in its README. `before-selected-id.sql` is the exact old statement. The script extracts the current statement from `HubTaskStore.cs`, starts a uniquely named PostgreSQL 17 container with `--network none`, 2 CPU and 2 GiB memory, and uses synthetic data only. Autovacuum is disabled only on the synthetic fixture table. Each mutating `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` is wrapped in a transaction and rolled back.

The matched fixture has 144,000 rows. It is analyzed with zero qualifying active entries, then 44,000 rows are activated without ANALYZE. Both old and new SQL run against that same stale state; then both run against the same fixture after ANALYZE (estimated active entries 44,045), and after churn with the estimate held at 44,045. Batches 1/8/16/32 are evaluated under `force_custom_plan` and `force_generic_plan`. Empty selection, missing ID and stale epoch cases are included. The stale baseline must exceed one million `hub_task` visits at batch 32; the current query is checked to remain at most four visits per selected ID, failing the harness otherwise.

| Fixture / SQL | Batch | Plan mode | Active-index estimate | `hub_task` visit work | Root shared hits | Execution ms |
|---|---:|---|---:|---:|---:|---:|
| stale-empty / before | 1 | custom | 0 | 44,002 | 1,131 | 8.158 |
| stale-empty / before | 8 | custom | 0 | 352,016 | 9,002 | 51.143 |
| stale-empty / before | 16 | custom | 0 | 704,032 | 18,007 | 86.623 |
| stale-empty / before | 32 | custom | 0 | 1,408,064 | 36,095 | 203.357 |
| stale-empty / current | 1 | custom | 0 | 3 | 33 | 0.282 |
| stale-empty / current | 8 | custom | 0 | 24 | 233 | 0.448 |
| stale-empty / current | 16 | custom | 0 | 48 | 488 | 0.606 |
| stale-empty / current | 32 | custom | 0 | 96 | 1,059 | 0.868 |
| fresh-44k / before | 32 | custom | 44,045 | 96 | 1,062 | 0.881 |
| fresh-44k / current | 32 | custom | 44,045 | 96 | 996 | 0.994 |
| churn-44k / before | 32 | custom | 44,045 | 96 | 991 | 0.779 |
| churn-44k / current | 32 | custom | 44,045 | 96 | 1,001 | 1.133 |

The complete 60-row matched summary, including generic plans, and raw JSON EXPLAIN trees are retained at:

- `C:/Users/haido-ws/AppData/Roaming/orca/chat-scratch/740b1a1da96a083620bbe9d96f86cc40/postgres-hub-task-fix-luna/matched-before-after-summary.csv`
- `C:/Users/haido-ws/AppData/Roaming/orca/chat-scratch/740b1a1da96a083620bbe9d96f86cc40/postgres-hub-task-fix-luna/matched-before-after-plans.json`

In the stale custom batch-32 plans, the old statement's second lookup is `Index Scan ix_hub_task_claim_sequence` with no index condition, 43,999 rows removed per loop over 32 loops; the first and final accesses are PK lookups. The fixed statement uses `hub_task_pkey` for all three task-table accesses (selected ID/epoch, authoritative locking probe, final update), each with `id` as index condition; scan work is exactly 3×batch in all stale custom/generic arms. The stale generic baseline is also broad (1,408,064 visits; 36,126 shared hits); fixed generic performs 96 visits/1,052 shared hits. Fresh and churn states use bounded PK probes for both statements, which shows the difference is tied to the stale-empty partial-index estimate rather than a generally faster query. Root shared-block hits and execution times are plan metrics, not PostgreSQL CPU measurements.

For edge cases in both plan modes, missing IDs returned 0 rows/0 task visits, an incorrect observed epoch returned 0 rows/1 task visit, and empty selection returned 0 rows/0 task visits. The current SQL passed these and all freshness/churn arms. These results establish bounded access on this fixture and those PostgreSQL 17 plans; they do not prove every possible catalog/statistics state or concurrent schedule. The real-store test coverage supplies transaction/locking semantics, but the synthetic plan harness itself is single-session and does not simulate concurrent workload contention.

## Verification

- Source commit before this uncommitted fix: `e9c6c4e3e60f17e2cc29ee8d04b9e964bad7380c`.
- PostgreSQL image: `postgres:17`, image ID `sha256:ff87138949ad9a46f32ff23bd6852b52ce024a13dffe9b4087be98292cdd3b4f`; server reported PostgreSQL 17.11. Owned diagnostic container was `pg-hub-task-fix-diag-20261115095710-4a7804`, labeled and removed automatically by the harness.
- Old SQL fixture SHA-256: `6661886f20c73c83cca0c5bcfab797bb031445bb3a5873a354d3feff21c06362`. Normalized final SQL extracted by the harness SHA-256: `cc0c148a07455727e088983ececcdb599ff48423bdea734c6a9834820d76e063`. Full edited `HubTaskStore.cs` SHA-256: `780402ae50dbbd5f9e81731976fd3301e2f7d41cd551d16a3c1f928d590999a5`.
- `dotnet build scaling/WorkflowCore.Scaling.slnx -c Release`: exit 0, 0 errors, 7 warnings already in unrelated deprecation/naming/xUnit analyzer locations.
- Focused PostgreSQL tests against explicit isolated PG17 `WFC_TEST_PG`: 9 passed, 0 failed, 0 skipped. This includes the plan test and all Hub candidate batch semantics tests.
- Full PostgreSQL suite against a fresh isolated PG17 database with explicit `WFC_TEST_PG`: 314 passed, 0 failed, 0 skipped (1m25s), exit 0. PostgreSQL migrations were applied to that isolated test database; no database-exercise skip was hidden by an unset environment variable.
- Logs: `C:/Users/haido-ws/AppData/Roaming/orca/chat-scratch/740b1a1da96a083620bbe9d96f86cc40/postgres-hub-task-fix-luna/focused-postgres-tests.log`, `full-postgresql-suite.log`, `full-test-db-migrations.log`, and `test-db-migrations.log`.

The changed files are the Hub SQL, its plan and semantics tests, the isolated synthetic reproduction and README, and this report. There was no shared-stack refresh, shared-database query/mutation, runtime setting change, CPU test, or end-to-end benchmark in this task; the refreshed integration stack was not rebuilt or deployed. A follow-up `make obs-test-4k` on the new source is still needed for live workload/CPU impact. The isolated test database container `pg-hubtask-fix-tests-luna-20261115` (container ID `2e12edf57f23293dd02213e91ed5080acab6d3c8a006f53b1941e9f291acb240`) was stopped after verification without removal, so its isolated database remains in its container layer; its owned network was left inactive. The shared development stack was left untouched.


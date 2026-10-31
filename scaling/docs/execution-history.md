# Distributed execution history

## Scope
Append-only workflow lifecycle and step-attempt journal. Current `wfc.Workflow` and `ExecutionPointer` remain the authoritative state snapshots. History is diagnostic and audit data, not deterministic replay.

## Semantics
- Use workflow gate revision plus an event index for transaction-scoped ordering.
- Write state transitions and corresponding journal rows in the same PostgreSQL transaction.
- Record attempt-start intent in a separate fenced transaction **before** running external step bodies. An attempt with no terminal outcome is `unknown`, not necessarily failed.
- Record completion, failure, retry scheduling, suspension, resume, signal delivery and terminal status.
- A worker crash between external effect and commit may cause another attempt. Journal entries do not guarantee exactly-once effects.
- Never record secrets, raw credentials or full arbitrary workflow payloads. Apply size bounds and redaction.
- Do not automatically delete journal records; define archival and retention separately.

## Implementation checklist
1. Apply `005_execution_history.sql` after existing migrations.
2. Add a typed journal writer that participates in the EF transaction.
3. Capture step-start before execution, using fencing and stable attempt IDs.
4. Capture pointer transitions by comparing the pre-execution snapshot with the result.
5. Capture control-plane transitions, event delivery and retry/dead-letter outcomes.
6. Add pagination API by workflow ID and ascending history ID.
7. Add PostgreSQL integration tests for crash/reclaim, concurrent workers, rollback, event fanout, and secret redaction.

## Caution
This commit introduces the schema and design only. Runtime journal emission is not yet wired. Do not treat the table as populated until the remaining checklist is implemented.

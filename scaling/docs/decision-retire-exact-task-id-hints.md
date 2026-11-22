# Decision: retire exact task-ID candidate hints

**Status:** Implemented in application code and migration 040  
**Decision date:** 2026-11-22  
**Scope:** Exact task-ID hint and hydration mode only

## Decision

The implementation removes `WFC_HUB_CANDIDATE_TASK_ID_HINTS_ENABLED` and its
exact task-ID producer, listener, hydration, configuration, and test paths. Do not rebuild this mode as
another per-task notification or candidate-hydration fast path without a new
architecture decision backed by controlled I/O measurements.

Keep the partition-owned in-memory candidate matcher, PostgreSQL-authoritative
claiming, coarse bounded shard/bucket readiness signals, and indexed periodic
recovery. This decision does **not** retire
`WFC_HUB_MATCHING_CANDIDATES_ENABLED` or the candidate matching architecture.

## Reason

The exact path opts task writers into row-level task change notifications,
including claim, retry, and completion invalidations, and then performs exact-ID
candidate hydration reads. That adds database notification/outbox and read
work proportional to task transitions. The default statement-level shard signal
already coalesces ready work by hub and bucket, while indexed discovery and
recovery preserve correctness. The exact path therefore adds an I/O-heavy
alternate path without being required for correctness.

This is a design decision based on the path's work shape; it is not a claim that
a particular production workload has been benchmarked.

## Implemented removal

1. Removed the environment flag and PostgreSQL session GUC from Hub
   configuration and task-writer setup. Removed exact-ID handling, bounded
   exact-ID queues, and hydration work from the Gateway and candidate registry
   while retaining coarse hint parsing, recovery requests, and owner routing.
2. Added ordered, additive migration 040 after migration 039. It removes the
   per-row exact-ID trigger path while retaining statement-level bucket signal
   triggers and the explicit outbox compatibility behavior. Earlier migrations
   remain unchanged; no task rows are reset or rewritten.
3. Removed the flag from Compose presets, scripts, docs, and exact-ID-only
   metrics. Updated tests around coarse signals and removed tests whose only
   purpose was the exact-ID mode. Candidate matching, fencing, and missed-hint
   recovery coverage remain.
4. Migration 040 applied in the local PostgreSQL test setup without resetting
   the database. The focused notification/outbox/acquisition/recovery tests
   passed (19/19); the full suite passed 272/273, with the remaining failure in
   the unrelated `HubPushBatchFailureTests` suite. A before/after I/O benchmark
   was not run, so this decision makes no measured performance claim.

Do not reintroduce the exact-ID path as another per-task notification or
candidate-hydration fast path without a new architecture decision backed by
controlled I/O measurements. The legacy custom PostgreSQL setting has no
effect after migration 040.

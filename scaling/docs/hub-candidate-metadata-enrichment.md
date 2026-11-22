# Hub candidate discovery and advisory metadata

The normal partition-owned candidate matcher remains enabled with
`WFC_HUB_MATCHING_CANDIDATES_ENABLED=true`. Discovery scans the existing indexed
ready and expired task streams by hub, partition bucket, state, due/lease time,
attempt count, finite horizon and keyset cursor. SQL returns bounded task
identity, routing fields and deadlines. The reader batches workflow-pin lookups
for `__workflow__` and `__native_workflow__` tasks. A bounded process-local LRU
reuses that metadata across indexed pages. Missing, oversized or failed lookups
remain advisory-unknown and can proceed to PostgreSQL admission. Activities can
carry a workflow ID without a pin lookup. Exact task-ID hydration was removed
with the notification fast path; see the [retirement decision](decision-retire-exact-task-id-hints.md).

PostgreSQL validates workflow pins, worker capabilities, owner and task epochs,
retries and lease state in the claim transaction.

Discovery keeps its horizon and keyset cursor across bounded pages, including
when a row fits individually but not in the remaining page memory budget.
Periodic recovery, owner-generation checks, reserved
entries and ambiguous-commit quarantine remain registry responsibilities.

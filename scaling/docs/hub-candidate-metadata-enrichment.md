# Hub candidate discovery and advisory metadata

The normal partition-owned candidate matcher remains enabled with
`WFC_HUB_MATCHING_CANDIDATES_ENABLED=true`. Discovery scans the existing indexed
ready and expired task streams by hub, partition bucket, state, due/lease time,
attempt count, finite horizon and keyset cursor. SQL returns bounded task
identity and routing fields only. The registry applies its page and process
memory budgets, reuses cached entries, and hydrates new or stale candidates in
bounded ID batches.

Hydration returns resource keys only when the complete set fits a conservative
per-candidate share of the batch byte budget. Missing or oversized sets remain
`ResourceKeysComplete=false`; they are unknown advisory constraints and cannot
enter the confidently-unconstrained batch path. Workflow definitions are
looked up in a separate bounded batch only for `__workflow__` and
`__native_workflow__` tasks, then reused through the existing bounded
process-local LRU. Missing, oversized, or failed workflow lookups remain
unknown. Activities do not fetch workflow definitions merely because they
carry a workflow ID.

These values only guide matching. PostgreSQL admission still validates current
workflow pins, resource capacity, strict FIFO order, owner/task fencing,
permits, retries and lease state in its authoritative transaction.

Discovery keeps its horizon and keyset cursor across bounded pages, including
when a row fits individually but not in the remaining page memory budget.
Periodic recovery, owner-generation checks, refresh-pending exclusion, reserved
entries and ambiguous-commit quarantine remain registry responsibilities.

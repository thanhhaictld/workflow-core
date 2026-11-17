# Hub candidate metadata enrichment

## Data flow

The partition-owned candidate matcher remains the normal Hub matching path. The
task-ID hint feature remains an independent experimental option controlled by
`WFC_HUB_CANDIDATE_TASK_ID_HINTS_ENABLED`; this enrichment change works when
that option is off.

Indexed discovery now pages only bounded task identity/routing fields: task ID,
sequence, hub/partition, task type/version, workflow ID, epoch and due/lease
deadline. `BuildPageSql` no longer joins `hub_workflow` or groups
`hub_task_constraint` for every recovery page. After the page is read, the
owner-generation registry reuses the enriched candidate it already holds when
the task's stable identity is unchanged and the known workflow metadata is
within its refresh window. New IDs, changed identities, and expired workflow
metadata are collected and sent through one bounded `HydrateAsync` ID batch.
The same hydration method serves optional exact task-ID hints; there is no
separate enrichment query shape for that feature.

Hydration rechecks current eligibility, routing, and epoch from PostgreSQL,
then returns bounded resource keys. Workflow definitions are fetched in one
additional batch for distinct workflow IDs not already known in the reader's
cache. This query uses `hub_workflow.id` probes, not a task-ledger scan. The
candidate registry retains task resource keys under its existing per-owner and
process candidate entry/byte limits. PostgreSQL claim-by-ID admission still
reloads current constraints and remains authoritative for owner/task epochs,
workflow pins, resources, strict FIFO, permits, retries and lease state.

## Workflow metadata cache and unknown values

The reader keeps positive and negative/oversized workflow lookup results in a
process-local LRU. Keys are `(hub, matching partition, workflow ID)`. The
default limits are 256 entries and 1 MiB per matching partition, plus 4,096
entries and 8 MiB process-wide. Definition IDs over 4,096 UTF-8 bytes are
suppressed in SQL. Entries expire after 30 seconds; the owner registry checks
that expiry before reusing workflow enrichment, so a definition changed by an
out-of-band writer is eventually re-read. Cache eviction only causes another
bounded lookup and never removes a durable task.

Missing or oversized workflow metadata sets `WorkflowMetadataComplete=false`.
Workflow-pin matching treats that value as unknown and permits an advisory
candidate; the DB claim enforces the actual pin. A known incompatible definition
can still be rejected in memory. If the metadata budget cannot carry a
definition, the candidate stays unknown and is refreshed after its short
metadata expiry.

Resource constraints follow the existing conservative rule. SQL returns a
complete key list only when its UTF-8 accounting fits the batch budget. Empty
keys are marked complete only when the database has established there are no
constraints. Omitted or oversized keys remain `ResourceKeysComplete=false`, so
batch admission cannot treat them as unconstrained. Resource release and policy
change hints remain conservative; PostgreSQL checks final capacity and FIFO.

## Recovery, bounds, and validation

The horizon/keyset pages, two-second readiness reconciliation, periodic sweep,
due-task and expired-lease recovery remain unchanged. A hydration cancellation or
query failure does not advance the page cursor; the page can be retried. Owner
generation loss prevents page results from being applied. Reserved and
ambiguous-commit entries remain protected by the existing registry fences.
All task IDs are unique within a bounded page batch, and each page performs at
most one batched task/resource hydration plus one batched workflow metadata
lookup; there is no per-task query loop.

The reader's workflow cache limits are constructor-injectable internally for
small-budget regression tests. `HubWorkflowCacheSnapshot` exposes bounded
entries/bytes and low-cardinality hit, miss, and eviction totals to tests; the
runtime exports matching aggregate counters without hub/task identifiers.

The optimization removes repeated workflow joins and constraint aggregation
from every recovery page. It adds a bounded hydration for new or refreshed
candidate entries and may therefore add a round trip for first-seen work. The
task-ID feature qualification does not measure this change. Compare equal
candidate-enabled configurations with task-ID hints off in both arms before
claiming an end-to-end benefit.

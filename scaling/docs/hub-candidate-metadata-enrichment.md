# Hub candidate metadata enrichment

## Data flow

The partition-owned candidate matcher remains the normal Hub matching path. The
exact task-ID hint feature is independent and remains experimental/off by
default under `WFC_HUB_CANDIDATE_TASK_ID_HINTS_ENABLED`.

Indexed discovery now pages only bounded task identity/routing fields: task ID,
sequence, hub/partition, task type/version, workflow ID, epoch and due/lease
deadline. `BuildPageSql` does not join `hub_workflow` or aggregate
`hub_task_constraint` for every recovery page. The owner-generation registry
reuses enriched candidates already held in its bounded entries when identity,
epoch and workflow metadata freshness permit. New IDs, changed identities, and
expired workflow metadata are collected for one bounded `HydrateAsync` ID batch.
The optional exact task-ID path shares this enrichment method.

Hydration rechecks current eligibility, routing, and epoch from PostgreSQL,
then returns resource keys. Workflow definitions are fetched in one additional
batch for distinct workflow IDs not already known in the reader's cache. This
query uses `hub_workflow.id` probes, not a task-ledger scan. PostgreSQL
claim-by-ID admission still reloads current constraints and remains
authoritative for owner/task epochs, workflow pins, resources, strict FIFO,
permits, retries and lease state.

## Workflow cache and unknown values

The reader keeps positive and negative/oversized workflow lookup results in a
process-local LRU. Keys are `(hub, matching partition, workflow ID)`. Defaults
are 256 entries and 1 MiB per matching partition, plus 4,096 entries and 8 MiB
process-wide. These cache limits are additional to the candidate registry's
existing entry/byte budgets. Definition IDs over 4,096 UTF-8 bytes are
suppressed in SQL. Entries expire after 30 seconds; the owner registry checks
expiry before reusing workflow enrichment, so out-of-band definition changes
are eventually re-read.

Cache accounting conservatively estimates UTF-16 managed strings and includes
node and scope-index overhead. One process-local workflow-fetch gate bounds
concurrent cold metadata lookup batches to one; a waiter rechecks the cache
before issuing a query, so overlapping shared-workflow misses do not duplicate
DB work. Cache eviction only causes another bounded lookup and never removes a
durable task. Aggregate hit, miss, lookup, row and eviction telemetry carries
no hub or task identifiers.

Missing or oversized workflow metadata sets `WorkflowMetadataComplete=false`.
Workflow-pin matching treats that value as unknown and allows an advisory
candidate; the DB claim enforces the actual pin. A known incompatible definition
can still be rejected in memory. If the metadata budget cannot carry a
definition, the candidate stays unknown and is refreshed after metadata expiry.
A workflow-only metadata query failure is reported through the existing error
callback and preserves the task/resource metadata already loaded; it leaves the
workflow pin unknown for that pass and does not populate the cache.

Resource constraints follow the existing conservative rule. SQL returns a
complete key list only when its UTF-8 accounting fits the batch budget. Empty
keys are marked complete only when the database established there are no
constraints. Omitted or oversized keys remain `ResourceKeysComplete=false`, so
batch admission cannot treat them as unconstrained. Resource release and policy
change hints remain conservative; PostgreSQL checks final capacity and FIFO.

Supported task creation paths insert `hub_task_constraint` rows in the same
transaction as the task (`HubRemoteWorkflowStore` for workflow expansion and
`HubTaskStore` for provisioner/native work). Runtime state, due time, lease
epoch, retry and owner transitions update `hub_task` but do not rewrite
constraint membership. Direct SQL that edits constraints is outside the
runtime contract; if that contract changes, resource enrichment must be
invalidated/refreshed. Missing or over-budget constraint metadata is always
unknown, never an empty set.

## Recovery, bounds, and failure behavior

Horizon/keyset pages, two-second readiness reconciliation, periodic sweep,
due-task and expired-lease recovery remain unchanged. Page SQL emits only
metadata fitting the wire budget after reserving requested-ID bytes. A
page-boundary candidate that fits a full request but not the current page share
causes a bounded retry at page size one. A candidate too large for that
one-row request is reported as oversized and the keyset cursor advances; a
large requested page therefore cannot divide a valid candidate into a
permanently too-small byte share. Page output and every hydration batch remain
bounded before text or resource arrays cross the connection.

Task/resource page hydration cancellation clears `RefreshPending`, marks the
partition dirty and retains the cursor for retry. A non-cancellation hydration
failure is logged through the existing registry error callback, advances only
that bounded page and requests a fresh dirty horizon; indexed recovery revisits
missed durable rows while other streams/admissions continue. Owner-generation
loss prevents asynchronous results from applying. `RefreshPending` entries are
excluded from matching and credit selection and are cleared on cancellation or
invalidation; reserved and ambiguous-commit entries remain protected by
registry fences.

Each page performs at most one batched task/resource hydration and one batched
workflow lookup; no per-task query loop is issued. Resource membership is
stable across supported runtime mutations because the task creation paths
write the constraint rows transactionally and later execution/retry/lease
updates change task state, not constraints. DB admission remains final resource,
policy, pin and FIFO authority.

## Tradeoffs and qualification

The optimization removes repeated workflow joins and constraint aggregation
from every recovery page. It adds a bounded hydration for new or refreshed
candidate entries and may add a cold workflow lookup. Compare equal
candidate-enabled configurations with task-ID hints off in both arms before
making claims about end-to-end benefit. Query simplicity alone is not evidence
of lower total work.

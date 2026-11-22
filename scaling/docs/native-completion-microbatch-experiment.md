# Experiment: fenced native activity completion microbatch

## Base / scope

Branch `experiment/fenced-native-completion-microbatch` starts at PR #42 commit
`40c97f96581cdba7d6f8261c976d12548e176c9e`. It does **not** include PR #43's slower single-statement, single-activity CTE optimization.

This experiment targets **routed native activity completions only**. Workflow/native snapshot commits and legacy/remote/managed/standalone tasks keep the original APIs and transaction boundaries.

## Data flow

1. The native activity worker still executes the activity **once** and freezes its result along with task ID, worker ID and lease epoch. Heartbeats continue while the worker awaits the completion ACK.
2. With `WFC_HUB_COMPLETION_MICROBATCH_ENABLED=true`, one bounded processor-local `Channel` collects finished activity results. Flush when 16–64 are available or the oldest waits 2–5 ms, whichever is first. The channel has bounded backpressure and up to four independent HTTP batch requests in flight.
3. `POST /api/hub/workers/tasks:complete-batch` accepts at most 64 independent native activity completions.
4. PostgreSQL resolves task/workflow metadata and locks workflow rows in stable ID order using `FOR UPDATE SKIP LOCKED`, followed by task rows in stable ID order with the same `SKIP LOCKED`. At most one item per workflow is admitted to each transaction; same-workflow duplicates and busy rows return **retry** without blocking other rows.
5. The database transaction updates eligible leased tasks with **worker + epoch + live lease CAS**, completes attempts, inserts native result inbox messages and durable `ActivityResultReceived` events and wakes runnable workflows. All writes are one atomic transaction. Results are returned only after COMMIT.
6. The batch response is per task: **completed**, **fenced**, or **retry**. The original worker retry loop resends *the frozen result*, never reruns the activity body. Replays compare worker, epoch and JSON result. A 100-ms transaction-local PostgreSQL lock timeout bounds unexpected index/trigger contention; on timeout the entire transaction rolls back and every item is retried.

## Settings (worker process)

| Environment variable | Default | Valid experiment range |
| --- | --- | --- |
| `WFC_HUB_COMPLETION_MICROBATCH_ENABLED` | `false` | `true` / `false` |
| `WFC_HUB_COMPLETION_BATCH_SIZE` | `32` | 16–64 |
| `WFC_HUB_COMPLETION_BATCH_DELAY_MS` | `3` | 2–5 |

The endpoint is authenticated with the same Hub bearer token as the existing single-task endpoint. It is implemented in the generic Hub layer; processors do not receive PostgreSQL credentials.

## Safety and trade-offs

- **Per-task fencing:** only the current task worker, epoch, and unexpired lease can produce success. Duplicate/stale results never append history.
- **Exactly-once database effects for an accepted (task, epoch)** under the existing unique constraints; business side effects still require idempotency on worker replay/failover.
- **Durable ACK:** no early acknowledgement of a buffered completion; unknown HTTP/commit outcome triggers idempotent retry.
- **Isolation:** workflow and task `SKIP LOCKED` avoids waiting on a known busy row; one task per workflow per transaction. Independent batches can make progress while another hits transient contention. This is **not strict per-task latency isolation**: a batch still has one shared COMMIT and one 100-ms timeout; a hard SQL error rolls back all its members. Larger batches increase this remaining coupling, so p95/p99 must be tested.
- **Backpressure:** bounded channel queues workers rather than growing memory indefinitely; cancellation during shutdown does not claim success, and leases recover.
- **No ordering changes:** the existing task eligibility and workflow row locks remain authoritative; matching owner mode stays strict in the A/B comparisons.
- **No automatic fallback inside a failed batch:** clients retry the immutable completion, preventing a long-running fallback on a contended row from blocking unrelated batch members.
- **Coverage:** legacy `/tasks/{id}/complete` remains default and is not modified. The new path only supports native routed activities; it cannot be used as a replacement for arbitrary task kinds.

## Benchmark and acceptance

CI: `.github/workflows/workflow-hub-completion-microbatch-ab.yml`. The independent A/B jobs use two Hub replicas, four matching shards, identical worker topology and 1,000 `policy-unordered` workflows with one routed activity each; *all 1,000 durable `ActivityResultReceived` events must be present*. Baseline uses the original individual REST/transaction completion. Experimental arms use batch settings `16/2ms`, `32/3ms` and `64/5ms` at eight and sixteen processors.

Compare workflows/s, workflow p95/p99, failure/retry/expired-lease counts, PostgreSQL commits and wait samples, and `workflow_hub_native_completion_batch_size`, `workflow_hub_native_completion_batch_duration_seconds` and `workflow_hub_native_completion_batch_outcomes`. PostgreSQL commits per workflow should fall if batches fill; the same number of logical task/history/inbox rows must remain durable. CI is a noisy shared-runner screening test, not sufficient for a production or capacity claim.

**Merge gate:** No missing history, false ACK or duplicate execution; bounded retry behavior under held workflow/task locks; repeatable throughput or p95 benefit; no unacceptable increase to p99 or timeout retries. The feature flag remains off by default.

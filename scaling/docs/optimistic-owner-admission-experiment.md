# Experiment: optimistic matching-owner admission

## Hypothesis

The Hub can avoid taking `FOR SHARE` on
`wfc_scale.hub_matching_partition_owner` during each task claim, reducing
contention with shard owner renewals, release and takeover. This is an
**experimental consistency trade-off**, not a production default.

Base: PR #41 (`thanhhaictld/hub-memory-candidates-luna`).

## Switching modes

| Setting | Meaning |
| --- | --- |
| `WFC_HUB_OPTIMISTIC_OWNER_ADMISSION=false` (default) | Hold a shared owner-row lock until the task claim/history transaction commits |
| `WFC_HUB_OPTIMISTIC_OWNER_ADMISSION=true` | Check owner/epoch/lease before candidate admission and after task/history writes, **without owner-row locks** |

Both candidate batch admission and owner-directed pull batch admission use
this flag. It is passed to both Hub replicas in the HA Compose setup.

## Guarantees retained

- Task eligibility is revalidated against PostgreSQL while taking the task row lock.
- Only one task claim CAS can win for the current task state and epoch.
- Task lease epoch increments atomically with owner assignment and attempts.
- Completion/heartbeat rejects an older task epoch.
- Task attempt rows and workflow claim events retain their existing transactional writes.
- Uncommitted admission rolls back on owner loss detected at the final recheck.
- Background candidate rediscovery still reconciles missed signals.

## Deliberately weaker matching-owner behavior

There is **no linearization guarantee across matching-owner takeover** in
optimistic mode. A claim may pass its final owner/epoch check, then a shard
takeover may commit, and then the old owner may commit its in-flight task
claim. The old-owner task lease remains valid; ownership change does not
itself invalidate a task lease. A competing owner cannot take the same task
until that lease expires or is safely released. Side-effecting activities
must remain retry-safe with a stable idempotency key.

Do not enable this mode for workflows that require a hard rule such as
"no old shard generation may admit anything after drain/takeover".
Graceful Hub drain becomes best-effort and relies on lease expiry and
recovery. **Worker draining is not changed by this experiment**.

## Correctness tests

`HubOptimisticOwnerAdmissionTests` covers:

1. Owner-directed pull claiming while another transaction holds a conflicting
   `FOR UPDATE` owner lock: optimistic admission completes without waiting.
2. A candidate claim paused after owner recheck: the old matching lease
   expires; the new owner takes over and claims a *different* task before
   the old transaction commits.
3. An already-leased task is not double claimed by the new owner, and a
   subsequently recovered task uses a newer epoch that rejects an old
   worker's completion.

Existing strict-mode owner takeover tests remain unchanged and use the
default `false` setting.

## CI A/B and measurement

- `.github/workflows/distributed-runtime.yml`: 1,000 four-step workflows
  using `legacy`, `candidate` (strict), `candidate-optimistic` arms.
  Same PostgreSQL runner, pool and worker topology for strict vs optimistic.
- `.github/workflows/workflow-hub-sse-stress-1000-16.yml`: strict versus
  optimistic at **4, 8, and 16** processor replicas, using two Hubs and
  four matching shards. Automatic only for pull requests targeting
  `thanhhaictld/hub-memory-candidates-luna`; also supports manual dispatch.
- Compare throughput (workflows/s), completion p95/p99, PostgreSQL commits,
  connection pressure, owner-lock wait samples, and task failure count.
- All 1,000 workflow completions must be durable with zero failures.

## Decision criteria

Prefer optimistic mode only if repeat runs demonstrate a meaningful
improvement at p95 (or saturation throughput) with no correctness regression.
A single GitHub-hosted runner gives noisy CPU and PostgreSQL measurements;
collect multiple same-topology runs before adopting the change.

This experiment intentionally does **not** change transactional attempt
history or workflow-event commits. Async history projection should be
evaluated as a separate experiment to isolate write-amplification effects.

# Matching partition candidate cache: validation proposal

Status: **candidate cache not implemented or performance validated**. The
experimental task-store API can fence an unconstrained batch using a captured
matching-owner generation, but the gateway does not invoke it yet.

## Problem and hypothesis

In one local 4,000-workflow run, the split unconstrained batch-claim statement
accounted for 119.5 seconds of cumulative execution time, out of approximately
207.6 seconds for all statements since PostgreSQL started. It reported 47.6
million shared-buffer hits and 3,144 shared-buffer reads. Peak PostgreSQL CPU
was about 3.5 cores averaged over one minute. These measurements include other
activity, are not query CPU measurements, and do not establish a causal plan
or a comparison with the unsplit query.

The hypothesis is that a partition owner can amortize candidate discovery by
loading a bounded page of task IDs, matching it to compatible worker demand,
and claiming IDs through indexed, conditional SQL updates. This may reduce
repeated ordered scans and buffer work. It adds refill queries, stale candidate
handling, memory, and potentially additional contention; only a controlled
end-to-end comparison can establish whether it helps.

An isolated PostgreSQL 17 plan probe with 20,000 synthetic tasks, of which
4,000 were ready, illustrates one possible discovery cost. For one shard and
capability, a read-only ready-candidate query returning 128 IDs read 4,000
index entries, filtered 3,000, visited 1,000 unconstrained candidates, and
reported 2,087 shared-buffer hits. PostgreSQL chose the capability-sequence
index followed by a top-N sort. This simplified probe omitted workflow status,
pins, expired candidates and row locks; its plan and timings cannot be
extrapolated to the live run or establish a cache speedup. It does establish
that even an indexed discovery query can examine substantially more rows than
the returned page in a plausible distribution. The disposable database was
removed after the probe; the running stack was not changed.

## Proposed boundary

- Opt in explicitly; keep the current claim path available as a fallback.
- Initially limit the experiment to unconstrained tasks in the sharded,
  event-gated matching path. Keep global/keyed permits and strict FIFO on the
  existing transaction path. Preserve direct and legacy claim behavior.
- Cache only bounded candidate metadata keyed by hub, matching partition and
  owner generation, with capability and workflow-version compatibility. A
  cache entry is a hint, never a lease or proof of eligibility.
- PostgreSQL remains authoritative for due time, state, attempts, partition,
  capability, workflow status and pins. Recheck them and absence of constraints
  while locking candidate task rows. Update execution epoch, lease, attempts,
  and relevant history atomically before delivering a task.
- Coordinate owner generation with takeover and shutdown. A local owner check
  alone cannot fence a paused process; design and test the exact transactional
  relationship between owner validation, claim and lease takeover. Existing
  task-lease fencing remains necessary even with such coordination.
- A committed but undelivered task lease expires normally. Existing worker
  leases survive owner shutdown and are not released just because matching
  ownership changes.

### Admission and takeover

The existing owner check is a separate query before `ClaimAsync`; direct claims
also bypass matching ownership. A paused old owner can therefore resume after
takeover and still attempt a task claim. Existing task locks and epochs protect
the task lease, but do not establish exclusive matching-owner admission. Adding
a snapshot `EXISTS` owner predicate alone would not serialize a concurrent
takeover on the owner row.

For the proposed stronger admission contract, capture `(hub, partition, count,
owner ID, owner epoch)` before waiting for admission. In a short transaction,
lock the corresponding owner row before locking tasks, validate that exact token
and unexpired lease using the database clock after acquiring the lock, and hold
it through the conditional task updates and history commit. Takeover and release
must update that same row. A claim admitted before takeover can still commit
after wall-clock lease expiry; the lock establishes transaction ordering, not
an expiry-at-commit guarantee. Bound lock/transaction duration and recheck time
near mutation. Do not hold the lock while waiting for demand or network I/O.
Discard late refill callbacks after an epoch change, including reacquisition by
the same node. This protocol needs concurrency testing and lock-order review;
it is not an assertion about the current implementation.

The task store now offers this fencing to explicit callers through its optional
`matchingOwner` argument. It requires one matching shard and unconstrained batch
claiming, locks the captured owner generation in the same transaction as the
batch lease and history, and does not enter the individually committed fallback.
Existing gateway calls do not supply that argument; no default dispatch behavior
has changed. Callers must still provide bounded fallback service for constrained
tasks and handle an invalid owner generation. This API alone is not a cache or
an end-to-end owner-fencing guarantee.

An isolated PostgreSQL 17 protocol check confirmed that a transaction holding
the owner row `FOR UPDATE` caused a concurrent owner update to hit a 500 ms
lock timeout. Once the lock was released and the row changed from owner `old`,
epoch 1 to owner `new`, epoch 2, the old-token locking query returned no row.
This tests the database locking primitive only: it does not validate task
claim atomicity, delivery, crash recovery, or a cache implementation.

## Liveness and selection concerns

Committed matching notifications include hub, type, version and partition,
not task IDs. Notifications can be lost or coalesced; future due times, retries
and lease expiry also require later rediscovery. Refill on demand and perform
bounded periodic reconciliation, including after takeover and cache eviction.
Do not use an irreversible sequence watermark: an older task can become eligible
after the cursor passes it. Refill and selection need a defined fairness policy
for scarce capabilities, pins, and old requeued tasks; a full page of unsuitable
work must not permanently hide another worker's eligible tasks. Bound memory,
refill concurrency and empty probes across hubs, partitions and capabilities.

Cancellation and ambiguous database commit outcomes require particular care:
do not reassign a task based on the disappearance of an in-memory reservation.
On invalidation, cache only affects rediscovery; PostgreSQL decides whether a
candidate is available. Design how empty/stale pages fall back without causing
an unbounded scan or busy loop. Preserve existing sequence-selection guarantees
or explicitly document and test any intended relaxation.

Sequence allocation also does not imply commit order: a transaction can allocate
an older sequence, pause, and commit after discovery has advanced. The current
matching hint sweep's 512-group limit also needs pagination or another finite
recovery mechanism; repeatedly scanning the same prefix is insufficient.
Reserve service for constrained fallback even under a continuous unconstrained
backlog, and preserve SSE credit accounting and the native atomic-batch rule.
The current statement selects oldest eligible unlocked candidates at its
snapshot. An incomplete cache can only offer approximate ordering unless it
performs a fresh database head check; do not silently weaken that behavior.

## Validation before enabling

1. Capture representative read-only candidate `EXPLAIN (ANALYZE, BUFFERS)`
   plans during a backlog, including ready and expired branches. Establish
   which indexes and filters cause the observed buffer work.
2. Test competing owners, an owner paused across lease expiry, takeover during
   a claim, database loss, graceful release, worker disconnect, uncertain
   commit, and a crash after commit before delivery. Check task epochs, attempts
   and durable history, not merely the number of responses.
3. Test delayed and retried older tasks, expired leases, missed/coalesced hints,
   cache eviction, cancellation, capacity bounds, incompatible capabilities and
   version pins, and mixed constrained/unconstrained traffic across partitions.
4. Compare cache off/on using repeated equivalent 4,000-workflow runs. Keep the
   data distribution, backlog, worker counts, topology and all other flags
   fixed. Record completion and durable audit, throughput and latency, CPU,
   query calls/time, shared and temp blocks, cache hit/refill/stale counts,
   memory, owner failover and oldest eligible task age. Record statistics reset
   boundaries and report variance. Do not reset an existing database as a
   routine test step; use an isolated test environment or statement deltas.

The experiment is successful only if correctness and recovery pass and the
controlled measurements show a useful reduction in database cost without
unacceptable latency, starvation or failover regressions.

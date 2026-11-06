# Distributed Task Matching for Hub: Phase 1 + Phase 2

## Status and architectural contract

Implementation branch: `feat/distributed-task-matching-ha`, based on PR #18
(`perf/p95-adaptive-claim-completion`), not the slower PR #19 single-scan
experiment. The existing `/workers/tasks:claim` and SSE paths remain
available. The new worker long-poll path is explicitly opt-in.

- Hub remains REST/auth/management/control-plane; no workflow business code
  is loaded into Hub.
- Processors execute business workflow/activity code and never need direct
  PostgreSQL access.
- Matching is an ephemeral coordination layer. `hub_task`, workflow
  snapshots, task attempts/history, retries, leases and concurrency constraints
  remain durable and authoritative in PostgreSQL.
- No claim is acknowledged to the worker before the SQL lease is committed.
- A matching-owner lease does **not** confer execution exclusivity: the
  existing resource/FIFO and epoch-guarded task lease transactions do that.

## Phase 1: local demand-driven matching

`POST /api/hub/workers/tasks:poll` authenticates and accepts the existing
`HubClaimRequest`. It registers a bounded, in-memory waiting poller, attempts
one immediate durable claim to catch existing backlog, and then waits up to
20 seconds for a ready-work hint. PostgreSQL notification triggers, completion
signals and the existing reconciliation sweep wake the next waiting poller.
Successful claims signal another waiter to drain remaining backlog.

The worker opts in using `WFC_HUB_LONG_POLL_ENABLED=true`. Unlike the
adaptive claim/backoff loop, it can wait for work inside a single HTTP request;
it does not require SSE alongside long polling. On overload/failure, it retries
through the existing REST client; old `tasks:claim` remains the fallback for
workers serving multiple Hub namespaces.

The existing 2-second reconciliation sweep remains enabled for lost hints,
expired leases, delayed tasks and crash recovery. This is **not** a zero-query
dispatcher: first polls, DB lease verification, reconciliations and actual
claims still perform SQL.

## Phase 2: distributed matching, two Hub replicas

`wfc_scale.hub_matching_owner` records one owner per **Hub namespace**
(`hub_id`), with `owner_id`, `owner_url`, `epoch`, and `lease_until`.
An atomic PostgreSQL UPSERT elects or renews the owner. The currently owning
Gateway renews every 2 seconds with an 8-second expiration window; if it
crashes or loses its lease, another Gateway can acquire the namespace and
increments the ownership epoch. Stale owners cannot renew or claim a matching
namespace once their owner epoch is lost.

A worker uses one stable load-balanced REST endpoint. Non-owning Hub replicas
forward a long poll once to the elected owner over the internal HTTP network.
The URL MUST match `WFC_HUB_MATCHING_PEERS`; forwarding uses the same bearer
token and refuses a second hop. External access to the internal peer listener
must be restricted. The gateway returns 503 on a failed/stale forward, and the
worker retries through the load balancer. The unexpired owner is never stolen
merely to balance traffic.

### Scope limitation

This increment supports multiple active Hub **Gateway** replicas, and
**Hub-namespace-level matching ownership/failover**, not several simultaneous
matching owners for different task types **inside the same Hub namespace**.
Cross-partition forwarding of individual task types and dynamic per-namespace
task queue partition scaling are future work. For a single large Hub, one
matching owner may become a throughput bottleneck. The two-Gateway test is
an HA test, not proof of linear matching throughput scale-out.

## Run the two-Gateway lab

```bash
docker compose \
  -f scaling/observability/compose.yaml \
  -f scaling/observability/compose.matching-ha.yaml \
  --env-file scaling/observability/.env.example \
  up -d --build hub hub-2 hub-lb

# The load balancer is on localhost:5110. Worker containers have
# WFC_HUB_LONG_POLL_ENABLED=true and use http://hub-lb:5100.
```

The overlay launches `hub` and `hub-2` with separate owner identities,
both pointing to the same durable PostgreSQL, and NGINX fronts them. Both
Hub schedulers are enabled; timer correctness still relies on the existing
durable scheduler store.

Inspect the owner (bearer secret required):

```bash
curl -H "Authorization: Bearer <token>" \
  http://127.0.0.1:5110/api/hub/matching/owner/policy-benchmark
```

Shut down the elected owner container to validate takeover. Under a healthy
database, a surviving Hub can acquire ownership after the 8-second expiration.
A task that was already leased before a response was lost remains governed
by its independent 45-second task lease; ownership failover does not
instantaneously revoke the worker's activity execution lease.

## Verification and benchmarks

The PR runs:
- PostgreSQL integration checks for single-winner election, expiration,
  owner-epoch fencing and parallel owner acquisition.
- The existing distributed runtime, journal, and ordering tests.
- A dedicated two-Hub Compose + K6 run: 150 partition FIFO workflows with
  elected-owner crash during load, PostgreSQL ordering assertions, owner
  epoch takeover; then restart the Hub and run 100 keyed FIFO workflows.
- Report failure/retry count, workflow p95/p99, throughput, claim RPC count,
  completion HTTP p95/p99 and activity Schedule-to-Start.
- Compare against PR #18 under the same offered load and CPU/DB allocation.
  More CPU and more DB connections due to a second Hub must be reported,
  not silently interpreted as a matching performance gain.

### Failure and durability boundaries

- PostgreSQL remains a *single point of failure* in the lab. Two Hub
  gateways provide HTTP/matching process HA, **not database HA**. Production
  requires PostgreSQL HA and tested PITR/restore.
- If the elected owner fails mid-request, the client retries. A committed
  task lease with a lost HTTP response may be recoverable only at its
  lease timeout. Correct task idempotency and fencing remain required.
- Authorization is a development bearer secret. For production use mTLS,
  service identity, scoped Hub RBAC, and strict internal peer allowlisting.
- This is one leased matching partition per Hub namespace, not Temporal's
  multi-partition task-queue forwarding implementation.

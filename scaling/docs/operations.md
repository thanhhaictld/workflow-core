# Operations and recovery runbook

**Caution:** This is a development-stage runbook. Any manual replay can
repeat external provisioning actions.

## Signals and initial alert thresholds (proposals)

| Signal | How to obtain | Suggested warning |
|---|---|---|
| Ready backlog | `GetStatsAsync("workflow"/"event")` | Increasing continuously for 5 min |
| Oldest ready age | `WorkQueueStats.OldestReadyAge` | Over agreed scheduling SLO |
| Leased work | queue stats / `state=1` SQL | Leased count high despite low completion |
| Dead items | queue stats / `state=3` SQL | Any new dead letter in production |
| Errors | `workflowcore.work.failed` counter | Sustained increase by queue |
| Deferrals | `workflowcore.work.deferred` | Repeated contention with no progress |
| Execution duration | `workflowcore.work.duration` histogram | P95/P99 over workload budget |
| PostgreSQL | pg_stat_activity, lock waits, CPU/WAL | Saturation / idle-in-transaction |
| Replica health | process crash/OOM/CPU | Abrupt loss / restart loop |

The Meter is `WorkflowCore.Distributed`, version 1.0.0, and the
`ActivitySource` uses the same name. The branch **emits** telemetry but does
not configure an OpenTelemetry exporter, collector, Grafana dashboard or
alert rules. Install/configure them in the hosting application.

## Common diagnosis

### Workflow not starting

1. Confirm `wfc."Workflow"` contains the instance, status is Runnable
   and registered definition/version is deployed on every worker.
2. Find `wfc_scale.work_item` by `item_id = workflowId`; check queue, state,
   due time, epoch, attempts and last error.
3. Check work pump logs and PostgreSQL `workflow_gate` expiry.
4. Check for a stale work key relative to `workflow_gate.revision`.
5. Do not enqueue by hand unless a repair plan preserves revision invariants.

### Stuck leased row

Use read-only SQL:

```sql
SELECT id, queue_name, item_id, state, owner, epoch, attempts,
       lease_until, now() - lease_until AS expired_by
FROM wfc_scale.work_item
WHERE state = 1
ORDER BY lease_until LIMIT 100;
```

An expired lease can be reclaimed on a new claim if attempts remain below
`MaxAttempts`. Items with expired final attempts are moved to dead state
by the periodic idle-path `DeadLetterExhaustedAsync`; this sweep is not
guaranteed to run quickly during permanently busy queues.

If the row does not progress, check DB connectivity, worker health, pool
exhaustion and whether any worker can claim `queue_name`. Avoid resetting
owners without examining active external calls.

### Event published but WaitFor did not resume

- Inspect unprocessed `wfc_scale.signal_event` rows by name/key/effective_at.
- Check `wfc."Subscription"` for matching subscription and subscribe-as-of.
- Check `wfc_scale.signal_delivery` for the event/subscription pair.
- Check due/leased event work and any failed `workflow` wakeup.
- The engine's ordering check uses hard-coded `wfc` schema; other schemas
  are not supported end to end.
- Exercise event races in staging before attempting manual recovery.

### Dead-letter inspection

```sql
SELECT id, queue_name, item_id, attempts, last_error, updated_at
FROM wfc_scale.work_item
WHERE state = 3 ORDER BY updated_at DESC LIMIT 100;
```

**Operator replay:** `FencedWorkQueue.RequeueDeadAsync(workId)` is the
existing supported application method. It resets attempts, advances epoch
and returns the row to ready. Build an authenticated and audited operator
command around it before production. Review external side effects first.

## Recovery actions by failure

| Fault | Expected behavior | Check |
|---|---|---|
| Worker stops before commit | Work lease expires; item reclaimable | Work state and `lease_until` |
| Worker loses gate ownership | Conditional commit rejected | Epoch, revision and error logs |
| DB write failure | Transaction rollback; retry/expiry | DB error; no partial state + ACK |
| Event fan-out interrupted | Delivery markers prevent re-applying processed subscriber | `signal_delivery` and event work |
| PostgreSQL primary outage | Processing blocks and retries; no regional failover built in | Restore primary, reconcile backlog |
| Downstream API timeout | May have succeeded remotely; step may rerun | External idempotency/reconciliation |

## Maintenance cautions

- The code has no built-in retention job or auto-vacuum tuning. The new
  append-only `wfc_scale.execution_history` table increases write/WAL volume;
  monitor growth and archive/purge with a separately authorized role. See
  [Execution history](execution-history.md).
- Backup `wfc` and `wfc_scale` together at one consistent point in time.
- Never replay a completed or expired item through SQL alone; conditional ACK
  checks protect ownership.
- Use read-only DB credentials for diagnostic dashboards.
- Rotation, DLQ count limits, per-tenant quotas and audit trail are additional
  operational features to implement.

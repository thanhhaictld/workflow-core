# Hybrid OpenTelemetry workflow correlation

The **Next.js Hub waterfall** remains the authoritative full-workflow timeline,
built from PostgreSQL task leases and event history. **Grafana Tempo** provides
technical spans for bounded execution segments; it does not automatically
merge multiple traces connected by span links into one waterfall.

## Correlation contract

- Scheduling persists W3C `traceparent` and optional `tracestate` in
  `wfc_scale.hub_task`, within the existing enqueue/commit transaction.
- Claim returns this scheduling context, the task creation time and the
  current (fenced) attempt count to the remote worker.
- The worker uses the scheduling span as its remote parent for a first attempt
  younger than `WFC_HUB_TRACE_SEGMENT_MAX_AGE_SECONDS` (default 120).
- A retry or an older queued task starts a separate consumer trace with a
  **span link** back to the scheduling context. No root activity is kept open
  across waiting periods, DB leases or timer delays.
- Worker HTTP calls retain the current execution context through standard
  OTel HttpClient propagation. On successful fenced completion, the Hub stores
  the resulting completion request trace/span IDs in the *same transaction*
  that finalizes `hub_task_attempt`. The monitor exposes a deep link to Tempo.
  A missing trace ID means no telemetry was available, not that execution
  failed; a pre-completion or lost-lease attempt may legitimately have no ID.
- Trace/span IDs appear only as trace attributes, logs and persisted journal
  fields. **Never** use workflow/task/trace IDs as Prometheus labels.

## Deployment

Apply `scaling/sql/020_hub_trace_correlation.sql` after 019 (idempotent).
Provision both gateway and remote workers with the existing OpenTelemetry
tracer, ASP.NET Core and HttpClient instrumentation, exporting to Tempo.
For browser links set `NEXT_PUBLIC_GRAFANA_URL` to the external Grafana
URL (for local Compose, `http://localhost:3001`) and
`NEXT_PUBLIC_GRAFANA_TEMPO_UID` to the provisioned Tempo datasource UID.
If unset, the monitor renders the trace ID without an external link.

## Verification scenarios

1. First short activity: claim/scheduling and worker execution share a trace.
2. Long timer (>120s): worker creates a fresh trace linked to origin.
3. Retry with higher fencing epoch: fresh linked trace, no repeated body.
4. Parallel native activities: each attempt remains independently correlated.
5. After completion: trace IDs appear on Hub `/workflows/{id}/trace`
   and the Next.js attempt inspector; Grafana Explore can open the ID.
6. Without OTel exporter: workflow history and waterfall continue unaffected.

Do not claim end-to-end span completeness from persisted IDs alone:
sampling, retention, application crashes, or unavailable instrumentations can
leave gaps. The lease/history tables remain the durable source of truth.

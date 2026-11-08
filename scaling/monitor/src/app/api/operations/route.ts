import { NextRequest, NextResponse } from "next/server";
type PrometheusMetric = "sseConnections" | "hubClaimP95Seconds" |
  "workerExecutionP95Seconds" | "httpP95Seconds" | "timerLagP95Seconds" |
  "timerSchedulerErrors5m" | "workerInflight" | "taskClaimsPerSecond" |
  "workflowFailures5m" | "postgresCpuPercent" | "postgresDatabaseBytes" |
  "postgresDeadTaskRows" | "postgresCommitsPerSecond" | "postgresRollbacksPerSecond" |
  "taskSeqRowsPerSecond" | "taskSeqScansPerSecond" | "taskIndexRowsPerSecond" | "taskIndexScansPerSecond" |
  "workflowSeqRowsPerSecond" | "workflowSeqScansPerSecond" | "workflowIndexRowsPerSecond" | "workflowIndexScansPerSecond";

const promQueries: Record<PrometheusMetric, string> = {
  sseConnections: "sum(workflow_hub_sse_connections)",
  hubClaimP95Seconds: "histogram_quantile(0.95,sum by (le) (rate(workflow_hub_claim_duration_seconds_bucket[5m])))",
  workerExecutionP95Seconds: "histogram_quantile(0.95,sum by (le) (rate(workflow_hub_worker_execution_duration_seconds_bucket[5m])))",
  httpP95Seconds: 'histogram_quantile(0.95,sum by (le) (rate(http_server_request_duration_seconds_bucket{service_name="workflow-hub"}[5m])))',
  timerLagP95Seconds: "histogram_quantile(0.95,sum by (le) (rate(workflow_hub_timer_fire_lag_seconds_bucket[5m])))",
  timerSchedulerErrors5m: "sum(increase(workflow_hub_timer_scheduler_errors_total[5m]))",
  workerInflight: "sum(workflow_hub_worker_inflight)",
  taskClaimsPerSecond: "sum(rate(workflow_hub_tasks_claimed_total[5m]))",
  workflowFailures5m: "sum(increase(workflow_hub_workflows_failed_total[5m]))",
  postgresCpuPercent: '100 * sum(rate(namedprocess_namegroup_cpu_seconds_total{job="postgres-process",groupname="postgres"}[1m]))',
  postgresDatabaseBytes: "sum(postgresql_db_size_bytes)",
  postgresDeadTaskRows: 'sum(postgresql_rows{postgresql_table_name="wfc_scale.hub_task",state="dead"})',
  postgresCommitsPerSecond: "sum(rate(postgresql_commits_total[1m]))",
  postgresRollbacksPerSecond: "sum(rate(postgresql_rollbacks_total[1m]))",
  taskSeqRowsPerSecond: 'sum(rate(workflow_postgres_table_seq_rows_read_total{schemaname="wfc_scale",relname="hub_task"}[1m]))',
  taskSeqScansPerSecond: 'sum(rate(workflow_postgres_table_seq_scans_total{schemaname="wfc_scale",relname="hub_task"}[1m]))',
  taskIndexRowsPerSecond: 'sum(rate(workflow_postgres_table_index_rows_fetched_total{schemaname="wfc_scale",relname="hub_task"}[1m]))',
  taskIndexScansPerSecond: 'sum(rate(workflow_postgres_table_index_scans_total{schemaname="wfc_scale",relname="hub_task"}[1m]))',
  workflowSeqRowsPerSecond: 'sum(rate(workflow_postgres_table_seq_rows_read_total{schemaname="wfc_scale",relname="hub_workflow"}[1m]))',
  workflowSeqScansPerSecond: 'sum(rate(workflow_postgres_table_seq_scans_total{schemaname="wfc_scale",relname="hub_workflow"}[1m]))',
  workflowIndexRowsPerSecond: 'sum(rate(workflow_postgres_table_index_rows_fetched_total{schemaname="wfc_scale",relname="hub_workflow"}[1m]))',
  workflowIndexScansPerSecond: 'sum(rate(workflow_postgres_table_index_scans_total{schemaname="wfc_scale",relname="hub_workflow"}[1m]))'
};

async function processMetrics() {
  const endpoint = process.env.WORKFLOW_PROMETHEUS_URL;
  if (!endpoint) return { available: false, scope: "global", values: {} };
  const results = await Promise.all(Object.entries(promQueries).map(async ([key, expression]) => {
    try {
      const url = new URL(endpoint.replace(/\/$/, "") + "/api/v1/query");
      url.searchParams.set("query", expression);
      const response = await fetch(url.toString(), { cache: "no-store", signal: AbortSignal.timeout(3000) });
      if (!response.ok) return [key, null, false] as const;
      const body = await response.json();
      if (body.status !== "success") return [key, null, false] as const;
      const raw = body.data?.result?.[0]?.value?.[1];
      const value = typeof raw === "string" ? Number(raw) : NaN;
      return [key, Number.isFinite(value) ? value : null, true] as const;
    } catch { return [key, null, false] as const; }
  }));
  return {
    available: results.some(x => x[2]),
    scope: "global",
    values: Object.fromEntries(results.map(([key, value]) => [key, value]))
  };
}

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

// Server-only token; browser receives aggregates, not the Hub bearer secret.
// Same authentication boundary as /api/hub-overview; do not publish this
// development monitor without gateway auth, scoped access and TLS.
export async function GET(request: NextRequest) {
  const base = process.env.WORKFLOW_HUB_URL;
  const token = process.env.WORKFLOW_HUB_TOKEN;
  if (!base || !token) return NextResponse.json(
    { error: "Hub connection is not configured on the monitor server" },
    { status: 503, headers: { "Cache-Control": "no-store" } }
  );
  const hub = request.nextUrl.searchParams.get("hub") || "";
  const windowMinutes = request.nextUrl.searchParams.get("windowMinutes") || "60";
  if (hub.length > 80 || !["60", "360", "1440"].includes(windowMinutes))
    return NextResponse.json({ error: "Invalid monitor filter" }, { status: 400 });
  const query = new URLSearchParams({ windowMinutes });
  if (hub) query.set("hub", hub);
  try {
    const upstream = await fetch(base.replace(/\/$/, "") + "/api/hub/operations?" + query.toString(), {
      headers: { Authorization: "Bearer " + token, Accept: "application/json" },
      cache: "no-store",
      signal: AbortSignal.timeout(15000)
    });
    if (!upstream.ok) return NextResponse.json(
      { error: "Hub monitoring gateway returned HTTP " + upstream.status },
      { status: upstream.status >= 500 ? 502 : upstream.status,
        headers: { "Cache-Control": "no-store" } }
    );
    return NextResponse.json({ ...(await upstream.json()), processMetrics: await processMetrics() }, { headers: { "Cache-Control": "no-store" } });
  } catch {
    return NextResponse.json(
      { error: "Hub monitoring gateway is unavailable" },
      { status: 502, headers: { "Cache-Control": "no-store" } }
    );
  }
}

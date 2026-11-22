import { NextResponse } from "next/server";

export function hubMonitorQueriesEnabled(): boolean {
  const value = process.env.WORKFLOW_HUB_QUERIES_ENABLED?.trim().toLowerCase();
  return value !== "false" && value !== "0";
}

export function rejectDisabledHubMonitorQueries(): NextResponse | null {
  if (hubMonitorQueriesEnabled()) return null;
  return NextResponse.json({ error: "Hub monitor queries are disabled. Use Operations for Prometheus metrics." },
    { status: 503, headers: { "Cache-Control": "no-store" } });
}

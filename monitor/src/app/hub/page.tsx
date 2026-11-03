"use client";

import { useEffect, useState } from "react";
import { Activity, Database, Gauge, RefreshCw, Server, ShieldCheck } from "lucide-react";

type Diagnostics = {
  databaseConnections: number;
  activeDbConnections: number;
  readyTasks: number;
  leasedTasks: number;
  deadTasks: number;
};
type Run = {
  runId: string;
  total: number;
  ready: number;
  running: number;
  completed: number;
  failed: number;
  lastCreatedAt: string;
};
type Overview = {
  diagnostics: Diagnostics;
  runs: Run[];
  observedAt: string;
};

const number = (value: number) => value.toLocaleString();

export default function HubRunsPage() {
  const [data, setData] = useState<Overview | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let active = true;
    const controller = new AbortController();
    async function refresh() {
      try {
        const response = await fetch("/api/hub-overview", {
          cache: "no-store",
          signal: controller.signal,
        });
        const value = await response.json();
        if (!response.ok) throw new Error(value.error || "Hub returned HTTP " + response.status);
        if (active) {
          setData(value as Overview);
          setError(null);
        }
      } catch (cause) {
        if (active && !controller.signal.aborted) {
          setError(cause instanceof Error ? cause.message : "Hub is not available");
        }
      } finally {
        if (active) setLoading(false);
      }
    }

    void refresh();
    const timer = setInterval(() => { void refresh(); }, 5000);
    return () => {
      active = false;
      clearInterval(timer);
      controller.abort();
    };
  }, []);

  const stats = data?.diagnostics;
  const cards = [
    { label: "PostgreSQL connections", value: stats?.databaseConnections, icon: Database },
    { label: "Active DB connections", value: stats?.activeDbConnections, icon: Server },
    { label: "Ready activities", value: stats?.readyTasks, icon: Activity },
    { label: "Leased activities", value: stats?.leasedTasks, icon: Gauge },
    { label: "Dead-letter activities", value: stats?.deadTasks, icon: ShieldCheck },
  ];

  return <div className="space-y-7">
    <div className="flex flex-wrap items-start justify-between gap-4">
      <div>
        <p className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">Remote execution</p>
        <h1 className="mt-2 text-2xl font-bold tracking-tight">Workflow Hub runs</h1>
        <p className="mt-2 max-w-2xl text-sm text-muted-foreground">
          Live REST/SSE Hub activity-chain executions. Existing Workflow Core
          execution history remains available under Workflow runs.
        </p>
      </div>
      <div className="flex items-center gap-2 rounded-lg border border-border bg-card px-3 py-2 text-xs text-muted-foreground">
        <RefreshCw size={14} className={loading ? "animate-spin" : ""}/>
        Refresh every 5 seconds
      </div>
    </div>

    {error && <div role="alert" className="rounded-lg border border-red-500/30 bg-red-500/5 px-4 py-3 text-sm text-red-600">
      {error}
    </div>}

    <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
      {cards.map(({ label, value, icon: Icon }) =>
        <div key={label} className="rounded-xl border border-border bg-card p-4">
          <div className="flex items-center justify-between gap-2 text-xs text-muted-foreground">
            {label} <Icon size={15}/>
          </div>
          <p className="mt-4 text-2xl font-semibold tabular-nums">{value === undefined ? "—" : number(value)}</p>
        </div>,
      )}
    </div>

    <div className="overflow-hidden rounded-xl border border-border bg-card">
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-border px-5 py-4">
        <div>
          <h2 className="font-semibold">Recent Hub workflow runs</h2>
          <p className="text-xs text-muted-foreground">Grouped by run ID, newest first · up to 20 runs</p>
        </div>
        <span className="text-xs text-muted-foreground">
          {data?.observedAt ? "Updated " + new Date(data.observedAt).toLocaleTimeString() : "Connecting…"}
        </span>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full min-w-[800px] text-left text-sm">
          <thead className="border-b border-border bg-muted/40 text-xs text-muted-foreground">
            <tr>
              <th className="px-5 py-3 font-medium">Run ID</th>
              <th className="px-3 py-3 text-right font-medium">Total</th>
              <th className="px-3 py-3 text-right font-medium">Ready</th>
              <th className="px-3 py-3 text-right font-medium">Running</th>
              <th className="px-3 py-3 text-right font-medium">Complete</th>
              <th className="px-3 py-3 text-right font-medium">Failed</th>
              <th className="px-5 py-3 font-medium">Progress</th>
            </tr>
          </thead>
          <tbody>
            {data?.runs.map(run => {
              const percent = run.total > 0 ? Math.round((run.completed / run.total) * 100) : 0;
              return <tr key={run.runId} className="border-b border-border last:border-0">
                <td className="max-w-64 truncate px-5 py-3 font-mono text-xs" title={run.runId}>{run.runId}</td>
                <td className="px-3 py-3 text-right tabular-nums">{number(run.total)}</td>
                <td className="px-3 py-3 text-right tabular-nums">{number(run.ready)}</td>
                <td className="px-3 py-3 text-right tabular-nums">{number(run.running)}</td>
                <td className="px-3 py-3 text-right tabular-nums">{number(run.completed)}</td>
                <td className="px-3 py-3 text-right tabular-nums">{number(run.failed)}</td>
                <td className="px-5 py-3">
                  <div className="flex items-center gap-3">
                    <div className="h-2 w-24 overflow-hidden rounded-full bg-muted">
                      <div className="h-full rounded-full bg-primary" style={{ width: percent + "%" }}/>
                    </div>
                    <span className="text-xs tabular-nums text-muted-foreground">{percent}%</span>
                  </div>
                </td>
              </tr>;
            })}
            {data?.runs.length === 0 && <tr>
              <td colSpan={7} className="px-5 py-12 text-center text-sm text-muted-foreground">
                No Hub workload runs yet. Start the k6 stress test to populate this view.
              </td>
            </tr>}
            {!data && <tr><td colSpan={7} className="px-5 py-12 text-center text-sm text-muted-foreground">
              {loading ? "Connecting to Workflow Hub…" : "No run data available."}
            </td></tr>}
          </tbody>
        </table>
      </div>
    </div>
    <p className="text-xs text-muted-foreground">
      For memory, throughput, p95/p99 latency, traces and runtime metrics, use the
      provisioned Workflow Hub dashboard in Grafana on port 3001.
      These are Hub-managed activity-chain runs, separate from legacy IWorkflow runs.
    </p>
  </div>;
}

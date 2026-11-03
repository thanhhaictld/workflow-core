"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useMemo, useState } from "react";
import {
  Activity, AlertTriangle, BarChart3, Clock3, GitBranch, History, Layers3,
  ListChecks, LockKeyhole, Network, RefreshCcw, Server
} from "lucide-react";
import { Badge, Button, Card, CardHeader, EmptyState, Select, cn } from "./ui";
import type { OperationsSnapshot, OperationsWindow } from "@/lib/operations";
import { formatAge, formatCount, partitionSkew } from "@/lib/operations";

const sections = [
  { id: "overview", title: "Overview", icon: BarChart3 },
  { id: "activities", title: "Activities", icon: ListChecks },
  { id: "partitions", title: "Partitions", icon: Layers3 },
  { id: "hub", title: "Hub & scheduler", icon: Network },
  { id: "concurrency", title: "Concurrency", icon: LockKeyhole },
  { id: "workers", title: "Workers", icon: Server },
  { id: "history", title: "History", icon: History }
] as const;

function Kpi({ label, value, caption, critical = false }: {
  label: string; value: string | number; caption: string; critical?: boolean;
}) {
  return <Card className="min-w-0 p-4">
    <p className="text-xs font-medium text-muted-foreground">{label}</p>
    <p className={cn("mt-2 truncate text-2xl font-bold tabular-nums", critical && "text-destructive")}>
      {typeof value === "number" ? formatCount(value) : value}
    </p>
    <p className="mt-1 text-xs text-muted-foreground">{caption}</p>
  </Card>;
}
function ChartBars({ title, description, points, property, isNullable = false }: {
  title: string; description: string; points: OperationsSnapshot["trends"];
  property: "runnable" | "ready" | "leased" | "started" | "completed" | "failed";
  isNullable?: boolean;
}) {
  const values = points.map(p => p[property]);
  const present = values.filter((n): n is number => n !== null && Number.isFinite(n));
  const max = Math.max(1, ...present);
  return <Card className="min-w-0">
    <CardHeader title={title} description={description} />
    {present.length === 0 ? <EmptyState title="No persisted samples"
      description="Samples appear after the Hub runs with migration 011 applied; missing samples are not shown as zero." /> :
      <div className="px-5 pb-5 pt-4">
        <div className="flex h-36 items-end gap-[3px]" role="img" aria-label={title + " over selected interval"}>
          {points.map((point, i) => {
            const value = values[i];
            return <div key={point.bucket} className="flex h-full min-w-0 flex-1 items-end"
              title={new Date(point.bucket).toLocaleString() + ": " + (value === null ? "no sample" : formatCount(value))}>
              {value === null
                ? <div className="h-[2px] w-full rounded bg-muted-foreground/20" />
                : <div className={cn("w-full rounded-t-sm", isNullable ? "bg-blue-500/80" : "bg-emerald-600/80")}
                    style={{ height: Math.max(2, 100 * value / max) + "%" }} />}
            </div>;
          })}
        </div>
        <div className="mt-2 flex justify-between text-[11px] text-muted-foreground">
          <span>{new Date(points[0]?.bucket ?? 0).toLocaleTimeString()}</span>
          <span>Peak {formatCount(max)}</span>
          <span>{new Date(points[points.length - 1]?.bucket ?? 0).toLocaleTimeString()}</span>
        </div>
      </div>}
  </Card>;
}
const th = "px-4 py-3 text-left text-[11px] font-semibold uppercase tracking-wide text-muted-foreground";
const td = "border-t border-border px-4 py-3 text-xs tabular-nums";
function TableFrame({ title, description, children }: {
  title: string; description: string; children: React.ReactNode;
}) {
  return <Card className="overflow-hidden">
    <CardHeader title={title} description={description} />
    <div className="overflow-x-auto"><table className="w-full min-w-[690px]">{children}</table></div>
  </Card>;
}
function PartitionRows({ data }: { data: OperationsSnapshot }) {
  const max = Math.max(1, ...data.partitions.map(p => p.ready));
  return <TableFrame title="Partition health" description="Ready work, active leases and oldest queue age by Hub partition. Ranked by ready backlog.">
    <thead><tr>{["Hub / Partition", "Active workflows", "Ready", "Leased", "Delayed", "Dead", "Oldest ready", "Relative load"].map(x => <th key={x} className={th}>{x}</th>)}</tr></thead>
    <tbody>{data.partitions.map(p => <tr key={p.hubId + ":" + p.partitionId} className="hover:bg-muted/30">
      <td className={td}><span className="font-semibold">{p.hubId}</span> / P{p.partitionId}</td>
      <td className={td}>{formatCount(p.activeWorkflows)}</td>
      <td className={td}>{formatCount(p.ready)}</td>
      <td className={td}>{formatCount(p.leased)}</td>
      <td className={td}>{formatCount(p.delayed)}</td>
      <td className={td}>{formatCount(p.dead)}</td>
      <td className={td}>{formatAge(p.oldestReadyAgeSeconds)}</td>
      <td className={td}><div className="h-2 min-w-20 overflow-hidden rounded-full bg-muted">
        <div className="h-full rounded-full bg-amber-500" style={{ width: (100 * p.ready / max) + "%" }} />
      </div></td>
    </tr>)}</tbody>
  </TableFrame>;
}
function ActivityRows({ data }: { data: OperationsSnapshot }) {
  return <TableFrame title="Activity backlog by type" description="Ready excludes delayed tasks and tasks that exhausted attempts. Processor tasks are labelled __workflow__.">
    <thead><tr>{["Hub / Activity", "Ready", "Executing leases", "Delayed", "Retry due", "Dead letter", "Oldest ready"].map(x => <th key={x} className={th}>{x}</th>)}</tr></thead>
    <tbody>{data.activities.map(a => <tr key={a.hubId + ":" + a.activityType} className="hover:bg-muted/30">
      <td className={td}><div className="font-semibold">{a.activityType}</div><div className="mt-1 text-muted-foreground">{a.hubId}</div></td>
      <td className={td}>{formatCount(a.ready)}</td><td className={td}>{formatCount(a.leased)}</td>
      <td className={td}>{formatCount(a.delayed)}</td><td className={td}>{formatCount(a.retryDue)}</td>
      <td className={td}>{formatCount(a.dead)}</td><td className={td}>{formatAge(a.oldestReadyAgeSeconds)}</td>
    </tr>)}</tbody>
  </TableFrame>;
}
function PolicyRows({ data }: { data: OperationsSnapshot }) {
  return <TableFrame title="Resource concurrency" description="Shared-resource capacity and active permits, aggregated by policy class; resource identifiers are never exposed.">
    <thead><tr>{["Policy class", "Resources", "Total slots", "In use", "Contending requests", "Saturated resources", "Slot occupancy"].map(x => <th key={x} className={th}>{x}</th>)}</tr></thead>
    <tbody>{data.policies.map(p => <tr key={p.name} className="hover:bg-muted/30">
      <td className={td + " font-medium"}>{p.name}</td>
      <td className={td}>{formatCount(p.resources)}</td><td className={td}>{formatCount(p.capacity)}</td>
      <td className={td}>{formatCount(p.active)}</td><td className={td}>{formatCount(p.queued)}</td>
      <td className={td}>{formatCount(p.saturated)}</td>
      <td className={td}><div className="h-2 min-w-24 overflow-hidden rounded-full bg-muted">
        <div className={cn("h-full rounded-full", p.active >= p.capacity ? "bg-red-500" : "bg-emerald-600")}
          style={{ width: Math.min(100, p.capacity ? 100 * p.active / p.capacity : 0) + "%" }} />
      </div></td>
    </tr>)}</tbody>
  </TableFrame>;
}
function WorkerRows({ data }: { data: OperationsSnapshot }) {
  return <TableFrame title="Active lease owners" description="Derived from unexpired PostgreSQL task leases. Idle workers and processor registrations are not observable here.">
    <thead><tr>{["Lease owner", "Tasks in flight", "Partitions touched", "Last claim / update"].map(x => <th key={x} className={th}>{x}</th>)}</tr></thead>
    <tbody>{data.workers.map(w => <tr key={w.workerId} className="hover:bg-muted/30">
      <td className={td + " font-mono"}>{w.workerId}</td>
      <td className={td}>{formatCount(w.leasedTasks)}</td>
      <td className={td}>{formatCount(w.partitions)}</td>
      <td className={td}>{w.lastClaimAt ? new Date(w.lastClaimAt).toLocaleString() : "N/A"}</td>
    </tr>)}</tbody>
  </TableFrame>;
}

export function OperationsDashboard() {
  const pathname = usePathname();
  const suffix = pathname.split("/")[2] || "overview";
  const section = sections.some(s => s.id === suffix) ? suffix : "overview";
  const [hub, setHub] = useState("");
  const [windowMinutes, setWindowMinutes] = useState<OperationsWindow>(60);
  const [revision, setRevision] = useState(0);
  const [data, setData] = useState<OperationsSnapshot | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let disposed = false;
    let active = false;
    async function load() {
      if (active) return;
      active = true;
      try {
        const query = new URLSearchParams({ windowMinutes: String(windowMinutes) });
        if (hub) query.set("hub", hub);
        const response = await fetch("/api/operations?" + query.toString(), { cache: "no-store" });
        const payload = await response.json();
        if (!response.ok) throw new Error(payload.error || "Operations API error");
        if (!disposed) { setData(payload as OperationsSnapshot); setError(null); }
      } catch (e) {
        if (!disposed) { setError(e instanceof Error ? e.message : "Operations API failed"); setData(null); }
      } finally {
        active = false;
        if (!disposed) setLoading(false);
      }
    }
    setLoading(true);
    void load();
    const timer = window.setInterval(() => void load(), 15000);
    return () => { disposed = true; window.clearInterval(timer); };
  }, [hub, windowMinutes, revision]);

  const hubs = useMemo(() => Array.from(new Set([
    ...(data?.partitions.map(p => p.hubId) ?? []),
    ...(data?.activities.map(a => a.hubId) ?? []),
    ...(hub ? [hub] : [])
  ])).sort(), [data, hub]);
  const skew = data ? partitionSkew(data.partitions.map(p => p.ready)) : null;
  const summary = data?.workflows;
  const tasks = data?.tasks;

  return <div className="space-y-6">
    <div className="flex flex-wrap items-start justify-between gap-4">
      <div>
        <div className="flex items-center gap-2"><h1 className="text-2xl font-bold tracking-tight">Workflow operations</h1>
          <Badge tone="blue">Hub runtime</Badge></div>
        <p className="mt-1 text-sm text-muted-foreground">
          Live PostgreSQL state · durable backlog history · auto-refresh every 15 seconds
        </p>
      </div>
      <div className="flex flex-wrap items-center gap-2">
        <Select aria-label="Select business Hub" value={hub} onChange={e => setHub(e.target.value)}>
          <option value="">All hubs</option>{hubs.map(h => <option key={h} value={h}>{h}</option>)}
        </Select>
        <Select aria-label="Select time range" value={windowMinutes}
          onChange={e => setWindowMinutes(Number(e.target.value) as OperationsWindow)}>
          <option value={60}>Last hour</option><option value={360}>Last 6 hours</option>
          <option value={1440}>Last 24 hours</option>
        </Select>
        <Button size="sm" onClick={() => setRevision(v => v + 1)}>
          <RefreshCcw size={14} /> Refresh
        </Button>
      </div>
    </div>
    <div className="flex flex-wrap gap-1 border-b border-border pb-2">
      {sections.map(s => {
        const Icon = s.icon;
        return <Link key={s.id} href={"/operations" + (s.id === "overview" ? "" : "/" + s.id)}
          className={cn("flex items-center gap-2 rounded-lg px-3 py-2 text-xs font-medium",
            section === s.id ? "bg-primary/10 text-foreground" : "text-muted-foreground hover:bg-muted")}>
          <Icon size={15} />{s.title}</Link>;
      })}
    </div>
    {error && <Card className="border-destructive/40 p-5" role="alert">
      <div className="flex items-center gap-2 font-semibold text-destructive"><AlertTriangle size={17} /> Monitoring unavailable</div>
      <p className="mt-2 text-sm text-muted-foreground">{error}</p>
      <p className="mt-1 text-xs text-muted-foreground">Configure WORKFLOW_HUB_URL and WORKFLOW_HUB_TOKEN; apply migration 011. No fake metrics are displayed.</p>
    </Card>}
    {!data && !error && <Card className="p-8 text-sm text-muted-foreground">{loading ? "Reading Hub metrics…" : "No metrics returned"}</Card>}
    {data && <>
      <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-muted-foreground">
        <span>Sampled {new Date(data.observedAt).toLocaleString()} · {hub || "All hubs"}</span>
        <span>{data.latency.queueSamples} first-claim latency samples in last hour</span>
      </div>
      {section === "overview" && <>
        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          <Kpi label="Active workflow instances" value={summary!.active} caption="Ready + running; may be waiting" />
          <Kpi label="Runnable workflow backlog" value={tasks!.runnableWorkflows} caption="Distinct workflows with eligible task" />
          <Kpi label="Activity ready backlog" value={tasks!.ready} caption="Due, unclaimed task rows" />
          <Kpi label="Capacity / FIFO blocked" value={data.contention.blockedReady} caption="Subset of ready tasks; sampled" critical={data.contention.blockedReady > 0} />
          <Kpi label="Activity leases in flight" value={tasks!.leased} caption="Unexpired owned task leases" />
          <Kpi label="Oldest ready age" value={formatAge(tasks!.oldestReadyAgeSeconds)} caption="Due timestamp → now" />
          <Kpi label="Queue wait P95" value={formatAge(data.latency.queueP95Seconds)} caption="First claim only, 1h" />
          <Kpi label="Completed / min" value={formatCount(summary!.completedPerMinute)} caption="Last 5m average" />
        </div>
        <div className="grid gap-4 lg:grid-cols-3">
          <ChartBars title="Runnable workflows" description="Distinct workflow IDs; persisted minute samples"
            points={data.trends} property="runnable" isNullable />
          <ChartBars title="Ready activities" description="Due and unclaimed; persisted minute samples"
            points={data.trends} property="ready" isNullable />
          <ChartBars title="Workflow completions" description="Completed in each time bucket"
            points={data.trends} property="completed" />
        </div>
        <div className="grid gap-3 sm:grid-cols-3">
          <Kpi label="Waiting workflows" value={summary!.waiting} caption="Waiting on external condition" />
          <Kpi label="Scheduled workflows" value={summary!.scheduled} caption="Pending future timer (may overlap waiting)" />
          <Kpi label="Dead-letter activities" value={tasks!.dead} caption="All-time state; manual intervention" critical={tasks!.dead > 0} />
        </div>
        <PartitionRows data={{ ...data, partitions: data.partitions.slice(0, 8) }} />
      </>}
      {section === "activities" && <>
        <div className="grid gap-3 sm:grid-cols-4">
          <Kpi label="Ready" value={tasks!.ready} caption="Due now" />
          <Kpi label="Leased" value={tasks!.leased} caption="In flight" />
          <Kpi label="Retry due" value={tasks!.retryDue} caption="Retries ready now" />
          <Kpi label="Delayed" value={tasks!.delayed} caption="Future due time" />
        </div>
        <ActivityRows data={data} />
      </>}
      {section === "partitions" && <>
        <div className="grid gap-3 sm:grid-cols-3">
          <Kpi label="Partitions observed" value={data.partitions.length} caption="Limited to 200 busiest partitions" />
          <Kpi label="Ready backlog skew" value={skew === null ? "N/A" : formatCount(skew) + "×"} caption="Max ready / mean ready across returned partitions" />
          <Kpi label="Reclaimable leases" value={tasks!.reclaimable} caption="Expired and below retry limit" />
        </div>
        <PartitionRows data={data} />
        <p className="text-xs text-muted-foreground">Relative load compares ready tasks only. Partition ownership, rebalance and configured worker slots require additional lease/worker registration instrumentation.</p>
      </>}
      {section === "hub" && <>
        <div className="grid gap-3 sm:grid-cols-3">
          <Kpi label="Pending timers" value={data.timers.pending} caption="Durable timers not yet fired" />
          <Kpi label="Overdue timers" value={data.timers.due} caption="Pending with due time passed" critical={data.timers.due > 0} />
          <Kpi label="Timers fired (24h)" value={data.timers.fired24h} caption="Durable scheduler completions" />
          <Kpi label="Leased tasks" value={tasks!.leased} caption="Hub queue leases" />
          <Kpi label="Dead letters" value={tasks!.dead} caption="Exhausted tasks" />
          <Kpi label="Completed workflows (24h)" value={summary!.completed24h} caption="Hub-managed workflow completions" />
        </div>
        <div className="grid gap-4 lg:grid-cols-2">
          <ChartBars title="Activity completion events" description="Terminal tasks per bucket, based on updated_at"
            points={data.trends} property="completed" />
          <ChartBars title="Active leases" description="Persisted queue samples" points={data.trends} property="leased" isNullable />
        </div>
        <Card className="p-5">
          <p className="font-semibold">Gateway delivery and SSE telemetry</p>
          <p className="mt-2 text-sm text-muted-foreground">SSE notification counts, live SSE connections, dispatch claim latency, and process metrics are already emitted through the WorkflowCore.Hub OpenTelemetry meter. These process-local measures are not inferred from durable task state. View the existing Grafana Workflow Hub runtime dashboard for those series.</p>
        </Card>
      </>}
      {section === "concurrency" && <>
        <div className="grid gap-3 sm:grid-cols-3">
          <Kpi label="Capacity / FIFO blocked" value={data.contention.blockedReady} caption="Unique currently ready tasks" />
          <Kpi label="Policy classes" value={data.policies.length} caption="Aggregated, no raw resource keys" />
          <Kpi label="Configured resources" value={data.policies.reduce((n, p) => n + p.resources, 0)} caption="Resources referenced by tasks" />
        </div>
        <PolicyRows data={data} />
        <p className="text-xs text-muted-foreground">A task can hold several resource constraints. Class queue totals may count the same task more than once; use the distinct blocked count for the system-wide estimate.</p>
      </>}
      {section === "workers" && <>
        <div className="grid gap-3 sm:grid-cols-3">
          <Kpi label="Active lease owners" value={data.workers.length} caption="Only workers with in-flight tasks" />
          <Kpi label="Tasks in flight" value={tasks!.leased} caption="Active leases across the Hub" />
          <Kpi label="Lease duration P95" value={formatAge(data.latency.leaseP95Seconds)} caption="Claim-to-finish span; not CPU time" />
        </div>
        <WorkerRows data={data} />
      </>}
      {section === "history" && <>
        <div className="grid gap-3 sm:grid-cols-3">
          <Kpi label="Completed (24h)" value={summary!.completed24h} caption="Terminal successful workflow state" />
          <Kpi label="Failed (24h)" value={summary!.failed24h} caption="Terminal failed workflow state" />
          <Kpi label="Dead letter total" value={tasks!.dead} caption="Currently dead-letter task rows" />
        </div>
        <div className="grid gap-4 lg:grid-cols-2">
          <ChartBars title="Workflows started" description="Created per bucket" points={data.trends} property="started" />
          <ChartBars title="Workflows failed" description="Terminal failures per bucket" points={data.trends} property="failed" />
        </div>
        <Card className="p-5">
          <div className="flex items-center gap-3"><GitBranch size={20} /><p className="font-semibold">Durable execution history</p></div>
          <p className="mt-2 text-sm text-muted-foreground">Open the workflow run browser for per-instance activity attempts, retries, timeline, state transitions and persisted errors.</p>
          <Link href="/workflows" className="mt-4 inline-block rounded-lg border border-border px-3 py-2 text-sm font-semibold hover:bg-muted">Browse workflow runs</Link>
        </Card>
      </>}
    </>}
  </div>;
}
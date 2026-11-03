"use client";

import Link from "next/link";
import { useCallback, useEffect, useState } from "react";
import { Activity, AlertTriangle, ArrowRight, CheckCircle2, Clock3, GitBranch, LoaderCircle, RefreshCw, Search, Workflow } from "lucide-react";
import { durationMs, formatDate, formatDuration, shortId } from "@/lib/format";
import type { WorkflowItem, WorkflowOverview, WorkflowPage } from "@/lib/types";
import { Badge, Button, Card, CardHeader, EmptyState, Input, Select, StatusBadge, buttonClasses } from "./ui";
import { DemoOnboarding } from "./demo-onboarding";

const PAGE_SIZE = 20;
const statusOptions = ["All", "Runnable", "Ready", "Running", "Complete", "Suspended", "Failed", "Terminated"];
type UnifiedPage = WorkflowPage & {
  overview: WorkflowOverview;
  hubs: string[];
  warnings: string[];
};

function Metric({ icon, label, value, hint, tone }: {
  icon: React.ReactNode; label: string; value: number | undefined; hint: string;
  tone: "blue" | "green" | "red" | "slate";
}) {
  const tones = {
    blue: "bg-blue-500/10 text-blue-600 dark:text-blue-400",
    green: "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400",
    red: "bg-rose-500/10 text-rose-600 dark:text-rose-400",
    slate: "bg-slate-500/10 text-slate-600 dark:text-slate-300"
  };
  return <Card className="p-5">
    <div className="flex items-center justify-between gap-3">
      <div className="text-xs font-medium text-muted-foreground">{label}</div>
      <div className={`grid size-9 place-items-center rounded-lg ${tones[tone]}`}>{icon}</div>
    </div>
    <div className="mt-2 text-3xl font-semibold tabular-nums tracking-tight">{value === undefined ? "—" : value.toLocaleString()}</div>
    <div className="mt-2 text-xs text-muted-foreground">{hint}</div>
  </Card>;
}

function WorkflowRow({ workflow }: { workflow: WorkflowItem }) {
  const path = `/workflows/${workflow.id}` +
    (workflow.source === "hub" ? "?source=hub" : "");
  const duration = durationMs(workflow.createdAt, workflow.completedAt);
  const age = workflow.completedAt ? duration : durationMs(workflow.createdAt, new Date().toISOString());
  return <tr className="border-t border-border/75 transition-colors hover:bg-muted/40">
    <td className="px-5 py-4">
      <Link href={path} className="group block max-w-[350px]">
        <span className="flex items-center gap-2 font-semibold text-foreground group-hover:text-blue-600 dark:group-hover:text-blue-400">
          <span className="truncate">{workflow.definitionId}</span><ArrowRight className="size-3.5 shrink-0 opacity-0 transition-opacity group-hover:opacity-100" />
        </span>
        <span className="mt-1 block font-mono text-[11px] text-muted-foreground">{shortId(workflow.id)} · v{workflow.version}</span>
      </Link>
    </td>
    <td className="max-w-[220px] truncate px-5 py-4 text-xs text-muted-foreground">{workflow.reference || "—"}</td>
    <td className="px-5 py-4">
      <Badge tone={workflow.source === "hub" ? "blue" : "slate"}>
        {workflow.source === "hub" ? workflow.hubId || "Hub" : "Legacy"}
      </Badge>
    </td>
    <td className="px-5 py-4"><StatusBadge status={workflow.status} /></td>
    <td className="whitespace-nowrap px-5 py-4 text-xs text-muted-foreground">{formatDate(workflow.createdAt)}</td>
    <td className="whitespace-nowrap px-5 py-4 text-xs tabular-nums text-muted-foreground">{formatDuration(age)}</td>
    <td className="px-5 py-4">
      {workflow.errorCount > 0 ? <Badge tone="red"><AlertTriangle size={12} /> {workflow.errorCount}</Badge>
        : <span className="text-xs text-muted-foreground">—</span>}
    </td>
    <td className="px-5 py-4">
      <Link aria-label={`Review execution trace for ${workflow.definitionId}`} href={`/workflows/${workflow.id}`}
        className={buttonClasses("outline", "sm")}>Trace <ArrowRight size={13} /></Link>
    </td>
  </tr>;
}

export function WorkflowList() {
  const [overview, setOverview] = useState<WorkflowOverview | null>(null);
  const [list, setList] = useState<WorkflowPage | null>(null);
  const [search, setSearch] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const [status, setStatus] = useState("All");
  const [hub, setHub] = useState("all");
  const [hubs, setHubs] = useState<string[]>([]);
  const [warnings, setWarnings] = useState<string[]>([]);
  const [page, setPage] = useState(1);
  const [refreshSeq, setRefreshSeq] = useState(0);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<string | null>(null);

  useEffect(() => {
    const selection = new URLSearchParams(window.location.search).get("hub");
    if (selection && /^[a-zA-Z0-9_.:-]{1,80}$/.test(selection)) setHub(selection);
  }, []);

  function changeHub(value: string) {
    setHub(value);
    setPage(1);
    const url = new URL(window.location.href);
    if (value === "all") url.searchParams.delete("hub");
    else url.searchParams.set("hub", value);
    window.history.replaceState(null, "", url.pathname + url.search);
  }

  useEffect(() => {
    const handle = window.setTimeout(() => {
      setDebouncedSearch(search);
      setPage(1);
    }, 300);
    return () => window.clearTimeout(handle);
  }, [search]);

  useEffect(() => {
    if (!autoRefresh) return;
    const interval = window.setInterval(() => setRefreshSeq(seq => seq + 1), 15_000);
    return () => window.clearInterval(interval);
  }, [autoRefresh]);

  const reload = useCallback(() => setRefreshSeq(seq => seq + 1), []);

  useEffect(() => {
    const controller = new AbortController();
    const params = new URLSearchParams({
      page: String(page), take: String(PAGE_SIZE),
      status, search: debouncedSearch, hub
    });
    setLoading(true);
    fetch(`/api/workflow-feed?${params.toString()}`, {
      cache: "no-store", signal: controller.signal
    }).then(async response => {
      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        throw new Error(body.error || `Workflow feed returned HTTP ${response.status}`);
      }
      return response.json() as Promise<UnifiedPage>;
    }).then(records => {
      if (controller.signal.aborted) return;
      setOverview(records.overview);
      setList(records);
      setHubs(records.hubs);
      setWarnings(records.warnings);
      setError(null);
      setLastUpdated(new Date().toISOString());
    }).catch((reason: unknown) => {
      if (!controller.signal.aborted) setError(reason instanceof Error ? reason.message : "Unable to load workflows");
    }).finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [status, hub, page, debouncedSearch, refreshSeq]);

  const pages = Math.max(1, Math.ceil((list?.total ?? 0) / PAGE_SIZE));

  return <div className="space-y-6">
    <div className="flex flex-wrap items-end justify-between gap-4">
      <div>
        <div className="mb-2 flex items-center gap-2 text-xs font-semibold tracking-wide text-blue-600 dark:text-blue-400">
          <Workflow size={14} /> WORKFLOW OPERATIONS
        </div>
        <h1 className="text-2xl font-bold tracking-tight sm:text-3xl">Workflow monitoring</h1>
        <p className="mt-2 text-sm text-muted-foreground">Monitor distributed executions, track failures, and investigate every persisted step attempt.</p>
      </div>
      <div className="flex flex-wrap items-center gap-2">
        <label className="flex cursor-pointer items-center gap-2 rounded-lg border border-border bg-card px-3 py-2 text-xs text-muted-foreground">
          <input type="checkbox" checked={autoRefresh} onChange={e => setAutoRefresh(e.target.checked)}
            className="accent-blue-600" />
          Auto-refresh 15s
        </label>
        <Button onClick={reload} disabled={loading}><RefreshCw size={15} className={loading ? "animate-spin" : ""} />Refresh</Button>
      </div>
    </div>

    {error && <div role="alert" className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-rose-300/50 bg-rose-500/5 px-4 py-3 text-sm text-rose-700 dark:text-rose-300">
      <span className="flex items-center gap-2"><AlertTriangle size={16} />{error}. Check the legacy monitoring API and Workflow Hub connectivity.</span>
      <Button size="sm" onClick={reload}>Retry</Button>
    </div>}

    {warnings.length > 0 && <div role="status" className="rounded-lg border border-amber-300/50 bg-amber-500/5 px-4 py-3 text-sm text-amber-700 dark:text-amber-300">
      {warnings.join(" ")} Showing results from the available sources.
    </div>}

    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
      <Metric icon={<GitBranch size={19} />} label="Total workflows" value={overview?.total} hint={`${overview?.created24h ?? "—"} started in the last 24 hours`} tone="slate" />
      <Metric icon={<Activity size={19} />} label="Runnable / active" value={overview?.running} hint="Legacy runnable plus Hub ready/running instances" tone="blue" />
      <Metric icon={<CheckCircle2 size={19} />} label="Completed" value={overview?.completed} hint={`${overview?.completed24h ?? "—"} completed in the last 24 hours`} tone="green" />
      <Metric icon={<AlertTriangle size={19} />} label="With execution errors" value={overview?.withErrors} hint="Legacy errors and failed Hub runs" tone="red" />
    </div>

    {(hub === "all" || hub === "legacy") && <DemoOnboarding onCreated={reload} />}

    <Card className="overflow-hidden">
      <CardHeader title="Workflow runs" description="Latest executions across legacy Workflow Core and Workflow Hub · click a run for history"
        action={<span className="inline-flex items-center gap-1.5 text-xs text-muted-foreground">
          {loading && <LoaderCircle size={13} className="animate-spin" />}
          {lastUpdated ? `Updated ${new Date(lastUpdated).toLocaleTimeString()}` : "Awaiting API"}
        </span>} />

      <div className="flex flex-wrap items-center justify-between gap-3 px-5 py-4">
        <div className="flex w-full flex-col gap-3 sm:w-auto sm:flex-row">
          <div className="relative w-full sm:w-80">
            <Search size={16} className="pointer-events-none absolute left-3 top-[11px] text-muted-foreground" />
            <Input aria-label="Search workflow ID, definition or reference" className="pl-9" placeholder="Search workflow ID, definition, reference..." value={search} onChange={e => setSearch(e.target.value)} />
          </div>
          <Select aria-label="Filter by hub" value={hub} onChange={e => changeHub(e.target.value)}>
            <option value="all">All hubs &amp; legacy</option>
            <option value="legacy">Legacy Workflow Core</option>
            <option value="hub">All Workflow Hubs</option>
            {hubs.map(id => <option key={id} value={id}>Hub: {id}</option>)}
          </Select>
          <Select aria-label="Filter by status" value={status} onChange={e => { setStatus(e.target.value); setPage(1); }}>
            {statusOptions.map(option => <option key={option} value={option}>{option === "All" ? "All statuses" : option}</option>)}
          </Select>
        </div>
        <span className="text-xs text-muted-foreground">{list ? list.total.toLocaleString() : "—"} matching workflows</span>
      </div>

      <div className="compact-scrollbar overflow-x-auto">
        <table className="w-full min-w-[1060px] text-left text-sm">
          <thead className="bg-muted/60 text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
            <tr><th className="px-5 py-3">Workflow</th><th className="px-5 py-3">Reference</th>
              <th className="px-5 py-3">Hub</th><th className="px-5 py-3">Status</th><th className="px-5 py-3">Started</th>
              <th className="px-5 py-3">Elapsed</th><th className="px-5 py-3">Errors</th><th className="px-5 py-3">Review</th></tr>
          </thead>
          <tbody>
            {list?.items.map(workflow => <WorkflowRow workflow={workflow} key={workflow.id} />)}
          </tbody>
        </table>
        {!error && !loading && list?.items.length === 0 && <EmptyState title="No workflows found"
          description="Try another status or search term. If the runtime has not started any workflows, the list will be empty." />}
        {loading && !list && <div className="flex items-center justify-center gap-2 p-10 text-sm text-muted-foreground">
          <Clock3 size={16} className="animate-pulse" />Loading workflow snapshots…
        </div>}
      </div>
      <div className="flex flex-wrap items-center justify-between gap-3 border-t border-border px-5 py-4">
        <span className="text-xs text-muted-foreground">
          Page {page} of {pages} · {list?.total ?? 0} result(s)
        </span>
        <div className="flex gap-2">
          <Button size="sm" disabled={loading || page <= 1} onClick={() => setPage(n => Math.max(1, n - 1))}>Previous</Button>
          <Button size="sm" disabled={loading || page >= pages} onClick={() => setPage(n => n + 1)}>Next</Button>
        </div>
      </div>
    </Card>
  </div>;
}

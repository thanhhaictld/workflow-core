"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import { AlertTriangle, ArrowLeft, CheckCircle2, CircleHelp, Code2, Copy,
  GitCompareArrows, GitFork, Layers3, LoaderCircle, Play, RefreshCw, ShieldCheck } from "lucide-react";
import { getJson } from "@/lib/api";
import type { CatalogIndex, CatalogVersion, DefinitionChange,
  DefinitionComparison, DefinitionLifecycle, DefinitionStep } from "@/lib/definitions";
import { Badge, Button, Card, CardHeader, EmptyState, Select, cn } from "./ui";

const states: DefinitionLifecycle[] = ["Published", "Deprecated", "Disabled"];
const changedColor = {
  Added: "bg-emerald-500/10 text-emerald-700 dark:text-emerald-300",
  Removed: "bg-rose-500/10 text-rose-700 dark:text-rose-300",
  Modified: "bg-amber-500/10 text-amber-700 dark:text-amber-300"
};

function prettyJson(value: string | null): string {
  if (value === null) return "—";
  try { return JSON.stringify(JSON.parse(value), null, 2); }
  catch { return value; }
}

function SchemaStep({ step }: { step: DefinitionStep }) {
  return <div className="rounded-lg border border-border bg-muted/10 p-3">
    <div className="flex items-center gap-2 text-xs font-semibold">
      <span className="font-mono text-muted-foreground">#{step.id}</span>
      <span className="break-all">{step.name}</span>
      {step.kind === "parallel" && <Badge tone="blue">Parallel</Badge>}
    </div>
    <p className="mt-1 break-all font-mono text-[11px] text-muted-foreground">{step.bodyType}</p>
    <div className="mt-2 flex flex-wrap gap-2 text-[11px] text-muted-foreground">
      {step.children.length > 0 && <span>Branches: {step.children.join(", ")}</span>}
      {step.outcomes.length > 0 && <span>Next: {step.outcomes.map(x => x.targetStepId).join(", ")}</span>}
      {step.inputs.length > 0 && <span>Inputs: {step.inputs.length}</span>}
      {step.outputs.length > 0 && <span>Outputs: {step.outputs.length}</span>}
      {step.errorBehavior && <span>On error: {step.errorBehavior}</span>}
    </div>
  </div>;
}

export function DefinitionDetail({ id }: { id: string }) {
  const [versions, setVersions] = useState<CatalogVersion[]>([]);
  const [writesEnabled, setWritesEnabled] = useState(false);
  const [from, setFrom] = useState<number | null>(null);
  const [to, setTo] = useState<number | null>(null);
  const [comparison, setComparison] = useState<DefinitionComparison | null>(null);
  const [filter, setFilter] = useState("All");
  const [showSchema, setShowSchema] = useState<"from" | "to" | null>(null);
  const [loading, setLoading] = useState(true);
  const [diffLoading, setDiffLoading] = useState(false);
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [created, setCreated] = useState<string | null>(null);
  const [refresh, setRefresh] = useState(0);

  const reload = useCallback(() => setRefresh(n => n + 1), []);
  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    Promise.all([
      getJson<CatalogVersion[]>("/definitions/" + encodeURIComponent(id) + "/versions", controller.signal),
      getJson<CatalogIndex>("/definitions", controller.signal)
    ]).then(([list, catalog]) => {
      setVersions(list.sort((a, b) => b.version - a.version));
      setWritesEnabled(catalog.writesEnabled);
      setFrom(current => current !== null && list.some(v => v.version === current)
        ? current : list.length > 1 ? list[1].version : list[0]?.version ?? null);
      setTo(current => current !== null && list.some(v => v.version === current)
        ? current : list[0]?.version ?? null);
      setError(null);
    }).catch(reason => {
      if (!controller.signal.aborted)
        setError(reason instanceof Error ? reason.message : "Unable to load workflow versions");
    }).finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [id, refresh]);

  useEffect(() => {
    if (from === null || to === null || from === to) {
      setComparison(null); setDiffLoading(false); return;
    }
    const controller = new AbortController();
    setDiffLoading(true);
    setComparison(null);
    getJson<DefinitionComparison>("/definitions/" + encodeURIComponent(id) +
      "/diff?from=" + from + "&to=" + to, controller.signal).then(value => {
      setComparison(value);
      setError(null);
    }).catch(reason => {
      if (!controller.signal.aborted)
        setError(reason instanceof Error ? reason.message : "Could not compare versions");
    }).finally(() => { if (!controller.signal.aborted) setDiffLoading(false); });
    return () => controller.abort();
  }, [id, from, to, refresh]);

  async function mutate(path: string, method: "POST" | "PUT", body?: object) {
    setBusy(path); setError(null);
    try {
      const response = await fetch("/backend" + path, {
        method,
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: body ? JSON.stringify(body) : undefined
      });
      const result: unknown = await response.json().catch(() => ({}));
      if (!response.ok)
        throw new Error(result && typeof result === "object" && "error" in result
          ? String((result as { error: unknown }).error) : "Request failed: " + response.status);
      reload();
      return result;
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Management operation failed");
      return null;
    } finally { setBusy(null); }
  }

  async function promote(version: number) {
    if (!window.confirm("Use v" + version + " for new default starts? Running workflows keep their current version.")) return;
    await mutate("/definitions/" + encodeURIComponent(id) + "/versions/" + version + "/default", "POST");
  }
  async function changeLifecycle(version: number, lifecycle: DefinitionLifecycle) {
    if (!window.confirm("Set v" + version + " to " + lifecycle + "? This changes which new sample executions may start.")) return;
    await mutate("/definitions/" + encodeURIComponent(id) + "/versions/" + version + "/lifecycle",
      "PUT", { lifecycle });
  }
  async function startVersion() {
    if (to === null || id !== "tenant-onboarding-versioned") return;
    const tenantId = "version-" + crypto.randomUUID().slice(0, 10);
    const result = await mutate("/definitions/tenant-onboarding-versioned/start",
      "POST", { tenantId, organizationName: "Schema comparison test", version: to });
    if (result && typeof result === "object" && "workflowId" in result)
      setCreated(String(result.workflowId));
  }

  const fromSchema = versions.find(v => v.version === from);
  const toSchema = versions.find(v => v.version === to);
  const changes = comparison?.changes ?? [];
  const visible = useMemo(() => changes.filter(change =>
    filter === "All" || change.change === filter), [changes, filter]);

  return <div className="space-y-6">
    <div className="flex flex-wrap items-start justify-between gap-3">
      <div className="min-w-0">
        <Link href="/definitions" className="mb-3 inline-flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground">
          <ArrowLeft size={14} /> Workflow definitions
        </Link>
        <div className="flex items-center gap-2">
          <Layers3 size={19} className="shrink-0 text-blue-500" />
          <h1 className="break-all text-xl font-bold tracking-tight sm:text-2xl">{id}</h1>
        </div>
        <p className="mt-2 text-sm text-muted-foreground">
          Registered schema versions, publication policy and structural metadata diff.
        </p>
      </div>
      <Button disabled={loading} onClick={reload}>
        <RefreshCw size={14} className={loading ? "animate-spin" : ""} /> Refresh
      </Button>
    </div>

    {error && <div role="alert" className="flex gap-2 rounded-lg border border-rose-500/40 bg-rose-500/5 p-4 text-sm text-rose-600">
      <AlertTriangle size={17} />{error}
    </div>}
    {loading && versions.length === 0 && <div className="flex items-center gap-2 p-8 text-xs text-muted-foreground">
      <LoaderCircle size={15} className="animate-spin" />Loading registered versions…
    </div>}
    {!loading && versions.length === 0 && <EmptyState
      title="Definition not found" description="The API has no registered metadata for this workflow ID." />}

    <Card className="overflow-hidden">
      <CardHeader title="Version management"
        description="Definitions are immutable. Change lifecycle or default selection without editing historical schemas."
        action={<Badge tone={writesEnabled ? "amber" : "slate"}>
          {writesEnabled ? "Lab writes enabled" : "Read-only"}
        </Badge>} />
      <div className="divide-y divide-border">
        {versions.map(item => <div key={item.version}
          className="flex flex-wrap items-center justify-between gap-4 p-5">
          <div>
            <div className="flex flex-wrap items-center gap-2 text-sm font-semibold">
              Version {item.version}
              {item.isDefault && <Badge tone="green"><CheckCircle2 size={12} /> Default</Badge>}
              <Badge tone={item.lifecycle === "Published" ? "blue" : item.lifecycle === "Deprecated" ? "amber" : "red"}>
                {item.lifecycle}
              </Badge>
            </div>
            <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-[11px] text-muted-foreground">
              <span>{item.schema.steps.length} steps</span>
              <span>SHA <code title={item.schemaSha256}>{item.schemaSha256.slice(0, 12)}</code></span>
              <span>Registered {new Date(item.registeredAt).toLocaleString()}</span>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <Button size="sm" variant="outline" onClick={() => { setFrom(item.version); setTo(versions[0]?.version ?? item.version); }}>
              <GitCompareArrows size={13} /> Compare
            </Button>
            {writesEnabled && <>
              {!item.isDefault && item.lifecycle === "Published" && <Button size="sm"
                disabled={busy !== null} onClick={() => promote(item.version)}>Set default</Button>}
              <Select aria-label={"Lifecycle for version " + item.version} disabled={busy !== null}
                value={item.lifecycle} onChange={e =>
                  changeLifecycle(item.version, e.target.value as DefinitionLifecycle)}>
                {states.map(state => <option key={state} value={state}
                  disabled={item.isDefault && state !== "Published"}>{state}</option>)}
              </Select>
            </>}
          </div>
        </div>)}
      </div>
    </Card>

    <Card className="overflow-hidden">
      <CardHeader title="Compare schema metadata"
        description="Normalized, immutable JSON metadata. Compares step IDs, types, mappings, transitions, parallel branches, error and retry policy."
        action={<Badge tone="blue"><GitCompareArrows size={12} /> Structural diff</Badge>} />
      <div className="flex flex-wrap items-end gap-3 border-b border-border p-5">
        <label className="flex min-w-[150px] flex-col gap-1.5 text-xs font-semibold">
          From version
          <Select value={from ?? ""} disabled={versions.length === 0}
            onChange={e => setFrom(Number(e.target.value))}>
            {versions.map(item => <option key={item.version} value={item.version}>v{item.version}</option>)}
          </Select>
        </label>
        <span className="mb-2 text-sm text-muted-foreground">→</span>
        <label className="flex min-w-[150px] flex-col gap-1.5 text-xs font-semibold">
          To version
          <Select value={to ?? ""} disabled={versions.length === 0}
            onChange={e => setTo(Number(e.target.value))}>
            {versions.map(item => <option key={item.version} value={item.version}>v{item.version}</option>)}
          </Select>
        </label>
        <div className="flex-1" />
        {id === "tenant-onboarding-versioned" && toSchema?.lifecycle === "Published" &&
          <Button size="sm" disabled={busy !== null} onClick={startVersion}>
            <Play size={13} /> Run v{to}
          </Button>}
        {created && <Link href={"/workflows/" + created}
          className="text-xs font-semibold text-blue-600 hover:underline">View new run</Link>}
      </div>
      {from !== null && to !== null && from === to
        ? <EmptyState title="Select two different versions"
            description="Choose two version numbers to see structural metadata changes." />
        : diffLoading ? <div className="flex items-center gap-2 p-8 text-xs text-muted-foreground">
            <LoaderCircle size={15} className="animate-spin" />Comparing version metadata…
          </div>
        : comparison && <div>
          <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border px-5 py-3">
            <div className="flex flex-wrap gap-2">
              {(["All", "Added", "Removed", "Modified"] as const).map(option =>
                <Button key={option} size="sm" variant={filter === option ? "primary" : "ghost"}
                  onClick={() => setFilter(option)}>
                  {option} ({option === "All" ? changes.length : changes.filter(c => c.change === option).length})
                </Button>)}
            </div>
            <span className="text-xs text-muted-foreground">{visible.length} changes</span>
          </div>
          <div className="compact-scrollbar overflow-x-auto">
            <table className="w-full min-w-[880px] table-fixed text-left text-xs">
              <thead className="bg-muted/50 text-muted-foreground"><tr>
                <th className="w-32 px-4 py-3">Change</th>
                <th className="w-[31%] px-4 py-3">Metadata path</th>
                <th className="px-4 py-3">Before (v{from})</th>
                <th className="px-4 py-3">After (v{to})</th>
              </tr></thead>
              <tbody>{visible.map((change: DefinitionChange, index: number) =>
                <tr className="border-t border-border align-top" key={change.path + "-" + index}>
                  <td className="px-4 py-3"><span className={cn("rounded px-2 py-1 text-[11px] font-semibold", changedColor[change.change])}>{change.change}</span></td>
                  <td className="break-all px-4 py-3 font-mono text-[11px]">{change.path}</td>
                  <td className="px-4 py-3"><pre className="max-h-36 overflow-auto whitespace-pre-wrap break-words font-mono text-[11px] text-muted-foreground">{prettyJson(change.before)}</pre></td>
                  <td className="px-4 py-3"><pre className="max-h-36 overflow-auto whitespace-pre-wrap break-words font-mono text-[11px]">{prettyJson(change.after)}</pre></td>
                </tr>)}</tbody>
            </table>
          </div>
          {changes.length === 0 && <div className="flex items-center gap-2 p-5 text-xs text-emerald-600">
            <CheckCircle2 size={15} />The normalized schemas are identical.</div>}
        </div>}
      <div className="flex flex-wrap items-center gap-3 border-t border-border px-5 py-3 text-[11px] text-muted-foreground">
        <CircleHelp size={13} />Matches steps by ExternalId when available, otherwise by numeric step ID.
        Inserting steps can produce multiple changes when IDs shift. Expressions are never serialized.
      </div>
    </Card>

    {(fromSchema || toSchema) && <Card className="overflow-hidden">
      <CardHeader title="Inspect version schema" description="Browse every definition step or copy the normalized JSON snapshot." />
      <div className="flex flex-wrap gap-2 px-5 py-4">
        <Button variant={showSchema === "from" ? "primary" : "outline"} size="sm"
          onClick={() => setShowSchema(value => value === "from" ? null : "from")} disabled={!fromSchema}>
          <Code2 size={13} /> v{from} metadata
        </Button>
        <Button variant={showSchema === "to" ? "primary" : "outline"} size="sm"
          onClick={() => setShowSchema(value => value === "to" ? null : "to")} disabled={!toSchema}>
          <Code2 size={13} /> v{to} metadata
        </Button>
        {showSchema && <Button size="sm" variant="ghost" onClick={() => {
          const schema = showSchema === "from" ? fromSchema?.schema : toSchema?.schema;
          if (schema) navigator.clipboard.writeText(JSON.stringify(schema, null, 2));
        }}><Copy size={13} /> Copy JSON</Button>}
      </div>
      {showSchema && <div className="border-t border-border p-5">
        <div className="mb-3 flex gap-2 text-xs text-muted-foreground">
          <GitFork size={14} /> Step order and branches are taken from the registered fluent workflow definition.
        </div>
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
          {(showSchema === "from" ? fromSchema : toSchema)?.schema.steps.map(step =>
            <SchemaStep key={step.id} step={step} />)}
        </div>
        <details className="mt-4 rounded-lg border border-border p-3">
          <summary className="cursor-pointer text-xs font-semibold">Raw normalized schema metadata</summary>
          <pre className="compact-scrollbar mt-3 max-h-[450px] overflow-auto whitespace-pre-wrap break-all font-mono text-[11px]">
            {JSON.stringify((showSchema === "from" ? fromSchema : toSchema)?.schema, null, 2)}
          </pre>
        </details>
      </div>}
    </Card>}
    <div className="flex gap-2 text-xs leading-5 text-muted-foreground">
      <ShieldCheck size={16} className="shrink-0" />
      Lifecycle changes govern new sample starts; they never replace executable code or migrate running workflow state.
      Management writes require an explicit private-lab opt-in.
    </div>
  </div>;
}

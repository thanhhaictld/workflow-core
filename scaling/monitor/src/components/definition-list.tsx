"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { AlertTriangle, GitCompareArrows,
  Layers3, LoaderCircle, RefreshCw, Search, ShieldCheck, Workflow } from "lucide-react";
import { groupHubDefinitions, type HubPublishedDefinition } from "@/lib/hub-definitions";
import { Badge, Button, Card, CardHeader, EmptyState, Input } from "./ui";

export function DefinitionList() {
  const [hubDefinitions, setHubDefinitions] = useState<HubPublishedDefinition[]>([]);
  const [hubError, setHubError] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [loading, setLoading] = useState(true);
  const [refresh, setRefresh] = useState(0);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    fetch("/api/hub-definitions", { cache: "no-store", signal: controller.signal, headers: { Accept: "application/json" } })
      .then(async response => {
        if (!response.ok) throw new Error(`Hub definition API returned HTTP ${response.status}`);
        const value: unknown = await response.json();
        if (!Array.isArray(value)) throw new Error("Invalid Hub definitions response");
        return value as HubPublishedDefinition[];
      }).then(value => { setHubDefinitions(value); setHubError(null); })
      .catch(reason => { if (!controller.signal.aborted) { setHubDefinitions([]); setHubError(reason instanceof Error ? reason.message : "Hub catalog unavailable"); } })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [refresh]);

  const hubGroups = useMemo(() => groupHubDefinitions(hubDefinitions).filter(group =>
    (group.hubId + "/" + group.definitionId).toLowerCase().includes(search.trim().toLowerCase())),
    [hubDefinitions, search]);
  const totalVersions = hubDefinitions.length;
  const totalDefinitions = groupHubDefinitions(hubDefinitions).length;

  return <div className="space-y-6">
    <div className="flex flex-wrap items-end justify-between gap-4">
      <div>
        <div className="mb-2 flex items-center gap-2 text-xs font-semibold tracking-wide text-blue-600">
          <Layers3 size={14} /> WORKFLOW CATALOG
        </div>
        <h1 className="text-2xl font-bold tracking-tight sm:text-3xl">Workflow definitions</h1>
        <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
          Browse published Hub workflow definitions.
          Review versioned steps, metadata and runtime ownership without changing executable definitions.
        </p>
      </div>
      <Button disabled={loading} onClick={() => setRefresh(n => n + 1)}>
        <RefreshCw size={15} className={loading ? "animate-spin" : ""} />Refresh
      </Button>
    </div>

    {hubError && <div role="alert" className="flex gap-2 rounded-lg border border-amber-500/40 bg-amber-500/5 p-4 text-sm text-amber-700 dark:text-amber-300">
      <AlertTriangle size={17} />Hub-managed definitions unavailable: {hubError}
    </div>}

    <div className="grid gap-4 sm:grid-cols-2">
      <Card className="p-4"><div className="flex items-center gap-2 text-xs text-muted-foreground"><Workflow size={15} />Definitions</div>
        <div className="mt-2 text-2xl font-bold">{totalDefinitions}</div></Card>
      <Card className="p-4"><div className="flex items-center gap-2 text-xs text-muted-foreground"><Layers3 size={15} />Registered versions</div>
        <div className="mt-2 text-2xl font-bold">{totalVersions}</div></Card>
    </div>
    <div className="rounded-lg border border-border bg-muted/20 px-4 py-3 text-xs leading-5 text-muted-foreground">
      <ShieldCheck size={14} className="mr-2 inline-block" />
      Read-only definition metadata from durable Hub REST registrations. Changes to executable workflows belong in code.
    </div>

    <Card className="overflow-hidden">
      <CardHeader title="Definition registry" description="Hub-published workflow definitions" />
      <div className="border-b border-border px-5 py-4">
        <div className="relative max-w-md">
          <Search size={16} className="pointer-events-none absolute left-3 top-2.5 text-muted-foreground" />
          <Input className="pl-9" value={search} onChange={e => setSearch(e.target.value)}
            aria-label="Search workflow definitions" placeholder="Search hub or definition ID..." />
        </div>
      </div>
      <div className="divide-y divide-border">
        {hubGroups.length > 0 && <div className="bg-muted/30 px-5 py-2 text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
          Hub-managed definitions · published by remote processors
        </div>}
        {hubGroups.map(group => {
          const latest = group.versions[0];
          return <Link key={group.hubId + ":" + group.definitionId}
            href={"/definitions/hub/" + encodeURIComponent(group.hubId) + "/" + encodeURIComponent(group.definitionId)}
            className="flex flex-wrap items-center justify-between gap-4 px-5 py-4 hover:bg-muted/40">
            <div className="min-w-0">
              <div className="flex flex-wrap items-center gap-2">
                <Workflow size={16} className="text-blue-500" />
                <span className="break-all text-sm font-semibold">{group.definitionId}</span>
                <Badge tone="blue">Hub: {group.hubId}</Badge>
                <Badge tone="green">{latest.executionMode ?? "remote"}</Badge>
              </div>
              <div className="mt-2 flex flex-wrap gap-3 text-[11px] text-muted-foreground">
                <span>{group.versions.length} version{group.versions.length === 1 ? "" : "s"}</span>
                <span>Latest: v{latest.version}</span>
                <span>{latest.stepCount ?? latest.steps?.length ?? 0} steps</span>
                <span>{latest.partitions} logical partitions</span>
                <span>PostgreSQL · Hub REST</span>
              </div>
            </div>
            <span className="flex items-center gap-2 text-xs font-semibold text-blue-600">
              <GitCompareArrows size={15} />Review definition
            </span>
          </Link>;
        })}
        {!loading && hubGroups.length === 0 && !hubError &&
          <EmptyState title="No definitions" description="Start a Hub processor to publish a definition." />}
        {loading && hubDefinitions.length === 0 && <div className="flex items-center gap-2 p-10 text-xs text-muted-foreground">
          <LoaderCircle size={15} className="animate-spin" />Loading catalog…
        </div>}
      </div>
      <div className="border-t border-border px-5 py-3 text-[11px] text-muted-foreground">
        Hub definitions are registered on processor startup.
        Distinct Hub versions are visible, but same-version Hub registration updates are not separately archived.
      </div>
    </Card>
  </div>;
}

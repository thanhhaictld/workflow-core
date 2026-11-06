"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { AlertTriangle, GitCompareArrows, GitBranch,
  Layers3, LoaderCircle, RefreshCw, Search, ShieldCheck, Workflow } from "lucide-react";
import { getJson } from "@/lib/api";
import type { CatalogIndex, CatalogVersion } from "@/lib/definitions";
import { groupHubDefinitions, type HubPublishedDefinition } from "@/lib/hub-definitions";
import { Badge, Button, Card, CardHeader, EmptyState, Input } from "./ui";

interface DefinitionGroup {
  id: string;
  versions: CatalogVersion[];
}
export function DefinitionList() {
  const [catalog, setCatalog] = useState<CatalogIndex | null>(null);
  const [hubDefinitions, setHubDefinitions] = useState<HubPublishedDefinition[]>([]);
  const [hubError, setHubError] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [refresh, setRefresh] = useState(0);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    // The legacy version archive and the remote Hub registration store are
    // distinct authoritative sources. Load both independently so a legacy
    // monitor outage does not hide running Hub workflows.
    const hub = fetch("/api/hub-definitions", {
      cache: "no-store", signal: controller.signal, headers: { Accept: "application/json" },
    }).then(async response => {
      if (!response.ok) throw new Error(`Hub definition API returned HTTP ${response.status}`);
      const value: unknown = await response.json();
      if (!Array.isArray(value)) throw new Error("Invalid Hub definitions response");
      return value as HubPublishedDefinition[];
    });
    Promise.allSettled([getJson<CatalogIndex>("/definitions", controller.signal), hub])
      .then(([legacyResult, hubResult]) => {
        if (controller.signal.aborted) return;
        if (legacyResult.status === "fulfilled") {
          setCatalog(legacyResult.value);
          setError(null);
        } else {
          setCatalog(null);
          setError(legacyResult.reason instanceof Error ? legacyResult.reason.message : "Legacy catalog unavailable");
        }
        if (hubResult.status === "fulfilled") {
          setHubDefinitions(hubResult.value);
          setHubError(null);
        } else {
          setHubDefinitions([]);
          setHubError(hubResult.reason instanceof Error ? hubResult.reason.message : "Hub catalog unavailable");
        }
      }).finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [refresh]);

  const groups = useMemo(() => {
    const grouped = new Map<string, CatalogVersion[]>();
    for (const version of catalog?.versions ?? []) {
      grouped.set(version.definitionId, [...(grouped.get(version.definitionId) ?? []), version]);
    }
    return [...grouped].map(([id, versions]) => ({
      id, versions: versions.sort((a, b) => b.version - a.version)
    })).filter(group => group.id.toLowerCase().includes(search.trim().toLowerCase()))
      .sort((a, b) => a.id.localeCompare(b.id));
  }, [catalog, search]);

  const hubGroups = useMemo(() => groupHubDefinitions(hubDefinitions).filter(group =>
    (group.hubId + "/" + group.definitionId).toLowerCase().includes(search.trim().toLowerCase())),
    [hubDefinitions, search]);
  const versions = catalog?.versions ?? [];
  const drifted = versions.filter(v => v.driftDetected).length;
  const totalVersions = versions.length + hubDefinitions.length;
  const totalDefinitions = new Set([
    ...versions.map(v => "legacy:" + v.definitionId),
    ...groupHubDefinitions(hubDefinitions).map(g => "hub:" + g.hubId + ":" + g.definitionId),
  ]).size;

  return <div className="space-y-6">
    <div className="flex flex-wrap items-end justify-between gap-4">
      <div>
        <div className="mb-2 flex items-center gap-2 text-xs font-semibold tracking-wide text-blue-600">
          <Layers3 size={14} /> WORKFLOW CATALOG
        </div>
        <h1 className="text-2xl font-bold tracking-tight sm:text-3xl">Workflow definitions</h1>
        <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
          Browse published Hub-native and legacy code-first workflow definitions.
          Review versioned steps, metadata and runtime ownership without changing executable definitions.
        </p>
      </div>
      <Button disabled={loading} onClick={() => setRefresh(n => n + 1)}>
        <RefreshCw size={15} className={loading ? "animate-spin" : ""} />Refresh
      </Button>
    </div>

    {error && <div role="alert" className="flex gap-2 rounded-lg border border-amber-500/40 bg-amber-500/5 p-4 text-sm text-amber-700 dark:text-amber-300">
      <AlertTriangle size={17} />Legacy archived catalog unavailable: {error}
    </div>}
    {hubError && <div role="alert" className="flex gap-2 rounded-lg border border-amber-500/40 bg-amber-500/5 p-4 text-sm text-amber-700 dark:text-amber-300">
      <AlertTriangle size={17} />Hub-managed definitions unavailable: {hubError}
    </div>}

    <div className="grid gap-4 sm:grid-cols-3">
      <Card className="p-4"><div className="flex items-center gap-2 text-xs text-muted-foreground"><Workflow size={15} />Definitions</div>
        <div className="mt-2 text-2xl font-bold">{totalDefinitions}</div></Card>
      <Card className="p-4"><div className="flex items-center gap-2 text-xs text-muted-foreground"><Layers3 size={15} />Registered versions</div>
        <div className="mt-2 text-2xl font-bold">{totalVersions}</div></Card>
      <Card className="p-4"><div className="flex items-center gap-2 text-xs text-muted-foreground"><AlertTriangle size={15} />Same-version drift warnings</div>
        <div className="mt-2 text-2xl font-bold">{drifted}</div></Card>
    </div>
    <div className="rounded-lg border border-border bg-muted/20 px-4 py-3 text-xs leading-5 text-muted-foreground">
      <ShieldCheck size={14} className="mr-2 inline-block" />
      Read-only definition metadata from two registries: durable Hub REST registrations
      and the legacy C# version archive. Changes to executable workflows belong in code.
    </div>

    <Card className="overflow-hidden">
      <CardHeader title="Definition registry" description="Hub-published workflow definitions and legacy C# metadata snapshots" />
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
        {groups.length > 0 && <div className="bg-muted/30 px-5 py-2 text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
          Legacy archived definitions
        </div>}
        {groups.map(group => {
          const latest = group.versions[0];
          return <Link key={group.id} href={"/definitions/" + encodeURIComponent(group.id)}
            className="flex flex-wrap items-center justify-between gap-4 px-5 py-4 hover:bg-muted/40">
            <div className="min-w-0">
              <div className="flex flex-wrap items-center gap-2">
                <GitBranch size={16} className="text-blue-500" />
                <span className="break-all text-sm font-semibold">{group.id}</span>
                {group.versions.some(v => v.driftDetected) && <Badge tone="amber">Metadata drift</Badge>}
              </div>
              <div className="mt-2 flex flex-wrap gap-3 text-[11px] text-muted-foreground">
                <span>{group.versions.length} version{group.versions.length === 1 ? "" : "s"}</span>
                <span>Latest: v{latest.version}</span>
                <span>{latest.schema.steps.length} steps (latest)</span>
                <span>Code-first · read only</span>
              </div>
            </div>
            <span className="flex items-center gap-2 text-xs font-semibold text-blue-600">
              <GitCompareArrows size={15} />Review changes
            </span>
          </Link>;
        })}
        {!loading && groups.length === 0 && hubGroups.length === 0 && !error && !hubError &&
          <EmptyState title="No definitions" description="Start a Hub processor to publish a definition, or register a legacy C# workflow in the monitoring API." />}
        {loading && !catalog && hubDefinitions.length === 0 && <div className="flex items-center gap-2 p-10 text-xs text-muted-foreground">
          <LoaderCircle size={15} className="animate-spin" />Loading catalog…
        </div>}
      </div>
      <div className="border-t border-border px-5 py-3 text-[11px] text-muted-foreground">
        Hub definitions are registered on processor startup; legacy metadata snapshots are retained separately.
        Distinct Hub versions are visible, but same-version Hub registration updates are not separately archived.
      </div>
    </Card>
  </div>;
}

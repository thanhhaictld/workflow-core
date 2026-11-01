"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { AlertTriangle, CheckCircle2, Clock3, GitCompareArrows, GitBranch,
  Layers3, LoaderCircle, RefreshCw, Search, ShieldCheck, Workflow } from "lucide-react";
import { getJson } from "@/lib/api";
import type { CatalogIndex, CatalogVersion } from "@/lib/definitions";
import { Badge, Button, Card, CardHeader, EmptyState, Input } from "./ui";

interface DefinitionGroup {
  id: string;
  versions: CatalogVersion[];
}
export function DefinitionList() {
  const [catalog, setCatalog] = useState<CatalogIndex | null>(null);
  const [search, setSearch] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [refresh, setRefresh] = useState(0);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    getJson<CatalogIndex>("/definitions", controller.signal).then(value => {
      setCatalog(value); setError(null);
    }).catch(reason => {
      if (!controller.signal.aborted)
        setError(reason instanceof Error ? reason.message : "Unable to load workflow definitions");
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

  const versions = catalog?.versions ?? [];
  const published = versions.filter(v => v.lifecycle === "Published").length;

  return <div className="space-y-6">
    <div className="flex flex-wrap items-end justify-between gap-4">
      <div>
        <div className="mb-2 flex items-center gap-2 text-xs font-semibold tracking-wide text-blue-600">
          <Layers3 size={14} /> WORKFLOW CATALOG
        </div>
        <h1 className="text-2xl font-bold tracking-tight sm:text-3xl">Workflow definitions</h1>
        <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
          Inspect registered schemas, manage release versions and compare immutable workflow metadata.
          Running instances remain pinned to their original definition versions.
        </p>
      </div>
      <Button disabled={loading} onClick={() => setRefresh(n => n + 1)}>
        <RefreshCw size={15} className={loading ? "animate-spin" : ""} />Refresh
      </Button>
    </div>

    {error && <div role="alert" className="flex gap-2 rounded-lg border border-rose-500/40 bg-rose-500/5 p-4 text-sm text-rose-600">
      <AlertTriangle size={17} />{error}
    </div>}

    <div className="grid gap-4 sm:grid-cols-3">
      <Card className="p-4"><div className="flex items-center gap-2 text-xs text-muted-foreground"><Workflow size={15} />Definitions</div>
        <div className="mt-2 text-2xl font-bold">{new Set(versions.map(v => v.definitionId)).size}</div></Card>
      <Card className="p-4"><div className="flex items-center gap-2 text-xs text-muted-foreground"><Layers3 size={15} />Registered versions</div>
        <div className="mt-2 text-2xl font-bold">{versions.length}</div></Card>
      <Card className="p-4"><div className="flex items-center gap-2 text-xs text-muted-foreground"><CheckCircle2 size={15} />Published versions</div>
        <div className="mt-2 text-2xl font-bold">{published}</div></Card>
    </div>
    <div className="rounded-lg border border-border bg-muted/20 px-4 py-3 text-xs leading-5 text-muted-foreground">
      <ShieldCheck size={14} className="mr-2 inline-block" />
      {catalog?.writesEnabled
        ? "Demo management writes enabled. Protect the API with authentication and authorization before exposing this beyond a private workstation."
        : "Management changes are read-only. Enable WFC_ENABLE_MANAGEMENT_WRITES=true only in your private demo lab."}
    </div>

    <Card className="overflow-hidden">
      <CardHeader title="Definition registry" description="Source-controlled code definitions · immutable per-version snapshots · release policy separate from schema" />
      <div className="border-b border-border px-5 py-4">
        <div className="relative max-w-md">
          <Search size={16} className="pointer-events-none absolute left-3 top-2.5 text-muted-foreground" />
          <Input className="pl-9" value={search} onChange={e => setSearch(e.target.value)}
            aria-label="Search workflow definitions" placeholder="Search definition ID..." />
        </div>
      </div>
      <div className="divide-y divide-border">
        {groups.map(group => {
          const latest = group.versions[0];
          const current = group.versions.find(v => v.isDefault);
          return <Link key={group.id} href={"/definitions/" + encodeURIComponent(group.id)}
            className="flex flex-wrap items-center justify-between gap-4 px-5 py-4 hover:bg-muted/40">
            <div className="min-w-0">
              <div className="flex flex-wrap items-center gap-2">
                <GitBranch size={16} className="text-blue-500" />
                <span className="break-all text-sm font-semibold">{group.id}</span>
                {current ? <Badge tone="green">Default v{current.version}</Badge> : <Badge tone="amber">No default</Badge>}
              </div>
              <div className="mt-2 flex flex-wrap gap-3 text-[11px] text-muted-foreground">
                <span>{group.versions.length} version{group.versions.length === 1 ? "" : "s"}</span>
                <span>Latest: v{latest.version}</span>
                <span>{latest.schema.steps.length} steps (latest)</span>
                <span>{group.versions.filter(v => v.lifecycle === "Published").length} published</span>
              </div>
            </div>
            <span className="flex items-center gap-2 text-xs font-semibold text-blue-600">
              <GitCompareArrows size={15} />Manage versions
            </span>
          </Link>;
        })}
        {!loading && !error && groups.length === 0 &&
          <EmptyState title="No definitions" description="Register code workflows in the API and apply migration 006 to populate the catalog." />}
        {loading && !catalog && <div className="flex items-center gap-2 p-10 text-xs text-muted-foreground">
          <LoaderCircle size={15} className="animate-spin" />Loading catalog…
        </div>}
      </div>
      <div className="flex gap-2 border-t border-border px-5 py-3 text-[11px] text-muted-foreground">
        <Clock3 size={13} />Lifecycle affects new controlled starts only; never mutates an existing execution.
      </div>
    </Card>
  </div>;
}

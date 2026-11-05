"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { ArrowLeft, RefreshCw } from "lucide-react";
import { stringify } from "yaml";
import { hubDefinitionMetadata, type HubPublishedDefinition } from "@/lib/hub-definitions";
import { Badge, Button, Card, EmptyState, Select } from "./ui";
import { YamlDiff, YamlViewer } from "./definition-yaml";

export function HubDefinitionDetail({ hubId, definitionId }: {
  hubId: string;
  definitionId: string;
}) {
  const [versions, setVersions] = useState<HubPublishedDefinition[] | null>(null);
  const [selected, setSelected] = useState<number | null>(null);
  const [base, setBase] = useState<number | null>(null);
  const [target, setTarget] = useState<number | null>(null);
  const [mode, setMode] = useState<"yaml" | "compare">("yaml");
  const [view, setView] = useState<"split" | "unified">("split");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [refresh, setRefresh] = useState(0);
  const [copy, setCopy] = useState("");

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    fetch("/api/hub-definitions", {
      cache: "no-store", signal: controller.signal,
      headers: { Accept: "application/json" },
    }).then(async response => {
      if (!response.ok) throw new Error("Hub returned HTTP " + response.status);
      const data: unknown = await response.json();
      if (!Array.isArray(data)) throw new Error("Invalid Hub definition catalog");
      return data as HubPublishedDefinition[];
    }).then(all => {
      if (controller.signal.aborted) return;
      const found = all.filter(d => d.hubId === hubId && d.definitionId === definitionId)
        .sort((a, b) => b.version - a.version);
      setVersions(found);
      const choose = (old: number | null, fallback: number | null) =>
        found.some(d => d.version === old) ? old : fallback;
      setSelected(old => choose(old, found[0]?.version ?? null));
      setBase(old => choose(old, found[1]?.version ?? found[0]?.version ?? null));
      setTarget(old => choose(old, found[0]?.version ?? null));
    }).catch(reason => {
      if (!controller.signal.aborted)
        setError(reason instanceof Error ? reason.message : "Could not load Hub definitions");
    }).finally(() => {
      if (!controller.signal.aborted) setLoading(false);
    });
    return () => controller.abort();
  }, [hubId, definitionId, refresh]);

  const current = versions?.find(v => v.version === selected);
  const from = versions?.find(v => v.version === base);
  const to = versions?.find(v => v.version === target);
  const yaml = (d: HubPublishedDefinition) => stringify(hubDefinitionMetadata(d), {
    indent: 2, lineWidth: 0,
  });
  const selectedYaml = useMemo(() => current ? yaml(current) : "",
    [current]);
  const select = (label: string, value: number | null, update: (number: number) => void) =>
    <label className="flex min-w-36 flex-col gap-1.5 text-xs font-semibold">
      {label}
      <Select value={value ?? ""} onChange={e => update(Number(e.target.value))}>
        {(versions ?? []).map(d => <option key={d.version} value={d.version}>v{d.version}</option>)}
      </Select>
    </label>;

  const copyYaml = async () => {
    if (!selectedYaml) return;
    try {
      await navigator.clipboard.writeText(selectedYaml);
      setCopy("YAML copied");
    } catch {
      setCopy("Clipboard unavailable");
    }
  };

  return <div className="space-y-5">
    <div className="flex flex-wrap items-start justify-between gap-3">
      <div>
        <Link href="/definitions" className="mb-3 inline-flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground">
          <ArrowLeft size={14} />Workflow definitions
        </Link>
        <h1 className="break-all text-xl font-bold sm:text-2xl">{definitionId}</h1>
        <div className="mt-2 flex flex-wrap items-center gap-2">
          <Badge tone="blue">Hub: {hubId}</Badge>
          <Badge tone="green">{current?.executionMode ?? "Hub-managed"}</Badge>
          <span className="text-xs text-muted-foreground">Published by a remote processor through the Hub REST API</span>
        </div>
      </div>
      <Button disabled={loading} onClick={() => setRefresh(n => n + 1)}>
        <RefreshCw size={14} className={loading ? "animate-spin" : ""} />Refresh
      </Button>
    </div>

    <p className="text-xs leading-5 text-muted-foreground">
      This registry is persisted in the Hub's PostgreSQL database, separately from the legacy
      archived definition catalog. Version comparison covers distinct published versions.
      Re-registering the same Hub definition version overwrites its current metadata;
      earlier same-version snapshots are not retained.
    </p>

    {loading && <p role="status" className="p-6 text-sm text-muted-foreground">Loading Hub definition…</p>}
    {error && <div role="alert" className="rounded-lg border border-rose-500/40 p-4 text-sm text-rose-700 dark:text-rose-300">
      Unable to retrieve the Hub definition: {error}
    </div>}
    {!loading && !error && versions?.length === 0 &&
      <EmptyState title="Hub definition not found"
        description="Start the matching processor to publish its workflow definition, then refresh this page." />}

    {current && <div className="grid gap-4 sm:grid-cols-3">
      <Card className="p-4"><div className="text-xs text-muted-foreground">Registered versions</div>
        <div className="mt-2 text-2xl font-bold">{versions?.length ?? 0}</div></Card>
      <Card className="p-4"><div className="text-xs text-muted-foreground">Steps (selected version)</div>
        <div className="mt-2 text-2xl font-bold">{current.stepCount ?? current.steps?.length ?? 0}</div></Card>
      <Card className="p-4"><div className="text-xs text-muted-foreground">Logical partitions</div>
        <div className="mt-2 text-2xl font-bold">{current.partitions}</div></Card>
    </div>}

    {current && <Card className="overflow-hidden">
      <div className="flex flex-wrap items-end gap-3 border-b border-border p-4">
        <div className="flex gap-2">
          <Button size="sm" variant={mode === "yaml" ? "primary" : "outline"} onClick={() => setMode("yaml")}>Metadata</Button>
          <Button size="sm" variant={mode === "compare" ? "primary" : "outline"} onClick={() => setMode("compare")}
            disabled={(versions?.length ?? 0) < 2}>Compare versions</Button>
        </div>
        {mode === "yaml" ? <>
          {select("Version", selected, n => { setSelected(n); setCopy(""); })}
          <Button size="sm" onClick={copyYaml}>Copy YAML</Button>
          <span role="status" className="text-xs text-muted-foreground">{copy}</span>
        </> : <>
          {select("Base", base, setBase)}
          {select("Target", target, setTarget)}
          <Button size="sm" variant={view === "split" ? "primary" : "outline"} onClick={() => setView("split")}>Split</Button>
          <Button size="sm" variant={view === "unified" ? "primary" : "outline"} onClick={() => setView("unified")}>Unified</Button>
        </>}
      </div>
      {mode === "yaml" ? <YamlViewer text={selectedYaml} /> :
        base === target ? <EmptyState title="Choose two versions" description="Select different versions to compare." /> :
        from && to ? <YamlDiff base={yaml(from)} target={yaml(to)} view={view} /> : null}
    </Card>}

    {current && <Card className="overflow-hidden">
      <div className="border-b border-border px-5 py-4">
        <h2 className="text-sm font-semibold">Published step topology · v{current.version}</h2>
        <p className="mt-1 text-xs text-muted-foreground">Declarative metadata only; executable logic stays in the processor.</p>
      </div>
      <div className="divide-y divide-border">
        {(current.steps ?? []).map((step, i) => <div key={step.stepId ?? i}
          className="flex flex-wrap items-center justify-between gap-3 px-5 py-3">
          <div className="min-w-0">
            <div className="text-sm font-medium">{step.name || step.activityType || step.bodyType || "Step " + (i + 1)}</div>
            <div className="mt-1 break-all font-mono text-[11px] text-muted-foreground">
              {step.bodyType || "Unknown body type"}
              {step.stepId !== undefined ? " · ID " + step.stepId : ""}
            </div>
          </div>
          <Badge tone={step.kind === "Activity" ? "amber" : "blue"}>{step.kind ?? "Step"}</Badge>
        </div>)}
      </div>
    </Card>}
  </div>;
}

"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { ArrowLeft, RefreshCw } from "lucide-react";
import { getJson } from "@/lib/api";
import type { CatalogVersion } from "@/lib/definitions";
import { definitionToYaml } from "@/lib/definition-yaml";
import { Badge, Button, Card, EmptyState, Select } from "./ui";
import { YamlDiff, YamlViewer } from "./definition-yaml";

type LoadState = { kind: "loading" } | { kind: "error"; message: string } | { kind: "loaded"; versions: CatalogVersion[] };

export function DefinitionDetail({ id }: { id: string }) {
  return <DefinitionContent key={id} id={id} />;
}

function DefinitionContent({ id }: { id: string }) {
  const [state, setState] = useState<LoadState>({ kind: "loading" });
  const [refresh, setRefresh] = useState(0);
  const [selected, setSelected] = useState<number | null>(null);
  const [base, setBase] = useState<number | null>(null);
  const [target, setTarget] = useState<number | null>(null);
  const [mode, setMode] = useState<"yaml" | "compare">("yaml");
  const [view, setView] = useState<"split" | "unified">("split");
  const [copy, setCopy] = useState("");

  useEffect(() => {
    const controller = new AbortController();
    setState({ kind: "loading" });
    getJson<CatalogVersion[]>(`/definitions/${encodeURIComponent(id)}/versions`, controller.signal).then(list => {
      if (controller.signal.aborted) return;
      const versions = [...list].sort((a, b) => b.version - a.version);
      const choose = (current: number | null, fallback: number | null) => versions.some(v => v.version === current) ? current : fallback;
      setSelected(current => choose(current, versions[0]?.version ?? null));
      setBase(current => choose(current, versions[1]?.version ?? versions[0]?.version ?? null));
      setTarget(current => choose(current, versions[0]?.version ?? null));
      setState({ kind: "loaded", versions });
    }).catch(reason => {
      if (!controller.signal.aborted) setState({ kind: "error", message: reason instanceof Error ? reason.message : "Unable to load workflow versions" });
    });
    return () => controller.abort();
  }, [id, refresh]);

  const versions = state.kind === "loaded" ? state.versions : [];
  const current = versions.find(v => v.version === selected);
  const from = versions.find(v => v.version === base);
  const to = versions.find(v => v.version === target);
  const selector = (label: string, value: number | null, change: (value: number) => void) => <label className="flex min-w-36 flex-col gap-1.5 text-xs font-semibold">{label}
    <Select value={value ?? ""} onChange={e => change(Number(e.target.value))}>{versions.map(v => <option key={v.version} value={v.version}>v{v.version}</option>)}</Select>
  </label>;
  const copyYaml = async () => {
    if (!current?.schema) return;
    try { await navigator.clipboard.writeText(definitionToYaml(current.schema)); setCopy("YAML copied."); }
    catch { setCopy("Could not copy YAML. Check clipboard permissions."); }
  };

  return <div className="space-y-5">
    <div className="flex flex-wrap items-start justify-between gap-3"><div>
      <Link href="/definitions" className="mb-3 inline-flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground"><ArrowLeft size={14} /> Workflow definitions</Link>
      <h1 className="break-all text-xl font-bold sm:text-2xl">{id}</h1>
      <p className="mt-2 text-sm text-muted-foreground">Read-only archived metadata generated from C# workflow definitions.</p>
    </div><Button disabled={state.kind === "loading"} onClick={() => setRefresh(n => n + 1)}><RefreshCw size={14} /> Refresh</Button></div>
    <p className="text-xs leading-5 text-muted-foreground">YAML omits executable expressions, closures, and step-body implementation. Matching metadata does not prove equivalent workflow behavior. C# remains the authority for execution.</p>
    {state.kind === "loading" && <p role="status" className="p-6 text-sm text-muted-foreground">Loading archived versions…</p>}
    {state.kind === "error" && <div role="alert" className="rounded-lg border border-rose-500/40 p-4 text-sm text-rose-700 dark:text-rose-300">Unable to load archived versions: {state.message}</div>}
    {state.kind === "loaded" && versions.length === 0 && <EmptyState title="No archived versions" description="No snapshots are available for this definition ID." />}
    {versions.length > 0 && <>
      <div className="flex flex-wrap gap-2" aria-label="Definition view">
        <Button size="sm" variant={mode === "yaml" ? "primary" : "outline"} onClick={() => setMode("yaml")} aria-pressed={mode === "yaml"}>YAML</Button>
        <Button size="sm" variant={mode === "compare" ? "primary" : "outline"} onClick={() => setMode("compare")} aria-pressed={mode === "compare"}>Compare versions</Button>
      </div>
      <Card className="overflow-hidden">
        <div className="flex flex-wrap items-end gap-3 border-b border-border p-4">
          {mode === "yaml" ? <>{selector("Version", selected, value => { setSelected(value); setCopy(""); })}<Button size="sm" onClick={copyYaml} disabled={!current?.schema}>Copy YAML</Button><span role="status" className="text-xs text-muted-foreground">{copy}</span></> : <>
            {selector("Base version", base, setBase)}<span className="pb-2 text-muted-foreground">→</span>{selector("Target version", target, setTarget)}
            <div className="ml-auto flex gap-2"><Button size="sm" variant={view === "split" ? "primary" : "outline"} onClick={() => setView("split")} aria-pressed={view === "split"}>Split</Button><Button size="sm" variant={view === "unified" ? "primary" : "outline"} onClick={() => setView("unified")} aria-pressed={view === "unified"}>Unified</Button></div>
          </>}
        </div>
        {mode === "yaml" ? current?.schema ? <YamlViewer text={definitionToYaml(current.schema)} /> : <EmptyState title="Snapshot unavailable" description="This version has no archived schema." /> :
          base === target ? <EmptyState title="Select two different versions" description="Choose distinct base and target versions to compare." /> : from?.schema && to?.schema ? <>
            <div className="grid grid-cols-2 border-b border-border bg-muted/30 text-xs font-semibold"><span className="p-3">Base · v{base}</span><span className="p-3">Target · v{target}</span></div>
            <YamlDiff base={definitionToYaml(from.schema)} target={definitionToYaml(to.schema)} view={view} />
          </> : <EmptyState title="Snapshot unavailable" description="One of the selected versions has no archived schema." />}
      </Card>
      {mode === "compare" && <p className="text-xs text-muted-foreground">Text comparison includes numeric step IDs; inserted steps can shift IDs and produce additional changes. On narrow screens the unified view is shown.</p>}
      <Card className="p-4"><h2 className="text-sm font-semibold">Archived versions</h2><div className="mt-3 space-y-3">{versions.map(v => <div key={v.version} className="text-xs text-muted-foreground"><div className="flex flex-wrap items-center gap-2"><strong className="text-foreground">v{v.version}</strong>{v.driftDetected && <Badge tone="amber">Same-version drift</Badge>}<span>Captured {new Date(v.registeredAt).toLocaleString()}</span><span>SHA <code title={v.schemaSha256}>{v.schemaSha256.slice(0, 12)}</code></span></div>{v.driftDetected && <p className="mt-1 text-amber-700 dark:text-amber-300">Currently registered C# metadata differs from this archived snapshot.</p>}</div>)}</div></Card>
    </>}
  </div>;
}

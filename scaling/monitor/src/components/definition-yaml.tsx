"use client";

import { useMemo, useRef } from "react";
import { definitionDiff, type DiffLine } from "@/lib/definition-yaml";

function Highlight({ text }: { text: string }) {
  const match = /^(\s*(?:-\s+)?)([^\s][^:]*:)(.*)$/.exec(text);
  return match ? <>{match[1]}<span className="text-blue-700 dark:text-sky-300">{match[2]}</span><span className="text-emerald-700 dark:text-emerald-300">{match[3]}</span></> : <>{text}</>;
}

function Line({ line, targetNumber, unified = false }: { line?: DiffLine; targetNumber?: number; unified?: boolean }) {
  const color = line?.kind === "added" ? "bg-emerald-500/15" : line?.kind === "removed" ? "bg-rose-500/15" : "";
  return <div className={`flex min-h-5 whitespace-pre font-mono text-xs leading-5 ${color}`}>
    <span className="w-12 shrink-0 select-none pr-2 text-right text-muted-foreground">{unified && line?.kind === "added" ? "" : line?.number}</span>
    {unified && <span className="w-12 shrink-0 select-none pr-2 text-right text-muted-foreground">{line?.kind === "added" ? line.number : targetNumber}</span>}
    <span className="w-5 shrink-0 select-none font-bold">{line?.kind === "added" ? "+" : line?.kind === "removed" ? "−" : " "}</span>
    <span className="pr-4">{line && <Highlight text={line.text} />}</span>
  </div>;
}

export function YamlViewer({ text }: { text: string }) {
  const lines = text.endsWith("\n") ? text.slice(0, -1).split("\n") : text.split("\n");
  return <div tabIndex={0} aria-label="YAML document" className="compact-scrollbar max-h-[65vh] overflow-auto py-3">{lines.map((line, i) => <Line key={i} line={{ text: line, number: i + 1, kind: "same" }} />)}</div>;
}

export function YamlDiff({ base, target, view }: { base: string; target: string; view: "split" | "unified" }) {
  const rows = useMemo(() => definitionDiff(base, target), [base, target]);
  const left = useRef<HTMLDivElement>(null), right = useRef<HTMLDivElement>(null);
  const sync = (source: HTMLDivElement, other: HTMLDivElement | null) => { if (other && other.scrollTop !== source.scrollTop) other.scrollTop = source.scrollTop; };
  const unchanged = rows.every(row => row.left?.kind === "same" && row.right?.kind === "same");
  return <>
    {unchanged && <p className="p-4 text-sm text-muted-foreground">The YAML documents have no text changes.</p>}
    <div className={view === "split" ? "hidden md:grid md:grid-cols-2" : "hidden"}>
      <div ref={left} onScroll={e => sync(e.currentTarget, right.current)} tabIndex={0} aria-label="Base YAML diff" className="compact-scrollbar min-w-0 max-h-[65vh] overflow-auto border-r border-border py-3">{rows.map((row, i) => <Line key={i} line={row.left} />)}</div>
      <div ref={right} onScroll={e => sync(e.currentTarget, left.current)} tabIndex={0} aria-label="Target YAML diff" className="compact-scrollbar min-w-0 max-h-[65vh] overflow-auto py-3">{rows.map((row, i) => <Line key={i} line={row.right} />)}</div>
    </div>
    <div tabIndex={0} aria-label="Unified YAML diff" className={`compact-scrollbar max-h-[65vh] overflow-auto py-3 ${view === "split" ? "md:hidden" : ""}`}>
      {rows.flatMap((row, i) => row.left?.kind === "same" ? [<Line key={`${i}-same`} line={row.left} targetNumber={row.right?.number} unified />] : [row.left && <Line key={`${i}-left`} line={row.left} unified />, row.right && <Line key={`${i}-right`} line={row.right} unified />])}
    </div>
  </>;
}

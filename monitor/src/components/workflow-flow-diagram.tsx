"use client";

import { useId, useMemo, useState } from "react";
import { GitBranch, Maximize2, Minus, Plus } from "lucide-react";
import { buildWorkflowFlow, type FlowNode, type FlowNodeKind } from "@/lib/flow-graph";
import type { WorkflowTopology } from "@/lib/trace-layout";
import type { ExecutionPointer } from "@/lib/types";
import { Button, cn } from "./ui";

interface AttemptSummary {
  id: string;
  stepId: number | null;
  state: string;
  retryCount: number;
}

type StepTone = "complete" | "failed" | "waiting" | "running" | "pending";

interface StepState {
  label: string;
  tone: StepTone;
  attempts: number;
}

function stepState(node: FlowNode, pointers: readonly ExecutionPointer[], attempts: readonly AttemptSummary[]): StepState {
  const matching = pointers.filter(pointer => pointer.stepId === node.stepId);
  const stepAttempts = attempts.filter(attempt => attempt.stepId === node.stepId);
  if (node.kind === "join") {
    if (matching.some(pointer => pointer.status === "Complete"))
      return { label: "Joined", tone: "complete", attempts: 0 };
    if (matching.some(pointer => pointer.status === "Failed"))
      return { label: "Failed", tone: "failed", attempts: 0 };
    return { label: matching.length ? "Waiting for branches" : "Not started",
      tone: matching.length ? "waiting" : "pending", attempts: 0 };
  }

  const count = stepAttempts.length;
  if (matching.some(pointer => pointer.status === "Complete"))
    return { label: "Completed", tone: "complete", attempts: count };
  if (matching.some(pointer => pointer.status === "Failed"))
    return { label: "Failed", tone: "failed", attempts: count };
  if (matching.some(pointer => pointer.status === "WaitingForEvent"))
    return { label: "Waiting for event", tone: "waiting", attempts: count };
  if (matching.some(pointer => pointer.status === "Sleeping"))
    return { label: "Sleeping / retrying", tone: "waiting", attempts: count };
  if (matching.some(pointer => pointer.status === "Running"))
    return { label: "Running", tone: "running", attempts: count };
  if (matching.some(pointer => pointer.status === "Pending" || pointer.status === "PendingPredecessor"))
    return { label: "Pending", tone: "running", attempts: count };
  if (stepAttempts.some(attempt => attempt.state === "Uncertain"))
    return { label: "Uncertain", tone: "waiting", attempts: count };
  if (stepAttempts.some(attempt => attempt.state === "Failed"))
    return { label: "Attempt failed", tone: "failed", attempts: count };
  return { label: "Not started", tone: "pending", attempts: count };
}

const tones: Record<StepTone, { box: string; dot: string }> = {
  complete: { box: "fill-emerald-500/10 stroke-emerald-500/70", dot: "fill-emerald-500" },
  failed: { box: "fill-rose-500/10 stroke-rose-500/70", dot: "fill-rose-500" },
  waiting: { box: "fill-amber-500/10 stroke-amber-500/70", dot: "fill-amber-500" },
  running: { box: "fill-blue-500/10 stroke-blue-500/70", dot: "fill-blue-500" },
  pending: { box: "fill-card stroke-border", dot: "fill-slate-400" }
};

function wrapLabel(label: string): string[] {
  const words = label.replace(/([a-z])([A-Z])/g, "$1 $2").split(/\s+/).filter(Boolean);
  const rows: string[] = [];
  for (const word of words) {
    const last = rows.length - 1;
    if (last >= 0 && rows[last].length + word.length + 1 <= 27)
      rows[last] += " " + word;
    else rows.push(word);
  }
  if (rows.length <= 2) return rows;
  return [rows[0], rows.slice(1).join(" ").slice(0, 25) + "…"];
}

export function WorkflowFlowDiagram({
  topology, pointers, attempts, selectedStepId, onSelectStep
}: {
  topology: WorkflowTopology;
  pointers: ExecutionPointer[];
  attempts: AttemptSummary[];
  selectedStepId: number | null;
  onSelectStep: (stepId: number) => void;
}) {
  const diagram = useMemo(() => buildWorkflowFlow(topology), [topology]);
  const [scale, setScale] = useState(1);
  const arrowId = useId().replace(/:/g, "");
  const byNodeId = new Map(diagram.nodes.map(node => [node.id, node]));
  const zoom = (value: number) => setScale(Math.max(0.5, Math.min(1.5, Math.round(value * 10) / 10)));

  return <div>
    <div className="flex flex-wrap items-center justify-between gap-2 border-b border-border px-4 py-2">
      <div className="flex flex-wrap items-center gap-3 text-[11px] text-muted-foreground">
        <span className="inline-flex items-center gap-1.5"><span className="size-2 rounded-full bg-emerald-500" />Completed</span>
        <span className="inline-flex items-center gap-1.5"><span className="size-2 rounded-full bg-blue-500" />Active</span>
        <span className="inline-flex items-center gap-1.5"><span className="size-2 rounded-full bg-amber-500" />Waiting</span>
        <span className="inline-flex items-center gap-1.5"><span className="size-2 rounded-full bg-rose-500" />Failed</span>
        <span className="inline-flex items-center gap-1.5"><span className="size-2 rounded-full bg-slate-400" />Not started</span>
      </div>
      <div className="flex items-center gap-1" aria-label="Flow diagram zoom controls">
        <Button size="sm" variant="ghost" aria-label="Zoom out" title="Zoom out"
          disabled={scale <= 0.5} onClick={() => zoom(scale - 0.1)}><Minus size={14} /></Button>
        <span className="min-w-10 text-center text-[11px] tabular-nums text-muted-foreground">{Math.round(scale * 100)}%</span>
        <Button size="sm" variant="ghost" aria-label="Zoom in" title="Zoom in"
          disabled={scale >= 1.5} onClick={() => zoom(scale + 0.1)}><Plus size={14} /></Button>
        <Button size="sm" variant="ghost" aria-label="Reset diagram zoom" title="Reset zoom"
          disabled={scale === 1} onClick={() => zoom(1)}><Maximize2 size={14} /></Button>
      </div>
    </div>

    <div className="compact-scrollbar max-h-[650px] overflow-auto bg-muted/10 p-3">
      {diagram.nodes.length === 0 ? <div className="p-8 text-center text-xs text-muted-foreground">This definition has no steps.</div> :
        <svg viewBox={"0 0 " + diagram.width + " " + diagram.height}
          width={diagram.width * scale} height={diagram.height * scale}
          className="mx-auto block max-w-none"
          role="group" aria-label="Workflow definition flow with observed execution status">
          <defs>
            <marker id={arrowId} markerWidth="8" markerHeight="8" refX="7" refY="3"
              orient="auto" markerUnits="userSpaceOnUse">
              <path d="M0 0 L7 3 L0 6 Z" className="fill-slate-400" />
            </marker>
          </defs>
          {diagram.edges.map(edge => {
            const from = byNodeId.get(edge.from);
            const to = byNodeId.get(edge.to);
            if (!from || !to) return null;
            const startX = from.x + from.width / 2;
            const endX = to.x + to.width / 2;
            const startY = from.y + from.height;
            const endY = to.y;
            const middleY = (startY + endY) / 2;
            return <path key={edge.from + "-" + edge.to}
              d={"M " + startX + " " + startY + " C " + startX + " " + middleY +
                ", " + endX + " " + middleY + ", " + endX + " " + (endY - 5)}
              fill="none" className="stroke-slate-400/70" strokeWidth="1.75"
              markerEnd={"url(#" + arrowId + ")"} />;
          })}
          {diagram.nodes.map(node => {
            const snapshot = stepState(node, pointers, attempts);
            const selected = node.kind !== "join" && node.stepId === selectedStepId;
            const canSelect = node.kind !== "join" &&
              attempts.some(attempt => attempt.stepId === node.stepId);
            const lines = wrapLabel(node.label);
            return <g key={node.id}
              role={canSelect ? "button" : undefined}
              tabIndex={canSelect ? 0 : undefined}
              aria-label={node.label + ", " + snapshot.label +
                (canSelect ? ", select latest execution attempt" : "")}
              aria-pressed={canSelect ? selected : undefined}
              onClick={canSelect ? () => onSelectStep(node.stepId) : undefined}
              onKeyDown={canSelect ? event => {
                if (event.key === "Enter" || event.key === " ") {
                  event.preventDefault();
                  onSelectStep(node.stepId);
                }
              } : undefined}
              className={cn(canSelect && "cursor-pointer outline-none", "group")}>
              <title>{node.label + " · " + snapshot.label +
                (node.kind === "join" ? " (logical synchronization, no independent span)" : "")}</title>
              <rect x={node.x} y={node.y} width={node.width} height={node.height}
                rx={node.kind === "join" ? 18 : 10}
                strokeWidth={selected ? 3 : 1.5}
                className={cn(tones[snapshot.tone].box,
                  selected && "stroke-blue-600",
                  canSelect && "transition-colors group-hover:stroke-blue-500 group-focus:stroke-blue-500")} />
              {node.kind === "fork" && <GitBranch x={node.x + 11} y={node.y + 12}
                size={14} className="text-blue-500" />}
              {lines.map((line, index) =>
                <text key={index} x={node.x + node.width / 2}
                  y={node.y + (lines.length === 1 ? 30 : 22) + index * 15}
                  textAnchor="middle" className="fill-foreground text-[12px] font-semibold">
                  {line}
                </text>)}
              <circle cx={node.x + 16} cy={node.y + node.height - 15} r={4}
                className={tones[snapshot.tone].dot} />
              <text x={node.x + 26} y={node.y + node.height - 11}
                className="fill-muted-foreground text-[10px]">{snapshot.label}</text>
              {snapshot.attempts > 1 && <text x={node.x + node.width - 10}
                y={node.y + node.height - 11} textAnchor="end"
                className="fill-muted-foreground text-[10px]">{snapshot.attempts} attempts</text>}
            </g>;
          })}
        </svg>}
    </div>
    <div className="flex flex-wrap items-center justify-between gap-2 border-t border-border px-4 py-3 text-[11px] text-muted-foreground">
      <span><GitBranch size={13} className="mr-1 inline-block" />
        Definition v{topology.version} · Select an executed step to inspect its latest attempt
      </span>
      <span>{diagram.nodes.length} nodes · {diagram.edges.length} connections
        {diagram.extraPaths > 0 ? " · " + diagram.extraPaths + " additional unconnected path(s)" : ""}
      </span>
    </div>
  </div>;
}

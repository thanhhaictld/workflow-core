"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import {
  Activity, AlertCircle, AlertTriangle, ArrowLeft, CheckCircle2,
  ChevronDown, CircleDashed, Clock3, Code2, Copy, GitBranch, Layers3,
  ListTree, LoaderCircle, RefreshCw, Search, Timer, Workflow, XCircle
} from "lucide-react";
import { getJson } from "@/lib/api";
import { buildTraceLayout, type WorkflowTopology, type TraceLayoutRow } from "@/lib/trace-layout";
import { detailsObject, displayEvent, durationMs, formatDate, formatDuration, formatTime, shortId } from "@/lib/format";
import type {
  ExecutionHistoryRecord, ExecutionPointer, HistoryPage, WorkflowDetail
} from "@/lib/types";
import { Badge, Button, Card, CardHeader, EmptyState, Input, Select, StatusBadge, cn } from "./ui";

type Tab = "trace" | "history" | "state";
type AttemptState = "Completed" | "Returned" | "Waiting" | "Failed" | "Uncertain";

interface TraceAttempt {
  id: string;
  pointerId: string | null;
  stepName: string;
  stepId: number | null;
  retryCount: number;
  startedAt: string;
  endedAt: string | null;
  state: AttemptState;
  duration: number | null;
  related: ExecutionHistoryRecord[];
}

const PAGE_SIZE = 300;
const outcomes = new Set(["StepBodyReturned", "StepBodyYielded", "StepBodyThrew"]);

function buildAttempts(history: ExecutionHistoryRecord[], pointers: ExecutionPointer[]): TraceAttempt[] {
  const byPointer = new Map(pointers.map(pointer => [pointer.id, pointer]));
  const byAttempt = new Map<string, ExecutionHistoryRecord[]>();
  for (const record of history) {
    if (!record.event.stepAttemptId) continue;
    const key = record.event.stepAttemptId;
    byAttempt.set(key, [...(byAttempt.get(key) ?? []), record]);
  }

  return history.filter(record => record.event.eventType === "StepAttemptStarted" && record.event.stepAttemptId)
    .map(start => {
      const pointerId = start.event.executionPointerId;
      const pointer = pointerId ? byPointer.get(pointerId) : undefined;
      const related = byAttempt.get(start.event.stepAttemptId!) ?? [];
      const finished = related.find(record => outcomes.has(record.event.eventType));
      const completed = related.some(record => record.event.eventType === "StepCompleted");
      const failed = related.some(record => ["StepBodyThrew", "StepFailed"].includes(record.event.eventType));
      const returned = finished?.event.eventType === "StepBodyReturned";
      const yielded = finished?.event.eventType === "StepBodyYielded";
      const state: AttemptState = failed ? "Failed"
        : completed ? "Completed"
        : yielded ? "Waiting"
        : returned ? "Returned"
        : "Uncertain";

      const meta = detailsObject(start.event.details);
      const stepName = pointer?.stepName ||
        (typeof meta.stepName === "string" ? meta.stepName : null) ||
        (start.event.stepId === null ? "Unknown step" : `Step ${start.event.stepId}`);
      const endedAt = finished?.event.occurredAt ??
        related.find(record => record.event.eventType === "StepCompleted")?.event.occurredAt ?? null;
      return {
        id: start.event.stepAttemptId!,
        pointerId, stepName, stepId: start.event.stepId,
        retryCount: typeof meta.retryCount === "number" ? meta.retryCount : pointer?.retryCount ?? 0,
        startedAt: start.event.occurredAt, endedAt, state,
        duration: durationMs(start.event.occurredAt, endedAt), related
      };
    }).sort((a, b) => new Date(a.startedAt).getTime() - new Date(b.startedAt).getTime());
}

function attemptTone(state: AttemptState) {
  switch (state) {
    case "Completed": return "green" as const;
    case "Failed": return "red" as const;
    case "Waiting": return "amber" as const;
    case "Returned": return "blue" as const;
    default: return "purple" as const;
  }
}

function TraceWaterfall({ attempts, pointers, topology, selectedId, onSelect }: {
  attempts: TraceAttempt[];
  pointers: ExecutionPointer[];
  topology: WorkflowTopology | null;
  selectedId: string | null;
  onSelect: (id: string) => void;
}) {
  const [collapsed, setCollapsed] = useState<number[]>([]);
  const startTimes = attempts.map(attempt => new Date(attempt.startedAt).getTime());
  const ends = attempts.map(attempt => new Date(attempt.endedAt ?? attempt.startedAt).getTime());
  const min = startTimes.length ? Math.min(...startTimes) : 0;
  const max = ends.length ? Math.max(...ends) : 0;
  const range = Math.max(1, max - min);
  const layout = useMemo(() => topology ? buildTraceLayout(topology) : [], [topology]);
  const attemptsByStep = new Map<number, TraceAttempt[]>();
  for (const attempt of attempts) {
    if (attempt.stepId === null) continue;
    const group = attemptsByStep.get(attempt.stepId) ?? [];
    group.push(attempt);
    attemptsByStep.set(attempt.stepId, group);
  }
  const pointersByStep = new Map<number, ExecutionPointer[]>();
  for (const pointer of pointers) {
    pointersByStep.set(pointer.stepId, [...(pointersByStep.get(pointer.stepId) ?? []), pointer]);
  }

  // A parallel container can have one attempt at fork and a later attempt
  // when its children finish. Join itself does NOT have a journal span.
  function joinState(stepId: number): string {
    const matches = pointersByStep.get(stepId) ?? [];
    if (matches.some(p => p.status === "Complete")) return "Joined";
    if (matches.some(p => p.status === "Failed")) return "Failed";
    if (matches.length) return "Waiting for branches";
    return "Not started";
  }

  function renderAttempt(attempt: TraceAttempt, depth: number, label?: string) {
    const left = ((new Date(attempt.startedAt).getTime() - min) / range) * 100;
    const duration = attempt.duration ?? 0;
    const width = Math.max(1.5, duration / range * 100);
    const color = {
      Completed: "bg-emerald-500", Failed: "bg-rose-500",
      Waiting: "bg-amber-500", Returned: "bg-blue-500", Uncertain: "bg-violet-500"
    }[attempt.state];
    return <button type="button" onClick={() => onSelect(attempt.id)} key={attempt.id}
      aria-pressed={selectedId === attempt.id}
      className={cn("grid w-full grid-cols-[270px_95px_1fr] items-center gap-3 border-b border-border/60 px-4 py-2.5 text-left transition-colors hover:bg-muted/60",
        selectedId === attempt.id && "bg-blue-500/5")}>
      <div className="min-w-0" style={{ paddingLeft: Math.min(depth, 7) * 17 }}>
        <div className="flex items-center gap-1.5 truncate text-xs font-semibold">
          {depth > 0 && <span className="shrink-0 text-muted-foreground/70" aria-hidden="true">└─</span>}
          <span className="truncate">{label ?? attempt.stepName}</span>
        </div>
        <div className="mt-1 flex items-center gap-1.5 text-[11px] text-muted-foreground">
          <span>Attempt {attempt.retryCount + 1}</span><Badge tone={attemptTone(attempt.state)}>{attempt.state}</Badge>
        </div>
      </div>
      <div className="text-xs tabular-nums text-muted-foreground">{formatDuration(attempt.duration)}</div>
      <div className="relative h-7 rounded-md bg-muted/60">
        <span className={cn("absolute top-[7px] h-3.5 rounded-md", color,
          selectedId === attempt.id && "ring-2 ring-offset-2 ring-offset-background ring-blue-500/30")}
          style={{ left: `${Math.min(left, 98)}%`, width: `${Math.min(width, 100 - left)}%`, minWidth: "6px" }} />
      </div>
    </button>;
  }

  function renderMissing(row: Extract<TraceLayoutRow, { kind: "step" | "fork" }>) {
    const matches = pointersByStep.get(row.stepId) ?? [];
    const status = matches.some(p => p.status === "Complete") ? "Completed" :
      matches.length ? matches[0].status : "Not scheduled";
    return <div key={`pending-${row.stepId}`}
      className="grid grid-cols-[270px_95px_1fr] items-center gap-3 border-b border-border/50 px-4 py-2 text-[11px] text-muted-foreground">
      <div className="truncate" style={{ paddingLeft: Math.min(row.depth, 7) * 17 }}>
        {row.depth > 0 && "└─ "}{row.label}
      </div>
      <span>{status}</span><div className="h-1 rounded-full border-t border-dashed border-border" />
    </div>;
  }

  function renderTopologyRow(row: TraceLayoutRow, index: number) {
    if (row.kind === "branch") {
      if (collapsed.includes(row.groupId)) return null;
      return <div key={`branch-${row.groupId}-${row.branchIndex}`}
        className="grid grid-cols-[270px_95px_1fr] items-center gap-3 border-b border-border/40 bg-muted/20 px-4 py-2 text-[11px] font-semibold text-muted-foreground">
        <div className="truncate" style={{ paddingLeft: Math.min(row.depth, 7) * 17 }}>
          <GitBranch size={12} className="mr-1.5 inline-block" />{row.label}
        </div><span>Parallel lane</span><span className="text-[10px] font-normal">Definition lane {row.branchIndex + 1}</span>
      </div>;
    }
    if (row.kind === "join") {
      return <div key={`join-${row.groupId}`}
        className="grid grid-cols-[270px_95px_1fr] items-center gap-3 border-b border-border bg-blue-500/5 px-4 py-2.5 text-xs">
        <div className="font-semibold" style={{ paddingLeft: Math.min(row.depth, 7) * 17 }}>
          <span className="mr-2 text-blue-600" aria-hidden="true">◆</span>{row.label}
        </div>
        <span className="text-[11px] text-muted-foreground">{joinState(row.stepId)}</span>
        <span className="text-[11px] text-muted-foreground">Synchronization marker · no synthetic span</span>
      </div>;
    }

    if (collapsed.some(group => group === row.groupId)) return null;
    const spans = attemptsByStep.get(row.stepId) ?? [];
    const isFork = row.kind === "fork";
    return <div key={`node-${row.stepId}-${index}`}>
      {isFork && <button type="button"
        className="flex w-full items-center gap-2 border-b border-border bg-blue-500/5 px-4 py-2 text-left text-xs font-semibold hover:bg-blue-500/10"
        aria-expanded={!collapsed.includes(row.stepId)}
        onClick={() => setCollapsed(current => current.includes(row.stepId) ?
          current.filter(x => x !== row.stepId) : [...current, row.stepId])}>
        {collapsed.includes(row.stepId) ? <ChevronDown size={15} className="-rotate-90" /> : <ChevronDown size={15} />}
        <GitBranch size={14} />{row.label}
      </button>}
      {spans.length
        ? spans.map(attempt => renderAttempt(attempt, isFork ? row.depth + 1 : row.depth,
            isFork ? "Parallel container" : row.label))
        : renderMissing(row)}
    </div>;
  }

  // Collapsed groups hide only their branch children; the actual join marker
  // and subsequent parent-path steps remain visible.
  const visibleRows = layout.map((row, index) =>
    row.ancestors.some(groupId => collapsed.includes(groupId))
      ? null : renderTopologyRow(row, index)
  );

  // For dynamic paths without a registered definition, retain the accurate
  // chronological attempt stream rather than inventing parent relationships.
  const missingTopologyAttempts = layout.length
    ? attempts.filter(attempt => attempt.stepId === null ||
        !layout.some(row => (row.kind === "step" || row.kind === "fork") && row.stepId === attempt.stepId))
    : attempts;
  return <div className="compact-scrollbar overflow-x-auto">
    <div className="min-w-[825px]">
      <div className="grid grid-cols-[270px_95px_1fr] gap-3 border-b border-border bg-muted/40 px-4 py-3 text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
        <span>Workflow execution · hierarchy</span><span>Duration</span>
        <span>Relative timeline · {formatDuration(range)}</span>
      </div>
      {layout.length ? visibleRows : null}
      {missingTopologyAttempts.map(attempt => renderAttempt(attempt, 0))}
      {!attempts.length && !layout.length && <div className="p-5 text-xs text-muted-foreground">No recorded attempts yet.</div>}
    </div>
  </div>;
}

function Inspector({ selected, workflowId }: { selected: TraceAttempt | undefined; workflowId: string }) {
  if (!selected) return <EmptyState title="Select an attempt" description="Choose a step in the waterfall to inspect its timestamps, retry identity, and emitted journal records." />;
  return <div className="space-y-4 p-5">
    <div className="flex items-center justify-between gap-2">
      <h3 className="text-sm font-semibold">{selected.stepName}</h3>
      <Badge tone={attemptTone(selected.state)}>{selected.state}</Badge>
    </div>
    {selected.state === "Uncertain" && <div className="flex gap-2 rounded-lg border border-violet-400/40 bg-violet-500/10 px-3 py-2 text-xs text-violet-700 dark:text-violet-300">
      <AlertTriangle className="mt-0.5 size-4 shrink-0" />
      Attempt started but no committed result exists yet. External effects might have succeeded. Reconcile before replay.
    </div>}
    <div className="grid grid-cols-2 gap-4 text-xs">
      <div><div className="text-muted-foreground">Step ID</div><div className="mt-1 font-medium">{selected.stepId ?? "—"}</div></div>
      <div><div className="text-muted-foreground">Retry number</div><div className="mt-1 font-medium">{selected.retryCount}</div></div>
      <div><div className="text-muted-foreground">Started</div><div className="mt-1 font-medium">{formatDate(selected.startedAt)}</div></div>
      <div><div className="text-muted-foreground">Duration</div><div className="mt-1 font-medium">{formatDuration(selected.duration)}</div></div>
    </div>
    <div className="space-y-2 border-t border-border pt-4">
      <div className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">Attempt ID</div>
      <div className="flex items-center gap-2">
        <code className="min-w-0 break-all text-[11px]">{selected.id}</code>
        <Button size="sm" variant="ghost" aria-label="Copy step attempt ID" onClick={() => navigator.clipboard.writeText(selected.id)}><Copy size={13} /></Button>
      </div>
      <div className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">Execution pointer</div>
      <div className="break-all font-mono text-[11px]">{selected.pointerId ?? "—"}</div>
      <div className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">Workflow</div>
      <div className="break-all font-mono text-[11px]">{workflowId}</div>
    </div>
    <div className="border-t border-border pt-4">
      <div className="mb-3 text-xs font-semibold">Related journal events</div>
      <div className="space-y-3">
        {selected.related.map(record => <div className="flex gap-2 text-xs" key={record.historyId}>
          <span className="mt-1.5 size-1.5 shrink-0 rounded-full bg-blue-500" />
          <div className="min-w-0"><div className="font-medium">{displayEvent(record.event.eventType)}</div>
            <div className="mt-0.5 text-[11px] text-muted-foreground">{formatTime(record.event.occurredAt)} · #{record.historyId}</div>
          </div>
        </div>)}
      </div>
    </div>
  </div>;
}

function HistoryView({ history }: { history: ExecutionHistoryRecord[] }) {
  const [text, setText] = useState("");
  const [kind, setKind] = useState("All");
  const [expanded, setExpanded] = useState<number | null>(null);
  const kinds = useMemo(() => Array.from(new Set(history.map(item => item.event.eventType))).sort(), [history]);
  const shown = history.filter(record =>
    (kind === "All" || record.event.eventType === kind) &&
    (!text || [record.event.eventType, record.event.stepAttemptId,
      record.event.executionPointerId, record.event.workItemId, String(record.historyId)]
      .some(value => (value ?? "").toLowerCase().includes(text.toLowerCase())))
  ).sort((a, b) => new Date(a.event.occurredAt).getTime() - new Date(b.event.occurredAt).getTime() || a.historyId - b.historyId);

  function eventColor(eventType: string) {
    if (/(Failed|Threw|Error|Dead)/.test(eventType)) return "text-rose-600 bg-rose-500/10";
    if (/(Completed|Committed|Returned)/.test(eventType)) return "text-emerald-600 bg-emerald-500/10";
    if (/(Waiting|Yielded|Scheduled|Sleeping)/.test(eventType)) return "text-amber-600 bg-amber-500/10";
    return "text-blue-600 bg-blue-500/10";
  }

  return <Card className="overflow-hidden">
    <CardHeader title="Immutable event history" description="Events are displayed by occurrence time. History ID is an insertion sequence, not a cross-transaction commit order."
      action={<Badge>{shown.length} visible</Badge>} />
    <div className="flex flex-wrap gap-3 border-b border-border px-5 py-4">
      <div className="relative min-w-[200px] flex-1">
        <Search className="pointer-events-none absolute left-3 top-[11px] text-muted-foreground" size={15} />
        <Input value={text} onChange={e => setText(e.target.value)} placeholder="Search events, attempt IDs..." className="pl-9" aria-label="Search history events" />
      </div>
      <Select value={kind} onChange={e => setKind(e.target.value)} aria-label="Filter event type">
        <option value="All">All event types</option>
        {kinds.map(value => <option key={value} value={value}>{displayEvent(value)}</option>)}
      </Select>
    </div>
    {shown.length === 0 ? <EmptyState title="No matching events" description="Change the event type or search terms, or refresh the workflow history." /> :
    <div className="px-5 py-5">
      <ol className="relative ml-3 border-l border-border">
        {shown.map(record => <li key={record.historyId} className="relative pb-5 pl-6 last:pb-0">
          <span className={cn("absolute -left-[13px] top-0 grid size-6 place-items-center rounded-full border-4 border-card", eventColor(record.event.eventType))}>
            {/(Failed|Threw|Error)/.test(record.event.eventType) ? <XCircle size={12} />
              : /(Completed|Committed)/.test(record.event.eventType) ? <CheckCircle2 size={12} />
              : <CircleDashed size={12} />}
          </span>
          <button onClick={() => setExpanded(value => value === record.historyId ? null : record.historyId)}
            aria-expanded={expanded === record.historyId} className="flex w-full flex-wrap items-center justify-between gap-2 text-left">
            <div className="flex min-w-0 items-center gap-2">
              <span className="text-sm font-medium">{displayEvent(record.event.eventType)}</span>
              <span className="text-[11px] text-muted-foreground">#{record.historyId}</span>
              {record.event.stepId !== null && <Badge>Step {record.event.stepId}</Badge>}
            </div>
            <span className="flex items-center gap-2 text-[11px] text-muted-foreground">{formatDate(record.event.occurredAt)}
              <ChevronDown size={14} className={expanded === record.historyId ? "rotate-180" : ""} /></span>
          </button>
          <div className="mt-1 font-mono text-[11px] text-muted-foreground">
            {record.event.stepAttemptId ? `Attempt ${shortId(record.event.stepAttemptId)}`
              : record.event.executionPointerId ? `Pointer ${shortId(record.event.executionPointerId)}`
              : record.event.workItemId ? `Work ${shortId(record.event.workItemId)}` : "Workflow"}
            {record.event.revision !== null ? ` · Revision ${record.event.revision}` : ""}
          </div>
          {expanded === record.historyId && <div className="mt-3 rounded-lg border border-border bg-muted/40 p-3">
            <div className="mb-2 flex items-center justify-between text-xs">
              <span className="font-semibold">Journal event details</span>
              <Button size="sm" variant="ghost" onClick={event => {
                event.stopPropagation();
                navigator.clipboard.writeText(JSON.stringify(record, null, 2));
              }}><Copy size={12} /> Copy</Button>
            </div>
            <pre className="compact-scrollbar max-h-64 overflow-auto whitespace-pre-wrap break-all font-mono text-[11px] leading-5">{JSON.stringify({
              ...record,
              event: { ...record.event, details: detailsObject(record.event.details) }
            }, null, 2)}</pre>
          </div>}
        </li>)}
      </ol>
    </div>}
  </Card>;
}

function StateView({ detail }: { detail: WorkflowDetail }) {
  const [selected, setSelected] = useState("steps");
  return <div className="space-y-5">
    <div className="flex flex-wrap gap-2">
      {(["steps", "work", "errors"] as const).map(tab => <Button key={tab}
        variant={selected === tab ? "primary" : "outline"} size="sm" onClick={() => setSelected(tab)}>
        {tab === "steps" ? `Execution pointers (${detail.pointers.length})`
          : tab === "work" ? `Work items (${detail.workItems.length})` : `Errors (${detail.errors.length})`}
      </Button>)}
    </div>
    {selected === "steps" && <Card className="overflow-hidden">
      <CardHeader title="Current execution pointers" description="Latest materialized pointer state. The journal preserves prior transitions." />
      {detail.pointers.length === 0 ? <EmptyState title="No pointers" description="No pointer snapshots were found for this workflow." /> :
      <div className="compact-scrollbar overflow-x-auto"><table className="w-full min-w-[740px] text-left text-xs">
        <thead className="bg-muted/50 text-muted-foreground"><tr>
          <th className="px-5 py-3">Step</th><th className="px-5 py-3">Status</th>
          <th className="px-5 py-3">Retries</th><th className="px-5 py-3">Started</th>
          <th className="px-5 py-3">Ended</th><th className="px-5 py-3">Pointer ID</th>
        </tr></thead><tbody>{detail.pointers.map(pointer => <tr key={pointer.id} className="border-t border-border">
          <td className="px-5 py-3 font-medium">{pointer.stepName || `Step ${pointer.stepId}`}</td>
          <td className="px-5 py-3"><StatusBadge status={pointer.status} /></td>
          <td className="px-5 py-3">{pointer.retryCount}</td>
          <td className="whitespace-nowrap px-5 py-3">{formatDate(pointer.startTime)}</td>
          <td className="whitespace-nowrap px-5 py-3">{formatDate(pointer.endTime)}</td>
          <td className="px-5 py-3 font-mono text-muted-foreground">{shortId(pointer.id)}</td>
        </tr>)}</tbody>
      </table></div>}
    </Card>}
    {selected === "work" && <Card className="overflow-hidden">
      <CardHeader title="Distributed work ledger" description="Last 100 workflow work items. Leases and dead-letter state come from PostgreSQL." />
      {detail.workItems.length === 0 ? <EmptyState title="No work items found" description="The distributed work queue has no records for this workflow." /> :
      <div className="compact-scrollbar overflow-x-auto"><table className="w-full min-w-[760px] text-left text-xs">
        <thead className="bg-muted/50 text-muted-foreground"><tr>
          <th className="px-5 py-3">Item</th><th className="px-5 py-3">State</th>
          <th className="px-5 py-3">Attempts</th><th className="px-5 py-3">Due</th>
          <th className="px-5 py-3">Lease expires</th><th className="px-5 py-3">Owner</th>
        </tr></thead><tbody>{detail.workItems.map(work => <tr key={work.id} className="border-t border-border">
          <td className="px-5 py-3 font-mono">{shortId(work.id)}</td>
          <td className="px-5 py-3"><StatusBadge status={work.state} /></td>
          <td className="px-5 py-3 tabular-nums">{work.attempts}</td>
          <td className="whitespace-nowrap px-5 py-3">{formatDate(work.dueAt)}</td>
          <td className="whitespace-nowrap px-5 py-3">{formatDate(work.leaseUntil)}</td>
          <td className="max-w-48 truncate px-5 py-3 font-mono text-muted-foreground">{work.owner || "—"}</td>
        </tr>)}</tbody>
      </table></div>}
    </Card>}
    {selected === "errors" && <Card className="overflow-hidden">
      <CardHeader title="Persisted execution errors" description="Most recent 100 errors. Messages are redacted unless explicitly enabled on the API." />
      {detail.errors.length === 0 ? <EmptyState title="No persisted errors" description="This workflow has no recorded execution errors." /> :
      <div className="divide-y divide-border">{detail.errors.map((error, index) =>
        <div className="flex gap-3 px-5 py-4 text-xs" key={`${error.executionPointerId}-${index}`}>
          <AlertCircle className="mt-0.5 shrink-0 text-rose-500" size={16} />
          <div className="min-w-0"><div className="font-semibold">{formatDate(error.errorTime)}</div>
            <div className="mt-1 font-mono text-muted-foreground">Pointer {shortId(error.executionPointerId)}</div>
            <div className="mt-2 break-words text-muted-foreground">{error.message || "Message redacted by server policy"}</div>
          </div>
        </div>)}</div>}
    </Card>}
  </div>;
}

export function WorkflowTrace({ workflowId }: { workflowId: string }) {
  const [detail, setDetail] = useState<WorkflowDetail | null>(null);
  const [topology, setTopology] = useState<WorkflowTopology | null>(null);
  const [history, setHistory] = useState<ExecutionHistoryRecord[]>([]);
  const [tab, setTab] = useState<Tab>("trace");
  const [selectedAttemptId, setSelectedAttemptId] = useState<string | null>(null);
  const [refreshSeq, setRefreshSeq] = useState(0);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [loading, setLoading] = useState(true);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [hasMore, setHasMore] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [approving, setApproving] = useState(false);
  const [approvalSent, setApprovalSent] = useState(false);

  async function approveOnboarding() {
    setApproving(true); setError(null);
    try {
      const response = await fetch(`/backend/demo/onboardings/${encodeURIComponent(workflowId)}/approve`, {
        method: "POST", headers: { Accept: "application/json" }
      });
      if (!response.ok) {
        throw new Error(`Unable to approve onboarding: HTTP ${response.status}`);
      }
      setApprovalSent(true);
      setRefreshSeq(value => value + 1);
    } catch (cause: unknown) {
      setError(cause instanceof Error ? cause.message : "Unable to approve demo onboarding");
    } finally {
      setApproving(false);
    }
  }

  useEffect(() => {
    if (!autoRefresh) return;
    const timer = window.setInterval(() => setRefreshSeq(value => value + 1), 15_000);
    return () => window.clearInterval(timer);
  }, [autoRefresh]);

  // The first read is deliberately from the beginning. Subsequent reads overlap
  // previous pages on manual reload; duplicate IDs are deduplicated in state.
  useEffect(() => {
    setHistory([]); setDetail(null); setTopology(null); setHasMore(false); setSelectedAttemptId(null);
  }, [workflowId]);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    Promise.all([
      getJson<WorkflowDetail>(`/monitor/workflows/${encodeURIComponent(workflowId)}`, controller.signal),
      getJson<HistoryPage>(`/workflows/${encodeURIComponent(workflowId)}/history?after=0&take=${PAGE_SIZE}`, controller.signal),
      getJson<WorkflowTopology>(`/monitor/workflows/${encodeURIComponent(workflowId)}/topology`, controller.signal)
        .catch(() => null)
    ]).then(([workflow, firstPage, definition]) => {
      setDetail(workflow); setTopology(definition); setError(null);
      setHistory(current => {
        const merged = new Map(current.map(event => [event.historyId, event]));
        firstPage.items.forEach(event => merged.set(event.historyId, event));
        return Array.from(merged.values()).sort((a, b) => a.historyId - b.historyId);
      });
      setHasMore(firstPage.items.length === PAGE_SIZE);
    }).catch((reason: unknown) => {
      if (!controller.signal.aborted) setError(reason instanceof Error ? reason.message : "Unable to read workflow execution");
    }).finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [workflowId, refreshSeq]);

  async function loadMore() {
    const cursor = history.at(-1)?.historyId ?? 0;
    setHistoryLoading(true);
    try {
      const next = await getJson<HistoryPage>(`/workflows/${encodeURIComponent(workflowId)}/history?after=${cursor}&take=${PAGE_SIZE}`);
      setHistory(current => {
        const byId = new Map(current.map(event => [event.historyId, event]));
        next.items.forEach(event => byId.set(event.historyId, event));
        return Array.from(byId.values()).sort((a, b) => a.historyId - b.historyId);
      });
      setHasMore(next.items.length === PAGE_SIZE);
      setError(null);
    } catch (reason: unknown) {
      setError(reason instanceof Error ? reason.message : "Unable to read more history");
    } finally { setHistoryLoading(false); }
  }

  const attempts = useMemo(() => buildAttempts(history, detail?.pointers ?? []), [history, detail]);
  const selected = attempts.find(item => item.id === selectedAttemptId) ?? attempts[0];
  const unresolved = attempts.filter(item => item.state === "Uncertain").length;
  const retryAttempts = attempts.filter(item => item.retryCount > 0).length;
  const workflow = detail?.workflow;

  return <div className="space-y-6">
    <div className="flex flex-wrap items-start justify-between gap-4">
      <div className="min-w-0">
        <Link href="/workflows" className="mb-3 inline-flex items-center gap-1.5 text-xs font-semibold text-muted-foreground hover:text-foreground">
          <ArrowLeft size={14} /> All workflows
        </Link>
        <div className="flex flex-wrap items-center gap-3">
          <div className="grid size-10 place-items-center rounded-xl border border-border bg-card"><GitBranch size={19} /></div>
          <div><h1 className="text-2xl font-bold tracking-tight">{workflow?.definitionId ?? "Workflow execution"}</h1>
            <p className="mt-0.5 text-xs text-muted-foreground">Version {workflow?.version ?? "—"} · Run inspection</p>
          </div>
          {workflow && <StatusBadge status={workflow.status} />}
        </div>
        <div className="mt-4 flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
          <span className="font-mono break-all">{workflowId}</span>
          <Button size="sm" variant="ghost" aria-label="Copy workflow ID" onClick={() => navigator.clipboard.writeText(workflowId)}><Copy size={13} /></Button>
          {workflow?.reference && <Badge>{workflow.reference}</Badge>}
        </div>
      </div>
      <div className="flex flex-wrap gap-2">
        {workflow?.definitionId === "tenant-onboarding-approval" && workflow.status === "Runnable" &&
          <Button variant="primary" disabled={approving || approvalSent} onClick={approveOnboarding}>
            {approving ? <LoaderCircle size={15} className="animate-spin" /> : <CheckCircle2 size={15} />}
            {approvalSent ? "Approval sent" : "Approve sample onboarding"}
          </Button>}
        <label className="flex cursor-pointer items-center gap-2 rounded-lg border border-border bg-card px-3 py-2 text-xs text-muted-foreground">
          <input type="checkbox" className="accent-blue-600" checked={autoRefresh} onChange={e => setAutoRefresh(e.target.checked)} />Auto-refresh
        </label>
        <Button onClick={() => setRefreshSeq(value => value + 1)} disabled={loading}>
          <RefreshCw size={15} className={loading ? "animate-spin" : ""} />Refresh
        </Button>
      </div>
    </div>

    {error && <div role="alert" className="flex items-center gap-2 rounded-lg border border-rose-500/40 bg-rose-500/10 px-4 py-3 text-sm text-rose-700 dark:text-rose-300">
      <AlertTriangle size={16} />{error}
    </div>}
    {loading && !detail && <Card className="flex min-h-56 items-center justify-center gap-3 text-sm text-muted-foreground">
      <LoaderCircle className="animate-spin" size={18} /> Loading workflow execution…
    </Card>}
    {detail && <>
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        <Card className="p-4"><div className="flex items-center gap-2 text-xs text-muted-foreground"><Clock3 size={15} />Started</div>
          <div className="mt-2 text-sm font-semibold">{formatDate(workflow?.createdAt)}</div></Card>
        <Card className="p-4"><div className="flex items-center gap-2 text-xs text-muted-foreground"><Timer size={15} />Elapsed</div>
          <div className="mt-2 text-lg font-semibold tabular-nums">{formatDuration(durationMs(workflow?.createdAt, workflow?.completedAt ?? new Date().toISOString()))}</div></Card>
        <Card className="p-4"><div className="flex items-center gap-2 text-xs text-muted-foreground"><Layers3 size={15} />Step attempts</div>
          <div className="mt-2 text-lg font-semibold">{attempts.length} <span className="text-xs font-normal text-muted-foreground">({retryAttempts} retries)</span></div></Card>
        <Card className="p-4"><div className="flex items-center gap-2 text-xs text-muted-foreground"><AlertCircle size={15} />Errors / uncertain</div>
          <div className="mt-2 text-lg font-semibold">{workflow?.errorCount ?? 0} <span className="text-xs font-normal text-muted-foreground">errors · {unresolved} uncertain</span></div></Card>
      </div>

      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border">
        <div role="tablist" aria-label="Execution review tabs" className="flex gap-5">
          {([
            ["trace", <ListTree key="trace" size={15} />, "Trace"],
            ["history", <Activity key="history" size={15} />, "Event history"],
            ["state", <Code2 key="state" size={15} />, "State & queue"]
          ] as const).map(([value, icon, label]) =>
            <button key={value} role="tab" aria-selected={tab === value} onClick={() => setTab(value)}
              className={cn("flex items-center gap-2 border-b-2 px-1 py-3 text-sm font-medium",
                tab === value ? "border-primary text-foreground" : "border-transparent text-muted-foreground hover:text-foreground")}>
              {icon}{label}
            </button>)}
        </div>
        <span className="pb-2 text-xs text-muted-foreground">{history.length} journal events loaded</span>
      </div>

      {tab === "trace" && <div className="grid items-start gap-5 xl:grid-cols-[minmax(0,1fr)_350px]">
        <div className="min-w-0 space-y-5">
          <Card className="overflow-hidden">
            <CardHeader title="Execution waterfall" description="Definition-derived fork/branch/join hierarchy; durations and statuses come from persisted attempts. Logical parallel lanes are not necessarily separate workers." action={<Badge tone="blue">{attempts.length} spans</Badge>} />
            {attempts.length === 0 && !topology ? <EmptyState title="No step attempts yet" description="The workflow has not executed a step body, or its definition topology is not available." /> :
              <TraceWaterfall attempts={attempts} pointers={detail.pointers} topology={topology}
                selectedId={selected?.id ?? null} onSelect={setSelectedAttemptId} />}
          </Card>
          {unresolved > 0 && <div className="flex gap-3 rounded-xl border border-violet-400/40 bg-violet-500/5 p-4 text-xs text-violet-700 dark:text-violet-300">
            <AlertTriangle className="mt-0.5 shrink-0" size={17} />
            <div><strong>{unresolved} attempt(s) have no committed outcome.</strong>
              <p className="mt-1 leading-5">This may represent in-flight work, a crashed worker, or a lost lease. It does not prove a downstream operation failed.</p></div>
          </div>}
          <Card><CardHeader title="Workflow path" description="Current execution pointers and their last materialized status." />
            <div className="grid gap-3 p-5 sm:grid-cols-2">
              {detail.pointers.map(pointer => <div key={pointer.id} className="rounded-lg border border-border bg-muted/20 p-3">
                <div className="flex items-center justify-between gap-2">
                  <div className="min-w-0 truncate text-xs font-semibold">{pointer.stepName || `Step ${pointer.stepId}`}</div>
                  <StatusBadge status={pointer.status} />
                </div>
                <div className="mt-2 text-[11px] text-muted-foreground">Step {pointer.stepId} · {pointer.retryCount} retries</div>
                <div className="mt-1 font-mono text-[10px] text-muted-foreground">{shortId(pointer.id)}</div>
              </div>)}
              {!detail.pointers.length && <p className="text-xs text-muted-foreground">No pointer state available.</p>}
            </div>
          </Card>
        </div>
        <Card className="min-w-0 overflow-hidden xl:sticky xl:top-[90px]">
          <CardHeader title="Span inspector" description="Attempt details and correlated journal records" />
          <Inspector selected={selected} workflowId={workflowId} />
        </Card>
      </div>}

      {tab === "history" && <HistoryView history={history} />}
      {tab === "state" && <StateView detail={detail} />}

      {hasMore && <div className="flex justify-center">
        <Button variant="outline" disabled={historyLoading} onClick={loadMore}>
          {historyLoading ? <LoaderCircle size={15} className="animate-spin" /> : <ChevronDown size={15} />}
          Load more journal events
        </Button>
      </div>}
      <p className="text-xs leading-5 text-muted-foreground">
        This is an execution review of persisted attempts, not deterministic Temporal replay.
        Event IDs are insertion ordered; database commit visibility and remote side effects may differ.
      </p>
    </>}
  </div>;
}

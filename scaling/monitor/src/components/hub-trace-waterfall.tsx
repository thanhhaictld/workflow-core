"use client";

import { useMemo, useState } from "react";
import { AlertTriangle, ChevronDown, ChevronRight, Clock3, ZoomIn, ZoomOut } from "lucide-react";
import { durationMs, formatDuration, formatTime, shortId } from "@/lib/format";
import { Badge, cn } from "./ui";
import { hubTimerWindows } from "@/lib/hub-timer-windows";

export type HubTraceAttempt = {
  epoch: number;
  attemptNumber: number;
  workerId: string;
  claimedAt: string;
  finishedAt: string | null;
  outcome: "Leased" | "Completed" | "RetryScheduled" | "Failed" | "LeaseExpired" | "Cancelled";
  source?: "lease" | "native-step";
  traceId?: string | null;
  spanId?: string | null;
};
export type HubTraceStep = {
  stepIndex: number;
  taskId: string;
  activityType: string;
  version: number;
  state: string;
  readyAt: string | null;
  attempts: HubTraceAttempt[];
  executionPointerId?: string | null;
  dispatchKey?: string | null;
};
export type HubTraceEvent = {
  stepIndex: number;
  activityType?: string;
  eventType: string;
  occurredAt: string;
  taskId?: string;
  executionPointerId?: string | null;
  payload?: unknown;
};
type WaterfallWorkflow = {
  createdAt: string;
  completedAt: string | null;
  state: string;
  definitionId: string;
};

const attemptsTone: Record<HubTraceAttempt["outcome"], {
  bar: string; label: string; tone: "green" | "blue" | "amber" | "red";
}> = {
  Leased: { bar: "bg-blue-500", label: "Leased", tone: "blue" },
  Completed: { bar: "bg-emerald-500", label: "Completed", tone: "green" },
  RetryScheduled: { bar: "bg-amber-500", label: "Retry scheduled", tone: "amber" },
  Failed: { bar: "bg-rose-500", label: "Failed", tone: "red" },
  LeaseExpired: { bar: "bg-violet-500", label: "Lease expired / replaced", tone: "amber" },
  Cancelled: { bar: "bg-slate-500", label: "Cancelled", tone: "amber" },
};

export function HubTraceWaterfall({
  workflow, trace, history, selectedStep, onSelectStep, grafana,
}: {
  workflow: WaterfallWorkflow;
  trace: HubTraceStep[];
  history: HubTraceEvent[];
  grafana?: { url: string | null; tempoUid: string };
  selectedStep: number;
  onSelectStep: (step: number) => void;
}) {
  const tempoLink = (traceId: string | null | undefined) => {
    if (!traceId || !/^[0-9a-f]{32}$/i.test(traceId) || !grafana?.url) return null;
    try {
      const base = new URL(grafana.url);
      if (!["http:", "https:"].includes(base.protocol)) return null;
      base.pathname = base.pathname.replace(/\\/$/, "") + "/explore";
      base.searchParams.set("left", JSON.stringify({
        datasource: grafana.tempoUid,
        queries: [{ refId: "A", queryType: "traceId", query: traceId }],
        range: { from: "now-24h", to: "now" },
      }));
      return base.toString();
    } catch { return null; }
  };
  const [collapsed, setCollapsed] = useState<string[]>([]);
  const [showQueue, setShowQueue] = useState(true);
  const [zoom, setZoom] = useState(1);
  const gridStyle = { gridTemplateColumns: `290px 118px minmax(${640 * zoom}px,1fr)` };
  const [selectedAttempt, setSelectedAttempt] = useState<string | null>(null);

  const start = Date.parse(workflow.createdAt);
  // A running workflow has an open-ended root span; end at render time.
  const now = Date.now();
  const end = Math.max(start + 1, Date.parse(workflow.completedAt ?? "") || now);
  const spanMs = Math.max(1, end - start);
  const offset = (stamp: string) => {
    const time = Date.parse(stamp);
    return Math.max(0, Math.min(100, (time - start) / spanMs * 100));
  };
  const range = (from: string, to: string) => {
    const x = offset(from), y = offset(to);
    return { left: x + "%", width: Math.max(0.35, y - x) + "%" };
  };
  const eventTime = (step: HubTraceStep) => history.find(event =>
    (step.executionPointerId ? event.executionPointerId === step.executionPointerId : event.stepIndex === step.stepIndex) && (event.eventType === "StepCompleted" ||
      event.eventType === "StepFailed"))?.occurredAt;

  // A timer wait is a durable span reconstructed from actual Schedule/Fire
  // timestamps, not an artificial long-lived OpenTelemetry activity.
  const timerWindows = useMemo(() => hubTimerWindows(history), [history]);
  const segments = [
    ...trace.map(step => ({ kind: "activity" as const, step, stepIndex: step.stepIndex })),
    ...timerWindows.map(timer => ({ kind: "timer" as const, timer, stepIndex: timer.stepIndex })),
  ].sort((a, b) => a.stepIndex - b.stepIndex);

  const allAttempts = useMemo(() => trace.flatMap(step => step.attempts), [trace]);
  const hasRecordedTimings = allAttempts.length > 0;
  const hasHistoricalEvents = history.some(event => event.eventType === "StepCompleted" || event.eventType === "StepFailed");
  const isFinished = ["Complete", "Failed", "Terminated"].includes(workflow.state);
  const rootDuration = durationMs(workflow.createdAt, workflow.completedAt ?? new Date(now).toISOString());

  const bar = (from: string, to: string, className: string, title: string) =>
    <div className={cn("absolute top-[8px] h-3.5 rounded-sm", className)}
      style={range(from, to)} title={title} />;

  const lane = (children: React.ReactNode) =>
    <div className="relative h-8 min-w-80 overflow-hidden rounded-md border border-border/40 bg-muted/40">
      <div className="absolute inset-y-0 left-1/4 w-px bg-border/60" />
      <div className="absolute inset-y-0 left-2/4 w-px bg-border/60" />
      <div className="absolute inset-y-0 left-3/4 w-px bg-border/60" />
      {children}
    </div>;

  return <div className="overflow-x-auto">
    <div style={{ minWidth: `${440 + 640 * zoom}px` }}>
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border p-4">
        <div>
          <p className="text-sm font-semibold">Distributed execution waterfall</p>
          <p className="mt-1 text-xs text-muted-foreground">
            Activity bars use durable Hub claim/finish timestamps. Native-only step bars use
            worker-observed start/finish times committed with workflow history.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-4">
          <label className="flex items-center gap-2 text-xs text-muted-foreground">
            <input type="checkbox" checked={showQueue} onChange={e => setShowQueue(e.target.checked)}
              className="accent-blue-600"/> Show queue / scheduling wait
          </label>
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            <button type="button" aria-label="Zoom out timeline"
              disabled={zoom <= 1} onClick={() => setZoom(current => Math.max(1, current / 2))}
              className="rounded-md border border-border p-1.5 disabled:opacity-40 hover:bg-muted">
              <ZoomOut size={15}/>
            </button>
            <span className="min-w-8 text-center font-mono">{zoom}x</span>
            <button type="button" aria-label="Zoom in timeline"
              disabled={zoom >= 8} onClick={() => setZoom(current => Math.min(8, current * 2))}
              className="rounded-md border border-border p-1.5 disabled:opacity-40 hover:bg-muted">
              <ZoomIn size={15}/>
            </button>
          </div>
        </div>
      </div>
      <div style={gridStyle} className="grid items-end gap-3 border-b border-border bg-muted/30 px-4 py-3 text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
        <span>Workflow / activity / attempt</span><span>Duration</span>
        <div className="flex justify-between font-mono font-normal normal-case tracking-normal">
          {[0, 25, 50, 75, 100].map(pct =>
            <span key={pct}>{formatDuration(spanMs * pct / 100)}</span>)}
        </div>
      </div>

      <div style={gridStyle} className="grid items-center gap-3 border-b border-border px-4 py-3">
        <div className="flex items-center gap-2 text-sm font-semibold">
          <Clock3 size={15}/> {workflow.definitionId}
          <Badge tone={workflow.state === "Complete" ? "green" : "blue"}>{workflow.state}</Badge>
        </div>
        <span className="text-xs tabular-nums">{formatDuration(rootDuration)}</span>
        {lane(bar(workflow.createdAt, workflow.completedAt ?? new Date(now).toISOString(),
          "bg-blue-500/60", "Whole workflow: " + formatDuration(rootDuration)))}
      </div>

      {segments.map(segment => {
        if (segment.kind === "timer") {
          const t = segment.timer;
          const until = t.finish ?? new Date(now).toISOString();
          const elapsed = durationMs(t.start, until);
          return <div key={"timer:" + t.key} style={gridStyle}
            className={cn("grid items-center gap-3 border-b border-border/70 px-4 py-2.5",
              selectedStep === t.stepIndex ? "bg-blue-500/5" : "hover:bg-muted/35")}>
            <button type="button" onClick={() => onSelectStep(t.stepIndex)}
              className="flex items-center gap-2 text-left">
              <Clock3 size={15}/>
              <span className="truncate text-xs font-semibold">{t.label}</span>
              <Badge tone={t.outcome === "TimerFired" ? "green" :
                t.outcome === "TimerCancelled" ? "amber" : "blue"}>
                {t.outcome === "TimerFired" ? "Fired" :
                  t.outcome === "TimerCancelled" ? "Cancelled" : "Waiting"}
              </Badge>
            </button>
            <span className="text-xs tabular-nums text-muted-foreground">
              {formatDuration(elapsed)}
            </span>
            {lane(bar(t.start, until,
              t.outcome === "TimerCancelled" ? "bg-amber-500/70" : "bg-violet-500/70",
              "Durable timer wait: " + formatDuration(elapsed)))}
          </div>;
        }
        const step = segment.step;
        const rowId = step.taskId + ":" + step.stepIndex + ":" + (step.executionPointerId ?? "");
        const folded = collapsed.includes(rowId);
        const terminalEvent = eventTime(step);
        const finished = step.attempts.filter(x => x.finishedAt);
        const firstClaim = step.attempts[0]?.claimedAt;
        const queuedMs = step.readyAt && firstClaim
          ? durationMs(step.readyAt, firstClaim) : null;
        const stateTone = step.state === "Completed" ? "green" as const
          : step.state === "Failed" ? "red" as const : "slate" as const;
        return <div key={rowId}>
          <div style={gridStyle} className={cn("grid items-center gap-3 border-b border-border/70 px-4 py-2.5",
            selectedStep === step.stepIndex ? "bg-blue-500/5" : "hover:bg-muted/35")}>
            <button type="button"
              onClick={() => {
                onSelectStep(step.stepIndex);
                setCollapsed(before => folded ? before.filter(n => n !== rowId)
                  : [...before, rowId]);
              }}
              className="flex min-w-0 items-center gap-2 text-left">
              {folded ? <ChevronRight size={15}/> : <ChevronDown size={15}/>}
              <span className="truncate text-xs font-semibold">{step.activityType}</span>
              {step.executionPointerId && <span className="font-mono text-[10px] text-muted-foreground" title={step.dispatchKey ?? step.executionPointerId}>
                {shortId(step.executionPointerId)}
              </span>}
              <Badge tone={stateTone}>{step.state}</Badge>
            </button>
            <span className="text-xs tabular-nums text-muted-foreground">
              {finished.length > 0 ? formatDuration(finished.reduce((sum, attempt) =>
                sum + (durationMs(attempt.claimedAt, attempt.finishedAt) ?? 0), 0))
                : step.attempts.length > 0 ? "In progress" : "—"}
            </span>
            {lane(<>
              {step.attempts.map(attempt => bar(attempt.claimedAt,
                attempt.finishedAt ?? new Date(now).toISOString(),
                attemptsTone[attempt.outcome].bar + "/70",
                `${attempt.source === "native-step" ? "Native step" : "Lease attempt"} ${attempt.attemptNumber}: ${attemptsTone[attempt.outcome].label}`))}
              {!step.attempts.length && terminalEvent && <div
                className="absolute top-[5px] h-5 w-0.5 bg-emerald-500"
                style={{ left: offset(terminalEvent) + "%" }}
                title="Historical event timestamp; exact start/duration not recorded"/>}
            </>)}
          </div>

          {!folded && <>
            {showQueue && step.readyAt && firstClaim && queuedMs !== null && queuedMs > 0 &&
              <div style={gridStyle} className="grid items-center gap-3 border-b border-border/50 px-4 py-1.5">
                <span className="pl-8 text-xs text-muted-foreground">↳ Queue / scheduling wait</span>
                <span className="text-xs tabular-nums text-muted-foreground">{formatDuration(queuedMs)}</span>
                {lane(bar(step.readyAt, firstClaim, "bg-slate-400/60",
                  "Eligible to claim → first lease"))}
              </div>}

            {step.attempts.map(attempt => {
              const key = rowId + ":" + attempt.epoch + ":" + attempt.attemptNumber;
              const active = selectedAttempt === key;
              const until = attempt.finishedAt ?? new Date(now).toISOString();
              const elapsed = durationMs(attempt.claimedAt, until);
              const tone = attemptsTone[attempt.outcome];
              return <div key={key}>
                <button type="button"
                  onClick={() => {
                    onSelectStep(step.stepIndex);
                    setSelectedAttempt(active ? null : key);
                  }}
                  aria-pressed={active}
                  style={gridStyle} className={cn("grid w-full items-center gap-3 border-b border-border/50 px-4 py-1.5 text-left",
                    active ? "bg-blue-500/10" : "hover:bg-muted/40")}>
                  <span className="flex min-w-0 items-center gap-2 pl-8 text-xs">
                    <span className="truncate">↳ {attempt.source === "native-step" ? "Native execution" : "Lease attempt"} {attempt.attemptNumber}</span>
                    <Badge tone={tone.tone}>{tone.label}</Badge>
                  </span>
                  <span className="text-xs tabular-nums text-muted-foreground">{formatDuration(elapsed)}</span>
                  {lane(bar(attempt.claimedAt, until, tone.bar,
                    `${tone.label} · ${attempt.workerId} · ${formatDuration(elapsed)}`))}
                </button>
                {active && <div className="grid grid-cols-2 gap-3 border-b border-border bg-muted/40 p-4 pl-12 text-xs lg:grid-cols-4">
                  <div><p className="text-muted-foreground">Worker</p>
                    <p className="mt-1 break-all font-mono">{attempt.workerId}</p></div>
                  <div><p className="text-muted-foreground">{attempt.source === "native-step" ? "Step started" : "Claimed"}</p>
                    <p className="mt-1 font-medium">{formatTime(attempt.claimedAt)}</p></div>
                  <div><p className="text-muted-foreground">{attempt.source === "native-step" ? "Step finished" : "Finished"}</p>
                    <p className="mt-1 font-medium">{formatTime(attempt.finishedAt)}</p></div>
                  <div><p className="text-muted-foreground">Fence epoch</p>
                    <p className="mt-1 font-medium">{attempt.epoch} · {shortId(step.taskId)}</p></div>
                  {attempt.traceId && <div className="col-span-2 lg:col-span-4">
                    <p className="text-muted-foreground">OpenTelemetry correlation</p>
                    <p className="mt-1 break-all font-mono">{attempt.traceId}
                      {attempt.spanId && <span className="ml-2 text-muted-foreground">span {attempt.spanId}</span>}
                    </p>
                    {tempoLink(attempt.traceId) && <a href={tempoLink(attempt.traceId)!}
                      target="_blank" rel="noopener noreferrer"
                      className="mt-1 inline-block font-medium text-blue-600 underline dark:text-blue-400">
                      Open trace in Grafana Tempo
                    </a>}
                  </div>}
                </div>}
              </div>;
            })}

            {!step.attempts.length && <div className="border-b border-border/50 px-4 py-2 pl-12 text-xs text-muted-foreground">
              {terminalEvent
                ? "Historical completion marker only: no claim timestamps were persisted for this run."
                : step.readyAt
                  ? "Eligible; waiting to be claimed. No worker attempt recorded yet."
                  : "Waiting for predecessor. No activity duration is inferred."}
            </div>}
          </>}
        </div>;
      })}
      <div className="flex flex-wrap items-center gap-5 border-t border-border bg-muted/20 px-4 py-3 text-xs text-muted-foreground">
        <span><span className="mr-1 inline-block h-2 w-4 rounded-sm bg-slate-400/60"/>Ready → claim</span>
        <span><span className="mr-1 inline-block h-2 w-4 rounded-sm bg-blue-500"/>Leased</span>
        <span><span className="mr-1 inline-block h-2 w-4 rounded-sm bg-emerald-500"/>Completed</span>
        <span><span className="mr-1 inline-block h-2 w-4 rounded-sm bg-amber-500"/>Retry</span>
        <span><span className="mr-1 inline-block h-2 w-4 rounded-sm bg-rose-500"/>Failed</span>
        <span><span className="mr-1 inline-block h-2 w-4 rounded-sm bg-violet-500"/>Durable timer wait</span>
        <span className="inline-flex items-center gap-1"><AlertTriangle size={13}/>
          Native spans measure worker-observed step duration; lease bars include dispatch and transport delay.
        </span>
      </div>
      {!hasRecordedTimings && isFinished && hasHistoricalEvents && <p className="px-4 py-3 text-xs text-amber-700 dark:text-amber-300">
        No recorded step-duration spans are available for this execution. Older native workflows
        and historical activities may only have durable completion events; exact start/end
        timestamps are not inferred from commit times.
      </p>}
    </div>
  </div>;
}

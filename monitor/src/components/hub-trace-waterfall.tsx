"use client";

import { useMemo, useState } from "react";
import { AlertTriangle, ChevronDown, ChevronRight, Clock3 } from "lucide-react";
import { durationMs, formatDuration, formatTime, shortId } from "@/lib/format";
import { Badge, cn } from "./ui";

export type HubTraceAttempt = {
  epoch: number;
  attemptNumber: number;
  workerId: string;
  claimedAt: string;
  finishedAt: string | null;
  outcome: "Leased" | "Completed" | "RetryScheduled" | "Failed" | "LeaseExpired";
};
export type HubTraceStep = {
  stepIndex: number;
  taskId: string;
  activityType: string;
  version: number;
  state: string;
  readyAt: string | null;
  attempts: HubTraceAttempt[];
};
export type HubTraceEvent = {
  stepIndex: number;
  eventType: string;
  occurredAt: string;
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
};

export function HubTraceWaterfall({
  workflow, trace, history, selectedStep, onSelectStep,
}: {
  workflow: WaterfallWorkflow;
  trace: HubTraceStep[];
  history: HubTraceEvent[];
  selectedStep: number;
  onSelectStep: (step: number) => void;
}) {
  const [collapsed, setCollapsed] = useState<number[]>([]);
  const [showQueue, setShowQueue] = useState(true);
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
  const eventTime = (step: number) => history.find(event =>
    event.stepIndex === step && (event.eventType === "StepCompleted" ||
      event.eventType === "StepFailed"))?.occurredAt;

  const allAttempts = useMemo(() => trace.flatMap(step => step.attempts), [trace]);
  const hasRecordedClaims = allAttempts.length > 0;
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
    <div className="min-w-[900px]">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border p-4">
        <div>
          <p className="text-sm font-semibold">Distributed execution waterfall</p>
          <p className="mt-1 text-xs text-muted-foreground">
            Hub lease spans are recorded at claim and committed completion.
            Exact worker CPU/handler time is available in OpenTelemetry traces.
          </p>
        </div>
        <label className="flex items-center gap-2 text-xs text-muted-foreground">
          <input type="checkbox" checked={showQueue} onChange={e => setShowQueue(e.target.checked)}
            className="accent-blue-600"/> Show queue / scheduling wait
        </label>
      </div>
      <div className="grid grid-cols-[290px_118px_minmax(370px,1fr)] items-end gap-3 border-b border-border bg-muted/30 px-4 py-3 text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
        <span>Workflow / activity / attempt</span><span>Duration</span>
        <div className="flex justify-between font-mono font-normal normal-case tracking-normal">
          {[0, 25, 50, 75, 100].map(pct =>
            <span key={pct}>{formatDuration(spanMs * pct / 100)}</span>)}
        </div>
      </div>

      <div className="grid grid-cols-[290px_118px_minmax(370px,1fr)] items-center gap-3 border-b border-border px-4 py-3">
        <div className="flex items-center gap-2 text-sm font-semibold">
          <Clock3 size={15}/> {workflow.definitionId}
          <Badge tone={workflow.state === "Complete" ? "green" : "blue"}>{workflow.state}</Badge>
        </div>
        <span className="text-xs tabular-nums">{formatDuration(rootDuration)}</span>
        {lane(bar(workflow.createdAt, workflow.completedAt ?? new Date(now).toISOString(),
          "bg-blue-500/60", "Whole workflow: " + formatDuration(rootDuration)))}
      </div>

      {trace.map(step => {
        const folded = collapsed.includes(step.stepIndex);
        const terminalEvent = eventTime(step.stepIndex);
        const finished = step.attempts.filter(x => x.finishedAt);
        const firstClaim = step.attempts[0]?.claimedAt;
        const queuedMs = step.readyAt && firstClaim
          ? durationMs(step.readyAt, firstClaim) : null;
        const stateTone = step.state === "Completed" ? "green" as const
          : step.state === "Failed" ? "red" as const : "slate" as const;
        return <div key={step.taskId}>
          <div className={cn("grid grid-cols-[290px_118px_minmax(370px,1fr)] items-center gap-3 border-b border-border/70 px-4 py-2.5",
            selectedStep === step.stepIndex ? "bg-blue-500/5" : "hover:bg-muted/35")}>
            <button type="button"
              onClick={() => {
                onSelectStep(step.stepIndex);
                setCollapsed(before => folded ? before.filter(n => n !== step.stepIndex)
                  : [...before, step.stepIndex]);
              }}
              className="flex min-w-0 items-center gap-2 text-left">
              {folded ? <ChevronRight size={15}/> : <ChevronDown size={15}/>}
              <span className="truncate text-xs font-semibold">{step.activityType}</span>
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
                `Attempt ${attempt.attemptNumber}: ${attemptsTone[attempt.outcome].label}`))}
              {!step.attempts.length && terminalEvent && <div
                className="absolute top-[5px] h-5 w-0.5 bg-emerald-500"
                style={{ left: offset(terminalEvent) + "%" }}
                title="Historical event timestamp; exact start/duration not recorded"/>}
            </>)}
          </div>

          {!folded && <>
            {showQueue && step.readyAt && firstClaim && queuedMs !== null && queuedMs > 0 &&
              <div className="grid grid-cols-[290px_118px_minmax(370px,1fr)] items-center gap-3 border-b border-border/50 px-4 py-1.5">
                <span className="pl-8 text-xs text-muted-foreground">↳ Queue / scheduling wait</span>
                <span className="text-xs tabular-nums text-muted-foreground">{formatDuration(queuedMs)}</span>
                {lane(bar(step.readyAt, firstClaim, "bg-slate-400/60",
                  "Eligible to claim → first lease"))}
              </div>}

            {step.attempts.map(attempt => {
              const key = step.taskId + ":" + attempt.epoch;
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
                  className={cn("grid w-full grid-cols-[290px_118px_minmax(370px,1fr)] items-center gap-3 border-b border-border/50 px-4 py-1.5 text-left",
                    active ? "bg-blue-500/10" : "hover:bg-muted/40")}>
                  <span className="flex min-w-0 items-center gap-2 pl-8 text-xs">
                    <span className="truncate">↳ Attempt {attempt.attemptNumber}</span>
                    <Badge tone={tone.tone}>{tone.label}</Badge>
                  </span>
                  <span className="text-xs tabular-nums text-muted-foreground">{formatDuration(elapsed)}</span>
                  {lane(bar(attempt.claimedAt, until, tone.bar,
                    `${tone.label} · ${attempt.workerId} · ${formatDuration(elapsed)}`))}
                </button>
                {active && <div className="grid grid-cols-2 gap-3 border-b border-border bg-muted/40 p-4 pl-12 text-xs lg:grid-cols-4">
                  <div><p className="text-muted-foreground">Worker</p>
                    <p className="mt-1 break-all font-mono">{attempt.workerId}</p></div>
                  <div><p className="text-muted-foreground">Claimed</p>
                    <p className="mt-1 font-medium">{formatTime(attempt.claimedAt)}</p></div>
                  <div><p className="text-muted-foreground">Finished</p>
                    <p className="mt-1 font-medium">{formatTime(attempt.finishedAt)}</p></div>
                  <div><p className="text-muted-foreground">Fence epoch</p>
                    <p className="mt-1 font-medium">{attempt.epoch} · {shortId(step.taskId)}</p></div>
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
        <span className="inline-flex items-center gap-1"><AlertTriangle size={13}/>
          Lease expiry means the Hub reclaimed ownership, not proof of stopped side effects.
        </span>
      </div>
      {!hasRecordedClaims && <p className="px-4 py-3 text-xs text-amber-700 dark:text-amber-300">
        This execution predates the durable attempt journal. Historical events are displayed as markers,
        but missing start/end timestamps are intentionally not reconstructed.
      </p>}
    </div>
  </div>;
}

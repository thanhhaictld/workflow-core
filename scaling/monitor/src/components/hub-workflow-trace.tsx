"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import {
  Activity, AlertTriangle, ArrowLeft, CheckCircle2, ChevronRight,
  Clock3, GitBranch, RefreshCw, Timer, Workflow,
} from "lucide-react";
import { durationMs, formatDate, formatDuration, shortId } from "@/lib/format";
import { Badge, Button, Card, CardHeader, StatusBadge } from "./ui";
import { HubTraceWaterfall, type HubTraceStep } from "./hub-trace-waterfall";

type HubWorkflow = {
  id: string;
  hubId: string;
  definitionId: string;
  version: number;
  partitionId: number;
  state: string;
  nextStep: number;
  stepCount: number;
  results: Record<string, unknown>;
  error: string | null;
  createdAt: string;
  completedAt: string | null;
  executionMode?: string;
  nativeStatus?: string | null;
};
type HubHistory = {
  stepIndex: number;
  taskId: string;
  activityType: string;
  attempts: number;
  eventType: string;
  occurredAt: string;
  payload: unknown;
  executionPointerId?: string | null;
  parentPointerId?: string | null;
  scope?: string[] | null;
  attempt?: number | null;
};
type HubDefinition = {
  hubId: string;
  definitionId: string;
  version: number;
  steps: { activityType: string | null; version?: number; name?: string }[];
};
type HubDetailResponse = {
  workflow: HubWorkflow;
  history: HubHistory[];
  trace: HubTraceStep[];
  definition: HubDefinition | null;
  nativeRuntime: { status: string; revision: number; snapshot: unknown } | null;
};
type Tab = "trace" | "history" | "state";

const eventTone = (event: string) => event === "StepCompleted" || event === "TimerFired" || event === "WorkflowCompleted" ? "green" as const
  : event === "StepFailed" || event === "WorkflowError" ? "red" as const : "amber" as const;

export function HubWorkflowTrace({ workflowId }: { workflowId: string }) {
  const [detail, setDetail] = useState<HubDetailResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [refresh, setRefresh] = useState(0);
  const [tab, setTab] = useState<Tab>("trace");
  const [selectedStep, setSelectedStep] = useState(0);
  const [selectedEvent, setSelectedEvent] = useState<number | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    fetch("/api/hub-workflows/" + encodeURIComponent(workflowId), {
      cache: "no-store",
      signal: controller.signal,
    }).then(async response => {
      if (!response.ok) {
        const result = await response.json().catch(() => ({}));
        throw new Error(result.error || "Unable to load Hub workflow");
      }
      return response.json() as Promise<HubDetailResponse>;
    }).then(result => {
      if (!controller.signal.aborted) {
        setDetail(result);
        setError(null);
      }
    }).catch(reason => {
      if (!controller.signal.aborted)
        setError(reason instanceof Error ? reason.message : "Unable to load Hub workflow");
    }).finally(() => {
      if (!controller.signal.aborted) setLoading(false);
    });
    return () => controller.abort();
  }, [workflowId, refresh]);

  const workflow = detail?.workflow;
  const history = detail?.history ?? [];
  const trace = detail?.trace ?? [];
  const count = workflow?.stepCount ?? 0;
  const steps = useMemo(() => Array.from({ length: count }, (_, index) => {
    const events = history.filter(record => record.stepIndex === index);
    const last = events[events.length - 1];
    const declared = detail?.definition?.steps[index]?.activityType ?? detail?.definition?.steps[index]?.name;
    const completed = events.some(event => event.eventType === "StepCompleted" ||
      event.eventType === "TimerFired");
    const failed = events.some(event => event.eventType === "StepFailed");
    const progress = workflow?.nextStep ?? 0;
    const state = completed || index < progress ? "Complete"
      : failed ? "Failed"
      : index === progress && workflow?.state === "Running" ? "Running"
      : index === progress && workflow?.state === "Failed" ? "Failed"
      : "Ready";
    return {
      index, name: declared ?? last?.activityType ?? `Step ${index + 1}`,
      state, events, attempts: last?.attempts ?? 0,
      completedAt: events.find(event => event.eventType === "StepCompleted")?.occurredAt,
    };
  }), [count, history, detail?.definition, workflow?.nextStep, workflow?.state]);

  const active = steps[selectedStep] ?? null;
  const event = selectedEvent === null ? null : history[selectedEvent] ?? null;
  const age = workflow ? durationMs(workflow.createdAt, workflow.completedAt ?? new Date().toISOString()) : null;

  return <div className="space-y-6">
    <div className="flex flex-wrap items-center justify-between gap-3">
      <Link href="/workflows?hub=hub"
        className="inline-flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground">
        <ArrowLeft size={16}/> All workflow runs
      </Link>
      <Button onClick={() => setRefresh(value => value + 1)} disabled={loading}>
        <RefreshCw size={14} className={loading ? "animate-spin" : ""}/> Refresh
      </Button>
    </div>

    {error && <div role="alert" className="rounded-lg border border-rose-400/40 bg-rose-500/5 p-4 text-sm text-rose-700 dark:text-rose-300">
      {error}
    </div>}
    {loading && !detail && <div className="flex items-center gap-2 p-7 text-sm text-muted-foreground">
      <Clock3 size={16} className="animate-pulse"/> Loading durable Hub execution history…
    </div>}

    {workflow && <>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="flex flex-wrap items-center gap-2">
            <Badge tone="blue">Hub: {workflow.hubId}</Badge>
            <Badge tone="slate">Partition {workflow.partitionId}</Badge>
            <Badge tone="slate">Code-first v{workflow.version}</Badge>
          </div>
          <h1 className="mt-3 text-2xl font-bold tracking-tight">{workflow.definitionId}</h1>
          <p className="mt-1 font-mono text-xs text-muted-foreground">{workflow.id}</p>
        </div>
        <StatusBadge status={workflow.state}/>
      </div>

      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        {[
          { label: workflow.executionMode === "native" ? "Definition steps" : "Completed steps", value: workflow.executionMode === "native" ? String(workflow.stepCount) : `${workflow.nextStep} / ${workflow.stepCount}`, icon: CheckCircle2 },
          { label: "Elapsed", value: formatDuration(age), icon: Timer },
          { label: "Started", value: formatDate(workflow.createdAt), icon: Clock3 },
          { label: "Recorded events", value: String(history.length), icon: GitBranch },
        ].map(({ label, value, icon: Icon }) =>
          <Card className="p-4" key={label}>
            <div className="flex items-center gap-2 text-xs text-muted-foreground"><Icon size={14}/> {label}</div>
            <p className="mt-2 text-sm font-semibold tabular-nums">{value}</p>
          </Card>)}
      </div>

      {workflow.error && <div className="flex gap-2 rounded-lg border border-rose-300/40 bg-rose-500/5 p-4 text-sm text-rose-700">
        <AlertTriangle size={17}/>{workflow.error}
      </div>}

      <div className="flex flex-wrap gap-2 border-b border-border pb-3">
        {(["trace", "history", "state"] as const).map(name =>
          <Button key={name} variant={tab === name ? "primary" : "ghost"} size="sm" onClick={() => setTab(name)}>
            {name === "trace" ? "Execution trace" : name === "history" ? "Event history" : "Workflow state"}
          </Button>)}
      </div>

      {tab === "trace" && <>
        <Card className="overflow-hidden">
          <HubTraceWaterfall workflow={workflow} trace={trace} history={history}
            selectedStep={selectedStep} onSelectStep={setSelectedStep}/>
        </Card>

        <Card className="overflow-hidden">
          <CardHeader title={active ? `Step ${active.index + 1}: ${active.name}` : "Step details"}
            description={active ? `${active.events.length} persisted event(s)` : "No steps"}/>
          {active && <div className="space-y-3 p-5">
            <div className="flex flex-wrap gap-3">
              <StatusBadge status={active.state}/>
              <span className="text-xs text-muted-foreground">Attempt count: {active.attempts || "not yet started"}</span>
              {active.completedAt && <span className="text-xs text-muted-foreground">
                Completed {formatDate(active.completedAt)}</span>}
            </div>
            {active.events.map((entry, index) =>
              <div className="rounded-lg border border-border p-3" key={entry.taskId + entry.eventType + index}>
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <Badge tone={eventTone(entry.eventType)}>{entry.eventType}</Badge>
                  <span className="text-xs text-muted-foreground">{formatDate(entry.occurredAt)}</span>
                </div>
                {entry.payload !== null && <pre className="mt-3 overflow-x-auto rounded-lg bg-muted/50 p-3 text-xs">
                  {JSON.stringify(entry.payload, null, 2)}
                </pre>}
              </div>)}
            {active.events.length === 0 && <p className="text-sm text-muted-foreground">
              This step has not recorded a completion, retry, or failure event.
            </p>}
          </div>}
        </Card>
      </>}

      {tab === "history" && <Card className="overflow-hidden">
        <CardHeader title="Durable workflow event history"
          description="Durable Hub completion, retry and failure journal. The waterfall separately displays persisted lease attempts."/>
        <div className="divide-y divide-border">
          {history.map((entry, index) =>
            <button key={entry.taskId + entry.eventType + index}
              type="button" onClick={() => setSelectedEvent(selectedEvent === index ? null : index)}
              className="flex w-full flex-wrap items-center justify-between gap-3 px-5 py-4 text-left hover:bg-muted/50">
              <div className="flex items-center gap-3">
                <Activity size={16} className="text-muted-foreground"/>
                <span className="text-sm font-semibold">{entry.activityType}</span>
                <Badge tone={eventTone(entry.eventType)}>{entry.eventType}</Badge>
                <span className="text-xs text-muted-foreground">Step {entry.stepIndex + 1}</span>
                {entry.executionPointerId && <span className="text-xs text-muted-foreground" title={entry.executionPointerId}>
                  Pointer {shortId(entry.executionPointerId)}{entry.attempt != null ? ` · Attempt ${entry.attempt + 1}` : ""}
                </span>}
              </div>
              <span className="text-xs text-muted-foreground">{formatDate(entry.occurredAt)}</span>
            </button>)}
          {history.length === 0 && <p className="px-5 py-10 text-sm text-muted-foreground">
            No durable step events recorded yet.
          </p>}
        </div>
        {event && <div className="border-t border-border p-5">
          <div className="text-sm font-semibold">{event.eventType} · {shortId(event.taskId)}</div>
          <pre className="mt-3 overflow-x-auto rounded-lg bg-muted/60 p-3 text-xs">
            {JSON.stringify(event, null, 2)}
          </pre>
        </div>}
      </Card>}

      {tab === "state" && <Card className="overflow-hidden">
        <CardHeader title="Persisted workflow state"
          description="Durable Hub execution results; no synthetic workflow-pointer reconstruction."/>
        <pre className="overflow-x-auto p-5 text-xs">
          {JSON.stringify({
            workflowId: workflow.id, hubId: workflow.hubId,
            definitionId: workflow.definitionId, version: workflow.version,
            partitionId: workflow.partitionId, status: workflow.state,
            nextStep: workflow.nextStep, stepCount: workflow.stepCount,
            results: workflow.results, error: workflow.error,
            ...(detail?.nativeRuntime ? { nativeStatus: detail.nativeRuntime.status,
              revision: detail.nativeRuntime.revision, snapshot: detail.nativeRuntime.snapshot } : {}),
          }, null, 2)}
        </pre>
      </Card>}
    </>}
  </div>;
}

"use client";

import Link from "next/link";
import { useState } from "react";
import { AlertTriangle, Check, ChevronRight, FlaskConical, LoaderCircle, Play, RotateCcw, ShieldCheck } from "lucide-react";
import { Badge, Button, Card, CardHeader } from "./ui";

type Scenario = "completed" | "retry" | "approval";
interface DemoCreated {
  workflowId: string;
  tenantId: string;
  requiresApproval: boolean;
  simulateTransientFailure: boolean;
}
interface ScenarioOption {
  id: Scenario;
  title: string;
  description: string;
  icon: React.ReactNode;
  accent: "green" | "amber" | "blue";
}

const scenarios: ScenarioOption[] = [
  { id: "completed", title: "Happy path", description: "Six provisioning steps to completion", icon: <Check size={16} />, accent: "green" },
  { id: "retry", title: "Transient retry", description: "Workspace step throws once, then retries", icon: <RotateCcw size={16} />, accent: "amber" },
  { id: "approval", title: "Wait for approval", description: "Pause on a durable event, then resume", icon: <ShieldCheck size={16} />, accent: "blue" }
];

async function postJson(path: string, body?: object): Promise<Record<string, unknown>> {
  const response = await fetch(`/backend${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: body ? JSON.stringify(body) : undefined
  });
  const parsed: unknown = await response.json().catch(() => ({}));
  if (!response.ok) {
    const description = parsed && typeof parsed === "object" && "error" in parsed
      ? String((parsed as { error: unknown }).error)
      : `HTTP ${response.status}`;
    throw new Error(description);
  }
  return parsed as Record<string, unknown>;
}

/** Explicitly marked development-only; real workflows are still monitored read-only. */
export function DemoOnboarding({ onCreated }: { onCreated: () => void }) {
  const [busy, setBusy] = useState<Scenario | "approve" | null>(null);
  const [created, setCreated] = useState<DemoCreated | null>(null);
  const [approved, setApproved] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function start(scenario: Scenario) {
    setBusy(scenario); setError(null); setApproved(false);
    try {
      const tenantId = `demo-${scenario}-${crypto.randomUUID().slice(0, 8)}`;
      const result = await postJson("/demo/onboardings", {
        tenantId, organizationName: `Example ${scenario} organization`,
        requiresApproval: scenario === "approval",
        simulateTransientFailure: scenario === "retry"
      });
      setCreated({
        workflowId: String(result.workflowId), tenantId,
        requiresApproval: scenario === "approval",
        simulateTransientFailure: scenario === "retry"
      });
      onCreated();
    } catch (reason: unknown) {
      setError(reason instanceof Error ? reason.message : "Unable to start demo onboarding");
    } finally {
      setBusy(null);
    }
  }

  async function approve() {
    if (!created) return;
    setBusy("approve"); setError(null);
    try {
      await postJson(`/demo/onboardings/${encodeURIComponent(created.workflowId)}/approve`);
      setApproved(true);
      onCreated();
    } catch (reason: unknown) {
      setError(reason instanceof Error ? reason.message : "Unable to send approval");
    } finally {
      setBusy(null);
    }
  }

  return <Card className="overflow-hidden">
    <CardHeader title="Onboarding workflow playground"
      description="Start real sample executions to explore the waterfall, retries, and signal handling."
      action={<Badge tone="purple"><FlaskConical size={12} /> Development only</Badge>} />
    <div className="grid gap-3 p-4 sm:grid-cols-3">
      {scenarios.map(scenario => <div key={scenario.id} className="flex flex-col rounded-lg border border-border bg-muted/20 p-4">
        <div className="flex items-center gap-2 text-sm font-semibold">{scenario.icon}{scenario.title}</div>
        <p className="mt-2 min-h-10 text-xs leading-5 text-muted-foreground">{scenario.description}</p>
        <Button size="sm" variant="outline" className="mt-3 w-full" disabled={busy !== null}
          onClick={() => start(scenario.id)}>
          {busy === scenario.id ? <LoaderCircle size={13} className="animate-spin" /> : <Play size={13} />}
          Create run
        </Button>
      </div>)}
    </div>
    {created && <div className="flex flex-wrap items-center justify-between gap-3 border-t border-border bg-blue-500/5 px-5 py-4">
      <div className="min-w-0">
        <p className="text-xs font-semibold">New onboarding: {created.tenantId}</p>
        <p className="mt-1 font-mono text-[11px] text-muted-foreground">{created.workflowId}</p>
        {created.requiresApproval && <p className="mt-1 text-xs text-muted-foreground">
          {approved ? "Approval signal published. Watch the run resume." : "This run will wait for an approval event."}
        </p>}
      </div>
      <div className="flex gap-2">
        {created.requiresApproval && !approved && <Button size="sm" variant="primary" disabled={busy !== null} onClick={approve}>
          {busy === "approve" ? <LoaderCircle size={14} className="animate-spin" /> : <ShieldCheck size={14} />}
          Approve
        </Button>}
        <Link href={`/workflows/${created.workflowId}`} className="inline-flex h-8 items-center gap-2 rounded-lg border border-border bg-card px-3 text-xs font-semibold hover:bg-muted">
          View trace <ChevronRight size={13} />
        </Link>
      </div>
    </div>}
    {error && <div role="alert" className="flex items-center gap-2 border-t border-border px-5 py-3 text-xs text-rose-600">
      <AlertTriangle size={15} />{error}
    </div>}
  </Card>;
}

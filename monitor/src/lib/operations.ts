export type OperationsWindow = 60 | 360 | 1440;
export interface OperationsSnapshot {
  observedAt: string;
  windowMinutes: OperationsWindow;
  database: { connections: number; activeConnections: number };
  processMetrics: {
    available: boolean;
    scope: "global";
    values: Partial<Record<
      "sseConnections" | "hubClaimP95Seconds" | "workerExecutionP95Seconds" |
      "httpP95Seconds" | "timerLagP95Seconds" | "timerSchedulerErrors5m" |
      "workerInflight" | "taskClaimsPerSecond" | "workflowFailures5m",
      number | null
    >>;
  };
  workflows: {
    active: number; ready: number; running: number; waiting: number;
    scheduled: number; completed24h: number; failed24h: number;
    completedPerMinute: number;
  };
  tasks: {
    ready: number; leased: number; delayed: number; retryDue: number;
    dead: number; reclaimable: number; runnableWorkflows: number;
    oldestReadyAgeSeconds: number | null;
  };
  contention: { blockedReady: number };
  latency: { queueP95Seconds: number | null; leaseP95Seconds: number | null; queueSamples: number };
  partitions: Array<{
    hubId: string; partitionId: number; activeWorkflows: number; ready: number;
    leased: number; delayed: number; dead: number; oldestReadyAgeSeconds: number | null;
  }>;
  activities: Array<{
    hubId: string; activityType: string; ready: number; leased: number;
    delayed: number; dead: number; retryDue: number; oldestReadyAgeSeconds: number | null;
  }>;
  policies: Array<{
    name: string; resources: number; capacity: number; active: number;
    queued: number; saturated: number;
  }>;
  workers: Array<{
    workerId: string; leasedTasks: number; partitions: number; lastClaimAt: string | null;
  }>;
  timers: { pending: number; due: number; fired24h: number };
  trends: Array<{
    bucket: string; started: number; completed: number; failed: number;
    activityCompleted: number; activityDead: number;
    ready: number | null; leased: number | null; runnable: number | null;
  }>;
}
export function formatCount(value: number): string {
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 }).format(value);
}
export function formatAge(seconds: number | null): string {
  if (seconds === null || !Number.isFinite(seconds)) return "N/A";
  if (seconds < 60) return formatCount(seconds) + "s";
  if (seconds < 3600) return formatCount(seconds / 60) + "m";
  return formatCount(seconds / 3600) + "h";
}
export function partitionSkew(values: number[]): number | null {
  if (!values.length) return null;
  const average = values.reduce((sum, n) => sum + n, 0) / values.length;
  return average <= 0 ? null : Math.max(...values) / average;
}
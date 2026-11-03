export type WorkflowStatus = "Runnable" | "Complete" | "Suspended" | "Terminated" | string;

export interface WorkflowOverview {
  total: number;
  running: number;
  completed: number;
  suspended: number;
  terminated: number;
  withErrors: number;
  created24h: number;
  completed24h: number;
  asOf: string;
}

export interface WorkflowItem {
  id: string;
  definitionId: string;
  version: number;
  reference: string | null;
  description: string | null;
  status: WorkflowStatus;
  createdAt: string;
  completedAt: string | null;
  errorCount: number;
  source?: "legacy" | "hub";
  hubId?: string | null;
}

export interface WorkflowPage {
  items: WorkflowItem[];
  total: number;
  page: number;
  take: number;
}

export interface ExecutionPointer {
  id: string;
  stepId: number;
  stepName: string | null;
  predecessorId: string | null;
  status: string;
  active: boolean;
  retryCount: number;
  startTime: string | null;
  endTime: string | null;
  sleepUntil: string | null;
  eventName: string | null;
  eventPublished: boolean;
}

export interface WorkflowError {
  executionPointerId: string | null;
  errorTime: string;
  message: string | null;
}

export interface WorkflowWorkItem {
  id: string;
  state: "Ready" | "Leased" | "Completed" | "DeadLetter" | string;
  attempts: number;
  createdAt: string;
  dueAt: string;
  leaseUntil: string | null;
  owner: string | null;
}

export interface WorkflowDetail {
  workflow: WorkflowItem;
  pointers: ExecutionPointer[];
  errors: WorkflowError[];
  workItems: WorkflowWorkItem[];
}

export interface ExecutionHistoryEvent {
  workflowId: string;
  eventType: string;
  workItemId: string | null;
  executionPointerId: string | null;
  stepId: number | null;
  stepAttemptId: string | null;
  revision: number | null;
  details: string | null;
  occurredAt: string;
}

export interface ExecutionHistoryRecord {
  historyId: number;
  recordedAt: string;
  event: ExecutionHistoryEvent;
}

export interface HistoryPage {
  items: ExecutionHistoryRecord[];
  nextAfter: number;
}

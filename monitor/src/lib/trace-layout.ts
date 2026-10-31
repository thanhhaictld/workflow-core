/** A version-pinned static workflow definition. Execution time is never
 * inferred from this graph: only the journal supplies step attempt bars. */
export interface TopologyStep {
  id: number;
  name: string;
  kind: "parallel" | "step";
  children: number[];
  nextStepIds: number[];
}

export interface WorkflowTopology {
  definitionId: string;
  version: number;
  steps: TopologyStep[];
}

export type TraceLayoutRow =
  | { kind: "step" | "fork"; stepId: number; depth: number; label: string; groupId?: number; ancestors: number[] }
  | { kind: "branch"; branchIndex: number; groupId: number; depth: number; label: string; ancestors: number[] }
  | { kind: "join"; groupId: number; stepId: number; depth: number; label: string; ancestors: number[] };

/**
 * Turns Workflow Core's Sequence.Children branch roots and Outcomes into
 * a nested, definition-ordered trace layout.
 *
 * Parallel().Do(...).Join() does not define a distinct Join step. The
 * Sequence step executes the fork, waits for children, and continues only
 * after all branches complete. The join row is a *visual marker*, never an
 * invented journal span.
 */
export function buildTraceLayout(topology: WorkflowTopology): TraceLayoutRow[] {
  const byId = new Map(topology.steps.map(step => [step.id, step]));
  const ordered = [...topology.steps].sort((a, b) => a.id - b.id);
  const emitted = new Set<number>();
  const rows: TraceLayoutRow[] = [];

  function walk(startId: number, depth: number, excluded: ReadonlySet<number> = new Set(), ancestors: number[] = []) {
    let id: number | undefined = startId;
    while (id !== undefined && byId.has(id) && !excluded.has(id) && !emitted.has(id)) {
      const step: TopologyStep = byId.get(id)!;
      emitted.add(id);

      if (step.kind === "parallel" && step.children.length > 1) {
        rows.push({ kind: "fork", stepId: step.id, depth, label: `Parallel · ${step.children.length} branches`, groupId: step.id, ancestors });

        // A separate Do() contributes a root child. Its continuation is the
        // next outcome of the Sequence container, not an outcome of a branch.
        const roots = [...new Set(step.children)].filter(child => byId.has(child));
        const continuation = step.nextStepIds.find(target => byId.has(target) && !roots.includes(target));
        roots.forEach((root, index) => {
          const label = byId.get(root)?.name ?? `Step ${root}`;
          rows.push({
            kind: "branch", groupId: step.id, branchIndex: index,
            depth: depth + 1, label: `Branch ${index + 1} · ${label}`, ancestors: [...ancestors, step.id]
          });
          // Sibling roots cannot be traversed as part of another branch.
          const blocked = new Set<number>([...excluded, ...roots.filter(value => value !== root)]);
          if (continuation !== undefined) blocked.add(continuation);
          walk(root, depth + 2, blocked, [...ancestors, step.id]);
        });
        rows.push({ kind: "join", groupId: step.id, stepId: step.id, depth, label: "Join · all branches", ancestors });
        id = continuation;
      } else {
        rows.push({ kind: "step", stepId: step.id, depth, label: step.name, ancestors });
        // Outcome arrays can contain decisions; a linear path follows the
        // first valid target. Other branches are included below as unmatched.
        id = step.nextStepIds.find(target => byId.has(target) && !excluded.has(target) && !emitted.has(target));
      }
    }
  }

  if (byId.has(0)) walk(0, 0);
  // Include alternative paths, but never infer their execution.
  for (const step of ordered) {
    if (!emitted.has(step.id)) walk(step.id, 0);
  }
  return rows;
}

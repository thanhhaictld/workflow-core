import type { WorkflowTopology, TopologyStep } from "./trace-layout";

export type FlowNodeKind = "step" | "fork" | "join";

export interface FlowNode {
  id: string;
  stepId: number;
  label: string;
  kind: FlowNodeKind;
  x: number;
  y: number;
  width: number;
  height: number;
}

export interface FlowEdge {
  from: string;
  to: string;
}

export interface FlowDiagram {
  nodes: FlowNode[];
  edges: FlowEdge[];
  width: number;
  height: number;
  extraPaths: number;
}

interface Block {
  nodes: FlowNode[];
  edges: FlowEdge[];
  width: number;
  height: number;
  entry: string;
  exit: string;
}

const NODE_WIDTH = 214;
const NODE_HEIGHT = 70;
const BRANCH_GAP_X = 48;
const BRANCH_GAP_Y = 64;
const STEP_GAP_Y = 52;
const PADDING = 32;

function translate(block: Block, x: number, y: number): Block {
  return {
    ...block,
    nodes: block.nodes.map(node => ({ ...node, x: node.x + x, y: node.y + y }))
  };
}

function stack(blocks: Block[]): Block {
  if (blocks.length === 0) throw new Error("A diagram path must contain a step");
  const width = Math.max(...blocks.map(block => block.width));
  let height = 0;
  let previous: Block | null = null;
  const nodes: FlowNode[] = [];
  const edges: FlowEdge[] = [];

  for (const block of blocks) {
    if (previous) height += STEP_GAP_Y;
    const placed = translate(block, (width - block.width) / 2, height);
    nodes.push(...placed.nodes);
    edges.push(...placed.edges);
    if (previous) edges.push({ from: previous.exit, to: placed.entry });
    height += block.height;
    previous = placed;
  }
  return { nodes, edges, width, height, entry: blocks[0].entry, exit: previous!.exit };
}

function simpleStep(step: TopologyStep): Block {
  const id = "step-" + step.id;
  return {
    nodes: [{
      id, stepId: step.id, label: step.name || "Step " + step.id, kind: "step",
      x: 0, y: 0, width: NODE_WIDTH, height: NODE_HEIGHT
    }],
    edges: [], width: NODE_WIDTH, height: NODE_HEIGHT, entry: id, exit: id
  };
}

/**
 * Position the registered definition as a top-down flow chart.
 *
 * Workflow Core's Sequence is the real fork/waiting pointer. Join() has no
 * separate executable step, so the synthetic join node shares its stepId
 * with the fork and does not claim a duration or a journal attempt.
 */
export function buildWorkflowFlow(topology: WorkflowTopology): FlowDiagram {
  const byId = new Map(topology.steps.map(step => [step.id, step]));
  const visited = new Set<number>();

  function sequence(start: number, excluded: ReadonlySet<number> = new Set()): Block | null {
    const blocks: Block[] = [];
    let current: number | undefined = start;

    while (current !== undefined && byId.has(current) &&
      !excluded.has(current) && !visited.has(current)) {
      const step: TopologyStep = byId.get(current)!;
      visited.add(current);

      const roots = [...new Set(step.children)].filter(id => byId.has(id));
      if (step.kind === "parallel" && roots.length > 1) {
        const continuation = step.nextStepIds.find(id => byId.has(id) &&
          !roots.includes(id) && !excluded.has(id));

        const branches = roots.map(root => {
          const branchExcluded = new Set<number>([
            ...excluded, ...roots.filter(id => id !== root)
          ]);
          if (continuation !== undefined) branchExcluded.add(continuation);
          return sequence(root, branchExcluded);
        }).filter((block): block is Block => block !== null);

        if (branches.length > 0) {
          const branchWidth = branches.reduce((sum, block) => sum + block.width, 0) +
            BRANCH_GAP_X * (branches.length - 1);
          const width = Math.max(NODE_WIDTH, branchWidth);
          const childY = NODE_HEIGHT + BRANCH_GAP_Y;
          const childHeight = Math.max(...branches.map(block => block.height));
          const joinY = childY + childHeight + BRANCH_GAP_Y;
          const forkId = "step-" + step.id;
          const joinId = "join-" + step.id;
          const nodes: FlowNode[] = [
            {
              id: forkId, stepId: step.id, label: "Parallel · " + roots.length + " branches",
              kind: "fork", x: (width - NODE_WIDTH) / 2, y: 0,
              width: NODE_WIDTH, height: NODE_HEIGHT
            },
            {
              id: joinId, stepId: step.id, label: "Join · all branches",
              kind: "join", x: (width - NODE_WIDTH) / 2, y: joinY,
              width: NODE_WIDTH, height: NODE_HEIGHT
            }
          ];
          const edges: FlowEdge[] = [];
          let columnX = (width - branchWidth) / 2;
          for (const branch of branches) {
            const placed = translate(branch, columnX, childY);
            nodes.push(...placed.nodes);
            edges.push(...placed.edges);
            edges.push({ from: forkId, to: placed.entry });
            edges.push({ from: placed.exit, to: joinId });
            columnX += branch.width + BRANCH_GAP_X;
          }
          blocks.push({
            nodes, edges, width, height: joinY + NODE_HEIGHT,
            entry: forkId, exit: joinId
          });
        } else {
          blocks.push(simpleStep(step));
        }
      } else {
        blocks.push(simpleStep(step));
      }

      current = step.nextStepIds.find(id => byId.has(id) &&
        !excluded.has(id) && !visited.has(id));
    }
    return blocks.length ? stack(blocks) : null;
  }

  const paths: Block[] = [];
  if (byId.has(0)) {
    const main = sequence(0);
    if (main) paths.push(main);
  }

  // Alternative or unreachable definition steps must not disappear. Keep
  // unconnected paths visually separate rather than inventing transitions.
  let extraPaths = 0;
  for (const step of [...topology.steps].sort((a, b) => a.id - b.id)) {
    if (visited.has(step.id)) continue;
    const path = sequence(step.id);
    if (path) {
      paths.push(path);
      extraPaths++;
    }
  }

  if (paths.length === 0) {
    return { nodes: [], edges: [], width: 0, height: 0, extraPaths: 0 };
  }

  const width = Math.max(...paths.map(path => path.width)) + PADDING * 2;
  const nodes: FlowNode[] = [];
  const edges: FlowEdge[] = [];
  let y = PADDING;
  for (const path of paths) {
    const placed = translate(path, (width - path.width) / 2, y);
    nodes.push(...placed.nodes);
    edges.push(...placed.edges);
    y += path.height + BRANCH_GAP_Y * 2;
  }

  return {
    nodes, edges, width, height: y - BRANCH_GAP_Y * 2 + PADDING,
    extraPaths
  };
}

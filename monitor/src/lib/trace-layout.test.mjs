import assert from "node:assert/strict";
import test from "node:test";
import { buildTraceLayout, visibleTraceRows } from "./trace-layout.ts";

const step = (id, name, nextStepIds = []) =>
  ({ id, name, kind: "step", children: [], nextStepIds });

test("groups .Parallel().Do().Join() into three nested lanes and parent continuation", () => {
  const graph = {
    definitionId: "tenant-onboarding-parallel", version: 1,
    steps: [
      step(0, "ValidateOnboarding", [1]),
      { id: 1, name: "Sequence", kind: "parallel", children: [2, 4, 6], nextStepIds: [8] },
      step(2, "CreateResourceGroup", [3]),
      step(3, "DeployDatabase"),
      step(4, "RegisterApplication", [5]),
      step(5, "ConfigureRoles"),
      step(6, "CreateBillingAccount", [7]),
      step(7, "ConfigureQuota"),
      step(8, "ActivateTenant", [9]),
      step(9, "SendWelcomeEmail")
    ]
  };
  const rows = buildTraceLayout(graph);
  assert.deepEqual(rows.map(row => row.kind), [
    "step", "fork", "branch", "step", "step", "branch", "step", "step",
    "branch", "step", "step", "join", "step", "step"
  ]);
  assert.deepEqual(rows.filter(row => row.kind === "step").map(row => [row.stepId, row.depth]), [
    [0, 0], [2, 2], [3, 2], [4, 2], [5, 2], [6, 2], [7, 2], [8, 0], [9, 0]
  ]);
  assert.deepEqual(rows.find(row => row.kind === "join"), {
    kind: "join", groupId: 1, stepId: 1, depth: 0,
    label: "Join · all branches", ancestors: []
  });
  assert.equal(rows.filter(row => row.kind === "step" && row.stepId === 8).length, 1);
});

test("nested fork/join carries ancestors to hide collapsed descendants", () => {
  const graph = {
    definitionId: "nested", version: 1,
    steps: [
      step(0, "Start", [1]),
      { id: 1, name: "Outer", kind: "parallel", children: [2, 7], nextStepIds: [9] },
      step(2, "NestedBranch", [3]),
      { id: 3, name: "Inner", kind: "parallel", children: [4, 5], nextStepIds: [6] },
      step(4, "InnerA"), step(5, "InnerB"),
      step(6, "FinishNested"),
      step(7, "Other", [8]), step(8, "OtherFinal"),
      step(9, "Finish")
    ]
  };
  const rows = buildTraceLayout(graph);
  assert.deepEqual(rows.find(row => row.kind === "step" && row.stepId === 4)?.ancestors, [1, 3]);
  assert.deepEqual(rows.find(row => row.kind === "join" && row.groupId === 3)?.ancestors, [1]);
  assert.deepEqual(rows.find(row => row.kind === "join" && row.groupId === 1)?.ancestors, []);
  assert.deepEqual(rows.find(row => row.kind === "step" && row.stepId === 9)?.ancestors, []);
});

test("unmatched dynamic/alternative paths are retained and cycles cannot loop", () => {
  const graph = {
    definitionId: "alternatives", version: 1,
    steps: [step(0, "A", [1]), step(1, "B", [0]), step(2, "Alternative")]
  };
  const rows = buildTraceLayout(graph);
  assert.deepEqual(rows.map(row => row.stepId), [0, 1, 2]);
});

test("collapsing parallel onboarding never hides the fork toggle, join, or parent steps", () => {
  const graph = {
    definitionId: "tenant-onboarding-parallel", version: 1,
    steps: [
      step(0, "ValidateTenantStep", [1]),
      { id: 1, name: "Sequence", kind: "parallel", children: [2, 4, 6], nextStepIds: [8] },
      step(2, "CreateResourceGroup", [3]), step(3, "DeployDatabase"),
      step(4, "RegisterApplication", [5]), step(5, "ConfigureRoles"),
      step(6, "CreateBillingAccount", [7]), step(7, "ConfigureQuota"),
      step(8, "ActivateTenantStep", [9]), step(9, "SendWelcomeStep")
    ]
  };
  const rows = buildTraceLayout(graph);
  const expanded = visibleTraceRows(rows, new Set());
  const collapsed = visibleTraceRows(rows, new Set([1]));
  assert.equal(expanded.length, rows.length);
  assert.deepEqual(collapsed.map(row => row.kind), ["step", "fork", "join", "step", "step"]);
  assert.equal(collapsed.find(row => row.kind === "fork")?.stepId, 1,
    "The button for the collapsed fork must remain mounted so it can be reopened");
  assert.equal(collapsed.find(row => row.kind === "join")?.stepId, 1);
  assert.deepEqual(collapsed.filter(row => row.kind === "step").map(row => row.stepId), [0, 8, 9]);
  assert.deepEqual(visibleTraceRows(rows, new Set()), expanded,
    "Re-expanding restores the same set and order of rows");
});

test("nested fork can be expanded again after its parent is collapsed", () => {
  const graph = {
    definitionId: "nested", version: 1,
    steps: [
      step(0, "Validate", [1]),
      { id: 1, name: "Outer", kind: "parallel", children: [2, 7], nextStepIds: [9] },
      step(2, "NestedBranch", [3]),
      { id: 3, name: "Inner", kind: "parallel", children: [4, 5], nextStepIds: [6] },
      step(4, "InnerA"), step(5, "InnerB"), step(6, "FinishNested"),
      step(7, "Other", [8]), step(8, "OtherFinal"), step(9, "Finish")
    ]
  };
  const rows = buildTraceLayout(graph);
  const innerFolded = visibleTraceRows(rows, new Set([3]));
  assert.equal(innerFolded.some(row => row.kind === "fork" && row.stepId === 3), true);
  assert.equal(innerFolded.some(row => row.kind === "step" && row.stepId === 4), false);
  assert.equal(innerFolded.some(row => row.kind === "step" && row.stepId === 6), true);

  const bothFolded = visibleTraceRows(rows, new Set([1, 3]));
  assert.equal(bothFolded.some(row => row.kind === "fork" && row.stepId === 1), true);
  assert.equal(bothFolded.some(row => row.kind === "fork" && row.stepId === 3), false);
  assert.equal(bothFolded.some(row => row.kind === "join" && row.groupId === 1), true);

  // Open parent while preserving inner collapse state; the inner toggle returns.
  assert.deepEqual(visibleTraceRows(rows, new Set([3])), innerFolded);
  assert.deepEqual(visibleTraceRows(rows, new Set()), rows);
});

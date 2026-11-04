import assert from "node:assert/strict";
import test from "node:test";
import { buildWorkflowFlow } from "./flow-graph.ts";

const step = (id, name, nextStepIds = []) =>
  ({ id, name, kind: "step", children: [], nextStepIds });

test("serial workflow produces one connected path and no fabricated join", () => {
  const result = buildWorkflowFlow({
    definitionId: "serial", version: 1,
    steps: [step(0, "Validate", [1]), step(1, "Activate", [2]), step(2, "Welcome")]
  });
  assert.deepEqual(result.nodes.map(node => node.id), ["step-0", "step-1", "step-2"]);
  assert.deepEqual(result.edges, [
    { from: "step-0", to: "step-1" },
    { from: "step-1", to: "step-2" }
  ]);
  assert.equal(result.extraPaths, 0);
  assert.equal(result.nodes.every(node => node.x >= 0 && node.y >= 0), true);
});

test("parallel onboarding displays three separate lanes, real fork and logical join", () => {
  const result = buildWorkflowFlow({
    definitionId: "tenant-onboarding-parallel", version: 1,
    steps: [
      step(0, "ValidateTenantStep", [1]),
      { id: 1, name: "Sequence", kind: "parallel", children: [2, 4, 6], nextStepIds: [8] },
      step(2, "CreateResourceGroup", [3]), step(3, "DeployDatabase"),
      step(4, "RegisterApplication", [5]), step(5, "ConfigureRoles"),
      step(6, "CreateBillingAccount", [7]), step(7, "ConfigureQuota"),
      step(8, "ActivateTenantStep", [9]), step(9, "SendWelcomeStep")
    ]
  });
  const edges = new Set(result.edges.map(edge => edge.from + ">" + edge.to));
  const fork = result.nodes.find(node => node.id === "step-1");
  const join = result.nodes.find(node => node.id === "join-1");
  assert.equal(result.nodes.length, 11);
  assert.equal(result.extraPaths, 0);
  assert.equal(fork?.kind, "fork");
  assert.equal(join?.kind, "join");
  assert.equal(join?.stepId, 1, "The join is not an invented step");
  for (const root of [2, 4, 6]) assert.ok(edges.has("step-1>step-" + root));
  for (const leaf of [3, 5, 7]) assert.ok(edges.has("step-" + leaf + ">join-1"));
  assert.ok(edges.has("step-0>step-1"));
  assert.ok(edges.has("join-1>step-8"), "Post-join activation follows the join marker");
  assert.ok(edges.has("step-8>step-9"));
  assert.equal(edges.has("step-3>step-8"), false, "A single branch cannot skip synchronization");
  assert.equal(edges.has("step-5>step-7"), false, "Parallel lanes must not be connected in series");
  const roots = [2, 4, 6].map(id => result.nodes.find(node => node.id === "step-" + id));
  assert.equal(new Set(roots.map(node => node?.x)).size, 3, "Branch columns must not overlap");
  assert.equal(roots.every(node => (node?.y ?? 0) > (fork?.y ?? 0)), true);
  assert.equal((join?.y ?? 0) > Math.max(...roots.map(node => node?.y ?? 0)), true);
  assert.equal(result.nodes.every(node => node.x >= 0 && node.y >= 0), true);
});

test("nested forks each join before returning to their parent path", () => {
  const result = buildWorkflowFlow({
    definitionId: "nested", version: 1,
    steps: [
      step(0, "Start", [1]),
      { id: 1, name: "Outer", kind: "parallel", children: [2, 7], nextStepIds: [9] },
      step(2, "NestedBranch", [3]),
      { id: 3, name: "Inner", kind: "parallel", children: [4, 5], nextStepIds: [6] },
      step(4, "InnerA"), step(5, "InnerB"), step(6, "FinishNested"),
      step(7, "Other", [8]), step(8, "OtherFinal"), step(9, "Finish")
    ]
  });
  const edges = new Set(result.edges.map(edge => edge.from + ">" + edge.to));
  assert.ok(edges.has("step-4>join-3"));
  assert.ok(edges.has("step-5>join-3"));
  assert.ok(edges.has("join-3>step-6"));
  assert.ok(edges.has("step-6>join-1"));
  assert.ok(edges.has("step-8>join-1"));
  assert.ok(edges.has("join-1>step-9"));
  assert.equal(result.extraPaths, 0);
});

test("cycles stop safely and disconnected definition steps remain visible without false edges", () => {
  const result = buildWorkflowFlow({
    definitionId: "other", version: 1,
    steps: [step(0, "Start", [1]), step(1, "Again", [0]), step(2, "Alternate")]
  });
  assert.deepEqual(result.nodes.map(node => node.id), ["step-0", "step-1", "step-2"]);
  assert.equal(result.extraPaths, 1);
  assert.deepEqual(result.edges, [{ from: "step-0", to: "step-1" }]);
});

import test from "node:test";
import assert from "node:assert/strict";
import { parse } from "yaml";
import { definitionToYaml, definitionDiff } from "./definition-yaml.ts";

const snapshot = {
  schemaFormat: 1, id: "true: # <script>", version: 1, description: "2026-01-01\nこんにちは & * !tag",
  dataType: null, defaultErrorBehavior: "Retry", defaultRetryInterval: null,
  steps: [{ id: 0, name: "01", externalId: null, bodyType: "A:B", kind: "parallel", children: [3, 2],
    outcomes: [{ targetStepId: 2, label: "false", outcomeType: "# label" }],
    inputs: [{ index: 0, kind: "expression", targetMember: "x.y", sourceMember: null }], outputs: [],
    errorBehavior: null, retryInterval: "00:00:01", compensationStepId: 3, hasCancelCondition: true, proceedOnCancel: false }]
};

test("YAML is deterministic, preserves all snapshot values and does not mutate", () => {
  const before = structuredClone(snapshot);
  const yaml = definitionToYaml(snapshot);
  assert.deepEqual(parse(yaml), snapshot);
  assert.deepEqual(snapshot, before);
  assert.equal(definitionToYaml(Object.fromEntries(Object.entries(snapshot).reverse())), yaml);
  assert.ok(yaml.endsWith("\n"));
  assert.ok(!yaml.includes("\r"));
});

test("line diff preserves insertions, reversals, version changes and context", () => {
  const base = definitionToYaml(snapshot);
  const target = definitionToYaml({ ...snapshot, version: 2, steps: [...snapshot.steps, { ...snapshot.steps[0], id: 1, name: "ConfigureAccessStep", children: [], outcomes: [] }] });
  const forward = definitionDiff(base, target);
  assert.ok(forward.some(row => row.right?.kind === "added" && row.right.text.includes("ConfigureAccessStep")));
  assert.ok(forward.some(row => row.left?.kind === "removed" && row.left.text.includes("version: 1")));
  assert.ok(forward.some(row => row.left?.kind === "same"));
  assert.ok(definitionDiff(target, base).some(row => row.left?.kind === "removed" && row.left.text.includes("ConfigureAccessStep")));
  assert.ok(definitionDiff(base, base).every(row => row.left?.kind === "same"));
  assert.ok(definitionDiff(base, definitionToYaml({ ...snapshot, version: 2 })).some(row => row.right?.text.includes("version: 2")));
  assert.deepEqual(definitionDiff("", ""), []);
});

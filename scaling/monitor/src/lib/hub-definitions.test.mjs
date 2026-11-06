import assert from "node:assert/strict";
import test from "node:test";
import { groupHubDefinitions, hubDefinitionMetadata } from "./hub-definitions.ts";

const definition = (hub, name, version) => ({
  hubId: hub, definitionId: name, version, partitions: 64, stepCount: 3,
  executionMode: "native", metadata: { implementation: "abc" },
  steps: [{ stepId: 0, name: "Validate", kind: "Native" }],
});

test("Hub definition grouping keeps Hub namespaces separate and latest version first", () => {
  const groups = groupHubDefinitions([
    definition("offboarding", "tenant-offboarding-fast", 1),
    definition("onboarding", "tenant-offboarding-fast", 1),
    definition("offboarding", "tenant-offboarding-fast", 3),
    definition("offboarding", "tenant-offboarding-fast", 2),
  ]);
  assert.equal(groups.length, 2);
  assert.equal(groups[0].hubId, "offboarding");
  assert.deepEqual(groups[0].versions.map(v => v.version), [3, 2, 1]);
  assert.equal(groups[1].hubId, "onboarding");
});

test("Hub workflow metadata preserves native step topology and identity", () => {
  const yamlData = hubDefinitionMetadata(definition("offboarding", "tenant-offboarding-fast", 1));
  assert.equal(yamlData.hubId, "offboarding");
  assert.equal(yamlData.stepCount, 3);
  assert.deepEqual(yamlData.steps, [{ stepId: 0, name: "Validate", kind: "Native" }]);
});

test("Invalid or incomplete registrations do not enter the catalog", () => {
  const groups = groupHubDefinitions([
    null, {}, { hubId: "offboarding", definitionId: "invalid", version: 0 },
    definition("offboarding", "tenant-offboarding-fast", 1),
  ]);
  assert.equal(groups.length, 1);
});

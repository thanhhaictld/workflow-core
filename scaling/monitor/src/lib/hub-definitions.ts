/** Persisted, code-first registrations published by remote Hub processors. */
export interface HubDefinitionStep {
  stepId?: number;
  name?: string | null;
  bodyType?: string | null;
  activityType?: string | null;
  kind?: "Activity" | "Native" | string;
  children?: number[];
  compensationStepId?: number | null;
}

export interface HubPublishedDefinition {
  hubId: string;
  definitionId: string;
  version: number;
  partitions: number;
  stepCount: number;
  executionMode: string | null;
  metadata: Record<string, unknown>;
  steps: HubDefinitionStep[];
}

export interface HubDefinitionGroup {
  hubId: string;
  definitionId: string;
  versions: HubPublishedDefinition[];
}

/** Never group only by definitionId: business Hubs can reuse the same name. */
export function groupHubDefinitions(definitions: HubPublishedDefinition[]): HubDefinitionGroup[] {
  const groups = new Map<string, HubDefinitionGroup>();
  for (const definition of definitions) {
    if (!definition || typeof definition.hubId !== "string" ||
        typeof definition.definitionId !== "string" ||
        !Number.isInteger(definition.version) || definition.version < 1)
      continue;
    const key = JSON.stringify([definition.hubId, definition.definitionId]);
    const group = groups.get(key) ?? {
      hubId: definition.hubId,
      definitionId: definition.definitionId,
      versions: [],
    };
    group.versions.push(definition);
    groups.set(key, group);
  }
  return [...groups.values()].map(group => ({
    ...group,
    versions: group.versions.sort((a, b) => b.version - a.version),
  })).sort((a, b) =>
    a.hubId.localeCompare(b.hubId) || a.definitionId.localeCompare(b.definitionId));
}

export function hubDefinitionMetadata(definition: HubPublishedDefinition): Record<string, unknown> {
  return {
    hubId: definition.hubId,
    definitionId: definition.definitionId,
    version: definition.version,
    executionMode: definition.executionMode ?? "remote",
    partitions: definition.partitions,
    stepCount: definition.stepCount ?? definition.steps?.length ?? 0,
    ...definition.metadata,
    steps: definition.steps ?? [],
  };
}

export interface DefinitionBinding {
  index: number;
  kind: string;
  targetMember: string | null;
  sourceMember: string | null;
}
export interface DefinitionOutcome {
  targetStepId: number;
  label: string | null;
  outcomeType: string;
}
export interface DefinitionStep {
  id: number;
  name: string;
  externalId: string | null;
  bodyType: string;
  kind: "parallel" | "step";
  children: number[];
  outcomes: DefinitionOutcome[];
  inputs: DefinitionBinding[];
  outputs: DefinitionBinding[];
  errorBehavior: string | null;
  retryInterval: string | null;
  compensationStepId: number | null;
  hasCancelCondition: boolean;
  proceedOnCancel: boolean;
}
export interface DefinitionSchema {
  schemaFormat: number;
  id: string;
  version: number;
  description: string | null;
  dataType: string | null;
  defaultErrorBehavior: string;
  defaultRetryInterval: string | null;
  steps: DefinitionStep[];
}
export type DefinitionLifecycle = "Published" | "Deprecated" | "Disabled";
export interface CatalogVersion {
  definitionId: string;
  version: number;
  lifecycle: DefinitionLifecycle;
  isDefault: boolean;
  schemaSha256: string;
  registeredAt: string;
  policyUpdatedAt: string;
  schema: DefinitionSchema;
}
export interface CatalogIndex {
  writesEnabled: boolean;
  versions: CatalogVersion[];
}
export interface DefinitionChange {
  change: "Added" | "Removed" | "Modified";
  path: string;
  before: string | null;
  after: string | null;
}
export interface DefinitionComparison {
  definitionId: string;
  fromVersion: number;
  toVersion: number;
  fromSha256: string;
  toSha256: string;
  changes: DefinitionChange[];
}

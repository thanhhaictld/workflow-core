-- Explicit scheduling policy is independent of Hub and partition placement.
-- Legacy rows retain prior hub/activity capacity semantics until migrated.
ALTER TABLE wfc_scale.hub_activity_registration
  ADD COLUMN IF NOT EXISTS execution_policy text NOT NULL DEFAULT 'Legacy';
ALTER TABLE wfc_scale.hub_activity_registration
  ADD CONSTRAINT ck_hub_activity_execution_policy
  CHECK (execution_policy IN ('Legacy', 'Unordered', 'PartitionSerial',
    'KeyedExclusive', 'KeyedFifo', 'KeyedBounded'));
-- No partition lock table is required: scoped permits use hub_resource.
-- Existing task permits remain durable until their leases complete.

-- Forward-only revision for operator-controlled activity capacity updates.
ALTER TABLE wfc_scale.hub_activity_registration
  ADD COLUMN IF NOT EXISTS revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0);

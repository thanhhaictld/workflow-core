-- The table lock orders the one-time boundary against concurrent workflow starts.
-- Run only after old Hub writers have been quiesced; old binaries do not enforce it.
BEGIN;
LOCK TABLE wfc_scale.hub_workflow IN ACCESS EXCLUSIVE MODE;
ALTER TABLE wfc_scale.hub_workflow
  ADD COLUMN IF NOT EXISTS business_policy_compatibility boolean NOT NULL DEFAULT false;
CREATE TABLE IF NOT EXISTS wfc_scale.hub_rollout_boundary (
  name text PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
WITH boundary AS (
  INSERT INTO wfc_scale.hub_rollout_boundary(name)
  VALUES ('business-policy-retirement') ON CONFLICT DO NOTHING RETURNING name
)
UPDATE wfc_scale.hub_workflow SET business_policy_compatibility=true
WHERE EXISTS (SELECT 1 FROM boundary);
COMMIT;

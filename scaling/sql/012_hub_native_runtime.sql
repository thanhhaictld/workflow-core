-- Additive native executor mode. Existing inline/remote rows are unchanged.
ALTER TABLE wfc_scale.hub_workflow
  ADD COLUMN IF NOT EXISTS native_snapshot jsonb,
  ADD COLUMN IF NOT EXISTS native_status text NOT NULL DEFAULT 'Runnable';
ALTER TABLE wfc_scale.hub_activity_registration
  ADD COLUMN IF NOT EXISTS contract jsonb;
ALTER TABLE wfc_scale.hub_timer ADD COLUMN IF NOT EXISTS execution_pointer_id text;
ALTER TABLE wfc_scale.hub_task
  ADD COLUMN IF NOT EXISTS execution_pointer_id text,
  ADD COLUMN IF NOT EXISTS native_step_id integer,
  ADD COLUMN IF NOT EXISTS dispatch_key text;
CREATE UNIQUE INDEX IF NOT EXISTS ux_hub_native_work
  ON wfc_scale.hub_task(workflow_id)
  WHERE task_type='__native_workflow__' AND state IN (0,1);
CREATE UNIQUE INDEX IF NOT EXISTS ux_hub_native_dispatch
  ON wfc_scale.hub_task(workflow_id,dispatch_key) WHERE dispatch_key IS NOT NULL;
CREATE TABLE IF NOT EXISTS wfc_scale.hub_native_inbox (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  workflow_id uuid NOT NULL REFERENCES wfc_scale.hub_workflow(id),
  pointer_id text NOT NULL,
  kind text NOT NULL,
  source_key text NOT NULL,
  payload jsonb NOT NULL,
  consumed boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE(workflow_id,pointer_id,kind,source_key)
);
CREATE INDEX IF NOT EXISTS ix_hub_native_inbox_pending
  ON wfc_scale.hub_native_inbox(workflow_id,id) WHERE NOT consumed;
CREATE TABLE IF NOT EXISTS wfc_scale.hub_native_subscription (
  workflow_id uuid NOT NULL REFERENCES wfc_scale.hub_workflow(id),
  pointer_id text NOT NULL,
  event_name text NOT NULL,
  event_key text NOT NULL,
  as_of timestamptz NOT NULL,
  PRIMARY KEY(workflow_id,pointer_id)
);
CREATE INDEX IF NOT EXISTS ix_hub_native_subscription_event
  ON wfc_scale.hub_native_subscription(event_name,event_key);
CREATE TABLE IF NOT EXISTS wfc_scale.hub_native_event (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  hub_id text NOT NULL,
  event_name text NOT NULL,
  event_key text NOT NULL,
  data jsonb NOT NULL,
  effective_at timestamptz NOT NULL,
  request_key text NOT NULL,
  UNIQUE(hub_id,request_key)
);
CREATE INDEX IF NOT EXISTS ix_hub_native_event_lookup
  ON wfc_scale.hub_native_event(hub_id,event_name,event_key,effective_at,id);
CREATE TABLE IF NOT EXISTS wfc_scale.hub_native_status_request (
  workflow_id uuid NOT NULL REFERENCES wfc_scale.hub_workflow(id),
  request_key text NOT NULL,
  status text NOT NULL,
  PRIMARY KEY(workflow_id,request_key)
);
ALTER TABLE wfc_scale.hub_workflow_event
  ADD COLUMN IF NOT EXISTS execution_pointer_id text,
  ADD COLUMN IF NOT EXISTS parent_pointer_id text,
  ADD COLUMN IF NOT EXISTS scope jsonb,
  ADD COLUMN IF NOT EXISTS attempt integer;
ALTER TABLE wfc_scale.hub_task_attempt DROP CONSTRAINT IF EXISTS hub_task_attempt_outcome_check;
ALTER TABLE wfc_scale.hub_task_attempt ADD CONSTRAINT hub_task_attempt_outcome_check
  CHECK(outcome IN ('Leased','Completed','RetryScheduled','Failed','LeaseExpired','Cancelled'));

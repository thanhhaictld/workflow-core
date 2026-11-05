-- Forward-only migration: remote workflow processor and durable timer scheduling.
-- Coexists with the pre-existing Hub-owned sequential activity-chain mode.
ALTER TABLE wfc_scale.hub_workflow
  ADD COLUMN IF NOT EXISTS execution_mode text NOT NULL DEFAULT 'inline',
  ADD COLUMN IF NOT EXISTS revision bigint NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS waiting_for text,
  ADD COLUMN IF NOT EXISTS processor_state jsonb NOT NULL DEFAULT '{}'::jsonb;
CREATE TABLE IF NOT EXISTS wfc_scale.hub_definition (
  hub_id text NOT NULL,
  definition_id text NOT NULL,
  version integer NOT NULL CHECK(version>0),
  partitions integer NOT NULL CHECK(partitions BETWEEN 1 AND 4096),
  step_count integer NOT NULL CHECK(step_count BETWEEN 1 AND 128),
  metadata jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(hub_id,definition_id,version)
);
CREATE TABLE IF NOT EXISTS wfc_scale.hub_activity_registration (
  hub_id text NOT NULL,
  activity_type text NOT NULL,
  version integer NOT NULL CHECK(version>0),
  partitions integer NOT NULL CHECK(partitions BETWEEN 1 AND 4096),
  hub_concurrency integer NOT NULL CHECK(hub_concurrency>0),
  activity_concurrency integer NOT NULL CHECK(activity_concurrency>0),
  resource_name text,
  resource_field text,
  resource_capacity integer NOT NULL DEFAULT 1 CHECK(resource_capacity>0),
  resource_fifo boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(hub_id,activity_type,version),
  CONSTRAINT ck_hub_resource_pair CHECK ((resource_name IS NULL)=(resource_field IS NULL))
);
CREATE TABLE IF NOT EXISTS wfc_scale.hub_timer (
  id uuid PRIMARY KEY,
  workflow_id uuid NOT NULL REFERENCES wfc_scale.hub_workflow(id),
  hub_id text NOT NULL,
  partition_id integer NOT NULL,
  step_index integer NOT NULL CHECK(step_index>=0),
  timer_key text NOT NULL,
  generation bigint NOT NULL CHECK(generation>0),
  due_at timestamptz NOT NULL,
  state smallint NOT NULL DEFAULT 0 CHECK(state BETWEEN 0 AND 2),
  scheduled_revision bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  fired_at timestamptz,
  cancelled_at timestamptz,
  UNIQUE(workflow_id,timer_key,generation)
);
CREATE INDEX IF NOT EXISTS ix_hub_timer_due
  ON wfc_scale.hub_timer(due_at, partition_id, id)
  WHERE state=0;
CREATE INDEX IF NOT EXISTS ix_hub_timer_workflow
  ON wfc_scale.hub_timer(workflow_id, state);
CREATE TABLE IF NOT EXISTS wfc_scale.hub_workflow_event (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  workflow_id uuid NOT NULL REFERENCES wfc_scale.hub_workflow(id),
  step_index integer NOT NULL,
  task_id uuid,
  activity_type text NOT NULL,
  event_type text NOT NULL,
  occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  payload jsonb,
  correlation_id text
);
CREATE INDEX IF NOT EXISTS ix_hub_workflow_event_trace
  ON wfc_scale.hub_workflow_event(workflow_id, occurred_at, id);

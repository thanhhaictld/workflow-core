-- Durable FIFO frontier for matched claims. All changes are transactional.
-- Backfill may take time on production tables; schedule during low traffic.
ALTER TABLE wfc_scale.hub_task_constraint
    ADD COLUMN IF NOT EXISTS queue_sequence bigint;
ALTER TABLE wfc_scale.hub_task_constraint
    ADD COLUMN IF NOT EXISTS fifo_pending boolean;

UPDATE wfc_scale.hub_task_constraint c
SET queue_sequence=t.sequence, fifo_pending=t.state IN (0,1)
FROM wfc_scale.hub_task t
WHERE t.id=c.task_id
  AND (c.queue_sequence IS DISTINCT FROM t.sequence
       OR c.fifo_pending IS DISTINCT FROM (t.state IN (0,1)));

ALTER TABLE wfc_scale.hub_task_constraint
    ALTER COLUMN queue_sequence SET NOT NULL,
    ALTER COLUMN fifo_pending SET NOT NULL;

-- Capture the authoritative task sequence for every new constraint, no
-- matter which gateway/runtime inserts it (native, managed or standalone).
CREATE OR REPLACE FUNCTION wfc_scale.set_hub_constraint_frontier()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    SELECT t.sequence, (t.state IN (0,1))
    INTO NEW.queue_sequence, NEW.fifo_pending
    FROM wfc_scale.hub_task t WHERE t.id=NEW.task_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Missing task % for FIFO constraint', NEW.task_id;
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS trg_hub_constraint_frontier ON wfc_scale.hub_task_constraint;
CREATE TRIGGER trg_hub_constraint_frontier
BEFORE INSERT OR UPDATE OF task_id ON wfc_scale.hub_task_constraint
FOR EACH ROW EXECUTE FUNCTION wfc_scale.set_hub_constraint_frontier();

-- A terminal task leaves the pending frontier in the same transaction as
-- its state update. Expiry/retry (states 0 or 1) remains pending and preserves
-- strict FIFO order across worker crashes and lease takeovers.
CREATE OR REPLACE FUNCTION wfc_scale.sync_hub_constraint_frontier()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE wfc_scale.hub_task_constraint
    SET fifo_pending=(NEW.state IN (0,1))
    WHERE task_id=NEW.id;
    RETURN NULL;
END $$;
DROP TRIGGER IF EXISTS trg_hub_task_frontier_state ON wfc_scale.hub_task;
CREATE TRIGGER trg_hub_task_frontier_state
AFTER UPDATE OF state ON wfc_scale.hub_task
FOR EACH ROW
WHEN ((OLD.state IN (0,1)) IS DISTINCT FROM (NEW.state IN (0,1)))
EXECUTE FUNCTION wfc_scale.sync_hub_constraint_frontier();

-- Only pending constraints participate in earlier-task checks. Completed
-- predecessors are absent without repeatedly joining/scanning their tasks.
CREATE INDEX IF NOT EXISTS ix_hub_fifo_pending_head
ON wfc_scale.hub_task_constraint(resource_key, queue_sequence)
WHERE fifo_pending;

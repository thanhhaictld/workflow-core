-- Durable W3C scheduling context plus per-lease trace lookup.
-- Idempotent so existing installations can upgrade without rewriting history.
ALTER TABLE wfc_scale.hub_task
    ADD COLUMN IF NOT EXISTS traceparent varchar(55),
    ADD COLUMN IF NOT EXISTS tracestate text;
ALTER TABLE wfc_scale.hub_task_attempt
    ADD COLUMN IF NOT EXISTS trace_id varchar(32),
    ADD COLUMN IF NOT EXISTS span_id varchar(16);
COMMENT ON COLUMN wfc_scale.hub_task.traceparent IS
    'W3C context of the scheduler; used as parent for short executions or link for long waits/retries.';
COMMENT ON COLUMN wfc_scale.hub_task_attempt.trace_id IS
    'OTel trace containing the fenced attempt completion; nullable if no trace was recorded.';

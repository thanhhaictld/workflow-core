-- Durable deduplicated per-subscription delivery, for partially completed event fanout.
CREATE TABLE IF NOT EXISTS wfc_scale.signal_delivery (
    event_id uuid NOT NULL REFERENCES wfc_scale.signal_event(id) ON DELETE CASCADE,
    subscription_id uuid NOT NULL,
    delivered_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(event_id,subscription_id)
);

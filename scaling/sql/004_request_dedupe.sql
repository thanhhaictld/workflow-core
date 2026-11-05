-- Exactly-once creation of command intent (NOT exactly-once external step effects).
CREATE TABLE IF NOT EXISTS wfc_scale.request_dedupe (
    kind text NOT NULL,
    request_key text NOT NULL,
    resource_id uuid NOT NULL,
    payload_hash bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (kind, request_key),
    CONSTRAINT ck_request_kind CHECK (kind IN ('workflow','event'))
);

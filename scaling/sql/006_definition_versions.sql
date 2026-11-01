-- Descriptive, immutable schema snapshots derived from executable C# definitions.
-- No workflow starts, version selection, or lifecycle policy may depend on
-- this table. It exists ONLY for audit/review and schema comparisons.
CREATE TABLE IF NOT EXISTS wfc_scale.definition_version (
    definition_id text NOT NULL,
    version integer NOT NULL CHECK (version > 0),
    schema_format integer NOT NULL DEFAULT 1,
    schema_json jsonb NOT NULL,
    schema_sha256 text NOT NULL CHECK (length(schema_sha256) = 64),
    registered_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(definition_id, version)
);
-- Existing test databases that ran the former management-only migration can
-- retain its unused policy columns; the projection never reads or writes them.
-- This feature never alters executable Workflow Core registry definitions.

-- Versioned, immutable workflow-definition schema snapshots.
-- Registry reconciliation may insert new (id, version) pairs, but may never
-- replace existing metadata at the same version. Mutable release policy is
-- stored separately from the frozen schema.
CREATE TABLE IF NOT EXISTS wfc_scale.definition_version (
    definition_id text NOT NULL,
    version integer NOT NULL CHECK (version > 0),
    schema_format integer NOT NULL DEFAULT 1,
    schema_json jsonb NOT NULL,
    schema_sha256 text NOT NULL CHECK (length(schema_sha256) = 64),
    lifecycle text NOT NULL DEFAULT 'Published'
        CHECK (lifecycle IN ('Published', 'Deprecated', 'Disabled')),
    is_default boolean NOT NULL DEFAULT false,
    registered_at timestamptz NOT NULL DEFAULT now(),
    policy_updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(definition_id, version),
    CHECK (NOT is_default OR lifecycle = 'Published')
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_definition_version_default
    ON wfc_scale.definition_version(definition_id) WHERE is_default;
CREATE INDEX IF NOT EXISTS ix_definition_version_lifecycle
    ON wfc_scale.definition_version(lifecycle, definition_id);
-- SQL permissions: definition schema is insert-only via the application.
-- Production deployments must separate read-only viewer and management roles.

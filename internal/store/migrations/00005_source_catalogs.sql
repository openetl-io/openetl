-- +goose Up
-- Catalogs that runners discovered, one row per distinct catalog of a source.
CREATE TABLE source_catalogs (
    org_id         uuid NOT NULL,
    source_id      uuid NOT NULL,
    -- SHA-256 of the streams (openetl.api.v1.Catalog, deterministic).
    digest         text NOT NULL,
    -- The source version last discovered with it.
    source_version text NOT NULL,
    -- openetl.api.v1.Catalog as protojson, streams only.
    catalog        jsonb NOT NULL,
    -- When it was last discovered.
    discover_time  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, source_id, digest),
    FOREIGN KEY (org_id, source_id) REFERENCES sources (org_id, id) ON DELETE CASCADE
);

-- +goose Down
DROP TABLE source_catalogs;

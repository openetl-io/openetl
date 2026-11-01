-- +goose Up
-- Immutable: every config a source or destination has had. A pipeline
-- version names the ones it runs.
CREATE TABLE source_versions (
    org_id           uuid NOT NULL,
    source_id        uuid NOT NULL,
    version          text NOT NULL,
    spec             jsonb NOT NULL,
    connector_digest text NOT NULL,
    secrets          text[] NOT NULL,
    -- Who applied it, and from where: such as cli:shop.yaml#1.
    provenance       text NOT NULL,
    create_time      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, source_id, version),
    FOREIGN KEY (org_id, source_id) REFERENCES sources (org_id, id) ON DELETE CASCADE
);

CREATE TABLE destination_versions (
    org_id           uuid NOT NULL,
    destination_id   uuid NOT NULL,
    version          text NOT NULL,
    spec             jsonb NOT NULL,
    connector_digest text NOT NULL,
    secrets          text[] NOT NULL,
    provenance       text NOT NULL,
    create_time      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, destination_id, version),
    FOREIGN KEY (org_id, destination_id) REFERENCES destinations (org_id, id) ON DELETE CASCADE
);

INSERT INTO source_versions (org_id, source_id, version, spec, connector_digest, secrets, provenance, create_time)
SELECT org_id, id, version, spec, connector_digest, secrets, '', update_time FROM sources;

INSERT INTO destination_versions (org_id, destination_id, version, spec, connector_digest, secrets, provenance, create_time)
SELECT org_id, id, version, spec, connector_digest, secrets, '', update_time FROM destinations;

ALTER TABLE pipeline_versions
    ADD COLUMN source_version text,
    ADD COLUMN destination_version text,
    -- The bundle of each dbt project the post-sync steps name, by name.
    ADD COLUMN dbt jsonb NOT NULL DEFAULT '{}',
    ADD COLUMN provenance text NOT NULL DEFAULT '';

-- Versions from before this pin what their syncs ran: the current configs
-- and bundles.
UPDATE pipeline_versions v SET source_version = s.version
FROM sources s WHERE s.org_id = v.org_id AND s.id = v.source_id;

UPDATE pipeline_versions v SET destination_version = d.version
FROM destinations d WHERE d.org_id = v.org_id AND d.id = v.destination_id;

UPDATE pipeline_versions v SET dbt = coalesce((
    SELECT jsonb_object_agg(d.name, d.digest)
    FROM jsonb_array_elements(coalesce(v.spec -> 'postSync', '[]')) step
    JOIN pipelines p ON p.org_id = v.org_id AND p.id = v.pipeline_id
    JOIN dbt_projects d ON d.org_id = v.org_id AND d.workspace_id = p.workspace_id AND d.name = step -> 'dbt' ->> 'project'
), '{}');

-- Their sources or destinations are gone, so they couldn't run.
DELETE FROM pipeline_versions WHERE source_version IS NULL OR destination_version IS NULL;

ALTER TABLE pipeline_versions
    ALTER COLUMN source_version SET NOT NULL,
    ALTER COLUMN destination_version SET NOT NULL,
    ALTER COLUMN dbt DROP DEFAULT,
    ALTER COLUMN provenance DROP DEFAULT;

-- +goose Down
ALTER TABLE pipeline_versions
    DROP COLUMN provenance,
    DROP COLUMN dbt,
    DROP COLUMN destination_version,
    DROP COLUMN source_version;
DROP TABLE destination_versions;
DROP TABLE source_versions;

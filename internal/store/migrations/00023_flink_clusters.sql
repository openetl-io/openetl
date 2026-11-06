-- +goose Up
-- A Flink cluster is where a continuous pipeline's Flink job runs. Pipeline
-- versions pin a version of its settings, as they pin a source's.
CREATE TABLE flink_clusters (
    org_id       uuid NOT NULL,
    workspace_id uuid NOT NULL,
    id           uuid NOT NULL,
    name         text NOT NULL,
    spec         jsonb NOT NULL,
    version      text NOT NULL,
    secrets      text[] NOT NULL,
    revision     bigint NOT NULL,
    create_time  timestamptz NOT NULL DEFAULT now(),
    update_time  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, id),
    UNIQUE (org_id, workspace_id, name),
    FOREIGN KEY (org_id, workspace_id) REFERENCES workspaces (org_id, id)
);

-- Immutable: every setting a Flink cluster has had.
CREATE TABLE flink_cluster_versions (
    org_id      uuid NOT NULL,
    cluster_id  uuid NOT NULL,
    version     text NOT NULL,
    spec        jsonb NOT NULL,
    secrets     text[] NOT NULL,
    provenance  text NOT NULL,
    create_time timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, cluster_id, version),
    FOREIGN KEY (org_id, cluster_id) REFERENCES flink_clusters (org_id, id) ON DELETE CASCADE
);

ALTER TABLE pipeline_versions
    -- The version of the Flink cluster the transform names, by name.
    ADD COLUMN flink_clusters jsonb NOT NULL DEFAULT '{}';

-- +goose Down
ALTER TABLE pipeline_versions DROP COLUMN flink_clusters;
DROP TABLE flink_cluster_versions;
DROP TABLE flink_clusters;

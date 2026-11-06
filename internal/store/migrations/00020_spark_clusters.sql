-- +goose Up
-- A Spark cluster is where a pipeline's Spark applications run. Pipeline
-- versions pin a version of its settings, as they pin a source's.
CREATE TABLE spark_clusters (
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

-- Immutable: every setting a Spark cluster has had.
CREATE TABLE spark_cluster_versions (
    org_id      uuid NOT NULL,
    cluster_id  uuid NOT NULL,
    version     text NOT NULL,
    spec        jsonb NOT NULL,
    secrets     text[] NOT NULL,
    provenance  text NOT NULL,
    create_time timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, cluster_id, version),
    FOREIGN KEY (org_id, cluster_id) REFERENCES spark_clusters (org_id, id) ON DELETE CASCADE
);

ALTER TABLE pipeline_versions
    -- The version of each Spark cluster the post-sync steps name, by name.
    ADD COLUMN spark_clusters jsonb NOT NULL DEFAULT '{}';

-- +goose Down
ALTER TABLE pipeline_versions DROP COLUMN spark_clusters;
DROP TABLE spark_cluster_versions;
DROP TABLE spark_clusters;

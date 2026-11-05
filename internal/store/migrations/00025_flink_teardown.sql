-- +goose Up
-- A teardown of a deleted pipeline's Flink jobs names the pipeline and the
-- version of the Flink cluster its last version pinned, since the
-- pipeline's own rows go with it.
ALTER TABLE connector_ops
    ADD COLUMN pipeline_id uuid,
    ADD COLUMN flink_cluster_id uuid,
    ADD COLUMN flink_version text,
    ADD FOREIGN KEY (org_id, flink_cluster_id, flink_version)
        REFERENCES flink_cluster_versions (org_id, cluster_id, version) ON DELETE CASCADE;

-- +goose Down
ALTER TABLE connector_ops DROP COLUMN flink_version, DROP COLUMN flink_cluster_id, DROP COLUMN pipeline_id;

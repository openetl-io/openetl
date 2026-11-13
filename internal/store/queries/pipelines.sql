-- name: GetPipeline :one
SELECT * FROM pipelines WHERE org_id = @org_id AND workspace_id = @workspace_id AND name = @name;

-- name: GetPipelineByID :one
SELECT * FROM pipelines WHERE org_id = @org_id AND id = @id;

-- name: ListPipelines :many
SELECT * FROM pipelines
WHERE org_id = @org_id AND workspace_id = @workspace_id AND name > @after
ORDER BY name LIMIT @max_rows;

-- name: InsertPipeline :exec
INSERT INTO pipelines (org_id, workspace_id, id, name, source_id, destination_id, data_plane_id, version, revision)
VALUES (@org_id, @workspace_id, @id, @name, @source_id, @destination_id, @data_plane_id, @version, 1);

-- name: UpdatePipeline :exec
UPDATE pipelines
SET source_id = @source_id, destination_id = @destination_id, data_plane_id = @data_plane_id,
    version = @version, revision = revision + 1, update_time = now()
WHERE org_id = @org_id AND id = @id;

-- name: DeletePipeline :execrows
DELETE FROM pipelines WHERE org_id = @org_id AND id = @id;

-- name: InsertPipelineVersion :exec
INSERT INTO pipeline_versions (org_id, pipeline_id, version, spec, source_id, destination_id, data_plane_id,
    source_version, destination_version, dbt, spark_clusters, flink_clusters, provenance, secrets, key_groups)
VALUES (@org_id, @pipeline_id, @version, @spec, @source_id, @destination_id, @data_plane_id,
    @source_version, @destination_version, @dbt, @spark_clusters, @flink_clusters, @provenance, @secrets,
    (SELECT key_groups FROM pipelines WHERE org_id = @org_id AND id = @pipeline_id))
ON CONFLICT (org_id, pipeline_id, version) DO NOTHING;

-- name: SetPipelineVersionSQLPlan :exec
UPDATE pipeline_versions SET sql_plan = @sql_plan
WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND version = @version;

-- name: GetPipelineVersion :one
SELECT * FROM pipeline_versions WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND version = @version;

-- name: GetSyncSpec :one
SELECT v.version, v.data_plane_id, v.spec AS pipeline_spec, v.key_groups, v.sql_plan, v.dbt, v.spark_clusters, v.flink_clusters, v.source_version, v.destination_version,
    sv.spec AS source_spec, dv.spec AS destination_spec,
    sv.connector_digest AS source_connector, dv.connector_digest AS destination_connector,
    sv.secrets AS source_secrets, dv.secrets AS destination_secrets, v.secrets AS access_secrets,
    p.workspace_id, p.name AS pipeline, w.name AS workspace, s.name AS source, d.name AS destination
FROM pipeline_versions v
JOIN pipelines p ON p.org_id = v.org_id AND p.id = v.pipeline_id
JOIN workspaces w ON w.org_id = p.org_id AND w.id = p.workspace_id
JOIN sources s ON s.org_id = v.org_id AND s.id = v.source_id
JOIN destinations d ON d.org_id = v.org_id AND d.id = v.destination_id
JOIN source_versions sv ON sv.org_id = v.org_id AND sv.source_id = v.source_id AND sv.version = v.source_version
JOIN destination_versions dv ON dv.org_id = v.org_id AND dv.destination_id = v.destination_id AND dv.version = v.destination_version
WHERE v.org_id = @org_id AND v.pipeline_id = @pipeline_id AND v.version = @version;

-- The pipelines whose current versions use any of the sources, destinations,
-- dbt projects, Spark clusters or Flink clusters.
-- name: ListPipelinesUsing :many
SELECT p.id, p.name, p.version, p.workspace_id, p.source_id, p.destination_id, w.name AS workspace, v.spec, v.dbt, v.spark_clusters, v.flink_clusters
FROM pipelines p
JOIN pipeline_versions v ON v.org_id = p.org_id AND v.pipeline_id = p.id AND v.version = p.version
JOIN workspaces w ON w.org_id = p.org_id AND w.id = p.workspace_id
WHERE p.org_id = @org_id AND (
    p.source_id = ANY(@sources::uuid[]) OR p.destination_id = ANY(@destinations::uuid[])
    OR EXISTS (
        SELECT 1 FROM jsonb_object_keys(v.dbt) k
        JOIN dbt_projects d ON d.org_id = p.org_id AND d.workspace_id = p.workspace_id AND d.name = k
        WHERE d.id = ANY(@dbt_projects::uuid[]))
    OR EXISTS (
        SELECT 1 FROM jsonb_object_keys(v.spark_clusters) k
        JOIN spark_clusters c ON c.org_id = p.org_id AND c.workspace_id = p.workspace_id AND c.name = k
        WHERE c.id = ANY(@spark_clusters::uuid[]))
    OR EXISTS (
        SELECT 1 FROM jsonb_object_keys(v.flink_clusters) k
        JOIN flink_clusters c ON c.org_id = p.org_id AND c.workspace_id = p.workspace_id AND c.name = k
        WHERE c.id = ANY(@flink_clusters::uuid[])))
ORDER BY w.name, p.name;

-- name: SetPipelinePaused :execrows
UPDATE pipelines SET paused = @paused WHERE org_id = @org_id AND id = @id AND paused <> @paused;

-- What events name a pipeline by.
-- name: GetPipelineNames :one
SELECT p.name, p.workspace_id, w.name AS workspace
FROM pipelines p JOIN workspaces w ON w.org_id = p.org_id AND w.id = p.workspace_id
WHERE p.org_id = @org_id AND p.id = @id;

-- name: StartLagging :execrows
UPDATE pipelines SET lagging_since = now() WHERE org_id = @org_id AND id = @id AND lagging_since IS NULL;

-- name: StopLagging :many
UPDATE pipelines p SET lagging_since = NULL
FROM (SELECT o.lagging_since FROM pipelines o WHERE o.org_id = @org_id AND o.id = @id FOR UPDATE) old
WHERE p.org_id = @org_id AND p.id = @id AND p.lagging_since IS NOT NULL
RETURNING old.lagging_since::timestamptz AS since;

-- Newest first, after the version (before_time, before_version) if given,
-- with the configs it pins where they're still there.
-- name: ListPipelineVersions :many
SELECT v.*, s.name AS source_name, sv.spec AS source_spec, sv.connector_digest AS source_connector,
    d.name AS destination_name, dv.spec AS destination_spec, dv.connector_digest AS destination_connector
FROM pipeline_versions v
LEFT JOIN sources s ON s.org_id = v.org_id AND s.id = v.source_id
LEFT JOIN source_versions sv ON sv.org_id = v.org_id AND sv.source_id = v.source_id AND sv.version = v.source_version
LEFT JOIN destinations d ON d.org_id = v.org_id AND d.id = v.destination_id
LEFT JOIN destination_versions dv ON dv.org_id = v.org_id AND dv.destination_id = v.destination_id AND dv.version = v.destination_version
WHERE v.org_id = @org_id AND v.pipeline_id = @pipeline_id
  AND (sqlc.narg('before_time')::timestamptz IS NULL OR (v.create_time, v.version) < (sqlc.narg('before_time'), @before_version::text))
ORDER BY v.create_time DESC, v.version DESC
LIMIT @max_rows;

-- name: GetPipelineVersionPins :one
SELECT v.*, s.name AS source_name, sv.spec AS source_spec, sv.connector_digest AS source_connector,
    d.name AS destination_name, dv.spec AS destination_spec, dv.connector_digest AS destination_connector
FROM pipeline_versions v
LEFT JOIN sources s ON s.org_id = v.org_id AND s.id = v.source_id
LEFT JOIN source_versions sv ON sv.org_id = v.org_id AND sv.source_id = v.source_id AND sv.version = v.source_version
LEFT JOIN destinations d ON d.org_id = v.org_id AND d.id = v.destination_id
LEFT JOIN destination_versions dv ON dv.org_id = v.org_id AND dv.destination_id = v.destination_id AND dv.version = v.destination_version
WHERE v.org_id = @org_id AND v.pipeline_id = @pipeline_id AND v.version = @version;

-- name: ListPipelinesToRun :many
-- The org's pipelines, with their current versions' specs, for what runs them,
-- and heal's override of how often syncs start, if any.
SELECT p.id, p.paused, v.spec,
    EXISTS (
        SELECT 1 FROM syncs s
        WHERE s.org_id = p.org_id AND s.pipeline_id = p.id AND s.continuous AND s.status = 'running'
    )::boolean AS running_run,
    (
        SELECT o.value FROM pipeline_overrides o
        WHERE o.org_id = p.org_id AND o.pipeline_id = p.id AND o.field = 'schedule.every'
    )::jsonb AS every_override
FROM pipelines p
JOIN pipeline_versions v ON v.org_id = p.org_id AND v.pipeline_id = p.id AND v.version = p.version
WHERE p.org_id = @org_id
ORDER BY p.id;

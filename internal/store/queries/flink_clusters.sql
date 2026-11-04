-- name: GetFlinkCluster :one
SELECT * FROM flink_clusters WHERE org_id = @org_id AND workspace_id = @workspace_id AND name = @name;

-- name: InsertFlinkCluster :exec
INSERT INTO flink_clusters (org_id, workspace_id, id, name, spec, version, secrets, revision)
VALUES (@org_id, @workspace_id, @id, @name, @spec, @version, @secrets, 1);

-- name: UpdateFlinkCluster :exec
UPDATE flink_clusters SET spec = @spec, version = @version, secrets = @secrets, revision = revision + 1, update_time = now()
WHERE org_id = @org_id AND id = @id;

-- name: InsertFlinkClusterVersion :exec
INSERT INTO flink_cluster_versions (org_id, cluster_id, version, spec, secrets, provenance)
VALUES (@org_id, @cluster_id, @version, @spec, @secrets, @provenance)
ON CONFLICT (org_id, cluster_id, version) DO NOTHING;

-- Keeps the cluster from being deleted until the transaction ends.
-- name: ShareFlinkCluster :one
SELECT * FROM flink_clusters WHERE org_id = @org_id AND workspace_id = @workspace_id AND name = @name
FOR SHARE;

-- name: LockFlinkCluster :one
SELECT * FROM flink_clusters WHERE org_id = @org_id AND workspace_id = @workspace_id AND name = @name
FOR UPDATE;

-- name: ListFlinkClusters :many
SELECT * FROM flink_clusters
WHERE org_id = @org_id AND workspace_id = @workspace_id AND name > @after
ORDER BY name LIMIT @max_rows;

-- name: DeleteFlinkCluster :exec
DELETE FROM flink_clusters WHERE org_id = @org_id AND id = @id;

-- The pipelines whose current versions use any of the clusters.
-- name: ListPipelinesUsingFlinkClusters :many
SELECT k.cluster::text AS cluster, p.name
FROM pipelines p
JOIN pipeline_versions v ON v.org_id = p.org_id AND v.pipeline_id = p.id AND v.version = p.version
CROSS JOIN LATERAL jsonb_object_keys(v.flink_clusters) AS k(cluster)
WHERE p.org_id = @org_id AND p.workspace_id = @workspace_id AND k.cluster = ANY(@names::text[])
ORDER BY p.name;

-- The pinned versions of a pipeline version's Flink clusters.
-- name: GetPinnedFlinkClusters :many
SELECT c.name, cv.version, cv.spec, cv.secrets
FROM pipeline_versions v
CROSS JOIN LATERAL jsonb_each_text(v.flink_clusters) AS k(name, version)
JOIN pipelines p ON p.org_id = v.org_id AND p.id = v.pipeline_id
JOIN flink_clusters c ON c.org_id = v.org_id AND c.workspace_id = p.workspace_id AND c.name = k.name
JOIN flink_cluster_versions cv ON cv.org_id = c.org_id AND cv.cluster_id = c.id AND cv.version = k.version
WHERE v.org_id = @org_id AND v.pipeline_id = @pipeline_id AND v.version = @version
ORDER BY c.name;

-- name: GetSparkCluster :one
SELECT * FROM spark_clusters WHERE org_id = @org_id AND workspace_id = @workspace_id AND name = @name;

-- name: InsertSparkCluster :exec
INSERT INTO spark_clusters (org_id, workspace_id, id, name, spec, version, secrets, revision)
VALUES (@org_id, @workspace_id, @id, @name, @spec, @version, @secrets, 1);

-- name: UpdateSparkCluster :exec
UPDATE spark_clusters SET spec = @spec, version = @version, secrets = @secrets, revision = revision + 1, update_time = now()
WHERE org_id = @org_id AND id = @id;

-- name: InsertSparkClusterVersion :exec
INSERT INTO spark_cluster_versions (org_id, cluster_id, version, spec, secrets, provenance)
VALUES (@org_id, @cluster_id, @version, @spec, @secrets, @provenance)
ON CONFLICT (org_id, cluster_id, version) DO NOTHING;

-- Keeps the cluster from being deleted until the transaction ends.
-- name: ShareSparkCluster :one
SELECT * FROM spark_clusters WHERE org_id = @org_id AND workspace_id = @workspace_id AND name = @name
FOR SHARE;

-- name: LockSparkCluster :one
SELECT * FROM spark_clusters WHERE org_id = @org_id AND workspace_id = @workspace_id AND name = @name
FOR UPDATE;

-- name: ListSparkClusters :many
SELECT * FROM spark_clusters
WHERE org_id = @org_id AND workspace_id = @workspace_id AND name > @after
ORDER BY name LIMIT @max_rows;

-- name: DeleteSparkCluster :exec
DELETE FROM spark_clusters WHERE org_id = @org_id AND id = @id;

-- The pipelines whose current versions use any of the clusters.
-- name: ListPipelinesUsingSparkClusters :many
SELECT k.cluster::text AS cluster, p.name
FROM pipelines p
JOIN pipeline_versions v ON v.org_id = p.org_id AND v.pipeline_id = p.id AND v.version = p.version
CROSS JOIN LATERAL jsonb_object_keys(v.spark_clusters) AS k(cluster)
WHERE p.org_id = @org_id AND p.workspace_id = @workspace_id AND k.cluster = ANY(@names::text[])
ORDER BY p.name;

-- The pinned versions of a pipeline version's Spark clusters.
-- name: GetPinnedSparkClusters :many
SELECT c.name, cv.version, cv.spec, cv.secrets
FROM pipeline_versions v
CROSS JOIN LATERAL jsonb_each_text(v.spark_clusters) AS k(name, version)
JOIN pipelines p ON p.org_id = v.org_id AND p.id = v.pipeline_id
JOIN spark_clusters c ON c.org_id = v.org_id AND c.workspace_id = p.workspace_id AND c.name = k.name
JOIN spark_cluster_versions cv ON cv.org_id = c.org_id AND cv.cluster_id = c.id AND cv.version = k.version
WHERE v.org_id = @org_id AND v.pipeline_id = @pipeline_id AND v.version = @version
ORDER BY c.name;

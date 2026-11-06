-- name: GetOrgKey :one
SELECT wrapped_key FROM org_keys WHERE org_id = @org_id;

-- name: InsertOrgKey :exec
INSERT INTO org_keys (org_id, wrapped_key) VALUES (@org_id, @wrapped_key)
ON CONFLICT (org_id) DO NOTHING;

-- name: GetSecret :one
SELECT * FROM secrets WHERE org_id = @org_id AND workspace_id = @workspace_id AND name = @name;

-- name: LockSecret :one
SELECT * FROM secrets WHERE org_id = @org_id AND workspace_id = @workspace_id AND name = @name
FOR UPDATE;

-- Keeps the secrets from being deleted until the transaction ends.
-- name: ShareSecrets :many
SELECT name FROM secrets WHERE org_id = @org_id AND workspace_id = @workspace_id AND name = ANY(@names::text[])
FOR SHARE;

-- name: ListSecrets :many
SELECT * FROM secrets
WHERE org_id = @org_id AND workspace_id = @workspace_id AND name > @after
ORDER BY name LIMIT @max_rows;

-- name: GetSecretValues :many
SELECT s.id, s.name, s.ciphertext, s.ref, w.name AS workspace
FROM secrets s JOIN workspaces w ON w.org_id = s.org_id AND w.id = s.workspace_id
WHERE s.org_id = @org_id AND s.workspace_id = @workspace_id AND s.name = ANY(@names::text[]);

-- name: InsertSecret :exec
INSERT INTO secrets (org_id, workspace_id, id, name, ciphertext, ref, revision)
VALUES (@org_id, @workspace_id, @id, @name, @ciphertext, @ref, 1);

-- name: UpdateSecret :exec
UPDATE secrets SET ciphertext = @ciphertext, ref = @ref, revision = revision + 1, update_time = now()
WHERE org_id = @org_id AND id = @id;

-- name: DeleteSecret :exec
DELETE FROM secrets WHERE org_id = @org_id AND id = @id;

-- The sources whose configs name any of the secrets.
-- name: ListSourcesUsingSecrets :many
SELECT name, secrets FROM sources
WHERE org_id = @org_id AND workspace_id = @workspace_id AND secrets && @names::text[]
ORDER BY name;

-- name: ListDestinationsUsingSecrets :many
SELECT name, secrets FROM destinations
WHERE org_id = @org_id AND workspace_id = @workspace_id AND secrets && @names::text[]
ORDER BY name;

-- name: ListSparkClustersUsingSecrets :many
SELECT name, secrets FROM spark_clusters
WHERE org_id = @org_id AND workspace_id = @workspace_id AND secrets && @names::text[]
ORDER BY name;

-- name: ListFlinkClustersUsingSecrets :many
SELECT name, secrets FROM flink_clusters
WHERE org_id = @org_id AND workspace_id = @workspace_id AND secrets && @names::text[]
ORDER BY name;

-- The pipelines whose current versions name any of the secrets.
-- name: ListPipelinesUsingSecrets :many
SELECT p.name, v.secrets FROM pipelines p
JOIN pipeline_versions v ON v.org_id = p.org_id AND v.pipeline_id = p.id AND v.version = p.version
WHERE p.org_id = @org_id AND p.workspace_id = @workspace_id AND v.secrets && @names::text[]
ORDER BY p.name;

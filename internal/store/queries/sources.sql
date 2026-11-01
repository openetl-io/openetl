-- name: GetSource :one
SELECT * FROM sources WHERE org_id = @org_id AND workspace_id = @workspace_id AND name = @name;

-- name: ListSources :many
SELECT * FROM sources
WHERE org_id = @org_id AND workspace_id = @workspace_id AND name > @after
ORDER BY name LIMIT @max_rows;

-- name: InsertSource :exec
INSERT INTO sources (org_id, workspace_id, id, name, spec, version, connector_digest, secrets, revision)
VALUES (@org_id, @workspace_id, @id, @name, @spec, @version, @connector_digest, @secrets, 1);

-- name: UpdateSource :exec
UPDATE sources SET spec = @spec, version = @version, connector_digest = @connector_digest, secrets = @secrets, revision = revision + 1, update_time = now()
WHERE org_id = @org_id AND id = @id;

-- name: DeleteSource :execrows
DELETE FROM sources WHERE org_id = @org_id AND id = @id;

-- name: GetSourceByID :one
SELECT * FROM sources WHERE org_id = @org_id AND id = @id;

-- name: UpsertSourceCatalog :exec
INSERT INTO source_catalogs (org_id, source_id, digest, source_version, catalog)
VALUES (@org_id, @source_id, @digest, @source_version, @catalog)
ON CONFLICT (org_id, source_id, digest)
DO UPDATE SET source_version = excluded.source_version, discover_time = now();

-- name: GetSourceCatalog :one
SELECT * FROM source_catalogs WHERE org_id = @org_id AND source_id = @source_id AND digest = @digest;

-- name: InsertSourceVersion :exec
INSERT INTO source_versions (org_id, source_id, version, spec, connector_digest, secrets, provenance)
VALUES (@org_id, @source_id, @version, @spec, @connector_digest, @secrets, @provenance)
ON CONFLICT (org_id, source_id, version) DO NOTHING;

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
SELECT id, name, ciphertext FROM secrets
WHERE org_id = @org_id AND workspace_id = @workspace_id AND name = ANY(@names::text[]);

-- name: InsertSecret :exec
INSERT INTO secrets (org_id, workspace_id, id, name, ciphertext, revision)
VALUES (@org_id, @workspace_id, @id, @name, @ciphertext, 1);

-- name: UpdateSecret :exec
UPDATE secrets SET ciphertext = @ciphertext, revision = revision + 1, update_time = now()
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

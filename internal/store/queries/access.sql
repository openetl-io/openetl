-- name: CreatePrincipal :exec
INSERT INTO principals (org_id, id, name, type, display_name, email)
VALUES (@org_id, @id, @name, @type, @display_name, @email);

-- name: GetPrincipal :one
SELECT * FROM principals WHERE org_id = @org_id AND name = @name;

-- name: ListPrincipals :many
SELECT * FROM principals
WHERE org_id = @org_id AND name > @after
ORDER BY name
LIMIT @max_rows;

-- name: DeletePrincipal :execrows
DELETE FROM principals WHERE org_id = @org_id AND id = @id;

-- name: SetPrincipalDisabled :exec
UPDATE principals SET disabled = @disabled WHERE org_id = @org_id AND id = @id;

-- name: ListRoleBindings :many
SELECT b.principal_id, b.role, coalesce(w.name, '')::text AS workspace
FROM role_bindings b LEFT JOIN workspaces w ON w.org_id = b.org_id AND w.id = b.workspace_id
WHERE b.org_id = @org_id AND b.principal_id = ANY(@principal_ids::uuid[])
ORDER BY b.principal_id, workspace, b.role;

-- name: InsertRoleBinding :execrows
INSERT INTO role_bindings (org_id, principal_id, role, workspace_id)
VALUES (@org_id, @principal_id, @role, sqlc.narg('workspace_id'))
ON CONFLICT DO NOTHING;

-- name: DeleteRoleBinding :execrows
DELETE FROM role_bindings
WHERE org_id = @org_id AND principal_id = @principal_id AND role = @role
  AND workspace_id IS NOT DISTINCT FROM sqlc.narg('workspace_id');

-- The enabled principals bound as owner on the org, locked, so changes that
-- could leave none are serialized.
-- name: LockOrgOwners :many
SELECT p.id FROM role_bindings b JOIN principals p ON p.org_id = b.org_id AND p.id = b.principal_id
WHERE b.org_id = @org_id AND b.role = 'owner' AND b.workspace_id IS NULL AND NOT p.disabled
FOR UPDATE;

-- name: InsertAPIKey :exec
INSERT INTO api_keys (org_id, id, principal_id, hash, prefix, expire_time)
VALUES (@org_id, @id, @principal_id, @hash, @prefix, sqlc.narg('expire_time'));

-- name: ListAPIKeys :many
SELECT id, prefix, expire_time, last_used_time, create_time FROM api_keys
WHERE org_id = @org_id AND principal_id = @principal_id
ORDER BY create_time, id;

-- name: DeleteAPIKey :execrows
DELETE FROM api_keys WHERE org_id = @org_id AND principal_id = @principal_id AND id = @id;

-- Before the org is known, so it isn't scoped by org_id. Each binding is
-- role/workspace, or the role alone on the org.
-- name: AuthenticateAPIKey :one
SELECT k.org_id, k.id, k.principal_id, p.name AS principal, k.last_used_time, k.expire_time,
    array(
        SELECT b.role || coalesce('/' || w.name, '')
        FROM role_bindings b LEFT JOIN workspaces w ON w.org_id = b.org_id AND w.id = b.workspace_id
        WHERE b.org_id = k.org_id AND b.principal_id = k.principal_id
    )::text[] AS bindings
FROM api_keys k JOIN principals p ON p.org_id = k.org_id AND p.id = k.principal_id
WHERE k.hash = @hash AND NOT p.disabled AND (k.expire_time IS NULL OR k.expire_time > now());

-- name: TouchAPIKey :exec
UPDATE api_keys SET last_used_time = now() WHERE org_id = @org_id AND id = @id;

-- name: InsertRunnerToken :exec
INSERT INTO runner_tokens (org_id, id, data_plane_id, hash, prefix)
VALUES (@org_id, @id, @data_plane_id, @hash, @prefix);

-- name: ListRunnerTokens :many
SELECT id, prefix, last_used_time, create_time FROM runner_tokens
WHERE org_id = @org_id AND data_plane_id = @data_plane_id
ORDER BY create_time, id;

-- name: DeleteRunnerToken :execrows
DELETE FROM runner_tokens WHERE org_id = @org_id AND data_plane_id = @data_plane_id AND id = @id;

-- Before the org is known, so it isn't scoped by org_id.
-- name: AuthenticateRunnerToken :one
SELECT org_id, id, data_plane_id, last_used_time FROM runner_tokens WHERE hash = @hash;

-- name: TouchRunnerToken :exec
UPDATE runner_tokens SET last_used_time = now() WHERE org_id = @org_id AND id = @id;

-- name: InsertConnectorOp :exec
INSERT INTO connector_ops (org_id, id, data_plane_id, source_id, destination_id, pipeline_id, flink_cluster_id, flink_version, expire_time)
VALUES (@org_id, @id, @data_plane_id, sqlc.narg('source_id'), sqlc.narg('destination_id'),
    sqlc.narg('pipeline_id'), sqlc.narg('flink_cluster_id'), sqlc.narg('flink_version'), @expire_time);

-- name: DeleteConnectorOp :exec
DELETE FROM connector_ops WHERE org_id = @org_id AND id = @id;

-- name: DeleteExpiredConnectorOps :exec
DELETE FROM connector_ops WHERE org_id = @org_id AND expire_time < now();

-- name: GetConnectorOp :one
SELECT source_id, destination_id, pipeline_id FROM connector_ops
WHERE org_id = @org_id AND id = @id AND data_plane_id = @data_plane_id AND expire_time > now();

-- A Flink teardown the API sent to the data plane: the deleted pipeline,
-- and the version of the Flink cluster it pinned.
-- name: GetFlinkTeardown :one
SELECT o.pipeline_id, c.workspace_id, c.name, cv.version, cv.spec, cv.secrets
FROM connector_ops o
JOIN flink_clusters c ON c.org_id = o.org_id AND c.id = o.flink_cluster_id
JOIN flink_cluster_versions cv ON cv.org_id = o.org_id AND cv.cluster_id = o.flink_cluster_id AND cv.version = o.flink_version
WHERE o.org_id = @org_id AND o.id = @id AND o.data_plane_id = @data_plane_id AND o.expire_time > now();

-- Whether the pipeline exists, and whether a version of it runs on the data
-- plane.
-- name: PipelineOnDataPlane :one
SELECT
    EXISTS (SELECT 1 FROM pipelines p WHERE p.org_id = @org_id AND p.id = @pipeline_id) AS found,
    EXISTS (
        SELECT 1 FROM pipeline_versions v
        WHERE v.org_id = @org_id AND v.pipeline_id = @pipeline_id AND v.data_plane_id = @data_plane_id
    ) AS on_data_plane;

-- name: ListDataPlanes :many
SELECT id, create_time FROM data_planes WHERE org_id = @org_id ORDER BY id;

-- A user who may sign in, by email, with their password if they have one.
-- name: GetSignIn :one
SELECT p.id, p.name, pw.hash, pw.locked_until
FROM principals p LEFT JOIN passwords pw ON pw.org_id = p.org_id AND pw.principal_id = p.id
WHERE p.org_id = @org_id AND lower(p.email) = lower(@email) AND p.type = 'user' AND NOT p.disabled;

-- name: GetPassword :one
SELECT hash, locked_until FROM passwords WHERE org_id = @org_id AND principal_id = @principal_id;

-- Sets a password and clears its failures and lock.
-- name: SetPassword :exec
INSERT INTO passwords (org_id, principal_id, hash)
VALUES (@org_id, @principal_id, @hash)
ON CONFLICT (org_id, principal_id) DO UPDATE SET hash = excluded.hash, failures = 0, locked_until = NULL, update_time = now();

-- Counts a failed sign-in, and locks the password for lock_seconds once
-- max_failures fail in a row. The count starts again once a lock is over.
-- name: RecordSignInFailure :exec
UPDATE passwords SET
    failures = CASE WHEN locked_until <= now() THEN 1 ELSE failures + 1 END,
    locked_until = CASE
        WHEN locked_until > now() THEN locked_until
        WHEN (CASE WHEN locked_until <= now() THEN 1 ELSE failures + 1 END) >= @max_failures::integer
            THEN now() + make_interval(secs => @lock_seconds::integer)
    END
WHERE org_id = @org_id AND principal_id = @principal_id;

-- name: ClearSignInFailures :exec
UPDATE passwords SET failures = 0, locked_until = NULL
WHERE org_id = @org_id AND principal_id = @principal_id AND failures > 0;

-- name: UpsertPasswordLink :exec
INSERT INTO password_links (org_id, principal_id, hash, expire_time)
VALUES (@org_id, @principal_id, @hash, @expire_time)
ON CONFLICT (org_id, principal_id) DO UPDATE SET hash = excluded.hash, expire_time = excluded.expire_time, create_time = now();

-- A link's user, if the link is still good.
-- name: GetPasswordLink :one
SELECT p.id, p.name, p.email
FROM password_links l JOIN principals p ON p.org_id = l.org_id AND p.id = l.principal_id
WHERE l.org_id = @org_id AND l.hash = @hash AND l.expire_time > now() AND NOT p.disabled;

-- name: DeletePasswordLink :execrows
DELETE FROM password_links WHERE org_id = @org_id AND hash = @hash;

-- name: InsertSession :exec
INSERT INTO sessions (org_id, id, principal_id, hash, expire_time, peer, user_agent)
VALUES (@org_id, @id, @principal_id, @hash, @expire_time, @peer, @user_agent);

-- Before the org is known, so it isn't scoped by org_id. A session idle
-- since before idle_since has ended.
-- name: AuthenticateSession :one
SELECT s.org_id, s.id, s.principal_id, p.name AS principal, s.last_used_time, s.expire_time,
    array(
        SELECT b.role || coalesce('/' || w.name, '')
        FROM role_bindings b LEFT JOIN workspaces w ON w.org_id = b.org_id AND w.id = b.workspace_id
        WHERE b.org_id = s.org_id AND b.principal_id = s.principal_id
    )::text[] AS bindings
FROM sessions s JOIN principals p ON p.org_id = s.org_id AND p.id = s.principal_id
WHERE s.hash = @hash AND NOT p.disabled AND s.expire_time > now() AND s.last_used_time > @idle_since;

-- name: TouchSession :exec
UPDATE sessions SET last_used_time = now() WHERE org_id = @org_id AND id = @id;

-- name: DeleteSessionByHash :one
WITH deleted AS (
    DELETE FROM sessions WHERE org_id = @org_id AND hash = @hash RETURNING id, principal_id
)
SELECT d.id, p.name AS principal
FROM deleted d JOIN principals p ON p.org_id = @org_id AND p.id = d.principal_id;

-- Every session of a principal but keep, which may be the nil UUID.
-- name: DeleteSessions :execrows
DELETE FROM sessions WHERE org_id = @org_id AND principal_id = @principal_id AND id <> @keep;

-- name: DeleteEndedSessions :exec
DELETE FROM sessions
WHERE org_id = @org_id AND principal_id = @principal_id AND (expire_time <= now() OR last_used_time <= @idle_since);

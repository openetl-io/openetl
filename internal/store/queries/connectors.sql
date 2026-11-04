-- name: InsertArtifact :exec
INSERT INTO artifacts (org_id, digest, media_type, content)
VALUES (@org_id, @digest, @media_type, @content)
ON CONFLICT (org_id, digest) DO NOTHING;

-- name: GetArtifact :one
SELECT * FROM artifacts WHERE org_id = @org_id AND digest = @digest;

-- name: InsertConnectorVersion :exec
INSERT INTO connector_versions (org_id, name, version, provenance, description, license, repository, docs, tags, author, logo, release_tag, release_url)
VALUES (@org_id, @name, @version, @provenance, @description, @license, @repository, @docs, @tags, @author, @logo, @release_tag, @release_url);

-- name: GetConnectorVersion :one
SELECT * FROM connector_versions
WHERE org_id = @org_id AND name = @name AND version = @version;

-- name: InsertConnectorRole :exec
INSERT INTO connector_roles (org_id, name, version, role, digest)
VALUES (@org_id, @name, @version, @role, @digest);

-- name: ListVersionRoles :many
SELECT * FROM connector_roles
WHERE org_id = @org_id AND name = @name AND version = @version;

-- name: ListConnectorRoles :many
-- A package's versions with role, each with its role's digest.
SELECT v.version, r.digest FROM connector_versions v
JOIN connector_roles r USING (org_id, name, version)
WHERE v.org_id = @org_id AND v.name = @name AND r.role = @role;

-- name: ListConnectorVersions :many
SELECT * FROM connector_versions
WHERE org_id = @org_id AND name = @name;

-- name: LatestRepository :one
-- The repository of the package's most recently applied version that has
-- one; no rows if none has.
SELECT repository FROM connector_versions
WHERE org_id = @org_id AND name = @name AND repository <> ''
ORDER BY create_time DESC
LIMIT 1;

-- name: ConnectorVersionsByDigest :many
SELECT v.*, r.role, r.digest FROM connector_versions v
JOIN connector_roles r USING (org_id, name, version)
WHERE v.org_id = @org_id AND r.digest = ANY(@digests::text[]);

-- name: ReportRunner :exec
INSERT INTO runners (org_id, data_plane_id, id, release, protocol, features, launchers, platform, report_time)
VALUES (@org_id, @data_plane_id, @id, @release, @protocol, @features, @launchers, @platform, now())
ON CONFLICT (org_id, data_plane_id, id) DO UPDATE SET
    release = EXCLUDED.release, protocol = EXCLUDED.protocol, features = EXCLUDED.features,
    launchers = EXCLUDED.launchers, platform = EXCLUDED.platform, report_time = now();

-- name: RecentRunners :many
-- A data plane's runners that reported since @since; old reports go.
SELECT * FROM runners
WHERE org_id = @org_id AND data_plane_id = @data_plane_id AND report_time >= @since
ORDER BY id;

-- name: DeleteStaleRunners :exec
DELETE FROM runners WHERE org_id = @org_id AND report_time < @before;

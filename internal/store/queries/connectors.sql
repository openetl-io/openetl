-- name: InsertArtifact :exec
INSERT INTO artifacts (org_id, digest, media_type, content)
VALUES (@org_id, @digest, @media_type, @content)
ON CONFLICT (org_id, digest) DO NOTHING;

-- name: GetArtifact :one
SELECT * FROM artifacts WHERE org_id = @org_id AND digest = @digest;

-- name: InsertConnectorVersion :exec
INSERT INTO connector_versions (org_id, kind, name, version, digest, provenance, author, logo, release_tag, release_url)
VALUES (@org_id, @kind, @name, @version, @digest, @provenance, @author, @logo, @release_tag, @release_url);

-- name: GetConnectorVersion :one
SELECT * FROM connector_versions
WHERE org_id = @org_id AND kind = @kind AND name = @name AND version = @version;

-- name: ListConnectorVersions :many
SELECT * FROM connector_versions
WHERE org_id = @org_id AND kind = @kind AND name = @name;

-- name: ConnectorVersionsByDigest :many
SELECT * FROM connector_versions WHERE org_id = @org_id AND digest = ANY(@digests::text[]);

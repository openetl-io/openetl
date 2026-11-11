-- name: UpsertConnectorSource :one
INSERT INTO connector_sources (org_id, name, url, username, secret, official)
VALUES (@org_id, @name, @url, @username, @secret, @official)
ON CONFLICT (org_id, name) DO UPDATE SET
    url = EXCLUDED.url, username = EXCLUDED.username, secret = EXCLUDED.secret, update_time = now(),
    -- A new URL starts its listing over.
    refresh_time = CASE WHEN connector_sources.url = EXCLUDED.url THEN connector_sources.refresh_time END,
    error = CASE WHEN connector_sources.url = EXCLUDED.url THEN connector_sources.error ELSE '' END,
    error_time = CASE WHEN connector_sources.url = EXCLUDED.url THEN connector_sources.error_time END
RETURNING (xmax = 0)::boolean AS created;

-- name: GetConnectorSource :one
SELECT * FROM connector_sources WHERE org_id = @org_id AND name = @name;

-- name: ListConnectorSources :many
SELECT s.*, (SELECT count(*) FROM connector_catalog c WHERE c.org_id = s.org_id AND c.source = s.name)::int AS packages
FROM connector_sources s WHERE s.org_id = @org_id ORDER BY s.name;

-- name: DeleteConnectorSource :execrows
DELETE FROM connector_sources WHERE org_id = @org_id AND name = @name;

-- name: ConnectOfficialSource :exec
-- Connects the official source once: an org that ever had it doesn't get it
-- back after deleting it.
WITH first AS (
    UPDATE orgs SET official_connectors_connected = true
    WHERE id = @org_id AND NOT official_connectors_connected
    RETURNING id
)
INSERT INTO connector_sources (org_id, name, url, official)
SELECT id, @name, @url, true FROM first
ON CONFLICT (org_id, name) DO NOTHING;

-- name: SourceListed :exec
UPDATE connector_sources SET refresh_time = now(), error = '', error_time = NULL
WHERE org_id = @org_id AND name = @name AND url = @url;

-- name: SourceFailed :exec
UPDATE connector_sources SET error = @error, error_time = now()
WHERE org_id = @org_id AND name = @name AND url = @url;

-- name: ListSourceCatalog :many
SELECT * FROM connector_catalog WHERE org_id = @org_id AND source = @source;

-- name: UpsertCatalogPackage :exec
INSERT INTO connector_catalog (
    org_id, source, package, key, title, version, author, description, license, repository, docs, roles, tags,
    reference, card_digest, logo_digest
)
VALUES (
    @org_id, @source, @package, @key, @title, @version, @author, @description, @license, @repository, @docs, @roles, @tags,
    @reference, @card_digest, @logo_digest
)
ON CONFLICT (org_id, source, package) DO UPDATE SET
    key = EXCLUDED.key, title = EXCLUDED.title, version = EXCLUDED.version, author = EXCLUDED.author,
    description = EXCLUDED.description, license = EXCLUDED.license, repository = EXCLUDED.repository,
    docs = EXCLUDED.docs, roles = EXCLUDED.roles, tags = EXCLUDED.tags, reference = EXCLUDED.reference,
    logo_digest = EXCLUDED.logo_digest, update_time = now(),
    -- Another card is another version, whose descriptor is fetched again.
    descriptor_digest = CASE WHEN connector_catalog.card_digest = EXCLUDED.card_digest THEN connector_catalog.descriptor_digest END,
    card_digest = EXCLUDED.card_digest;

-- name: DeleteCatalogPackages :exec
-- The source's packages that its last listing didn't list.
DELETE FROM connector_catalog
WHERE org_id = @org_id AND source = @source AND NOT (package = ANY(@keep::text[]));

-- name: ListCatalog :many
-- Each package with the highest version the org has applied, if any.
SELECT c.*, s.official AS official_source,
    (SELECT coalesce(array_agg(v.version), '{}')::text[] FROM connector_versions v WHERE v.org_id = c.org_id AND v.name = c.package) AS applied
FROM connector_catalog c
JOIN connector_sources s ON s.org_id = c.org_id AND s.name = c.source
WHERE c.org_id = @org_id
ORDER BY c.package, c.source;

-- name: ListManualConnectorVersions :many
-- Every applied version of the packages no connected source lists, with its
-- roles.
SELECT v.*,
    (SELECT coalesce(array_agg(r.role ORDER BY r.role), '{}')::text[] FROM connector_roles r
     WHERE r.org_id = v.org_id AND r.name = v.name AND r.version = v.version) AS roles
FROM connector_versions v
WHERE v.org_id = @org_id AND NOT EXISTS (SELECT 1 FROM connector_catalog c WHERE c.org_id = v.org_id AND c.package = v.name)
ORDER BY v.name, v.version;

-- name: GetCatalogPackage :one
SELECT c.*, s.official AS official_source,
    (SELECT coalesce(array_agg(v.version), '{}')::text[] FROM connector_versions v WHERE v.org_id = c.org_id AND v.name = c.package) AS applied
FROM connector_catalog c
JOIN connector_sources s ON s.org_id = c.org_id AND s.name = c.source
WHERE c.org_id = @org_id AND c.source = @source AND c.package = @package;

-- name: SetCatalogDescriptor :exec
UPDATE connector_catalog SET descriptor_digest = @descriptor_digest
WHERE org_id = @org_id AND source = @source AND package = @package AND card_digest = @card_digest;

-- name: GetLogo :one
SELECT media_type, content FROM artifacts
WHERE org_id = @org_id AND digest = @digest AND media_type IN ('image/svg+xml', 'image/png');

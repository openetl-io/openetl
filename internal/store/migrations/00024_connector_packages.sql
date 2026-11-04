-- +goose Up
-- A connector version is a package's: owner/name and a version, with a
-- source role, a destination role or both. Each role's artifact is in
-- connector_roles; a v1alpha2 descriptor is one artifact for every role it
-- has, and v1alpha1 descriptors and manifests are one artifact per role.
CREATE TABLE connector_roles (
    org_id  uuid NOT NULL,
    name    text NOT NULL,
    version text NOT NULL,
    role    text NOT NULL CHECK (role IN ('source', 'destination')),
    digest  text NOT NULL,
    PRIMARY KEY (org_id, name, version, role),
    FOREIGN KEY (org_id, digest) REFERENCES artifacts (org_id, digest)
);
CREATE INDEX connector_roles_digest ON connector_roles (org_id, digest);

-- Names were bare; a bare name is openetl's.
INSERT INTO connector_roles (org_id, name, version, role, digest)
SELECT org_id, 'openetl/' || name, version, kind, digest FROM connector_versions;

-- A source and a destination of one name and version become one version,
-- keeping the earlier one's metadata.
DELETE FROM connector_versions d USING connector_versions s
WHERE d.org_id = s.org_id AND d.name = s.name AND d.version = s.version
  AND (d.create_time, d.kind) > (s.create_time, s.kind);
ALTER TABLE connector_versions DROP CONSTRAINT connector_versions_pkey;
ALTER TABLE connector_versions DROP CONSTRAINT connector_versions_org_id_digest_fkey;
UPDATE connector_versions SET name = 'openetl/' || name;
ALTER TABLE connector_versions
    DROP COLUMN kind,
    DROP COLUMN digest,
    ADD COLUMN description text NOT NULL DEFAULT '',
    ADD COLUMN license     text NOT NULL DEFAULT '',
    ADD COLUMN repository  text NOT NULL DEFAULT '',
    ADD COLUMN docs        text NOT NULL DEFAULT '',
    ADD COLUMN tags        text[] NOT NULL DEFAULT '{}',
    ADD PRIMARY KEY (org_id, name, version);
ALTER TABLE connector_roles
    ADD FOREIGN KEY (org_id, name, version) REFERENCES connector_versions (org_id, name, version);

-- What each runner last reported of itself, for the plan's checks of
-- connectors against a data plane's runners.
CREATE TABLE runners (
    org_id        uuid NOT NULL,
    data_plane_id text NOT NULL,
    id            text NOT NULL,
    release       text NOT NULL,
    protocol      text NOT NULL,
    features      text[] NOT NULL,
    launchers     text[] NOT NULL,
    platform      text NOT NULL,
    report_time   timestamptz NOT NULL,
    PRIMARY KEY (org_id, data_plane_id, id),
    FOREIGN KEY (org_id, data_plane_id) REFERENCES data_planes (org_id, id)
);

-- +goose Down
DROP TABLE runners;
ALTER TABLE connector_roles DROP CONSTRAINT connector_roles_org_id_name_version_fkey;
ALTER TABLE connector_versions
    DROP CONSTRAINT connector_versions_pkey,
    DROP COLUMN description,
    DROP COLUMN license,
    DROP COLUMN repository,
    DROP COLUMN docs,
    DROP COLUMN tags,
    ADD COLUMN kind text,
    ADD COLUMN digest text;
INSERT INTO connector_versions (org_id, kind, name, version, digest, provenance, create_time, author, logo, release_tag, release_url)
SELECT v.org_id, r.role, v.name, v.version, r.digest, v.provenance, v.create_time, v.author, v.logo, v.release_tag, v.release_url
FROM connector_versions v JOIN connector_roles r USING (org_id, name, version);
DELETE FROM connector_versions WHERE kind IS NULL;
DROP TABLE connector_roles;
UPDATE connector_versions SET name = regexp_replace(name, '^openetl/', '');
ALTER TABLE connector_versions
    ALTER COLUMN kind SET NOT NULL,
    ALTER COLUMN digest SET NOT NULL,
    ADD PRIMARY KEY (org_id, kind, name, version),
    ADD FOREIGN KEY (org_id, digest) REFERENCES artifacts (org_id, digest);

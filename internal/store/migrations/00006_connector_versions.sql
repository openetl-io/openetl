-- +goose Up
-- Content-addressed: the digest is sha256:{hex} of the content.
CREATE TABLE artifacts (
    org_id      uuid NOT NULL REFERENCES orgs (id),
    digest      text NOT NULL,
    media_type  text NOT NULL,
    content     bytea NOT NULL,
    create_time timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, digest)
);

-- Immutable. A connector is a name per kind, source or destination, and each
-- version is an artifact: an external connector's descriptor, or a
-- declarative source's manifest. The content holds the version, so a digest
-- is one version.
CREATE TABLE connector_versions (
    org_id      uuid NOT NULL,
    kind        text NOT NULL,
    name        text NOT NULL,
    version     text NOT NULL,
    digest      text NOT NULL,
    -- Who applied it, and from where: such as cli:connectors.yaml#1.
    provenance  text NOT NULL,
    create_time timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, kind, name, version),
    FOREIGN KEY (org_id, digest) REFERENCES artifacts (org_id, digest)
);

-- The connector version each one resolved to when it was applied; empty for
-- a built-in connector, or one a runner finds in its connectors directory.
ALTER TABLE sources ADD COLUMN connector_digest text NOT NULL DEFAULT '';
ALTER TABLE destinations ADD COLUMN connector_digest text NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE destinations DROP COLUMN connector_digest;
ALTER TABLE sources DROP COLUMN connector_digest;
DROP TABLE connector_versions;
DROP TABLE artifacts;

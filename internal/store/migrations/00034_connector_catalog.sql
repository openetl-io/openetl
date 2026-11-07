-- +goose Up
-- Connected sources of connectors: an OCI registry namespace or a GitHub
-- repository, listed into the catalog. secret is a secret's resource name,
-- workspaces/{workspace}/secrets/{secret}, read when the source is listed or
-- a runner fetches under its URL.
CREATE TABLE connector_sources (
    org_id        uuid NOT NULL REFERENCES orgs (id),
    name          text NOT NULL,
    url           text NOT NULL,
    username      text NOT NULL DEFAULT '',
    secret        text NOT NULL DEFAULT '',
    official      boolean NOT NULL DEFAULT false,
    create_time   timestamptz NOT NULL DEFAULT now(),
    update_time   timestamptz NOT NULL DEFAULT now(),
    -- The last listing without an error, and the last error since.
    refresh_time  timestamptz,
    error         text NOT NULL DEFAULT '',
    error_time    timestamptz,
    PRIMARY KEY (org_id, name)
);

-- Each package a source last listed, at its highest version, as its index
-- card or release says. card_digest tells a listing what it has fetched
-- already; descriptor_digest is the highest version's descriptor, fetched
-- when someone opens the package, and logo_digest its logo; both are
-- artifacts.
CREATE TABLE connector_catalog (
    org_id            uuid NOT NULL,
    source            text NOT NULL,
    package           text NOT NULL,
    -- The index tag or repository it was listed by.
    key               text NOT NULL,
    title             text NOT NULL,
    version           text NOT NULL,
    author            text NOT NULL DEFAULT '',
    description       text NOT NULL DEFAULT '',
    license           text NOT NULL DEFAULT '',
    repository        text NOT NULL DEFAULT '',
    docs              text NOT NULL DEFAULT '',
    roles             text[] NOT NULL,
    tags              text[] NOT NULL DEFAULT '{}',
    reference         text NOT NULL,
    card_digest       text NOT NULL,
    logo_digest       text,
    descriptor_digest text,
    update_time       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, source, package),
    FOREIGN KEY (org_id, source) REFERENCES connector_sources (org_id, name) ON DELETE CASCADE
);

-- Whether the official source was ever connected, so deleting it sticks.
ALTER TABLE orgs ADD COLUMN official_connectors_connected boolean NOT NULL DEFAULT false;

ALTER TABLE connector_versions
    ADD COLUMN title       text NOT NULL DEFAULT '',
    ADD COLUMN logo_digest text;

-- +goose Down
ALTER TABLE connector_versions DROP COLUMN title, DROP COLUMN logo_digest;
ALTER TABLE orgs DROP COLUMN official_connectors_connected;
DROP TABLE connector_catalog;
DROP TABLE connector_sources;

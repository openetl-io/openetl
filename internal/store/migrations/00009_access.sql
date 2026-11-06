-- +goose Up
-- Users and service accounts. A disabled principal's keys don't work.
CREATE TABLE principals (
    org_id       uuid NOT NULL REFERENCES orgs (id),
    id           uuid NOT NULL,
    name         text NOT NULL,
    -- user or service_account.
    type         text NOT NULL,
    display_name text NOT NULL DEFAULT '',
    email        text NOT NULL DEFAULT '',
    disabled     boolean NOT NULL DEFAULT false,
    create_time  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, id),
    UNIQUE (org_id, name)
);

-- Keys by their SHA-256; prefix is the key's first characters, to tell
-- keys apart.
CREATE TABLE api_keys (
    org_id         uuid NOT NULL,
    id             uuid NOT NULL,
    principal_id   uuid NOT NULL,
    hash           bytea NOT NULL UNIQUE,
    prefix         text NOT NULL,
    expire_time    timestamptz,
    last_used_time timestamptz,
    create_time    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, id),
    FOREIGN KEY (org_id, principal_id) REFERENCES principals (org_id, id) ON DELETE CASCADE
);

-- A NULL workspace means the org.
CREATE TABLE role_bindings (
    org_id       uuid NOT NULL,
    principal_id uuid NOT NULL,
    role         text NOT NULL,
    workspace_id uuid,
    create_time  timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (org_id, principal_id) REFERENCES principals (org_id, id) ON DELETE CASCADE,
    FOREIGN KEY (org_id, workspace_id) REFERENCES workspaces (org_id, id)
);

CREATE UNIQUE INDEX role_bindings_unique ON role_bindings (org_id, principal_id, role, workspace_id) NULLS NOT DISTINCT;

CREATE TABLE runner_tokens (
    org_id         uuid NOT NULL,
    id             uuid NOT NULL,
    data_plane_id  text NOT NULL,
    hash           bytea NOT NULL UNIQUE,
    prefix         text NOT NULL,
    last_used_time timestamptz,
    create_time    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, id),
    FOREIGN KEY (org_id, data_plane_id) REFERENCES data_planes (org_id, id) ON DELETE CASCADE
);

-- Checks and discovers the API has sent to a data plane. Its runners read a
-- connector's config only for one of these.
CREATE TABLE connector_ops (
    org_id         uuid NOT NULL,
    id             uuid NOT NULL,
    data_plane_id  text NOT NULL,
    source_id      uuid,
    destination_id uuid,
    expire_time    timestamptz NOT NULL,
    PRIMARY KEY (org_id, id),
    FOREIGN KEY (org_id, source_id) REFERENCES sources (org_id, id) ON DELETE CASCADE,
    FOREIGN KEY (org_id, destination_id) REFERENCES destinations (org_id, id) ON DELETE CASCADE
);

-- +goose Down
DROP TABLE connector_ops;
DROP TABLE runner_tokens;
DROP TABLE role_bindings;
DROP TABLE api_keys;
DROP TABLE principals;

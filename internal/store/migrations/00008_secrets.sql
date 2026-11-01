-- +goose Up
-- Each org's data key, wrapped by the install's key-encryption key.
CREATE TABLE org_keys (
    org_id      uuid PRIMARY KEY REFERENCES orgs (id),
    wrapped_key bytea NOT NULL,
    create_time timestamptz NOT NULL DEFAULT now()
);

-- A secret's value, sealed with its org's data key and bound to its ID.
CREATE TABLE secrets (
    org_id       uuid NOT NULL REFERENCES org_keys (org_id),
    workspace_id uuid NOT NULL,
    id           uuid NOT NULL,
    name         text NOT NULL,
    ciphertext   bytea NOT NULL,
    revision     bigint NOT NULL,
    create_time  timestamptz NOT NULL DEFAULT now(),
    update_time  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, id),
    UNIQUE (org_id, workspace_id, name),
    FOREIGN KEY (org_id, workspace_id) REFERENCES workspaces (org_id, id)
);

-- The secrets each config names, in its workspace. A secret can't be deleted
-- while one names it.
ALTER TABLE sources ADD COLUMN secrets text[] NOT NULL DEFAULT '{}';
ALTER TABLE destinations ADD COLUMN secrets text[] NOT NULL DEFAULT '{}';

-- +goose Down
ALTER TABLE destinations DROP COLUMN secrets;
ALTER TABLE sources DROP COLUMN secrets;
DROP TABLE secrets;
DROP TABLE org_keys;

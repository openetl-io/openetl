-- +goose Up
-- Users sign in with their email, so it's unique among an org's users.
CREATE UNIQUE INDEX principals_user_email ON principals (org_id, lower(email)) WHERE type = 'user' AND email <> '';

-- Users' passwords, as argon2id PHC strings. failures counts sign-ins that
-- failed in a row; locked_until refuses them for a while after too many.
CREATE TABLE passwords (
    org_id       uuid NOT NULL,
    principal_id uuid NOT NULL,
    hash         text NOT NULL,
    failures     integer NOT NULL DEFAULT 0,
    locked_until timestamptz,
    update_time  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, principal_id),
    FOREIGN KEY (org_id, principal_id) REFERENCES principals (org_id, id) ON DELETE CASCADE
);

-- One-use links that set a user's password, by their token's SHA-256; a
-- user has at most one.
CREATE TABLE password_links (
    org_id       uuid NOT NULL,
    principal_id uuid NOT NULL,
    hash         bytea NOT NULL UNIQUE,
    expire_time  timestamptz NOT NULL,
    create_time  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, principal_id),
    FOREIGN KEY (org_id, principal_id) REFERENCES principals (org_id, id) ON DELETE CASCADE
);

-- Browsers' sign-ins, by their cookie's SHA-256. A session ends at
-- expire_time, or once it's idle for the server's idle timeout.
CREATE TABLE sessions (
    org_id         uuid NOT NULL,
    id             uuid NOT NULL,
    principal_id   uuid NOT NULL,
    hash           bytea NOT NULL UNIQUE,
    expire_time    timestamptz NOT NULL,
    last_used_time timestamptz NOT NULL DEFAULT now(),
    create_time    timestamptz NOT NULL DEFAULT now(),
    peer           text NOT NULL DEFAULT '',
    user_agent     text NOT NULL DEFAULT '',
    PRIMARY KEY (org_id, id),
    FOREIGN KEY (org_id, principal_id) REFERENCES principals (org_id, id) ON DELETE CASCADE
);

CREATE INDEX sessions_principal ON sessions (org_id, principal_id);

-- +goose Down
DROP TABLE sessions;
DROP TABLE password_links;
DROP TABLE passwords;
DROP INDEX principals_user_email;

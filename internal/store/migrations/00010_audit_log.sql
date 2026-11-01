-- +goose Up
-- One row per audited call, written before the call runs and completed once
-- when it ends. code is null until then, and stays null if the server stopped
-- first. Nothing refers to principals or resources, so rows outlive them.
CREATE TABLE audit_log (
    org_id     uuid NOT NULL,
    -- UUIDv7, so ids sort by time.
    id         uuid NOT NULL,
    time       timestamptz NOT NULL,
    -- principals/{p} or dataPlanes/{dp}, or openetld for its own changes.
    actor      text NOT NULL,
    -- principals/{p}/keys/{k} or dataPlanes/{dp}/tokens/{t}; empty for
    -- openetld.
    credential text NOT NULL,
    -- The RPC's procedure, or what openetld did.
    method     text NOT NULL,
    -- The resources the call named or touched, secrets it read included.
    resources  text[] NOT NULL,
    details    jsonb NOT NULL DEFAULT '{}',
    peer       text NOT NULL DEFAULT '',
    user_agent text NOT NULL DEFAULT '',
    -- A Connect code, such as ok or permission_denied.
    code       text,
    error      text NOT NULL DEFAULT '',
    end_time   timestamptz,
    PRIMARY KEY (org_id, id)
);

CREATE INDEX audit_log_resources ON audit_log USING gin (resources);
CREATE INDEX audit_log_actor ON audit_log (org_id, actor, id);

-- +goose StatementBegin
CREATE FUNCTION audit_log_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND OLD.code IS NULL AND NEW.code IS NOT NULL
        AND (NEW.org_id, NEW.id, NEW.time, NEW.actor, NEW.credential, NEW.method, NEW.peer, NEW.user_agent)
            = (OLD.org_id, OLD.id, OLD.time, OLD.actor, OLD.credential, OLD.method, OLD.peer, OLD.user_agent) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'audit_log is append-only: an entry is completed once, and never changes after';
END
$$;
-- +goose StatementEnd

CREATE TRIGGER audit_log_append_only BEFORE UPDATE OR DELETE ON audit_log
FOR EACH ROW EXECUTE FUNCTION audit_log_append_only();

-- +goose Down
DROP TABLE audit_log;
DROP FUNCTION audit_log_append_only;

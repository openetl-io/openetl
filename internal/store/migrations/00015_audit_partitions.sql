-- +goose Up
-- The audit log, partitioned by month of time (UTC) so retention drops whole
-- months: audit_log_{yyyymm}. The store creates a month's partition when an
-- entry first needs it, and Migrate this month's and the next ahead.
ALTER TABLE audit_log RENAME TO audit_log_unpartitioned;
ALTER INDEX audit_log_pkey RENAME TO audit_log_unpartitioned_pkey;
ALTER INDEX audit_log_resources RENAME TO audit_log_unpartitioned_resources;
ALTER INDEX audit_log_actor RENAME TO audit_log_unpartitioned_actor;

CREATE TABLE audit_log (
    org_id     uuid NOT NULL,
    id         uuid NOT NULL,
    time       timestamptz NOT NULL,
    actor      text NOT NULL,
    credential text NOT NULL,
    method     text NOT NULL,
    resources  text[] NOT NULL,
    details    jsonb NOT NULL DEFAULT '{}',
    peer       text NOT NULL DEFAULT '',
    user_agent text NOT NULL DEFAULT '',
    code       text,
    error      text NOT NULL DEFAULT '',
    end_time   timestamptz,
    PRIMARY KEY (org_id, id, time)
) PARTITION BY RANGE (time);

-- +goose StatementBegin
DO $$
DECLARE m timestamp;
BEGIN
    FOR m IN SELECT generate_series(
        date_trunc('month', (SELECT coalesce(min(time), now()) FROM audit_log_unpartitioned) AT TIME ZONE 'UTC'),
        date_trunc('month', now() AT TIME ZONE 'UTC') + interval '1 month',
        interval '1 month')
    LOOP
        EXECUTE format('CREATE TABLE %I PARTITION OF audit_log FOR VALUES FROM (%L) TO (%L)',
            'audit_log_' || to_char(m, 'YYYYMM'), m AT TIME ZONE 'UTC', (m + interval '1 month') AT TIME ZONE 'UTC');
    END LOOP;
END
$$;
-- +goose StatementEnd

INSERT INTO audit_log SELECT * FROM audit_log_unpartitioned;
DROP TABLE audit_log_unpartitioned;

CREATE INDEX audit_log_resources ON audit_log USING gin (resources);
CREATE INDEX audit_log_actor ON audit_log (org_id, actor, id);
CREATE TRIGGER audit_log_append_only BEFORE UPDATE OR DELETE ON audit_log
FOR EACH ROW EXECUTE FUNCTION audit_log_append_only();

-- +goose Down
ALTER TABLE audit_log RENAME TO audit_log_partitioned;
ALTER INDEX audit_log_pkey RENAME TO audit_log_partitioned_pkey;
ALTER INDEX audit_log_resources RENAME TO audit_log_partitioned_resources;
ALTER INDEX audit_log_actor RENAME TO audit_log_partitioned_actor;
CREATE TABLE audit_log (
    org_id     uuid NOT NULL,
    id         uuid NOT NULL,
    time       timestamptz NOT NULL,
    actor      text NOT NULL,
    credential text NOT NULL,
    method     text NOT NULL,
    resources  text[] NOT NULL,
    details    jsonb NOT NULL DEFAULT '{}',
    peer       text NOT NULL DEFAULT '',
    user_agent text NOT NULL DEFAULT '',
    code       text,
    error      text NOT NULL DEFAULT '',
    end_time   timestamptz,
    PRIMARY KEY (org_id, id)
);
INSERT INTO audit_log SELECT * FROM audit_log_partitioned;
DROP TABLE audit_log_partitioned;
CREATE INDEX audit_log_resources ON audit_log USING gin (resources);
CREATE INDEX audit_log_actor ON audit_log (org_id, actor, id);
CREATE TRIGGER audit_log_append_only BEFORE UPDATE OR DELETE ON audit_log
FOR EACH ROW EXECUTE FUNCTION audit_log_append_only();

-- +goose Up
-- What planning a version's transform.sql gave when the version was made:
-- an openetl.api.v1.SqlPlan as JSON. Null without transform.sql, or if it
-- couldn't be planned then.
ALTER TABLE pipeline_versions ADD COLUMN sql_plan jsonb;

-- +goose Down
ALTER TABLE pipeline_versions DROP COLUMN sql_plan;

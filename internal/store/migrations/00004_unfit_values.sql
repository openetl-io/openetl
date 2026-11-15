-- +goose Up
-- A stream's quarantine became unfit_values (ROW is QUARANTINE, CELL is
-- HOLD, with the same behaviour), and types.infinity's INFINITY_QUARANTINE
-- became INFINITY_UNFIT. Stored versions' specs are rewritten so they still
-- decode. jsonb's text form is canonical ("key": value), and a quote inside
-- a string is escaped, so these patterns match only the fields themselves.
UPDATE pipeline_versions SET spec = replace(replace(replace(spec::text,
    '"quarantine": "QUARANTINE_SCOPE_ROW"', '"unfitValues": "UNFIT_VALUES_QUARANTINE"'),
    '"quarantine": "QUARANTINE_SCOPE_CELL"', '"unfitValues": "UNFIT_VALUES_HOLD"'),
    '"infinity": "INFINITY_QUARANTINE"', '"infinity": "INFINITY_UNFIT"')::jsonb
WHERE spec::text LIKE '%QUARANTINE%';

-- +goose Down
UPDATE pipeline_versions SET spec = replace(replace(replace(spec::text,
    '"unfitValues": "UNFIT_VALUES_QUARANTINE"', '"quarantine": "QUARANTINE_SCOPE_ROW"'),
    '"unfitValues": "UNFIT_VALUES_HOLD"', '"quarantine": "QUARANTINE_SCOPE_CELL"'),
    '"infinity": "INFINITY_UNFIT"', '"infinity": "INFINITY_QUARANTINE"')::jsonb
WHERE spec::text LIKE '%UNFIT%';

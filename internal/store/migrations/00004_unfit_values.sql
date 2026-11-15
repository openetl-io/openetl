-- +goose Up
-- A stream's quarantine became unfit_values (ROW is QUARANTINE, CELL is
-- HOLD, with the same behaviour), and types.infinity's INFINITY_QUARANTINE
-- became INFINITY_UNFIT. Stored versions' specs are rewritten so they still
-- decode.
UPDATE pipeline_versions v
SET spec = jsonb_set(v.spec, '{source,streams}', (
    SELECT jsonb_agg(
        CASE WHEN s ? 'quarantine' THEN
            (s - 'quarantine') || jsonb_build_object('unfitValues',
                CASE s->>'quarantine'
                    WHEN 'QUARANTINE_SCOPE_ROW' THEN to_jsonb('UNFIT_VALUES_QUARANTINE'::text)
                    WHEN 'QUARANTINE_SCOPE_CELL' THEN to_jsonb('UNFIT_VALUES_HOLD'::text)
                    ELSE s->'quarantine'
                END)
        ELSE s END
        ORDER BY i)
    FROM jsonb_array_elements(v.spec->'source'->'streams') WITH ORDINALITY AS e(s, i)))
WHERE jsonb_typeof(v.spec->'source'->'streams') = 'array'
    AND EXISTS (SELECT 1 FROM jsonb_array_elements(v.spec->'source'->'streams') s WHERE s ? 'quarantine');

UPDATE pipeline_versions
SET spec = jsonb_set(spec, '{types,infinity}', to_jsonb('INFINITY_UNFIT'::text))
WHERE spec->'types'->>'infinity' = 'INFINITY_QUARANTINE';

-- +goose Down
UPDATE pipeline_versions v
SET spec = jsonb_set(v.spec, '{source,streams}', (
    SELECT jsonb_agg(
        CASE WHEN s ? 'unfitValues' THEN
            (s - 'unfitValues') || jsonb_build_object('quarantine',
                CASE s->>'unfitValues'
                    WHEN 'UNFIT_VALUES_QUARANTINE' THEN to_jsonb('QUARANTINE_SCOPE_ROW'::text)
                    WHEN 'UNFIT_VALUES_HOLD' THEN to_jsonb('QUARANTINE_SCOPE_CELL'::text)
                    ELSE s->'unfitValues'
                END)
        ELSE s END
        ORDER BY i)
    FROM jsonb_array_elements(v.spec->'source'->'streams') WITH ORDINALITY AS e(s, i)))
WHERE jsonb_typeof(v.spec->'source'->'streams') = 'array'
    AND EXISTS (SELECT 1 FROM jsonb_array_elements(v.spec->'source'->'streams') s WHERE s ? 'unfitValues');

UPDATE pipeline_versions
SET spec = jsonb_set(spec, '{types,infinity}', to_jsonb('INFINITY_QUARANTINE'::text))
WHERE spec->'types'->>'infinity' = 'INFINITY_UNFIT';

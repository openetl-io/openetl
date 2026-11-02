-- +goose Up
-- A secret holds a sealed value, or a reference to a cloud secret store that
-- runners read. Only sealed values need the org's data key.
ALTER TABLE secrets DROP CONSTRAINT secrets_org_id_fkey;
ALTER TABLE secrets ALTER COLUMN ciphertext DROP NOT NULL;
ALTER TABLE secrets ADD COLUMN ref text;
ALTER TABLE secrets ADD CONSTRAINT secrets_value_or_ref CHECK ((ciphertext IS NULL) <> (ref IS NULL));

-- +goose Down
DELETE FROM secrets WHERE ref IS NOT NULL;
ALTER TABLE secrets DROP CONSTRAINT secrets_value_or_ref;
ALTER TABLE secrets DROP COLUMN ref;
ALTER TABLE secrets ALTER COLUMN ciphertext SET NOT NULL;
ALTER TABLE secrets ADD CONSTRAINT secrets_org_id_fkey FOREIGN KEY (org_id) REFERENCES org_keys (org_id);

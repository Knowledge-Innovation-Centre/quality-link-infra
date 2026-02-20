-- SCHAC identifier of provider
ALTER TABLE provider ADD COLUMN IF NOT EXISTS schac_code VARCHAR;


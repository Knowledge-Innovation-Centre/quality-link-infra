-- auth data for data source
ALTER TABLE source ADD COLUMN IF NOT EXISTS source_auth JSONB;


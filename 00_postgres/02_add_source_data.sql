-- additional data on sources

-- identifier chosen by provider
ALTER TABLE source ADD COLUMN IF NOT EXISTS source_id VARCHAR;
-- refresh interval (minimum hours)
ALTER TABLE source ADD COLUMN IF NOT EXISTS source_refresh INTEGER;
-- header fields to send with requests
ALTER TABLE source ADD COLUMN IF NOT EXISTS source_headers JSONB;
-- query parameters
ALTER TABLE source ADD COLUMN IF NOT EXISTS source_parameters JSONB;
-- any other (source type-specific) parameters
ALTER TABLE source ADD COLUMN IF NOT EXISTS source_other JSONB;


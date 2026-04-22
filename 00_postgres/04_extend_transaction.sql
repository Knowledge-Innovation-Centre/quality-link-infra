-- Extend transaction table into a per-run ledger
-- (lifecycle, bronze/log paths, course count, error message).

-- Drop the old (provider_uuid, source_version_uuid, created_at_date) uniqueness
ALTER TABLE transaction DROP CONSTRAINT transaction_provider_uuid_source_version_uuid_created_at_da_key;

ALTER TABLE transaction ADD COLUMN IF NOT EXISTS source_uuid UUID REFERENCES source(source_uuid);
ALTER TABLE transaction ADD COLUMN IF NOT EXISTS run_number INTEGER NOT NULL DEFAULT 1;
ALTER TABLE transaction ADD COLUMN IF NOT EXISTS status VARCHAR(20) NOT NULL DEFAULT 'success';
ALTER TABLE transaction ADD COLUMN IF NOT EXISTS started_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE transaction ADD COLUMN IF NOT EXISTS finished_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE transaction ADD COLUMN IF NOT EXISTS bronze_file_path VARCHAR;
ALTER TABLE transaction ADD COLUMN IF NOT EXISTS log_file_path VARCHAR;
ALTER TABLE transaction ADD COLUMN IF NOT EXISTS course_count INTEGER;
ALTER TABLE transaction ADD COLUMN IF NOT EXISTS error_message TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS uq_transaction_run
    ON transaction (provider_uuid, source_version_uuid, source_uuid, created_at_date, run_number);
CREATE INDEX IF NOT EXISTS idx_transaction_source_uuid ON transaction(source_uuid);
CREATE INDEX IF NOT EXISTS idx_transaction_provider_date
    ON transaction(provider_uuid, source_version_uuid, source_uuid, created_at_date DESC);

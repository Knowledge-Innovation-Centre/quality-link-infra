CREATE DATABASE mage;
CREATE DATABASE backend;

GRANT ALL PRIVILEGES ON DATABASE mage TO quality_link;
GRANT ALL PRIVILEGES ON DATABASE backend TO quality_link;

\c backend;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS provider (
    provider_uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    deqar_id VARCHAR,
    eter_id VARCHAR,
    base_id INTEGER,
    metadata JSONB,
    manifest_json JSONB,
    name_concat VARCHAR,
    provider_name VARCHAR,
    last_deqar_pull TIMESTAMP WITH TIME ZONE,
    last_manifest_pull TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS source_version (
    source_version_uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    provider_uuid UUID NOT NULL,
    version_date DATE NOT NULL,
    version_id INTEGER NOT NULL,
    source_json JSONB NOT NULL,
    source_uuid_json JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    FOREIGN KEY (provider_uuid) REFERENCES provider(provider_uuid) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS source (
    source_uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_version_uuid UUID NOT NULL,
    source_name VARCHAR,
    source_path VARCHAR,
    source_type VARCHAR,
    source_version VARCHAR,
    last_date_pull DATE,
    last_file_pushed VARCHAR,
    last_file_pushed_date TIMESTAMP WITH TIME ZONE,
    last_file_pushed_path VARCHAR, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    FOREIGN KEY (source_version_uuid) REFERENCES source_version(source_version_uuid) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_provider_deqar_id ON provider(deqar_id);
CREATE INDEX IF NOT EXISTS idx_provider_name_concat ON provider(name_concat);
CREATE INDEX IF NOT EXISTS idx_source_version_provider_uuid ON source_version(provider_uuid);
CREATE INDEX IF NOT EXISTS idx_source_source_version_uuid ON source(source_version_uuid);
CREATE INDEX IF NOT EXISTS idx_source_source_type ON source(source_type);
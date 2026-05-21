-- Per-provider OAuth 2.0 client credentials, used when a manifest source
-- declares auth.type = 'oauth2.0' without inline client_id/client_secret.
-- Looked up by (provider_uuid, token_endpoint).
CREATE TABLE IF NOT EXISTS provider_oauth_cred (
    cred_uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    provider_uuid UUID NOT NULL,
    token_endpoint VARCHAR NOT NULL,
    client_id VARCHAR NOT NULL,
    client_secret VARCHAR NOT NULL,
    scope VARCHAR,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    FOREIGN KEY (provider_uuid) REFERENCES provider(provider_uuid) ON DELETE CASCADE,
    UNIQUE (provider_uuid, token_endpoint)
);

CREATE INDEX IF NOT EXISTS idx_provider_oauth_cred_provider ON provider_oauth_cred(provider_uuid);

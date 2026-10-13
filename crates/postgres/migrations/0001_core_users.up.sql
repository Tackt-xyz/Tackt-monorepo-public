-- Core owns user lifecycle, source-account associations, and external identities.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TYPE indexer_source AS ENUM ('ethereum', 'hyperliquid');

CREATE TYPE user_lifecycle_status AS ENUM ('pending', 'active', 'restricted', 'closed');

CREATE TABLE users (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id text NOT NULL UNIQUE,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    status user_lifecycle_status NOT NULL DEFAULT 'pending',
    metadata jsonb NOT NULL DEFAULT '{}',
    CONSTRAINT users_external_id_nonempty CHECK (length(btrim(external_id)) BETWEEN 1 AND 255),
    CONSTRAINT users_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE user_accounts (
    user_id uuid NOT NULL REFERENCES users(id),
    source indexer_source NOT NULL,
    account text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, account),
    UNIQUE (user_id, source, account),
    CHECK (account = lower(account))
);

CREATE UNIQUE INDEX user_accounts_one_per_source ON user_accounts(user_id, source);

CREATE TABLE user_identities (
    issuer text COLLATE "C" NOT NULL,
    subject text COLLATE "C" NOT NULL,
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (issuer, subject),
    CHECK (octet_length(issuer) BETWEEN 1 AND 2048),
    CHECK (octet_length(subject) BETWEEN 1 AND 255)
);

CREATE INDEX user_identities_user_idx ON user_identities(user_id);

CREATE FUNCTION reject_user_identity_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'user identity mappings are append-only' USING ERRCODE='55000';
END $$;
CREATE TRIGGER user_identities_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON user_identities
FOR EACH STATEMENT EXECUTE FUNCTION reject_user_identity_mutation();
REVOKE UPDATE, DELETE, TRUNCATE ON user_identities FROM PUBLIC;

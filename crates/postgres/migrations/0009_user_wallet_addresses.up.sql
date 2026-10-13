-- Nullable only to permit Core to derive addresses for existing demo keys at startup.
ALTER TABLE user_signing_keys
    ADD COLUMN ethereum_address text,
    ADD COLUMN solana_address text,
    ADD CONSTRAINT user_wallet_addresses_complete CHECK (
        (ethereum_address IS NULL AND solana_address IS NULL) OR
        (ethereum_address IS NOT NULL AND solana_address IS NOT NULL)
    ),
    ADD CONSTRAINT user_ethereum_address_format CHECK (ethereum_address ~ '^0x[0-9a-f]{40}$'),
    ADD CONSTRAINT user_solana_address_format CHECK (solana_address ~ '^[1-9A-HJ-NP-Za-km-z]{32,44}$');

CREATE UNIQUE INDEX user_signing_keys_ethereum_address ON user_signing_keys (ethereum_address);
CREATE UNIQUE INDEX user_signing_keys_solana_address ON user_signing_keys (solana_address);

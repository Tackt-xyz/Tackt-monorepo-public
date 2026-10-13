ALTER TABLE user_signing_keys
    DROP CONSTRAINT user_wallet_addresses_complete,
    DROP COLUMN ethereum_address,
    DROP COLUMN solana_address;

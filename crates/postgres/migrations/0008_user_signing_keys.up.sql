-- Demo custody: raw, unencrypted signing material is deliberately kept separate
-- from user profiles and login identities. Solana stores the 32-byte Ed25519 seed.
CREATE TABLE user_signing_keys (
    user_id uuid PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    ethereum_private_key bytea NOT NULL CHECK (octet_length(ethereum_private_key) = 32),
    solana_private_key bytea NOT NULL CHECK (octet_length(solana_private_key) = 32),
    created_at timestamptz NOT NULL DEFAULT now()
);

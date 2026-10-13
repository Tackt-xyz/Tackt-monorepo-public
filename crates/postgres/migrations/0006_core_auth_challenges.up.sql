CREATE TABLE core_auth_used_challenges (
    id uuid PRIMARY KEY,
    expires_at timestamptz NOT NULL
);
CREATE INDEX core_auth_used_challenges_expiry ON core_auth_used_challenges(expires_at);

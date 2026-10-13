CREATE TABLE core_auth_sessions (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES users(id),
    refresh_hash bytea NOT NULL,
    expires_at timestamptz NOT NULL
);
CREATE INDEX core_auth_sessions_expiry ON core_auth_sessions(expires_at);
CREATE INDEX core_auth_sessions_user ON core_auth_sessions(user_id);

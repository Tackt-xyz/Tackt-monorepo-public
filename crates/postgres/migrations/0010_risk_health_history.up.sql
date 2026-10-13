-- Derived display history only; never an accounting or authorization input.
CREATE TABLE risk_health_samples (
    sample_id uuid PRIMARY KEY,
    user_id uuid NOT NULL,
    sampled_at timestamptz NOT NULL,
    health numeric(39,6),
    classification text NOT NULL
);
CREATE INDEX risk_health_samples_user_time
    ON risk_health_samples (user_id, sampled_at DESC, sample_id DESC);

-- Preproduction contract cleanup. SQLx migration ordering/checksums remain authoritative.
ALTER TABLE observations DROP COLUMN asset_registry_version;
COMMENT ON COLUMN observations.normalized_asset IS 'Validated accounting asset code';

ALTER TABLE core_actions
    DROP COLUMN asset_registry_version,
    DROP COLUMN policy_version,
    DROP COLUMN risk_asset_registry_version,
    DROP COLUMN risk_registry_version,
    DROP COLUMN risk_registry_digest;

ALTER TABLE core_actions
    ADD CONSTRAINT core_actions_risk_versions_valid CHECK (
        portfolio_version > 0 AND risk_processing_epoch >= 0),
    ADD CONSTRAINT core_actions_recommendation_evidence_complete CHECK (
        (risk_recommendation_id IS NULL AND portfolio_version IS NULL)
        OR (risk_recommendation_id IS NOT NULL AND portfolio_version IS NOT NULL
            AND risk_processing_epoch IS NOT NULL)),
    ADD CONSTRAINT core_actions_evaluation_evidence_complete CHECK (
        (risk_evaluation_id IS NULL AND risk_input_digest IS NULL
            AND risk_account_version IS NULL AND risk_input_version IS NULL
            AND risk_evaluation IS NULL)
        OR (risk_evaluation_id IS NOT NULL AND risk_input_digest IS NOT NULL
            AND risk_account_version IS NOT NULL AND risk_input_version IS NOT NULL
            AND risk_processing_epoch IS NOT NULL AND risk_evaluation IS NOT NULL)),
    ADD CONSTRAINT core_actions_evaluation_versions_valid CHECK (
        risk_account_version > 0 AND risk_input_version > 0);

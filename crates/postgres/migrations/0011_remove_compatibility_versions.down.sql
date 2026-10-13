-- Restore historical storage only for an empty preproduction database. Removed metadata
-- cannot be reconstructed for records created under the current application contract.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM observations) OR EXISTS (SELECT 1 FROM core_actions) THEN
        RAISE EXCEPTION 'cannot restore compatibility metadata for existing observations or actions';
    END IF;
END
$$;

ALTER TABLE observations ADD COLUMN asset_registry_version integer;
COMMENT ON COLUMN observations.normalized_asset IS
    'Validated Release 1A accounting asset code; requires asset_registry_version';

ALTER TABLE core_actions
    DROP CONSTRAINT core_actions_risk_versions_valid,
    DROP CONSTRAINT core_actions_recommendation_evidence_complete,
    DROP CONSTRAINT core_actions_evaluation_evidence_complete,
    DROP CONSTRAINT core_actions_evaluation_versions_valid,
    ADD COLUMN asset_registry_version integer NOT NULL
        CONSTRAINT core_actions_asset_registry_version_positive CHECK (asset_registry_version > 0),
    ADD COLUMN policy_version numeric(20,0),
    ADD COLUMN risk_asset_registry_version integer,
    ADD COLUMN risk_registry_version numeric(20,0),
    ADD COLUMN risk_registry_digest text,
    ADD CONSTRAINT core_actions_risk_versions_valid CHECK (
        policy_version > 0 AND portfolio_version > 0
        AND risk_processing_epoch >= 0 AND risk_asset_registry_version > 0),
    ADD CONSTRAINT core_actions_recommendation_evidence_complete CHECK (
        (risk_recommendation_id IS NULL AND portfolio_version IS NULL)
        OR (risk_recommendation_id IS NOT NULL AND portfolio_version IS NOT NULL
            AND policy_version IS NOT NULL AND risk_processing_epoch IS NOT NULL
            AND risk_asset_registry_version IS NOT NULL)
    ),
    ADD CONSTRAINT core_actions_evaluation_evidence_complete CHECK (
        (risk_evaluation_id IS NULL AND risk_input_digest IS NULL
            AND risk_account_version IS NULL AND risk_input_version IS NULL
            AND risk_registry_version IS NULL AND risk_registry_digest IS NULL
            AND risk_evaluation IS NULL)
        OR (risk_evaluation_id IS NOT NULL AND risk_input_digest IS NOT NULL
            AND policy_version IS NOT NULL AND risk_account_version IS NOT NULL
            AND risk_input_version IS NOT NULL AND risk_processing_epoch IS NOT NULL
            AND risk_asset_registry_version IS NOT NULL AND risk_registry_version IS NOT NULL
            AND risk_registry_digest IS NOT NULL AND risk_evaluation IS NOT NULL)
    ),
    ADD CONSTRAINT core_actions_risk_registry_digest_valid
        CHECK (risk_registry_digest ~ '^[0-9a-f]{64}$'),
    ADD CONSTRAINT core_actions_evaluation_versions_valid CHECK (
        risk_account_version > 0
        AND risk_input_version > 0
        AND risk_registry_version > 0
    );

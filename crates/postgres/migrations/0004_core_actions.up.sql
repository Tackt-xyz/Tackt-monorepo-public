-- Core action workflows, immutable audit history, and recommendation/evaluation evidence.
SET LOCAL statement_timeout = 0;
SET LOCAL lock_timeout = '5s';

CREATE TYPE core_action_kind AS ENUM (
    'custody_outflow',
    'hyperliquid_margin',
    'venue_order'
);

CREATE TYPE core_action_state AS ENUM (
    'pending',
    'waiting_for_funding',
    'waiting_for_signing',
    'externally_pending',
    'unknown_outcome',
    'failed',
    'succeeded'
);

-- The single SQL definition of "active". Both partial indexes below and the recovery scan in
-- `active_core_actions` call it, so the predicate cannot drift between them. IMMUTABLE is required
-- for a partial-index predicate; keeping the body one plain SQL expression lets the planner inline
-- it, which is what allows a query saying `core_action_state_is_active(state)` to match the
-- partial index built on the same call.
CREATE FUNCTION core_action_state_is_active(state core_action_state) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT state NOT IN ('failed', 'succeeded')
$$;

CREATE TABLE core_actions (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    kind core_action_kind NOT NULL,
    asset text NOT NULL CONSTRAINT core_actions_asset_present CHECK (asset <> ''),
    asset_registry_version integer NOT NULL
        CONSTRAINT core_actions_asset_registry_version_positive CHECK (asset_registry_version > 0),
    quantity numeric(39,0) NOT NULL
        CONSTRAINT core_actions_quantity_positive_integer CHECK (quantity > 0 AND scale(quantity) = 0),
    state core_action_state NOT NULL DEFAULT 'pending',
    risk_recommendation_id text,
    policy_version numeric(20,0),
    portfolio_version numeric(20,0),
    risk_processing_epoch numeric(20,0),
    risk_asset_registry_version integer,
    external_references jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    risk_evaluation_id text,
    risk_input_digest text,
    risk_account_version numeric(20,0),
    risk_input_version numeric(20,0),
    risk_registry_version numeric(20,0),
    risk_registry_digest text,
    risk_evaluation jsonb,
    CONSTRAINT core_actions_external_references_object
        CHECK (jsonb_typeof(external_references) = 'object'),
    CONSTRAINT core_actions_risk_recommendation_id_valid
        CHECK (length(btrim(risk_recommendation_id)) BETWEEN 1 AND 255),
    CONSTRAINT core_actions_risk_versions_valid CHECK (
        policy_version > 0 AND portfolio_version > 0
        AND risk_processing_epoch >= 0 AND risk_asset_registry_version > 0),
    CONSTRAINT core_actions_recommendation_evidence_complete CHECK (
        (risk_recommendation_id IS NULL AND portfolio_version IS NULL)
        OR (risk_recommendation_id IS NOT NULL AND portfolio_version IS NOT NULL
            AND policy_version IS NOT NULL AND risk_processing_epoch IS NOT NULL
            AND risk_asset_registry_version IS NOT NULL)
    ),
    CONSTRAINT core_actions_evaluation_evidence_complete CHECK (
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
    CONSTRAINT core_actions_one_risk_evidence_kind CHECK (
        risk_recommendation_id IS NULL OR risk_evaluation_id IS NULL
    ),
    CONSTRAINT core_actions_risk_evaluation_id_valid
        CHECK (length(btrim(risk_evaluation_id)) BETWEEN 1 AND 255),
    CONSTRAINT core_actions_risk_input_digest_valid
        CHECK (risk_input_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT core_actions_risk_registry_digest_valid
        CHECK (risk_registry_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT core_actions_risk_evaluation_object
        CHECK (jsonb_typeof(risk_evaluation) = 'object'),
    CONSTRAINT core_actions_evaluation_versions_valid CHECK (
        risk_account_version > 0
        AND risk_input_version > 0
        AND risk_registry_version > 0
    )
);

COMMENT ON COLUMN core_actions.risk_evaluation_id IS
    'Deterministic identifier returned by the synchronous Risk evaluation used by Core.';

-- Waiting and ambiguous actions keep the slot. Only a terminal state releases it.
CREATE UNIQUE INDEX core_actions_one_active_custody_exit_per_user
    ON core_actions(user_id)
    WHERE kind IN ('custody_outflow', 'hyperliquid_margin')
      AND core_action_state_is_active(state);
CREATE INDEX core_actions_active_updated_idx ON core_actions(updated_at, id)
    WHERE core_action_state_is_active(state);

CREATE TABLE core_action_transitions (
    transition_id uuid PRIMARY KEY,
    action_id uuid NOT NULL REFERENCES core_actions(id) ON DELETE RESTRICT,
    from_state core_action_state,
    to_state core_action_state NOT NULL,
    reason text NOT NULL CONSTRAINT core_action_transitions_reason_valid
        CHECK (octet_length(btrim(reason)) BETWEEN 1 AND 1024),
    evidence jsonb NOT NULL DEFAULT '{}',
    occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT core_action_transitions_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
    -- A NULL `from_state` marks the genesis row. It must branch explicitly: in a flat OR chain
    -- every `from_state = '...'` comparison against NULL yields NULL rather than false, the whole
    -- expression collapses to NULL, and a CHECK constraint accepts NULL — which would let any
    -- (NULL -> any state) row into the append-only log.
    CONSTRAINT core_action_transitions_state_transition_valid CHECK (
        CASE WHEN from_state IS NULL THEN to_state = 'pending'
        ELSE (from_state = 'pending' AND to_state IN (
                'waiting_for_funding', 'waiting_for_signing', 'externally_pending',
                'unknown_outcome', 'failed'
            ))
            OR (from_state = 'waiting_for_funding' AND to_state IN (
                'waiting_for_signing', 'externally_pending', 'unknown_outcome', 'failed'
            ))
            OR (from_state = 'waiting_for_signing' AND to_state IN (
                'externally_pending', 'unknown_outcome', 'failed'
            ))
            OR (from_state = 'externally_pending' AND to_state IN (
                'unknown_outcome', 'failed', 'succeeded'
            ))
            OR (from_state = 'unknown_outcome' AND to_state IN (
                'externally_pending', 'failed', 'succeeded'
            ))
        END
    )
);
CREATE INDEX core_action_transitions_action_time_idx
    ON core_action_transitions(action_id, occurred_at, transition_id);

CREATE TYPE core_action_ledger_step AS ENUM (
    'reserve_custody_exit',
    'draw_credit_to_custody_exit',
    'consume_custody_exit',
    'release_custody_exit',
    'reserve_venue_order'
);

CREATE TABLE core_action_ledger_requests (
    request_id uuid PRIMARY KEY,
    action_id uuid NOT NULL REFERENCES core_actions(id) ON DELETE RESTRICT,
    step core_action_ledger_step NOT NULL,
    idempotency_key text NOT NULL UNIQUE
        CONSTRAINT core_action_ledger_requests_idempotency_key_valid
        CHECK (octet_length(btrim(idempotency_key)) BETWEEN 1 AND 256),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (action_id, step)
);
CREATE INDEX core_action_ledger_requests_action_idx
    ON core_action_ledger_requests(action_id, created_at, request_id);

CREATE FUNCTION reject_core_action_audit_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'Core action audit records are append-only' USING ERRCODE='55000';
END $$;

CREATE TRIGGER core_action_transitions_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON core_action_transitions
FOR EACH STATEMENT EXECUTE FUNCTION reject_core_action_audit_mutation();

CREATE TRIGGER core_action_ledger_requests_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON core_action_ledger_requests
FOR EACH STATEMENT EXECUTE FUNCTION reject_core_action_audit_mutation();

REVOKE UPDATE, DELETE, TRUNCATE ON core_action_transitions FROM PUBLIC;
REVOKE UPDATE, DELETE, TRUNCATE ON core_action_ledger_requests FROM PUBLIC;

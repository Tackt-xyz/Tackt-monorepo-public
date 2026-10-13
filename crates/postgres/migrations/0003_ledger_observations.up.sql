-- Ledger persists trusted Indexer facts, deposit decisions, and durable delivery state.
CREATE TABLE observations (
    event_id text PRIMARY KEY,
    source text NOT NULL,
    kind text NOT NULL,
    user_id uuid,
    account text,
    asset text,
    quantity text,
    source_timestamp timestamptz NOT NULL,
    observed_at timestamptz NOT NULL,
    block_number bigint,
    block_hash text,
    confirmations bigint,
    canonical boolean NOT NULL DEFAULT true,
    finalized boolean NOT NULL DEFAULT false,
    raw jsonb NOT NULL,
    provenance jsonb NOT NULL,
    inserted_at timestamptz NOT NULL DEFAULT now(),
    normalized_asset text,
    asset_registry_version integer,
    chain_id numeric(20,0)
);
CREATE INDEX observations_user_time_idx ON observations(user_id, source_timestamp DESC);
CREATE INDEX observations_source_kind_time_idx ON observations(source, kind, source_timestamp DESC);
CREATE INDEX observations_block_idx ON observations(source, block_number) WHERE block_number IS NOT NULL;

-- Raw observations are append-only facts. Canonical/finality flags may change during reorg resolution.

COMMENT ON COLUMN observations.asset IS
    'Untrusted source-native asset evidence; never sufficient for accounting or risk support';
COMMENT ON COLUMN observations.normalized_asset IS
    'Validated Release 1A accounting asset code; requires asset_registry_version';

CREATE INDEX observations_ethereum_block_canonical_height_idx
    ON observations(block_number DESC) INCLUDE(block_hash)
    WHERE source = 'ethereum'
      AND kind = 'ethereum_block'
      AND canonical
      AND block_number IS NOT NULL
      AND block_hash IS NOT NULL;

CREATE INDEX observations_chain_height ON observations(chain_id,block_number)
    WHERE canonical;
CREATE INDEX observations_unfinalized_height ON observations(chain_id,block_number)
    WHERE canonical AND NOT finalized;

CREATE TABLE ledger.observation_cursor (
    stream text PRIMARY KEY,
    sequence numeric(20,0) NOT NULL,
    -- Preserve JetStream creation identity without timestamp precision loss.
    stream_created_at text
);
CREATE TABLE ledger.deposits (
    event_id text PRIMARY KEY,
    chain_id numeric(20,0) NOT NULL,
    block_number bigint NOT NULL,
    user_id uuid NOT NULL,
    asset text NOT NULL,
    quantity numeric(39,0) NOT NULL,
    effective_at timestamptz NOT NULL,
    status text NOT NULL,
    detail text
);
CREATE INDEX deposits_status_height ON ledger.deposits(chain_id,status,block_number,event_id);

-- Chain facts and committed delivery position; accounting rules remain in Ledger.
CREATE TABLE ledger.chain_state (
    chain_id numeric(20,0) PRIMARY KEY,
    block_number bigint NOT NULL,
    block_hash text NOT NULL,
    finalized_number bigint NOT NULL,
    finalized_hash text NOT NULL,
    sequence numeric(20,0) NOT NULL
);

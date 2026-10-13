-- Ledger owns accounting validation. PostgreSQL stores its decisions and
-- uses the migrating database user as owner; keys and indexes provide storage identity and lookup.
CREATE SCHEMA ledger;

CREATE TABLE ledger.accounts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    code text NOT NULL,
    user_id uuid,
    bucket text NOT NULL,
    asset text NOT NULL,
    allow_negative boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX accounts_code_lookup ON ledger.accounts(code);
CREATE INDEX accounts_owner_lookup ON ledger.accounts(user_id,bucket,asset);

CREATE TABLE ledger.transactions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    command_source text NOT NULL,
    chain_id numeric(20,0) NOT NULL,
    command_kind text NOT NULL,
    command_hash text NOT NULL,
    idempotency_key text NOT NULL,
    external_reference text,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    effective_at timestamptz NOT NULL,
    reverses_transaction_id uuid,
    audit jsonb NOT NULL
);
CREATE INDEX transactions_recorded_id_idx ON ledger.transactions(recorded_at,id);
CREATE INDEX transactions_idempotency_lookup ON ledger.transactions(idempotency_key);
CREATE INDEX transactions_external_lookup ON ledger.transactions(command_source,chain_id,external_reference);
CREATE INDEX transactions_reversal_lookup ON ledger.transactions(reverses_transaction_id);

CREATE TABLE ledger.postings (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id uuid NOT NULL,
    account_id uuid NOT NULL,
    asset text NOT NULL,
    side text NOT NULL,
    quantity numeric(39,0) NOT NULL
);
CREATE INDEX postings_account_transaction_idx ON ledger.postings(account_id,transaction_id);
CREATE INDEX postings_transaction_lookup ON ledger.postings(transaction_id,account_id,side);

CREATE TABLE ledger.current_balances (
    account_id uuid PRIMARY KEY,
    debit_quantity numeric(39,0) NOT NULL DEFAULT 0,
    credit_quantity numeric(39,0) NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL
);

CREATE TABLE ledger.reservations (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL,
    kind text NOT NULL,
    destination text,
    asset text NOT NULL,
    original_quantity numeric(39,0) NOT NULL,
    remaining_quantity numeric(39,0) NOT NULL,
    status text NOT NULL DEFAULT 'open',
    created_transaction_id uuid NOT NULL,
    updated_at timestamptz NOT NULL
);

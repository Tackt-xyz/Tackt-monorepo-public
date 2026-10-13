# TODO

- Migrate ledger deposit settlement off the PostgreSQL `observations` table: the Indexer no
  longer writes observations to PostgreSQL (they flow over the durable `TACKT_OBSERVATIONS`
  JetStream stream), so settlement books no new deposits until it consumes the stream or a
  durable consumer projects observations back into PostgreSQL. The observation repository
  surface in `tackt-postgres` remains only for the ledger's current queries and tests.

- Move price-aggregation policy (per-asset sources, weights, freshness windows, quorum,
  deviation) plus each source's quote currency into the locked asset registry.
  Until then USD/USDC/USDT-quoted markets are aggregated at par, with depegs handled by the
  deviation guard's fail-closed majority rule.
- Migrate the Ethereum `newHeads` WebSocket supervision in `source/evm` onto the shared
  `price_feed::ws` shell so the service has one socket lifecycle implementation.
- Consider spawning each price venue's stream as its own supervised task once venue count or
  message rate grows; today both venues and the aggregate reporter interleave on one task.

- Batch ledger commit locks, account resolution, posting inserts, and balance projection updates to
  reduce per-command PostgreSQL round trips while preserving deterministic locking and atomicity.
- Replace the internal JSON reservation instructions with a typed Rust enum.
- Add cursor pagination or streaming for ledger exports larger than the current 10,000-transaction
  safety limit.
- Make Ethereum reorg invalidation and replacement insertion one typed Postgres transaction, and
  reactivate an existing orphaned hash if the canonical chain oscillates back to it.
- Wire `tackt-nats` into a consumer or remove it; if kept for financial events, upgrade the
  transport from core NATS at-most-once delivery to JetStream with acknowledged publishes.
- Replace the single shared operator bearer token with per-operator credentials and derive the
  audit actor from the credential instead of the client-supplied `x-operator-id` header.
- Promote the indexer's remaining Ethereum catch-up tuning constants into `Config`, and make
  `ETHEREUM_RPC_URL` required instead of defaulting to a public endpoint.
- Export the metrics the risk engine already records (`metrics` recorder plus `/metrics`
  endpoints), and integrate `risk-engine` into the workspace conventions (edition 2024,
  workspace package inheritance).

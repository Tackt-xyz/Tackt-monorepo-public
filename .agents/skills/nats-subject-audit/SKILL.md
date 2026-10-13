---
name: nats-subject-audit
description: Audit Tackt NATS subjects, streams, publishers, consumers, schemas, acknowledgement policy, retries, and service usage for rogue, duplicate, orphaned, or incompatible flows. Use for messaging incidents, contract changes, or delivery-guarantee reviews.
---

# NATS subject audit

Treat `crates/nats` as the complete authority for Tackt NATS behavior and wire contracts.

## Static audit

1. Inventory subjects, stream names, durable consumers, message envelopes, publishers, and consumer helpers in `crates/nats`.
2. Search the entire workspace for direct `async-nats` use, literal subjects or stream names, locally defined wire contracts, and service-side retry or acknowledgement logic.
3. Map every publisher and consumer to its contract and delivery class:
   - Core lifecycle and indexer observation messages are acceleration channels; consumers reconcile from PostgreSQL.
   - Risk streams require durable JetStream delivery, explicit acknowledgement, retry, and idempotency. The Ledger has no NATS ingress; a subject or stream proposing one is a finding.
4. Flag publisher or consumer payloads that disagree with their typed contracts in `crates/nats`.
5. Flag subject or stream literals defined outside their owner in `crates/nats`.
6. Flag duplicate subscribers, durable flows weakened to core NATS, and acceleration events treated as authoritative.
7. Run `./scripts/check-boundaries.sh` after any fix and extend it when a new invariant can be checked mechanically.

## Runtime audit

Inspect a live NATS account only when the user places that environment in scope and credentials/tools are available. Start read-only: list streams and consumers, compare filter subjects, durable names, delivery and ack policies, pending/redelivered counts, and inactive consumers against the static inventory. Do not delete consumers, purge streams, change retention, or replay messages without explicit authorization.

Report evidence separately as repository findings, live-state findings, and unverified hypotheses.


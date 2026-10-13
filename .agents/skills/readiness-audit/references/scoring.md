# Readiness scoring

Score five dimensions from 0–20:

1. Architecture and authority boundaries
2. Functional and domain correctness
3. Verified test evidence
4. Security and data integrity
5. Operational durability and observability

## Caps

- Any known path to incorrect ledger postings, lost authoritative data, credential exposure, or unauthorized mutation: total score at most 49.
- Missing durable delivery, acknowledgement, retry, or idempotency on an authoritative ledger/risk flow: at most 59.
- An unapplied or unsafe required migration, broken startup initialization, or failing required verification: at most 69.
- Required tests not executed in the current environment: verified-test dimension at most 10/20.
- Material behavior documented but neither implemented nor tested: affected dimension at most 10/20.

Confidence is high only when code, executed tests, and operational evidence agree; medium when one evidence class is unavailable; low when conclusions rely mainly on documentation or inference.


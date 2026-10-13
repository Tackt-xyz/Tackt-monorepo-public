# Independent audit prompts

Use only when delegation is authorized. Give each auditor the exact target, governing instructions, base revision, and permission boundaries. Ask for file-and-line evidence and a 0–20 dimension score.

## Architecture auditor

Check crate ownership, dependency direction, typed API boundaries, initialization ownership, and duplicated asset, SQL, NATS, or telemetry behavior.

## Correctness auditor

Trace success and failure paths. Focus on transactions, immutable accounting, reservations, deterministic risk arithmetic, validation, idempotency, concurrency, and compatibility.

## Test auditor

Map requirements and failure modes to tests, execute the relevant commands, and distinguish observed passes from unexecuted coverage. Identify tests that assert the wrong behavior or omit restart/retry cases.

## Durability and security auditor

Challenge crash windows, reconnects, redelivery, reconciliation, degraded dependencies, resource limits, TLS, authentication, secrets, audit attribution, backup/restore safety, and operational observability.

Auditors report findings, evidence, executed commands, unknowns, and remediation ideas. The lead independently verifies every load-bearing claim before scoring.

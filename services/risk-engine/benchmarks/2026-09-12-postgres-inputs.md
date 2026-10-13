# PostgreSQL credit input loading — 2026-09-12

This change adds host IO only. The deterministic credit evaluator and the existing processing
algorithms are unchanged, so their previous microbenchmarks do not measure this new work.

Each refresh performs one read-only repeatable-read transaction: read the database timestamp,
read all user-owned current Ledger balance projections, then commit. Results are grouped by
user and installed as a complete replacement. One worker reads every five seconds by default;
reads do not overlap or acquire Ledger's writer lock. During replacement, the old and incoming
snapshots coexist in memory. This first version uses a full read, with memory proportional to
all user-owned balance rows; it does not add paging, change cursors, or incremental updates.

Local database latency and production-scale throughput are unmeasured because Docker socket
access is unavailable. Compose-backed integration tests cover committed visibility during a
concurrent Ledger write, read-only runtime access, periodic refresh, and failure/recovery.
Those are correctness checks, not a performance benchmark or a liquidation-latency guarantee.
A representative dataset in disposable Compose infrastructure is needed before selecting a
production refresh interval based on measured database load.

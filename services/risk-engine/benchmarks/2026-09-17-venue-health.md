# Venue margin reads — 2026-09-17

Baseline `4920163` was built from an unmodified worktree on this same machine. Runs were
sequential, with no concurrent build/test workload during measurement: baseline, candidate,
baseline repeat, candidate repeat. Intel Core i7-14700KF, Linux x86_64, release build;
full toolchain and configuration are retained in the [raw results](2026-09-17-venue-health.json).

All runs: 1M accounts/affected records (500k spot + 500k perp), 8 workers, 10 warmups,
200 price updates, 20 generation installs, 1,000 account patches, 10% near-threshold accounts.

| Run | Price p50 ms | Price p99 ms | Install p50 ms | Patch p50 ms | Patch max ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| baseline | 33.547 | 43.251 | 914.192 | 0.107 | 1.489 |
| candidate | 33.902 | 45.993 | 937.926 | 0.105 | 0.844 |
| baseline-repeat | 33.953 | 49.412 | 932.071 | 0.111 | 1.267 |
| candidate-repeat | 34.186 | 47.572 | 959.650 | 0.102 | 1.261 |

Candidate price P99 remains below 100 ms and patches remain below single-digit milliseconds.
Baseline price P99 varied from 43.3 to 49.4 ms; candidate ranged from 46.0 to 47.6 ms.
These runs do not establish a statistically significant speed change. No price-update,
patch-commit, or generation-install processing or storage layout was changed.

The new venue query measured **50 ns** median batch-average cost in both candidate runs,
with **zero allocations across 100,000 calls** (enforced by an assertion in the benchmark).
This measures a warm one-pool account in the 1M-account processor. Query work is O(pools in
that account), using its existing slot list; three-pool/spilled-slot correctness is tested.
The measurement excludes HTTP, serialization, source ingestion, and database latency.
It is not a single-call tail-latency measurement. Queries run after all update measurements.

Reproduce each checkout from the workspace root:

```sh
cargo run --locked --release -p risk-engine --bin risk-benchmark
```

Validation: 132 pure Risk tests passed, including worst-pool isolation, independent uncertainty,
zero-maintenance sentinels, pool removal/replacement, poisoned reads, 1/8-worker determinism,
and the patch/full-install oracle. Boundaries, locked metadata, formatting, cargo-deny,
strict workspace Clippy, and all workspace test-target compilation passed.
Local Compose integration execution and `make check` are blocked: Docker socket permission
denied, including outside the sandbox, and sudo requires interactive authentication.
No other database or broker was used. The PR's CI Tests job provisions the repository Compose
stack and executes the full locked workspace test suite.

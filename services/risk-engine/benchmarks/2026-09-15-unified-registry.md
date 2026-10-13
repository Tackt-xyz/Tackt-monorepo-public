# Canonical assets with collateral settings — 2026-09-15

Compared pre-revision commit `1ae340b` with the integrated asset model on the same Intel Core
i7-14700KF, Linux x86_64, Rust 1.98.0 release configuration. Runs were sequential without concurrent
builds/tests during measurement: one million accounts and affected records, eight workers, ten
warmups, 200 price updates, 20 generation installs, 1,000 patches, and 10% near-threshold accounts.
History sampling was disabled in both runs.

| Revision | Price p50 | Price p99 |
| --- | ---: | ---: |
| Before | 33.737 ms | 43.984 ms |
| Integrated asset model | 33.580 ms | 44.751 ms |

Registry loading happens before measurement. Configured factors, tiers, indices, decimal scales,
and processing arithmetic are unchanged. Both runs meet the 100 ms price-update P99 target.
Single-run variation does not establish a speedup or statistically significant regression.
Full configuration and secondary metrics are in the adjacent JSON file.

```sh
cargo run --locked --release -p risk-engine --bin risk-benchmark
```

Verification passed: 43 primitives tests, 48 NATS unit tests, 126 pure Risk tests, 3 sampler tests,
54 Core unit tests, strict workspace Clippy, formatting, boundaries, dependency policy, OpenAPI
freshness, and all test-target compilation. Tests verify that one asset entry supplies identity
and collateral settings, invalid references fail closed, and HyperCore entries remain excluded
from accounting. Local Docker socket permissions still block Compose-backed integration execution;
CI must verify PostgreSQL/NATS integration. No deployment or recovery-state mutation was performed.

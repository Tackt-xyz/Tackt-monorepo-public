# Native standard Hyperliquid margin and venue leverage — 2026-09-17

Baseline `442c4b9` and candidate were measured sequentially on the same machine with the
default release benchmark: 1M affected records, 8 workers, 200 measured price updates,
20 installs, 1,000 patches, 10% near-threshold accounts. Full machine, toolchain, and runtime
metadata are in the [raw results](2026-09-17-native-venue-margin.json). This is one baseline
and one final-candidate run, so the differences are descriptive, not a statistical claim.

| Run | Price p50 ms | Price p99 ms | Install p50 ms | Patch p50 ms | Patch max ms | Venue query ns |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline | 33.645 | 44.668 | 910.121 | 0.099 | 1.391 | 50 |
| candidate | 37.310 | 49.359 | 854.244 | 0.074 | 0.850 | 164 |

The update now calculates current-tier maintenance using leverage divisors and differences of
rounded values, replacing the anchored average-rate approximation. The candidate stays below
the 100 ms P99 target and single-digit millisecond patch target. Price updates cost more than
the baseline; baseline query timing covers health only, while the candidate additionally
calculates gross exposure and leverage. Both queries allocate zero times across 100,000 calls.
The query number is median warm batch-average time, not single-query tail latency.

The default workload exercises base-tier positions. Separate regressions exercise higher tiers,
boundary crossings, 15x leverage precision, fractional price replay, patch/full-install/rebuild
equivalence, and independent pools. No live exchange account comparison was performed.

Validation: 137 tests across Risk's library, processing, evaluation, and persistence recovery
suites passed. Strict full-workspace Clippy, architecture boundaries, locked metadata,
formatting, dependency policy, and compilation of every workspace test target passed.
Docker socket access is denied locally and noninteractive sudo is unavailable, so the full
Compose-backed workspace test run remains blocked locally. No substitute broker/database was used.

Reproduce from each checkout's workspace root:

```sh
cargo run --locked --release -p risk-engine --bin risk-benchmark
```

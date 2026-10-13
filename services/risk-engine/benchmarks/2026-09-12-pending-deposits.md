# Pending deposit eligibility — 2026-09-12

Policy version 4 counts controlled pending deposits using the existing spot valuation path.
The benchmark adds a third scenario: move the SOL balance from Available to Pending in the
mixed spot/perp account. The number of balances and economic exposure remain the same.

```sh
cargo run --locked --release -p risk-engine --example credit_health_benchmark
```

Baseline `179bed8` was remeasured before edits on the same Intel Core i7-14700KF / Linux x86_64
machine, Rust 1.98.0 release profile. Each scenario warms 10,000 calls, then measures 100 batches
of 10,000 evaluations with input construction excluded. All scenarios assert zero allocations.

| Scenario | Batch p50 | Batch p99 | p50 / call | Allocations / reallocations |
| --- | ---: | ---: | ---: | ---: |
| Baseline spot | 5.799869 ms | 5.904435 ms | 579 ns | 0 / 0 |
| Updated spot | 5.876001 ms | 8.076047 ms | 587 ns | 0 / 0 |
| Baseline spot + perps | 7.664316 ms | 8.343374 ms | 766 ns | 0 / 0 |
| Updated spot + perps | 7.731891 ms | 10.277167 ms | 773 ns | 0 / 0 |
| Updated spot including pending + perps | 9.364796 ms | 9.520290 ms | 936 ns | 0 / 0 |

The two comparable median batch times increased approximately 1.3% and 0.9%. The pending
scenario exercises policy selection and all ordinary custody, freshness, normalization, and
valuation checks; it has no baseline counterpart because pending deposits were excluded.
These warm repeated-account batch measurements include run-to-run noise and do not establish
individual-call tail latency, large-account scaling, or liquidation execution timing.

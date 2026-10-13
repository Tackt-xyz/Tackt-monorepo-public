# Account collateral eligibility — 2026-09-10

The credit microbenchmark now supplies authoritative Ledger balance buckets in native atomic
units and verified custody facts. Eligibility, pooling, and normalization run inside the same
evaluator. The spot case uses policy 1; the mixed case uses policy 3 and adds one $5,000 equity
pool shared by two opposing $12,500-notional positions.

```sh
cargo run --locked --release -p risk-engine --example credit_health_benchmark
```

Each scenario warms up 10,000 calls, then measures 100 batches of 10,000 evaluations through
`black_box`. Construction is outside measurement. Both loops assert zero allocation and
reallocation calls. Machine: Intel Core i7-14700KF, Linux x86_64, 28 logical CPUs; Rust 1.98.0,
release profile. Baseline `42ce1cd` was remeasured before edits on this machine.

| Scenario | Batch p50 | Batch p99 | p50 / call | Allocations / reallocations |
| --- | ---: | ---: | ---: | ---: |
| Baseline spot | 3.052900 ms | 3.758676 ms | 305 ns | 0 / 0 |
| Eligible spot | 5.833642 ms | 6.019474 ms | 583 ns | 0 / 0 |
| Baseline spot + perps | 4.769311 ms | 4.966280 ms | 476 ns | 0 / 0 |
| Eligible spot + perps | 7.675395 ms | 7.955864 ms | 767 ns | 0 / 0 |

Median batch time increased approximately 91% for spot and 61% for mixed credit, about
0.28–0.29 microseconds per call. The added work validates balance freshness and custody,
selects Ledger buckets, and pools exact atomic quantities separately for borrowing and
liquidation before conservative quantization. Neither scenario allocates during evaluation.

These are warm repeated-account measurements with three balances, not individual-call tail
latencies or large-portfolio scaling results. They do not measure ingestion, source reconciliation,
the existing processor's million-record target, or end-to-end liquidation submission/execution.

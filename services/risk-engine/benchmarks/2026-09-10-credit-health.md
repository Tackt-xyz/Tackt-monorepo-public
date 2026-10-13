# On-demand credit health — 2026-09-10

Measured the new stateless calculator with one account holding three economic assets (ETH,
BTC, SOL), account debt, pending undrawn commitments, and the initial 85%/90% policy.
The release example warms up 10,000 evaluations, then records 100 batches of 10,000 calls
with borrowed inputs behind `black_box`. It checks allocator counts around the measured loop.

```sh
cargo run --locked --release -p risk-engine --example credit_health_benchmark
```

Machine: Intel Core i7-14700KF, Linux x86_64, 28 logical CPUs; Rust 1.98.0, release profile.
Single-threaded calculator; no external services. Workspace Clippy was running concurrently.

| Measurement | Result |
| --- | ---: |
| Evaluations measured | 1,000,000 |
| Batch p50 | 2.366074 ms |
| Batch p99 | 3.357245 ms |
| p50 batch time / evaluation | 236 ns |
| p99 batch time / evaluation | 335 ns |
| Allocation / reallocation calls | 0 / 0 |
| Allocated bytes | 0 |

These are repeated evaluations of one warm account. Dividing batch latency by call count
does not measure individual-call tail latency, cold-account behavior, service throughput, or
one million distinct accounts. This is the first measurement of the new API; there is no
earlier credit-calculator baseline. Existing processor update paths are unchanged, and this
report makes no regression or performance-target claim for their separate 1M-record benchmark.

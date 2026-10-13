# Portfolio credit with pledged perps — 2026-09-10

The on-demand credit benchmark now measures both a three-asset spot account and the same
account with one $5,000 equity pool shared by two opposing $12,500-notional positions.
The mixed case uses policy version 2 with 10% opening / 5% maintenance reserves.

```sh
cargo run --locked --release -p risk-engine --example credit_health_benchmark
```

Each scenario warms up 10,000 calls, then measures 100 batches of 10,000 evaluations of
borrowed inputs behind `black_box`. Input construction is outside allocation measurement.
The example asserts zero allocation and reallocation calls within each measured loop.

Machine: Intel Core i7-14700KF, Linux x86_64, 28 logical CPUs; Rust 1.98.0, release profile.
Runs were sequential on this machine. Baseline is main at `e5fe353`, remeasured before edits
using its original spot-only example, rather than copied from the previous report.

| Scenario | Batch p50 | Batch p99 | p50 / call | Allocations / reallocations |
| --- | ---: | ---: | ---: | ---: |
| Baseline spot only | 2.357992 ms | 5.174425 ms | 235 ns | 0 / 0 |
| Updated spot only | 3.044681 ms | 3.137312 ms | 304 ns | 0 / 0 |
| Spot + shared-margin perps | 4.734907 ms | 4.787660 ms | 473 ns | 0 / 0 |

Spot-only median batch time increased about 29% with the unified signed-capacity calculation
and policy checks. Signed totals use I256 accumulation so offsetting contributions do not
depend on intermediate Value bounds. The mixed case additionally validates pool versions and
freshness, position membership, and both per-position reserve requirements.

These are warm repeated-account microbenchmarks; divided batch times are not individual-call
tail latencies. The noisy baseline p99 does not establish an improvement. This report makes
no claim about end-to-end liquidation submission latency, venue execution, or the existing
processor's separate one-million-record update target. HTTP/NATS and liquidation execution
are outside this calculator and were not exercised by this benchmark.

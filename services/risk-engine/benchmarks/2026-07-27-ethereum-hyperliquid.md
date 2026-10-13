# Ethereum and Hyperliquid mixed-lane benchmark

Date: 2026-07-27

Profile: release, thin LTO, one codegen unit

Registry: v2, digest `567968ff8b948384ec1565fcac1381bb96e8c22015e76a3c7c582b113188cc8e`

## Workload

- 1,000,000 accounts and affected records per update
- 500,000 standalone HyperCore balance records
- 500,000 Hyperliquid perp records and margin domains
- overall account health using pooled value and total maintenance
- 10% of perp domains near a policy threshold
- alternating Hyperliquid marks
- 8 persistent workers
- 10 warmup updates
- 200 measured updates

## Result

| Measurement | Result |
| --- | ---: |
| P50 | 72.263 ms |
| P95 | 88.489 ms |
| P99 | 93.048 ms |
| Maximum | 103.532 ms |
| Throughput at P99 | 10.75 million exposures/s |
| Changed-state events | 10,000,000 |
| Process CPU time | 84.357 s |
| Wall time | 15.321 s |
| Average logical cores used | 5.51 |
| Allocation calls | 3,606 |
| Allocated bytes | 1,120,201,120 |
| RSS | 526,340 KiB |

Machine: AMD Ryzen 9 3900 12-Core Processor, 24 logical cores

Toolchain: rustc 1.91.1, x86_64-unknown-linux-gnu

The P99 result is 6.952 ms below the 100 ms target.

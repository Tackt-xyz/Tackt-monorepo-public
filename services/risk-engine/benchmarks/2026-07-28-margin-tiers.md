# Margin-tier maintenance factors benchmark

Date: 2026-07-28

Profile: release, thin LTO, one codegen unit

Registry: v3, digest `a35858147b30b9a03d08757db585518796ce53461918e73f8ac1e44d1e4c69fe`

## Workload

- 1,000,000 accounts and affected records per update
- 500,000 standalone HyperCore balance records
- 500,000 Hyperliquid perp records and margin domains
- per-position average maintenance rates derived from registry margin tiers
- overall account health using pooled value and total maintenance
- 10% of perp domains near a policy threshold
- alternating Hyperliquid marks
- 8 persistent workers
- 10 warmup updates
- 200 measured updates
- 20 measured full-generation installs

## Price-update result

| Measurement | Result |
| --- | ---: |
| P50 | 71.795 ms |
| P95 | 88.285 ms |
| P99 | 91.452 ms |
| Maximum | 102.052 ms |
| Throughput at P99 | 10.93 million exposures/s |
| Changed-state events | 10,000,000 |
| Process CPU time | 86.543 s |
| Wall time | 15.324 s |
| Average logical cores used | 5.65 |
| Allocation calls | 3,606 |
| Allocated bytes | 1,120,201,120 |
| RSS | 975,092 KiB |

## Full-generation install result

One install compiles, validates, and rebuilds all 1,000,000 records; realtime position streams
must therefore coalesce updates into batched generations rather than install per update.

| Measurement | Result |
| --- | ---: |
| P50 | 1255.706 ms |
| P95 | 1273.578 ms |
| Maximum | 1380.459 ms |

Machine: AMD Ryzen 9 3900 12-Core Processor, 24 logical cores

Toolchain: rustc 1.91.1 (ed61e7d7e 2025-11-07), x86_64-unknown-linux-gnu

The price-update P99 remains below the sub-100 ms target after switching perp lanes to
tier-derived average maintenance rates.

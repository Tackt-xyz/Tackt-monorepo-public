# Per-account incremental install benchmark

Date: 2026-07-28

Profile: release, thin LTO, one codegen unit

Registry: v3, digest `a35858147b30b9a03d08757db585518796ce53461918e73f8ac1e44d1e4c69fe`

## Workload

- 1,000,000 accounts and affected records per price update
- 200 measured price updates, 8 full-generation installs
- 1000 single-account patches (domain snapshot + perp replacement) across
  distinct accounts, followed by 30 further price updates over the patched state
- 8 persistent workers

## Price updates

| Measurement | Result |
| --- | ---: |
| P50 | 84.078 ms |
| P95 | 90.851 ms |
| P99 | 100.418 ms |
| P50 after 1,000 patches | 76.645 ms |
| Maximum after 1,000 patches | 93.554 ms |

Post-patch price latency shows no fragmentation penalty: spliced lanes keep the dense
account-sorted layout the sweep was designed for.

## Account patches

| Measurement | Result |
| --- | ---: |
| P50 | 0.189 ms |
| P95 | 0.379 ms |
| Maximum | 1.635 ms |

A single account's portfolio replacement costs ~189 µs at the median —
roughly 8,500x cheaper than the 1,611 ms whole-generation install — so a
realtime position stream can install per account update without batching.

## Full-generation install (reconciliation path)

| Measurement | Result |
| --- | ---: |
| P50 | 1611 ms |
| RSS after run | 1,465,460 KiB |

Install cost grew with patch bookkeeping (per-account digests, position ownership maps);
acceptable for the reconciliation-sweep cadence, with parallel digesting as the known
optimization if it matters.

Machine: AMD Ryzen 9 3900 12-Core Processor, 24 logical cores

Toolchain: rustc 1.91.1 (ed61e7d7e 2025-11-07), x86_64-unknown-linux-gnu

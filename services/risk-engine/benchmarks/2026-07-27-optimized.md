# CPU-optimized processing baseline — 2026-07-27

Command:

```sh
cargo run --release --bin risk-benchmark
```

The optimized implementation was measured from one through eight persistent workers. Eight
workers were selected as the default latency/CPU operating point. The timed path includes
1,000,000 affected accounts, 1,000,000 changed-asset exposures, risk evaluation, changed-event
collection, and the shard barrier.

## Result

| Measurement | Original baseline | Optimized default |
| --- | ---: | ---: |
| Workers | 24 | 8 |
| Warm-up iterations | 10 | 10 |
| Measured iterations | 200 | 200 |
| P50 | 61.621 ms | 50.792 ms |
| P95 | 76.493 ms | 55.812 ms |
| P99 | 83.893 ms | 59.468 ms |
| Maximum | 84.278 ms | 63.283 ms |
| Average CPU cores | 18.788 | 5.614 |
| Total-machine CPU | 78.3% | 23.4% |
| Process CPU time | 242.117 s | 58.171 s |
| Allocation calls | 45,600 | 3,606 |
| Allocated bytes | 5,282,880,000 | 1,600,137,120 |
| Resident memory | 220,012 KiB | 262,124 KiB |

The optimized default reduces average machine CPU usage by about 70% and cumulative process CPU
time by about 76%. P99 improves by about 29% and remains 190.5 ms below the 250 ms objective.
Resident memory is higher because preallocated shard delta buffers preserve the
validate-before-mutate guarantee; compact hot exposure records limit that tradeoff.

This run loaded asset registry version `1` with two chains and two assets. HyperCore and Ethereum
USDC were resolved from their external identifiers before fixture construction. The registry
digest was `386d28b7ac7f475ed8b5ee339b46dfff114ccb1f302d1c27f0dc894586af79a3`.

## Implemented changes

- Persistent Rayon worker pool instead of creating OS threads twice per epoch.
- One contribution-calculation pass into reusable per-shard delta buffers, followed by validation
  and application without recalculating contributions.
- Precomputed `amount * factor` in compact hot exposure records.
- Division-free policy classification; exact health is calculated only for transitions and
  account snapshots.
- Reused per-shard event buffers and one exactly sized result allocation.
- Eight-worker default selected from measured scaling results.

Machine and toolchain are unchanged from the original baseline: AMD Ryzen 9 3900, 12 physical
cores/24 logical CPUs, one NUMA node, 125 GiB RAM, Rust 1.91.1, and
`x86_64-unknown-linux-gnu`.

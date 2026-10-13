# Streamed health history — 2026-09-15

Baseline `aca3f1c` was measured from an unmodified temporary checkout on the same Intel Core i7-14700KF,
Linux x86_64, Rust 1.98.0 release build (thin LTO, one codegen unit). Runs were sequential;
no test/build workload was launched during the measured phases. All runs use one million
accounts/affected records, eight workers, ten warmups, 200 measured price updates,
20 generation installs, 1,000 account patches, and 10% near-threshold perp accounts.

| Scenario | Price p50 | Price p99 | Install p50 | Patch p50 |
| --- | ---: | ---: | ---: | ---: |
| Before change | 33.781 ms | 43.631 ms | 918.698 ms | 0.100 ms |
| After, sampling disabled | 33.520 ms | 42.479 ms | 932.588 ms | 0.099 ms |
| After, two samples per price update | 33.781 ms | 43.996 ms | 926.779 ms | 0.099 ms |
| After, queue permanently full | 33.616 ms | 45.587 ms | 923.210 ms | 0.100 ms |

The enabled benchmark captures two accounts after **every** measured price update, far more
frequently than the demo’s one-minute timer. It drives the same library sampler (`risk_engine::egress::history`) the host uses.
The full-queue run retains all queued samples and never drains during measurement. Both paths
remain below the existing 100 ms price-update P99 target. Small timing differences across runs
are not evidence that sampling accelerates calculations; no statistically significant claim is made.

## Capture cost

- Draining queue: **155 ns per pair** at median batch-average cost.
- Full queue: **117 ns per pair** at median batch-average cost.
- **Zero capture allocations** across 100,000 warmed pair captures in either case.
- Includes clock reading and copying/enqueueing scalar snapshots. Excludes consumer drain,
  JSON serialization, PostgreSQL, first-use channel allocation, and host scheduler contention.
- These are warm batch averages, not individual-call tail-latency or complete HTTP latency claims.

## Numeric-only simplification

Repeated before/after measurements on the same machine and configuration, with the immediate
pre-simplification revision `54e29de` as baseline. Each run samples two accounts per price update.
Runs were sequential without concurrent builds/tests. The reduced handoff carries only health,
classification, subject, and capture time; PostgreSQL stores the ratio as `NUMERIC(39,6)`.

| Scenario | Price p50 | Price p99 | Warm pair capture | Capture allocations |
| --- | ---: | ---: | ---: | ---: |
| Before simplification | 33.709 ms | 43.403 ms | 154 ns | 0 |
| Numeric-only, queue drained | 33.994 ms | 44.892 ms | 134 ns | 0 |
| Numeric-only, queue full | 33.565 ms | 47.086 ms | 109 ns | 0 |

These single-run differences do not establish a calculation speedup or regression. Both reduced
sampler paths remain below the 100 ms P99 target; storage formatting and DB latency are excluded.

## Reproduce

```sh
cargo run --locked --release -p risk-engine --bin risk-benchmark
RISK_BENCH_HISTORY_SAMPLING=true cargo run --locked --release -p risk-engine --bin risk-benchmark
RISK_BENCH_HISTORY_SAMPLING=true RISK_BENCH_HISTORY_QUEUE_FULL=true cargo run --locked --release -p risk-engine --bin risk-benchmark
```

Full configuration, environment, allocations, and secondary metrics are in
[`2026-09-15-health-history.json`](2026-09-15-health-history.json).

## Verification limits

The PostgreSQL migration/read tests and actual host→PostgreSQL→Core path require the disposable
Compose services. Local Docker socket access was denied even outside the sandbox; sudo required
interactive authentication. Consequently database-backed tests and DB write latency were not
verified locally. No host-installed or shared database/broker was substituted. The full workspace
integration suite and `make check` remain blocked locally; all targets were compiled, and the
pure Risk, capture, configuration, Core unit, and serialization tests were run separately.
The PR must retain this limitation until the Compose-backed CI test job passes.

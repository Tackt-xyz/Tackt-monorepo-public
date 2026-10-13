# Asset onboarding credit benchmark

Compared base commit `ee64718` with the asset-classification/onboarding candidate on the same
machine, using `cargo run --locked --release -p risk-engine --example credit_health_benchmark`.
The candidate prints registry generation 6 with 20 representations. Each scenario evaluates
three existing collateral assets in 100 batches of 10,000 evaluations, after a 10,000-evaluation
warmup. These are batch latency percentiles divided by batch size, not per-call percentiles.

| Scenario | Base p50 / p99 per evaluation | Candidate p50 / p99 per evaluation |
| --- | --- | --- |
| Spot only | 588 ns / 658 ns | 589 ns / 670 ns |
| Spot and shared-margin perps | 768 ns / 789 ns | 769 ns / 791 ns |
| Pending deposit and shared-margin perps | 771 ns / 793 ns | 769 ns / 788 ns |

Both revisions report zero allocations, reallocations, and allocated bytes in all measured
scenarios. Median changes are within 1–2 ns in this run; the largest p99 change is 12 ns for spot
only. This is a single local comparison, not a statistical regression threshold or a claim
about the separate million-record processing benchmark. The new-instrument exclusion behavior
is covered by credit eligibility tests rather than these existing-asset timing fixtures.

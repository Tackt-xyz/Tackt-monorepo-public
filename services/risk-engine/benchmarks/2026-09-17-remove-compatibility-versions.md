# Remove compatibility versions

Same-machine baseline (`c04f162`) and candidate measurements with the default release benchmark:
1,000,000 accounts, 8 workers, 200 measured price updates, 20 generation installs, and
1,000 account patches. Builds completed before measurement; no other local builds or tests
ran during either measurement. The baseline was built in a detached worktree.

| Metric | Baseline | Candidate |
| --- | ---: | ---: |
| Price P50 (ms) | 33.446 | 33.684 |
| Price P99 (ms) | 44.727 | 46.499 |
| Install P50 (ms) | 990.978 | 928.497 |
| Install P95 (ms) | 1042.607 | 982.313 |
| Patch P50 (ms) | 0.100 | 0.102 |
| Patch maximum (ms) | 1.440 | 1.110 |
| Measured allocations | 3810.000 | 3810.000 |
| RSS (KiB) | 1734664.000 | 1811948.000 |

Price P99 remains below the 100 ms target. The observed increase is approximately 4%; this
single run does not establish a statistically significant regression. Generation installs
improved and account patches remain within the single-digit millisecond target. These changes
remove registry/policy metadata and comparisons without changing arithmetic or worker routing.

Raw results: [JSON](2026-09-17-remove-compatibility-versions.json).

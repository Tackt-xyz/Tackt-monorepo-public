---
title: Use argMax Instead of FINAL for ReplacingMergeTree
impact: CRITICAL
impactDescription: "FINAL causes OOM on large tables (100M+ rows); argMax is 10-100x faster"
tags: [query, FINAL, argMax, ReplacingMergeTree, performance, OOM]
---

## Use argMax Instead of FINAL for ReplacingMergeTree

**Impact: CRITICAL**

`FINAL` modifier on ReplacingMergeTree tables forces deduplication at query time by reading ALL rows, causing OOM on large tables. Use `argMax()` with `GROUP BY` instead.

**Incorrect (FINAL on large table):**

```sql
-- OOM on 189M rows, >15GB memory
SELECT * FROM token_metrics FINAL WHERE token_mint IN (...)

-- Slow: reads all rows then deduplicates
SELECT * FROM position_overview FINAL WHERE trader = ?
```

**Correct (argMax pattern):**

```sql
-- Fast: bounded memory, ~100ms
SELECT
    token_mint,
    argMax(price_usd, last_updated) AS price_usd,
    argMax(market_cap_usd, last_updated) AS market_cap_usd,
    max(last_updated) AS last_updated
FROM token_metrics
WHERE token_mint IN (...)
GROUP BY token_mint

-- For single row lookup, ORDER BY LIMIT is fine
SELECT * FROM token_metrics
WHERE token_mint = ?
ORDER BY last_updated DESC LIMIT 1
```

**Problems with FINAL:**
- Reads entire table before filtering
- Memory grows with table size (OOM at ~100M rows)
- Cannot use parallel processing effectively
- Ignores indexes during dedup phase

**When to use which pattern:**

| Use Case | Pattern | Rule |
|----------|---------|------|
| Single row by key | `ORDER BY version DESC LIMIT 1` | — |
| Batch lookup (IN list) | `argMax() GROUP BY key` | — |
| GROUP BY matches sort-key prefix (any col count) | `argMax() GROUP BY key` | — |
| GROUP BY ≠ sort-key prefix on wide table | Semi-join dedup | `query-semijoin-dedup` |
| Pagination/search | Two-stage: filter first, hydrate second | `query-two-stage-pagination` |
| Leaderboards | Pre-aggregated MVs with argMax | — |

**Nuance on column count (revised 2026-05-12):** argMax on 20+ columns is NOT automatically an anti-pattern. The actual cliff is whether your `GROUP BY` matches the table's `ORDER BY` prefix. See `query-semijoin-dedup.md` for the full decision flow. Quick summary:
- **GROUP BY matches sort-key prefix** → argMax is fine even at 100+ columns (stream aggregation)
- **GROUP BY ≠ sort-key prefix** → switch to semi-join dedup, especially on wide tables

**Benchmark results (`token_metrics`, 7.7M rows, 123 cols, `ORDER BY token_mint`):**

| Pattern | Latency | Memory | Bytes read |
|----------|---------|--------|------------|
| FINAL | ~2,900ms | 710 MiB | 4.34 GiB |
| **argMax × 122 explicit calls** (GROUP BY matches sort key) | **~70ms** | **12 MiB** | **~10 MiB** |
| Semi-join dedup (same query, GROUP BY matches sort key) | ~520ms | 200 MiB | 350 MiB |

So argMax beats both FINAL and semi-join dedup in the prefix-matching case. Reference: validated on dev cluster 2026-05-12, 5 warm runs per pattern.

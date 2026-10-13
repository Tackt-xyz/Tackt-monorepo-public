---
title: Enable Lazy Materialization for Top N Queries
impact: HIGH
impactDescription: "Up to 1576x faster for ORDER BY ... LIMIT queries"
tags: [query, ORDER BY, LIMIT, Top N, lazy materialization, performance]
---

## Enable Lazy Materialization for Top N Queries

**Impact: HIGH**

Lazy materialization defers reading columns until after the Top N rows are identified. For queries with ORDER BY and LIMIT, this dramatically reduces data read.

**Settings to apply:**

```sql
SETTINGS
    query_plan_optimize_lazy_materialization = 1,
    query_plan_max_limit_for_lazy_materialization = 0
```

**Without lazy materialization:**

```sql
-- Reads ALL columns for ALL rows, then sorts and limits
SELECT token_mint, token_symbol, price_usd, market_cap_usd, ...50 more columns
FROM token_metrics
ORDER BY market_cap_usd DESC
LIMIT 100
-- Reads: 98M rows × 50 columns = slow
```

**With lazy materialization:**

```sql
SELECT token_mint, token_symbol, price_usd, market_cap_usd, ...
FROM token_metrics
ORDER BY market_cap_usd DESC
LIMIT 100
SETTINGS
    query_plan_optimize_lazy_materialization = 1,
    query_plan_max_limit_for_lazy_materialization = 0
-- Reads: market_cap_usd for 98M rows → finds Top 100 → reads other columns for 100 rows only
```

**Benchmark results:**
- Before: 2,500ms
- After: 1.6ms (1576x faster)

**When to use:**
- Any query with `ORDER BY ... LIMIT`
- Especially beneficial when selecting many columns
- Works with all MergeTree table engines

**Can be set at client level:**

```rust
// Rust clickhouse client
Client::default()
    .with_option("query_plan_optimize_lazy_materialization", "1")
    .with_option("query_plan_max_limit_for_lazy_materialization", "0")
```

Reference: https://clickhouse.com/blog/clickhouse-gets-lazier-and-faster-introducing-lazy-materialization

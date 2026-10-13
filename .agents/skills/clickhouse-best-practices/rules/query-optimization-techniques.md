---
title: Query Optimization Techniques (Same Schema)
impact: HIGH
impactDescription: "Query-level optimizations that don't require schema changes"
tags: [query, optimization, argMax, PREWHERE, lazy-materialization, parallel-replicas]
source: Production benchmarks on token_metrics table (Feb 2026)
---

# Query Optimization Techniques

These optimizations can be applied at query level without changing table schema.

---

## Quick Reference

| Technique | Impact | When to Use |
|-----------|--------|-------------|
| **Lazy materialization** | ~15-20% faster | Always enable for argMax queries |
| **PREWHERE** | Reduces I/O | When filtering before reading heavy columns |
| **ORDER BY LIMIT 1** | Faster than argMax | Single row lookups |
| **Select only needed columns** | Less I/O | Don't SELECT * |
| **Parallel replicas** | 3-5x faster | Full table scans (100M+ rows, no filter) |

---

## 1. Lazy Materialization

**Settings:**
```sql
SETTINGS
    query_plan_optimize_lazy_materialization = 1,
    query_plan_max_limit_for_lazy_materialization = 0
```

**Impact:** ~15-20% faster for argMax queries

**How it works:** Defers column materialization until actually needed, reducing memory usage.

**Benchmark:**
```
Without: 680ms
With:    561ms (17% faster)
```

---

## 2. PREWHERE (Filter Before Reading)

**Usage:**
```sql
-- Instead of WHERE
SELECT token_mint, bundler_wallets
FROM token_metrics
PREWHERE token_mint = 'xxx'  -- Filters BEFORE reading heavy columns

-- ClickHouse often auto-converts WHERE to PREWHERE, but explicit is safer
```

**Impact:** Reduces I/O by filtering rows before reading all columns

**Best for:** Queries with selective filters + heavy columns (arrays, strings)

---

## 3. ORDER BY LIMIT 1 vs argMax

**For single row lookups, ORDER BY LIMIT 1 is faster:**

```sql
-- SLOWER: argMax (must aggregate all rows)
SELECT argMax(price_usd, last_updated)
FROM token_metrics
WHERE token_mint = 'xxx'
GROUP BY token_mint

-- FASTER: ORDER BY LIMIT 1 (stops after first row)
SELECT price_usd
FROM token_metrics
WHERE token_mint = 'xxx'
ORDER BY last_updated DESC
LIMIT 1
```

**Benchmark:**
```
argMax:           ~100ms
ORDER BY LIMIT 1: ~20ms (5x faster)
```

**Note:** For multiple tokens, argMax with GROUP BY is still the right choice.

---

## 4. Select Only Needed Columns

**Avoid SELECT * - especially with array columns:**

```sql
-- BAD: Reads all 100+ columns including heavy arrays
SELECT * FROM token_metrics WHERE token_mint = 'xxx'

-- GOOD: Only read what you need
SELECT token_mint, price_usd, market_cap_usd
FROM token_metrics WHERE token_mint = 'xxx'
```

**Benchmark (4 tokens):**
```
Without arrays: 8ms,  3.52 MiB
With arrays:    59ms, 105 MiB (7x slower)
```

---

## 5. Parallel Replicas

**Settings:**
```sql
SETTINGS
    enable_parallel_replicas = 1,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = '<tackt-cluster-name>'
```

**Impact:** 3-5x faster for large table scans

**When to use:**
- Full table scans (100M+ rows)
- Aggregations without selective filters
- NOT for point lookups (WHERE token_mint = 'xxx')

**When NOT to use:**
- Filtered queries (adds overhead)
- Small result sets
- Point lookups

**Benchmark:**
```
enriched_trades full scan:
Without parallel: 3.4s
With parallel:    0.65s (5.2x faster)
```

---

## Combined Settings for argMax Queries

```sql
SELECT
    token_mint,
    argMax(price_usd, last_updated) AS price_usd,
    argMax(market_cap_usd, last_updated) AS market_cap_usd
FROM token_metrics
WHERE token_mint IN ('token1', 'token2', 'token3')
GROUP BY token_mint
SETTINGS
    query_plan_optimize_lazy_materialization = 1,
    query_plan_max_limit_for_lazy_materialization = 0
```

---

## Anti-Patterns

### 1. Using FINAL on Large Tables
```sql
-- BAD: FINAL causes OOM on 100M+ rows
SELECT * FROM token_metrics FINAL WHERE token_mint = 'xxx'

-- GOOD: Use argMax instead
SELECT token_mint, argMax(price_usd, last_updated)
FROM token_metrics WHERE token_mint = 'xxx'
GROUP BY token_mint
```

### 2. Partition by Version Column (ReplacingMergeTree)
```sql
-- BAD: Prevents cross-partition deduplication
PARTITION BY toYYYYMMDD(last_updated)

-- GOOD: No partition for ReplacingMergeTree
PARTITION BY tuple()
```

### 3. Reading Array Columns in Batch Queries
```sql
-- BAD: Arrays (bundler_wallets, etc.) cause 7x slowdown
SELECT *, bundler_wallets, sniper_wallets FROM token_metrics

-- GOOD: Load arrays only when needed (detail view)
SELECT token_mint, price_usd, market_cap_usd FROM token_metrics
```

---

## Diagnostic Queries

### Check Query Performance
```sql
SELECT
    query_duration_ms,
    read_rows,
    formatReadableSize(read_bytes) as read_size,
    Settings['query_plan_optimize_lazy_materialization'] as lazy_mat
FROM system.query_log
WHERE type = 'QueryFinish'
    AND query LIKE '%your_table%'
    AND event_time > now() - INTERVAL 1 HOUR
ORDER BY event_time DESC
LIMIT 10
```

### Check Query Plan
```sql
EXPLAIN indexes=1
SELECT ... FROM your_table WHERE ...
```

---

Reference: Production benchmarks on `token_metrics` table, Feb 2026

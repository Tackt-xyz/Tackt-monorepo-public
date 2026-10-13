---
title: Use Semi-Join Dedup When GROUP BY Doesn't Match Sort-Key Prefix
impact: CRITICAL
impactDescription: "When GROUP BY misaligns the sort key, argMax on 35 cols = 5-8s, 8-12 GiB. Semi-join dedup = 0.2-0.4s, 200 MiB. When GROUP BY matches the prefix, argMax is fine even at 100+ cols."
tags: [query, argMax, semi-join, dedup, ReplacingMergeTree, performance, OOM]
---

## Use Semi-Join Dedup When GROUP BY Doesn't Match Sort-Key Prefix

**Impact: CRITICAL — but only in the specific shape described below.**

The **actual cliff** for when argMax becomes pathological is structural, not column count. Whether to use argMax or semi-join dedup depends on the alignment between your `GROUP BY` and the table's `ORDER BY` prefix:

- ✅ **`GROUP BY` matches sort-key prefix** → ClickHouse stream-aggregates without building a hash table. argMax is essentially free even at 100+ columns. **Use argMax.**
- ❌ **`GROUP BY` ≠ sort-key prefix** → forces hash aggregation across the working set. argMax cost scales linearly with column count. **Use semi-join dedup.**

Column count alone is not the trigger — it's only the multiplier for how bad the bad case gets when the structural cliff is hit.

### When argMax performs worst

1. GROUP BY column(s) don't match the table's `ORDER BY` prefix → forces hash aggregation across the full data set.
2. Wide tables (20+ columns) AND the GROUP BY-vs-sort-key mismatch above → exponentially worse.
3. No selective WHERE filter → argMax has to dedupe across the whole table before pruning.

### When argMax is fine

1. GROUP BY matches the sort-key prefix → stream-aggregation, column count basically free.
2. Selective WHERE clause (time window, key prefix) → small working set.

### Bad case (the historical motivation for this rule)

When you need the latest row per key from a ReplacingMergeTree table, want top-N results, AND your `GROUP BY` doesn't match the sort-key prefix → don't use argMax on all columns. Use a semi-join that deduplicates via `(key, max(version))`, then read full rows for matches only.

**Incorrect (argMax on many columns, GROUP BY misaligned with sort key — slow, high memory):**

```sql
-- 5-8s, 8-12 GiB memory, OOM on concurrent requests
-- Table is `ORDER BY (stage, created_at, token_mint)` but GROUP BY uses (token_mint, stage)
-- → hash aggregation forced; argMax cost is O(rows × cols)
SELECT token_mint, stage,
    argMax(col1, last_updated) AS col1,
    argMax(col2, last_updated) AS col2,
    ... argMax(col35, last_updated) AS col35,
    max(last_updated) AS last_updated
FROM discovery
WHERE stage = 'graduated'
GROUP BY token_mint, stage
ORDER BY volume_24h_usd DESC LIMIT 20
```

**Correct (semi-join dedup — fast, low memory):**

```sql
-- 0.2-0.4s, ~200 MiB memory
SELECT *
FROM discovery
WHERE (token_mint, last_updated) IN (
    SELECT token_mint, max(last_updated)
    FROM discovery
    WHERE last_updated >= now() - INTERVAL 1 DAY
    GROUP BY token_mint
)
AND bonding_curve_pct IS NULL  -- business filter
ORDER BY volume_24h_usd DESC LIMIT 20
```

**Why semi-join wins in the bad-case shape:**
- Inner query reads only `token_mint` + `last_updated` columns → lightweight GROUP BY
- Semi-join matches `(token_mint, last_updated)` using primary key → fast lookup
- Outer query reads all columns but only for matching rows → bounded I/O
- Time window in inner query reduces scan from millions to thousands

### Good case (argMax actually wins)

When `GROUP BY` matches the table's `ORDER BY` prefix, argMax is the right call — even at very high column counts. **Don't** rewrite these to semi-join dedup; you'll regress performance.

**Example: `token_metrics` (123 columns, `ORDER BY token_mint`):**

```sql
-- ~70ms, 12 MiB on 7.7M rows
-- GROUP BY token_mint matches sort-key prefix → stream-aggregation
SELECT token_mint,
    argMax(price_usd, last_updated) AS price_usd,
    argMax(market_cap_usd, last_updated) AS market_cap_usd,
    ... 120 more argMax calls,
    max(last_updated) AS last_updated
FROM token_metrics
WHERE last_updated > now() - INTERVAL 1 DAY
GROUP BY token_mint
ORDER BY argMax(stats_24h_volume_usd, last_updated) DESC LIMIT 50
```

Trying to "optimize" this with semi-join dedup makes it ~7× SLOWER because the outer `SELECT *` doesn't get the same prefix-grouped streaming advantage.

### When to use which pattern (updated)

| Scenario | Pattern | Why |
|----------|---------|-----|
| GROUP BY matches sort-key prefix (any column count) | `argMax() GROUP BY key` | Stream aggregation, cheap |
| GROUP BY ≠ sort-key prefix on wide table | **Semi-join dedup** | Hash agg + N×argMax is O(rows × cols) |
| Single row by key | `ORDER BY version DESC LIMIT 1` | Simplest, fastest |
| Lookup by specific keys (IN list, low cardinality) | `argMax() GROUP BY key` | Key filter reduces rows first |
| Search/filter then paginate | Two-stage (see `query-two-stage-pagination`) | Need filter before key selection |

### Combining with time window

Adding `WHERE last_updated >= now() - INTERVAL {N} DAY` in the inner subquery is critical for:
1. Performance — reduces scan from millions to recent rows only
2. Correctness — excludes stale tokens with outdated metrics (e.g., volume from weeks ago)
3. Business logic — shows only actively trading tokens

### Benchmarks

**Bad-case shape (`discovery`, 35 cols, 2.9M rows, `ORDER BY (stage, created_at, token_mint)` but `GROUP BY (token_mint, stage)`):**

| Approach | newly_created | about_to_graduate | graduated |
|----------|--------------|-------------------|-----------|
| argMax 35 cols (mv_discovery_by_stage) | 5.8s | 1.6s | 7.6s |
| Two-stage argMax (discovery) | 0.7s | 0.9s | 0.75s |
| **Semi-join dedup (discovery)** | **0.21s** | **0.27s** | **0.35s** |

→ 27× win with semi-join. Reference: PR #488 (Arthur, Apr 2026).

**Good-case shape (`token_metrics`, 123 cols, 7.7M rows, `ORDER BY token_mint`, `GROUP BY token_mint`):**

| Approach | Latency | Memory | Bytes read |
|----------|---------|--------|------------|
| **argMax × 122 explicit calls** | **~70ms** | **12 MiB** | **~10 MiB** |
| Semi-join dedup | ~520ms | 200 MiB | 350 MiB |
| FINAL | ~2,900ms | 710 MiB | 4.34 GiB |

→ 7× regression if you "optimize" to semi-join here. argMax is correct. Reference: validated 2026-05-12 on dev cluster, 5 warm runs per pattern.

### Anti-pattern: MV with wrong sort key

If a Materialized View includes a column in ORDER BY that changes over time (e.g., `stage`), ReplacingMergeTree creates separate rows for each value — ghost rows that never get replaced:

```sql
-- BAD: stage in ORDER BY → ghost rows on stage transitions
CREATE TABLE mv_discovery_by_stage ... ORDER BY (stage, created_at, token_mint)

-- Token moves from about_to_graduate → graduated:
-- Row 1: (about_to_graduate, ..., token_abc) ← STAYS FOREVER
-- Row 2: (graduated, ..., token_abc) ← new row, different sort key
```

Fix: query the source table with correct ORDER BY (token_mint only), not the broken MV.

### How to detect which case you're in (decision flow)

1. Run `SELECT sorting_key FROM system.tables WHERE name = '<your_table>'` — what's the `ORDER BY`?
2. Compare to your query's `GROUP BY`. Does it match the leading columns?
   - **Yes** → argMax is fine.
   - **No** → semi-join dedup, or rewrite to use a more aligned table/MV.
3. If still unsure, benchmark both shapes on a representative token/key. Don't guess.

### References

- Bad case: PR #488 (Arthur, Apr 2026) — discovery table 35-col cliff
- Good case: 2026-05-12 dev benchmark on `token_metrics` (123 cols) — argMax wins

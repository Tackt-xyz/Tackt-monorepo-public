---
title: Skip GROUP BY-by-key — use uniqExactIf at row level when you only need counts
impact: HIGH
impactDescription: "Replacing per-key GROUP BY + count() with row-level uniqExactIf cut a real trending query 1.52s → 0.53s (65% faster) on dev"
tags: [query, GROUP BY, uniqExactIf, uniq, argMax, performance, trending]
---

## Skip GROUP BY-by-key — use uniqExactIf at row level when you only need counts

**Impact: HIGH**

When a query's pattern is *"GROUP BY high-cardinality key, then count groups,"* you're paying for a hash table of N keys with per-key state — pure overhead if all you need is the count of distinct keys matching a condition. Replace with **`uniqExactIf(key, condition)`** at row level.

The original anti-pattern often hides inside a "we need argMax to get one value per token" misconception. If the only thing the per-key aggregation produces is something you later `count()` — you don't need the aggregation at all.

**Incorrect (per-key GROUP BY just to count):**

```sql
-- 1.52s — builds a hash table of 1.75M tokens, then counts the buckets
SELECT venue, count() AS launches
FROM (
    SELECT token_mint,
        argMax(launchpad_protocol, last_updated) AS venue
    FROM token_metrics
    WHERE toDate(creation_timestamp) >= today() - 30
    GROUP BY token_mint              -- <-- 1.75M groups built just to count
)
GROUP BY venue
```

**Correct (row-level distinct count):**

```sql
-- 0.53s — no per-token state; uniqExactIf does it in one pass
SELECT
    if(coalesce(launchpad_protocol, '') != '', launchpad_protocol, 'other') AS venue,
    uniqExactIf(token_mint, toDate(creation_timestamp) >= today() - 30) AS launches
FROM token_metrics
PREWHERE toDate(creation_timestamp) >= today() - 30
GROUP BY venue
```

**When this pattern is safe (REQUIRED preconditions):**

1. The grouping column (here `launchpad_protocol`) is **stable per key** — every row for a given `token_mint` has the same value. Verify before rewriting:
   ```sql
   SELECT count() FROM (
       SELECT key_col, countDistinct(grouping_col) AS d
       FROM tbl GROUP BY key_col HAVING d > 1
   );
   -- Must return 0.
   ```
2. The condition you're testing (creation date, social presence, etc.) is computable **at row level** — not from a per-key aggregate. If you need `max(graduated_at) >= cutoff`, this won't work directly; see the social-link example below for the row-level transform.

**The argMax-to-row-level transform for "did this key EVER match":**

```sql
-- Wrong (per-key argMax just to OR strings):
GROUP BY token_mint
  argMax(twitter_link, ts) AS twitter,
  argMax(telegram_link, ts) AS telegram,
  ...
-- Then in outer query: countIf(twitter != '' OR telegram != '' OR ...)

-- Better (row-level uniqExactIf):
uniqExactIf(token_mint,
    coalesce(twitter_link, '') != ''
    OR coalesce(telegram_link, '') != ''
    OR coalesce(website_link, '') != ''
    OR coalesce(discord_link, '') != '')
```

The row-level form treats "ever had a social link" as the metric. That matches what we actually want — the original `argMax` form returned the LATEST value, which differs only if a producer ever CLEARS a previously-set link. For most data (social links on tokens, names, addresses) this is fine; verify on your specific column.

**Decision flow:**

```
Q1: Do you GROUP BY a high-cardinality key (>10K distinct)?
    NO  → no concern; ship as-is.
    YES → Q2.

Q2: Is the only thing you do with each group a count() or a single aggregate?
    NO  (you need the per-key argMax values in subsequent logic)
        → keep the GROUP BY.
    YES → Q3.

Q3: Is the grouping column you'll bucket by (venue, region, etc.)
    stable per key? (verified via countDistinct == 1)
    NO  → must keep GROUP BY for correctness.
    YES → REWRITE to uniqExactIf at row level.
```

**Approximate variant for even more speed:**

`uniq()` (HyperLogLog) is ~5x faster than `uniqExactIf` with ~0.5% standard error. If exact counts aren't required for display, swap:

| Function | Latency | Accuracy |
|---|---|---|
| `uniqExactIf` | 0.53s | exact |
| `uniqIf` (HLL) | 0.40s | ~0.5% error |

For trending widgets where counts display as `402K+ launches`, the HLL error is invisible. For integration tests asserting exact numbers, use `uniqExactIf`.

**Combined patterns that compound the win (from the PR #83 optimization):**

1. **Two scans → one scan**: if you LEFT JOIN two subqueries that both scan the same table, combine the WHERE conditions with `OR` and pull into a single scan.
2. **argMax(stable_col, ts) → any(col)**: when verified stable per key, `any()` is the cheapest aggregate.
3. **argMax(String, ts) for ever-matched check → row-level OR**: see the social-links example above.
4. **`query_plan_optimize_lazy_materialization = 1`** for queries with ORDER BY + LIMIT (Discord-pattern leaderboards) — see `query-lazy-materialization.md`.

**Benchmark (real, trending launchpad query on dev cluster, 2026-05-13):**

| Version | Latency | Rows scanned | Memory |
|---|---|---|---|
| GROUP BY token_mint + argMax + LEFT JOIN | 1.52s | 5.23M (×2 scans) | 437 MiB |
| Single scan, row-level uniqExactIf | **0.53s** | 2.6M | 427 MiB |
| Same + `uniqIf` (HLL) | 0.40s | 2.6M | similar |

**When NOT to use this pattern:**

- You actually need the argMax String values (e.g., displaying the latest link in a UI).
- The grouping column varies per key (verify via the countDistinct check above).
- Counts are over a small key set (<10K) — GROUP BY is fine; no win to be had.
- The condition requires per-key aggregate state (e.g., "tokens whose `max(price)` exceeded X").

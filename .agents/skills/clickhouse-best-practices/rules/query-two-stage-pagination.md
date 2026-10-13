---
title: Use Two-Stage Queries for Large Table Pagination
impact: HIGH
impactDescription: "Prevents OOM when paginating/searching tables with 90+ columns"
tags: [query, pagination, search, OOM, memory, two-stage]
---

## Use Two-Stage Queries for Large Table Pagination

**Impact: HIGH**

When paginating or searching tables with many columns (e.g., token_metrics with 90+ columns), a single-stage query with full column hydration can cause OOM. Split into two stages.

**Incorrect (single stage OOM):**

```sql
-- Stage 1 + 2 combined: OOM on 98M rows with 90 columns
SELECT * FROM (
    SELECT token_mint, argMax(col1, lu) AS col1, ..., argMax(col90, lu) AS col90
    FROM token_metrics
    GROUP BY token_mint
)
WHERE token_name ILIKE '%PEPE%'
ORDER BY market_cap_usd DESC
LIMIT 50
```

**Correct (two-stage approach):**

```sql
-- Stage 1: Lightweight - find candidate keys with minimal columns
SELECT token_mint FROM (
    SELECT token_mint,
        argMax(token_name, last_updated) AS token_name,
        argMax(token_symbol, last_updated) AS token_symbol,
        argMax(market_cap_usd, last_updated) AS market_cap_usd
    FROM token_metrics
    GROUP BY token_mint
)
WHERE token_name ILIKE '%PEPE%'
ORDER BY market_cap_usd DESC
LIMIT 50

-- Stage 2: Full hydration only for selected keys (bounded, no OOM)
SELECT token_mint, argMax(col1, lu) AS col1, ..., argMax(col90, lu) AS col90
FROM token_metrics
WHERE token_mint IN (<results from stage 1>)
GROUP BY token_mint
```

**Why this works:**
- Stage 1: Only reads columns needed for filtering/sorting
- Stage 2: Only processes rows matching the small key set
- Memory stays bounded regardless of table size

**Implementation pattern (Rust):**

```rust
// Stage 1: Get candidate mints
let mints: Vec<String> = client
    .query("SELECT token_mint FROM (...) WHERE ... ORDER BY ... LIMIT ?")
    .fetch_all()
    .await?;

if mints.is_empty() {
    return Ok(vec![]);
}

// Stage 2: Full hydration for selected mints only
let rows = client
    .query("SELECT ?fields FROM (...) WHERE token_mint IN ?")
    .bind(&mints)
    .fetch_all()
    .await?;
```

**Use this pattern when:**
- Table has 50+ columns
- Table has 10M+ rows
- Query involves search (ILIKE) or complex filtering
- Pagination with cursor-based navigation

Reference: Validated on production 98M row token_metrics (90+ columns)

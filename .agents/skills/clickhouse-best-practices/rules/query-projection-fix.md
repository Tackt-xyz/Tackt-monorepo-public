---
title: Disable Projections for ORDER BY DESC LIMIT Queries
impact: MEDIUM
impactDescription: "Prevents Code 352 block structure mismatch errors"
tags: [query, projections, ORDER BY, error, Code 352]
---

## Disable Projections for ORDER BY DESC LIMIT Queries

**Impact: MEDIUM**

Tables with projections can cause "Block structure mismatch" errors (Code 352) when using `ORDER BY ... DESC LIMIT` patterns. Disable projection optimization for these queries.

**Error you'll see:**

```
Code: 352. DB::Exception: Block structure mismatch in Pipe::unitePipes
```

**Settings to apply:**

```sql
SETTINGS
    allow_experimental_projection_optimization = 0,
    optimize_use_projections = 0
```

**For tables with projections (e.g., token_metrics, enriched_trades):**

```sql
-- Will fail with Code 352 if table has projections
SELECT * FROM enriched_trades
WHERE token_mint = ?
ORDER BY block_timestamp DESC
LIMIT 100

-- Fixed: disable projection optimization
SELECT * FROM enriched_trades
WHERE token_mint = ?
ORDER BY block_timestamp DESC
LIMIT 100
SETTINGS
    allow_experimental_projection_optimization = 0,
    optimize_use_projections = 0
```

**Can be combined with lazy materialization:**

```sql
SETTINGS
    allow_experimental_projection_optimization = 0,
    optimize_use_projections = 0,
    query_plan_optimize_lazy_materialization = 1,
    query_plan_max_limit_for_lazy_materialization = 0
```

**Set at client level for convenience:**

```rust
Client::default()
    .with_option("allow_experimental_projection_optimization", "0")
    .with_option("optimize_use_projections", "0")
```

**When to apply:**
- Tables that have projections defined
- Queries with `ORDER BY ... DESC LIMIT`
- If you see Code 352 errors

**Note:** This disables projection optimization only; it doesn't break projections for INSERT.

Reference: Observed on production enriched_trades and token_metrics tables with projections

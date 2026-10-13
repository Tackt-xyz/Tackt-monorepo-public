---
title: Handle Corrupt Data with toValidUTF8 and Boolean Conversion
impact: MEDIUM
impactDescription: "Prevents query failures from invalid UTF-8 or non-boolean UInt8 values"
tags: [schema, data quality, UTF-8, boolean, corrupt data]
---

## Handle Corrupt Data with toValidUTF8 and Boolean Conversion

**Impact: MEDIUM**

Real-world data ingestion can introduce corrupt values: invalid UTF-8 sequences in strings, or values like 2/3/255 in UInt8 columns meant to be boolean. Handle these in queries to prevent failures.

**Problem: Invalid UTF-8**

```sql
-- Fails if twitter_link contains invalid UTF-8 bytes
SELECT argMax(twitter_link, last_updated) AS twitter_link
FROM token_metrics
GROUP BY token_mint
-- Error: Cannot parse string, invalid UTF-8 sequence
```

**Solution: Wrap with toValidUTF8()**

```sql
SELECT toValidUTF8(argMax(twitter_link, last_updated)) AS twitter_link
FROM token_metrics
GROUP BY token_mint
```

**Problem: Non-boolean UInt8**

```sql
-- Column is UInt8 but Rust struct expects bool
-- Value 2 cannot deserialize to bool
SELECT argMax(dex_paid, last_updated) AS dex_paid  -- might be 0, 1, 2, or 255
FROM token_metrics
```

**Solution: Convert to boolean with != 0**

```sql
SELECT argMax(dex_paid, last_updated) != 0 AS dex_paid  -- true if non-zero
FROM token_metrics
```

**Common columns that need protection:**

| Column Type | Issue | Fix |
|-------------|-------|-----|
| String URLs (twitter_link, website_link) | Invalid UTF-8 | `toValidUTF8()` |
| String addresses (dev_wallet_address) | Invalid UTF-8 | `toValidUTF8()` |
| UInt8 flags (dex_paid, no_mint, no_freeze) | Values > 1 | `!= 0` |
| UInt8 booleans (is_blacklisted) | Values > 1 | `!= 0` |

**Full example:**

```sql
SELECT
    token_mint,
    toValidUTF8(argMax(launchpad_protocol, last_updated)) AS launchpad_protocol,
    toValidUTF8(argMax(twitter_link, last_updated)) AS twitter_link,
    toValidUTF8(argMax(dev_wallet_address, last_updated)) AS dev_wallet_address,
    argMax(dex_paid, last_updated) != 0 AS dex_paid,
    argMax(no_mint, last_updated) != 0 AS no_mint,
    argMax(is_blacklisted, last_updated) != 0 AS is_blacklisted
FROM token_metrics
WHERE token_mint IN (?)
GROUP BY token_mint
```

**When to apply:**
- Data comes from external sources (scrapers, APIs)
- String columns may contain binary data or emoji
- UInt8 columns are mapped to boolean in application code

Reference: Observed in production token_metrics with scraped social media links

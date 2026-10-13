---
title: Trillion-Row Table Best Practices (ClickPy Reference)
impact: CRITICAL
impactDescription: "Comprehensive guide for designing trillion-row ClickHouse deployments"
tags: [schema, compression, partitioning, materialized-views, trillion-row, clickpy]
source: https://clickhouse.com/blog/clickpy-trillion-row-best-practices
---

# Trillion-Row ClickHouse Best Practices

Based on real-world implementations like ClickPy (2.21 trillion rows of PyPI download data).

---

## 1. Data Type Optimization

**Rule: Use the smallest possible data type for each column.**

| Data Type | Size | Use When |
|-----------|------|----------|
| `Date` | 2 bytes | Daily granularity sufficient |
| `DateTime` | 4 bytes | Second precision needed |
| `DateTime64` | 8 bytes | Sub-second precision needed |
| `UInt8` | 1 byte | Values 0-255 |
| `UInt16` | 2 bytes | Values 0-65,535 |
| `UInt32` | 4 bytes | Values 0-4 billion |
| `LowCardinality(String)` | ~1-2 bytes | < 10,000 distinct values |
| `String` | Variable | High cardinality text |
| `FixedString(N)` | N bytes | Fixed-length codes |
| `Enum8` | 1 byte | < 128 known values |
| `Enum16` | 2 bytes | < 32,768 known values |

### LowCardinality Effectiveness Check

```sql
SELECT
    column,
    type,
    uniq(column) as cardinality
FROM system.parts_columns
WHERE type LIKE 'LowCardinality%'
GROUP BY column, type
-- Warning if cardinality > 10,000
```

### Alias Columns for Derived Values

**Don't store what you can compute:**

```sql
CREATE TABLE events (
    timestamp DateTime64(3),
    -- Alias columns compute on read, store nothing
    date Date ALIAS toDate(timestamp),
    hour UInt8 ALIAS toHour(timestamp),
    day_of_week UInt8 ALIAS toDayOfWeek(timestamp)
)
```

---

## 2. ORDER BY Design

### The Golden Rule

**ORDER BY should match your most common query patterns.**

| Position | Column Type | Example |
|----------|-------------|---------|
| First | Most selective filter | `project`, `user_id`, `tenant_id` |
| Second | Time dimension | `date`, `timestamp` |
| Third+ | Secondary filters | `version`, `country`, `type` |

### ClickPy Example

```sql
ORDER BY (project, date, version, country_code, python_minor, system)
```

Why this order:
1. **project** - Most queries are "show me stats for package X"
2. **date** - Time range is the second most common filter
3. **version** - Version breakdowns are frequent
4. **Remaining** - Less frequently filtered dimensions

### ORDER BY and Compression

Good ORDER BY improves compression because similar values are stored together:

```
-- Poor compression (random order)
[A, B, C, A, B, A, C, B] -> minimal patterns

-- Good compression (sorted)
[A, A, A, B, B, B, C, C] -> long runs of identical values
```

---

## 3. Partitioning Strategy

### When to Partition

| Use Case | Partition By | Reason |
|----------|-------------|--------|
| Time-series with TTL | `toYYYYMM(date)` | Easy data lifecycle |
| Multi-tenant | `tenant_id` | Tenant isolation |
| Analytics (no deletion) | None or `tuple()` | Simpler merges |

### ClickPy Approach

ClickPy uses **no explicit partitioning** because:
- Historical data is never deleted (analytics use case)
- ORDER BY provides sufficient query optimization
- Fewer partitions = simpler merge operations

### Partitioning Health Check

```sql
SELECT database, table, count(DISTINCT partition_id) as partitions
FROM system.parts
WHERE active = 1
GROUP BY database, table
HAVING partitions > 500
-- Warning: > 500 partitions
-- Critical: > 1000 partitions
```

### Anti-Pattern: Partitioning ReplacingMergeTree by Version Column

```sql
-- WRONG: Same key ends up in different partitions, can't deduplicate
PARTITION BY toYYYYMMDD(last_updated)
ENGINE = ReplacingMergeTree(last_updated)

-- CORRECT: No partition or stable partition key
PARTITION BY tuple()  -- or toYYYYMM(created_at)
ENGINE = ReplacingMergeTree(last_updated)
```

---

## 4. Materialized Views Architecture

### The Pattern

**Never query trillion-row tables directly. Pre-aggregate with materialized views.**

```
Raw Data (2 trillion rows)
    |
    v INSERT
Staging Table (Null engine)
    |
    v Materialized View
Main Table
    |
    v 14 Materialized Views
Aggregation Tables (millions of rows each)
    |
    v QUERY
Sub-second responses
```

### SummingMergeTree for Counters

```sql
CREATE MATERIALIZED VIEW downloads_per_day
ENGINE = SummingMergeTree
ORDER BY (project, date)
AS SELECT
    project,
    date,
    count() AS count
FROM raw_events
GROUP BY project, date;
```

### AggregatingMergeTree for Complex Aggregations

```sql
CREATE MATERIALIZED VIEW first_last_download
ENGINE = AggregatingMergeTree
ORDER BY project
AS SELECT
    project,
    minState(date) AS first_download,
    maxState(date) AS last_download,
    uniqState(country_code) AS unique_countries
FROM raw_events
GROUP BY project;

-- Query with merge functions
SELECT
    project,
    minMerge(first_download) AS first,
    maxMerge(last_download) AS last,
    uniqMerge(unique_countries) AS countries
FROM first_last_download
GROUP BY project;
```

### MV Health Check

```sql
SELECT
    database, table,
    max(modification_time) as last_update,
    dateDiff('minute', max(modification_time), now()) as lag_minutes
FROM system.parts
WHERE table LIKE '%_mv' OR table LIKE 'agg_%'
GROUP BY database, table
HAVING lag_minutes > 30
-- Warning if MV is lagging > 30 minutes
```

---

## 5. Compression Optimization

### Codec Selection

| Codec | Best For |
|-------|----------|
| `Delta` | Monotonically increasing integers |
| `DoubleDelta` | Timestamps, sequences |
| `Gorilla` | Floating point with small changes |
| `T64` | Integers with limited range |
| `LZ4` | Fast compression, moderate ratio |
| `ZSTD` | High compression ratio |
| `ZSTD(N)` | N=1-22, higher = better compression, slower |

### Example with Codecs

```sql
CREATE TABLE events (
    -- Monotonically increasing IDs
    id UInt64 CODEC(Delta, ZSTD),

    -- Timestamps (monotonic within partitions)
    timestamp DateTime CODEC(DoubleDelta, ZSTD),

    -- Low cardinality strings
    event_type LowCardinality(String) CODEC(ZSTD),

    -- High entropy data
    user_agent String CODEC(ZSTD(3)),

    -- Floating point metrics
    value Float64 CODEC(Gorilla, ZSTD)
)
```

### Compression Ratio Check

```sql
SELECT
    database, table,
    formatReadableSize(sum(data_compressed_bytes)) as compressed,
    formatReadableSize(sum(data_uncompressed_bytes)) as uncompressed,
    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) as ratio
FROM system.parts
WHERE active = 1
GROUP BY database, table
ORDER BY ratio ASC
-- Warning: ratio < 3
-- Target: ratio > 10
```

### ClickPy Compression Achievement

| Metric | Value |
|--------|-------|
| Rows | 2.21 trillion |
| Uncompressed | 15.26 TiB |
| Compressed | 47.75 GiB |
| Ratio | 327x |
| Bytes per row | ~0.02 bytes |

---

## 6. Ingestion Best Practices

### Batch Size Guidelines

| Metric | Value |
|--------|-------|
| Minimum batch | 10,000 rows |
| Recommended | 100,000 rows |
| Maximum | 1,000,000 rows |
| Insert rate warning | > 5 inserts/sec to same table |
| Insert rate critical | > 10 inserts/sec to same table |

### Insert Pattern Health Check

```sql
SELECT
    tables[1] as table,
    count() / 3600 as inserts_per_second,
    avg(written_rows) as avg_batch_size
FROM system.query_log
WHERE type = 'QueryFinish'
    AND query_kind = 'Insert'
    AND event_time > now() - INTERVAL 1 HOUR
GROUP BY tables[1]
HAVING inserts_per_second > 5 OR avg_batch_size < 1000
```

### Staging Table Pattern (Null Engine)

```sql
-- Stage 1: Raw data lands here (Null engine discards after MV processes)
CREATE TABLE raw_events_staging (
    timestamp DateTime64(3),
    data String
) ENGINE = Null;

-- Stage 2: MV transforms and inserts to main table
CREATE MATERIALIZED VIEW raw_events_mv TO main_events AS
SELECT
    toDate(timestamp) AS date,
    JSONExtractString(data, 'type') AS event_type,
    ...
FROM raw_events_staging;
```

---

## 7. Query Optimization

### Enforce Query Limits

```sql
-- Prevent runaway queries on raw tables
SET max_rows_to_read = 10000000000;  -- 10 billion row limit
SET max_execution_time = 300;         -- 5 minute timeout
SET max_memory_usage = 10000000000;   -- 10 GB per query
```

### Query Aggregated Tables, Not Raw

```sql
-- BAD: Scans 2 trillion rows
SELECT date, count(*)
FROM raw_events
WHERE project = 'pandas'
GROUP BY date;

-- GOOD: Scans millions of rows (pre-aggregated)
SELECT date, sum(count)
FROM downloads_per_day
WHERE project = 'pandas'
GROUP BY date;
```

### Use Dictionaries for Lookups

```sql
CREATE DICTIONARY projects_dict (
    project String,
    description String,
    author String
)
PRIMARY KEY project
SOURCE(CLICKHOUSE(TABLE 'projects'))
LIFETIME(MIN 3600 MAX 7200)
LAYOUT(HASHED());

-- Use in queries
SELECT
    project,
    dictGet('projects_dict', 'description', project) AS description,
    sum(count) AS downloads
FROM downloads_per_day
GROUP BY project
ORDER BY downloads DESC
LIMIT 10;
```

### Conditional Aggregation Instead of JOINs

```sql
-- BAD: Multiple scans with subqueries
SELECT
    project,
    (SELECT count() FROM events WHERE type = 'download' AND project = e.project) AS downloads,
    (SELECT count() FROM events WHERE type = 'view' AND project = e.project) AS views
FROM events e;

-- GOOD: Single scan with conditional aggregation
SELECT
    project,
    countIf(type = 'download') AS downloads,
    countIf(type = 'view') AS views
FROM events
GROUP BY project;
```

---

## 8. Data Lifecycle & Corrections

### Lightweight Deletes (ClickHouse 23.3+)

```sql
DELETE FROM events
WHERE date = '2024-01-15' AND project = 'bad_package';
```

### Correcting Historical Data

**For daily-aggregated tables:**
```sql
-- Delete aggregated data for the date
DELETE FROM downloads_per_day WHERE date = '2024-01-15';

-- Re-insert from source (MVs will not auto-repopulate!)
INSERT INTO downloads_per_day
SELECT project, date, count()
FROM raw_events
WHERE date = '2024-01-15'
GROUP BY project, date;
```

---

## 9. Hardware Sizing

### Storage Calculation

```
Rows x Average_Row_Size_Uncompressed / Compression_Ratio = Disk_Needed

Example (ClickPy):
2.21 trillion x ~7 bytes / 327 = 47.75 GiB
```

### Memory Sizing

| Use Case | RAM Recommendation |
|----------|-------------------|
| Query processing | 2-4 GB per concurrent query |
| Mark cache | 5-10% of compressed data size |
| Primary key cache | Based on ORDER BY column cardinality |
| Dictionary storage | Sum of dictionary sizes |

**Formula:**
```
RAM = (Concurrent_Queries x 4GB) + (Compressed_Size x 0.1) + Dictionary_Size + Buffer
```

---

## 10. Trillion-Row Checklist

| Category | Checklist Item | Target |
|----------|---------------|--------|
| **Data Types** | Use smallest types | `Date` (2 bytes) not `DateTime64` (8 bytes) |
| **Data Types** | Use `LowCardinality` for < 10K values | country, dex, stage, status |
| **Data Types** | Use `Enum` for known sets | trade_direction, order_status |
| **ORDER BY** | Match query patterns | Most-filtered column first |
| **ORDER BY** | Most selective column first | token_mint, trader, user_id |
| **Partitioning** | Use for data lifecycle, not queries | `toYYYYMM()` or none |
| **Partitioning** | Never partition by version column | Breaks ReplacingMergeTree |
| **MVs** | Pre-aggregate all common queries | One MV per query pattern |
| **MVs** | Use SummingMergeTree for counts | Auto-merges on insert |
| **MVs** | Use AggregatingMergeTree for min/max/uniq | *State/*Merge pattern |
| **Compression** | Use codecs for time series | `DoubleDelta` for timestamps |
| **Compression** | Target > 10x ratio | ClickPy achieved 327x |
| **Ingestion** | Batch > 10K rows | < 10 inserts/sec/table |
| **Queries** | Never query raw table | Use aggregation tables |
| **Queries** | Enforce row limits | `max_rows_to_read` |
| **Queries** | Use dictionaries for lookups | Avoid JOINs |

---

## Quick Audit Queries

### Full Health Check

```sql
-- 1. Compression ratios
SELECT table,
    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 1) as ratio
FROM system.parts WHERE active = 1
GROUP BY table ORDER BY ratio ASC LIMIT 10;

-- 2. Partition counts
SELECT table, count(DISTINCT partition_id) as partitions
FROM system.parts WHERE active = 1
GROUP BY table HAVING partitions > 100 ORDER BY partitions DESC;

-- 3. Insert patterns
SELECT tables[1], count() as inserts, avg(written_rows) as avg_batch
FROM system.query_log
WHERE query_kind = 'Insert' AND event_time > now() - INTERVAL 1 HOUR
GROUP BY tables[1] HAVING avg_batch < 1000 OR count() > 100;

-- 4. Raw table scans
SELECT normalizeQuery(query), count(), avg(read_rows)
FROM system.query_log
WHERE type = 'QueryFinish' AND read_rows > 10000000
GROUP BY 1 ORDER BY 3 DESC LIMIT 10;
```

---

Reference: [ClickPy Blog - Trillion Row Best Practices](https://clickhouse.com/blog/clickpy-trillion-row-best-practices)

---
title: Validate RowBinary Type Mapping Between Rust and ClickHouse
impact: CRITICAL
impactDescription: "Positional type mismatches can reject messages or corrupt every subsequent field"
tags: [insert, rowbinary, nats, rust, serialization]
---

## Validate RowBinary type mapping

RowBinary is positional. Rust serialization and ClickHouse columns must agree exactly on field order, count, null markers, byte width, endianness, and variable-length prefixes. Never approve a mapping from Rust and ClickHouse type names alone; verify the serializer actually used by the producer.

### High-risk mappings

| Rust value | ClickHouse expectation | Required verification |
|---|---|---|
| `String` or bytes | length-prefixed `String` | Confirm the exact length encoding and byte payload |
| integer | matching signedness and width | Confirm serializer width and little-endian encoding |
| `bool` | `Bool` or compatible byte | Confirm encoded values and width |
| `Option<T>` | `Nullable(T)` | Confirm null-marker meaning and whether bytes for `T` follow |
| decimal | matching `Decimal(P,S)` backing integer | Confirm precision, scale, width, and overflow behavior |
| timestamp | matching `DateTime64(P)` integer | Confirm unit, timezone assumptions, and signed width |
| UUID | 16 raw bytes for `UUID` | Ensure generic byte serialization does not add a length prefix |
| enum | `Enum8`, `Enum16`, integer, or string | Verify discriminants/labels and compatibility across versions |
| vector or nested value | array/tuple/nested encoding | Require a byte-level fixture; never infer compatibility |

### Tackt checklist

- The NATS subject, envelope, and serialization contract are defined in `tackt-nats`.
- Field order and count match the ingestion table exactly.
- Optionality, decimal scale, timestamp precision, enum representation, and UUID encoding are tested.
- A golden fixture produced by Rust is consumed by the intended ClickHouse format, or an equivalent byte-level compatibility test proves the mapping.
- Additive changes use a rollout that supports mixed producer and consumer versions.
- Preproduction contract changes coordinate producers and consumers without compatibility version stamps.
- ClickHouse remains a projection of the authoritative source and has a documented reconciliation or rebuild path.

When debugging, capture the failing payload shape and compare the produced bytes field by field with ClickHouse’s expected encoding. Do not experiment against production streams or replay messages without explicit authorization.

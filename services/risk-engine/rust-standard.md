# Rust Development Standards

## 1. Required Dependencies

| Crate | Purpose | Notes |
|-------|---------|-------|
| `serde` + `serde_json` | Serialization | Enable `derive` feature |
| `thiserror` | Error types | All custom errors must use this |
| `strum` | Enum utilities | `Display`, `EnumString`, `EnumIter` derives. Only required when enum string conversion is needed. |
| `tokio` | Async runtime | `rt-multi-thread` and `macros` features |
| `dashmap` | Concurrent maps | Prefer over `Mutex<HashMap>` |
| `tracing` | Logging | Primary logging interface |
| `tracing-subscriber` | Log formatting | `fmt` and `EnvFilter` features |
| `metrics` | Metrics | Define names as constants |
| `metrics-exporter-prometheus` | Prometheus | Expose via Prometheus exporter (dedicated port or `/metrics` route) |
| `dotenvy` | Env loading | Load `.env` in development |
| `envy` | Env parsing | Parse env vars into `Config` structs |
| `time` | Date/time | `serde` and `formatting` features |
| `axum` | Web framework | Primary choice for REST APIs. Not required for services using alternative frameworks (e.g., `warp`). |
| `utoipa` | OpenAPI | Generate specs from annotations. Only required for services exposing REST APIs. |
| `utoipa-swagger-ui` | Swagger UI | Serve at `/swagger-ui`. Only required for services exposing REST APIs. |

**Required API Endpoints:** `/health`, `/ready`, `/metrics`

> **Exception:** Geyser plugins and non-HTTP services (e.g., Solana Engine) may expose only `/ready`. The `/health` endpoint applies to services with an HTTP API. Metrics may be served on a dedicated Prometheus exporter port rather than the main API router.

---

## 2. Code Organization

Organize by **functionality**, not by architectural layers:

```
src/
├── main.rs           # Entry point
├── config.rs         # Configuration loading
├── error.rs          # Shared error types
├── order/
│   ├── mod.rs        # Public exports
│   ├── types.rs      # Order types
│   └── processor.rs  # Order processing logic
├── execution/
│   ├── mod.rs
│   └── engine.rs
└── api/
    ├── mod.rs
    └── handlers.rs
```

### Import Order

Group imports: `std` → external crates → internal crates → `crate`/`super`.

```rust
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::error::OrderError;
```

### Import style

Prefer `use` imports over fully qualified paths in function bodies and signatures. Fully qualified paths add noise and make signatures harder to read. Exceptions: macro arguments (e.g. `utoipa` `responses(...)`) where the short name may be ambiguous, and one-off usages where importing would obscure the source.

```rust
// Bad — noisy, repeated module path
) -> Result<Json<crate::models::orders::UiOrder>, crate::models::error::ErrorCode> {
    Ok(Json(crate::models::orders::UiOrder { ... }))
}

// Good — import at top of file, short name in signatures
use crate::models::{error::ErrorCode, orders::UiOrder};

) -> Result<Json<UiOrder>, ErrorCode> {
    Ok(Json(UiOrder { ... }))
}
```

---

## 4. Error Handling

Use `thiserror` for all error types. Use `expect("reason")` over `unwrap()`.

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum OrderError {
    #[error("order {0} not found")]
    NotFound(OrderId),

    #[error("insufficient balance: required {required}, available {available}")]
    InsufficientBalance { required: Decimal, available: Decimal },

    /// Wraps database errors with automatic From conversion
    #[error(transparent)]
    Database(#[from] DatabaseError),
}
```

Define a `Result` type alias in each service:

```rust
pub type Result<T> = std::result::Result<T, OrderError>;
```

### From impls for error conversion

When a layer-specific error type (e.g. a service client error) needs to map to an API error type, implement `From` rather than writing a standalone conversion function. A `From` impl enables `?` to work directly in handlers, removes `map_err` boilerplate at every call site, and keeps the conversion logic in one place next to the source error type.

The impl belongs in the same module where the source error type is defined.

```rust
// Bad — free function, must be threaded through every call site
fn service_err_to_api(e: ServiceError) -> ErrorCode { ... }

state.service.call().await.map_err(service_err_to_api)?;
state.service.other().await.map_err(service_err_to_api)?;

// Good — From impl defined once next to ServiceError, ? works everywhere
impl From<ServiceError> for ErrorCode {
    fn from(e: ServiceError) -> Self {
        match e {
            ServiceError::NotFound    => ErrorCode::ResourceNotFound,
            ServiceError::Unavailable => ErrorCode::ServiceUnavailable { ... },
        }
    }
}

state.service.call().await?;
state.service.other().await?;
```

---

## 5. Observability

### Tracing

```rust
use tracing::{info, error, instrument};

/// Process an order with automatic span creation.
/// `skip(db)` excludes db from span fields, `fields()` adds custom fields.
#[instrument(skip(db), fields(order_id = %order.id))]
pub async fn process_order(order: Order, db: &Database) -> Result<(), OrderError> {
    info!("processing order");
    // ...
}
```

### Metrics

```rust
use metrics::{counter, histogram};

// Increment counter with labels
counter!("orders_processed_total", "type" => order_type).increment(1);

// Record timing histogram
histogram!("order_processing_seconds").record(duration_secs);
```

---

## 6. Testing

```
src/
├── order/
│   └── processor.rs      # Contains unit tests at the bottom
tests/
└── order_flow_test.rs    # Integration tests
```

```rust
// Unit tests at bottom of src file
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_order_with_valid_input_returns_success() {
        let order = Order::builder().amount(Decimal::from(100)).build();
        assert!(process_order(order).is_ok());
    }
}
```

---

## 7. Build & Development

### Makefile

```makefile
.PHONY: build test lint fmt check

build:
	cargo build --release

test:
	cargo test --all-features

lint:
	cargo clippy --all-targets --all-features -- -D warnings

fmt:
	cargo fmt --all

check: fmt lint test
```

### Dockerfile

```dockerfile
# Build stage
FROM rust:1.91-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/service /usr/local/bin/
CMD ["service"]
```

### docker-compose.yml

```yaml
services:
  service:
    build: .
    ports:
      - "8080:8080"
    environment:
      - RUST_LOG=info
```

### Graceful Shutdown

```rust
use tokio::signal;

/// Waits for Ctrl+C or SIGTERM before returning.
async fn shutdown_signal() {
    signal::ctrl_c().await.expect("failed to install handler");
    tracing::info!("shutdown signal received");
}

// Usage with axum:
// axum::serve(listener, app).with_graceful_shutdown(shutdown_signal()).await?;
```

---

## 8. Configuration

```rust
use serde::Deserialize;

/// Application configuration loaded from environment variables.
#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default = "default_port")]
    pub port: u16,
    pub database_url: String,
}

fn default_port() -> u16 { 8080 }

impl Config {
    /// Load config from environment, optionally reading .env file first.
    pub fn load() -> Result<Self, envy::Error> {
        dotenvy::dotenv().ok();
        envy::from_env()
    }
}
```

---

## 9. Documentation

**Required:** Doc comments (`///`) on all public types and their fields.

```rust
/// A pending order waiting for execution.
pub struct Order {
    /// Unique identifier assigned at creation.
    pub id: OrderId,
    /// Target price; `None` for market orders.
    pub limit_price: Option<Decimal>,
}

/// Order execution status.
pub enum OrderStatus {
    /// Awaiting matching engine processing.
    Pending,
    /// Fully executed at the specified price.
    Filled { price: Decimal },
}
```

**Optional:** Inline comments (`//`) only when logic isn't self-evident.

```rust
// Skip zero-quantity orders to avoid division by zero in avg price calc
if order.quantity.is_zero() {
    continue;
}
```

**Avoid:** Obvious comments that restate the code.

---

## 10. Enums Over Strings

Use an enum whenever the set of valid values is known and finite. Strings allow invalid states at compile time and force string matching that the compiler cannot verify.

```rust
// Bad — compiler cannot check exhaustiveness, typos compile silently
pub side: String  // caller can pass "yes", "no", or "banana"

// Good — invalid values rejected at deserialisation, match arms are exhaustive
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContractSide {
    Yes,
    No,
}
pub side: ContractSide
```

### Error code enums

When an external service sends a fixed set of error codes, model them as an enum. If serde encounters an unknown code it returns `Err`, which surfaces as an unexpected error — forcing you to explicitly handle new codes rather than silently falling through.

```rust
// Bad — string match, _ arm hides unhandled codes
match code.as_str() {
    "geo_blocked" => ...,
    "kyc_required" => ...,
    _ => ErrorCode::InternalServerError,  // new codes silently ignored
}

// Good — exhaustive match, compiler error if a new variant is added
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BusinessErrorCode {
    GeoBlocked,
    Maintenance,
    KycRequired,
}

match code {
    BusinessErrorCode::GeoBlocked  => ErrorCode::PmGeoBlocked,
    BusinessErrorCode::Maintenance => ErrorCode::PmMaintenance,
    BusinessErrorCode::KycRequired => ErrorCode::PmKycRequired,
    // adding a variant here without updating this match is a compile error
}
```

### When strings are appropriate

- Open-ended values: free-text fields, user-supplied strings, venue names, currency codes
- Values sourced from systems with no finite schema (e.g. arbitrary token addresses)

---

## 11. Typing

If a real type already exists for a value (e.g. `Signature`, `Pubkey`, `Slot`, an alias such as `UnixTimestamp(u64)`), use it — never substitute a bare primitive. When the wire representation differs from the in-memory type, bridge the two with `#[serde(with = "...")]` rather than changing the field's type.

When no domain type exists, a bare primitive is only acceptable if the value and its wire representation are the same shape. A `Signature` is `[u8; 64]` in memory but a base58 `String` on the wire — using `String` throws away the type, its validation, and its helper methods, and call sites no longer know what the value originally was.

For example:

```rust
use ed25519_dalek::Signature;

// BAD — bare String loses the type, validation, and helpers
#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    name: String,             // OK — free-text, no domain type
    proof_signature: String,  // BAD — Signature is bytes, not a String
}

// GOOD — keep the real type, use serde(with = "..") to control wire format
#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    name: String,
    #[serde(with = "serde_signature_string")]
    proof_signature: Signature,
}
```

---

## Summary

| Category | Standard |
|----------|----------|
| Errors | `thiserror`, `Result<T>` alias, `expect("reason")` over `unwrap()` |
| Logging | `tracing` with `#[instrument]` |
| Metrics | `metrics` crate, expose `/metrics` |
| Concurrency | `DashMap` for concurrent maps |
| Async | `tokio` |
| Date/Time | `time` crate |
| Web APIs | `axum` + `utoipa`, expose `/health`, `/ready`, `/metrics` (Geyser plugins: `/ready` only) |
| Structure | Flat, component-based modules |
| Imports | `std` → external → internal → `crate` |
| Docs | `///` on public types + fields, `//` only when non-obvious |
| Build | Makefile with `build`, `test`, `lint`, `fmt` |
| Types | Enums for finite value sets, never bare strings |
| Typing | Use the real type (e.g. `Signature`, `UnixTimestamp(u64)`) with `#[serde(with = "..")]` when value and wire shape differ; bare primitive is fine only when they match |
| Error conversion | `From` impls, not standalone functions; `?` at call sites |
| Imports | `use` imports for repeated types; fully qualified only in macros or one-off usages |

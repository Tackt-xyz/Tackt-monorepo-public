# Tackt REST API Best Practices

Standards and conventions for building REST APIs at Tackt.

---

## 1. URL Design

### Conventions

| Rule | Example |
|------|---------|
| Use kebab-case | `/discovery-filters`, not `/discoveryFilters` |
| Plural resource names | `/tokens`, `/orders`, `/wallets` |
| Nested resources for ownership | `/users/me/presets`, `/orders/{id}/executions` |
| No verbs in URLs | Use HTTP methods instead |

### HTTP Methods

| Method | Purpose |
|--------|---------|
| `GET` | Retrieve resource(s) |
| `POST` | Create resource or trigger action |
| `PUT` | Replace entire resource |
| `PATCH` | Partial update |
| `DELETE` | Remove resource |

### Path Patterns

```http
# Resource collection
GET    /api/v1/orders          # List orders
POST   /api/v1/orders          # Create order

# Single resource
GET    /api/v1/orders/{id}     # Get order
PATCH  /api/v1/orders/{id}     # Update order
DELETE /api/v1/orders/{id}     # Cancel order

# Nested resources
GET    /api/v1/orders/{id}/executions   # List executions for order

# Current user shorthand
GET    /api/v1/users/me                 # Get current user
PATCH  /api/v1/users/me                 # Update current user
```

---

## 2. Response Structure

Responses return resources directly without wrapper objects. Minimal nesting, maximum clarity.

### Single Resource

```json
{
  "id": "ord_abc123",
  "status": "filled",
  "amount": "1000.5",
  "createdAt": 1723180800000
}
```

Three conventions this example carries, all enforced elsewhere:

| Field | Shape | Why |
|-------|-------|-----|
| Every name | camelCase | `tackt-core-api` response types derive `#[serde(rename_all = "camelCase")]` |
| Money | decimal **string** | Use exact decimal representations; an `f64` loses cents |
| Instants | unix **milliseconds** | Nanoseconds exceed JavaScript's safe integer range |

### Request ID Header

Every response includes an `X-Request-ID` header for distributed tracing:

```http
X-Request-ID: req_abc123
```

If the client sends `X-Request-ID` in the request, echo it back. Otherwise, generate a UUID. This ID propagates through all downstream services for end-to-end debugging.

---

## 3. Pagination

Use cursor-based pagination for list endpoints.

### Request

```http
GET /orders?limit=20&cursor=eyJpZCI6MTIzfQ
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `limit` | integer | Max items to return (configure sensible default and max) |
| `cursor` | string | Opaque cursor from previous response |

### Response

```json
{
  "items": [...],
  "nextCursor": "eyJpZCI6MTQzfQ",
  "hasMore": true
}
```

**Why cursor-based?** Offset pagination returns inconsistent results when items are inserted or deleted between requests. Cursors provide stable traversal.

---

## 4. Filtering & Sorting

### Query Parameters

```http
GET /tokens?q=SOL&minVolume=1000000&sort=volume:desc
GET /orders?status=pending&type=limit&sort=createdAt:desc
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `sort` | string | Field and direction (e.g., `volume:desc`) |
| `q` | string | Search query (where applicable) |
| Field-specific | varies | Filter by field value (e.g., `status=pending`) |

---

## 5. Error Handling

### HTTP Status Codes

| Code | Meaning | When to Use |
|------|---------|-------------|
| `200 OK` | Success | Successful GET, PATCH, DELETE |
| `201 Created` | Resource created | Successful POST creating resource |
| `202 Accepted` | Request accepted | Async operations (e.g., order submission) |
| `400 Bad Request` | Invalid input | Malformed JSON, validation errors |
| `401 Unauthorized` | Not authenticated | Missing or invalid JWT |
| `403 Forbidden` | Not authorized | Valid auth but insufficient permissions |
| `404 Not Found` | Resource not found | Resource doesn't exist |
| `429 Too Many Requests` | Rate limited | Rate limit exceeded |
| `500 Internal Server Error` | Server error | Unexpected server failure |
| `503 Service Unavailable` | Dependency down | Database, message bus, or external service unavailable |

### Async Operations

For operations that execute asynchronously (like order submission), return `202 Accepted` with the resource ID. Clients can poll for status or receive updates via WebSocket.

```text
POST /orders → 202 Accepted { "orderId": "..." }
```

### Error response body

Every error response must include a machine-readable code, a human-readable message, and whether that message is safe to display:

```json
{
  "error": {
    "code": "PM_GEO_BLOCKED",
    "message": "Prediction markets are not available in your region.",
    "sanitized": true
  }
}
```

- `code`: `SCREAMING_SNAKE_CASE`, stable across releases — clients switch on this
- `message`: human-readable, written for the end user
- `sanitized`: whether `message` is safe to show as-is. When `false`, the frontend must show a generic error message instead — `message` may carry raw text from a deserializer, a downstream service, or a third-party venue that was never curated for display.

Never swap the roles of `code` and `message`. A frontend should never parse `message` to determine behaviour — that belongs in `code`. And a frontend must never display `message` without checking `sanitized` first — an unsanitized message is not fit for the UI.

New error variants default to `sanitized: true` only when every field in the variant is either absent or interpolated from backend-authored text (a fixed string, an enum, a number, a user-supplied identifier they already know). Mark a variant `sanitized: false` when its message can carry text the backend didn't author — a deserializer's error output, a downstream service's response body, or a third-party API's error text. Prefer fixing the message at the source (substitute curated text, log the raw detail server-side) over marking a whole variant unsanitized, since one variant is often reused by many call sites and only a few may need it.

### One code per failure condition

Each distinct failure condition a client needs to handle differently must have its own error code. Do not collapse unrelated conditions into a single generic code.

Bad — three conditions, one code, client cannot distinguish:

```text
PM_FORBIDDEN  ← geo-blocked? maintenance? KYC required?
```

Good — each condition is actionable and unambiguous:

```text
PM_GEO_BLOCKED   → inform user, no action available
PM_MAINTENANCE   → show retry UI, poll for recovery
PM_KYC_REQUIRED  → redirect to identity verification flow
```

The test: could a frontend engineer write correct conditional logic using only the `code`? If not, split it.

### HTTP status must reflect the actual condition

The status code communicates the nature of the failure, not just that something went wrong.

| Condition | Correct status | Wrong status |
|-----------|---------------|--------------|
| User lacks permission | 403 | 400 |
| Service in maintenance | 503 | 403 |
| Resource not found | 404 | 400 |
| Downstream API unavailable | 503 | 500 |
| Input validation failed | 400 | 500 |

A service being in maintenance is a service-side condition, not a user permissions issue — use 503, not 403.

### Error message quality

The `message` field must be written for the person reading it, not for the developer who wrote it.

| Bad | Good |
|-----|------|
| `"pm-engine unavailable"` | `"Prediction markets are temporarily unavailable. Please try again shortly."` |
| `"forbidden"` | `"Prediction markets are not available in your region."` |
| `"kyc required"` | `"Identity verification is required to access prediction markets."` |

Format: `[What happened]. [What to do next — if actionable].`

Omit the second sentence when there is no user action available (e.g. geo-blocking).

---

## 6. Authentication

### Dual-Mode Strategy

| Method | Use Case | Mechanism |
|--------|----------|-----------|
| JWT Cookie | Browser-based clients | HttpOnly cookie |
| Service JWT | Server-to-server | `Authorization: Bearer <jwt>` header + `X-User-Id` |

### Token Lifecycle

| Token Type | TTL | Storage | Notes |
|------------|-----|---------|-------|
| Access token | Short (minutes) | Client (cookie) | Signed JWT, frequently refreshed |
| Refresh token | Longer (days) | Database (hashed) | Single-use, rotated on refresh |

### Cookie Configuration

- `HttpOnly`: Prevent XSS access
- `Secure`: HTTPS only
- `SameSite=Strict`: CSRF protection
- `Path=/api`: Scope to API routes

---

## 7. Rate Limiting

### Sliding Window Algorithm

Use distributed cache for rate limiting across instances. Sliding window provides smoother enforcement than fixed windows.

### Required Limits

| Scope | Description |
|-------|-------------|
| Per-IP | Baseline protection against abuse |
| Per-User | Stricter limits on sensitive endpoints (auth, withdrawals) |
| Per-Endpoint | Different limits for reads vs writes |

### Response Headers

```http
X-RateLimit-Limit: <configured_limit>
X-RateLimit-Remaining: <remaining>
X-RateLimit-Reset: <unix_timestamp>
```

When exceeded, return `429 Too Many Requests`.

---

## 8. Idempotency

### For Non-Idempotent Operations

Support `X-Idempotency-Key` header for POST requests that shouldn't be duplicated on retry.

```http
POST /orders
X-Idempotency-Key: 550e8400-e29b-41d4-a716-446655440000
```

Use a client-generated UUID v4 for the idempotency key.

### Implementation

- Store idempotency responses in cache with a TTL
- Return cached response for duplicate keys
- Critical for financial operations (prevents duplicate transactions on network failures)

---

## 9. Input Validation

### Principles

| Practice | Description |
|----------|-------------|
| Strong typing | Deserialize into strict types, reject unknown fields |
| Fail fast | Reject malformed requests before processing |
| Business constraints | Enforce domain-specific limits (amounts, percentages, ranges) |
| Format validation | Validate addresses, emails, IDs before processing |
| SQL injection prevention | Always use parameterized queries |

### Validation Response

Return structured errors with field-level details:

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid request",
    "details": [
      { "field": "amount", "message": "must be positive" },
      { "field": "email", "message": "invalid format" }
    ]
  }
}
```

---

## 10. Security Headers

### Required Headers

| Header | Value | Purpose |
|--------|-------|---------|
| `X-Content-Type-Options` | `nosniff` | Prevent MIME sniffing |
| `X-Frame-Options` | `DENY` | Prevent clickjacking |
| `Content-Security-Policy` | `default-src 'self'` | XSS protection |
| `Strict-Transport-Security` | `max-age=<long_duration>` | Force HTTPS |

### CORS Configuration

| Setting | Value |
|---------|-------|
| Allowed origins | Explicit allowlist of your domains (no wildcards in production) |
| Allowed methods | `GET, POST, PUT, PATCH, DELETE, OPTIONS` |
| Allowed headers | `Authorization, Content-Type, X-Request-ID, X-Idempotency-Key` |
| Credentials | Enabled (required for cookies) |
| Max age | Set preflight cache duration to reduce OPTIONS requests |

---

## 11. WebSocket Design

### Message Protocol

```typescript
// Client → Server
{ "action": "subscribe", "channel": "<channel>", "filters": { ... } }
{ "action": "unsubscribe", "channel": "<channel>", "subscriptionId": "<uuid>" }
{ "action": "ping" }

// Server → Client
{ "type": "ack", "channel": "<channel>", "subscriptionId": "<uuid>", "status": "success", "message": "subscribe successful" }
{ "type": "update", "channel": "<channel>", "subscriptionId": "<uuid>", "data": { ... } }
{ "type": "error", "code": "<CODE>", "message": "...", "sanitized": true }
{ "type": "pong" }
```

The backend creates `subscriptionId` and returns it in the existing subscribe acknowledgment.
Clients do not acknowledge it; they use it to associate updates and to unsubscribe exactly.

`subscriptionId` is optional on unsubscribe: omitting it removes **every** subscription on that
channel (the pre-ID behaviour, kept for clients that don't track IDs yet), and the resulting
ack carries no `subscriptionId`.

### Error Codes

The code set is a contract, not a list to copy: the WebSocket error enum owned by `services/core-api` is its source, and the generated `ws-contract.json` is what clients read. Add a variant there — never a row here.

### Rate Limits

Configure limits for:
- Max subscriptions per connection
- Max messages per second per channel

### Backpressure

Each socket owns a bounded outbox with two lanes: a control lane and a data lane. A producer
pushes synchronously and never waits on a socket, so one slow client cannot delay the fan-out to
every other client. The writer drains the control lane first, so a subscribe ack always reaches
the wire ahead of that subscription's first update.

Replies that must stay ordered against a subscription's updates take the data lane instead. An
unsubscribe ack is one: a client that reads an update *after* the ack that unbound its
`subscriptionId` treats the stream as desynced and reconnects.

A client that cannot keep up is dropped rather than buffered: when either lane fills, or one
frame write passes its deadline, the server closes the socket with application code `4008` and a
`slow consumer: …` reason. Clients treat 4008 as a signal to reconnect and resubscribe. On the
write-deadline path that close frame is best effort — a peer whose window stayed full for the
whole deadline usually cannot read it either — but the server sheds the socket regardless.

---

## 12. Observability

### Required Endpoints

These are root-level infrastructure endpoints (not under `/api/v1/`):

| Endpoint | Purpose |
|----------|---------|
| `/health` | Liveness check (is the process running?) |
| `/ready` | Readiness check (are dependencies available?) |
| `/metrics` | Prometheus metrics |

### Key Metrics

Track at minimum:

| Metric | Type | Description |
|--------|------|-------------|
| `http_request_duration_seconds` | Histogram | Request latency (set p95/p99 targets) |
| `http_requests_total` | Counter | Request count by endpoint/status (alert on error rate) |
| `ws_connections_active` | Gauge | Active WebSocket connections (alert on capacity) |
| `rate_limit_rejections_total` | Counter | Rate limit hits |

### Logging

Include `X-Request-ID` (as `trace_id`) in all log entries for correlation. Log:

| Event | Level |
|-------|-------|
| Request completed | INFO |
| Auth failure | WARN |
| Rate limit exceeded | WARN |
| Database/dependency error | ERROR |

---

## 13. Resilience

### Circuit Breaker Pattern

Protect against cascading failures from dependency outages. Configure per dependency:
- Failure threshold (consecutive failures before opening)
- Observation window
- Open duration before retry

### Failure Modes

| Failure | Response | Mitigation |
|---------|----------|------------|
| Database unavailable | 503 | Circuit breaker, return error |
| Cache unavailable | Continue | Fail open for rate limits, reject auth |
| Message bus unavailable | 503 | Retry with backoff |
| External API down | 503 | Show status to users |

---

## 14. Audit Logging

Log sensitive operations for compliance and incident investigation.

| Operation | Required Fields |
|-----------|-----------------|
| Authentication | `user_id`, `ip`, `method`, `success` |
| Resource creation | `user_id`, `resource_id`, `resource_type`, `ip` |
| Financial transactions | `user_id`, `transaction_id`, `amount`, `direction` |
| Privilege changes | `user_id`, `target_user`, `action`, `ip` |

---

## 15. Breaking Changes

`openapi.json` and `ws-contract.json`, owned by `services/core-api`, are the contracts that
Tackt frontend applications and SDKs generate their clients
from. The pull-request API breaking-change check must compare both against
the base branch and fail on a break. Its rule table defines what
counts. A request is input and a response is output. Consumers generate `z.enum`
and `.strict()` validators from the artifacts, so a new value in an output enum
or a new property on a closed output object is breaking too.

Retire an endpoint in two steps. Mark the handler
`#[deprecated(note = "use …")]`, which utoipa renders as `deprecated: true`, and
keep it for at least one release. The job then lets the operation go.

A deliberate break carries the `api-breaking-change` label on the pull request.
The job runs again on the label and passes. Coordinate the client updates in
Tackt frontend applications and SDKs before you merge.

---

## Summary

| Category | Standard |
|----------|----------|
| URLs | Kebab-case, plural nouns, no verbs |
| Responses | Direct resource objects, no wrappers |
| Tracing | `X-Request-ID` header on all responses |
| Pagination | Cursor-based with `limit` and `cursor` |
| Auth | JWT cookies for browsers, service JWTs for servers |
| Rate limiting | Sliding window, distributed, per-IP and per-user |
| Idempotency | Cache-backed for write operations |
| Validation | Strict types, business constraints, parameterized queries |
| Security | HTTPS only, security headers, scoped CORS |
| Async ops | Return 202, notify via WebSocket |
| Observability | `/health`, `/ready`, `/metrics`, structured logging |
| Resilience | Circuit breakers, graceful degradation |
| Errors | One code per condition, `SCREAMING_SNAKE_CASE`, user-facing messages, status reflects actual cause |
| Breaking changes | The API breaking-change check gates both contract artifacts; deprecate before removal; the `api-breaking-change` label acknowledges a deliberate break |

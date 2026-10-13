# Core API — users and authentication

Core owns Tackt users, their OIDC, Solana, and EVM wallet login methods, and dedicated Ethereum and Hyperliquid accounts.
Operator provisioning routes require `Authorization: Bearer $CORE_API_OPERATOR_TOKEN`.
User routes accept only Core-issued access JWTs. `/health`, `/ready`, and `/metrics` are public.

All authentication handlers and verification live in `src/auth/`. The frontend receives an
OIDC ID token, and Core verifies its signature, issuer, audience, expiry, and challenge nonce
with `jsonwebtoken` before resolving the user and issuing its own access JWT and refresh cookie.
It supports RS256/ES256 ID tokens with lifetimes up to one hour, starting with Google Identity
Services. Provider tokens are never stored. `cookie` handles cookie parsing and serialization.

## Configuration

Core requires the issuer, signing secret, and trusted frontend URI at startup. Solana and
EVM login are always available, with no enable/disable settings. Configure OIDC providers
with their public client IDs when needed:

```sh
CORE_API_AUTH_ISSUER=https://api.example.com
# At least 32 cryptographically random bytes encoded as hex; inject from your secret manager.
# Generate a new deployment secret with: openssl rand -hex 32
CORE_API_AUTH_JWT_SECRET=<hex-encoded-secret>
CORE_API_AUTH_PROVIDERS='[{"name":"google","issuer":"https://accounts.google.com","client_id":"<google-client-id>"}]'
# Shared trusted frontend URI shown in both wallet sign-in messages.
CORE_API_AUTH_FRONTEND_URI=https://app.example.com
CORE_API_USER_SIGNUP_ENABLED=false
# Optional shared code; inject through deployment secrets. Unset disables redemption.
CORE_API_BETA_INVITE_CODE=<shared-invite-code>
```

Register the terminal origin (scheme, host, and port; no path) under Google's **Authorized
JavaScript origins**. The GIS popup flow requires no redirect URI or client secret.
Provider names are unique lowercase names; configure up to eight OIDC providers.
`solana` and `evm` are reserved for the built-in wallet methods. Omitting
`CORE_API_AUTH_PROVIDERS` or setting it to `[]` leaves both wallet methods available.
The old `CORE_API_AUTH_SOLANA_URI` and `CORE_API_AUTH_EVM_URI` settings are rejected at
startup; replace them with `CORE_API_AUTH_FRONTEND_URI`. Remote URLs require HTTPS;
loopback HTTP is supported for local development. Provider metadata and keys are loaded before
startup accepts work, shared across requests without copying key sets, refreshed after five minutes,
and fetched with bounded sizes and timeouts.
A provider outage after the cache expires returns `503`; login may fail during key rotation
until the next refresh. Existing Tackt sessions do not depend on provider availability.

All Core replicas must share the signing secret and issuer. The secret signs HS256 access JWTs
and short-lived login and wallet-challenge envelopes. Changing it invalidates existing access/challenge tokens;
refresh sessions remain usable. Keep it out of source control and logs.

## Web terminal setup

[`terminal.env.example`](terminal.env.example) lists the values needed by the terminal's
Google signup/login integration. Populate the deployment environment from this template;
Core does not load dotenv files automatically. Keep filled credentials out of version control.
The empty required secrets and Google client ID deliberately require configuration before use.

The terminal receives `{clientId,nonce,challengeToken,expiresAt}` from Core and initializes
Google Identity Services with that client ID and nonce. The Google JWT is submitted directly
to `POST /api/v1/auth/sessions`. No frontend client-ID environment variable is needed.
Never put Core signing/operator secrets in `VITE_*` variables.

Choose either of these host arrangements:

- Same origin: forward `/api/*` from the terminal host to Core, preserving the path, browser
  `Origin`, and all `Set-Cookie` headers. The terminal leaves `VITE_CORE_API_ORIGIN` empty.
  Route frontend navigation to the terminal's SPA. Do not cache API responses or log credentials.
- Separate origins on the same HTTPS site: for example, terminal `https://app.example.com`
  and Core `https://api.example.com`. Set the terminal's public build-time
  `VITE_CORE_API_ORIGIN=https://api.example.com`. Set Core's issuer to the API origin and
  frontend URI to the terminal origin. The existing CORS allowlist
  derives from these values. Unrelated `workers.dev` and API domains will not send Strict
  cookies; use matching custom domains or the same-origin proxy.

For local development, the terminal offers an optional Vite `/api` proxy using
`CORE_API_PROXY_TARGET=http://127.0.0.1:8080`. Use a consistent frontend origin for Core's
frontend URI and Google Authorized JavaScript origins. Loopback HTTP is accepted by Core, but browser support for
Secure cookies on loopback varies; use local HTTPS or an HTTPS reverse proxy when necessary.
Tailnet IPs over plain HTTP are not a working authentication setup. Keep Core on loopback behind
TLS termination; this integration does not require weakening the cookie or origin protections.

Set `CORE_API_USER_SIGNUP_ENABLED=true` to allow new users to create accounts. Both terminal
signup and sign-in use the same Google flow. New accounts have `betaAccess=false`; the terminal
then asks authenticated users for `CORE_API_BETA_INVITE_CODE` and admits them after redemption.
This is a UI access gate. Financial and other protected API handlers remain responsible for
server-side authorization; beta access does not activate a user or grant financial permissions.
The terminal does not display fictitious queue positions or referral links.

Before release, configure gateway abuse limits for authentication and invite redemption, and
exercise Google signup, returning login, invite redemption, page reload, refresh, and logout on
the final HTTPS domains. Browser contract tests use mocked responses and do not validate your
OAuth credentials, proxy configuration, or live Google service.

## User authentication endpoints

All new user routes use `/api/v1`, camelCase JSON, direct resources, and Unix-millisecond
profile/session timestamps. List responses are `{items,nextCursor,hasMore}`; `limit` defaults
to 20 and is bounded to 1–100. Treat `cursor` as opaque. Errors are
`{error:{code,message,sanitized}}`, with curated messages and `400` for malformed input.

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/v1/auth/providers` | Page through configured login methods |
| POST | `/api/v1/auth/providers/{provider}/challenges` | Start OIDC login with `{}` |
| POST | `/api/v1/auth/providers/solana/challenges` | Start wallet login/signup with `{address}` |
| POST | `/api/v1/auth/providers/evm/challenges` | Start Ethereum wallet login/signup with `{address}` |
| POST | `/api/v1/auth/sessions` | Verify an OIDC, Solana, or EVM proof and create a session |
| POST | `/api/v1/users/me/identities/{provider}/challenges` | Start linking another OIDC identity with `{}` |
| POST | `/api/v1/users/me/identities/solana/challenges` | Start linking a wallet with `{address}` |
| POST | `/api/v1/users/me/identities/evm/challenges` | Start linking an Ethereum wallet with `{address}` |
| POST | `/api/v1/users/me/identities` | Complete a linking proof |
| GET | `/api/v1/users/me/identities` | Page through the current user's identities |
| POST | `/api/v1/auth/session-tokens` | Rotate the refresh credential; browser body `{}` |
| DELETE | `/api/v1/users/me/sessions/current` | Revoke the current session and clear its cookies |
| GET | `/api/v1/users/me` | Read the current user's safe profile and provisioned wallet addresses |
| POST | `/api/v1/users/me/beta-access` | Redeem a beta invite with `{inviteCode}` |

Session creation and rotation return `201` with `{id,user,expiresAt,refreshExpiresAt}`.
The user contains only `{id,status,betaAccess,createdAt,updatedAt}`. Access and refresh credentials are
returned in `__Secure-tackt-access` and `__Secure-tackt-refresh` cookies, each with
`HttpOnly; Secure; SameSite=Strict; Path=/api`. Tokens are absent from the JSON body.
Browser requests use `credentials: 'include'`; authenticated writes require an allowed
`Origin`. Serve the frontend and API on the same site so Strict cookies can be sent.
CORS allows the configured Core issuer origin and shared frontend origin, with credentials enabled. Foreign origins cannot initiate login writes.

Server clients can extract credentials from `Set-Cookie`, then send the access JWT as
`Authorization: Bearer <jwt>` together with `X-User-Id: <session user ID>`. The ID must match
the verified JWT subject. Do not combine bearer and access-cookie authentication. Server
refresh requests may send `{refreshToken}` without a refresh cookie; sending both is rejected.

### Beta access

Migration `0007` defaults existing and new users to `betaAccess: false`. Authenticated users
redeem with `POST /api/v1/users/me/beta-access` and `{"inviteCode":"<code>"}`. Success returns
`200 {"betaAccess":true}`; repeating redemption preserves the modification timestamp.
Current-user and session profiles expose `betaAccess`; operator responses use `beta_access`.

Set the same `CORE_API_BETA_INVITE_CODE` on every replica. It is reusable, case-sensitive,
read at startup, and limited to 1–128 bytes without surrounding whitespace or control characters.
Core compares digests and never returns or logs the code. Unset configuration returns
`503 BETA_ACCESS_UNAVAILABLE`; invalid configuration fails startup. Wrong codes return
`403 INVALID_INVITE_CODE`, malformed input returns `400`, and closed users cannot enroll.
The usual authentication/origin checks apply; include this route in gateway abuse limits.
Enrollment persists across sessions, restarts, and code rotation/removal; it does not alter
user lifecycle status or grant financial permissions.

### OIDC login and linking

1. POST `{}` to `/api/v1/auth/providers/google/challenges`. Keep the returned `challengeToken`
   in memory. The response also contains `clientId`, `nonce`, and `expiresAt` (Unix milliseconds).
2. Initialize `google.accounts.id` with `client_id`, `nonce`, `ux_mode: 'popup'`, and a JavaScript
   credential handler. Render Google's standard button with `renderButton`.
3. When Google returns `credential`, POST
   `{"method":"oidc","idToken":"<credential>","challengeToken":"..."}` to `/api/v1/auth/sessions`.
4. Core verifies the token and atomically consumes the nonce across replicas before creating
   a session. Challenges expire after ten minutes and cannot be reused. After success, restore
   the current profile and navigate to the original safe terminal destination.

Keep both tokens out of storage, URLs, and logs. Retry with a fresh challenge after errors or
expiry. Google identities use the stable `sub` claim, never email; both Google's supported
issuer spellings resolve to `https://accounts.google.com`, preserving existing users.

The authorization-code proof and provider `client_secret`/`redirect_url` fields have been
removed; old payloads/configuration are rejected. Deploy the matching terminal and Core changes
together. Existing Tackt sessions and linked identities remain valid; in-progress sign-ins restart.
See [Google's JavaScript API](https://developers.google.com/identity/gsi/web/reference/js-reference)
and [server verification guide](https://developers.google.com/identity/gsi/web/guides/verify-google-id-token).

To link an identity, start at `/api/v1/users/me/identities/google/challenges`, then submit the
same proof shape to `/api/v1/users/me/identities`. Authenticate both requests with the same
Tackt session. Completion returns `{userId,issuer,subject}` with `201` for a new association
or `200` for an existing one, without replacing the session. A login proof cannot be used
as a linking proof, or vice versa.

### Solana wallet login and signup

Solana login is always available. Core verifies Ed25519 signatures locally using
`ed25519-dalek`; no Solana RPC, transaction, or wallet service is required.

1. POST `{"address":"<base58-public-key>"}` to `/api/v1/auth/providers/solana/challenges`.
   The `201` response contains `challengeToken`, the exact UTF-8 `message`, and `signInInput`.
2. Ask the wallet to sign `signInInput` through Wallet Standard `signIn`, or the exact bytes
   of `message` through `signMessage(new TextEncoder().encode(message))`. Start a new challenge
   if the wallet changes the address or message. SIWS date fields retain the RFC3339 format
   required by that signing protocol; API profile/session timestamps use Unix milliseconds.
3. POST `{"method":"solana","challengeToken":"...","signature":"<standard-base64>"}`
   to `/api/v1/auth/sessions`. Encode the 64 signature bytes as standard base64, not base58.
   Keep the challenge token local to the initiating login; never take it from a URL.

For linking, start at `/api/v1/users/me/identities/solana/challenges`, sign the returned
message, and submit the proof to `/api/v1/users/me/identities`. Both requests require the
same session. The signed message names the target Tackt user. Completion returns the same
identity resource as OIDC linking.

Challenges bind the application domain/URI, wallet, purpose, nonce, and timestamps, and expire
after five minutes. PostgreSQL consumes a valid proof once across replicas; invalid signatures
do not consume it. After a lost success response or a subsequent persistence failure, request
a fresh challenge. Migration `0006` adds the used-nonce table and expiry index.

### EVM wallet login and signup

EVM login is always available. Core verifies secp256k1 signatures locally with RustCrypto
`k256` and Keccak-256 (`sha3`). Core constructs the expected message from its signed challenge
and compares the recovered signer to the challenged wallet. Messages follow [SIWE (ERC-4361)](https://eips.ethereum.org/EIPS/eip-4361)
and use [ERC-191 personal_sign](https://eips.ethereum.org/EIPS/eip-191). This flow supports
externally owned accounts (EOAs) with Ethereum mainnet `chainId: 1`; contract-wallet
(ERC-1271) and counterfactual-wallet verification are not supported. No RPC is required.

1. POST `{"address":"<0x-prefixed-20-byte-address>"}` to `/api/v1/auth/providers/evm/challenges`.
   Lowercase, uppercase, and valid EIP-55 checksummed addresses are accepted. Invalid
   mixed-case checksums are rejected. The `201` response contains `challengeToken`, the
   exact UTF-8 `message`, and `signInInput` (including a checksummed `address` and `chainId`).
2. Ask the connected wallet to sign the exact returned `message` using `personal_sign`, or
   a wallet library's `signMessage({message})`. For raw JSON-RPC, pass the hex-encoded UTF-8
   message and wallet address to `personal_sign`. Do not hash or prefix the message yourself;
   the wallet applies the ERC-191 prefix. Start a new challenge if the wallet changes.
3. POST `{"method":"evm","challengeToken":"...","signature":"<0x-prefixed-signature>"}`
   to `/api/v1/auth/sessions`. The signature must contain 65 bytes (`r || s || v`), with
   recovery byte `v` equal to 27/28 or 0/1. Keep the challenge token local to the initiating
   login and out of URLs, then discard it after completion.

For linking, start at `/api/v1/users/me/identities/evm/challenges` and complete at
`/api/v1/users/me/identities`, using the same authenticated session for both requests.
The signed statement names the target user. EVM challenges have the same five-minute
expiry, shared PostgreSQL single-use enforcement, signup controls, and session behavior
as Solana. Solana, EVM, OIDC, and access-token envelopes cannot be substituted for one another.

### Identity and session rules

Each exact `(issuer,subject)` belongs to one Tackt user. A user may have multiple accounts at
one provider and multiple wallets. Solana uses issuer `urn:tackt:auth:solana` and the canonical,
case-sensitive base58 public key as subject. EVM uses issuer `urn:tackt:auth:evm` and the
lowercase `0x`-prefixed address as subject, so address casing cannot create another user.
An identity owned by someone else returns
`409 IDENTITY_CONFLICT`. Email never merges accounts. Identity mappings are append-only;
there is no unlink or account-merge endpoint. Operators may enroll identities through
`POST /v1/users/{id}/identities`.

Unknown identities create a pending user only when `CORE_API_USER_SIGNUP_ENABLED=true`;
otherwise they receive `403 IDENTITY_NOT_REGISTERED`. Existing identities still log in when
signup is disabled. Closed users cannot log in or refresh. Profile and identity handlers load
and reject closed users; logout remains available. Authentication does not grant financial
permissions or provision source accounts.

`AuthenticatedUser` verifies the access JWT locally and supplies `user_id` and `session_id`,
without querying PostgreSQL. Signature, algorithm, issuer, audience, token type, expiry, cookie,
and bearer-header checks still apply. Handlers load user records and enforce account-status
rules when needed. The extractor does not check whether the session still exists.

Access JWTs last at most 15 minutes. Sessions expire after 30 days; PostgreSQL stores only
SHA-256 refresh-token hashes. Refresh rotates the credential atomically, with one winner for
concurrent requests. Serialize refresh calls; if a successful response is lost, sign in again.
Logout returns `200 {id,revoked:true}`, clears the browser cookies, and immediately prevents
that session from refreshing. Already-issued access JWTs remain valid until expiry (up to
15 minutes), including for identity linking. Refresh does not extend the original 30-day
session deadline. Other sessions remain active. Migration `0005` adds session storage.

Auth responses use `Cache-Control: no-store` and bodies are limited to 32 KiB. Every response
has an `X-Request-ID` and security headers. The ID propagates to OIDC HTTP calls and lifecycle NATS message headers. Request logs and HTTP metrics use bounded route
labels. `/health` is liveness, `/ready` checks PostgreSQL, and `/metrics` serves Prometheus.
Optional lifecycle NATS publishing remains an acceleration channel: its outage cannot undo
or fail a committed user mutation.

The existing service has no distributed rate-limit/idempotency cache, generated WS
contracts, breaking-change compatibility CI gate, or per-dependency circuit breakers. Those platform requirements in
`REST-API-STANDARDS.MD` remain outstanding; challenge single-use and refresh rotation prevent
replay but do not provide cached retry responses. Apply gateway abuse limits before exposing
signup. This auth change does not claim full service-wide standards compliance.

### Migration from external bearer authentication

The old `CORE_API_USER_AUTH_*` settings are rejected with migration guidance. Old external
bearer JWTs cannot access user routes. Deploy the new frontend flow and Core configuration
together and require users to sign in again. Existing identity mappings survive, but broker
and direct-provider identities can have different issuer/subject pairs; explicitly link or
enroll them rather than merging by email. `GET /v1/users/me` has been removed;
use `/api/v1/users/me` for the current profile. The former external
bearer signup route `POST /v1/users/me` is replaced by proof-based session creation. This is
an intentional authentication-contract break and needs coordinated client rollout.

## Account page demo

Both endpoints require user cookie or bearer authentication, reject closed
users, and return `Cache-Control: no-store`. User identity always comes from the authenticated
session. They are read-only; no ledger, price, collateral configuration, or authorization state
is modified. `account-summary` is the sole account balance endpoint.

### Summary and balances

`GET /api/v1/users/me/account-summary` reads one Ledger balance snapshot, then attaches the
latest price received by this Core process for each recorded asset. The response contains
`userId`, `asOf` (Unix milliseconds), `calculationMode: "demo"`,
`complete`, `missingPriceAssetIds`, the four totals below, and `assets`.

| Field | Calculation |
| --- | --- |
| `netEquityUsd` | Sum of included token holdings × price. For this demo the label means gross portfolio value, **before** subtracting debt. |
| `creditLimitUsd` | Sum of `max(0, asset value) × 0.85`. |
| `outstandingDebtUsd` | Sum of `max(0, principal_liability credits − debits)` converted to tokens and valued at the asset price. This is booked principal only. |
| `availableCreditUsd` | `max(0, creditLimitUsd − outstandingDebtUsd)`. |

Included holdings use debits minus credits from `available`, `reserved`, `order_reserved`,
`withdrawal_reserved`, `hyperliquid_margin`, `liquidation_proceeds`, and `customer_surplus`.
`pending`, `principal_liability`, and `provider_shortfall` are excluded from portfolio value.
These are booked Ledger quantities, not a live external wallet or perpetual-position valuation.
Every recorded bucket is still returned, including excluded and zero-net buckets.

Each asset includes `assetId`, `symbol`, `chain` (CAIP-2), `caip19`, nullable `tokenAddress`,
`decimals`, nullable `collateral`, and a `buckets` map. Metadata comes from the embedded registry.
Each bucket exposes separate cumulative `debitQuantity` and `creditQuantity` atomic integer
strings and `updatedAt` in Unix milliseconds. The `collateral` object contains registry `enabled`,
`factor`, `venue`, and `price` settings; its factor is not the demo LTV. Each asset also includes:

- `quantity` and `outstandingDebtQuantity`: decimal strings in **token units**.
- `priceUsd`: registry fixed USD price, or latest received aggregate for market-priced assets.
  `priceAsOf` is the aggregate computation time in Unix milliseconds; it is null for fixed
  prices and missing aggregates. All registered USDC representations are fixed at $1.
- `valueUsd`, `creditContributionUsd`, and `outstandingDebtUsd`: USD decimal strings.
- `ltv: "0.85"` and `liquidationThreshold: "0.90"`: uniform **demo display settings for every
  asset**, including assets whose registry collateral setting is absent or disabled. The
  liquidation threshold is displayed only; neither rate changes Risk policy or grants credit.

USD values use eight fractional digits, truncating toward zero per asset; totals sum those
values. Arithmetic uses checked wide integers, never floating point. Assets are ordered by
`assetId`. An empty account returns empty assets and zero totals. A missing price for a nonzero
included holding or debt makes `complete` false and lists the asset in `missingPriceAssetIds`;
only affected valuations and dependent totals become null. Zero quantities can still be valued
at zero without a price. Fixed pricing is explicit in the registry and overrides live quotes;
it is never inferred from a stablecoin label. Last received prices have no freshness
cutoff: inspect `priceAsOf`; `asOf` is the database read time, not a freshness guarantee.

For 5 ETH at $3,000 plus 2,000 USDC at $1, with 2,000 USDC principal debt, the totals are:

```json
{
  "netEquityUsd": "17000.00000000",
  "creditLimitUsd": "14450.00000000",
  "outstandingDebtUsd": "2000.00000000",
  "availableCreditUsd": "12450.00000000"
}
```

### Recent activity

`GET /api/v1/users/me/activity?limit=20&cursor=...` returns `{ "items": [...], "nextCursor": ... }`.
`limit` defaults to 20 and accepts 1–100. Pass the returned opaque cursor unchanged to fetch the
next page; null means no further entries at that read. Malformed cursors, invalid limits, and
unknown query parameters return 400. Ordering is newest `recorded_at`, then transaction UUID;
cursors preserve database microseconds, even though displayed timestamps are milliseconds.

Each item is one committed Ledger transaction, with `id`, `type`, `commandKind`, `status`,
`timestamp`, `effectiveAt`, nullable `reversesTransactionId`, and `changes`. Each change contains
`assetId`, `symbol`, `decimals`, `bucket`, atomic `debitQuantity` / `creditQuantity` strings, and
`quantityChange` in token units. Positive means a bucket increase: debits minus credits for
asset buckets, credits minus debits for principal liability. A credit draw can increase both
available funds and debt; a reservation can decrease one bucket and increase another. Do not
sum all changes as cash flow or treat accounting debit/credit sides as money out/in.

Display categories include `deposit`, `credit`, `repayment`, `withdrawal`, `fee`, `reversal`,
and fallback `ledger_transaction`; `commandKind` always preserves the exact journal kind.
Deposit `pending`, `finalized`, and `rejected` are separate lifecycle transactions, each grouped
across its postings; the `status` is the phase **at that event**, not current deposit state.
Other transactions have `status: "recorded"`, which does not claim external settlement.
Unjournaled observations are not included. Only the authenticated user's postings appear,
even if a transaction also contains system accounts or another user's postings. Audit blobs,
account codes, and idempotency keys are never returned.

The separate read-only `GET /api/v1/users/me/risk-health` and `/risk-health/history` endpoints
serve Risk's sampled pooled-margin health from PostgreSQL. They do not calculate Tackt credit
health or grant financial authorization. See the [history contract](../../architecture/risk-health-history.md)
for timestamps, exact units, missing data, authentication, and bounded history queries.

## Asset and market catalogs

Both catalogs require the same user cookie or bearer authentication as account-summary. They read only
the embedded registry and return the complete configured list in registry order, independently
of user holdings. They use `Cache-Control: no-store`.

- `GET /api/v1/assets` returns `assets`: symbol, economic-asset grouping, chain, CAIP-19 identity,
  token address, decimals, and the same `collateral` object used in account-summary. `assetId` is the
  Ledger code, or null for Risk-only assets. Use `caip19` to identify every representation.
- `GET /api/v1/markets` returns `markets`: registry `marketId`, name, symbol, venue, price mapping,
  and `marginTiers`. Each tier has an exact decimal `lowerBoundNotional` and integer `maxLeverage`.

Venue objects contain `kind` and `name`; price mappings contain `source`, `symbol`, and `externalId`.
Collateral factors use six fractional digits and tier notionals use eight, without floating-point
conversion. Catalogs describe configured support; they do not report live prices, venue health,
user borrowing limits, or authorization decisions.

## OpenAPI

[`openapi.json`](openapi.json) describes all mounted Core API routes using OpenAPI 3.1.
Runtime routes and documentation share `openapi::routes!` registrations; request and response
schemas derive from the Rust DTOs. Register new endpoints the same way and update annotations
when behavior, validation, or authentication changes.

Regenerate from the workspace root after changing the API:

```sh
cargo run --locked -p tackt-core-api --example export_openapi -- --write
```

Commit the generated JSON with the code change. The **OpenAPI freshness** CI job runs on every
pull request and main push, regenerates the spec, and fails if the committed file differs.
Run the same command with `--check` to check locally. No database, NATS, or credentials are needed.
This checks documentation freshness; it does not reject intentional breaking API changes.

## Run locally

```sh
docker compose -f deploy/compose.yaml up -d postgres
cp .env.example .env
set -a; . ./.env; set +a
cargo run --locked -p tackt-core-api
```

Run the Indexer separately with `cargo run --locked -p tackt-indexer -- serve`. The Indexer reads
the Core-owned `users` and `user_accounts` tables through the typed Postgres repository. It has no
user mutation endpoint.

## Operator provisioning endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/users` | Create idempotently by unique `external_id` |
| `GET` | `/v1/users?limit=50&cursor=...` | List users, keyset-paginated |
| `GET` | `/v1/users/{id}` | Retrieve a user |
| `PUT` | `/v1/users/{id}/status` | Set `pending`, `active`, `restricted`, or `closed` |
| `PUT` | `/v1/users/{id}/accounts/{source}` | Associate or replace the dedicated account for a source (`ethereum` or `hyperliquid`) |
| `POST` | `/v1/users/{id}/identities` | Explicitly enroll an exact broker issuer/subject (operator only) |

Listing returns `{"data": [...], "next_cursor": "..."}`; pass `next_cursor` back as `cursor` for
the next page, and a `null` cursor means the listing is exhausted. User objects expose account
associations as a map keyed by source, e.g. `"accounts": {"ethereum": "0x..."}`, so adding a
source never changes the response shape.

Lifecycle transitions are enforced: `pending → active|closed`, `active → restricted|closed`, and
`restricted → active|closed`. `closed` is terminal and no status returns to `pending`; an invalid
transition returns `409` with code `INVALID_TRANSITION`.

```sh
curl -i http://127.0.0.1:8080/v1/users \
  -H "Authorization: Bearer $CORE_API_OPERATOR_TOKEN" \
  -H 'X-Operator-Id: local-operator' -H 'X-Request-Id: create-tester-001' \
  -H 'Content-Type: application/json' \
  -d '{"external_id":"tester-001","metadata":{"cohort":"private-beta"}}'

curl -X PUT http://127.0.0.1:8080/v1/users/$USER_ID/accounts/ethereum \
  -H "Authorization: Bearer $CORE_API_OPERATOR_TOKEN" -H 'X-Operator-Id: local-operator' \
  -H 'Content-Type: application/json' \
  -d '{"account":"0x1111111111111111111111111111111111111111"}'

curl -X PUT http://127.0.0.1:8080/v1/users/$USER_ID/accounts/hyperliquid \
  -H "Authorization: Bearer $CORE_API_OPERATOR_TOKEN" -H 'X-Operator-Id: local-operator' \
  -H 'Content-Type: application/json' \
  -d '{"account":"0x2222222222222222222222222222222222222222"}'

curl -X PUT http://127.0.0.1:8080/v1/users/$USER_ID/status \
  -H "Authorization: Bearer $CORE_API_OPERATOR_TOKEN" -H 'X-Operator-Id: local-operator' \
  -H 'Content-Type: application/json' -d '{"status":"active"}'
```

The first identical create returns `201` and `created: true`; a retry returns `200`, the same ID,
and `created: false`. Reusing the external ID with different creation metadata returns `409`.
Errors use `{"error":{"code":"...","message":"...","sanitized":true}}`.

EVM addresses must be 20-byte `0x`-prefixed hexadecimal and are stored lowercase. An account can
belong to only one user and each user has at most one of each type. Metadata must be an object;
keys suggesting secrets, passwords, private/signing keys, credentials, mnemonics, or seed phrases
are rejected. User records contain no signing material.

## Demo custody keys

Operator user creation and first-login signup generate an independent Ethereum secp256k1 key
and Solana Ed25519 seed using OS randomness. The user, keys, and (for signup) login identity
commit atomically. Repeated creation, login, and concurrent signup preserve the winning keys.
Linking another login identity does not create or replace custody keys.

Migration `0008_user_signing_keys` adds a separate `user_signing_keys` table with one row per
user: `user_id`, `ethereum_private_key`, `solana_private_key`, and `created_at`. Both keys are
unencrypted `bytea` values for this demo: Ethereum uses a 32-byte big-endian private scalar;
Solana uses a 32-byte Ed25519 seed, from which the usual 64-byte seed-plus-public-key export
can be reconstructed. Private keys are retrieved only through the typed internal repository
API, separately from user profiles; HTTP responses and lifecycle events contain no key bytes.

Migration `0009_user_wallet_addresses` adds `ethereum_address` and `solana_address`, each
with a unique index. Ethereum addresses are lowercase hex; Solana addresses are the base58
public key and are case-sensitive. New signup stores these atomically with the keys. Core
startup backfills missing addresses for existing key pairs in bounded batches before listening;
restarts and concurrent replicas preserve the original keys and already stored addresses.
Users without keys are not backfilled. The PostgreSQL initializer applies migrations before
this backfill. Source-account associations remain explicitly managed through the account API.

Wallet reads and address lookup use the internal typed repository API:
`user_wallet_addresses(user_id)` and `user_id_by_wallet_address(WalletAddress::Ethereum(address))`
or `WalletAddress::Solana(address)`. Ethereum lookup uses lowercase hex; Solana preserves case.
`GET /api/v1/users/me` returns the authenticated user's profile with `solanaAddress`
(case-sensitive base58 public key) and `evmAddress` (lowercase `0x`-prefixed address),
alongside `id`, `status`, `betaAccess`, `createdAt`, and `updatedAt`. Both address fields
are present and `null` for users without a provisioned wallet. These are the user's
Core-provisioned custody addresses, separate from linked login wallets and source-account
associations. Public addresses are joined into the existing user query; the source-account
query is unchanged, keeping the endpoint at two database queries. Private keys are never
loaded by this lookup. Session creation and refresh responses retain their existing user
profile shape. There is no HTTP
endpoint to look up other users by wallet address.

Each chain exposes an internal signing function; there are no signing HTTP endpoints or a
shared transaction dispatcher:

```rust
use tackt_core_api::signing::{evm, svm, hypercore};

let signed = evm::sign(&repo, user_id, evm::EthereumTransaction {
    chain_id: 11155111,
    nonce: 0,
    gas_limit: 21000,
    max_fee_per_gas: "2000000000".into(),
    max_priority_fee_per_gas: "1000000000".into(),
    to: Some("0x1111111111111111111111111111111111111111".into()),
    value: "1000000000000000".into(),
    data: "0x".into(),
}).await?;

let signed = svm::sign(&repo, user_id, &base64_wire_transaction).await?;

let signed = hypercore::sign(&repo, user_id, hypercore::HypercoreTransaction {
    action: hypercore::Action::L1(hypercore::L1Action::UpdateLeverage {
        asset: asset_index,
        is_cross: true,
        leverage: 3,
    }),
    network: hypercore::Network::Testnet,
    nonce: timestamp_ms,
    vault_address: None,
    expires_after: None,
}).await?;
```

`evm.rs` supports EIP-1559/type 2 with an empty access list. Fees and value are decimal wei
strings; `to: None` requests contract creation. Its result contains the address, hex
`signed_transaction` suitable for `eth_sendRawTransaction`, and `transaction_hash`.
Encoding and signature preimages follow [EIP-1559](https://eips.ethereum.org/EIPS/eip-1559).

`svm.rs` accepts a base64 legacy or v0 Solana wire transaction with its recent blockhash and
signature slots (zero-filled for missing signatures). The user's Solana public key must be a
required static signer. It fills that user's signature slot while preserving the message and
other slots. Its result contains the address, base64 `signed_transaction`, and the user's base58
`signature`. Other required signers must still sign before submission; this signature is the
network transaction ID only when this user is the first signer.

`hypercore.rs` uses the user's Ethereum key and the Hyperliquid SDK signing schemes:

- `Action::L1` supports orders (limit and trigger), cancel, cancel-by-cloid, batch modify,
  leverage and isolated-margin updates, scheduled cancel, subaccount creation, subaccount/vault
  USD transfers, and no-op. Typed fields preserve the required MessagePack order, then nonce,
  optional vault address, and optional expiry are hashed into the EIP-712 phantom agent.
- `Action::User` supports USD send, spot send, bridge withdrawal, spot/perp USD class transfer,
  and send-asset. These use their action-specific EIP-712 types and network separation. Outer
  vault/expiry options are rejected for this scheme; send-asset has an explicit subaccount field.

Specify `Network::Mainnet` or `Network::Testnet` and supply a unique nonce (normally Unix
milliseconds); reuse it only for an exact retry. Prices and amounts are decimal strings and
are normalized before signing and returning the payload. The caller resolves asset indices
and token identifiers; signing performs no market lookup. Unmodelled actions, including agent
approval and multisig workflows, require extending the typed action enums.

The HyperCore result contains `address` and `payload`. Serialize `payload` directly to obtain
an `/exchange` request body containing the action, nonce, `{r,s,v}` signature, and supported
optional fields. It retains the action's wire field order; converting it through an unordered
JSON map before signing again can change the hash. SDK compatibility is checked against 27
public digest vectors from `hyperliquid-python-sdk` 0.24.0, including both environments, vaults,
expiry, and both signing schemes. No keys are stored in those fixtures. See the
[official signing implementation](https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/master/hyperliquid/utils/signing.py).

All three functions load the user's key through the repository and return `SigningError` for
missing keys, malformed transactions, or storage failures. They do not broadcast transactions,
reserve funds, estimate fees, choose nonces, or fetch blockhashes.

## Signing and funding boundaries

Core owns the provider-neutral signing and funding boundaries for future durable action workflows.
The workflow capabilities currently define boundary types only: no workflow constructs or invokes either
capability, and there is no runtime configuration for them yet. The unavailable implementations
return `WaitingForSigning` or `WaitingForFunding`; test-only simulators create visibly fake
`SIMULATED-` receipts and no external evidence.

The future custody-exit workflow must construct its signing token only from the successful Ledger
response branch. The token records Ledger's committed transaction ID as an ordering marker; it is
not authorization and does not independently prove that the action and reservation match the
persisted Ledger request. Real providers and external submission remain deferred to M8.

## Financial action orchestration

Core's `orchestration` module coordinates a durable action through synchronous Risk evaluation
and an allowlisted Ledger reservation. It records Risk's deterministic `evaluation_id` before the
Ledger call, allocates one stable Ledger authorization/idempotency key per action step, retries an
ambiguous HTTP outcome with that same key, and advances the action only after Ledger returns a
committed transaction. Only `healthy` evaluations are authorized; every other classification
fails closed without contacting Ledger.

The complete post-action portfolio comes from the internal `ActionProjectionProvider` capability.
It is deliberately not accepted from an authenticated-user or operator request: a financial
caller must never be allowed to fabricate the projection Risk evaluates. A production action
route must therefore wire an authoritative Core-owned projection provider before it is mounted.
The coordinator's restart scan is exposed by `ActionOrchestrator::resume_active`; signing remains
separate and can begin only after the returned action reaches `waiting_for_signing` with a stored
Ledger transaction ID.

Risk and Ledger endpoints are configured together through `CORE_API_RISK_URL`,
`CORE_API_RISK_TOKEN`, `CORE_API_LEDGER_URL`, and `CORE_API_LEDGER_TOKEN`; partial configuration
fails startup. Remote URLs require HTTPS, while loopback HTTP is accepted for development tests.
`CORE_API_FINANCIAL_HTTP_TIMEOUT_MS` defaults to 5000 and is bounded to 30000 milliseconds.

## Data

`users` stores identity, lifecycle, metadata, and timestamps; `user_accounts` stores associations.
There is deliberately no application-level user audit, event, or outbox table. If change capture
is needed later, it will be implemented from PostgreSQL WAL or an STG pipeline rather than stored
alongside user records.

## Lifecycle events (optional, enabled by `NATS_URL`)

After each committed mutation the service publishes onto the `TACKT_USERS` JetStream stream:
`tackt.users.created` (only on first insert, not idempotent replays), `tackt.users.status_changed`
(suppressed for no-op requests already at the target status), and `tackt.users.account_associated`.
The stream is an acceleration channel, not a source of truth: a publish lost between commit and
NATS (including a crash in that window) is acceptable because consumers reconcile from the users
table — the indexer refreshes affected users on events and periodically compares its complete
registry. Publish failures are logged and never fail the API response. Without `NATS_URL` no
events are published.

Run database and NATS integration tests only against the disposable repository Compose stack:

```sh
export POSTGRES_DB=tackt_auth_test
docker compose -f deploy/compose.yaml up -d --wait postgres nats
export DATABASE_URL="postgres://tackt:tackt@127.0.0.1:5432/${POSTGRES_DB}"
export NATS_URL=nats://127.0.0.1:4222
cargo test --locked -p tackt-core-api
cargo test --locked -p tackt-postgres --test user_provisioning_integration
docker compose -f deploy/compose.yaml down
```

The auth integration tests use local mock OIDC providers and generated Solana and Ethereum wallets. They
check multiple login methods, identity ownership, single-use wallet challenges and refresh
rotation, signup policy, closed users, and logout.
The provider unit tests require only a loopback HTTP listener.

## Aggregate price cache

When `NATS_URL` is set, Core also requires the pre-provisioned aggregate-price stream.
Each process creates its own ephemeral consumer of live updates. Startup and reconnect
subscribe from new messages only, with no backfill, historical replay, or KV lookup.

`ApiState.prices.get_latest(asset)` returns the last aggregate received for that asset, or
`None` until this process has received one. Moka retains one immutable record per asset
without TTL, expiry, timestamp checks, decay, or clock-skew tolerance. The original price,
producer sequence, computation timestamp and evidence references are
preserved. Downstream consumers own freshness and tolerance policy. This module adds no
HTTP price or valuation endpoint.

Only the feed task writes the cache. Within a subscription it uses JetStream's stored message
sequence to ignore older/duplicate redeliveries; the producer's clock-derived counter does
not determine ordering. Each new live subscription starts a new broker cursor, so stream
recreation or producer clock rollback cannot wedge the cache.

Reads continue returning the last received value when the feed disconnects, reconnects, or
stops. Reconnect retains cached values and waits for live updates to replace them. A cold
process starts empty. The one-second reconnect backoff only controls transport retries; it
never expires a price. Core aborts and joins the feed on shutdown. With no `NATS_URL`, the
cache stays empty. `/ready` remains database-based.

The closed asset enum bounds memory. A missing value is not zero or an assumed dollar peg.
USDC has no aggregate by design; the account summary reads its explicit fixed-$1 registry rule
for both holdings and debt, with no cache lookup or fabricated market timestamp. Financial
authorization and price decay/tolerance remain the responsibility of downstream services.

Validation (against the disposable services in `deploy/compose.yaml`):

```sh
cargo test --locked -p tackt-core-api --lib prices::tests
NATS_URL=nats://127.0.0.1:4222 cargo test --locked -p tackt-core-api --test prices
```

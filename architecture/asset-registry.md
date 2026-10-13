# One canonical asset registry

The root `asset-registry.json` has one asset list. Each entry contains
its symbol, structured classification, chain, token identity, decimals, explicit `pricing`, `ledger_asset`, and
optional `collateral` settings. A collateral block contains the streamed asset index, venue,
price reference, factor, and enablement. There is no separate Risk asset or chain registry.

Core summary pricing is explicit: `aggregate` requires received market evidence; `fixed_usd`
declares a positive integer `price_e8` (100000000 = $1). All USDC representations use fixed $1
for holdings and debt, independent of feed availability. No observation timestamp is fabricated.
This rule does not change source-specific streamed Risk prices or collateral eligibility.

For example, Ethereum USDC's identity and 900000 collateral factor are on the same entry. Core
reads `assets::info(LedgerAsset::Usdc).collateral` alongside its existing metadata. Risk builds
indexed processing lanes from that same canonical entry. Common top-level chains, venues,
price factors, and perp markets supply the referenced identities and margin tiers.

## Accounting support and validation

`assets::registry().catalog()` exposes all twenty-two canonical entries. The twenty accounting entries
have explicit Ledger identities. The two existing HyperCore entries have `ledger_asset: null`;
they are visible to Risk but remain rejected by accounting lookups, funding, and signing paths.
Existing `assets::all()`, `info()`, and accounting lookups cover those twenty accounting assets. Ledger
support cannot be inferred from a matching display symbol, economic group, or collateral factor.

Primitives validates token/chain identities, unique indices and identities, Ledger support, and
collateral/market references. Risk retains source/venue compatibility checks, margin-tier
calculation, and account-level eligibility rules. Credit LTV/liquidation policy and custody/bucket
rules remain in Rust; they are not replaced by streamed collateral factors. The existing factors,
price indices, tier values, and decimal scales are preserved. The twelve new accounting
representations have no streamed collateral settings and remain unconfigured in the credit policy.

The file is embedded only by primitives. The retired `RISK_ENGINE_ASSET_REGISTRY_PATH` and
`RISK_BENCH_ASSET_REGISTRY_PATH` overrides fail explicitly when set.

## Preproduction contract changes

Registry data is embedded without a version or exact-file digest handshake. Asset and chain
lookups, link validation, source isolation, and financial checks remain mandatory.

Application contracts have one current shape. Coordinate producers and consumers when changing
registry definitions or wire shapes. Older snapshots, queued payloads, and stored evidence are
not supported by a compatibility decoder. Use fresh disposable state for this preproduction
cutover; this PR does not reset any environment. SQLx migration ordering and checksums remain.

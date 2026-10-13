use crate::source::price_feed::{PriceMarkets, VenueMarket};
use anyhow::{Context, Result, bail};
use std::{env, net::SocketAddr, time::Duration};
use tackt_primitives::ledger::LedgerAsset;

/// About 2.3 hours at 12 blocks/s; larger values turn every account change into
/// an unexpectedly long recovery job rather than a bounded overlap.
const MAX_HYPERLIQUID_LOOKBACK_BLOCKS: u64 = 100_000;

/// One EVM chain to ingest: HTTP JSON-RPC for authoritative reads, WebSocket
/// for the low-latency `newHeads` wake signal.
#[derive(Clone)]
pub struct EvmChainConfig {
    pub chain_id: u64,
    pub rpc_url: String,
    pub ws_url: String,
}

#[derive(Clone)]
pub struct Config {
    pub database_url: String,
    pub listen_addr: SocketAddr,
    /// Ethereum mainnet first, then any optional additional EVM chains (Base
    /// is enabled by setting `BASE_WS_URL`).
    pub evm_chains: Vec<EvmChainConfig>,
    /// Required QuickNode StreamData base URL; credentials are configured separately.
    pub hyperliquid_grpc_url: String,
    pub hyperliquid_grpc_token: String,
    /// First-run bootstrap and account-registry overlap only. Process restarts
    /// recover from the broker-maintained per-feed KV observation instead.
    pub hyperliquid_initial_lookback_blocks: u64,
    pub hyperliquid_reconcile_interval: Duration,
    /// Required: the acknowledged JetStream publish is the Indexer's only durable
    /// side effect for observations and price evidence alike, so the service is
    /// useless without a NATS connection.
    pub nats_url: String,
    /// Per-venue price ingestion. Every venue defaults to [`PriceMarkets::defaults`] —
    /// BTC, ETH, and SOL, plus JitoSOL/USD on Kraken for the demo.
    /// A venue's variable overrides its default market list
    /// (`BINANCE_MARKETS=BTC:BTCUSDT,ETH:ETHUSDT`, `KRAKEN_MARKETS=ETH:ETH/USD`,
    /// `BYBIT_MARKETS=ETH:ETHUSDT`, `COINBASE_MARKETS=ETH:ETH-USD`), and setting it
    /// empty disables that venue.
    pub price_markets: PriceMarkets,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            database_url: required("DATABASE_URL")?,
            listen_addr: env::var("INDEXER_LISTEN_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:8081".into())
                .parse()
                .context("invalid INDEXER_LISTEN_ADDR")?,
            evm_chains: {
                let mut chains = vec![EvmChainConfig {
                    chain_id: parse("ETHEREUM_CHAIN_ID", 1)?,
                    rpc_url: env::var("ETHEREUM_RPC_URL")
                        .unwrap_or_else(|_| "https://ethereum-rpc.publicnode.com".into()),
                    ws_url: websocket_url(&required("ETHEREUM_WS_URL")?)?,
                }];
                // Base ingestion is opt-in: absent BASE_WS_URL, the indexer
                // runs mainnet-only exactly as before.
                if let Ok(base_ws) = env::var("BASE_WS_URL") {
                    chains.push(EvmChainConfig {
                        chain_id: parse("BASE_CHAIN_ID", 8453)?,
                        rpc_url: env::var("BASE_RPC_URL")
                            .unwrap_or_else(|_| "https://mainnet.base.org".into()),
                        ws_url: websocket_url(&base_ws)?,
                    });
                }
                let mut ids: Vec<u64> = chains.iter().map(|chain| chain.chain_id).collect();
                ids.sort_unstable();
                ids.dedup();
                if ids.len() != chains.len() {
                    bail!("EVM chain ids must be distinct")
                }
                chains
            },
            hyperliquid_grpc_url: grpc_url(&required("HYPERLIQUID_GRPC_URL")?)?,
            hyperliquid_grpc_token: required_trimmed("HYPERLIQUID_GRPC_TOKEN")?,
            hyperliquid_initial_lookback_blocks: bounded_lookback(
                "HYPERLIQUID_INITIAL_LOOKBACK_BLOCKS",
                3_600,
            )?,
            hyperliquid_reconcile_interval: positive_duration(
                "HYPERLIQUID_RECONCILE_SECONDS",
                300,
            )?,
            price_markets: {
                let defaults = PriceMarkets::defaults();
                // One binding per venue key, so the variable read and the errors it is named in
                // cannot drift apart.
                let venue = |key: &str, defaults: Vec<VenueMarket>| {
                    let configured = env::var(key).ok();
                    venue_markets(key, configured.as_deref(), defaults)
                };
                PriceMarkets {
                    binance: venue("BINANCE_MARKETS", defaults.binance)?,
                    kraken: venue("KRAKEN_MARKETS", defaults.kraken)?,
                    bybit: venue("BYBIT_MARKETS", defaults.bybit)?,
                    coinbase: venue("COINBASE_MARKETS", defaults.coinbase)?,
                }
            },
            nats_url: required("NATS_URL")?,
        })
    }
}

/// Resolves one venue's market list: the venue's defaults when its variable is unset,
/// nothing when it is set empty, and otherwise its `ASSET:MARKET` pairs. Anything
/// malformed or unknown to the registry fails startup.
fn venue_markets(
    key: &str,
    value: Option<&str>,
    defaults: Vec<VenueMarket>,
) -> Result<Vec<VenueMarket>> {
    let Some(value) = value else {
        return Ok(defaults);
    };
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }
    let mut markets = Vec::new();
    for entry in value.split(',') {
        let entry = entry.trim();
        let (asset, market) = entry
            .split_once(':')
            .with_context(|| format!("{key} entry {entry:?} is not ASSET:MARKET"))?;
        let asset = LedgerAsset::from_code(asset.trim())
            .with_context(|| format!("{key} entry {entry:?}"))?;
        let market = market.trim();
        if market.is_empty() {
            bail!("{key} entry {entry:?} names no market")
        }
        markets.push(VenueMarket {
            asset,
            market: market.to_owned(),
        });
    }
    Ok(markets)
}

fn websocket_url(value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() {
        bail!("ETHEREUM_WS_URL must not be empty")
    }
    let parsed = url::Url::parse(value).context("invalid ETHEREUM_WS_URL")?;
    let loopback = match parsed.host() {
        Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    };
    match parsed.scheme() {
        "wss" => {}
        "ws" if loopback => {}
        "ws" => bail!("ETHEREUM_WS_URL requires wss:// for non-loopback endpoints"),
        scheme => bail!("ETHEREUM_WS_URL must use ws:// or wss://, got {scheme}"),
    }
    Ok(value.to_owned())
}

fn grpc_url(value: &str) -> Result<String> {
    let value = value.trim();
    let parsed = url::Url::parse(value).context("invalid HYPERLIQUID_GRPC_URL")?;
    let loopback = match parsed.host() {
        Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    };
    match parsed.scheme() {
        "https" => {}
        "http" if loopback => {}
        "http" => bail!("HYPERLIQUID_GRPC_URL requires https:// for non-loopback endpoints"),
        scheme => bail!("HYPERLIQUID_GRPC_URL must use https://, got {scheme}"),
    }
    if parsed.username() != ""
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.path() != "/"
    {
        bail!("HYPERLIQUID_GRPC_URL must not contain credentials, a token path, query, or fragment")
    }
    Ok(value.to_owned())
}

fn required(key: &str) -> Result<String> {
    env::var(key).with_context(|| format!("{key} is required"))
}

fn required_trimmed(key: &str) -> Result<String> {
    let value = required(key)?;
    let value = value.trim();
    if value.is_empty() {
        bail!("{key} must not be empty")
    }
    Ok(value.to_owned())
}

fn bounded_lookback(key: &str, default: u64) -> Result<u64> {
    let blocks = parse(key, default)?;
    if blocks > MAX_HYPERLIQUID_LOOKBACK_BLOCKS {
        bail!("{key} must not exceed {MAX_HYPERLIQUID_LOOKBACK_BLOCKS} blocks")
    }
    Ok(blocks)
}

fn positive_duration(key: &str, default: u64) -> Result<Duration> {
    let seconds = parse(key, default)?;
    if seconds == 0 {
        bail!("{key} must be greater than zero")
    }
    Ok(Duration::from_secs(seconds))
}

fn parse<T>(key: &str, default: T) -> Result<T>
where
    T: std::str::FromStr + ToString,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    env::var(key)
        .unwrap_or_else(|_| default.to_string())
        .parse()
        .with_context(|| format!("invalid {key}"))
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_HYPERLIQUID_LOOKBACK_BLOCKS, bounded_lookback, grpc_url, venue_markets, websocket_url,
    };
    use tackt_primitives::ledger::LedgerAsset;

    #[test]
    fn venue_markets_parse_and_fail_closed() {
        let markets = venue_markets(
            "KRAKEN_MARKETS",
            Some("ETH:ETH/USD, BTC:BTC/USD"),
            Vec::new(),
        )
        .expect("valid markets");
        assert_eq!(markets.len(), 2);
        assert_eq!(markets[0].asset, LedgerAsset::Eth);
        assert_eq!(markets[0].market, "ETH/USD");

        let defaults = crate::source::price_feed::PriceMarkets::defaults();
        assert_eq!(
            venue_markets("BINANCE_MARKETS", None, defaults.binance.clone())
                .expect("unset falls back to the defaults"),
            defaults.binance
        );
        assert!(
            venue_markets("BINANCE_MARKETS", Some("  "), defaults.binance)
                .expect("an empty value disables the venue")
                .is_empty()
        );
        for invalid in ["ETHUSDT", "DOGE:DOGEUSDT", "ETH:", "eth:ETHUSDT", ","] {
            assert!(
                venue_markets("BINANCE_MARKETS", Some(invalid), Vec::new()).is_err(),
                "{invalid:?}"
            );
        }
    }

    #[test]
    fn grpc_url_requires_tls_and_forbids_token_paths() {
        assert_eq!(
            grpc_url("  https://example.com:10000  ").unwrap(),
            "https://example.com:10000"
        );
        assert!(grpc_url("http://example.com:10000").is_err());
        assert!(grpc_url("http://127.0.0.1:10000").is_ok());
        assert!(grpc_url("http://[::1]:10000").is_ok());
        assert!(grpc_url("https://example.com/token").is_err());
        assert!(grpc_url("https://token@example.com").is_err());
        assert!(grpc_url("https://example.com?token=secret").is_err());
    }

    #[test]
    fn lookback_has_a_bounded_default() {
        assert_eq!(
            bounded_lookback("UNSET_TEST_LOOKBACK", 3_600).unwrap(),
            3_600
        );
        assert!(
            bounded_lookback("UNSET_TEST_LOOKBACK", MAX_HYPERLIQUID_LOOKBACK_BLOCKS + 1).is_err()
        );
    }

    #[test]
    fn websocket_url_requires_tls_except_on_loopback() {
        assert_eq!(
            websocket_url("  wss://example.com/path  ").unwrap(),
            "wss://example.com/path"
        );
        assert!(websocket_url("ws://example.com").is_err());
        assert!(websocket_url("ws://127.0.0.1:8546").is_ok());
        assert!(websocket_url("ws://[::1]:8546").is_ok());
        assert!(websocket_url(" ").is_err());
    }
}

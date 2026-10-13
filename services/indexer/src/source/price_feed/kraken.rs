//! Kraken spot-market ingestion.
//!
//! # Feed choice
//!
//! The subscription is the `trade` channel. Each trade carries the venue's monotonic per-pair
//! `trade_id` (the `source_sequence`) and the venue's own RFC 3339 timestamp; the `ticker`
//! channel's mid prices carry neither. On subscribe, Kraken sends a snapshot of recent trades
//! before live updates — those are processed like any other trade: the deterministic evidence
//! identity deduplicates the republish and the engine's sequence guard rejects anything that
//! does not advance.
//!
//! # Mapping onto the price-evidence contract
//!
//! | Kraken fact | Evidence fact |
//! |---|---|
//! | `trade_id` | `source_sequence` (strictly increasing per pair) |
//! | `price` (JSON number, captured as raw text) | `PriceQuote` value and scale, exactly as sent |
//! | `timestamp` (RFC 3339) | `source_timestamp` |
//! | the decoded trade | bounded `raw` evidence |
//!
//! Kraken sends prices as JSON numbers, not strings. They are captured as raw wire text and
//! parsed by the shared exact-decimal parser, so no float ever touches a price.
//!
//! # Subject identity
//!
//! Kraken pairs are spelled with a `/` (`ETH/USD`), which the market-symbol charset forbids.
//! The evidence subject replaces it with `-` (`ETH-USD`), a deterministic one-way spelling:
//! every v2 pair contains exactly one `/`, so no two pairs can collapse onto one subject.

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;
use serde_json::value::RawValue;
use tackt_nats::prices::{PriceAuthority, PriceEvidence, PriceQuote};
use tackt_primitives::Provenance;

use super::venue::{Market, Subscription, Venue};

/// The Kraken v2 spot venue.
pub(super) struct Kraken;

/// One trade as Kraken states it.
///
/// `price` and `qty` are JSON numbers on the wire; they are captured as raw text so the shared
/// exact-decimal parser sees exactly what the venue sent. Unknown fields are tolerated: this is
/// a provider wire that grows additively.
#[derive(Debug, Deserialize)]
pub(super) struct KrakenTrade {
    /// Pair as configured, uppercase (`ETH/USD`).
    pub symbol: String,
    /// Taker side; carried into raw evidence only.
    pub side: String,
    /// Decimal price, exactly as sent; the quote.
    pub price: Box<RawValue>,
    /// Decimal quantity, exactly as sent; carried into raw evidence only.
    pub qty: Box<RawValue>,
    /// The venue's monotonic per-pair trade id; the feed's `source_sequence`.
    pub trade_id: u64,
    /// RFC 3339 trade time; the feed's `source_timestamp`.
    pub timestamp: String,
}

/// One frame's envelope; `data` stays raw until the channel says how to read it.
#[derive(Deserialize)]
struct Frame {
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    method: Option<String>,
    #[serde(default)]
    success: Option<bool>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    data: Option<Box<RawValue>>,
}

impl Venue for Kraken {
    const AUTHORITY: PriceAuthority = PriceAuthority::Kraken;
    const NAME: &'static str = "Kraken";
    const ENDPOINT: &'static str = "wss://ws.kraken.com/v2";
    const SEPARATOR: Option<char> = Some('/');
    /// A subscribe-time trade snapshot is a few tens of trades; anything bigger is not this
    /// subscription.
    const MAX_FRAME_BYTES: usize = 256 * 1024;

    type Trade = KrakenTrade;

    fn subscription(markets: &[Market<Self>]) -> Subscription {
        let symbols: Vec<&str> = markets.iter().map(Market::symbol).collect();
        Subscription::Frame {
            url: Self::ENDPOINT.to_owned(),
            request: json!({
                "method": "subscribe",
                "params": { "channel": "trade", "symbol": symbols },
            })
            .to_string(),
        }
    }

    /// Heartbeats, connection status, and successful subscribe acknowledgements decode to no
    /// trades.
    fn decode(frame: &str) -> Result<Vec<KrakenTrade>> {
        let frame: Frame = serde_json::from_str(frame).context("decode Kraken frame")?;
        if let Some(method) = &frame.method {
            if frame.success == Some(false) {
                bail!(
                    "Kraken rejected {method}: {}",
                    frame.error.as_deref().unwrap_or("no error given")
                )
            }
            return Ok(Vec::new());
        }
        match frame.channel.as_deref() {
            // Both snapshot and update frames carry real trades; the engine's sequence guard
            // and the deterministic evidence identity make the snapshot replay harmless.
            Some("trade") => {
                let data = frame.data.context("Kraken trade frame missing data")?;
                serde_json::from_str(data.get()).context("decode Kraken trades")
            }
            Some("heartbeat") | Some("status") => Ok(Vec::new()),
            other => bail!("Kraken delivered unsubscribed frame {other:?}"),
        }
    }

    fn market_of(trade: &KrakenTrade) -> &str {
        &trade.symbol
    }

    fn evidence(
        market: &Market<Self>,
        trade: &KrakenTrade,
        observed_at: DateTime<Utc>,
    ) -> Result<PriceEvidence> {
        let price = PriceQuote::parse_decimal(trade.price.get()).with_context(|| {
            format!(
                "Kraken {} trade price {}",
                market.symbol(),
                trade.price.get()
            )
        })?;
        let source_timestamp = DateTime::parse_from_rfc3339(&trade.timestamp)
            .map(|timestamp| timestamp.with_timezone(&Utc))
            .with_context(|| format!("invalid Kraken trade timestamp {:?}", trade.timestamp))?;
        PriceEvidence::new(
            Self::AUTHORITY,
            market.subject().clone(),
            price,
            trade.trade_id,
            source_timestamp,
            observed_at,
            Provenance {
                endpoint: Self::ENDPOINT.to_owned(),
                method: "trade".to_owned(),
                source_id: format!("{}:{}", market.symbol(), trade.trade_id),
            },
            // Built by hand rather than serialized: converting the raw price back through a
            // JSON number would round it to f64, and the archived evidence must carry the
            // venue's exact wire text. The numeric fields travel as strings for that reason.
            json!({
                "symbol": trade.symbol,
                "side": trade.side,
                "price": trade.price.get(),
                "qty": trade.qty.get(),
                "trade_id": trade.trade_id,
                "timestamp": trade.timestamp,
            }),
        )
        .with_context(|| format!("Kraken trade {} on {}", trade.trade_id, market.symbol()))
    }
}

#[cfg(test)]
mod tests {
    use super::super::venue::harness;
    use super::*;

    fn market() -> Market<Kraken> {
        Market::new("ETH/USD").expect("market")
    }

    fn trade_frame(kind: &str, id: u64, price: &str) -> String {
        format!(
            r#"{{"channel":"trade","type":"{kind}","data":[{{"symbol":"ETH/USD","side":"buy","price":{price},"qty":0.25,"ord_type":"market","trade_id":{id},"timestamp":"2027-01-15T08:00:00.123456Z"}}]}}"#
        )
    }

    fn trade(price: &str, trade_id: u64, timestamp: &str) -> KrakenTrade {
        serde_json::from_str(&format!(
            r#"{{"symbol":"ETH/USD","side":"buy","price":{price},"qty":0.25,
                "trade_id":{trade_id},"timestamp":"{timestamp}"}}"#
        ))
        .expect("trade")
    }

    #[test]
    fn pairs_are_canonicalized_and_map_onto_market_subjects() {
        harness::canonicalizes_markets::<Kraken>(
            &["eth/usd", "ETH/USD"],
            "ETH/USD",
            "ETH-USD",
            &[
                "",
                "ETHUSD",
                "ETH/",
                "/USD",
                "ETH/US/D",
                "ETH USD",
                "ETH/US-D",
                &format!("{}/USD", "E".repeat(40)),
            ],
        );
    }

    #[test]
    fn the_subscribe_request_names_every_pair() {
        let Subscription::Frame { url, request } = Kraken::subscription(&[market()]) else {
            panic!("Kraken subscribes over the socket")
        };
        assert_eq!(url, Kraken::ENDPOINT);
        assert_eq!(
            request,
            r#"{"method":"subscribe","params":{"channel":"trade","symbol":["ETH/USD"]}}"#
        );
    }

    #[test]
    fn decodes_trades_from_updates_and_snapshots() {
        for kind in ["update", "snapshot"] {
            let trades = Kraken::decode(&trade_frame(kind, 7, "2446.02")).expect(kind);
            assert_eq!(trades.len(), 1);
            assert_eq!(trades[0].symbol, "ETH/USD");
            assert_eq!(trades[0].trade_id, 7);
            // The raw wire text survives untouched — no float in the path.
            assert_eq!(trades[0].price.get(), "2446.02");
            assert_eq!(trades[0].timestamp, "2027-01-15T08:00:00.123456Z");
        }
    }

    #[test]
    fn control_frames_decode_to_no_trades_and_failures_fail() {
        for quiet in [
            r#"{"channel":"heartbeat"}"#,
            r#"{"channel":"status","type":"update","data":[{"system":"online"}]}"#,
            r#"{"method":"subscribe","success":true,"result":{"channel":"trade","symbol":"ETH/USD"}}"#,
        ] {
            assert!(Kraken::decode(quiet).expect(quiet).is_empty());
        }
        assert!(
            Kraken::decode(r#"{"method":"subscribe","success":false,"error":"unknown pair"}"#)
                .is_err()
        );
        assert!(Kraken::decode(r#"{"channel":"ticker","data":[]}"#).is_err());
        assert!(Kraken::decode("{").is_err());
    }

    #[test]
    fn a_trade_becomes_exact_fixed_point_evidence() {
        let trade = trade("2446.02", 7, "2027-01-15T08:00:00.123456Z");
        let evidence =
            Kraken::evidence(&market(), &trade, harness::observed_at()).expect("evidence");
        assert_eq!(evidence.authority(), PriceAuthority::Kraken);
        assert_eq!(evidence.price().value(), 244_602);
        assert_eq!(evidence.price().scale(), 2);
        assert_eq!(evidence.source_sequence(), 7);
        assert_eq!(
            evidence.source_timestamp().timestamp_micros(),
            1_800_000_000_123_456
        );
        assert_eq!(evidence.provenance().endpoint, Kraken::ENDPOINT);
        assert_eq!(evidence.provenance().source_id, "ETH/USD:7");
        assert_eq!(
            evidence.raw().to_string(),
            r#"{"price":"2446.02","qty":"0.25","side":"buy","symbol":"ETH/USD","timestamp":"2027-01-15T08:00:00.123456Z","trade_id":7}"#
        );
        // Golden digest covers provider facts only, independent of registry metadata.
        assert_eq!(
            evidence.evidence_id(),
            "priceev_e3609a9ab0672d2b9e413d33ebdc940d813253ff6c0db88b645213226b965bf8"
        );
        // The identity is deterministic: the same trade redelivered — including through a
        // subscribe-time snapshot — is the same evidence.
        let again = Kraken::evidence(&market(), &trade, harness::observed_at()).expect("evidence");
        assert_eq!(again.evidence_id(), evidence.evidence_id());
    }

    #[test]
    fn high_precision_wire_numbers_survive_exactly() {
        // Past f64 precision: only the raw-text capture keeps this exact.
        let trade = trade("0.12345678901234567891", 7, "2027-01-15T08:00:00Z");
        let evidence =
            Kraken::evidence(&market(), &trade, harness::observed_at()).expect("evidence");
        assert_eq!(evidence.price().value(), 12_345_678_901_234_567_891);
        assert_eq!(evidence.price().scale(), 20);
    }

    #[test]
    fn broken_trades_fail_closed() {
        let stamp = "2027-01-15T08:00:00Z";
        for broken in [
            trade("0", 7, stamp),
            trade("-1", 7, stamp),
            trade("2.4e3", 7, stamp),
            trade("2446.02", 0, stamp),
            trade("2446.02", 7, "not-a-timestamp"),
        ] {
            assert!(
                Kraken::evidence(&market(), &broken, harness::observed_at()).is_err(),
                "{broken:?} was accepted"
            );
        }
    }

    #[tokio::test]
    async fn a_session_subscribes_then_delivers_trades() {
        let delivered = harness::delivers_replayed_frames(
            &[market()],
            &[
                trade_frame("snapshot", 1, "2446.02"),
                trade_frame("update", 2, "2446.13"),
            ],
        )
        .await;
        let ids: Vec<u64> = delivered.iter().map(|trade| trade.trade_id).collect();
        assert_eq!(ids, vec![1, 2]);
    }

    /// Live smoke test against the real endpoint; excluded from CI, run explicitly with
    /// `cargo test -p tackt-indexer live_kraken -- --ignored`.
    #[tokio::test]
    #[ignore = "requires network access to Kraken"]
    async fn live_kraken_delivers_a_normalizable_trade() {
        harness::live_trade_normalizes(&[market()]).await;
    }

    #[tokio::test]
    #[ignore = "requires network access to Kraken"]
    async fn live_kraken_jitosol_delivers_a_normalizable_trade() {
        let market = Market::<Kraken>::new("JITOSOL/USD").expect("JitoSOL market");
        harness::live_trade_normalizes(&[market]).await;
    }
}

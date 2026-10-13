//! Coinbase Exchange spot-market ingestion.
//!
//! Coinbase quotes true USD, so it balances the USDT-quoted venues in a multi-venue aggregate.
//!
//! # Feed choice
//!
//! The subscription is the `matches` channel — executed trades, each carrying the venue's
//! monotonic per-product `trade_id` (the `source_sequence`) and the venue's own RFC 3339
//! timestamp. On subscribe, Coinbase sends one `last_match` per product before live matches —
//! processed like any other trade: the deterministic evidence identity deduplicates the
//! republish and the engine's sequence guard rejects anything that does not advance. The
//! `heartbeat` channel is subscribed alongside so a thin market's quiet stretches stay inside
//! the shell's idle limit without any client keepalive.
//!
//! # Mapping onto the price-evidence contract
//!
//! | Coinbase fact | Evidence fact |
//! |---|---|
//! | `trade_id` | `source_sequence` (strictly increasing per product) |
//! | `price` (decimal string) | `PriceQuote` value and scale, exactly as sent |
//! | `time` (RFC 3339) | `source_timestamp` |
//! | the decoded match | bounded `raw` evidence |
//!
//! Coinbase product ids (`ETH-USD`) already satisfy the market-symbol charset, so the evidence
//! subject is the product id verbatim.

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;
use tackt_nats::prices::{PriceAuthority, PriceEvidence, PriceQuote};
use tackt_primitives::Provenance;

use super::venue::{Market, Subscription, Venue};

/// The Coinbase Exchange spot venue.
pub(super) struct Coinbase;

/// One executed trade as Coinbase states it. Prices and sizes are decimal strings on the wire,
/// so the shared exact-decimal parser sees exactly what the venue sent. Unknown fields are
/// tolerated: this is a provider wire that grows additively.
#[derive(Debug, Deserialize)]
pub(super) struct CoinbaseMatch {
    /// Product as configured, uppercase (`ETH-USD`).
    pub product_id: String,
    /// The venue's monotonic per-product trade id; the feed's `source_sequence`.
    pub trade_id: u64,
    /// Decimal price, exactly as sent; the quote.
    pub price: String,
    /// Decimal quantity, exactly as sent; carried into raw evidence only.
    pub size: String,
    /// Maker side; carried into raw evidence only.
    pub side: String,
    /// RFC 3339 trade time; the feed's `source_timestamp`.
    pub time: String,
}

/// One frame, discriminated by its `type` field. An unknown type fails to decode, which is
/// correct: this build subscribed exactly two channels, so anything else means the
/// subscription is not what was asked for.
#[derive(Deserialize)]
#[serde(tag = "type")]
enum Frame {
    /// A live match, or the one `last_match` snapshot Coinbase sends per product on
    /// subscribe — both carry a real trade, and the engine's sequence guard plus the
    /// deterministic evidence identity make the snapshot replay harmless.
    #[serde(rename = "match", alias = "last_match")]
    Match(CoinbaseMatch),
    #[serde(rename = "subscriptions")]
    Subscriptions {},
    #[serde(rename = "heartbeat")]
    Heartbeat {},
    #[serde(rename = "error")]
    Error {
        #[serde(default)]
        message: Option<String>,
        #[serde(default)]
        reason: Option<String>,
    },
}

impl Venue for Coinbase {
    const AUTHORITY: PriceAuthority = PriceAuthority::Coinbase;
    const NAME: &'static str = "Coinbase";
    const ENDPOINT: &'static str = "wss://ws-feed.exchange.coinbase.com";
    const SEPARATOR: Option<char> = Some('-');

    type Trade = CoinbaseMatch;

    /// Every configured product on both the `matches` and `heartbeat` channels.
    fn subscription(markets: &[Market<Self>]) -> Subscription {
        let products: Vec<&str> = markets.iter().map(Market::symbol).collect();
        Subscription::Frame {
            url: Self::ENDPOINT.to_owned(),
            request: json!({
                "type": "subscribe",
                "channels": [
                    { "name": "matches", "product_ids": products },
                    { "name": "heartbeat", "product_ids": products },
                ],
            })
            .to_string(),
        }
    }

    /// Heartbeats and subscription acknowledgements decode to no trades; a server-reported
    /// error fails the connection.
    fn decode(frame: &str) -> Result<Vec<CoinbaseMatch>> {
        match serde_json::from_str(frame).context("decode Coinbase frame")? {
            Frame::Match(matched) => Ok(vec![matched]),
            Frame::Subscriptions {} | Frame::Heartbeat {} => Ok(Vec::new()),
            Frame::Error { message, reason } => bail!(
                "Coinbase reported an error: {} ({})",
                message.as_deref().unwrap_or("no message given"),
                reason.as_deref().unwrap_or("no reason given"),
            ),
        }
    }

    fn market_of(matched: &CoinbaseMatch) -> &str {
        &matched.product_id
    }

    fn evidence(
        market: &Market<Self>,
        matched: &CoinbaseMatch,
        observed_at: DateTime<Utc>,
    ) -> Result<PriceEvidence> {
        let price = PriceQuote::parse_decimal(&matched.price).with_context(|| {
            format!("Coinbase {} match price {}", market.symbol(), matched.price)
        })?;
        let source_timestamp = DateTime::parse_from_rfc3339(&matched.time)
            .map(|timestamp| timestamp.with_timezone(&Utc))
            .with_context(|| format!("invalid Coinbase match timestamp {:?}", matched.time))?;
        PriceEvidence::new(
            Self::AUTHORITY,
            market.subject().clone(),
            price,
            matched.trade_id,
            source_timestamp,
            observed_at,
            Provenance {
                endpoint: Self::ENDPOINT.to_owned(),
                method: "matches".to_owned(),
                source_id: format!("{}:{}", market.symbol(), matched.trade_id),
            },
            // The numeric fields arrive as wire strings and travel as strings, so the archived
            // evidence carries the venue's exact text with no float in the path.
            json!({
                "product_id": matched.product_id,
                "trade_id": matched.trade_id,
                "price": matched.price,
                "size": matched.size,
                "side": matched.side,
                "time": matched.time,
            }),
        )
        .with_context(|| format!("Coinbase match {} on {}", matched.trade_id, market.symbol()))
    }
}

#[cfg(test)]
mod tests {
    use super::super::venue::harness;
    use super::*;

    fn market() -> Market<Coinbase> {
        Market::new("ETH-USD").expect("market")
    }

    fn match_frame(kind: &str, id: u64, price: &str) -> String {
        format!(
            r#"{{"type":"{kind}","trade_id":{id},"maker_order_id":"a","taker_order_id":"b","side":"sell","size":"0.25","price":"{price}","product_id":"ETH-USD","sequence":100,"time":"2027-01-15T08:00:00.123456Z"}}"#
        )
    }

    fn matched(price: &str, trade_id: u64, time: &str) -> CoinbaseMatch {
        serde_json::from_str(&format!(
            r#"{{"product_id":"ETH-USD","trade_id":{trade_id},"price":"{price}",
                "size":"0.25","side":"sell","time":"{time}"}}"#
        ))
        .expect("match")
    }

    #[test]
    fn products_are_canonicalized_and_map_onto_market_subjects() {
        harness::canonicalizes_markets::<Coinbase>(
            &["eth-usd", "ETH-USD"],
            "ETH-USD",
            "ETH-USD",
            &[
                "",
                "ETHUSD",
                "ETH-",
                "-USD",
                "ETH-US-D",
                "ETH USD",
                "ETH-US/D",
                &format!("{}-USD", "E".repeat(40)),
            ],
        );
    }

    #[test]
    fn the_subscribe_request_names_every_product_on_both_channels() {
        let Subscription::Frame { url, request } = Coinbase::subscription(&[market()]) else {
            panic!("Coinbase subscribes over the socket")
        };
        assert_eq!(url, Coinbase::ENDPOINT);
        assert_eq!(
            request,
            r#"{"channels":[{"name":"matches","product_ids":["ETH-USD"]},{"name":"heartbeat","product_ids":["ETH-USD"]}],"type":"subscribe"}"#
        );
    }

    #[test]
    fn decodes_matches_from_updates_and_snapshots() {
        for kind in ["match", "last_match"] {
            let matches = Coinbase::decode(&match_frame(kind, 7, "2446.02")).expect(kind);
            assert_eq!(matches.len(), 1);
            assert_eq!(matches[0].product_id, "ETH-USD");
            assert_eq!(matches[0].trade_id, 7);
            assert_eq!(matches[0].price, "2446.02");
            assert_eq!(matches[0].time, "2027-01-15T08:00:00.123456Z");
        }
    }

    #[test]
    fn control_frames_decode_to_no_match_and_failures_fail() {
        for quiet in [
            r#"{"type":"subscriptions","channels":[{"name":"matches","product_ids":["ETH-USD"]}]}"#,
            r#"{"type":"heartbeat","sequence":90,"last_trade_id":20,"product_id":"ETH-USD","time":"2027-01-15T08:00:00.000000Z"}"#,
        ] {
            assert!(Coinbase::decode(quiet).expect(quiet).is_empty());
        }
        assert!(Coinbase::decode(r#"{"type":"error","message":"Failed to subscribe"}"#).is_err());
        assert!(Coinbase::decode(r#"{"type":"ticker","price":"1"}"#).is_err());
        assert!(Coinbase::decode("{").is_err());
    }

    #[test]
    fn a_match_becomes_exact_fixed_point_evidence() {
        let matched = matched("2446.02", 7, "2027-01-15T08:00:00.123456Z");
        let evidence =
            Coinbase::evidence(&market(), &matched, harness::observed_at()).expect("evidence");
        assert_eq!(evidence.authority(), PriceAuthority::Coinbase);
        assert_eq!(evidence.price().value(), 244_602);
        assert_eq!(evidence.price().scale(), 2);
        assert_eq!(evidence.source_sequence(), 7);
        assert_eq!(
            evidence.source_timestamp().timestamp_micros(),
            1_800_000_000_123_456
        );
        assert_eq!(evidence.provenance().endpoint, Coinbase::ENDPOINT);
        assert_eq!(evidence.provenance().source_id, "ETH-USD:7");
        assert_eq!(
            evidence.raw().to_string(),
            r#"{"price":"2446.02","product_id":"ETH-USD","side":"sell","size":"0.25","time":"2027-01-15T08:00:00.123456Z","trade_id":7}"#
        );
        // Golden digest covers provider facts only, independent of registry metadata.
        assert_eq!(
            evidence.evidence_id(),
            "priceev_d2643bd61b4f96d431d2504cc457fb3ff8c3a28437fc3026e279331aa47cb7ce"
        );
        // The identity is deterministic: the same match redelivered — including through a
        // subscribe-time `last_match` — is the same evidence.
        let again =
            Coinbase::evidence(&market(), &matched, harness::observed_at()).expect("evidence");
        assert_eq!(again.evidence_id(), evidence.evidence_id());
    }

    #[test]
    fn broken_matches_fail_closed() {
        let stamp = "2027-01-15T08:00:00Z";
        for broken in [
            matched("0", 7, stamp),
            matched("-1", 7, stamp),
            matched("2.4e3", 7, stamp),
            matched("2446.02", 0, stamp),
            matched("2446.02", 7, "not-a-timestamp"),
        ] {
            assert!(
                Coinbase::evidence(&market(), &broken, harness::observed_at()).is_err(),
                "{broken:?} was accepted"
            );
        }
    }

    #[tokio::test]
    async fn a_session_subscribes_then_delivers_matches() {
        let delivered = harness::delivers_replayed_frames(
            &[market()],
            &[
                match_frame("last_match", 1, "2446.02"),
                match_frame("match", 2, "2446.13"),
            ],
        )
        .await;
        let ids: Vec<u64> = delivered.iter().map(|matched| matched.trade_id).collect();
        assert_eq!(ids, vec![1, 2]);
    }

    /// Live smoke test against the real endpoint; excluded from CI, run explicitly with
    /// `cargo test -p tackt-indexer live_coinbase -- --ignored`.
    #[tokio::test]
    #[ignore = "requires network access to Coinbase"]
    async fn live_coinbase_delivers_a_normalizable_match() {
        harness::live_trade_normalizes(&[market()]).await;
    }
}

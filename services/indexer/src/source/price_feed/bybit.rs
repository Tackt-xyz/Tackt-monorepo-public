//! Bybit spot-market ingestion.
//!
//! # Feed choice
//!
//! The subscription is the `publicTrade` topic. There is no subscribe-time snapshot — the
//! first trade after connecting is live.
//!
//! # Keepalive
//!
//! Bybit expects a client heartbeat every 20 seconds, so the venue sets the shared shell's
//! scheduled client keepalive: an `{"op":"ping"}` frame on that cadence regardless of inbound
//! traffic, answered by an `op` acknowledgement frame.
//!
//! # Sequence choice
//!
//! Bybit spot trade ids are UUIDs, not integers, so they cannot be the numeric
//! `source_sequence`. The venue's millisecond trade timestamp is used instead. Consequences,
//! both acceptable for a current-price feed: several trades in one millisecond advance the
//! aggregation table at most once (later same-millisecond trades are rejected as
//! non-advancing), and two same-millisecond trades at the same price collapse onto one
//! evidence identity — which the identity's price and timestamp components make a genuine
//! duplicate opinion, not a collision. The UUID is preserved in provenance and raw evidence.
//!
//! # Mapping onto the price-evidence contract
//!
//! | Bybit fact | Evidence fact |
//! |---|---|
//! | `T` (trade time, epoch milliseconds) | `source_sequence` and `source_timestamp` |
//! | `p` (decimal string) | `PriceQuote` value and scale, exactly as sent |
//! | `i` (UUID trade id) | provenance `source_id` and raw evidence only |
//! | the decoded trade | bounded `raw` evidence |

use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;
use serde_json::value::RawValue;
use tackt_nats::prices::{PriceAuthority, PriceEvidence, PriceQuote};
use tackt_primitives::Provenance;

use super::venue::{Market, Subscription, Venue};
use super::ws::ClientPing;

/// The Bybit v5 spot venue.
pub(super) struct Bybit;

/// One trade as Bybit states it. Prices and sizes are decimal strings on the wire, so the
/// shared exact-decimal parser sees exactly what the venue sent. Unknown fields are tolerated:
/// this is a provider wire that grows additively.
#[derive(Debug, Deserialize)]
pub(super) struct BybitTrade {
    /// Symbol as configured, uppercase (`ETHUSDT`).
    #[serde(rename = "s")]
    pub symbol: String,
    /// The venue's UUID trade id; provenance only — see the module docs' sequence choice.
    #[serde(rename = "i")]
    pub trade_id: String,
    /// Decimal price, exactly as sent; the quote.
    #[serde(rename = "p")]
    pub price: String,
    /// Decimal quantity, exactly as sent; carried into raw evidence only.
    #[serde(rename = "v")]
    pub volume: String,
    /// Taker side; carried into raw evidence only.
    #[serde(rename = "S")]
    pub side: String,
    /// Trade time in milliseconds since the Unix epoch; the feed's `source_sequence` and
    /// `source_timestamp`.
    #[serde(rename = "T")]
    pub timestamp_ms: i64,
}

/// One frame's envelope; `data` stays raw until the topic says how to read it.
#[derive(Deserialize)]
struct Frame {
    #[serde(default)]
    topic: Option<String>,
    #[serde(default)]
    op: Option<String>,
    #[serde(default)]
    success: Option<bool>,
    #[serde(default)]
    ret_msg: Option<String>,
    #[serde(default)]
    data: Option<Box<RawValue>>,
}

impl Venue for Bybit {
    const AUTHORITY: PriceAuthority = PriceAuthority::Bybit;
    const NAME: &'static str = "Bybit";
    const ENDPOINT: &'static str = "wss://stream.bybit.com/v5/public/spot";
    const SEPARATOR: Option<char> = None;
    /// A push frame is a handful of trades; anything bigger is not this subscription.
    const MAX_FRAME_BYTES: usize = 256 * 1024;
    const CLIENT_PING: Option<ClientPing> = Some(ClientPing {
        interval: Duration::from_secs(20),
        payload: r#"{"op":"ping"}"#,
    });

    type Trade = BybitTrade;

    fn subscription(markets: &[Market<Self>]) -> Subscription {
        let topics: Vec<String> = markets
            .iter()
            .map(|market| format!("publicTrade.{}", market.symbol()))
            .collect();
        Subscription::Frame {
            url: Self::ENDPOINT.to_owned(),
            request: json!({ "op": "subscribe", "args": topics }).to_string(),
        }
    }

    /// Keepalive acknowledgements and successful subscribe acknowledgements decode to no
    /// trades.
    fn decode(frame: &str) -> Result<Vec<BybitTrade>> {
        let frame: Frame = serde_json::from_str(frame).context("decode Bybit frame")?;
        if let Some(op) = &frame.op {
            if frame.success == Some(false) {
                bail!(
                    "Bybit rejected {op}: {}",
                    frame.ret_msg.as_deref().unwrap_or("no message given")
                )
            }
            return Ok(Vec::new());
        }
        match frame.topic.as_deref() {
            Some(topic) if topic.starts_with("publicTrade.") => {
                let data = frame.data.context("Bybit trade frame missing data")?;
                serde_json::from_str(data.get()).context("decode Bybit trades")
            }
            other => bail!("Bybit delivered unsubscribed frame {other:?}"),
        }
    }

    fn market_of(trade: &BybitTrade) -> &str {
        &trade.symbol
    }

    fn evidence(
        market: &Market<Self>,
        trade: &BybitTrade,
        observed_at: DateTime<Utc>,
    ) -> Result<PriceEvidence> {
        let price = PriceQuote::parse_decimal(&trade.price)
            .with_context(|| format!("Bybit {} trade price {}", market.symbol(), trade.price))?;
        let source_sequence = u64::try_from(trade.timestamp_ms).with_context(|| {
            format!(
                "Bybit trade timestamp {} is not positive",
                trade.timestamp_ms
            )
        })?;
        let source_timestamp =
            DateTime::from_timestamp_millis(trade.timestamp_ms).with_context(|| {
                format!(
                    "Bybit trade timestamp {} is out of range",
                    trade.timestamp_ms
                )
            })?;
        PriceEvidence::new(
            Self::AUTHORITY,
            market.subject().clone(),
            price,
            source_sequence,
            source_timestamp,
            observed_at,
            Provenance {
                endpoint: Self::ENDPOINT.to_owned(),
                method: "publicTrade".to_owned(),
                source_id: format!("{}:{}", market.symbol(), trade.trade_id),
            },
            // The numeric fields arrive as wire strings and travel as strings, so the archived
            // evidence carries the venue's exact text with no float in the path.
            json!({
                "s": trade.symbol,
                "i": trade.trade_id,
                "p": trade.price,
                "v": trade.volume,
                "S": trade.side,
                "T": trade.timestamp_ms,
            }),
        )
        .with_context(|| format!("Bybit trade {} on {}", trade.trade_id, market.symbol()))
    }
}

#[cfg(test)]
mod tests {
    use super::super::venue::harness;
    use super::*;

    fn market() -> Market<Bybit> {
        Market::new("ETHUSDT").expect("market")
    }

    fn trade_frame(id: &str, price: &str) -> String {
        format!(
            r#"{{"topic":"publicTrade.ETHUSDT","type":"snapshot","ts":1798000000870,"data":[{{"i":"{id}","T":1798000000865,"p":"{price}","v":"0.25","S":"Buy","s":"ETHUSDT","BT":false}}]}}"#
        )
    }

    fn trade(price: &str, trade_id: &str, timestamp_ms: i64) -> BybitTrade {
        serde_json::from_str(&format!(
            r#"{{"s":"ETHUSDT","i":"{trade_id}","p":"{price}","v":"0.25",
                "S":"Buy","T":{timestamp_ms}}}"#
        ))
        .expect("trade")
    }

    #[test]
    fn symbols_are_canonicalized_and_map_onto_market_subjects() {
        harness::canonicalizes_markets::<Bybit>(
            &["ethusdt", "ETHUSDT"],
            "ETHUSDT",
            "ETHUSDT",
            &["", "ETH-USDT", "ETH USDT", &"E".repeat(33)],
        );
    }

    #[test]
    fn the_subscribe_request_names_every_topic() {
        let Subscription::Frame { url, request } = Bybit::subscription(&[market()]) else {
            panic!("Bybit subscribes over the socket")
        };
        assert_eq!(url, Bybit::ENDPOINT);
        assert_eq!(
            request,
            r#"{"args":["publicTrade.ETHUSDT"],"op":"subscribe"}"#
        );
    }

    #[test]
    fn decodes_trades_and_keeps_wire_text_exact() {
        let trades = Bybit::decode(&trade_frame(
            "20f43950-d8dd-5b31-9112-a178eb6023af",
            "2446.02",
        ))
        .expect("trades");
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].symbol, "ETHUSDT");
        assert_eq!(trades[0].trade_id, "20f43950-d8dd-5b31-9112-a178eb6023af");
        assert_eq!(trades[0].price, "2446.02");
        assert_eq!(trades[0].timestamp_ms, 1_798_000_000_865);
    }

    #[test]
    fn control_frames_decode_to_no_trades_and_failures_fail() {
        for quiet in [
            r#"{"success":true,"ret_msg":"subscribe","conn_id":"a","op":"subscribe"}"#,
            r#"{"success":true,"ret_msg":"pong","conn_id":"a","op":"ping"}"#,
        ] {
            assert!(Bybit::decode(quiet).expect(quiet).is_empty());
        }
        assert!(
            Bybit::decode(r#"{"success":false,"ret_msg":"Invalid symbol","op":"subscribe"}"#)
                .is_err()
        );
        assert!(Bybit::decode(r#"{"topic":"tickers.ETHUSDT","data":{}}"#).is_err());
        assert!(Bybit::decode("{").is_err());
    }

    #[test]
    fn a_trade_becomes_exact_fixed_point_evidence() {
        let trade = trade(
            "2446.02",
            "20f43950-d8dd-5b31-9112-a178eb6023af",
            1_800_000_000_123,
        );
        let evidence =
            Bybit::evidence(&market(), &trade, harness::observed_at()).expect("evidence");
        assert_eq!(evidence.authority(), PriceAuthority::Bybit);
        assert_eq!(evidence.price().value(), 244_602);
        assert_eq!(evidence.price().scale(), 2);
        assert_eq!(evidence.source_sequence(), 1_800_000_000_123);
        assert_eq!(
            evidence.source_timestamp().timestamp_millis(),
            1_800_000_000_123
        );
        assert_eq!(
            evidence.provenance().source_id,
            "ETHUSDT:20f43950-d8dd-5b31-9112-a178eb6023af"
        );
        assert_eq!(evidence.provenance().endpoint, Bybit::ENDPOINT);
        assert_eq!(
            evidence.raw().to_string(),
            r#"{"S":"Buy","T":1800000000123,"i":"20f43950-d8dd-5b31-9112-a178eb6023af","p":"2446.02","s":"ETHUSDT","v":"0.25"}"#
        );
        // Golden digest covers provider facts only, independent of registry metadata.
        assert_eq!(
            evidence.evidence_id(),
            "priceev_57576e855ae85aca445bb3bcf1343b3c4fb98d74b82462d3af0ca217e9d486f1"
        );
        // The identity is deterministic: the same trade redelivered is the same evidence.
        let again = Bybit::evidence(&market(), &trade, harness::observed_at()).expect("evidence");
        assert_eq!(again.evidence_id(), evidence.evidence_id());
    }

    #[test]
    fn same_millisecond_trades_at_distinct_prices_keep_distinct_identities() {
        let first = Bybit::evidence(
            &market(),
            &trade("2446.02", "aa", 1_800_000_000_123),
            harness::observed_at(),
        )
        .expect("evidence");
        let second = Bybit::evidence(
            &market(),
            &trade("2446.13", "bb", 1_800_000_000_123),
            harness::observed_at(),
        )
        .expect("evidence");
        assert_ne!(first.evidence_id(), second.evidence_id());
    }

    #[test]
    fn broken_trades_fail_closed() {
        for broken in [
            trade("0", "aa", 1_800_000_000_123),
            trade("-1", "aa", 1_800_000_000_123),
            trade("2.4e3", "aa", 1_800_000_000_123),
            trade("2446.02", "aa", 0),
            trade("2446.02", "aa", -5),
        ] {
            assert!(
                Bybit::evidence(&market(), &broken, harness::observed_at()).is_err(),
                "{broken:?} was accepted"
            );
        }
    }

    #[tokio::test]
    async fn a_session_subscribes_then_delivers_trades() {
        let delivered = harness::delivers_replayed_frames(
            &[market()],
            &[trade_frame("aa", "2446.02"), trade_frame("bb", "2446.13")],
        )
        .await;
        let ids: Vec<&str> = delivered
            .iter()
            .map(|trade| trade.trade_id.as_str())
            .collect();
        assert_eq!(ids, vec!["aa", "bb"]);
    }

    /// Live smoke test against the real endpoint; excluded from CI, run explicitly with
    /// `cargo test -p tackt-indexer live_bybit -- --ignored`.
    #[tokio::test]
    #[ignore = "requires network access to Bybit"]
    async fn live_bybit_delivers_a_normalizable_trade() {
        harness::live_trade_normalizes(&[market()]).await;
    }
}

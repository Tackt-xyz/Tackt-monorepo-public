//! Binance spot-market ingestion.
//!
//! # Feed choice
//!
//! The subscription is per-symbol `@aggTrade` (aggregate trades). It is the one public spot
//! stream that satisfies the evidence contract on its own terms: each message carries the
//! venue's monotonic aggregate-trade id and the venue's own trade timestamp. `bookTicker`
//! (best bid/ask) has no event timestamp, so a mid-price from it would need our receive clock
//! as `source_timestamp` — local time wearing a provider claim — and is deliberately not used.
//!
//! # Mapping onto the price-evidence contract
//!
//! | Binance fact | Evidence fact |
//! |---|---|
//! | `a` (aggregate trade id) | `source_sequence` (strictly increasing per symbol) |
//! | `p` (decimal price string) | `PriceQuote` value and scale, exactly as quoted |
//! | `T` (trade time, ms) | `source_timestamp` |
//! | the decoded trade message | bounded `raw` evidence |
//!
//! # Connection model
//!
//! One combined-stream connection carries every configured symbol
//! (`wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/...`); streams are named in
//! the URL, so no subscribe handshake exists to race. Binance pings the client and closes
//! connections after 24 hours; the supervisor must answer pings promptly and treat the daily
//! close as a routine reconnect. Missed trades during a reconnect are not backfilled — this is
//! a current-price feed, and the aggregation engine's staleness decay covers the gap.
//! Binance.com geo-blocks US IPs; where the indexer runs decides whether this source can be
//! configured at all.

use anyhow::{Context, Result, bail};
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use tackt_nats::prices::{PriceAuthority, PriceEvidence, PriceQuote};
use tackt_primitives::Provenance;

use super::venue::{Market, Subscription, Venue};

/// The Binance spot venue.
pub(super) struct Binance;

/// One aggregate trade as Binance states it.
///
/// Unknown fields are tolerated: this is a provider wire that grows additively, and the fields
/// below are the ones that carry evidence. Everything semantic — price syntax, positivity,
/// timestamp sanity — is checked in [`Venue::evidence`], not here.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct AggTrade {
    /// Symbol as configured, uppercase (`BTCUSDT`).
    #[serde(rename = "s")]
    pub symbol: String,
    /// The venue's monotonic aggregate-trade id; the feed's `source_sequence`.
    #[serde(rename = "a")]
    pub aggregate_trade_id: u64,
    /// Decimal price string, exactly as quoted.
    #[serde(rename = "p")]
    pub price: String,
    /// Decimal quantity string; carried into raw evidence, never into the quote.
    #[serde(rename = "q")]
    pub quantity: String,
    /// Trade time in milliseconds; the feed's `source_timestamp`.
    #[serde(rename = "T")]
    pub trade_time_ms: i64,
}

#[derive(Deserialize)]
struct CombinedFrame {
    stream: String,
    data: EventFrame,
}

#[derive(Deserialize)]
struct EventFrame {
    #[serde(rename = "e")]
    event: String,
    #[serde(flatten)]
    trade: AggTrade,
}

/// The stream name for the combined-stream URL (`btcusdt@aggTrade`), and the provenance method
/// naming it.
fn stream_name(market: &Market<Binance>) -> String {
    format!("{}@aggTrade", market.symbol().to_ascii_lowercase())
}

impl Venue for Binance {
    const AUTHORITY: PriceAuthority = PriceAuthority::Binance;
    const NAME: &'static str = "Binance";
    const ENDPOINT: &'static str = "wss://stream.binance.com:9443/stream";
    const SEPARATOR: Option<char> = None;

    type Trade = AggTrade;

    /// The combined-stream URL naming every configured stream, so a reconnect re-establishes
    /// the complete subscription with no handshake to race.
    fn subscription(markets: &[Market<Self>]) -> Subscription {
        let streams: Vec<String> = markets.iter().map(stream_name).collect();
        Subscription::InUrl(format!("{}?streams={}", Self::ENDPOINT, streams.join("/")))
    }

    /// Fails on an event type other than `aggTrade` — the combined URL subscribes only trade
    /// streams, so anything else means the subscription is not what this build asked for.
    fn decode(frame: &str) -> Result<Vec<AggTrade>> {
        let combined: CombinedFrame =
            serde_json::from_str(frame).context("decode Binance combined-stream frame")?;
        if combined.data.event != "aggTrade" {
            bail!(
                "Binance stream {} delivered unexpected event {:?}",
                combined.stream,
                combined.data.event
            )
        }
        Ok(vec![combined.data.trade])
    }

    fn market_of(trade: &AggTrade) -> &str {
        &trade.symbol
    }

    fn evidence(
        market: &Market<Self>,
        trade: &AggTrade,
        observed_at: DateTime<Utc>,
    ) -> Result<PriceEvidence> {
        let price = PriceQuote::parse_decimal(&trade.price).with_context(|| {
            format!("Binance {} trade price {:?}", market.symbol(), trade.price)
        })?;
        let source_timestamp = Utc
            .timestamp_millis_opt(trade.trade_time_ms)
            .single()
            .with_context(|| {
                format!("unrepresentable Binance trade time {}", trade.trade_time_ms)
            })?;
        PriceEvidence::new(
            Self::AUTHORITY,
            market.subject().clone(),
            price,
            trade.aggregate_trade_id,
            source_timestamp,
            observed_at,
            Provenance {
                endpoint: Self::ENDPOINT.to_owned(),
                method: stream_name(market),
                source_id: format!("{}:{}", market.symbol(), trade.aggregate_trade_id),
            },
            serde_json::to_value(trade).context("encode Binance trade as raw evidence")?,
        )
        .with_context(|| {
            format!(
                "Binance trade {} on {}",
                trade.aggregate_trade_id,
                market.symbol()
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::venue::harness;
    use super::*;

    fn market() -> Market<Binance> {
        Market::new("BTCUSDT").expect("market")
    }

    fn trade_frame(id: u64) -> String {
        format!(
            r#"{{"stream":"btcusdt@aggTrade","data":{{"e":"aggTrade","E":1,"s":"BTCUSDT","a":{id},"p":"65000.10","q":"0.002","T":1672515782134}}}}"#
        )
    }

    fn trade() -> AggTrade {
        AggTrade {
            symbol: "BTCUSDT".to_string(),
            aggregate_trade_id: 12345,
            price: "65000.10".to_string(),
            quantity: "0.002".to_string(),
            trade_time_ms: 1_800_000_000_000,
        }
    }

    #[test]
    fn symbols_are_canonicalized_and_map_onto_market_subjects() {
        harness::canonicalizes_markets::<Binance>(
            &["btcUsdt", "BTCUSDT"],
            "BTCUSDT",
            "BTCUSDT",
            &["", "BTC-USDT", "BTC USDT", "btc/usdt", &"B".repeat(33)],
        );
    }

    #[test]
    fn decodes_an_aggregate_trade_frame() {
        assert_eq!(
            Binance::decode(&trade_frame(12345)).expect("trade frame"),
            vec![AggTrade {
                symbol: "BTCUSDT".to_string(),
                aggregate_trade_id: 12345,
                price: "65000.10".to_string(),
                quantity: "0.002".to_string(),
                trade_time_ms: 1_672_515_782_134,
            }]
        );
    }

    #[test]
    fn refuses_frames_that_are_not_aggregate_trades() {
        let ticker = r#"{
            "stream": "btcusdt@bookTicker",
            "data": {"e": "bookTicker", "s": "BTCUSDT", "b": "1", "B": "1", "a": "2", "A": "1"}
        }"#;
        assert!(Binance::decode(ticker).is_err());
        assert!(Binance::decode("{").is_err());
    }

    #[test]
    fn the_url_names_every_stream() {
        let Subscription::InUrl(url) =
            Binance::subscription(&[market(), Market::new("ETHUSDT").expect("market")])
        else {
            panic!("Binance names its streams in the URL")
        };
        // The endpoint's path matters: without one, the `@` in the stream names would parse as
        // a userinfo delimiter, exactly as it would against the real endpoint.
        assert_eq!(
            url,
            "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade"
        );
    }

    #[test]
    fn a_trade_becomes_exact_fixed_point_evidence() {
        let evidence =
            Binance::evidence(&market(), &trade(), harness::observed_at()).expect("evidence");
        assert_eq!(evidence.authority(), PriceAuthority::Binance);
        assert_eq!(evidence.price().value(), 6_500_010);
        assert_eq!(evidence.price().scale(), 2);
        assert_eq!(evidence.source_sequence(), 12345);
        assert_eq!(
            evidence.source_timestamp().timestamp_millis(),
            1_800_000_000_000
        );
        assert_eq!(evidence.provenance().endpoint, Binance::ENDPOINT);
        assert_eq!(evidence.provenance().source_id, "BTCUSDT:12345");
        assert_eq!(evidence.provenance().method, "btcusdt@aggTrade");
        assert_eq!(
            evidence.raw().to_string(),
            r#"{"T":1800000000000,"a":12345,"p":"65000.10","q":"0.002","s":"BTCUSDT"}"#
        );
        // Golden digest covers provider facts only, independent of registry metadata.
        assert_eq!(
            evidence.evidence_id(),
            "priceev_b7774771a71aaf480c0795117874ae849c582156a66c33a67f0e29bf3946f2f9"
        );
        // The identity is deterministic: the same trade redelivered is the same evidence.
        let again =
            Binance::evidence(&market(), &trade(), harness::observed_at()).expect("evidence");
        assert_eq!(again.evidence_id(), evidence.evidence_id());
    }

    #[test]
    fn broken_trades_fail_closed() {
        for broken in [
            AggTrade {
                price: "0.00".to_string(),
                ..trade()
            },
            AggTrade {
                price: "-1".to_string(),
                ..trade()
            },
            AggTrade {
                price: "1e3".to_string(),
                ..trade()
            },
            AggTrade {
                price: String::new(),
                ..trade()
            },
            AggTrade {
                aggregate_trade_id: 0,
                ..trade()
            },
            AggTrade {
                trade_time_ms: i64::MAX,
                ..trade()
            },
        ] {
            assert!(
                Binance::evidence(&market(), &broken, harness::observed_at()).is_err(),
                "{broken:?} was accepted"
            );
        }
    }

    #[tokio::test]
    async fn a_session_decodes_and_delivers_in_order() {
        let delivered =
            harness::delivers_replayed_frames(&[market()], &[trade_frame(1), trade_frame(2)]).await;
        let ids: Vec<u64> = delivered
            .iter()
            .map(|trade| trade.aggregate_trade_id)
            .collect();
        assert_eq!(ids, vec![1, 2]);
    }

    /// Live smoke test against the real endpoint; excluded from CI, run explicitly with
    /// `cargo test -p tackt-indexer live_binance -- --ignored`.
    #[tokio::test]
    #[ignore = "requires network access to Binance"]
    async fn live_binance_delivers_a_normalizable_trade() {
        harness::live_trade_normalizes(&[market()]).await;
    }
}

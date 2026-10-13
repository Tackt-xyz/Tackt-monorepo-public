//! Price-feed ingestion boundary.
//!
//! [`aggregation`] holds the deterministic aggregation engine; `venue` holds what a price venue
//! is — the contract `binance`, `kraken`, `bybit`, and `coinbase` each fill in — over the shared
//! reconnecting WebSocket shell in `ws`. [`run`] wires them together: every decoded trade
//! becomes typed price evidence, the evidence is published through the acknowledged JetStream
//! contract, and only then does it reach the one engine shared by every venue — an asset
//! configured on several venues is aggregated across them.
//!
//! Failure severity is deliberately split three ways. A publish failure is a transport failure:
//! it fails the venue's connection cycle so the shell's backoff throttles ingestion until the
//! durable handoff recovers. A trade that cannot become evidence — or arrives for a symbol
//! nobody configured — is a data failure: it is logged and dropped, because tearing the
//! connection down would silence the venue's healthy markets and reconnecting cannot fix bad
//! data. An engine rejection after the publish is not a failure at all: the evidence is
//! durable, the table simply did not advance.
//!
//! Aggregation policy (weights, freshness windows, quorum, deviation) is interim service
//! configuration below; moving it into the locked asset registry is the accepted follow-up so
//! the policy shares the registry's version and digest. Until the registry also carries each
//! source's quote currency, markets quoted in USD, USDC, and USDT are aggregated at par —
//! a documented simplification, with stablecoin depegs handled by the deviation guard's
//! fail-closed majority rule rather than priced in.

pub mod aggregation;
mod binance;
mod bybit;
mod coinbase;
mod kraken;
mod venue;
mod ws;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use tackt_nats::NatsClient;
use tackt_nats::jetstream::StreamSpec;
use tackt_nats::prices::{PriceAuthority, PriceEvidence, PriceSubject};
use tackt_primitives::ledger::LedgerAsset;

use aggregation::{
    AggregationConfig, AggregationEngine, AggregationError, AssetAggregationConfig,
    PriceSourceConfig, WEIGHT_TOTAL_BPS,
};
use binance::Binance;
use bybit::Bybit;
use coinbase::Coinbase;
use kraken::Kraken;
use venue::{Market, Venue};

/// Interim per-source policy, until the asset registry carries it per asset. An asset's total
/// weight is split evenly across its configured sources, the remainder going to the first.
///
/// A trade stream earns full weight only while trades actually arrive, so the full-weight
/// window is generous enough for a thin market's gaps and the staleness cutoff bounds how old
/// a quiet market's last trade may get before the price goes unavailable. The quorum keeps a
/// two-venue asset priced while either venue is healthy; with more sources per asset it must
/// be revisited, which the registry move will force anyway.
const FULL_WEIGHT_WITHIN: Duration = Duration::from_secs(30);
const MAX_STALENESS: Duration = Duration::from_secs(5 * 60);
const MIN_QUORUM_WEIGHT_BPS: u16 = 2_500;
const MAX_DEVIATION_BPS: u32 = 500;

/// Fastest one asset may publish: the first update after quiet publishes immediately, and a
/// burst batches into at most one composite per interval.
const AGGREGATE_MIN_PUBLISH_INTERVAL: Duration = Duration::from_millis(100);
/// Unconditional republish cadence when nothing changes — the heartbeat that separates a
/// stable price from a stalled publisher.
const AGGREGATE_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

/// One priced asset and the venue market that prices it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VenueMarket {
    pub asset: LedgerAsset,
    pub market: String,
}

/// Every venue's configured markets; an unconfigured venue is an empty list.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PriceMarkets {
    pub binance: Vec<VenueMarket>,
    pub kraken: Vec<VenueMarket>,
    pub bybit: Vec<VenueMarket>,
    pub coinbase: Vec<VenueMarket>,
}

impl PriceMarkets {
    /// Whether no venue configures any market — the state in which the price pipeline must
    /// not start.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.binance.is_empty()
            && self.kraken.is_empty()
            && self.bybit.is_empty()
            && self.coinbase.is_empty()
    }

    /// The default market set: BTC, ETH, and SOL on every venue, plus JitoSOL/USD on Kraken
    /// as a single-source demo feed. USDC is deliberately absent — it is the base asset,
    /// valued at par, and needs no feed. `defaults_form_a_valid_aggregation_plan` proves every
    /// spelling against its venue's validator.
    #[must_use]
    pub fn defaults() -> Self {
        fn markets(assets_to_markets: &[(LedgerAsset, &str)]) -> Vec<VenueMarket> {
            assets_to_markets
                .iter()
                .map(|&(asset, market)| VenueMarket {
                    asset,
                    market: market.to_owned(),
                })
                .collect()
        }
        Self {
            binance: markets(&[
                (LedgerAsset::Btc, "BTCUSDT"),
                (LedgerAsset::Eth, "ETHUSDT"),
                (LedgerAsset::Sol, "SOLUSDT"),
            ]),
            kraken: markets(&[
                (LedgerAsset::Btc, "BTC/USD"),
                (LedgerAsset::Eth, "ETH/USD"),
                (LedgerAsset::Sol, "SOL/USD"),
                (LedgerAsset::Jitosol, "JITOSOL/USD"),
            ]),
            bybit: markets(&[
                (LedgerAsset::Btc, "BTCUSDT"),
                (LedgerAsset::Eth, "ETHUSDT"),
                (LedgerAsset::Sol, "SOLUSDT"),
            ]),
            coinbase: markets(&[
                (LedgerAsset::Btc, "BTC-USD"),
                (LedgerAsset::Eth, "ETH-USD"),
                (LedgerAsset::Sol, "SOL-USD"),
            ]),
        }
    }
}

/// Everything one delivered trade needs: the durable handoff and the shared engine.
struct Delivery {
    events: Arc<NatsClient>,
    stream: StreamSpec,
    aggregate_stream: StreamSpec,
    engine: Mutex<AggregationEngine>,
    /// Assets whose table advanced since the publisher last looked, and its wake signal, so a
    /// fresh update publishes immediately instead of waiting for the heartbeat.
    dirty: Mutex<HashSet<LedgerAsset>>,
    wake: tokio::sync::Notify,
}

impl Delivery {
    /// Publishes one trade's evidence durably, then records it as the source's latest opinion.
    ///
    /// The acknowledged publish is the price path's durable boundary: a publish failure fails
    /// the delivery, and the connection shell's backoff throttles ingestion until the handoff
    /// recovers. Past the publish, an engine rejection (a redelivered or reordered trade) is
    /// not a transport failure and must not tear down the connection.
    async fn record(&self, evidence: &PriceEvidence) -> Result<()> {
        let published = self
            .events
            .publish_price_evidence(&self.stream, evidence)
            .await;
        if published.is_err() {
            metrics::counter!("indexer_price_evidence_publish_failures_total").increment(1);
        }
        published.context("price-evidence publish failed")?;
        metrics::counter!("indexer_price_evidence_published_total").increment(1);
        let accepted = self
            .engine
            .lock()
            .expect("aggregation engine lock")
            .accept(evidence);
        match accepted {
            Ok(aggregation::Accepted::Updated { asset }) => {
                self.dirty.lock().expect("dirty asset lock").insert(asset);
                self.wake.notify_one();
            }
            Ok(aggregation::Accepted::Duplicate { .. }) => {}
            Err(rejection) => {
                tracing::debug!(%rejection, "trade did not advance the aggregation table");
            }
        }
        Ok(())
    }

    /// Records one venue trade against its configured market. A trade for a market nobody
    /// configured, and one that cannot become evidence, are both data failures: logged, dropped,
    /// and never allowed to tear down the venue connection.
    async fn record_trade<V: Venue>(
        &self,
        market: Option<&Market<V>>,
        trade: &V::Trade,
    ) -> Result<()> {
        let Some(market) = market else {
            tracing::warn!(
                venue = V::NAME,
                market = V::market_of(trade),
                "dropping trade for an unconfigured market"
            );
            return Ok(());
        };
        let evidence = match V::evidence(market, trade, Utc::now()) {
            Ok(evidence) => evidence,
            Err(error) => {
                tracing::warn!(%error, venue = V::NAME, "dropping trade that is not valid evidence");
                return Ok(());
            }
        };
        self.record(&evidence).await
    }

    /// Takes the set of assets updated since the last drain.
    fn drain_dirty(&self) -> HashSet<LedgerAsset> {
        std::mem::take(&mut *self.dirty.lock().expect("dirty asset lock"))
    }
}

/// Runs one venue's whole path: its supervised connection, and every decoded trade normalized
/// against its configured market, published durably, and offered to the shared engine.
///
/// An unconfigured venue parks forever instead of resolving: the supervisors never return, so
/// a resolved [`run`] arm would end the whole price pipeline.
async fn run_venue<V: Venue>(delivery: Arc<Delivery>, markets: Vec<Market<V>>) -> Result<()> {
    if markets.is_empty() {
        return std::future::pending().await;
    }
    let by_symbol = markets_by_symbol(&markets);
    venue::supervise(&markets, move |trade: V::Trade| {
        let delivery = delivery.clone();
        let configured = by_symbol.get(V::market_of(&trade)).cloned();
        async move { delivery.record_trade(configured.as_deref(), &trade).await }
    })
    .await
}

/// Runs the price pipeline forever: one supervised stream per configured venue, all feeding
/// the durable evidence publish and the shared aggregation engine, beside a periodic
/// aggregate report.
///
/// # Errors
///
/// Returns only for a configuration that must not run: no markets at all, an inconsistent
/// market list, or a required stream that is unavailable.
pub async fn run(events: Arc<NatsClient>, markets: &PriceMarkets) -> Result<()> {
    if markets.is_empty() {
        bail!("price ingestion requires at least one configured market")
    }
    let stream = tackt_nats::subjects::price_evidence_stream();
    let aggregate_stream = tackt_nats::subjects::price_aggregate_stream();
    // The acknowledged publish is the price path's completion boundary: without its streams
    // this configuration must fail instead of ingesting.
    events
        .require_stream(&stream)
        .await
        .context("require the price-evidence stream")?;
    events
        .require_stream(&aggregate_stream)
        .await
        .context("require the aggregate-price stream")?;
    let plan = combined_aggregation(markets)?;
    let engine = AggregationEngine::new(plan.config);
    let assets = engine.assets();
    let delivery = Arc::new(Delivery {
        events,
        stream,
        aggregate_stream,
        engine: Mutex::new(engine),
        dirty: Mutex::new(HashSet::new()),
        wake: tokio::sync::Notify::new(),
    });

    tokio::select! {
        result = run_venue(delivery.clone(), plan.binance_feeds) => result,
        result = run_venue(delivery.clone(), plan.kraken_feeds) => result,
        result = run_venue(delivery.clone(), plan.bybit_feeds) => result,
        result = run_venue(delivery.clone(), plan.coinbase_feeds) => result,
        () = publish_aggregates(delivery.clone(), assets) => Ok(()),
    }
}

/// The startup lookup from a venue's market spelling to its configured market, shared by every
/// delivered trade. The map holds `Arc`s so each delivered trade clones a refcount.
fn markets_by_symbol<V: Venue>(markets: &[Market<V>]) -> HashMap<String, Arc<Market<V>>> {
    markets
        .iter()
        .map(|market| (market.symbol().to_owned(), Arc::new(market.clone())))
        .collect()
}

/// The validated multi-venue plan: one engine configuration plus each venue's feed list.
struct AggregationPlan {
    config: AggregationConfig,
    binance_feeds: Vec<Market<Binance>>,
    kraken_feeds: Vec<Market<Kraken>>,
    bybit_feeds: Vec<Market<Bybit>>,
    coinbase_feeds: Vec<Market<Coinbase>>,
}

/// One venue's configured markets, each registered as a source for the asset it prices.
///
/// # Errors
///
/// Fails on any spelling the venue would refuse — a startup failure, never a running-state one.
fn venue_markets<V: Venue>(
    configured: &[VenueMarket],
    add_source: &mut impl FnMut(LedgerAsset, PriceAuthority, PriceSubject),
) -> Result<Vec<Market<V>>> {
    configured
        .iter()
        .map(|configured| {
            let market = Market::<V>::new(configured.market.clone())?;
            add_source(configured.asset, V::AUTHORITY, market.subject().clone());
            Ok(market)
        })
        .collect()
}

/// Builds the plan from the configured markets, merging every venue's sources per asset and
/// splitting the asset's weight evenly across them. Every validation failure here is a startup
/// failure, never a running-state one.
fn combined_aggregation(markets: &PriceMarkets) -> Result<AggregationPlan> {
    let mut per_asset: Vec<(LedgerAsset, Vec<(PriceAuthority, PriceSubject)>)> = Vec::new();
    let mut add_source =
        |asset: LedgerAsset, authority: PriceAuthority, subject: PriceSubject| match per_asset
            .iter_mut()
            .find(|(existing, _)| *existing == asset)
        {
            Some((_, sources)) => sources.push((authority, subject)),
            None => per_asset.push((asset, vec![(authority, subject)])),
        };
    let binance_feeds = venue_markets::<Binance>(&markets.binance, &mut add_source)?;
    let kraken_feeds = venue_markets::<Kraken>(&markets.kraken, &mut add_source)?;
    let bybit_feeds = venue_markets::<Bybit>(&markets.bybit, &mut add_source)?;
    let coinbase_feeds = venue_markets::<Coinbase>(&markets.coinbase, &mut add_source)?;
    let mut asset_configs = Vec::new();
    for (asset, sources) in per_asset {
        let count = u16::try_from(sources.len()).with_context(|| {
            format!("{asset:?} configures more sources than a weight can split")
        })?;
        let weight = WEIGHT_TOTAL_BPS / count;
        let remainder = WEIGHT_TOTAL_BPS % count;
        let sources: Vec<PriceSourceConfig> = sources
            .into_iter()
            .enumerate()
            .map(|(index, (authority, subject))| {
                PriceSourceConfig::new(
                    authority,
                    subject,
                    weight + if index == 0 { remainder } else { 0 },
                    FULL_WEIGHT_WITHIN,
                    MAX_STALENESS,
                )
            })
            .collect::<Result<_, _>>()?;
        asset_configs.push(AssetAggregationConfig::new(
            asset,
            sources,
            MIN_QUORUM_WEIGHT_BPS,
            MAX_DEVIATION_BPS,
        )?);
    }
    Ok(AggregationPlan {
        config: AggregationConfig::new(asset_configs)?,
        binance_feeds,
        kraken_feeds,
        bybit_feeds,
        coinbase_feeds,
    })
}

/// What an asset's most recent evaluation produced, so availability transitions log exactly
/// once. `Unknown` is the state before any evaluation, distinct from an observed outage.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Availability {
    Unknown,
    Priced,
    Unavailable,
}

/// One asset's publishing state: its strictly increasing sequence, its last evaluation's
/// availability, and when it last published (the throttle).
struct AssetPublishState {
    last_sequence: u64,
    availability: Availability,
    last_published: Option<tokio::time::Instant>,
}

/// The next per-asset sequence: the computation clock in milliseconds, clamped strictly
/// increasing within this process, including ticks that share a millisecond. State is volatile:
/// a cold restart after a clock rollback can lower the sequence.
fn next_sequence(last_sequence: &mut u64, computed_at: chrono::DateTime<Utc>) -> u64 {
    let clock = u64::try_from(computed_at.timestamp_millis()).unwrap_or(0);
    let next = clock.max(*last_sequence + 1);
    *last_sequence = next;
    next
}

/// Recomputes and durably publishes aggregates: immediately when an asset's table advances
/// (throttled to one publish per asset per [`AGGREGATE_MIN_PUBLISH_INTERVAL`]), and
/// unconditionally every [`AGGREGATE_HEARTBEAT_INTERVAL`] — the republish of an unchanged
/// price is what separates "stable" from "stalled".
///
/// Quorum loss simply stops that asset's publishing — absence is the fail-closed signal, and
/// downstream freshness policies take over. A failed publish is logged and superseded by the
/// next attempt: aggregates are derived data, recomputable from the evidence stream, so
/// nothing is lost by not retrying one.
async fn publish_aggregates(delivery: Arc<Delivery>, assets: Vec<LedgerAsset>) {
    let mut heartbeat = tokio::time::interval(AGGREGATE_HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Retries updates a burst left throttled, so the last trade of a burst never waits for
    // the full heartbeat.
    let mut flush = tokio::time::interval(AGGREGATE_MIN_PUBLISH_INTERVAL);
    flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut states: HashMap<LedgerAsset, AssetPublishState> = assets
        .iter()
        .map(|&asset| {
            (
                asset,
                AssetPublishState {
                    last_sequence: 0,
                    availability: Availability::Unknown,
                    last_published: None,
                },
            )
        })
        .collect();
    let mut pending: HashSet<LedgerAsset> = HashSet::new();
    loop {
        tokio::select! {
            () = delivery.wake.notified() => {
                pending.extend(delivery.drain_dirty());
                publish_due(&delivery, &mut states, &mut pending).await;
            }
            _ = flush.tick() => {
                publish_due(&delivery, &mut states, &mut pending).await;
            }
            _ = heartbeat.tick() => {
                for &asset in &assets {
                    let state = states.get_mut(&asset).expect("every asset has state");
                    evaluate_and_publish(&delivery, asset, state).await;
                }
                pending.clear();
            }
        }
    }
}

/// Publishes every pending asset whose throttle has elapsed; the rest stay pending for the
/// next flush or heartbeat.
async fn publish_due(
    delivery: &Delivery,
    states: &mut HashMap<LedgerAsset, AssetPublishState>,
    pending: &mut HashSet<LedgerAsset>,
) {
    let now = tokio::time::Instant::now();
    for (&asset, state) in states.iter_mut() {
        let throttled = state
            .last_published
            .is_some_and(|last| now.duration_since(last) < AGGREGATE_MIN_PUBLISH_INTERVAL);
        if throttled || !pending.remove(&asset) {
            continue;
        }
        evaluate_and_publish(delivery, asset, state).await;
    }
}

/// Computes one asset's aggregate, logs availability transitions once, and publishes when a
/// price exists.
async fn evaluate_and_publish(
    delivery: &Delivery,
    asset: LedgerAsset,
    state: &mut AssetPublishState,
) {
    let computed = delivery
        .engine
        .lock()
        .expect("aggregation engine lock")
        .aggregate(asset, Utc::now());
    match computed {
        Ok(aggregate) => {
            if state.availability != Availability::Priced {
                tracing::info!(
                    asset = ?asset,
                    price = aggregate.price.value(),
                    inputs = aggregate.inputs.len(),
                    "price aggregate available"
                );
                state.availability = Availability::Priced;
            }
            state.last_published = Some(tokio::time::Instant::now());
            publish_one(delivery, state, aggregate).await;
        }
        Err(
            error @ (AggregationError::BelowQuorum { .. }
            | AggregationError::NoDeviationMajority { .. }),
        ) => {
            if state.availability != Availability::Unavailable {
                tracing::warn!(%error, asset = ?asset, "price aggregate unavailable");
                state.availability = Availability::Unavailable;
            }
        }
        Err(error) => tracing::error!(%error, asset = ?asset, "price aggregation failed"),
    }
}

async fn publish_one(
    delivery: &Delivery,
    state: &mut AssetPublishState,
    aggregate: aggregation::Aggregate,
) {
    let inputs = aggregate
        .inputs
        .into_iter()
        .map(|input| tackt_nats::aggregates::AggregateSource {
            authority: input.authority,
            evidence_id: input.evidence_id,
            effective_weight_bps: input.effective_weight_bps,
        })
        .collect();
    let sequence = next_sequence(&mut state.last_sequence, aggregate.computed_at);
    let message = match tackt_nats::aggregates::AggregatePrice::new(
        aggregate.asset,
        aggregate.price,
        sequence,
        aggregate.computed_at,
        inputs,
    ) {
        Ok(message) => message,
        // The engine's output failed the wire contract: a build defect, never provider data.
        Err(error) => {
            tracing::error!(%error, asset = ?aggregate.asset, "aggregate refused by its own contract");
            return;
        }
    };
    match delivery
        .events
        .publish_aggregate_price(&delivery.aggregate_stream, &message)
        .await
    {
        Ok(_) => {
            metrics::counter!("indexer_price_aggregates_published_total").increment(1);
            tracing::debug!(
                asset = ?message.asset(),
                price = message.price().value(),
                sequence = message.sequence(),
                "aggregate price published"
            );
        }
        Err(error) => {
            metrics::counter!("indexer_price_aggregate_publish_failures_total").increment(1);
            tracing::warn!(%error, asset = ?message.asset(), "aggregate publish failed; next tick supersedes it");
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::json;
    use tackt_nats::prices::{MarketSymbol, PriceQuote};
    use tackt_primitives::Provenance;

    use super::*;

    fn market(asset: LedgerAsset, market: &str) -> VenueMarket {
        VenueMarket {
            asset,
            market: market.to_string(),
        }
    }

    fn evidence(authority: PriceAuthority, market: &str, value: u128) -> PriceEvidence {
        PriceEvidence::new(
            authority,
            PriceSubject::Market(MarketSymbol::new(market).expect("symbol")),
            PriceQuote::new(value, 8).expect("quote"),
            1,
            Utc.timestamp_opt(1_800_000_000, 0).single().expect("time"),
            Utc.timestamp_opt(1_800_000_000, 0).single().expect("time"),
            Provenance {
                endpoint: "https://example.com".to_string(),
                method: "test".to_string(),
                source_id: "seq:1".to_string(),
            },
            json!({ "test": true }),
        )
        .expect("evidence")
    }

    #[test]
    fn one_asset_on_two_venues_splits_its_weight_evenly() {
        let plan = combined_aggregation(&PriceMarkets {
            binance: vec![
                market(LedgerAsset::Eth, "ethusdt"),
                market(LedgerAsset::Btc, "BTCUSDT"),
            ],
            kraken: vec![market(LedgerAsset::Eth, "ETH/USD")],
            ..PriceMarkets::default()
        })
        .expect("plan");
        assert_eq!(plan.binance_feeds.len(), 2);
        assert_eq!(plan.kraken_feeds.len(), 1);
        let mut engine = AggregationEngine::new(plan.config);
        assert_eq!(engine.assets(), vec![LedgerAsset::Eth, LedgerAsset::Btc]);

        // Feed both of ETH's venues and read the split back off the aggregate: 5000/5000,
        // whatever the configuration casing was.
        engine
            .accept(&evidence(
                PriceAuthority::Binance,
                "ETHUSDT",
                244_000_000_000,
            ))
            .expect("binance quote");
        engine
            .accept(&evidence(
                PriceAuthority::Kraken,
                "ETH-USD",
                244_200_000_000,
            ))
            .expect("kraken quote");
        let aggregate = engine
            .aggregate(
                LedgerAsset::Eth,
                Utc.timestamp_opt(1_800_000_001, 0).single().expect("time"),
            )
            .expect("both venues fresh");
        let weights: Vec<u16> = aggregate
            .inputs
            .iter()
            .map(|input| input.effective_weight_bps)
            .collect();
        assert_eq!(weights, vec![5_000, 5_000]);
        assert_eq!(aggregate.price.value(), 244_100_000_000);
    }

    #[test]
    fn aggregate_sequences_are_strictly_increasing_within_shared_millis() {
        let mut last = 0;
        let tick = Utc
            .timestamp_millis_opt(1_800_000_000_123)
            .single()
            .expect("time");
        assert_eq!(next_sequence(&mut last, tick), 1_800_000_000_123);
        // A second aggregate in the same millisecond still advances.
        assert_eq!(next_sequence(&mut last, tick), 1_800_000_000_124);
        let later = Utc
            .timestamp_millis_opt(1_800_000_005_000)
            .single()
            .expect("time");
        assert_eq!(next_sequence(&mut last, later), 1_800_000_005_000);
    }

    #[test]
    fn one_asset_on_four_venues_splits_its_weight_four_ways() {
        let plan = combined_aggregation(&PriceMarkets {
            binance: vec![market(LedgerAsset::Eth, "ETHUSDT")],
            kraken: vec![market(LedgerAsset::Eth, "ETH/USD")],
            bybit: vec![market(LedgerAsset::Eth, "ETHUSDT")],
            coinbase: vec![market(LedgerAsset::Eth, "ETH-USD")],
        })
        .expect("plan");
        assert_eq!(plan.bybit_feeds.len(), 1);
        assert_eq!(plan.coinbase_feeds.len(), 1);
        let mut engine = AggregationEngine::new(plan.config);

        engine
            .accept(&evidence(
                PriceAuthority::Binance,
                "ETHUSDT",
                244_000_000_000,
            ))
            .expect("binance quote");
        engine
            .accept(&evidence(
                PriceAuthority::Kraken,
                "ETH-USD",
                244_200_000_000,
            ))
            .expect("kraken quote");
        engine
            .accept(&evidence(PriceAuthority::Bybit, "ETHUSDT", 244_100_000_000))
            .expect("bybit quote");
        engine
            .accept(&evidence(
                PriceAuthority::Coinbase,
                "ETH-USD",
                244_300_000_000,
            ))
            .expect("coinbase quote");
        let aggregate = engine
            .aggregate(
                LedgerAsset::Eth,
                Utc.timestamp_opt(1_800_000_001, 0).single().expect("time"),
            )
            .expect("all venues fresh");
        let weights: Vec<u16> = aggregate
            .inputs
            .iter()
            .map(|input| input.effective_weight_bps)
            .collect();
        assert_eq!(weights, vec![2_500, 2_500, 2_500, 2_500]);
    }

    #[test]
    fn defaults_form_a_valid_aggregation_plan() {
        let plan = combined_aggregation(&PriceMarkets::defaults()).expect("default markets");
        let engine = AggregationEngine::new(plan.config);
        assert_eq!(
            engine.assets(),
            vec![
                LedgerAsset::Btc,
                LedgerAsset::Eth,
                LedgerAsset::Sol,
                LedgerAsset::Jitosol,
            ]
        );
        assert_eq!(plan.binance_feeds.len(), 3);
        assert_eq!(plan.kraken_feeds.len(), 4);
        assert_eq!(plan.bybit_feeds.len(), 3);
        assert_eq!(plan.coinbase_feeds.len(), 3);
    }

    #[test]
    fn kraken_jitosol_trades_price_only_jitosol_and_replay_does_not_refresh_staleness() {
        let plan = combined_aggregation(&PriceMarkets::defaults()).expect("default markets");
        let market = plan
            .kraken_feeds
            .iter()
            .find(|market| market.symbol() == "JITOSOL/USD")
            .expect("default JitoSOL market");
        let trades = Kraken::decode(
            r#"{"channel":"trade","type":"snapshot","data":[{
                "symbol":"JITOSOL/USD","side":"buy","price":135.27,"qty":0.5,
                "trade_id":42,"timestamp":"2027-01-15T08:00:00Z"
            }]}"#,
        )
        .expect("Kraken snapshot");
        let trade = &trades[0];
        let now = Utc.timestamp_opt(1_800_000_000, 0).single().expect("time");
        let quote = Kraken::evidence(market, trade, now).expect("JitoSOL evidence");
        let mut engine = AggregationEngine::new(plan.config);
        assert_eq!(
            engine.accept(&quote).expect("accept JitoSOL"),
            aggregation::Accepted::Updated {
                asset: LedgerAsset::Jitosol,
            }
        );
        let aggregate = engine
            .aggregate(LedgerAsset::Jitosol, now)
            .expect("single Kraken source prices JitoSOL");
        assert_eq!(aggregate.price, PriceQuote::new(13_527_000_000, 8).unwrap());
        assert_eq!(aggregate.inputs.len(), 1);
        assert_eq!(aggregate.inputs[0].authority, PriceAuthority::Kraken);
        assert_eq!(aggregate.inputs[0].effective_weight_bps, WEIGHT_TOTAL_BPS);
        assert_eq!(aggregate.inputs[0].evidence_id, quote.evidence_id());
        assert!(matches!(
            engine.aggregate(LedgerAsset::Sol, now),
            Err(AggregationError::BelowQuorum { .. })
        ));

        // A reconnect may replay an old trade. Its receipt time must not make it fresh.
        let later = now + chrono::Duration::minutes(5);
        let replay = Kraken::evidence(market, trade, later).expect("replayed evidence");
        assert_eq!(
            engine.accept(&replay).expect("duplicate snapshot"),
            aggregation::Accepted::Duplicate {
                asset: LedgerAsset::Jitosol,
            }
        );
        assert!(matches!(
            engine.aggregate(LedgerAsset::Jitosol, later),
            Err(AggregationError::BelowQuorum {
                surviving_weight_bps: 0,
                ..
            })
        ));
    }

    #[test]
    fn inconsistent_market_lists_fail_at_startup() {
        let binance_only = |markets: Vec<VenueMarket>| PriceMarkets {
            binance: markets,
            ..PriceMarkets::default()
        };
        // The same venue market twice is one source claimed twice — including when the
        // spellings differ only by case.
        assert!(
            combined_aggregation(&binance_only(vec![
                market(LedgerAsset::Eth, "ETHUSDT"),
                market(LedgerAsset::Btc, "ethusdt"),
            ]))
            .is_err()
        );
        // A market a venue would refuse must fail configuration.
        assert!(
            combined_aggregation(&PriceMarkets {
                kraken: vec![market(LedgerAsset::Eth, "ETHUSD")],
                ..PriceMarkets::default()
            })
            .is_err()
        );
        assert!(
            combined_aggregation(&binance_only(vec![market(LedgerAsset::Eth, "ETH-USDT")]))
                .is_err()
        );
        assert!(
            combined_aggregation(&PriceMarkets {
                bybit: vec![market(LedgerAsset::Eth, "ETH-USDT")],
                ..PriceMarkets::default()
            })
            .is_err()
        );
        assert!(
            combined_aggregation(&PriceMarkets {
                coinbase: vec![market(LedgerAsset::Eth, "ETHUSD")],
                ..PriceMarkets::default()
            })
            .is_err()
        );
    }
}

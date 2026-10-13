use std::alloc::System;
use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

use cpu_time::ProcessTime;
use risk_engine::egress::history::{CHANNEL_CAPACITY, CapturedHealth, HistorySampler};
use risk_engine::processing::{
    AMOUNT_SCALE, AccountMode, AccountPortfolio, Amount, AssetRegistry, FACTOR_SCALE, Factor,
    HyperCoreBalance, HyperliquidSnapshot, MarginDomain, MarginMode, PRICE_SCALE, PerpPosition,
    PortfolioGeneration, Price, PriceSourceKind, PriceUpdate, RiskPolicy, RiskProcessor,
    SignedAmount, Value,
};
use serde::{Deserialize, Serialize};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use tackt_primitives::accounts::AccountSubject;
use uuid::Uuid;

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// Prime stride that walks patch targets across the perp-holding account range.
///
/// The walk visits distinct accounts only while the iteration count fits within one cycle
/// of the stride: `perp_count` iterations when the stride does not divide `perp_count`,
/// `perp_count / PATCH_STRIDE` otherwise. `Config::load` rejects configurations that would
/// exceed the cycle so the patch benchmark always measures distinct accounts.
const PATCH_STRIDE: usize = 7_919;

fn subject(account: u32) -> AccountSubject {
    AccountSubject::new(Uuid::from_u128(u128::from(account) + 1))
        .expect("benchmark account subject is non-zero")
}

fn account_subjects(account_count: usize) -> Result<BTreeMap<u32, AccountSubject>, Box<dyn Error>> {
    (0..account_count)
        .map(|raw| {
            let account = u32::try_from(raw)?;
            Ok((account, subject(account)))
        })
        .collect()
}

#[derive(Debug, Default, Deserialize)]
struct Environment {
    accounts: Option<usize>,
    workers: Option<usize>,
    warmup_iterations: Option<usize>,
    measured_iterations: Option<usize>,
    install_iterations: Option<usize>,
    patch_iterations: Option<usize>,
    near_threshold_percent: Option<usize>,
    asset_registry_path: Option<PathBuf>,
    history_sampling: Option<bool>,
    history_queue_full: Option<bool>,
}

#[derive(Debug)]
struct Config {
    accounts: usize,
    workers: usize,
    warmup_iterations: usize,
    measured_iterations: usize,
    install_iterations: usize,
    patch_iterations: usize,
    near_threshold_percent: usize,
    history_sampling: bool,
    history_queue_full: bool,
}

impl Config {
    fn load() -> Result<Self, Box<dyn Error>> {
        dotenvy::dotenv().ok();
        let environment = envy::prefixed("RISK_BENCH_").from_env::<Environment>()?;
        let available_workers = std::thread::available_parallelism()?.get();
        if environment.asset_registry_path.is_some() {
            return Err("RISK_BENCH_ASSET_REGISTRY_PATH is no longer supported; the root registry is embedded".into());
        }
        let config = Self {
            history_sampling: environment.history_sampling.unwrap_or(false),
            history_queue_full: environment.history_queue_full.unwrap_or(false),
            accounts: environment.accounts.unwrap_or(1_000_000),
            workers: environment.workers.unwrap_or(available_workers.min(8)),
            warmup_iterations: environment.warmup_iterations.unwrap_or(10),
            measured_iterations: environment.measured_iterations.unwrap_or(200),
            install_iterations: environment.install_iterations.unwrap_or(20),
            patch_iterations: environment.patch_iterations.unwrap_or(1_000),
            near_threshold_percent: environment.near_threshold_percent.unwrap_or(10),
        };
        if config.accounts < 2
            || config.workers == 0
            || config.workers > config.accounts
            || config.measured_iterations < 100
            || config.install_iterations < 2
            || config.patch_iterations < 10
            || config.near_threshold_percent > 100
        {
            return Err("invalid benchmark configuration".into());
        }
        let perp_count = config.accounts - config.accounts / 2;
        let distinct_patch_targets = if perp_count.is_multiple_of(PATCH_STRIDE) {
            perp_count / PATCH_STRIDE
        } else {
            perp_count
        };
        if config.patch_iterations > distinct_patch_targets {
            return Err(format!(
                "patch_iterations ({}) exceeds the {distinct_patch_targets} distinct \
                 perp accounts reachable by the patch stride; lower \
                 RISK_BENCH_PATCH_ITERATIONS or raise RISK_BENCH_ACCOUNTS",
                config.patch_iterations,
            )
            .into());
        }
        Ok(config)
    }
}

#[derive(Debug, Serialize)]
struct Report {
    configured_assets: usize,
    configured_perp_markets: usize,
    accounts: usize,
    affected_balance_exposures: usize,
    affected_perp_exposures: usize,
    affected_exposures_total: usize,
    workers: usize,
    warmup_iterations: usize,
    measured_iterations: usize,
    near_threshold_percent: usize,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    maximum_ms: f64,
    install_iterations: usize,
    install_p50_ms: f64,
    install_p95_ms: f64,
    install_maximum_ms: f64,
    patch_iterations: usize,
    patch_p50_ms: f64,
    patch_p95_ms: f64,
    patch_maximum_ms: f64,
    post_patch_price_p50_ms: f64,
    post_patch_price_maximum_ms: f64,
    exposures_per_second_at_p99: f64,
    changed_state_events: usize,
    allocation_calls_measured: usize,
    bytes_allocated_measured: usize,
    resident_memory_kib: Option<u64>,
    process_cpu_seconds: f64,
    wall_seconds: f64,
    average_cpu_cores_used: f64,
    cpu_model: Option<String>,
    logical_cores: usize,
    rust_version: String,
    target: String,
    build_profile: &'static str,
    history_sampling: bool,
    history_queue_full: bool,
    capture_pair_p50_ns: Option<u128>,
    capture_allocations: Option<usize>,
    venue_query_p50_ns: u128,
    venue_query_allocations: usize,
}

#[allow(clippy::too_many_lines)]
fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::load()?;
    let registry = AssetRegistry::bundled()?;
    let (mut processor, balance_count, perp_count) = fixture(&config, registry.clone())?;
    let mut sequence = 1;

    processor.apply_price_update(price_update(100, sequence))?;
    for iteration in 0..config.warmup_iterations {
        sequence += 1;
        processor.apply_price_update(price_update(alternating_price(iteration), sequence))?;
    }

    let (sender, mut history_receiver) = tokio::sync::mpsc::channel(CHANNEL_CAPACITY);
    let history_sampler = HistorySampler::new(
        &[subject(0), subject(u32::try_from(balance_count)?)],
        sender,
    );
    if config.history_sampling && config.history_queue_full {
        for _ in 0..CHANNEL_CAPACITY {
            history_sampler.capture(&processor, chrono::Utc::now());
        }
    }
    let mut latencies = Vec::with_capacity(config.measured_iterations);
    let mut changed_state_events = 0;
    let allocation_region = Region::new(GLOBAL);
    let cpu_started = ProcessTime::now();
    let wall_started = Instant::now();

    for iteration in 0..config.measured_iterations {
        sequence += 1;
        let update = price_update(
            alternating_price(iteration + config.warmup_iterations),
            sequence,
        );
        let started = Instant::now();
        let outcome = processor.apply_price_update(update)?;
        if config.history_sampling {
            history_sampler.capture(&processor, chrono::Utc::now());
        }
        latencies.push(started.elapsed());
        if !config.history_queue_full {
            drain_captures(&mut history_receiver);
        }
        changed_state_events += outcome.events.len();
    }

    let wall = wall_started.elapsed();
    let cpu = cpu_started.elapsed();
    let allocation = allocation_region.change();
    latencies.sort_unstable();
    let p99 = percentile(&latencies, 99);

    let install_latencies = measure_installs(&config, &mut processor)?;
    let patch_version_base = 2 + config.install_iterations as u64;
    let patch_latencies = measure_patches(
        &config,
        &mut processor,
        balance_count,
        perp_count,
        patch_version_base,
    )?;
    let mut post_patch_latencies = Vec::with_capacity(30);
    for iteration in 0..30 {
        sequence += 1;
        let update = price_update(
            alternating_price(iteration + config.warmup_iterations + config.measured_iterations),
            sequence,
        );
        let started = Instant::now();
        processor.apply_price_update(update)?;
        post_patch_latencies.push(started.elapsed());
    }
    post_patch_latencies.sort_unstable();
    let capture = config.history_sampling.then(|| {
        measure_capture(
            &processor,
            &history_sampler,
            &mut history_receiver,
            config.history_queue_full,
        )
    });
    let (capture_pair_p50_ns, capture_allocations) = capture.unzip();
    let (venue_query_p50_ns, venue_query_allocations) =
        measure_venue_query(&processor, u32::try_from(balance_count)?);
    let affected_exposures = u32::try_from(balance_count + perp_count)?;
    let (rust_version, target) = toolchain();
    let report = Report {
        history_sampling: config.history_sampling,
        history_queue_full: config.history_queue_full,
        capture_pair_p50_ns,
        capture_allocations,
        venue_query_p50_ns,
        venue_query_allocations,
        configured_assets: registry.asset_count(),
        configured_perp_markets: registry.perp_market_count(),
        accounts: config.accounts,
        affected_balance_exposures: balance_count,
        affected_perp_exposures: perp_count,
        affected_exposures_total: balance_count + perp_count,
        workers: config.workers,
        warmup_iterations: config.warmup_iterations,
        measured_iterations: config.measured_iterations,
        near_threshold_percent: config.near_threshold_percent,
        p50_ms: milliseconds(percentile(&latencies, 50)),
        p95_ms: milliseconds(percentile(&latencies, 95)),
        p99_ms: milliseconds(p99),
        maximum_ms: milliseconds(*latencies.last().expect("latencies are non-empty")),
        install_iterations: config.install_iterations,
        install_p50_ms: milliseconds(percentile(&install_latencies, 50)),
        install_p95_ms: milliseconds(percentile(&install_latencies, 95)),
        install_maximum_ms: milliseconds(
            *install_latencies.last().expect("installs are non-empty"),
        ),
        patch_iterations: config.patch_iterations,
        patch_p50_ms: milliseconds(percentile(&patch_latencies, 50)),
        patch_p95_ms: milliseconds(percentile(&patch_latencies, 95)),
        patch_maximum_ms: milliseconds(*patch_latencies.last().expect("patches are non-empty")),
        post_patch_price_p50_ms: milliseconds(percentile(&post_patch_latencies, 50)),
        post_patch_price_maximum_ms: milliseconds(
            *post_patch_latencies
                .last()
                .expect("post-patch prices are non-empty"),
        ),
        exposures_per_second_at_p99: f64::from(affected_exposures) / p99.as_secs_f64(),
        changed_state_events,
        allocation_calls_measured: allocation.allocations,
        bytes_allocated_measured: allocation.bytes_allocated,
        resident_memory_kib: resident_memory_kib(),
        process_cpu_seconds: cpu.as_secs_f64(),
        wall_seconds: wall.as_secs_f64(),
        average_cpu_cores_used: cpu.as_secs_f64() / wall.as_secs_f64(),
        cpu_model: cpu_model(),
        logical_cores: std::thread::available_parallelism()?.get(),
        rust_version,
        target,
        build_profile: if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
    };

    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn fixture(
    config: &Config,
    registry: AssetRegistry,
) -> Result<(RiskProcessor, usize, usize), Box<dyn Error>> {
    let (balances, domains, perps) = portfolio_records(config)?;
    let balance_count = balances.len();
    let perp_count = perps.len();
    let policy = RiskPolicy::new(Factor::from_raw(1_200_000), Factor::from_raw(FACTOR_SCALE))?;
    let processor = RiskProcessor::new(
        config.accounts,
        config.workers,
        policy,
        registry,
        &PortfolioGeneration::new(1, Vec::new(), balances, domains, perps)
            .with_account_subjects(account_subjects(config.accounts)?),
    )?;
    Ok((processor, balance_count, perp_count))
}

type PortfolioRecords = (Vec<HyperCoreBalance>, Vec<MarginDomain>, Vec<PerpPosition>);

fn portfolio_records(config: &Config) -> Result<PortfolioRecords, Box<dyn Error>> {
    let balance_count = config.accounts / 2;
    let perp_count = config.accounts - balance_count;
    let near_threshold_count = perp_count.saturating_mul(config.near_threshold_percent) / 100;
    let mut balances = Vec::with_capacity(balance_count);
    let mut domains = Vec::with_capacity(perp_count);
    let mut perps = Vec::with_capacity(perp_count);

    for raw_account in 0..config.accounts {
        let account = u32::try_from(raw_account)?;
        if raw_account < balance_count {
            balances.push(HyperCoreBalance {
                account,
                asset: 3,
                amount: Amount::from_raw(AMOUNT_SCALE),
                included_in_account_value: false,
                position: account,
                source_version: 1,
            });
        } else {
            let perp_offset = raw_account - balance_count;
            let near_threshold = perp_offset < near_threshold_count;
            domains.push(MarginDomain {
                account,
                domain: 1,
                account_mode: AccountMode::Standard,
                margin_mode: MarginMode::Cross,
                snapshot: Some(HyperliquidSnapshot {
                    account_value: Value::from_raw(
                        if near_threshold { 20 } else { 200 } * PRICE_SCALE,
                    ),
                    maintenance: Value::from_raw(
                        if near_threshold { 18 } else { 10 } * PRICE_SCALE,
                    ),
                    source_sequence: 1,
                }),
                source_version: 1,
            });
            perps.push(PerpPosition {
                account,
                domain: 1,
                market: 1,
                signed_size: SignedAmount::from_raw(if raw_account.is_multiple_of(2) {
                    AMOUNT_SCALE
                } else {
                    -AMOUNT_SCALE
                }),
                anchor_price: Some(Price::from_raw(100 * PRICE_SCALE)),
                position: account,
                source_version: 1,
            });
        }
    }
    Ok((balances, domains, perps))
}

/// Measures full-generation install latency: compile, validate, and rebuild at fixture size.
fn measure_installs(
    config: &Config,
    processor: &mut RiskProcessor,
) -> Result<Vec<Duration>, Box<dyn Error>> {
    let (balances, domains, perps) = portfolio_records(config)?;
    let mut latencies = Vec::with_capacity(config.install_iterations);
    for iteration in 0..config.install_iterations {
        let generation = PortfolioGeneration::new(
            2 + u64::try_from(iteration)?,
            Vec::new(),
            balances.clone(),
            domains.clone(),
            perps.clone(),
        )
        .with_account_subjects(account_subjects(config.accounts)?);
        let started = Instant::now();
        let install = processor.install_portfolio_generation(&generation)?;
        latencies.push(started.elapsed());
        assert!(install.installed, "install must replace the generation");
    }
    latencies.sort_unstable();
    Ok(latencies)
}

const fn alternating_price(iteration: usize) -> i128 {
    if iteration.is_multiple_of(2) { 90 } else { 110 }
}

const fn price_update(price: i128, source_sequence: u64) -> PriceUpdate {
    PriceUpdate {
        price_index: 3,
        source: PriceSourceKind::Hyperliquid,
        source_sequence,
        price: Price::from_raw(price * PRICE_SCALE),
    }
}

/// Measures single-account patch latency across distinct perp-holding accounts.
fn measure_patches(
    config: &Config,
    processor: &mut RiskProcessor,
    balance_count: usize,
    perp_count: usize,
    version_base: u64,
) -> Result<Vec<Duration>, Box<dyn Error>> {
    let mut latencies = Vec::with_capacity(config.patch_iterations);
    for iteration in 0..config.patch_iterations {
        let account = u32::try_from(balance_count + (iteration * PATCH_STRIDE) % perp_count)?;
        let patch = AccountPortfolio {
            account,
            account_subject: subject(account),
            version: version_base + u64::try_from(iteration)?,
            ethereum_balances: Vec::new(),
            hypercore_balances: Vec::new(),
            margin_domains: vec![MarginDomain {
                account,
                domain: 1,
                account_mode: AccountMode::Standard,
                margin_mode: MarginMode::Cross,
                snapshot: Some(HyperliquidSnapshot {
                    account_value: Value::from_raw(150 * PRICE_SCALE),
                    maintenance: Value::from_raw(12 * PRICE_SCALE),
                    source_sequence: 2,
                }),
                source_version: 2,
            }],
            perp_positions: vec![PerpPosition {
                account,
                domain: 1,
                market: 1,
                signed_size: SignedAmount::from_raw(2 * AMOUNT_SCALE),
                anchor_price: Some(Price::from_raw(100 * PRICE_SCALE)),
                position: account,
                source_version: 2,
            }],
        };
        let started = Instant::now();
        processor.apply_account_patch(&patch)?;
        latencies.push(started.elapsed());
    }
    latencies.sort_unstable();
    Ok(latencies)
}

fn percentile(sorted: &[Duration], percentile: usize) -> Duration {
    let index = sorted
        .len()
        .saturating_mul(percentile)
        .div_ceil(100)
        .saturating_sub(1);
    sorted[index.min(sorted.len() - 1)]
}

fn milliseconds(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn resident_memory_kib() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse().ok())
}

fn cpu_model() -> Option<String> {
    let cpu_info = fs::read_to_string("/proc/cpuinfo").ok()?;
    cpu_info.lines().find_map(|line| {
        line.strip_prefix("model name")
            .and_then(|value| value.split_once(':'))
            .map(|(_, value)| value.trim().to_owned())
    })
}

fn toolchain() -> (String, String) {
    let Ok(output) = Command::new("rustc").arg("-vV").output() else {
        return ("unknown".to_owned(), std::env::consts::ARCH.to_owned());
    };
    let details = String::from_utf8_lossy(&output.stdout);
    let version = details.lines().next().unwrap_or("unknown").to_owned();
    let target = details
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .unwrap_or(std::env::consts::ARCH)
        .to_owned();
    (version, target)
}

/// Empties the handoff so the benchmark measures capture, not a saturated queue.
fn drain_captures(receiver: &mut tokio::sync::mpsc::Receiver<CapturedHealth>) {
    while receiver.try_recv().is_ok() {}
}

/// Warm batch cost for one venue read; excludes setup and contains no price-update work.
fn measure_venue_query(processor: &RiskProcessor, account: u32) -> (u128, usize) {
    let mut times = Vec::with_capacity(100);
    let region = Region::new(GLOBAL);
    for _ in 0..100 {
        let started = Instant::now();
        for _ in 0..1000 {
            std::hint::black_box(
                std::hint::black_box(processor)
                    .venue_margin(
                        account,
                        risk_engine::processing::VenueKind::HyperliquidPerps,
                    )
                    .expect("valid venue")
                    .expect("perp account"),
            );
        }
        times.push(started.elapsed() / 1000);
    }
    let allocations = region.change().allocations;
    assert_eq!(allocations, 0, "venue reads must not allocate");
    times.sort_unstable();
    (percentile(&times, 50).as_nanos(), allocations)
}

/// Warm batch cost for capturing two accounts, excluding consumer drain and persistence.
fn measure_capture(
    processor: &RiskProcessor,
    sampler: &HistorySampler,
    receiver: &mut tokio::sync::mpsc::Receiver<CapturedHealth>,
    queue_full: bool,
) -> (u128, usize) {
    let mut times = Vec::with_capacity(100);
    // Prime the channel's internal allocation before counting steady-state capture work.
    for _ in 0..1000 {
        sampler.capture(processor, chrono::Utc::now());
        if !queue_full {
            drain_captures(receiver);
        }
    }
    let mut allocations = 0;
    for _ in 0..100 {
        let mut elapsed = Duration::ZERO;
        for _ in 0..1000 {
            let region = Region::new(GLOBAL);
            let started = Instant::now();
            sampler.capture(std::hint::black_box(processor), chrono::Utc::now());
            elapsed += started.elapsed();
            allocations += region.change().allocations;
            if !queue_full {
                drain_captures(receiver);
            }
        }
        times.push(elapsed / 1000);
    }
    times.sort_unstable();
    (percentile(&times, 50).as_nanos(), allocations)
}

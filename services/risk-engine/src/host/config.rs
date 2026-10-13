//! Host configuration from `RISK_ENGINE_`-prefixed environment variables,
//! mirroring the benchmark's `Environment` → `Config` shape.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, bail};
use risk_engine::egress::history::MAX_ACCOUNTS;
use risk_engine::processing::{Factor, RiskPolicy};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tackt_primitives::accounts::AccountSubject;

/// Raw environment: every field optional, defaults applied in [`Config::load`].
#[derive(Default, Deserialize)]
struct Environment {
    account_count: Option<usize>,
    shard_count: Option<usize>,
    warning_health: Option<i64>,
    liquidation_health: Option<i64>,
    asset_registry_path: Option<PathBuf>,
    snapshot_dir: Option<PathBuf>,
    snapshot_interval_secs: Option<u64>,
    snapshot_event_count: Option<u64>,
    nats_url: Option<String>,
    database_url: Option<String>,
    migration_database_url: Option<String>,
    ledger_refresh_interval_secs: Option<u64>,
    listen_addr: Option<SocketAddr>,
    initial_generation_version: Option<u64>,
    ingest_channel_capacity: Option<usize>,
    stream_prefix: Option<String>,
    internal_token: Option<String>,
    allow_cleartext: Option<bool>,
    evaluation_channel_capacity: Option<usize>,
    history_accounts: Option<String>,
    history_interval_secs: Option<u64>,
}

/// Validated host configuration.
#[derive(Clone)]
pub struct Config {
    pub account_count: usize,
    pub shard_count: usize,
    /// Raw `Factor` units (1e6): `1_200_000` = health 1.2.
    pub warning_health: i64,
    /// Raw `Factor` units (1e6): `1_000_000` = health 1.0.
    pub liquidation_health: i64,
    pub snapshot_dir: PathBuf,
    pub snapshot_interval: Duration,
    /// Accepted events between snapshots; whichever of interval/count first.
    pub snapshot_event_count: u64,
    pub nats_url: String,
    /// `PostgreSQL` runtime credentials; never logged or included in `Debug` output.
    pub database_url: String,
    /// Optional separate migrator, matching the Ledger service's startup convention.
    pub migration_database_url: Option<String>,
    /// Interval between complete reads of current Ledger balances.
    pub ledger_refresh_interval: Duration,
    pub listen_addr: SocketAddr,
    /// Version stamped on the first-boot empty generation. The first real
    /// reconciliation sweep must be stamped strictly greater or it conflicts
    /// with the empty generation and is dropped.
    pub initial_generation_version: u64,
    pub ingest_channel_capacity: usize,
    /// Stream-name prefix (`TACKT_RISK` in production); subjects derive from
    /// it. Overridden by tests for isolation.
    pub stream_prefix: String,
    /// SHA-256 of the bearer token; the secret itself is never retained or logged.
    pub internal_token_hash: [u8; 32],
    /// Explicit acknowledgement that TLS terminates before a non-loopback listener.
    pub allow_cleartext: bool,
    /// Bounded capacity reserved for synchronous evaluation requests.
    pub evaluation_channel_capacity: usize,
    /// Explicit demo subjects; an empty list disables display-history sampling.
    pub history_accounts: Vec<AccountSubject>,
    pub history_interval: Duration,
}

impl Config {
    /// Loads and validates configuration from the environment.
    pub fn load() -> anyhow::Result<Self> {
        dotenvy::dotenv().ok();
        let environment = envy::prefixed("RISK_ENGINE_")
            .from_env::<Environment>()
            .context("invalid RISK_ENGINE_ environment configuration")?;
        if environment.asset_registry_path.is_some() {
            bail!(
                "RISK_ENGINE_ASSET_REGISTRY_PATH is no longer supported; the root registry is embedded"
            );
        }
        let internal_token = environment
            .internal_token
            .context("RISK_ENGINE_INTERNAL_TOKEN is required")?;
        if internal_token.len() < 24 || !internal_token.bytes().all(|byte| byte.is_ascii_graphic())
        {
            bail!("RISK_ENGINE_INTERNAL_TOKEN must be at least 24 printable ASCII bytes");
        }
        let config = Self {
            account_count: environment.account_count.unwrap_or(100_000),
            shard_count: environment.shard_count.unwrap_or_else(|| {
                std::thread::available_parallelism().map_or(1, |workers| workers.get().min(8))
            }),
            warning_health: environment.warning_health.unwrap_or(1_200_000),
            liquidation_health: environment.liquidation_health.unwrap_or(1_000_000),
            snapshot_dir: environment
                .snapshot_dir
                .context("RISK_ENGINE_SNAPSHOT_DIR is required")?,
            snapshot_interval: Duration::from_secs(
                environment.snapshot_interval_secs.unwrap_or(300),
            ),
            snapshot_event_count: environment.snapshot_event_count.unwrap_or(100_000),
            database_url: environment
                .database_url
                .or_else(|| std::env::var("DATABASE_URL").ok())
                .context("RISK_ENGINE_DATABASE_URL or DATABASE_URL is required")?,
            migration_database_url: environment.migration_database_url,
            ledger_refresh_interval: Duration::from_secs(
                environment.ledger_refresh_interval_secs.unwrap_or(5),
            ),
            nats_url: environment
                .nats_url
                .unwrap_or_else(|| "nats://127.0.0.1:4222".to_owned()),
            listen_addr: environment
                .listen_addr
                .unwrap_or_else(|| "127.0.0.1:8083".parse().expect("valid default address")),
            initial_generation_version: environment.initial_generation_version.unwrap_or(1),
            ingest_channel_capacity: environment.ingest_channel_capacity.unwrap_or(1024),
            stream_prefix: environment
                .stream_prefix
                .unwrap_or_else(|| tackt_nats::subjects::RISK_STREAM_PREFIX.to_owned()),
            internal_token_hash: Sha256::digest(internal_token.as_bytes()).into(),
            allow_cleartext: environment.allow_cleartext.unwrap_or(false),
            evaluation_channel_capacity: environment.evaluation_channel_capacity.unwrap_or(64),
            history_accounts: history_accounts(
                environment.history_accounts.as_deref().unwrap_or(""),
            )?,
            history_interval: Duration::from_secs(environment.history_interval_secs.unwrap_or(60)),
        };
        if config.account_count == 0 {
            bail!("RISK_ENGINE_ACCOUNT_COUNT must be at least 1");
        }
        if config.shard_count == 0 || config.shard_count > config.account_count {
            bail!("RISK_ENGINE_SHARD_COUNT must be within 1..=account_count");
        }
        if config.ledger_refresh_interval.is_zero() {
            bail!("RISK_ENGINE_LEDGER_REFRESH_INTERVAL_SECS must be positive");
        }
        if config.snapshot_interval.is_zero() {
            bail!("RISK_ENGINE_SNAPSHOT_INTERVAL_SECS must be at least 1");
        }
        if config.snapshot_event_count == 0 {
            bail!("RISK_ENGINE_SNAPSHOT_EVENT_COUNT must be at least 1");
        }
        if config.initial_generation_version == 0 {
            bail!("RISK_ENGINE_INITIAL_GENERATION_VERSION must be at least 1");
        }
        if config.ingest_channel_capacity == 0 {
            bail!("RISK_ENGINE_INGEST_CHANNEL_CAPACITY must be at least 1");
        }
        if config.stream_prefix.is_empty()
            || !config
                .stream_prefix
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            bail!("RISK_ENGINE_STREAM_PREFIX must be non-empty [A-Za-z0-9_-]");
        }
        if config.evaluation_channel_capacity == 0 {
            bail!("RISK_ENGINE_EVALUATION_CHANNEL_CAPACITY must be at least 1");
        }
        if !(1..=3600).contains(&config.history_interval.as_secs()) {
            bail!("RISK_ENGINE_HISTORY_INTERVAL_SECS must be within 1..=3600");
        }
        Ok(config)
    }

    /// Builds the risk policy these thresholds describe.
    pub fn policy(&self) -> anyhow::Result<RiskPolicy> {
        RiskPolicy::new(
            Factor::from_raw(i128::from(self.warning_health)),
            Factor::from_raw(i128::from(self.liquidation_health)),
        )
        .context("invalid risk policy configuration")
    }
}

fn history_accounts(value: &str) -> anyhow::Result<Vec<AccountSubject>> {
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }
    let accounts = value
        .split(',')
        .map(|id| AccountSubject::new(id.trim().parse()?).map_err(anyhow::Error::from))
        .collect::<anyhow::Result<Vec<_>>>()?;
    anyhow::ensure!(
        accounts.len() <= MAX_ACCOUNTS,
        "at most {MAX_ACCOUNTS} history accounts are supported"
    );
    let unique: std::collections::BTreeSet<_> = accounts.iter().collect();
    anyhow::ensure!(unique.len() == accounts.len(), "duplicate history account");
    Ok(accounts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_requires_bounded_unique_non_nil_subjects() {
        let id = uuid::Uuid::from_u128(1).to_string();
        assert!(history_accounts("").expect("disabled").is_empty());
        assert_eq!(history_accounts(&id).expect("one account").len(), 1);
        assert!(history_accounts(&format!("{id},{id}")).is_err());
        assert!(history_accounts("00000000-0000-0000-0000-000000000000").is_err());
        assert!(history_accounts("invalid").is_err());
        let too_many = (1..=33)
            .map(|id| uuid::Uuid::from_u128(id).to_string())
            .collect::<Vec<_>>()
            .join(",");
        assert!(history_accounts(&too_many).is_err());
    }
}

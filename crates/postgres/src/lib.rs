//! Canonical Postgres schema, migrations, and typed persistence operations.

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::{Executor, PgPool, Row, postgres::PgPoolOptions};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tackt_primitives::*;
use tackt_primitives::{
    accounts::{AccountSubject, SourceAccount},
    assets::AssetReference,
    ledger::LedgerAsset,
};
use tokio::sync::RwLock;
use uuid::Uuid;

mod ledger;
mod ledger_activity;
pub use ledger::{
    LedgerAccount, LedgerAccountDefinition, LedgerBalanceSnapshot, LedgerJournalEntry,
    LedgerPosting, LedgerPostingWrite, LedgerReservation, LedgerWrite, UserLedgerBalanceRead,
};
pub use ledger_activity::{
    LedgerActivityCursor, MAX_ACTIVITY_LIMIT, UserLedgerActivity, UserLedgerActivityRead,
    UserLedgerPosting,
};
mod actions;
pub use actions::{
    ActionEvaluationEvidence, ActionLedgerRequest, ActionLedgerStep, ActionRepositoryError,
    ActionRiskEvidence, ActionTransitionRequest, CoreAction, CoreActionTransition, NewCoreAction,
    TransitionResult,
};
pub use tackt_primitives::{CoreActionKind, CoreActionState};
mod observations;
pub use observations::{ChainState, Deposit, ObservationLease};
#[cfg(feature = "test-support")]
pub mod test_support;
mod users;
pub use users::UserRepositoryError;
mod signing_keys;
pub use signing_keys::{UserSigningKeys, UserWalletAddresses, WalletAddress};
mod auth;
pub use auth::{AuthSession, LoginIdentity};
mod risk_history;
pub use risk_history::{MAX_HISTORY_LIMIT, RiskHealthSample, UserRiskHealthRead};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HyperliquidAccountRegistration {
    pub user_id: Uuid,
    pub account: String,
    pub active_since: DateTime<Utc>,
}

#[async_trait]
pub trait Repository: Send + Sync + 'static {
    async fn users(&self) -> Result<Vec<User>>;
    /// Returns one Indexer-facing user projection for a lifecycle-event refresh.
    async fn user_registration(&self, user_id: Uuid) -> Result<Option<User>>;
    async fn hyperliquid_account_registrations(
        &self,
    ) -> Result<Vec<HyperliquidAccountRegistration>>;
    async fn user_by_account(&self, source: Source, account: &str) -> Result<Option<Uuid>>;
    async fn observations(
        &self,
        user_id: Uuid,
        source: Option<Source>,
        limit: u32,
    ) -> Result<Vec<Observation>>;
}

#[derive(Clone)]
pub struct PgRepository {
    pool: PgPool,
}

impl PgRepository {
    /// Check database readiness without exposing the connection pool.
    pub async fn is_ready(&self) -> bool {
        sqlx::query("SELECT 1").execute(&self.pool).await.is_ok()
    }

    /// Connect to Postgres and bring the shared schema to the required version.
    ///
    /// SQLx records each migration and checksum in `_sqlx_migrations`, so this is
    /// safe to call on every service startup. Its migration locking serializes
    /// concurrent initializers from multiple service instances.
    pub async fn initialise(url: &str, max_connections: u32) -> Result<Self> {
        let repository = Self::connect(url, max_connections).await?;
        repository.migrate().await?;
        Ok(repository)
    }

    pub async fn connect(url: &str, max_connections: u32) -> Result<Self> {
        enforce_transport_security(url)?;
        // Bounded waits everywhere: a wedged transaction holding a hot lock must surface as an
        // error instead of pinning every pool connection until /health fails.
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(10))
            .after_connect(|conn, _| {
                Box::pin(async move {
                    conn.execute("SET statement_timeout='30s';SET lock_timeout='10s'")
                        .await?;
                    Ok(())
                })
            })
            .connect(url)
            .await
            .context("connect PostgreSQL")?;
        Ok(Self { pool })
    }

    /// Apply every pending platform migration idempotently.
    pub async fn migrate(&self) -> Result<()> {
        sqlx::migrate!()
            .run(&self.pool)
            .await
            .context("run migrations")
    }
}

/// Refuses cleartext connections to remote PostgreSQL servers.
///
/// Every service reaches PostgreSQL through this crate, so the policy lives here
/// rather than in per-script shell checks that can drift or be bypassed. A
/// non-loopback, non-socket host requires `sslmode=verify-full`; `host`/`hostaddr`
/// query parameters are inspected too because they override the URL authority.
/// `TACKT_POSTGRES_ALLOW_INSECURE=true` opts a development environment out
/// explicitly and loudly.
fn enforce_transport_security(url: &str) -> Result<()> {
    let parsed = url::Url::parse(url).context("invalid PostgreSQL URL")?;
    let query: HashMap<_, _> = parsed.query_pairs().collect();
    // libpq-style overrides take precedence over the authority host.
    let host = query
        .get("hostaddr")
        .or_else(|| query.get("host"))
        .map(|v| v.to_string())
        .or_else(|| parsed.host_str().map(str::to_owned))
        .unwrap_or_default();
    let local = host.is_empty()
        // Unix socket paths, either literal (query parameters) or percent-encoded
        // (URL authority).
        || host.starts_with('/')
        || host.to_ascii_lowercase().starts_with("%2f")
        || host == "localhost"
        || host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());
    let verify_full = query.get("sslmode").map(|v| v.to_string()) == Some("verify-full".into());
    if local || verify_full {
        return Ok(());
    }
    if std::env::var("TACKT_POSTGRES_ALLOW_INSECURE").as_deref() == Ok("true") {
        tracing::warn!(
            host,
            "TACKT_POSTGRES_ALLOW_INSECURE=true: connecting to a remote PostgreSQL server without sslmode=verify-full"
        );
        return Ok(());
    }
    anyhow::bail!(
        "refusing cleartext connection to remote PostgreSQL host {host:?}: append sslmode=verify-full to DATABASE_URL, or set TACKT_POSTGRES_ALLOW_INSECURE=true in development only"
    )
}

fn source(s: &Source) -> &'static str {
    match s {
        Source::Ethereum => "ethereum",
        Source::Hyperliquid => "hyperliquid",
    }
}
fn parse_source(s: &str) -> Result<Source> {
    match s {
        "ethereum" => Ok(Source::Ethereum),
        "hyperliquid" => Ok(Source::Hyperliquid),
        _ => anyhow::bail!("unknown source {s}"),
    }
}

async fn insert_observation<'e, E>(
    executor: E,
    observation: &Observation,
    chain: Option<u64>,
) -> Result<bool>
where
    E: Executor<'e, Database = sqlx::Postgres>,
{
    let inserted = sqlx::query("INSERT INTO observations(event_id,source,kind,user_id,account,normalized_asset,asset,quantity,source_timestamp,observed_at,block_number,block_hash,confirmations,canonical,finalized,raw,provenance,chain_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18::numeric) ON CONFLICT(event_id) DO NOTHING")
        .bind(&observation.event_id).bind(observation.source.as_str()).bind(observation.kind.as_str())
        .bind(observation.account.as_ref().map(|account| account.subject().user_id()))
        .bind(observation.account.as_ref().map(SourceAccount::external_id))
        .bind(observation.asset.map(|asset| asset.asset().code()))
        .bind(&observation.external_asset).bind(&observation.quantity).bind(observation.source_timestamp).bind(observation.observed_at)
        .bind(observation.block_number.map(i64::try_from).transpose()?).bind(&observation.block_hash)
        .bind(observation.confirmations.map(i64::try_from).transpose()?).bind(observation.canonical).bind(observation.finalized)
        .bind(&observation.raw).bind(serde_json::to_value(&observation.provenance)?).bind(chain.map(|chain| chain.to_string()))
        .execute(executor).await?.rows_affected();
    Ok(inserted == 1)
}

#[async_trait]
impl Repository for PgRepository {
    async fn users(&self) -> Result<Vec<User>> {
        let rows = sqlx::query("SELECT u.id,u.external_id,u.enabled,COALESCE(array_agg(a.account) FILTER(WHERE a.source='ethereum'), '{}') eth,COALESCE(array_agg(a.account) FILTER(WHERE a.source='hyperliquid'), '{}') hl FROM users u LEFT JOIN user_accounts a ON a.user_id=u.id GROUP BY u.id ORDER BY u.external_id").fetch_all(&self.pool).await?;
        rows.into_iter().map(row_user).collect()
    }

    async fn user_registration(&self, user_id: Uuid) -> Result<Option<User>> {
        let row = sqlx::query("SELECT u.id,u.external_id,u.enabled,COALESCE(array_agg(a.account) FILTER(WHERE a.source='ethereum'), '{}') eth,COALESCE(array_agg(a.account) FILTER(WHERE a.source='hyperliquid'), '{}') hl FROM users u LEFT JOIN user_accounts a ON a.user_id=u.id WHERE u.id=$1 GROUP BY u.id")
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(row_user).transpose()
    }

    async fn hyperliquid_account_registrations(
        &self,
    ) -> Result<Vec<HyperliquidAccountRegistration>> {
        let rows = sqlx::query(
            "SELECT a.user_id,a.account,a.created_at active_since              FROM user_accounts a JOIN users u ON u.id=a.user_id              WHERE a.source='hyperliquid' AND u.enabled ORDER BY a.account",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(HyperliquidAccountRegistration {
                    user_id: row.try_get("user_id")?,
                    account: row.try_get("account")?,
                    active_since: row.try_get("active_since")?,
                })
            })
            .collect()
    }

    async fn user_by_account(&self, s: Source, account: &str) -> Result<Option<Uuid>> {
        Ok(sqlx::query_scalar("SELECT u.id FROM users u JOIN user_accounts a ON a.user_id=u.id WHERE a.source=$1::indexer_source AND a.account=$2 AND u.enabled")
            .bind(source(&s)).bind(account.to_lowercase()).fetch_optional(&self.pool).await?)
    }

    async fn observations(
        &self,
        user_id: Uuid,
        filter: Option<Source>,
        limit: u32,
    ) -> Result<Vec<Observation>> {
        let rows = sqlx::query("SELECT event_id,source::text,kind,user_id,account,normalized_asset,asset AS external_asset,quantity,source_timestamp,observed_at,block_number,block_hash,confirmations,canonical,finalized,raw,provenance FROM observations WHERE user_id=$1 AND ($2::text IS NULL OR source::text=$2) ORDER BY source_timestamp DESC,event_id LIMIT $3")
            .bind(user_id).bind(filter.as_ref().map(source)).bind(limit.min(1000) as i64).fetch_all(&self.pool).await?;
        rows.into_iter().map(row_observation).collect()
    }
}

fn row_observation(r: sqlx::postgres::PgRow) -> Result<Observation> {
    let source = parse_source(r.try_get("source")?)?;
    let user_id: Option<Uuid> = r.try_get("user_id")?;
    let external_account: Option<String> = r.try_get("account")?;
    let account = match (user_id, external_account) {
        (Some(user_id), Some(external_account)) => Some(SourceAccount::new(
            AccountSubject::new(user_id)?,
            source,
            &external_account,
        )?),
        (None, None) => None,
        _ => anyhow::bail!("persisted observation has a partial account identity"),
    };
    let asset_code: Option<String> = r.try_get("normalized_asset")?;
    let asset = asset_code
        .as_deref()
        .map(LedgerAsset::from_code)
        .transpose()?
        .map(AssetReference::new);
    let observation = Observation {
        event_id: r.try_get("event_id")?,
        source,
        kind: serde_json::from_value(json!(r.try_get::<String, _>("kind")?))?,
        account,
        asset,
        external_asset: r.try_get("external_asset")?,
        quantity: r.try_get("quantity")?,
        source_timestamp: r.try_get("source_timestamp")?,
        observed_at: r.try_get("observed_at")?,
        block_number: r
            .try_get::<Option<i64>, _>("block_number")?
            .map(|v| v as u64),
        block_hash: r.try_get("block_hash")?,
        confirmations: r
            .try_get::<Option<i64>, _>("confirmations")?
            .map(|v| v as u64),
        canonical: r.try_get("canonical")?,
        finalized: r.try_get("finalized")?,
        raw: r.try_get("raw")?,
        provenance: serde_json::from_value(r.try_get("provenance")?)?,
    };
    Ok(observation)
}

fn row_user(r: sqlx::postgres::PgRow) -> Result<User> {
    Ok(User {
        id: r.try_get("id")?,
        external_id: r.try_get("external_id")?,
        enabled: r.try_get("enabled")?,
        ethereum_addresses: r.try_get("eth")?,
        hyperliquid_accounts: r.try_get("hl")?,
    })
}

#[derive(Clone, Default)]
pub struct MemoryRepository {
    state: Arc<RwLock<MemoryState>>,
}

impl MemoryRepository {
    /// Seed the user projection directly. User writes are owned by the Core API;
    /// the [`Repository`] trait deliberately exposes no user mutation.
    pub async fn insert_user(&self, user: User) {
        self.state.write().await.users.insert(user.id, user);
    }
}
#[derive(Default)]
struct MemoryState {
    observations: HashMap<String, Observation>,
    users: HashMap<Uuid, User>,
}

#[async_trait]
impl Repository for MemoryRepository {
    async fn users(&self) -> Result<Vec<User>> {
        Ok(self.state.read().await.users.values().cloned().collect())
    }
    async fn user_registration(&self, user_id: Uuid) -> Result<Option<User>> {
        Ok(self.state.read().await.users.get(&user_id).cloned())
    }
    async fn hyperliquid_account_registrations(
        &self,
    ) -> Result<Vec<HyperliquidAccountRegistration>> {
        let state = self.state.read().await;
        Ok(state
            .users
            .values()
            .filter(|user| user.enabled)
            .flat_map(|user| {
                user.hyperliquid_accounts
                    .iter()
                    .map(|account| HyperliquidAccountRegistration {
                        user_id: user.id,
                        account: account.clone(),
                        active_since: DateTime::<Utc>::UNIX_EPOCH,
                    })
            })
            .collect())
    }

    async fn user_by_account(&self, src: Source, account: &str) -> Result<Option<Uuid>> {
        let a = account.to_lowercase();
        Ok(self
            .state
            .read()
            .await
            .users
            .values()
            .find(|u| {
                u.enabled
                    && match src {
                        Source::Ethereum => u.ethereum_addresses.contains(&a),
                        Source::Hyperliquid => u.hyperliquid_accounts.contains(&a),
                    }
            })
            .map(|u| u.id))
    }
    async fn observations(
        &self,
        user: Uuid,
        src: Option<Source>,
        limit: u32,
    ) -> Result<Vec<Observation>> {
        let mut v: Vec<_> = self
            .state
            .read()
            .await
            .observations
            .values()
            .filter(|o| {
                o.account
                    .as_ref()
                    .is_some_and(|account| account.subject().user_id() == user)
                    && src.as_ref().is_none_or(|s| &o.source == s)
            })
            .cloned()
            .collect();
        v.sort_by_key(|o| std::cmp::Reverse((o.source_timestamp, o.event_id.clone())));
        v.truncate(limit.min(1000) as usize);
        Ok(v)
    }
}

#[cfg(test)]
mod tests {
    use super::enforce_transport_security;

    #[test]
    fn permits_local_connections_without_tls() {
        for url in [
            "postgres://postgres:postgres@127.0.0.1:5432/postgres",
            "postgres://postgres@localhost/postgres",
            "postgres://postgres@[::1]:5432/postgres",
            "postgres://postgres@%2Fvar%2Frun%2Fpostgresql/postgres",
            "postgres:///postgres?host=/var/run/postgresql",
        ] {
            assert!(enforce_transport_security(url).is_ok(), "{url}");
        }
    }

    #[test]
    fn permits_remote_connections_with_verify_full() {
        assert!(
            enforce_transport_security(
                "postgres://app@db.internal.example:5432/tackt?sslmode=verify-full"
            )
            .is_ok()
        );
    }

    #[test]
    fn rejects_cleartext_remote_connections() {
        for url in [
            "postgres://app@db.internal.example:5432/tackt",
            "postgres://app@db.internal.example:5432/tackt?sslmode=require",
            "postgres://app@10.0.0.7:5432/tackt",
            // host overrides must not smuggle a remote target past a loopback authority
            "postgres://app@127.0.0.1:5432/tackt?host=db.internal.example",
            "postgres://app@127.0.0.1:5432/tackt?hostaddr=10.0.0.7",
        ] {
            assert!(enforce_transport_security(url).is_err(), "{url}");
        }
    }
}

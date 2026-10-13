use crate::{LedgerWrite, PgRepository};
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::Row;
use tackt_primitives::chain::BlockRef;
use tackt_primitives::{Observation, Provenance, ledger::LedgerAsset};
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq)]
pub struct ChainState {
    pub tip: BlockRef,
    pub finalized: BlockRef,
    pub sequence: u64,
}

pub struct ObservationLease(sqlx::pool::PoolConnection<sqlx::Postgres>);
impl ObservationLease {
    pub async fn check(&mut self) -> Result<()> {
        sqlx::query("SELECT 1").execute(&mut *self.0).await?;
        Ok(())
    }
}

/// Frozen deposit values, written in the same transaction as their accounting entries.
pub struct Deposit {
    pub event_id: String,
    pub chain_id: u64,
    pub block_number: u64,
    pub user_id: Uuid,
    pub asset: LedgerAsset,
    pub quantity: u128,
    pub effective_at: DateTime<Utc>,
    pub status: String,
}

impl PgRepository {
    pub async fn acquire_observer(&self) -> Result<ObservationLease> {
        let mut connection = self.pool.acquire().await?;
        connection.close_on_drop();
        let acquired: bool = sqlx::query_scalar(
            "SELECT pg_try_advisory_lock(hashtextextended('ledger-observer',0))",
        )
        .fetch_one(&mut *connection)
        .await?;
        anyhow::ensure!(acquired, "another Ledger observer is active");
        Ok(ObservationLease(connection))
    }
    pub async fn observation_sequence(&self, stream: &str) -> Result<u64> {
        let sequence: Option<String> = sqlx::query_scalar(
            "SELECT sequence::text FROM ledger.observation_cursor WHERE stream=$1",
        )
        .bind(stream)
        .fetch_optional(&self.pool)
        .await?;
        Ok(sequence
            .map(|value| value.parse())
            .transpose()?
            .unwrap_or(0))
    }
}

impl LedgerWrite {
    pub async fn chain_state(&mut self, chain: u64) -> Result<Option<ChainState>> {
        let row = sqlx::query("SELECT block_number,block_hash,finalized_number,finalized_hash,sequence::text FROM ledger.chain_state WHERE chain_id=$1::numeric")
            .bind(chain.to_string()).fetch_optional(&mut *self.0).await?;
        row.map(|row| {
            Ok(ChainState {
                tip: BlockRef {
                    number: u64::try_from(row.try_get::<i64, _>("block_number")?)?,
                    hash: row.try_get("block_hash")?,
                },
                finalized: BlockRef {
                    number: u64::try_from(row.try_get::<i64, _>("finalized_number")?)?,
                    hash: row.try_get("finalized_hash")?,
                },
                sequence: row.try_get::<String, _>("sequence")?.parse()?,
            })
        })
        .transpose()
    }

    pub async fn store_chain_state(&mut self, chain: u64, state: &ChainState) -> Result<()> {
        sqlx::query("INSERT INTO ledger.chain_state(chain_id,block_number,block_hash,finalized_number,finalized_hash,sequence) VALUES($1::numeric,$2,$3,$4,$5,$6::numeric) ON CONFLICT(chain_id) DO UPDATE SET block_number=excluded.block_number,block_hash=excluded.block_hash,finalized_number=excluded.finalized_number,finalized_hash=excluded.finalized_hash,sequence=excluded.sequence")
            .bind(chain.to_string()).bind(i64::try_from(state.tip.number)?).bind(&state.tip.hash)
            .bind(i64::try_from(state.finalized.number)?).bind(&state.finalized.hash).bind(state.sequence.to_string())
            .execute(&mut *self.0).await?;
        Ok(())
    }

    /// Bulk persistence of the canonicality/finality facts supplied by Ledger.
    pub async fn store_observations(
        &mut self,
        observations: &[&Observation],
        chain: u64,
    ) -> Result<()> {
        let rows = observations
            .iter()
            .map(|observation| ObservationRow::new(observation, chain))
            .collect::<Vec<_>>();
        sqlx::query("INSERT INTO observations(event_id,source,kind,user_id,account,normalized_asset,asset,quantity,source_timestamp,observed_at,block_number,block_hash,confirmations,canonical,finalized,raw,provenance,chain_id) SELECT * FROM jsonb_to_recordset($1) AS r(event_id text,source text,kind text,user_id uuid,account text,normalized_asset text,asset text,quantity text,source_timestamp timestamptz,observed_at timestamptz,block_number bigint,block_hash text,confirmations bigint,canonical boolean,finalized boolean,raw jsonb,provenance jsonb,chain_id numeric) ON CONFLICT(event_id) DO UPDATE SET canonical=excluded.canonical,finalized=excluded.finalized")
            .bind(serde_json::to_value(rows)?).execute(&mut *self.0).await?;
        Ok(())
    }

    pub async fn deposit(&mut self, event_id: &str) -> Result<Option<Deposit>> {
        sqlx::query(&format!("{DEPOSIT_COLUMNS} WHERE event_id=$1"))
            .bind(event_id)
            .fetch_optional(&mut *self.0)
            .await?
            .map(deposit_row)
            .transpose()
    }

    pub async fn observation_stream_created_at(&mut self, stream: &str) -> Result<Option<String>> {
        Ok(sqlx::query_scalar(
            "SELECT stream_created_at FROM ledger.observation_cursor WHERE stream=$1",
        )
        .bind(stream)
        .fetch_optional(&mut *self.0)
        .await?
        .flatten())
    }

    pub async fn store_observation_stream_created_at(
        &mut self,
        stream: &str,
        created_at: &str,
    ) -> Result<()> {
        sqlx::query("INSERT INTO ledger.observation_cursor(stream,sequence,stream_created_at) VALUES($1,0,$2) ON CONFLICT(stream) DO UPDATE SET stream_created_at=excluded.stream_created_at")
            .bind(stream).bind(created_at).execute(&mut *self.0).await?;
        Ok(())
    }

    pub async fn observation_sequence(&mut self, stream: &str) -> Result<u64> {
        let sequence: Option<String> = sqlx::query_scalar(
            "SELECT sequence::text FROM ledger.observation_cursor WHERE stream=$1",
        )
        .bind(stream)
        .fetch_optional(&mut *self.0)
        .await?;
        Ok(sequence
            .map(|value| value.parse())
            .transpose()?
            .unwrap_or(0))
    }

    pub async fn store_observation(
        &mut self,
        observation: &Observation,
        chain: Option<u64>,
    ) -> Result<bool> {
        crate::insert_observation(&mut *self.0, observation, chain).await
    }

    pub async fn store_observation_sequence(&mut self, stream: &str, sequence: u64) -> Result<()> {
        sqlx::query("INSERT INTO ledger.observation_cursor(stream,sequence) VALUES($1,$2::numeric) ON CONFLICT(stream) DO UPDATE SET sequence=excluded.sequence")
            .bind(stream).bind(sequence.to_string()).execute(&mut *self.0).await?;
        Ok(())
    }

    pub async fn deposits(
        &mut self,
        chain: u64,
        from: u64,
        through: u64,
        statuses: &[&str],
    ) -> Result<Vec<Deposit>> {
        sqlx::query(&format!("{DEPOSIT_COLUMNS} WHERE chain_id=$1::numeric AND block_number BETWEEN $2 AND $3 AND status=ANY($4) ORDER BY block_number,event_id"))
            .bind(chain.to_string()).bind(i64::try_from(from)?).bind(i64::try_from(through)?).bind(statuses)
            .fetch_all(&mut *self.0).await?.into_iter().map(deposit_row).collect()
    }

    pub async fn store_deposit(&mut self, deposit: &Deposit, detail: Option<&str>) -> Result<()> {
        sqlx::query("INSERT INTO ledger.deposits(event_id,chain_id,block_number,user_id,asset,quantity,effective_at,status,detail) VALUES($1,$2::numeric,$3,$4,$5,$6::numeric,$7,$8,$9) ON CONFLICT(event_id) DO UPDATE SET status=excluded.status,detail=excluded.detail")
            .bind(&deposit.event_id).bind(deposit.chain_id.to_string()).bind(i64::try_from(deposit.block_number)?)
            .bind(deposit.user_id).bind(deposit.asset.code()).bind(deposit.quantity.to_string())
            .bind(deposit.effective_at).bind(&deposit.status).bind(detail).execute(&mut *self.0).await?;
        Ok(())
    }

    pub async fn tracked_sender(&mut self, address: &str) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM user_accounts WHERE source='ethereum' AND account=$1)",
        )
        .bind(address)
        .fetch_one(&mut *self.0)
        .await?)
    }

    /// The caller has already selected the orphaned range from trusted block facts.
    pub async fn mark_orphaned(&mut self, chain: u64, from: u64) -> Result<()> {
        sqlx::query("UPDATE observations SET canonical=false,finalized=false WHERE chain_id=$1::numeric AND block_number >= $2 AND canonical")
            .bind(chain.to_string()).bind(i64::try_from(from)?).execute(&mut *self.0).await?;
        Ok(())
    }

    pub async fn mark_finalized(&mut self, chain: u64, through: u64) -> Result<()> {
        sqlx::query("UPDATE observations SET finalized=true WHERE chain_id=$1::numeric AND block_number<=$2 AND canonical AND NOT finalized")
            .bind(chain.to_string()).bind(i64::try_from(through)?).execute(&mut *self.0).await?;
        Ok(())
    }

    /// Keep the observation even if its accounting consequence needs operator review.
    pub async fn accounting_savepoint(&mut self) -> Result<()> {
        sqlx::query("SAVEPOINT accounting")
            .execute(&mut *self.0)
            .await?;
        Ok(())
    }
    pub async fn finish_accounting(&mut self, rollback: bool) -> Result<()> {
        if rollback {
            sqlx::query("ROLLBACK TO SAVEPOINT accounting")
                .execute(&mut *self.0)
                .await?;
        }
        sqlx::query("RELEASE SAVEPOINT accounting")
            .execute(&mut *self.0)
            .await?;
        Ok(())
    }
}

const DEPOSIT_COLUMNS: &str = "SELECT event_id,chain_id::text,block_number,user_id,asset,quantity::text,effective_at,status FROM ledger.deposits";

fn deposit_row(row: sqlx::postgres::PgRow) -> Result<Deposit> {
    Ok(Deposit {
        event_id: row.try_get("event_id")?,
        chain_id: row.try_get::<String, _>("chain_id")?.parse()?,
        block_number: u64::try_from(row.try_get::<i64, _>("block_number")?)?,
        user_id: row.try_get("user_id")?,
        asset: crate::ledger::parse_asset(row.try_get("asset")?)?,
        quantity: row.try_get::<String, _>("quantity")?.parse()?,
        effective_at: row.try_get("effective_at")?,
        status: row.try_get("status")?,
    })
}

/// One `observations` row as `jsonb_to_recordset` consumes it; field order matches the column list.
#[derive(Serialize)]
struct ObservationRow<'a> {
    event_id: &'a str,
    source: &'a str,
    kind: &'a str,
    user_id: Option<Uuid>,
    account: Option<&'a str>,
    normalized_asset: Option<&'a str>,
    asset: Option<&'a str>,
    quantity: Option<&'a str>,
    source_timestamp: DateTime<Utc>,
    observed_at: DateTime<Utc>,
    block_number: Option<u64>,
    block_hash: Option<&'a str>,
    confirmations: Option<u64>,
    canonical: bool,
    finalized: bool,
    raw: &'a serde_json::Value,
    provenance: &'a Provenance,
    chain_id: u64,
}

impl<'a> ObservationRow<'a> {
    fn new(o: &'a Observation, chain: u64) -> Self {
        Self {
            event_id: &o.event_id,
            source: o.source.as_str(),
            kind: o.kind.as_str(),
            user_id: o.account.as_ref().map(|a| a.subject().user_id()),
            account: o.account.as_ref().map(|a| a.external_id()),
            normalized_asset: o.asset.map(|a| a.asset().code()),
            asset: o.external_asset.as_deref(),
            quantity: o.quantity.as_deref(),
            source_timestamp: o.source_timestamp,
            observed_at: o.observed_at,
            block_number: o.block_number,
            block_hash: o.block_hash.as_deref(),
            confirmations: o.confirmations,
            canonical: o.canonical,
            finalized: o.finalized,
            raw: &o.raw,
            provenance: &o.provenance,
            chain_id: chain,
        }
    }
}

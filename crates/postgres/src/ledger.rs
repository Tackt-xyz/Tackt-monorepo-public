//! Ledger persistence. Accounting decisions belong to the Ledger service.

use crate::PgRepository;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{Postgres, Row, Transaction};
use std::collections::{BTreeMap, HashMap};
use tackt_primitives::ledger::*;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LedgerAccountDefinition {
    pub code: String,
    pub user_id: Option<Uuid>,
    pub bucket: String,
    pub asset: LedgerAsset,
    pub allow_negative: bool,
}

/// Outcome of an atomic lifecycle check and user balance read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UserLedgerBalanceRead {
    UserNotFound,
    UserClosed,
    Available(UserLedgerBalanceSnapshot),
}

pub struct LedgerAccount {
    pub id: Uuid,
    pub definition: LedgerAccountDefinition,
    pub debit: u128,
    pub credit: u128,
}

pub struct LedgerPostingWrite {
    pub account_id: Uuid,
    pub asset: LedgerAsset,
    pub side: PostingSide,
    pub quantity: u128,
}

pub struct LedgerPosting {
    pub account: LedgerAccountDefinition,
    pub side: PostingSide,
    pub quantity: u128,
}

pub struct LedgerReservation {
    pub id: Uuid,
    pub user_id: Uuid,
    pub kind: String,
    pub destination: Option<String>,
    pub asset: LedgerAsset,
    pub original_quantity: u128,
    pub remaining_quantity: u128,
    pub status: String,
    pub created_transaction_id: Uuid,
}

pub struct LedgerJournalEntry<'a> {
    pub source: LedgerCommandSource,
    pub chain_id: u64,
    pub kind: &'a str,
    pub hash: &'a str,
    pub idempotency_key: &'a str,
    pub external_reference: Option<&'a str>,
    pub effective_at: DateTime<Utc>,
    pub audit: &'a Value,
    pub reverses: Option<Uuid>,
}

/// One consistent read of all user-owned Ledger balance projections.
/// Quantities retain Ledger's sign and atomic units, including booked principal liabilities.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LedgerBalanceSnapshot {
    /// Database observation time shared by every balance in this snapshot.
    pub as_of: DateTime<Utc>,
    /// Complete balance sets for every user with a Ledger balance projection.
    /// System accounts are excluded; no collateral or lending policy is applied here.
    pub accounts: BTreeMap<Uuid, Vec<LedgerBalance>>,
}

/// An opaque storage capability, not a database handle. Dropping it rolls back.
pub struct LedgerWrite(pub(super) Transaction<'static, Postgres>);

impl PgRepository {
    pub async fn ledger_health(&self) -> Result<()> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    pub async fn begin_ledger_write(&self) -> Result<LedgerWrite> {
        let mut tx = self.pool.begin().await?;
        // Serialize all replicas, including keys with no row to lock yet.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('ledger-accounting',0))")
            .execute(&mut *tx)
            .await?;
        Ok(LedgerWrite(tx))
    }
}

impl LedgerWrite {
    pub async fn existing_command(&mut self, key: &str) -> Result<Option<(CommandResult, String)>> {
        sqlx::query(
            "SELECT id,recorded_at,command_hash FROM ledger.transactions WHERE idempotency_key=$1",
        )
        .bind(key)
        .fetch_optional(&mut *self.0)
        .await?
        .map(|row| {
            Ok((
                CommandResult {
                    transaction_id: row.try_get("id")?,
                    recorded_at: row.try_get("recorded_at")?,
                    duplicate: true,
                },
                row.try_get("command_hash")?,
            ))
        })
        .transpose()
    }

    pub async fn external_reference_exists(
        &mut self,
        source: LedgerCommandSource,
        chain: u64,
        reference: &str,
    ) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM ledger.transactions WHERE command_source=$1 AND chain_id=$2::numeric AND external_reference=$3)")
            .bind(source.code()).bind(chain.to_string()).bind(reference).fetch_one(&mut *self.0).await?)
    }

    pub async fn transaction_kind(&mut self, id: Uuid) -> Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT command_kind FROM ledger.transactions WHERE id=$1")
                .bind(id)
                .fetch_optional(&mut *self.0)
                .await?,
        )
    }

    pub async fn reversal_exists(&mut self, id: Uuid) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM ledger.transactions WHERE reverses_transaction_id=$1)",
        )
        .bind(id)
        .fetch_one(&mut *self.0)
        .await?)
    }

    pub async fn postings(&mut self, transaction: Uuid) -> Result<Vec<LedgerPosting>> {
        sqlx::query("SELECT a.code,a.user_id,a.bucket,p.asset,p.side,p.quantity::text,a.allow_negative FROM ledger.postings p JOIN ledger.accounts a ON a.id=p.account_id WHERE p.transaction_id=$1 ORDER BY p.id")
            .bind(transaction).fetch_all(&mut *self.0).await?.into_iter()
            .map(|row| Ok(LedgerPosting {
                account: account_definition(&row)?, side: parse_side(row.try_get("side")?)?,
                quantity: row.try_get::<String,_>("quantity")?.parse()?,
            })).collect()
    }

    pub async fn accounts(&mut self, codes: &[&str]) -> Result<HashMap<String, LedgerAccount>> {
        sqlx::query("SELECT a.id,a.code,a.user_id,a.bucket,a.asset,a.allow_negative,COALESCE(b.debit_quantity,0)::text debit,COALESCE(b.credit_quantity,0)::text credit FROM ledger.accounts a LEFT JOIN ledger.current_balances b ON b.account_id=a.id WHERE a.code=ANY($1)")
            .bind(codes).fetch_all(&mut *self.0).await?.into_iter().map(|row| {
                let account = LedgerAccount { id: row.try_get("id")?, definition: account_definition(&row)?,
                    debit: row.try_get::<String,_>("debit")?.parse()?, credit: row.try_get::<String,_>("credit")?.parse()? };
                Ok((account.definition.code.clone(), account))
            }).collect()
    }

    pub async fn append_transaction(
        &mut self,
        entry: LedgerJournalEntry<'_>,
    ) -> Result<CommandResult> {
        let row = sqlx::query("INSERT INTO ledger.transactions(command_source,chain_id,command_kind,command_hash,idempotency_key,external_reference,effective_at,audit,reverses_transaction_id) VALUES($1,$2::numeric,$3,$4,$5,$6,$7,$8,$9) RETURNING id,recorded_at")
            .bind(entry.source.code()).bind(entry.chain_id.to_string()).bind(entry.kind).bind(entry.hash)
            .bind(entry.idempotency_key).bind(entry.external_reference).bind(entry.effective_at)
            .bind(entry.audit).bind(entry.reverses).fetch_one(&mut *self.0).await?;
        Ok(CommandResult {
            transaction_id: row.try_get("id")?,
            recorded_at: row.try_get("recorded_at")?,
            duplicate: false,
        })
    }

    /// Persist already-decided account totals in one round trip, independent of posting count.
    pub async fn store_accounts(
        &mut self,
        accounts: &[LedgerAccount],
        at: DateTime<Utc>,
    ) -> Result<()> {
        let mut query = sqlx::QueryBuilder::<Postgres>::new(
            "WITH input(id,code,user_id,bucket,asset,allow_negative,debit,credit) AS (",
        );
        query.push_values(accounts, |mut row, account| {
            row.push_bind(account.id)
                .push_bind(&account.definition.code)
                .push_bind(account.definition.user_id)
                .push_bind(&account.definition.bucket)
                .push_bind(account.definition.asset.code())
                .push_bind(account.definition.allow_negative)
                .push_bind(account.debit.to_string())
                .push_unseparated("::numeric")
                .push_bind(account.credit.to_string())
                .push_unseparated("::numeric");
        });
        query.push("), inserted AS (INSERT INTO ledger.accounts(id,code,user_id,bucket,asset,allow_negative) SELECT id,code,user_id,bucket,asset,allow_negative FROM input ON CONFLICT(id) DO NOTHING) INSERT INTO ledger.current_balances(account_id,debit_quantity,credit_quantity,updated_at) SELECT id,debit,credit,")
            .push_bind(at).push(" FROM input ON CONFLICT(account_id) DO UPDATE SET debit_quantity=excluded.debit_quantity,credit_quantity=excluded.credit_quantity,updated_at=excluded.updated_at");
        query.build().execute(&mut *self.0).await?;
        Ok(())
    }

    pub async fn append_postings(
        &mut self,
        transaction: Uuid,
        postings: &[LedgerPostingWrite],
    ) -> Result<()> {
        let mut query = sqlx::QueryBuilder::<Postgres>::new(
            "INSERT INTO ledger.postings(transaction_id,account_id,asset,side,quantity) ",
        );
        query.push_values(postings, |mut row, posting| {
            row.push_bind(transaction)
                .push_bind(posting.account_id)
                .push_bind(posting.asset.code())
                .push_bind(posting.side.as_ref())
                .push_bind(posting.quantity.to_string())
                .push_unseparated("::numeric");
        });
        query.build().execute(&mut *self.0).await?;
        Ok(())
    }

    pub async fn reservation(&mut self, id: Uuid) -> Result<Option<LedgerReservation>> {
        sqlx::query("SELECT id,user_id,kind,destination,asset,original_quantity::text,remaining_quantity::text,status,created_transaction_id FROM ledger.reservations WHERE id=$1")
            .bind(id).fetch_optional(&mut *self.0).await?
            .map(|row| Ok(LedgerReservation {
                id: row.try_get("id")?, user_id: row.try_get("user_id")?, kind: row.try_get("kind")?,
                destination: row.try_get("destination")?, asset: parse_asset(row.try_get("asset")?)?,
                original_quantity: row.try_get::<String,_>("original_quantity")?.parse()?,
                remaining_quantity: row.try_get::<String,_>("remaining_quantity")?.parse()?,
                status: row.try_get("status")?, created_transaction_id: row.try_get("created_transaction_id")?,
            })).transpose()
    }

    pub async fn store_reservation(
        &mut self,
        value: &LedgerReservation,
        at: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query("INSERT INTO ledger.reservations(id,user_id,kind,destination,asset,original_quantity,remaining_quantity,status,created_transaction_id,updated_at) VALUES($1,$2,$3,$4,$5,$6::numeric,$7::numeric,$8,$9,$10) ON CONFLICT(id) DO UPDATE SET remaining_quantity=excluded.remaining_quantity,status=excluded.status,updated_at=excluded.updated_at")
            .bind(value.id).bind(value.user_id).bind(&value.kind).bind(&value.destination).bind(value.asset.code())
            .bind(value.original_quantity.to_string()).bind(value.remaining_quantity.to_string()).bind(&value.status)
            .bind(value.created_transaction_id).bind(at).execute(&mut *self.0).await?;
        Ok(())
    }

    pub async fn commit(self) -> Result<()> {
        self.0.commit().await?;
        Ok(())
    }
}

fn account_definition(row: &sqlx::postgres::PgRow) -> Result<LedgerAccountDefinition> {
    Ok(LedgerAccountDefinition {
        code: row.try_get("code")?,
        user_id: row.try_get("user_id")?,
        bucket: row.try_get("bucket")?,
        asset: parse_asset(row.try_get("asset")?)?,
        allow_negative: row.try_get("allow_negative")?,
    })
}

fn parse_side(value: &str) -> Result<PostingSide> {
    value
        .parse()
        .map_err(|_| anyhow::anyhow!("stored posting side is invalid: {value}"))
}

impl PgRepository {
    /// Check lifecycle and read current totals in one statement and database snapshot.
    /// A closure committed before this statement cannot expose balances through a stale check.
    /// Uses the maintained balance projection; never scans the journal or nets quantities.
    pub async fn user_ledger_balance_snapshot(
        &self,
        user_id: Uuid,
    ) -> Result<UserLedgerBalanceRead> {
        let rows = sqlx::query(
            "WITH balances AS (
                SELECT a.asset, a.bucket, b.debit_quantity::text AS debit_quantity,
                       b.credit_quantity::text AS credit_quantity, b.updated_at
                FROM ledger.accounts a
                JOIN ledger.current_balances b ON b.account_id = a.id
                WHERE a.user_id = $1
            )
            SELECT statement_timestamp() AS as_of, u.status::text AS user_status, balances.*
            FROM users u
            LEFT JOIN balances ON u.status <> 'closed'
            WHERE u.id = $1
            ORDER BY balances.asset, balances.bucket",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;
        let Some(first) = rows.first() else {
            return Ok(UserLedgerBalanceRead::UserNotFound);
        };
        if crate::users::parse_status(first.try_get("user_status")?)?
            == tackt_primitives::UserStatus::Closed
        {
            return Ok(UserLedgerBalanceRead::UserClosed);
        }
        // The left join supplies a timestamp for an open user with no balances.
        let as_of = first.try_get("as_of")?;
        let mut balances = Vec::with_capacity(rows.len());
        for row in rows {
            let Some(asset) = row.try_get::<Option<String>, _>("asset")? else {
                continue;
            };
            let bucket: String = row.try_get("bucket")?;
            balances.push(LedgerBalanceTotals {
                asset: parse_asset(&asset)?,
                bucket: bucket
                    .parse()
                    .map_err(|_| anyhow::anyhow!("stored account bucket is invalid"))?,
                debit_quantity: row.try_get::<String, _>("debit_quantity")?.parse()?,
                credit_quantity: row.try_get::<String, _>("credit_quantity")?.parse()?,
                updated_at: row.try_get("updated_at")?,
            });
        }
        Ok(UserLedgerBalanceRead::Available(
            UserLedgerBalanceSnapshot { as_of, balances },
        ))
    }

    pub async fn ledger_balances(
        &self,
        user_id: Uuid,
        as_of: Option<DateTime<Utc>>,
        time_basis: BalanceTimeBasis,
    ) -> Result<Vec<LedgerBalance>, anyhow::Error> {
        let query = match (as_of, time_basis) {
            (None, _) => {
                "SELECT a.asset::text,a.bucket::text bucket,(b.debit_quantity-b.credit_quantity)::text quantity,clock_timestamp() as as_of FROM ledger.accounts a JOIN ledger.current_balances b ON b.account_id=a.id WHERE a.user_id=$1"
            }
            (Some(_), BalanceTimeBasis::Recorded) => {
                "SELECT a.asset::text,a.bucket::text bucket,COALESCE(sum(CASE WHEN t.id IS NULL THEN 0 WHEN p.side='debit' THEN p.quantity ELSE -p.quantity END),0)::text quantity,$2 as as_of FROM ledger.accounts a LEFT JOIN ledger.postings p ON p.account_id=a.id LEFT JOIN ledger.transactions t ON t.id=p.transaction_id AND t.recorded_at<=$2 WHERE a.user_id=$1 GROUP BY a.id,a.asset,a.bucket"
            }
            (Some(_), BalanceTimeBasis::Effective) => {
                "SELECT a.asset::text,a.bucket::text bucket,COALESCE(sum(CASE WHEN t.id IS NULL THEN 0 WHEN p.side='debit' THEN p.quantity ELSE -p.quantity END),0)::text quantity,$2 as as_of FROM ledger.accounts a LEFT JOIN ledger.postings p ON p.account_id=a.id LEFT JOIN ledger.transactions t ON t.id=p.transaction_id AND t.effective_at<=$2 WHERE a.user_id=$1 GROUP BY a.id,a.asset,a.bucket"
            }
        };
        let mut query = sqlx::query(query).bind(user_id);
        if let Some(as_of) = as_of {
            query = query.bind(as_of);
        }
        let rows = query.fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|row| row_balance(row, user_id, time_basis))
            .collect()
    }

    /// Loads current balances for Risk startup and refresh without taking a Ledger writer lock.
    /// All accounts and the read timestamp belong to the same read-only database snapshot.
    pub async fn ledger_balance_snapshot(&self) -> Result<LedgerBalanceSnapshot> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *transaction)
            .await?;
        let as_of: DateTime<Utc> = sqlx::query_scalar("SELECT transaction_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let rows = sqlx::query(
            "SELECT a.user_id,a.asset::text,a.bucket::text bucket,(b.debit_quantity-b.credit_quantity)::text quantity,$1::timestamptz as as_of FROM ledger.accounts a JOIN ledger.current_balances b ON b.account_id=a.id WHERE a.user_id IS NOT NULL ORDER BY a.user_id,a.asset,a.bucket,a.id",
        )
        .bind(as_of)
        .fetch_all(&mut *transaction)
        .await?;
        let mut accounts = BTreeMap::<Uuid, Vec<LedgerBalance>>::new();
        for row in rows {
            let user_id = row.try_get("user_id")?;
            accounts.entry(user_id).or_default().push(row_balance(
                row,
                user_id,
                BalanceTimeBasis::Recorded,
            )?);
        }
        transaction.commit().await?;
        Ok(LedgerBalanceSnapshot { as_of, accounts })
    }

    pub async fn ledger_export(&self, limit: u32) -> Result<LedgerExport, anyhow::Error> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *transaction)
            .await?;
        let txs=sqlx::query("SELECT id,command_source::text,chain_id::text,command_kind,idempotency_key,external_reference,recorded_at,effective_at,reverses_transaction_id,audit FROM ledger.transactions ORDER BY recorded_at,id LIMIT $1")
            .bind(i64::from(limit))
            .fetch_all(&mut *transaction)
            .await?;
        let transaction_ids = txs
            .iter()
            .map(|row| row.try_get("id"))
            .collect::<Result<Vec<Uuid>, _>>()?;
        let rows=sqlx::query("SELECT p.transaction_id,a.code,p.asset,p.side::text,p.quantity::text FROM ledger.postings p JOIN ledger.accounts a ON a.id=p.account_id WHERE p.transaction_id=ANY($1) ORDER BY p.transaction_id,p.id")
            .bind(&transaction_ids)
            .fetch_all(&mut *transaction)
            .await?;
        let mut postings_by_transaction: HashMap<Uuid, Vec<ExportPosting>> = HashMap::new();
        for row in rows {
            postings_by_transaction
                .entry(row.try_get("transaction_id")?)
                .or_default()
                .push(ExportPosting {
                    account_code: row.try_get("code")?,
                    asset: parse_asset(row.try_get("asset")?)?,
                    side: parse_side(row.try_get("side")?)?,
                    quantity: row
                        .try_get::<String, _>("quantity")?
                        .parse()
                        .map_err(|_| anyhow::anyhow!("stored quantity is invalid"))?,
                });
        }
        let mut transactions = Vec::new();
        for t in txs {
            let id = t.try_get("id")?;
            transactions.push(ExportTransaction {
                id,
                source: parse_command_source(t.try_get("command_source")?)?,
                chain_id: t
                    .try_get::<String, _>("chain_id")?
                    .parse()
                    .map_err(|_| anyhow::Error::msg("stored chain id is invalid"))?,
                command_kind: t.try_get("command_kind")?,
                idempotency_key: t.try_get("idempotency_key")?,
                external_reference: t.try_get("external_reference")?,
                recorded_at: t.try_get("recorded_at")?,
                effective_at: t.try_get("effective_at")?,
                reverses_transaction_id: t.try_get("reverses_transaction_id")?,
                audit: t.try_get("audit")?,
                postings: postings_by_transaction.remove(&id).unwrap_or_default(),
            });
        }
        let export = LedgerExport {
            exported_at: Utc::now(),
            transactions,
        };
        transaction.commit().await?;
        Ok(export)
    }
}

/// Resolves a stored asset code through the locked asset registry, so a row written under a
/// code this build no longer supports surfaces as a conflict instead of being interpreted.
pub(crate) fn parse_asset(v: &str) -> Result<LedgerAsset, anyhow::Error> {
    LedgerAsset::from_code(v).map_err(|error| anyhow::Error::msg(error.to_string()))
}
fn parse_command_source(value: &str) -> Result<LedgerCommandSource, anyhow::Error> {
    value
        .parse()
        .map_err(|_| anyhow::anyhow!("stored command source is invalid: {value}"))
}

fn row_balance(
    row: sqlx::postgres::PgRow,
    user_id: Uuid,
    time_basis: BalanceTimeBasis,
) -> Result<LedgerBalance> {
    let bucket_name: String = row.try_get("bucket")?;
    let bucket = bucket_name
        .parse()
        .map_err(|_| anyhow::anyhow!("stored account bucket is invalid: {bucket_name}"))?;
    Ok(LedgerBalance {
        user_id,
        asset: parse_asset(row.try_get("asset")?)?,
        bucket,
        quantity: row
            .try_get::<String, _>("quantity")?
            .parse()
            .map_err(|_| anyhow::anyhow!("stored quantity is invalid"))?,
        as_of: row.try_get("as_of")?,
        time_basis,
    })
}

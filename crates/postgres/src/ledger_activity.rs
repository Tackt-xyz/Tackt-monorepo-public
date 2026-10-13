//! User-scoped journal reads. This module preserves postings without interpreting accounting.

use anyhow::{Result, ensure};
use chrono::{DateTime, Utc};
use sqlx::Row;
use tackt_primitives::{
    UserStatus,
    ledger::{BalanceBucket, LedgerAsset, PostingSide},
};
use uuid::Uuid;

use crate::PgRepository;

pub const MAX_ACTIVITY_LIMIT: u32 = 100;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LedgerActivityCursor {
    pub recorded_at: DateTime<Utc>,
    pub transaction_id: Uuid,
}

#[derive(Debug)]
pub struct UserLedgerActivity {
    pub id: Uuid,
    pub command_kind: String,
    pub recorded_at: DateTime<Utc>,
    pub effective_at: DateTime<Utc>,
    pub reverses_transaction_id: Option<Uuid>,
    pub postings: Vec<UserLedgerPosting>,
}

#[derive(Debug)]
pub struct UserLedgerPosting {
    pub asset: LedgerAsset,
    pub bucket: BalanceBucket,
    pub side: PostingSide,
    pub quantity: u128,
}

#[derive(Debug)]
pub enum UserLedgerActivityRead {
    UserNotFound,
    UserClosed,
    Available(Vec<UserLedgerActivity>),
}

impl PgRepository {
    /// Newest transactions first, with all and only the user's postings for each transaction.
    /// Accepts one extra transaction for next-page detection. Lifecycle and journal share one
    /// statement snapshot; pagination happens before joining postings so no transaction is split.
    pub async fn user_ledger_activity(
        &self,
        user: Uuid,
        before: Option<LedgerActivityCursor>,
        limit: u32,
    ) -> Result<UserLedgerActivityRead> {
        ensure!(
            (1..=MAX_ACTIVITY_LIMIT + 1).contains(&limit),
            "invalid activity limit"
        );
        let rows = sqlx::query(
            "SELECT u.status::text AS user_status, t.id, t.command_kind, t.recorded_at,
                    t.effective_at, t.reverses_transaction_id,
                    p.asset, p.bucket, p.side, p.quantity
             FROM users u
             LEFT JOIN LATERAL (
                 SELECT t.id,t.command_kind,t.recorded_at,t.effective_at,t.reverses_transaction_id
                 FROM ledger.transactions t
                 WHERE u.status <> 'closed'
                   AND ($2::timestamptz IS NULL OR (t.recorded_at,t.id) < ($2,$3::uuid))
                   AND EXISTS (
                       SELECT 1 FROM ledger.postings p
                       JOIN ledger.accounts a ON a.id=p.account_id
                       WHERE p.transaction_id=t.id AND a.user_id=u.id)
                 ORDER BY t.recorded_at DESC,t.id DESC LIMIT $4
             ) t ON true
             LEFT JOIN LATERAL (
                 SELECT p.id,p.asset,a.bucket,p.side,p.quantity::text AS quantity
                 FROM ledger.postings p JOIN ledger.accounts a ON a.id=p.account_id
                 WHERE p.transaction_id=t.id AND a.user_id=u.id
             ) p ON true
             WHERE u.id=$1 ORDER BY t.recorded_at DESC,t.id DESC,p.id",
        )
        .bind(user)
        .bind(before.map(|cursor| cursor.recorded_at))
        .bind(before.map(|cursor| cursor.transaction_id))
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        let Some(first) = rows.first() else {
            return Ok(UserLedgerActivityRead::UserNotFound);
        };
        if crate::users::parse_status(first.try_get("user_status")?)? == UserStatus::Closed {
            return Ok(UserLedgerActivityRead::UserClosed);
        }
        let mut entries: Vec<UserLedgerActivity> = Vec::new();
        for row in rows {
            let Some(id) = row.try_get::<Option<Uuid>, _>("id")? else {
                continue;
            };
            if entries.last().is_none_or(|entry| entry.id != id) {
                entries.push(UserLedgerActivity {
                    id,
                    command_kind: row.try_get("command_kind")?,
                    recorded_at: row.try_get("recorded_at")?,
                    effective_at: row.try_get("effective_at")?,
                    reverses_transaction_id: row.try_get("reverses_transaction_id")?,
                    postings: Vec::new(),
                });
            }
            entries
                .last_mut()
                .expect("entry inserted")
                .postings
                .push(UserLedgerPosting {
                    asset: LedgerAsset::from_code(row.try_get("asset")?)?,
                    bucket: row.try_get::<String, _>("bucket")?.parse()?,
                    side: row.try_get::<String, _>("side")?.parse()?,
                    quantity: row.try_get::<String, _>("quantity")?.parse()?,
                });
        }
        Ok(UserLedgerActivityRead::Available(entries))
    }
}

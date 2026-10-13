//! Read-only account activity, grouped by committed Ledger transaction.

use std::collections::BTreeMap;

use axum::{
    Json, Router,
    extract::{Query, State},
};
use chrono::DateTime;
use ethnum::I256;
use serde::{Deserialize, Serialize};
use tackt_postgres::{
    LedgerActivityCursor, MAX_ACTIVITY_LIMIT, UserLedgerActivity, UserLedgerActivityRead,
};
use tackt_primitives::{
    assets,
    ledger::{BalanceBucket, PostingSide},
};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use crate::{
    account_summary::decimal,
    api::ApiState,
    auth::{AuthError, AuthenticatedUser},
    openapi::routes,
};

pub(crate) fn router() -> (Router<ApiState>, utoipa::openapi::OpenApi) {
    routes!(activity => __path_activity)
}

#[derive(Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
struct ActivityQuery {
    /// Transactions per page, 1–100; defaults to 20.
    limit: Option<u32>,
    /// Opaque nextCursor from the preceding response.
    cursor: Option<String>,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct ActivityPage {
    items: Vec<ActivityItem>,
    #[schema(required = true)]
    next_cursor: Option<String>,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct ActivityItem {
    id: Uuid,
    /// Display category; commandKind retains the exact Ledger event.
    #[serde(rename = "type")]
    kind: &'static str,
    command_kind: String,
    /// Deposit phase at this event, otherwise "recorded". Not a current settlement status.
    status: &'static str,
    /// Journal recording time in Unix milliseconds; determines page order.
    timestamp: i64,
    effective_at: i64,
    #[schema(required = true)]
    reverses_transaction_id: Option<Uuid>,
    /// All user-owned bucket changes. System and other users' postings are omitted.
    changes: Vec<ActivityChange>,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct ActivityChange {
    asset_id: &'static str,
    symbol: String,
    decimals: u8,
    bucket: String,
    /// Accounting posting totals in atomic units, not cash-flow labels.
    debit_quantity: String,
    credit_quantity: String,
    /// Signed change in token units: debits minus credits, except liabilities use credits minus debits.
    quantity_change: String,
}

#[utoipa::path(get, path = "/api/v1/users/me/activity", params(ActivityQuery),
    responses((status = 200, body = ActivityPage)),
    security(("AccessCookie" = []), ("UserBearer" = [], "UserId" = [])), tag = "Account")]
async fn activity(
    user: AuthenticatedUser,
    State(state): State<ApiState>,
    Query(query): Query<ActivityQuery>,
) -> Result<Json<ActivityPage>, AuthError> {
    let limit = query.limit.unwrap_or(20);
    if !(1..=MAX_ACTIVITY_LIMIT).contains(&limit) {
        return Err(AuthError::InvalidField("limit"));
    }
    let cursor = query.cursor.as_deref().map(parse_cursor).transpose()?;
    let read = state
        .repo
        .user_ledger_activity(user.user_id, cursor, limit + 1)
        .await
        .map_err(|error| {
            tracing::error!(%error, user_id = %user.user_id, "account activity read failed");
            AuthError::Internal
        })?;
    let mut entries = match read {
        UserLedgerActivityRead::UserNotFound => return Err(AuthError::NotRegistered),
        UserLedgerActivityRead::UserClosed => return Err(AuthError::Closed),
        UserLedgerActivityRead::Available(entries) => entries,
    };
    let has_more = entries.len() > limit as usize;
    entries.truncate(limit as usize);
    let next_cursor = entries
        .last()
        .filter(|_| has_more)
        .map(|entry| format!("{}.{}", entry.recorded_at.timestamp_micros(), entry.id));
    let items = entries
        .into_iter()
        .map(ActivityItem::new)
        .collect::<anyhow::Result<_>>()
        .map_err(|error| {
            tracing::error!(%error, "account activity conversion failed");
            AuthError::Internal
        })?;
    Ok(Json(ActivityPage { items, next_cursor }))
}

fn parse_cursor(value: &str) -> Result<LedgerActivityCursor, AuthError> {
    let invalid = || AuthError::InvalidField("cursor");
    if value.len() > 64 {
        return Err(invalid());
    }
    let (micros, id) = value.split_once('.').ok_or_else(invalid)?;
    let recorded_at = micros
        .parse()
        .ok()
        .and_then(DateTime::from_timestamp_micros)
        .ok_or_else(invalid)?;
    Ok(LedgerActivityCursor {
        recorded_at,
        transaction_id: id.parse().map_err(|_| invalid())?,
    })
}

impl ActivityItem {
    fn new(entry: UserLedgerActivity) -> anyhow::Result<Self> {
        let mut groups = BTreeMap::new();
        for posting in entry.postings {
            let totals = groups
                .entry((posting.asset, posting.bucket))
                .or_insert((I256::ZERO, I256::ZERO));
            let side = match posting.side {
                PostingSide::Debit => &mut totals.0,
                PostingSide::Credit => &mut totals.1,
            };
            *side = side
                .checked_add(I256::from(posting.quantity))
                .ok_or_else(|| anyhow::anyhow!("activity quantity overflow"))?;
        }
        let changes = groups
            .into_iter()
            .map(|((asset, bucket), (debit, credit))| {
                let info = assets::info(asset);
                let net = if bucket == BalanceBucket::PrincipalLiability {
                    credit.checked_sub(debit)
                } else {
                    debit.checked_sub(credit)
                }
                .ok_or_else(|| anyhow::anyhow!("activity change overflow"))?;
                Ok(ActivityChange {
                    asset_id: asset.code(),
                    symbol: info.symbol.clone(),
                    decimals: info.decimals,
                    bucket: bucket.as_ref().to_owned(),
                    debit_quantity: debit.to_string(),
                    credit_quantity: credit.to_string(),
                    quantity_change: decimal(net, info.decimals),
                })
            })
            .collect::<anyhow::Result<_>>()?;
        let (kind, status) = match entry.command_kind.as_str() {
            "pending" => ("deposit", "pending"),
            "finalized" => ("deposit", "finalized"),
            "rejected" => ("deposit", "rejected"),
            "draw_credit" | "draw_credit_to_custody_exit" | "reserve_credit_for_order" => {
                ("credit", "recorded")
            }
            "record_principal_repayment" => ("repayment", "recorded"),
            "settle_withdrawal" => ("withdrawal", "recorded"),
            "record_venue_fee" => ("fee", "recorded"),
            "reverse_transaction" => ("reversal", "recorded"),
            _ => ("ledger_transaction", "recorded"),
        };
        Ok(Self {
            id: entry.id,
            kind,
            command_kind: entry.command_kind,
            status,
            timestamp: entry.recorded_at.timestamp_millis(),
            effective_at: entry.effective_at.timestamp_millis(),
            reverses_transaction_id: entry.reverses_transaction_id,
            changes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use tackt_postgres::UserLedgerPosting;
    use tackt_primitives::ledger::LedgerAsset;

    #[test]
    fn cursor_preserves_microsecond_order_and_rejects_malformed_input() {
        let id = Uuid::new_v4();
        let micros = 1_800_000_000_123_456;
        let cursor = parse_cursor(&format!("{micros}.{id}")).unwrap();
        assert_eq!(cursor.recorded_at.timestamp_micros(), micros);
        assert_eq!(cursor.transaction_id, id);
        for invalid in [
            "",
            "123",
            "abc.def",
            "123.invalid",
            "99999999999999999999999999999999.00000000-0000-0000-0000-000000000000",
        ] {
            assert!(parse_cursor(invalid).is_err());
        }
    }

    #[test]
    fn groups_postings_by_bucket_and_shows_debt_increase_with_credit_draw() {
        let asset = LedgerAsset::Usdc;
        let unit = 10_u128.pow(u32::from(assets::info(asset).decimals));
        let posting = |bucket, side, quantity| UserLedgerPosting {
            asset,
            bucket,
            side,
            quantity,
        };
        let item = ActivityItem::new(UserLedgerActivity {
            id: Uuid::new_v4(),
            command_kind: "draw_credit".into(),
            recorded_at: Utc::now(),
            effective_at: Utc::now(),
            reverses_transaction_id: None,
            postings: vec![
                posting(BalanceBucket::Available, PostingSide::Debit, unit),
                posting(BalanceBucket::Available, PostingSide::Debit, unit),
                posting(
                    BalanceBucket::PrincipalLiability,
                    PostingSide::Credit,
                    unit * 2,
                ),
            ],
        })
        .unwrap();
        assert_eq!(item.kind, "credit");
        assert_eq!(item.changes.len(), 2);
        for change in &item.changes {
            assert_eq!(
                change.quantity_change,
                decimal(I256::from(unit * 2), assets::info(asset).decimals)
            );
        }
        assert_eq!(item.changes[0].debit_quantity, (unit * 2).to_string());
        assert_eq!(item.changes[1].credit_quantity, (unit * 2).to_string());
    }
}

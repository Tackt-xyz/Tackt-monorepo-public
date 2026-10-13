//! Authenticated display reads; these samples never enter financial orchestration.

use axum::{
    Json, Router,
    extract::{Query, State},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tackt_postgres::{MAX_HISTORY_LIMIT, RiskHealthSample, UserRiskHealthRead};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use crate::{
    api::ApiState,
    auth::{AuthError, AuthenticatedUser},
    catalog::fixed_decimal,
    openapi::routes,
};

/// Every response describes the streamed pooled-margin model, never Tackt credit health.
const METRIC: &str = "pooled_margin_health";
/// The current stream carries sequences, not source observation times.
const INPUT_FRESHNESS: &str = "unknown";

pub(crate) fn router() -> (Router<ApiState>, utoipa::openapi::OpenApi) {
    routes!(latest => __path_latest, history => __path_history)
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct LatestHealth {
    /// Always pooled_margin_health: (pooled collateral - liabilities) / maintenance.
    metric: &'static str,
    /// Always unknown: the current stream carries sequences, not source observation times.
    input_freshness: &'static str,
    /// Missing when this user has no captured history. No fabricated healthy default.
    latest: Option<HealthSample>,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct HealthHistory {
    metric: &'static str,
    input_freshness: &'static str,
    /// Newest first, with sample ID breaking capture-time ties.
    samples: Vec<HealthSample>,
    /// Narrow the requested range or increase limit when true. No silent truncation.
    truncated: bool,
}

#[derive(Deserialize, IntoParams)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct HistoryQuery {
    /// Inclusive Unix milliseconds. Defaults to 24 hours before toMs.
    from_ms: Option<i64>,
    /// Exclusive Unix milliseconds. Defaults to now; range must not exceed 31 days.
    to_ms: Option<i64>,
    /// Maximum samples, 1–2000; defaults to 1440.
    limit: Option<u32>,
}

impl HistoryQuery {
    fn bounds(self, now: DateTime<Utc>) -> Result<(DateTime<Utc>, DateTime<Utc>, u32), AuthError> {
        let to = self
            .to_ms
            .map_or(Some(now), DateTime::from_timestamp_millis)
            .ok_or(AuthError::InvalidField("toMs"))?;
        let from = self
            .from_ms
            .map_or_else(
                || to.checked_sub_signed(chrono::Duration::days(1)),
                DateTime::from_timestamp_millis,
            )
            .ok_or(AuthError::InvalidField("fromMs"))?;
        let limit = self.limit.unwrap_or(1440);
        if from >= to
            || to - from > chrono::Duration::days(31)
            || !(1..=MAX_HISTORY_LIMIT).contains(&limit)
        {
            return Err(AuthError::InvalidField("range"));
        }
        Ok((from, to, limit))
    }
}

#[utoipa::path(get, path = "/api/v1/users/me/risk-health",
    responses((status = 200, body = LatestHealth)),
    security(("AccessCookie" = []), ("UserBearer" = [], "UserId" = [])), tag = "Risk history")]
async fn latest(
    user: AuthenticatedUser,
    State(state): State<ApiState>,
) -> Result<Json<LatestHealth>, AuthError> {
    let samples = checked_read(
        user.user_id,
        state.repo.latest_user_risk_health(user.user_id).await,
    )?;
    Ok(Json(LatestHealth {
        metric: METRIC,
        input_freshness: INPUT_FRESHNESS,
        latest: samples.into_iter().next().map(HealthSample::from),
    }))
}

#[utoipa::path(get, path = "/api/v1/users/me/risk-health/history", params(HistoryQuery),
    responses((status = 200, body = HealthHistory)),
    security(("AccessCookie" = []), ("UserBearer" = [], "UserId" = [])), tag = "Risk history")]
async fn history(
    user: AuthenticatedUser,
    State(state): State<ApiState>,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<HealthHistory>, AuthError> {
    let (from, to, limit) = query.bounds(Utc::now())?;
    let mut samples = checked_read(
        user.user_id,
        state
            .repo
            .user_risk_health_history(user.user_id, from, to, limit + 1)
            .await,
    )?;
    let truncated = samples.len() > limit as usize;
    samples.truncate(limit as usize);
    Ok(Json(HealthHistory {
        metric: METRIC,
        input_freshness: INPUT_FRESHNESS,
        samples: samples.into_iter().map(HealthSample::from).collect(),
        truncated,
    }))
}

fn checked_read(
    user_id: Uuid,
    read: anyhow::Result<UserRiskHealthRead>,
) -> Result<Vec<RiskHealthSample>, AuthError> {
    match read.map_err(|error| {
        tracing::error!(%error, %user_id, "risk history read failed");
        AuthError::Internal
    })? {
        UserRiskHealthRead::UserNotFound => Err(AuthError::NotRegistered),
        UserRiskHealthRead::UserClosed => Err(AuthError::Closed),
        UserRiskHealthRead::Available(samples) => Ok(samples),
    }
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct HealthSample {
    sampled_at: DateTime<Utc>,
    #[schema(value_type = String, examples("healthy", "watch", "liquidatable", "data_incomplete"))]
    classification: tackt_primitives::risk::RiskClassification,
    /// Exact finite ratio as a decimal string; null for incomplete or unbounded health.
    #[schema(examples("1.200000"))]
    health: Option<String>,
}

impl From<RiskHealthSample> for HealthSample {
    fn from(sample: RiskHealthSample) -> Self {
        Self {
            sampled_at: sample.sampled_at,
            classification: sample.classification,
            health: sample.health.map(|raw| fixed_decimal(raw, 6)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn samples_expose_only_display_fields_with_exact_precision_and_null() {
        let mut sample =
            tackt_postgres::test_support::risk_health_sample(uuid::Uuid::new_v4(), Utc::now());
        for (raw, expected) in [
            (Some(1_200_000), serde_json::json!("1.200000")),
            (Some(-1), serde_json::json!("-0.000001")),
            (
                Some(i128::MAX - 1),
                serde_json::json!("170141183460469231731687303715884.105726"),
            ),
            (None, serde_json::Value::Null),
        ] {
            sample.health = raw;
            let json = serde_json::to_value(HealthSample::from(sample.clone())).unwrap();
            assert_eq!(
                json,
                serde_json::json!({
                    "sampledAt": sample.sampled_at,
                    "classification": sample.classification,
                    "health": expected,
                })
            );
        }
    }

    #[test]
    fn range_is_bounded_and_invalid_timestamps_are_rejected() {
        let now = DateTime::from_timestamp_millis(1_800_000_000_000).unwrap();
        let (from, to, limit) = HistoryQuery {
            from_ms: None,
            to_ms: None,
            limit: None,
        }
        .bounds(now)
        .unwrap();
        assert_eq!(to - from, chrono::Duration::days(1));
        assert_eq!(limit, 1440);
        for query in [
            HistoryQuery {
                from_ms: None,
                to_ms: Some(i64::MAX),
                limit: None,
            },
            HistoryQuery {
                from_ms: Some(0),
                to_ms: None,
                limit: None,
            },
            HistoryQuery {
                from_ms: None,
                to_ms: None,
                limit: Some(2001),
            },
            HistoryQuery {
                from_ms: Some(now.timestamp_millis()),
                to_ms: None,
                limit: None,
            },
        ] {
            assert!(query.bounds(now).is_err());
        }
    }
}

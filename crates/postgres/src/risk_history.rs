//! Typed persistence for best-effort, non-authoritative Risk display samples.

use anyhow::{Result, ensure};
use chrono::{DateTime, Utc};
use sqlx::Row;
use tackt_primitives::{UserStatus, accounts::AccountSubject, risk::RiskClassification};
use uuid::Uuid;

use crate::PgRepository;

/// Largest history page a caller may request; reads accept one extra row for truncation detection.
pub const MAX_HISTORY_LIMIT: u32 = 2000;

/// A sample of the streamed pooled-margin model, not Tackt credit health.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RiskHealthSample {
    pub sample_id: Uuid,
    pub account: AccountSubject,
    /// Host capture time, not source observation time.
    pub sampled_at: DateTime<Utc>,
    pub classification: RiskClassification,
    /// Finite ratio in 1e6 fixed point; stored as NUMERIC(39,6).
    /// None means incomplete inputs or an unbounded ratio; classification distinguishes them.
    pub health: Option<i128>,
}

/// Lifecycle check and samples come from the same database statement snapshot.
#[derive(Debug)]
pub enum UserRiskHealthRead {
    UserNotFound,
    UserClosed,
    Available(Vec<RiskHealthSample>),
}

impl PgRepository {
    /// Insert one sample. Duplicate sample IDs fail; the display writer does not retry.
    pub async fn record_risk_health(&self, sample: &RiskHealthSample) -> Result<()> {
        ensure!(!sample.sample_id.is_nil(), "nil risk sample identity");
        ensure!(
            sample.classification != RiskClassification::DataIncomplete || sample.health.is_none(),
            "incomplete risk sample must omit health"
        );
        ensure!(
            !matches!(sample.health, Some(i128::MIN | i128::MAX)),
            "unbounded health must be null"
        );
        let health = sample.health.map(|raw| raw.to_string());
        sqlx::query(
            "INSERT INTO risk_health_samples(sample_id,user_id,sampled_at,health,classification) \
             VALUES($1,$2,$3,$4::text::numeric * 0.000001,$5)",
        )
        .bind(sample.sample_id)
        .bind(sample.account.user_id())
        .bind(sample.sampled_at)
        .bind(&health)
        .bind(sample.classification.to_string())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Latest captured sample. Its capture time does not establish source freshness.
    pub async fn latest_user_risk_health(&self, user: Uuid) -> Result<UserRiskHealthRead> {
        self.read_user_risk_health(user, None, None, 1).await
    }

    /// Newest-first bounded history in [from, to), at most `MAX_HISTORY_LIMIT + 1` rows.
    pub async fn user_risk_health_history(
        &self,
        user: Uuid,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        limit: u32,
    ) -> Result<UserRiskHealthRead> {
        ensure!(from < to, "invalid risk history range");
        ensure!(
            to - from <= chrono::Duration::days(31),
            "risk history range exceeds 31 days"
        );
        ensure!(
            (1..=MAX_HISTORY_LIMIT + 1).contains(&limit),
            "invalid risk history limit"
        );
        self.read_user_risk_health(user, Some(from), Some(to), limit)
            .await
    }

    async fn read_user_risk_health(
        &self,
        user: Uuid,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
        limit: u32,
    ) -> Result<UserRiskHealthRead> {
        // Absent bounds collapse to infinities so the range stays an index condition
        // under a generic prepared plan.
        let rows = sqlx::query(
            "SELECT u.status::text AS status, s.sample_id, s.sampled_at, s.classification, \
             (s.health * 1000000)::numeric(39,0)::text AS health_raw \
             FROM users u LEFT JOIN LATERAL \
               (SELECT sample_id,sampled_at,health,classification FROM risk_health_samples \
                WHERE user_id=u.id AND u.status <> 'closed' \
                  AND sampled_at >= COALESCE($2::timestamptz, '-infinity'::timestamptz) \
                  AND sampled_at < COALESCE($3::timestamptz, 'infinity'::timestamptz) \
                ORDER BY sampled_at DESC,sample_id DESC LIMIT $4) s ON true \
             WHERE u.id=$1 ORDER BY s.sampled_at DESC,s.sample_id DESC",
        )
        .bind(user)
        .bind(from)
        .bind(to)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        let Some(first) = rows.first() else {
            return Ok(UserRiskHealthRead::UserNotFound);
        };
        if crate::users::parse_status(first.try_get("status")?)? == UserStatus::Closed {
            return Ok(UserRiskHealthRead::UserClosed);
        }
        let account = AccountSubject::new(user)?;
        let mut samples = Vec::with_capacity(rows.len());
        for row in rows {
            let Some(sample_id) = row.try_get::<Option<Uuid>, _>("sample_id")? else {
                continue;
            };
            let classification: String = row.try_get("classification")?;
            let health: Option<String> = row.try_get("health_raw")?;
            samples.push(RiskHealthSample {
                sample_id,
                account,
                sampled_at: row.try_get("sampled_at")?,
                classification: classification.parse()?,
                health: health.map(|raw| raw.parse()).transpose()?,
            });
        }
        Ok(UserRiskHealthRead::Available(samples))
    }
}

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sqlx::{PgExecutor, Postgres, Row, Transaction};
use tackt_primitives::{
    CoreActionKind, CoreActionState,
    assets::AssetReference,
    ledger::{CustodyExitDestination, LedgerAsset, valid_display_text, valid_idempotency_key},
    risk::{RiskEvaluation, RiskRecommendation},
};
use thiserror::Error;
use uuid::Uuid;

use crate::PgRepository;

const MAX_REFERENCE_KEY_BYTES: usize = 64;
const MAX_REFERENCE_VALUE_BYTES: usize = 1024;
const MAX_REASON_BYTES: usize = 1024;
const MAX_ACTIVE_SCAN_PAGE: u32 = 200;
const ACTION_COLUMNS: &str = "id,user_id,kind::text,asset,quantity::text,\
    state::text,risk_recommendation_id,portfolio_version::text,\
    risk_processing_epoch::text,risk_evaluation_id,risk_input_digest,\
    risk_account_version::text,risk_input_version::text,\
    risk_evaluation::text AS risk_evaluation,external_references,created_at,updated_at";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActionRiskEvidence {
    pub recommendation_id: String,
    pub portfolio_version: u64,
    pub processing_epoch: u64,
}

impl From<&RiskRecommendation> for ActionRiskEvidence {
    fn from(value: &RiskRecommendation) -> Self {
        Self {
            recommendation_id: value.recommendation_id().to_owned(),
            portfolio_version: value.portfolio_version(),
            processing_epoch: value.processing_epoch(),
        }
    }
}

/// Reproducible evidence returned by Risk's synchronous financial-evaluation endpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActionEvaluationEvidence {
    pub evaluation: RiskEvaluation,
    pub evaluation_id: String,
    pub input_digest: String,
    pub account_version: u64,
    pub input_version: u64,
    pub processing_epoch: u64,
}

impl From<&RiskEvaluation> for ActionEvaluationEvidence {
    fn from(value: &RiskEvaluation) -> Self {
        let versions = value.versions();
        Self {
            evaluation: value.clone(),
            evaluation_id: value.evaluation_id().to_owned(),
            input_digest: value.input_digest().to_owned(),
            account_version: versions.account_version,
            input_version: versions.input_version,
            processing_epoch: versions.processing_epoch,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewCoreAction {
    pub id: Uuid,
    pub user_id: Uuid,
    pub kind: CoreActionKind,
    pub asset: AssetReference,
    pub quantity: u128,
    pub external_references: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoreAction {
    pub id: Uuid,
    pub user_id: Uuid,
    pub kind: CoreActionKind,
    pub asset: AssetReference,
    pub quantity: u128,
    pub state: CoreActionState,
    pub risk_evidence: Option<ActionRiskEvidence>,
    pub evaluation_evidence: Option<ActionEvaluationEvidence>,
    pub external_references: BTreeMap<String, String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl CoreAction {
    /// Ledger binds every custody-exit reservation to the stable Core action UUID.
    #[must_use]
    pub fn custody_exit_reservation_id(&self) -> Option<Uuid> {
        self.kind.is_custody_exit().then_some(self.id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActionTransitionRequest {
    pub transition_id: Uuid,
    pub action_id: Uuid,
    pub expected_state: CoreActionState,
    pub next_state: CoreActionState,
    pub reason: String,
    pub risk_evidence: Option<RiskRecommendation>,
    pub external_references: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoreActionTransition {
    pub transition_id: Uuid,
    pub action_id: Uuid,
    pub from_state: Option<CoreActionState>,
    pub to_state: CoreActionState,
    pub reason: String,
    pub evidence: Value,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransitionResult {
    pub action: CoreAction,
    pub transition: CoreActionTransition,
    pub duplicate: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActionLedgerStep {
    ReserveCustodyExit,
    DrawCreditToCustodyExit,
    ConsumeCustodyExit,
    ReleaseCustodyExit,
    ReserveVenueOrder,
}

impl ActionLedgerStep {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReserveCustodyExit => "reserve_custody_exit",
            Self::DrawCreditToCustodyExit => "draw_credit_to_custody_exit",
            Self::ConsumeCustodyExit => "consume_custody_exit",
            Self::ReleaseCustodyExit => "release_custody_exit",
            Self::ReserveVenueOrder => "reserve_venue_order",
        }
    }

    fn from_str(value: &str) -> Result<Self, ActionRepositoryError> {
        match value {
            "reserve_custody_exit" => Ok(Self::ReserveCustodyExit),
            "draw_credit_to_custody_exit" => Ok(Self::DrawCreditToCustodyExit),
            "consume_custody_exit" => Ok(Self::ConsumeCustodyExit),
            "release_custody_exit" => Ok(Self::ReleaseCustodyExit),
            "reserve_venue_order" => Ok(Self::ReserveVenueOrder),
            _ => Err(ActionRepositoryError::InvalidPersistedState(format!(
                "unknown Ledger step {value}"
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActionLedgerRequest {
    pub request_id: Uuid,
    pub action_id: Uuid,
    pub step: ActionLedgerStep,
    pub idempotency_key: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum ActionRepositoryError {
    #[error("action not found")]
    NotFound,
    #[error("an active custody exit already exists for this user")]
    ActiveCustodyExit,
    #[error("action identity was reused with different contents")]
    ActionIdempotencyConflict,
    #[error("transition identity was reused with different contents")]
    TransitionIdempotencyConflict,
    #[error("Ledger request identity or idempotency key was reused")]
    LedgerRequestIdempotencyConflict,
    #[error("cannot transition action from {} to {}", .0.as_str(), .1.as_str())]
    InvalidTransition(CoreActionState, CoreActionState),
    #[error("stale action state: expected {}, found {}", .expected.as_str(), .actual.as_str())]
    StaleTransition {
        expected: CoreActionState,
        actual: CoreActionState,
    },
    #[error("risk evidence conflicts with evidence already stored for this action")]
    RiskEvidenceConflict,
    #[error("risk evaluation conflicts with evidence already stored for this action")]
    EvaluationEvidenceConflict,
    #[error("risk evaluation does not identify this action and user")]
    EvaluationIdentityMismatch,
    #[error("an external reference conflicts with evidence already stored for this action")]
    ExternalReferenceConflict,
    #[error("Ledger hold steps apply only to custody-exit actions")]
    NotCustodyExit,
    #[error("invalid action input: {0}")]
    InvalidInput(String),
    #[error("invalid persisted action state: {0}")]
    InvalidPersistedState(String),
    #[error("database error")]
    Database(#[from] sqlx::Error),
}

impl PgRepository {
    pub async fn create_core_action(
        &self,
        input: NewCoreAction,
    ) -> Result<(CoreAction, bool), ActionRepositoryError> {
        validate_new_action(&input)?;
        let references = references_json(&input.external_references);
        let mut transaction = self.pool.begin().await?;
        let insert_sql = format!(
            "INSERT INTO core_actions(id,user_id,kind,asset,quantity,external_references) \
             VALUES($1,$2,$3::core_action_kind,$4,$5::numeric,$6) ON CONFLICT(id) DO NOTHING \
             RETURNING {ACTION_COLUMNS}"
        );
        let inserted = sqlx::query(&insert_sql)
            .bind(input.id)
            .bind(input.user_id)
            .bind(input.kind.as_str())
            .bind(input.asset.asset().code())
            .bind(input.quantity.to_string())
            .bind(&references)
            .fetch_optional(&mut *transaction)
            .await;
        let inserted = match inserted {
            Ok(value) => value,
            Err(error) if is_unique_violation(&error) && input.kind.is_custody_exit() => {
                return Err(ActionRepositoryError::ActiveCustodyExit);
            }
            Err(error) => return Err(error.into()),
        };

        let Some(row) = inserted else {
            let existing = load_action(&mut *transaction, input.id).await?;
            transaction.commit().await?;
            if action_matches_input(&existing, &input) {
                return Ok((existing, false));
            }
            return Err(ActionRepositoryError::ActionIdempotencyConflict);
        };
        let action = action_from_row(row)?;
        sqlx::query(
            "INSERT INTO core_action_transitions(transition_id,action_id,from_state,to_state,reason) \
             VALUES($1,$1,NULL,'pending','action_created')",
        )
        .bind(input.id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok((action, true))
    }

    pub async fn core_action(&self, id: Uuid) -> Result<CoreAction, ActionRepositoryError> {
        load_action(&self.pool, id).await
    }

    /// Records the exact synchronous Risk evaluation before any Ledger side effect.
    /// Replays of identical evidence are harmless; conflicting evidence fails closed.
    pub async fn record_action_evaluation(
        &self,
        action_id: Uuid,
        evaluation: &RiskEvaluation,
    ) -> Result<(CoreAction, bool), ActionRepositoryError> {
        if evaluation.action_id() != action_id {
            return Err(ActionRepositoryError::EvaluationIdentityMismatch);
        }
        let evidence = ActionEvaluationEvidence::from(evaluation);
        let update_sql = format!(
            "UPDATE core_actions SET risk_evaluation_id=$2,risk_input_digest=$3, \
             risk_account_version=$4::numeric,risk_input_version=$5::numeric, \
             risk_processing_epoch=$6::numeric,risk_evaluation=$7::jsonb,updated_at=clock_timestamp() \
             WHERE id=$1 AND user_id=$8 AND risk_recommendation_id IS NULL \
             AND risk_evaluation_id IS NULL RETURNING {ACTION_COLUMNS}"
        );
        let row = sqlx::query(&update_sql)
            .bind(action_id)
            .bind(&evidence.evaluation_id)
            .bind(&evidence.input_digest)
            .bind(evidence.account_version.to_string())
            .bind(evidence.input_version.to_string())
            .bind(evidence.processing_epoch.to_string())
            .bind(serde_json::to_string(evaluation).map_err(|error| {
                ActionRepositoryError::InvalidInput(format!("invalid risk evaluation: {error}"))
            })?)
            .bind(evaluation.account().user_id())
            .fetch_optional(&self.pool)
            .await?;
        if let Some(row) = row {
            return Ok((action_from_row(row)?, true));
        }
        let current = self.core_action(action_id).await?;
        if current.user_id != evaluation.account().user_id() {
            return Err(ActionRepositoryError::EvaluationIdentityMismatch);
        }
        if current.evaluation_evidence.as_ref() == Some(&evidence) {
            return Ok((current, false));
        }
        Err(ActionRepositoryError::EvaluationEvidenceConflict)
    }

    /// Lists restartable work in stable `(updated_at, id)` order. Terminal actions are audit
    /// records, not work, and are deliberately excluded from this recovery scan.
    pub async fn active_core_actions(
        &self,
        limit: u32,
        after: Option<(DateTime<Utc>, Uuid)>,
    ) -> Result<Vec<CoreAction>, ActionRepositoryError> {
        let (after_updated_at, after_id) = after.unzip();
        let select_sql = format!(
            "SELECT {ACTION_COLUMNS} FROM core_actions WHERE core_action_state_is_active(state) \
             AND (updated_at,id) > (COALESCE($2,'-infinity'::timestamptz), \
                                    COALESCE($3,'00000000-0000-0000-0000-000000000000'::uuid)) \
             ORDER BY updated_at,id LIMIT $1"
        );
        let rows = sqlx::query(&select_sql)
            .bind(i64::from(limit.min(MAX_ACTIVE_SCAN_PAGE)))
            .bind(after_updated_at)
            .bind(after_id)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(action_from_row).collect()
    }

    pub async fn transition_core_action(
        &self,
        request: ActionTransitionRequest,
    ) -> Result<TransitionResult, ActionRepositoryError> {
        validate_transition(&request)?;
        let evidence = transition_evidence(&request);
        let references = references_json(&request.external_references);
        let mut transaction = self.pool.begin().await?;

        if let Some(existing) = load_transition(&mut transaction, request.transition_id).await? {
            return replay_transition(transaction, &request, evidence, existing).await;
        }

        let risk = request.risk_evidence.as_ref().map(ActionRiskEvidence::from);
        let update_sql = format!(
            "UPDATE core_actions SET state=$3::core_action_state, \
             risk_recommendation_id=COALESCE(risk_recommendation_id,$4), \
             portfolio_version=COALESCE(portfolio_version,$5::numeric), \
             risk_processing_epoch=COALESCE(risk_processing_epoch,$6::numeric), \
             external_references=external_references||$7,updated_at=clock_timestamp() \
             WHERE id=$1 AND state=$2::core_action_state \
             AND ($4::text IS NULL OR risk_recommendation_id IS NULL OR \
                  (risk_recommendation_id=$4 AND portfolio_version=$5::numeric \
                   AND risk_processing_epoch=$6::numeric)) \
             AND NOT EXISTS (SELECT 1 FROM jsonb_each_text($7) incoming \
                 WHERE external_references ? incoming.key \
                   AND external_references->>incoming.key<>incoming.value) \
             RETURNING {ACTION_COLUMNS}"
        );
        let updated = sqlx::query(&update_sql)
            .bind(request.action_id)
            .bind(request.expected_state.as_str())
            .bind(request.next_state.as_str())
            .bind(risk.as_ref().map(|value| value.recommendation_id.as_str()))
            .bind(
                risk.as_ref()
                    .map(|value| value.portfolio_version.to_string()),
            )
            .bind(
                risk.as_ref()
                    .map(|value| value.processing_epoch.to_string()),
            )
            .bind(&references)
            .fetch_optional(&mut *transaction)
            .await?;

        let Some(row) = updated else {
            if let Some(existing) = load_transition(&mut transaction, request.transition_id).await?
            {
                return replay_transition(transaction, &request, evidence, existing).await;
            }
            let current = load_action(&mut *transaction, request.action_id).await?;
            transaction.rollback().await?;
            if current.state != request.expected_state {
                return Err(ActionRepositoryError::StaleTransition {
                    expected: request.expected_state,
                    actual: current.state,
                });
            }
            if risk.is_some() && current.risk_evidence.as_ref() != risk.as_ref() {
                return Err(ActionRepositoryError::RiskEvidenceConflict);
            }
            return Err(ActionRepositoryError::ExternalReferenceConflict);
        };
        let action = action_from_row(row)?;
        let row = sqlx::query(
            "INSERT INTO core_action_transitions(transition_id,action_id,from_state,to_state,reason,evidence) \
             VALUES($1,$2,$3::core_action_state,$4::core_action_state,$5,$6) \
             RETURNING transition_id,action_id,from_state::text,to_state::text,reason,evidence,occurred_at",
        )
        .bind(request.transition_id)
        .bind(request.action_id)
        .bind(request.expected_state.as_str())
        .bind(request.next_state.as_str())
        .bind(&request.reason)
        .bind(&evidence)
        .fetch_one(&mut *transaction)
        .await?;
        let transition = transition_from_row(row)?;
        transaction.commit().await?;
        Ok(TransitionResult {
            action,
            transition,
            duplicate: false,
        })
    }

    pub async fn core_action_transitions(
        &self,
        action_id: Uuid,
    ) -> Result<Vec<CoreActionTransition>, ActionRepositoryError> {
        let rows = sqlx::query(
            "SELECT transition_id,action_id,from_state::text,to_state::text,reason,evidence,occurred_at \
             FROM core_action_transitions WHERE action_id=$1 ORDER BY occurred_at,transition_id",
        )
        .bind(action_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(transition_from_row).collect()
    }

    pub async fn record_action_ledger_request(
        &self,
        request_id: Uuid,
        action_id: Uuid,
        step: ActionLedgerStep,
        idempotency_key: String,
    ) -> Result<(ActionLedgerRequest, bool), ActionRepositoryError> {
        if request_id.is_nil() || !valid_idempotency_key(&idempotency_key) {
            return Err(ActionRepositoryError::InvalidInput(
                "a non-nil request ID and valid idempotency key are required".into(),
            ));
        }
        let row = sqlx::query(
            "INSERT INTO core_action_ledger_requests(request_id,action_id,step,idempotency_key) \
             SELECT $1,$2,$3::core_action_ledger_step,$4 FROM core_actions \
             WHERE id=$2 AND ((kind IN ('custody_outflow','hyperliquid_margin') \
                 AND $3<>'reserve_venue_order') OR (kind='venue_order' \
                 AND $3='reserve_venue_order')) \
             ON CONFLICT DO NOTHING \
             RETURNING request_id,action_id,step::text,idempotency_key,created_at",
        )
        .bind(request_id)
        .bind(action_id)
        .bind(step.as_str())
        .bind(&idempotency_key)
        .fetch_optional(&self.pool)
        .await?;
        if let Some(row) = row {
            return Ok((ledger_request_from_row(row)?, true));
        }
        let existing = sqlx::query(
            "SELECT request_id,action_id,step::text,idempotency_key,created_at \
             FROM core_action_ledger_requests \
             WHERE request_id=$1",
        )
        .bind(request_id)
        .fetch_optional(&self.pool)
        .await?;
        if let Some(existing) = existing.map(ledger_request_from_row).transpose()? {
            if existing.request_id == request_id
                && existing.action_id == action_id
                && existing.step == step
                && existing.idempotency_key == idempotency_key
            {
                return Ok((existing, false));
            }
            return Err(ActionRepositoryError::LedgerRequestIdempotencyConflict);
        }
        let kind =
            sqlx::query_scalar::<_, String>("SELECT kind::text FROM core_actions WHERE id=$1")
                .bind(action_id)
                .fetch_optional(&self.pool)
                .await?
                .ok_or(ActionRepositoryError::NotFound)?;
        let kind = decode_action_kind(&kind)?;
        if (kind.is_custody_exit() && step != ActionLedgerStep::ReserveVenueOrder)
            || (kind == CoreActionKind::VenueOrder && step == ActionLedgerStep::ReserveVenueOrder)
        {
            return Err(ActionRepositoryError::LedgerRequestIdempotencyConflict);
        }
        if kind == CoreActionKind::VenueOrder {
            Err(ActionRepositoryError::NotCustodyExit)
        } else {
            Err(ActionRepositoryError::InvalidInput(
                "venue-order Ledger steps apply only to venue-order actions".into(),
            ))
        }
    }

    /// Atomically allocates or reloads the stable Ledger request for an action step.
    pub async fn ensure_action_ledger_request(
        &self,
        action_id: Uuid,
        step: ActionLedgerStep,
    ) -> Result<(ActionLedgerRequest, bool), ActionRepositoryError> {
        let request_id = Uuid::new_v4();
        let key = request_id.to_string();
        match self
            .record_action_ledger_request(request_id, action_id, step, key)
            .await
        {
            Ok(result) => Ok(result),
            Err(ActionRepositoryError::LedgerRequestIdempotencyConflict) => {
                let existing = self.action_ledger_requests(action_id).await?;
                let request = existing
                    .into_iter()
                    .find(|request| request.step == step)
                    .ok_or(ActionRepositoryError::LedgerRequestIdempotencyConflict)?;
                Ok((request, false))
            }
            Err(error) => Err(error),
        }
    }

    pub async fn action_ledger_requests(
        &self,
        action_id: Uuid,
    ) -> Result<Vec<ActionLedgerRequest>, ActionRepositoryError> {
        let rows = sqlx::query(
            "SELECT request_id,action_id,step::text,idempotency_key,created_at \
             FROM core_action_ledger_requests WHERE action_id=$1 ORDER BY created_at,request_id",
        )
        .bind(action_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(ledger_request_from_row).collect()
    }
}

fn validate_new_action(input: &NewCoreAction) -> Result<(), ActionRepositoryError> {
    if input.id.is_nil() || input.user_id.is_nil() || input.quantity == 0 {
        return Err(ActionRepositoryError::InvalidInput(
            "non-nil action/user IDs and positive quantity are required".into(),
        ));
    }
    if input.kind.custody_exit_destination() == Some(CustodyExitDestination::HyperliquidMargin)
        && input.asset.asset() != LedgerAsset::Usdc
    {
        return Err(ActionRepositoryError::InvalidInput(
            "Hyperliquid custody exits require USDC".into(),
        ));
    }
    validate_references(&input.external_references)
}

fn validate_transition(request: &ActionTransitionRequest) -> Result<(), ActionRepositoryError> {
    if request.transition_id.is_nil() || request.action_id.is_nil() {
        return Err(ActionRepositoryError::InvalidInput(
            "non-nil transition and action IDs are required".into(),
        ));
    }
    if !request.expected_state.can_transition_to(request.next_state) {
        return Err(ActionRepositoryError::InvalidTransition(
            request.expected_state,
            request.next_state,
        ));
    }
    validate_text(&request.reason, MAX_REASON_BYTES, "transition reason")?;
    validate_references(&request.external_references)?;
    Ok(())
}

fn validate_references(references: &BTreeMap<String, String>) -> Result<(), ActionRepositoryError> {
    for (key, value) in references {
        validate_text(key, MAX_REFERENCE_KEY_BYTES, "external reference key")?;
        validate_text(value, MAX_REFERENCE_VALUE_BYTES, "external reference value")?;
    }
    Ok(())
}

fn validate_text(value: &str, max_bytes: usize, field: &str) -> Result<(), ActionRepositoryError> {
    if !valid_display_text(value, max_bytes) {
        return Err(ActionRepositoryError::InvalidInput(format!(
            "{field} must be non-blank, bounded, and contain no control characters"
        )));
    }
    Ok(())
}

async fn load_action<'e, E: PgExecutor<'e>>(
    executor: E,
    id: Uuid,
) -> Result<CoreAction, ActionRepositoryError> {
    let select_sql = format!("SELECT {ACTION_COLUMNS} FROM core_actions WHERE id=$1");
    let row = sqlx::query(&select_sql)
        .bind(id)
        .fetch_optional(executor)
        .await?
        .ok_or(ActionRepositoryError::NotFound)?;
    action_from_row(row)
}

fn action_from_row(row: sqlx::postgres::PgRow) -> Result<CoreAction, ActionRepositoryError> {
    let asset_code: String = row.try_get("asset")?;
    let asset = LedgerAsset::from_code(&asset_code)
        .map_err(|error| ActionRepositoryError::InvalidPersistedState(error.to_string()))?;
    let asset = AssetReference::new(asset);
    let risk_evidence = risk_evidence_from_row(&row)?;
    let evaluation_evidence = evaluation_evidence_from_row(&row)?;
    let references: Value = row.try_get("external_references")?;
    Ok(CoreAction {
        id: row.try_get("id")?,
        user_id: row.try_get("user_id")?,
        kind: decode_action_kind(&row.try_get::<String, _>("kind")?)?,
        asset,
        quantity: parse_u128(&row.try_get::<String, _>("quantity")?, "quantity")?,
        state: decode_action_state(&row.try_get::<String, _>("state")?)?,
        risk_evidence,
        evaluation_evidence,
        external_references: references_from_json(references)?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn decode_action_kind(value: &str) -> Result<CoreActionKind, ActionRepositoryError> {
    match value {
        "custody_outflow" => Ok(CoreActionKind::CustodyExit(
            CustodyExitDestination::CustodyOutflow,
        )),
        "hyperliquid_margin" => Ok(CoreActionKind::CustodyExit(
            CustodyExitDestination::HyperliquidMargin,
        )),
        "venue_order" => Ok(CoreActionKind::VenueOrder),
        _ => Err(ActionRepositoryError::InvalidPersistedState(format!(
            "unknown action kind {value}"
        ))),
    }
}

fn decode_action_state(value: &str) -> Result<CoreActionState, ActionRepositoryError> {
    CoreActionState::ALL
        .into_iter()
        .find(|state| state.as_str() == value)
        .ok_or_else(|| {
            ActionRepositoryError::InvalidPersistedState(format!("unknown action state {value}"))
        })
}

fn risk_evidence_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<Option<ActionRiskEvidence>, ActionRepositoryError> {
    let Some(recommendation_id) = row.try_get::<Option<String>, _>("risk_recommendation_id")?
    else {
        return Ok(None);
    };
    Ok(Some(ActionRiskEvidence {
        recommendation_id,
        portfolio_version: parse_u64_option(row, "portfolio_version")?,
        processing_epoch: parse_u64_option(row, "risk_processing_epoch")?,
    }))
}

fn evaluation_evidence_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<Option<ActionEvaluationEvidence>, ActionRepositoryError> {
    let Some(evaluation_id) = row.try_get::<Option<String>, _>("risk_evaluation_id")? else {
        return Ok(None);
    };
    let persisted = ActionEvaluationEvidence {
        evaluation: serde_json::from_str(
            &row.try_get::<Option<String>, _>("risk_evaluation")?
                .ok_or_else(incomplete_risk_evidence)?,
        )
        .map_err(|error| {
            ActionRepositoryError::InvalidPersistedState(format!(
                "invalid risk evaluation: {error}"
            ))
        })?,
        evaluation_id,
        input_digest: required_string(row, "risk_input_digest")?,
        account_version: parse_u64_option(row, "risk_account_version")?,
        input_version: parse_u64_option(row, "risk_input_version")?,
        processing_epoch: parse_u64_option(row, "risk_processing_epoch")?,
    };
    if ActionEvaluationEvidence::from(&persisted.evaluation) != persisted {
        return Err(ActionRepositoryError::InvalidPersistedState(
            "risk evaluation columns disagree with the persisted evidence".into(),
        ));
    }
    Ok(Some(persisted))
}

fn required_string(
    row: &sqlx::postgres::PgRow,
    column: &str,
) -> Result<String, ActionRepositoryError> {
    row.try_get::<Option<String>, _>(column)?
        .ok_or_else(incomplete_risk_evidence)
}

fn incomplete_risk_evidence() -> ActionRepositoryError {
    ActionRepositoryError::InvalidPersistedState("incomplete risk evidence".into())
}

fn parse_u64_option(
    row: &sqlx::postgres::PgRow,
    column: &str,
) -> Result<u64, ActionRepositoryError> {
    let value = row
        .try_get::<Option<String>, _>(column)?
        .ok_or_else(incomplete_risk_evidence)?;
    value
        .parse()
        .map_err(|_| ActionRepositoryError::InvalidPersistedState(format!("invalid {column}")))
}

fn parse_u128(value: &str, field: &str) -> Result<u128, ActionRepositoryError> {
    value
        .parse()
        .map_err(|_| ActionRepositoryError::InvalidPersistedState(format!("invalid {field}")))
}

fn references_json(references: &BTreeMap<String, String>) -> Value {
    Value::Object(
        references
            .iter()
            .map(|(key, value)| (key.clone(), Value::String(value.clone())))
            .collect(),
    )
}

fn references_from_json(value: Value) -> Result<BTreeMap<String, String>, ActionRepositoryError> {
    let Value::Object(values) = value else {
        return Err(ActionRepositoryError::InvalidPersistedState(
            "external references are not an object".into(),
        ));
    };
    values
        .into_iter()
        .map(|(key, value)| match value {
            Value::String(value) => Ok((key, value)),
            _ => Err(ActionRepositoryError::InvalidPersistedState(
                "external reference is not a string".into(),
            )),
        })
        .collect()
}

fn transition_evidence(request: &ActionTransitionRequest) -> Value {
    let risk = request.risk_evidence.as_ref().map(|value| {
        json!({
            "recommendation_id": value.recommendation_id(),
            "portfolio_version": value.portfolio_version(),
            "processing_epoch": value.processing_epoch(),
        })
    });
    json!({
        "risk": risk,
        "external_references": references_json(&request.external_references),
    })
}

async fn load_transition(
    transaction: &mut Transaction<'_, Postgres>,
    transition_id: Uuid,
) -> Result<Option<CoreActionTransition>, ActionRepositoryError> {
    sqlx::query(
        "SELECT transition_id,action_id,from_state::text,to_state::text,reason,evidence,occurred_at \
         FROM core_action_transitions WHERE transition_id=$1",
    )
    .bind(transition_id)
    .fetch_optional(&mut **transaction)
    .await?
    .map(transition_from_row)
    .transpose()
}

fn transition_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<CoreActionTransition, ActionRepositoryError> {
    Ok(CoreActionTransition {
        transition_id: row.try_get("transition_id")?,
        action_id: row.try_get("action_id")?,
        from_state: row
            .try_get::<Option<String>, _>("from_state")?
            .map(|value| decode_action_state(&value))
            .transpose()?,
        to_state: decode_action_state(&row.try_get::<String, _>("to_state")?)?,
        reason: row.try_get("reason")?,
        evidence: row.try_get("evidence")?,
        occurred_at: row.try_get("occurred_at")?,
    })
}

async fn replay_transition(
    mut transaction: Transaction<'_, Postgres>,
    request: &ActionTransitionRequest,
    evidence: Value,
    transition: CoreActionTransition,
) -> Result<TransitionResult, ActionRepositoryError> {
    if transition.action_id != request.action_id
        || transition.from_state != Some(request.expected_state)
        || transition.to_state != request.next_state
        || transition.reason != request.reason
        || transition.evidence != evidence
    {
        return Err(ActionRepositoryError::TransitionIdempotencyConflict);
    }
    let action = load_action(&mut *transaction, request.action_id).await?;
    transaction.commit().await?;
    Ok(TransitionResult {
        action,
        transition,
        duplicate: true,
    })
}

fn ledger_request_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ActionLedgerRequest, ActionRepositoryError> {
    Ok(ActionLedgerRequest {
        request_id: row.try_get("request_id")?,
        action_id: row.try_get("action_id")?,
        step: ActionLedgerStep::from_str(&row.try_get::<String, _>("step")?)?,
        idempotency_key: row.try_get("idempotency_key")?,
        created_at: row.try_get("created_at")?,
    })
}

fn action_matches_input(action: &CoreAction, input: &NewCoreAction) -> bool {
    action.id == input.id
        && action.user_id == input.user_id
        && action.kind == input.kind
        && action.asset == input.asset
        && action.quantity == input.quantity
        && input
            .external_references
            .iter()
            .all(|(key, value)| action.external_references.get(key) == Some(value))
}

fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_and_transition_rules_are_explicit() {
        assert!(CoreActionState::Pending.is_active());
        assert!(CoreActionState::UnknownOutcome.is_active());
        assert!(!CoreActionState::Succeeded.is_active());
        assert!(CoreActionState::ExternallyPending.can_transition_to(CoreActionState::Succeeded));
        assert!(
            CoreActionState::UnknownOutcome.can_transition_to(CoreActionState::ExternallyPending)
        );
        assert!(!CoreActionState::Succeeded.can_transition_to(CoreActionState::Pending));
        assert_eq!(
            CoreActionKind::CustodyExit(CustodyExitDestination::HyperliquidMargin)
                .custody_exit_destination(),
            Some(CustodyExitDestination::HyperliquidMargin)
        );
    }

    #[test]
    fn reference_validation_rejects_log_unsafe_values() {
        let references = BTreeMap::from([("transaction_hash".into(), "bad\nvalue".into())]);
        assert!(matches!(
            validate_references(&references),
            Err(ActionRepositoryError::InvalidInput(_))
        ));
    }

    #[test]
    fn ledger_steps_match_the_ledger_command_contract() {
        let steps = [
            ActionLedgerStep::ReserveCustodyExit,
            ActionLedgerStep::DrawCreditToCustodyExit,
            ActionLedgerStep::ConsumeCustodyExit,
            ActionLedgerStep::ReleaseCustodyExit,
        ]
        .map(ActionLedgerStep::as_str);
        assert_eq!(steps, tackt_primitives::ledger::CUSTODY_EXIT_COMMAND_KINDS);
    }
}

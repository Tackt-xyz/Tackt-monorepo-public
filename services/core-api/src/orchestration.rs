//! Durable Core coordination across synchronous Risk and Ledger HTTP boundaries.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::{StatusCode, Url, redirect::Policy};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tackt_postgres::{
    ActionLedgerStep, ActionRepositoryError, ActionTransitionRequest, CoreAction, PgRepository,
};
use tackt_primitives::{
    CoreActionKind, CoreActionState,
    accounts::AccountSubject,
    risk::{RiskClassification, RiskEvaluation},
};
use thiserror::Error;
use uuid::Uuid;

const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

/// Complete normalized post-action account projection accepted by Risk.
///
/// The nested account records deliberately remain their strict JSON wire values:
/// their domain types are owned by the Risk service and service crates must not depend on one
/// another. Core validates all stable cross-service identity and version fields before sending.
#[derive(Clone, Debug, Serialize)]
pub struct RiskEvaluationRequest {
    pub action_id: Uuid,
    pub account: AccountSubject,
    pub account_version: u64,
    pub input_version: u64,
    pub projected_account: Value,
}

impl RiskEvaluationRequest {
    pub fn new(
        action_id: Uuid,
        account: AccountSubject,
        account_version: u64,
        input_version: u64,
        projected_account: Value,
    ) -> Result<Self, OrchestrationError> {
        if account_version == 0 || input_version == 0 {
            return Err(OrchestrationError::InvalidInput(
                "risk input versions must be positive",
            ));
        }
        if !projected_account.is_object() {
            return Err(OrchestrationError::InvalidInput(
                "projected account must be a JSON object",
            ));
        }
        Ok(Self {
            action_id,
            account,
            account_version,
            input_version,
            projected_account,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CustodyFunding {
    Available,
    Credit,
}

#[derive(Clone, Debug)]
pub struct ActionExecution {
    pub risk: RiskEvaluationRequest,
    pub chain_id: u64,
    pub custody_funding: CustodyFunding,
}

/// Trusted Core-owned source for the complete post-action projection sent to Risk.
///
/// This capability is intentionally not implemented from an API request: allowing the financial
/// caller to assert its own projected portfolio would make risk authorization bypassable.
#[async_trait]
pub trait ActionProjectionProvider: Send + Sync {
    async fn execution_for(&self, action: &CoreAction) -> Result<ActionExecution, ProjectionError>;
}

#[derive(Debug, Error)]
#[error("trusted action projection is unavailable")]
pub struct ProjectionError;

/// HTTP dependency configuration. Tokens are intentionally private and this type is not `Debug`.
#[derive(Clone)]
pub struct FinancialHttpConfig {
    risk_endpoint: Url,
    risk_token: Arc<str>,
    ledger_base: Url,
    ledger_token: Arc<str>,
    timeout: Duration,
}

impl FinancialHttpConfig {
    pub fn new(
        risk_base: &str,
        risk_token: String,
        ledger_base: &str,
        ledger_token: String,
        timeout: Duration,
    ) -> Result<Self, OrchestrationError> {
        let risk_base = service_url(risk_base, "risk")?;
        let ledger_base = service_url(ledger_base, "ledger")?;
        valid_token(&risk_token, "risk")?;
        valid_token(&ledger_token, "ledger")?;
        if timeout.is_zero() || timeout > Duration::from_secs(30) {
            return Err(OrchestrationError::InvalidConfiguration(
                "financial HTTP timeout must be within 1ms..=30s".into(),
            ));
        }
        Ok(Self {
            risk_endpoint: risk_base
                .join("internal/v1/risk/evaluations")
                .map_err(|error| OrchestrationError::InvalidConfiguration(error.to_string()))?,
            risk_token: Arc::from(risk_token),
            ledger_base,
            ledger_token: Arc::from(ledger_token),
            timeout,
        })
    }
}

#[derive(Clone)]
pub struct FinancialHttpClient {
    client: reqwest::Client,
    config: FinancialHttpConfig,
}

impl FinancialHttpClient {
    pub fn new(config: FinancialHttpConfig) -> Result<Self, OrchestrationError> {
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .redirect(Policy::none())
            .https_only(false)
            .build()
            .map_err(|error| OrchestrationError::InvalidConfiguration(error.to_string()))?;
        Ok(Self { client, config })
    }

    async fn evaluate(
        &self,
        request: &RiskEvaluationRequest,
    ) -> Result<RiskEvaluation, OrchestrationError> {
        let response = self
            .client
            .post(self.config.risk_endpoint.clone())
            .bearer_auth(self.config.risk_token.as_ref())
            .json(request)
            .send()
            .await
            .map_err(|error| {
                tracing::warn!(error = %error, "Risk evaluation transport failed");
                OrchestrationError::Risk(DependencyError::Transport(error))
            })?;
        let status = response.status();
        if !status.is_success() {
            tracing::warn!(%status, "Risk evaluation returned an error status");
            return Err(OrchestrationError::Risk(DependencyError::status(status)));
        }
        bounded_json(response)
            .await
            .map_err(OrchestrationError::Risk)
    }

    async fn reserve(
        &self,
        action: &CoreAction,
        execution: &ActionExecution,
        step: ActionLedgerStep,
        ledger_effective_at: DateTime<Utc>,
        idempotency_key: &str,
        evaluation: &RiskEvaluation,
    ) -> Result<LedgerOutcome, OrchestrationError> {
        let authorization = json!({
            "core_action_id": action.id.to_string(),
            "core_authorization_id": idempotency_key,
            "risk_evaluation_id": evaluation.evaluation_id(),
            "reason": "Core policy approved a healthy projected account"
        });
        let header = json!({
            "chain_id": execution.chain_id,
            // The durable request allocation time is stable across ambiguous retries, so Ledger
            // sees a byte-identical command under the reused idempotency key.
            "effective_at": ledger_effective_at,
            "external_reference": format!("{}:{}", action.id, step.as_str()),
            "authorization": authorization
        });
        let (path, body) = match action.kind {
            CoreActionKind::CustodyExit(destination) => {
                let funding = match execution.custody_funding {
                    CustodyFunding::Available => json!({
                        "funding": "available",
                        "header": header,
                        "user_id": action.user_id,
                        "reservation_id": action.id,
                        "asset": action.asset.asset(),
                        "quantity": action.quantity.to_string(),
                        "destination": destination
                    }),
                    CustodyFunding::Credit => json!({
                        "funding": "credit",
                        "header": header,
                        "user_id": action.user_id,
                        "reservation_id": action.id,
                        "quantity": action.quantity.to_string(),
                        "destination": destination
                    }),
                };
                ("internal/v1/custody-exit-reservations", funding)
            }
            CoreActionKind::VenueOrder => (
                "internal/v1/order-reservations",
                json!({
                    "header": header,
                    "user_id": action.user_id,
                    "reservation_id": action.id,
                    "quantity": action.quantity.to_string()
                }),
            ),
        };
        let endpoint = self
            .config
            .ledger_base
            .join(path)
            .map_err(|error| OrchestrationError::InvalidConfiguration(error.to_string()))?;
        let response = self
            .client
            .post(endpoint)
            .bearer_auth(self.config.ledger_token.as_ref())
            .header("Idempotency-Key", idempotency_key)
            .json(&body)
            .send()
            .await
            .map_err(|error| {
                tracing::warn!(error = %error, "Ledger reservation transport failed");
                OrchestrationError::Ledger(DependencyError::Transport(error))
            })?;
        let status = response.status();
        if status.is_success() {
            return bounded_json(response)
                .await
                .map_err(OrchestrationError::Ledger);
        }
        match bounded_json::<LedgerOutcome>(response).await {
            Ok(outcome @ LedgerOutcome::Rejected { .. }) => Ok(outcome),
            _ => {
                tracing::warn!(%status, "Ledger reservation returned an error status");
                Err(OrchestrationError::Ledger(DependencyError::status(status)))
            }
        }
    }
}

/// Core-owned coordinator. Risk supplies evidence; this type makes the authorization decision.
pub struct ActionOrchestrator {
    repo: Arc<PgRepository>,
    financial: FinancialHttpClient,
    projections: Arc<dyn ActionProjectionProvider>,
}

impl ActionOrchestrator {
    pub fn new(
        repo: Arc<PgRepository>,
        financial: FinancialHttpClient,
        projections: Arc<dyn ActionProjectionProvider>,
    ) -> Self {
        Self {
            repo,
            financial,
            projections,
        }
    }

    #[tracing::instrument(skip(self), fields(action_id = %action_id))]
    pub async fn execute(&self, action_id: Uuid) -> Result<ExecutionOutcome, OrchestrationError> {
        self.execute_action(self.repo.core_action(action_id).await?)
            .await
    }

    async fn execute_action(
        &self,
        action: CoreAction,
    ) -> Result<ExecutionOutcome, OrchestrationError> {
        if !action.state.is_active() {
            return Ok(ExecutionOutcome::AlreadyTerminal(action.state));
        }
        if action.state != CoreActionState::Pending {
            return Ok(ExecutionOutcome::AwaitingExternalStep(action.state));
        }
        let execution = self
            .projections
            .execution_for(&action)
            .await
            .map_err(OrchestrationError::Projection)?;
        validate_execution(&action, &execution)?;

        let evaluation = if let Some(stored) = &action.evaluation_evidence {
            stored.evaluation.clone()
        } else {
            let evaluation = self.financial.evaluate(&execution.risk).await?;
            validate_evaluation(&action, &execution.risk, &evaluation)?;
            self.repo
                .record_action_evaluation(action.id, &evaluation)
                .await?;
            evaluation
        };

        if evaluation.classification() != RiskClassification::Healthy {
            tracing::warn!(
                evaluation_id = evaluation.evaluation_id(),
                classification = ?evaluation.classification(),
                "Risk denied the Core action"
            );
            let state = self
                .transition(
                    &action,
                    CoreActionState::Failed,
                    "risk_evaluation_denied",
                    [("risk_evaluation_id", evaluation.evaluation_id())],
                )
                .await?;
            return Ok(ExecutionOutcome::RiskDenied {
                state,
                classification: evaluation.classification(),
            });
        }

        let step = match (action.kind, execution.custody_funding) {
            (CoreActionKind::CustodyExit(_), CustodyFunding::Available) => {
                ActionLedgerStep::ReserveCustodyExit
            }
            (CoreActionKind::CustodyExit(_), CustodyFunding::Credit) => {
                ActionLedgerStep::DrawCreditToCustodyExit
            }
            (CoreActionKind::VenueOrder, _) => ActionLedgerStep::ReserveVenueOrder,
        };
        let (request, _) = self
            .repo
            .ensure_action_ledger_request(action.id, step)
            .await?;
        match self
            .financial
            .reserve(
                &action,
                &execution,
                step,
                request.created_at,
                &request.idempotency_key,
                &evaluation,
            )
            .await?
        {
            LedgerOutcome::Committed {
                transaction_id,
                replayed,
                ..
            } => {
                let transaction = transaction_id.to_string();
                let state = self
                    .transition(
                        &action,
                        CoreActionState::WaitingForSigning,
                        "ledger_reservation_committed",
                        [
                            ("risk_evaluation_id", evaluation.evaluation_id()),
                            ("ledger_transaction_id", transaction.as_str()),
                        ],
                    )
                    .await?;
                Ok(ExecutionOutcome::Reserved {
                    state,
                    transaction_id: transaction,
                    replayed,
                })
            }
            LedgerOutcome::Rejected { code, .. } => {
                tracing::warn!(%code, "Ledger rejected the Core action reservation");
                let state = self
                    .transition(
                        &action,
                        CoreActionState::Failed,
                        "ledger_reservation_rejected",
                        [
                            ("risk_evaluation_id", evaluation.evaluation_id()),
                            ("ledger_rejection_code", code.as_str()),
                        ],
                    )
                    .await?;
                Ok(ExecutionOutcome::LedgerRejected { state, code })
            }
        }
    }

    /// Replays the stable active-action scan used after a process restart.
    pub async fn resume_active(
        &self,
        limit: u32,
    ) -> Result<Vec<(Uuid, Result<ExecutionOutcome, OrchestrationError>)>, ActionRepositoryError>
    {
        let mut outcomes = Vec::with_capacity(limit as usize);
        let mut after = None;
        while outcomes.len() < limit as usize {
            let remaining = limit - outcomes.len() as u32;
            let actions = self.repo.active_core_actions(remaining, after).await?;
            if actions.is_empty() {
                break;
            }
            for action in actions {
                after = Some((action.updated_at, action.id));
                let id = action.id;
                outcomes.push((id, self.execute_action(action).await));
            }
        }
        Ok(outcomes)
    }

    async fn transition<const N: usize>(
        &self,
        action: &CoreAction,
        next_state: CoreActionState,
        reason: &str,
        references: [(&str, &str); N],
    ) -> Result<CoreActionState, OrchestrationError> {
        let request = ActionTransitionRequest {
            transition_id: Uuid::new_v4(),
            action_id: action.id,
            expected_state: action.state,
            next_state,
            reason: reason.into(),
            risk_evidence: None,
            external_references: references
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect(),
        };
        Ok(self
            .repo
            .transition_core_action(request)
            .await?
            .action
            .state)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionOutcome {
    Reserved {
        state: CoreActionState,
        transaction_id: String,
        replayed: bool,
    },
    RiskDenied {
        state: CoreActionState,
        classification: RiskClassification,
    },
    LedgerRejected {
        state: CoreActionState,
        code: String,
    },
    AwaitingExternalStep(CoreActionState),
    AlreadyTerminal(CoreActionState),
}

#[derive(Debug, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
enum LedgerOutcome {
    Committed {
        transaction_id: Uuid,
        #[allow(dead_code)]
        recorded_at: DateTime<Utc>,
        replayed: bool,
    },
    Rejected {
        code: String,
        #[allow(dead_code)]
        message: String,
    },
}

#[derive(Debug, Error)]
pub enum OrchestrationError {
    #[error("invalid financial HTTP configuration: {0}")]
    InvalidConfiguration(String),
    #[error("invalid action execution: {0}")]
    InvalidInput(&'static str),
    #[error("trusted action projection is unavailable")]
    Projection(#[source] ProjectionError),
    #[error("Risk dependency failed: {0}")]
    Risk(#[source] DependencyError),
    #[error("Ledger dependency failed: {0}")]
    Ledger(#[source] DependencyError),
    #[error("action persistence failed: {0}")]
    Repository(#[from] ActionRepositoryError),
}

#[derive(Debug, Error)]
pub enum DependencyError {
    #[error("transport unavailable")]
    Transport(#[source] reqwest::Error),
    #[error("dependency returned HTTP {0}")]
    Status(StatusCode),
    #[error("dependency response was invalid or too large")]
    InvalidResponse,
}

impl DependencyError {
    fn status(status: StatusCode) -> Self {
        Self::Status(status)
    }
}

fn validate_execution(
    action: &CoreAction,
    execution: &ActionExecution,
) -> Result<(), OrchestrationError> {
    if execution.chain_id == 0
        || execution.risk.action_id != action.id
        || execution.risk.account.user_id() != action.user_id
        || tackt_primitives::assets::info(action.asset.asset()).evm_chain_id()
            != Some(execution.chain_id)
    {
        return Err(OrchestrationError::InvalidInput(
            "execution identity or chain does not match the action",
        ));
    }
    if action.kind == CoreActionKind::VenueOrder
        && action.asset.asset() != tackt_primitives::ledger::LedgerAsset::Usdc
    {
        return Err(OrchestrationError::InvalidInput(
            "venue-order reservations require USDC credit",
        ));
    }
    if action.kind.is_custody_exit()
        && execution.custody_funding == CustodyFunding::Credit
        && action.asset.asset() != tackt_primitives::ledger::LedgerAsset::Usdc
    {
        return Err(OrchestrationError::InvalidInput(
            "credit-funded custody exits require USDC",
        ));
    }
    Ok(())
}

fn validate_evaluation(
    action: &CoreAction,
    request: &RiskEvaluationRequest,
    evaluation: &RiskEvaluation,
) -> Result<(), OrchestrationError> {
    let versions = evaluation.versions();
    if evaluation.action_id() != action.id
        || evaluation.account().user_id() != action.user_id
        || versions.account_version != request.account_version
        || versions.input_version != request.input_version
    {
        return Err(OrchestrationError::InvalidInput(
            "Risk evidence does not match the submitted action and versions",
        ));
    }
    Ok(())
}

fn service_url(value: &str, service: &str) -> Result<Url, OrchestrationError> {
    let mut url = Url::parse(value)
        .map_err(|error| OrchestrationError::InvalidConfiguration(error.to_string()))?;
    if url.cannot_be_a_base()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(OrchestrationError::InvalidConfiguration(format!(
            "{service} URL must be an absolute base URL without userinfo, query, or fragment"
        )));
    }
    let secure = url.scheme() == "https";
    let loopback_http = crate::auth::is_loopback_http(&url);
    if !secure && !loopback_http {
        return Err(OrchestrationError::InvalidConfiguration(format!(
            "{service} URL requires HTTPS; loopback HTTP is development-only"
        )));
    }
    url.set_path(&format!("{}/", url.path().trim_end_matches('/')));
    Ok(url)
}

fn valid_token(token: &str, service: &str) -> Result<(), OrchestrationError> {
    if !(tackt_primitives::ledger::MIN_INTERNAL_TOKEN_BYTES
        ..=tackt_primitives::ledger::MAX_INTERNAL_TOKEN_BYTES)
        .contains(&token.len())
        || !token.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return Err(OrchestrationError::InvalidConfiguration(format!(
            "{service} token must contain 32-512 printable ASCII bytes"
        )));
    }
    Ok(())
}

async fn bounded_json<T: DeserializeOwned>(
    mut response: reqwest::Response,
) -> Result<T, DependencyError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
    {
        return Err(DependencyError::InvalidResponse);
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(DependencyError::Transport)? {
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(DependencyError::InvalidResponse);
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|_| DependencyError::InvalidResponse)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dependency_urls_fail_closed() {
        let token = "x".repeat(32);
        assert!(
            FinancialHttpConfig::new(
                "http://risk.example",
                token.clone(),
                "https://ledger.example",
                token.clone(),
                Duration::from_secs(5),
            )
            .is_err()
        );
        assert!(
            FinancialHttpConfig::new(
                "http://127.0.0.1:8081",
                token.clone(),
                "http://[::1]:8082",
                token,
                Duration::from_secs(5),
            )
            .is_ok()
        );
    }
}

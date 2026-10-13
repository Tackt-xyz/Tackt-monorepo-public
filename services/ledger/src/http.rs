use crate::{LedgerError, LedgerRepository};
use axum::{
    BoxError, Json, Router,
    error_handling::HandleErrorLayer,
    extract::{DefaultBodyLimit, Request, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{future::Future, sync::Arc, time::Duration};
use subtle::ConstantTimeEq;

use tackt_primitives::{
    is_reserved_observation_reference,
    ledger::{
        AuditMetadata, CommandHeader, CommandResult, CoreAuthorization, CoreAuthorizationError,
        CreditDestination, CustodyExitDestination, LedgerAsset, LedgerCommand,
        MAX_INTERNAL_TOKEN_BYTES, MIN_INTERNAL_TOKEN_BYTES,
    },
};
use tower::{
    ServiceBuilder,
    limit::GlobalConcurrencyLimitLayer,
    load_shed::{LoadShedLayer, error::Overloaded},
    timeout::error::Elapsed,
};
use tower_http::trace::TraceLayer;
use uuid::Uuid;

use crate::outcome;

const CORE_ACTOR: &str = "core-api";
const MAX_REFERENCE_BYTES: usize = 256;
const MAX_REASON_BYTES: usize = 512;
const MAX_CONFIGURABLE_BODY_BYTES: usize = 1024 * 1024;
const MAX_CONFIGURABLE_IN_FLIGHT: usize = 1024;
/// The readiness probe touches the database, so an operational request answers a plain 503
/// rather than hanging behind a stalled connection. A backstop, not a probe deadline: an
/// orchestrator's own timeout is what should normally decide a probe.
const OPERATIONAL_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct FinancialHttpConfig {
    auth: CoreAuth,
    max_body_bytes: usize,
    max_in_flight: usize,
    timeout: Duration,
}

impl FinancialHttpConfig {
    pub fn new(
        bearer_token: String,
        max_body_bytes: usize,
        max_in_flight: usize,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            (MIN_INTERNAL_TOKEN_BYTES..=MAX_INTERNAL_TOKEN_BYTES).contains(&bearer_token.len())
                && bearer_token.bytes().all(|byte| byte.is_ascii_graphic()),
            "LEDGER_INTERNAL_TOKEN must contain 32-512 printable ASCII characters"
        );
        anyhow::ensure!(
            (1..=MAX_CONFIGURABLE_BODY_BYTES).contains(&max_body_bytes),
            "LEDGER_HTTP_MAX_BODY_BYTES must be between 1 and {MAX_CONFIGURABLE_BODY_BYTES}"
        );
        anyhow::ensure!(
            (1..=MAX_CONFIGURABLE_IN_FLIGHT).contains(&max_in_flight),
            "LEDGER_HTTP_MAX_IN_FLIGHT must be between 1 and {MAX_CONFIGURABLE_IN_FLIGHT}"
        );
        Ok(Self {
            auth: CoreAuth(Arc::from(bearer_token)),
            max_body_bytes,
            max_in_flight,
            timeout: Duration::from_secs(30),
        })
    }
}

/// Operational routes stay public and disjoint from the authenticated,
/// explicitly allowlisted financial resources.
pub fn router<R, F, Fut>(repository: Arc<R>, readiness: F, financial: FinancialHttpConfig) -> Router
where
    R: LedgerRepository,
    F: Fn() -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = bool> + Send + 'static,
{
    let operational = operational_router(readiness, OPERATIONAL_TIMEOUT);
    let mutations = Router::new()
        .route("/credit-draws", post(credit_draw::<R>))
        .route(
            "/custody-exit-reservations",
            post(custody_exit_reservation::<R>),
        )
        .route(
            "/custody-exit-reservations/consume",
            post(consume_custody_exit::<R>),
        )
        .route(
            "/custody-exit-reservations/release",
            post(release_custody_exit::<R>),
        )
        .route("/order-reservations", post(order_reservation::<R>))
        .route(
            "/withdrawal-reservations",
            post(withdrawal_reservation::<R>),
        )
        .layer(DefaultBodyLimit::max(financial.max_body_bytes))
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(handle_service_error))
                .layer(LoadShedLayer::new())
                // `Router::layer` clones the layer once per route, so the limit must
                // own its semaphore: the global layer shares one across all three
                // routes instead of granting each its own set of permits.
                .layer(GlobalConcurrencyLimitLayer::new(financial.max_in_flight))
                .layer(tower::timeout::TimeoutLayer::new(financial.timeout)),
        )
        .route_layer(middleware::from_fn_with_state(financial.auth, require_core))
        .with_state(repository);

    Router::new()
        .merge(operational)
        .nest("/internal/v1", mutations)
        .layer(TraceLayer::new_for_http())
}

fn operational_router<F, Fut>(readiness: F, timeout: Duration) -> Router
where
    F: Fn() -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = bool> + Send + 'static,
{
    let ready = move || {
        let readiness = readiness.clone();
        async move {
            if readiness().await {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            }
        }
    };

    Router::new()
        .route("/health", get(|| async { StatusCode::OK }))
        .route("/ready", get(ready))
        .layer(tower_http::timeout::TimeoutLayer::with_status_code(
            StatusCode::SERVICE_UNAVAILABLE,
            timeout,
        ))
}

/// Both stack failures answer 503, but they are not the same incident: a shed
/// request never reached the repository, while a timed-out request may have
/// already committed. The response stays identical so the retry advice holds in
/// both cases; only the log distinguishes them.
async fn handle_service_error(error: BoxError) -> Response {
    if error.is::<Overloaded>() {
        tracing::warn!(%error, "financial command shed before execution; nothing was applied");
    } else if error.is::<Elapsed>() {
        tracing::error!(%error, "financial command timed out; commit state is unknown");
    } else {
        tracing::error!(%error, "financial command failed inside the service stack");
    }
    ApiError::unavailable().into_response()
}

#[derive(Clone)]
struct CoreAuth(Arc<str>);

async fn require_core(
    State(auth): State<CoreAuth>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let mut values = request.headers().get_all(AUTHORIZATION).iter();
    let offered = values.next();
    let presented = offered
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' '))
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("Bearer"))
        .map(|(_, token)| token);
    let ambiguous = values.next().is_some();
    let valid = !ambiguous
        && presented
            .is_some_and(|token| token.as_bytes().ct_eq(auth.0.as_bytes()).unwrap_u8() == 1);
    if !valid {
        // `reason` must stay coarse: neither the presented nor the configured
        // credential may reach the log, in whole or in part.
        let reason = if ambiguous {
            "more than one Authorization header"
        } else if offered.is_none() {
            "no Authorization header"
        } else if presented.is_none() {
            "Authorization header is not a printable Bearer credential"
        } else {
            "Bearer credential did not match"
        };
        tracing::warn!(reason, method = %request.method(), path = %request.uri().path(), "rejected an unauthenticated financial request");
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "valid Core credentials are required",
        ));
    }
    Ok(next.run(request).await)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HttpCommandHeader {
    chain_id: u64,
    effective_at: DateTime<Utc>,
    #[serde(default)]
    external_reference: Option<String>,
    authorization: HttpCoreAuthorization,
}

impl HttpCommandHeader {
    fn into_domain(self, idempotency_key: String) -> Result<CommandHeader, ApiError> {
        if self.chain_id == 0 {
            return Err(ApiError::invalid("chain_id must be positive"));
        }
        if let Some(reference) = &self.external_reference
            && !tackt_primitives::ledger::valid_display_text(reference, MAX_REFERENCE_BYTES)
        {
            return Err(ApiError::invalid("external_reference is invalid"));
        }
        // Observation-derived settlement keys are precomputable from public chain
        // data, so a Core caller that claimed one would occupy a deposit's row
        // before the settlement worker writes it and strand that deposit.
        if is_reserved_observation_reference(&idempotency_key)
            || self
                .external_reference
                .as_deref()
                .is_some_and(is_reserved_observation_reference)
        {
            return Err(ApiError::reserved_namespace());
        }
        let (authorization, reason) = self.authorization.into_domain()?;
        // Transport-level: the key Core retries under must be the authorization it approved,
        // so a newly generated key cannot book the same authorization twice.
        if authorization.core_authorization_id() != idempotency_key {
            return Err(ApiError::invalid_idempotency());
        }
        Ok(CommandHeader {
            chain_id: self.chain_id,
            idempotency_key,
            external_reference: self.external_reference,
            effective_at: self.effective_at,
            audit: AuditMetadata {
                actor: CORE_ACTOR.into(),
                reason,
                risk_snapshot_id: None,
                authorization: Some(authorization),
                attributes: Default::default(),
            },
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HttpCoreAuthorization {
    core_action_id: String,
    core_authorization_id: String,
    risk_evaluation_id: String,
    reason: String,
}

impl HttpCoreAuthorization {
    /// The typed evidence and the reason it records. The ledger enforces the evidence on every
    /// ingress; these checks only shape this transport's rejection.
    fn into_domain(self) -> Result<(CoreAuthorization, String), ApiError> {
        let authorization = CoreAuthorization::new(
            self.core_action_id,
            self.core_authorization_id,
            self.risk_evaluation_id,
        )
        .map_err(|error| match error {
            CoreAuthorizationError::InvalidIdentifier { .. } => {
                ApiError::invalid("authorization identifier is invalid")
            }
        })?;
        if !tackt_primitives::ledger::valid_display_text(&self.reason, MAX_REASON_BYTES) {
            return Err(ApiError::invalid("authorization reason is invalid"));
        }
        Ok((authorization, self.reason))
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Quantity {
    Text(String),
    Integer(u64),
    Other(serde::de::IgnoredAny),
}

impl Quantity {
    fn parse(self) -> Result<u128, ApiError> {
        match self {
            Self::Text(value) => value.parse().map_err(|_| ApiError::invalid_quantity()),
            Self::Integer(value) => Ok(value.into()),
            Self::Other(_) => Err(ApiError::invalid_quantity()),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreditDrawRequest {
    header: HttpCommandHeader,
    user_id: Uuid,
    quantity: Quantity,
    destination: CreditDestination,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OrderReservationRequest {
    header: HttpCommandHeader,
    user_id: Uuid,
    reservation_id: Uuid,
    quantity: Quantity,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WithdrawalReservationRequest {
    header: HttpCommandHeader,
    user_id: Uuid,
    reservation_id: Uuid,
    asset: LedgerAsset,
    quantity: Quantity,
}

#[derive(Deserialize)]
#[serde(tag = "funding", rename_all = "snake_case", deny_unknown_fields)]
enum CustodyExitReservationRequest {
    Available {
        header: HttpCommandHeader,
        user_id: Uuid,
        reservation_id: Uuid,
        asset: LedgerAsset,
        quantity: Quantity,
        destination: CustodyExitDestination,
    },
    Credit {
        header: HttpCommandHeader,
        user_id: Uuid,
        reservation_id: Uuid,
        quantity: Quantity,
        destination: CustodyExitDestination,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CustodyExitTransitionRequest {
    header: HttpCommandHeader,
    user_id: Uuid,
    reservation_id: Uuid,
    asset: LedgerAsset,
    quantity: Quantity,
    destination: CustodyExitDestination,
}

async fn credit_draw<R: LedgerRepository>(
    State(repository): State<Arc<R>>,
    headers: HeaderMap,
    payload: Result<Json<CreditDrawRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let (key, request) = decode(&headers, payload)?;
    non_nil(request.user_id, "user_id")?;
    let command = LedgerCommand::DrawCredit {
        header: request.header.into_domain(key)?,
        user_id: request.user_id,
        quantity: request.quantity.parse()?,
        destination: request.destination,
    };
    Ok(execute(repository.as_ref(), command).await)
}

async fn order_reservation<R: LedgerRepository>(
    State(repository): State<Arc<R>>,
    headers: HeaderMap,
    payload: Result<Json<OrderReservationRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let (key, request) = decode(&headers, payload)?;
    non_nil(request.user_id, "user_id")?;
    non_nil(request.reservation_id, "reservation_id")?;
    let command = LedgerCommand::ReserveCreditForOrder {
        header: request.header.into_domain(key)?,
        user_id: request.user_id,
        reservation_id: request.reservation_id,
        quantity: request.quantity.parse()?,
    };
    Ok(execute(repository.as_ref(), command).await)
}

async fn withdrawal_reservation<R: LedgerRepository>(
    State(repository): State<Arc<R>>,
    headers: HeaderMap,
    payload: Result<Json<WithdrawalReservationRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let (key, request) = decode(&headers, payload)?;
    non_nil(request.user_id, "user_id")?;
    non_nil(request.reservation_id, "reservation_id")?;
    let command = LedgerCommand::ReserveWithdrawal {
        header: request.header.into_domain(key)?,
        user_id: request.user_id,
        reservation_id: request.reservation_id,
        asset: request.asset,
        quantity: request.quantity.parse()?,
    };
    Ok(execute(repository.as_ref(), command).await)
}

async fn custody_exit_reservation<R: LedgerRepository>(
    State(repository): State<Arc<R>>,
    headers: HeaderMap,
    payload: Result<Json<CustodyExitReservationRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let (key, request) = decode(&headers, payload)?;
    let command = match request {
        CustodyExitReservationRequest::Available {
            header,
            user_id,
            reservation_id,
            asset,
            quantity,
            destination,
        } => {
            non_nil(user_id, "user_id")?;
            non_nil(reservation_id, "reservation_id")?;
            LedgerCommand::ReserveCustodyExit {
                header: header.into_domain(key)?,
                user_id,
                reservation_id,
                asset,
                quantity: quantity.parse()?,
                destination,
            }
        }
        CustodyExitReservationRequest::Credit {
            header,
            user_id,
            reservation_id,
            quantity,
            destination,
        } => {
            non_nil(user_id, "user_id")?;
            non_nil(reservation_id, "reservation_id")?;
            LedgerCommand::DrawCreditToCustodyExit {
                header: header.into_domain(key)?,
                user_id,
                reservation_id,
                quantity: quantity.parse()?,
                destination,
            }
        }
    };
    Ok(execute(repository.as_ref(), command).await)
}

async fn consume_custody_exit<R: LedgerRepository>(
    State(repository): State<Arc<R>>,
    headers: HeaderMap,
    payload: Result<Json<CustodyExitTransitionRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let (key, request) = decode(&headers, payload)?;
    non_nil(request.user_id, "user_id")?;
    non_nil(request.reservation_id, "reservation_id")?;
    let command = LedgerCommand::ConsumeCustodyExit {
        header: request.header.into_domain(key)?,
        user_id: request.user_id,
        reservation_id: request.reservation_id,
        asset: request.asset,
        quantity: request.quantity.parse()?,
        destination: request.destination,
    };
    Ok(execute(repository.as_ref(), command).await)
}

async fn release_custody_exit<R: LedgerRepository>(
    State(repository): State<Arc<R>>,
    headers: HeaderMap,
    payload: Result<Json<CustodyExitTransitionRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let (key, request) = decode(&headers, payload)?;
    non_nil(request.user_id, "user_id")?;
    non_nil(request.reservation_id, "reservation_id")?;
    let command = LedgerCommand::ReleaseCustodyExit {
        header: request.header.into_domain(key)?,
        user_id: request.user_id,
        reservation_id: request.reservation_id,
        asset: request.asset,
        quantity: request.quantity.parse()?,
        destination: request.destination,
    };
    Ok(execute(repository.as_ref(), command).await)
}

fn decode<T>(
    headers: &HeaderMap,
    payload: Result<Json<T>, JsonRejection>,
) -> Result<(String, T), ApiError> {
    let key = idempotency_key(headers)?;
    match payload {
        Ok(Json(payload)) => Ok((key, payload)),
        Err(error) => match error.status() {
            StatusCode::PAYLOAD_TOO_LARGE => Err(ApiError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "request_too_large",
                "request body exceeds the configured limit",
            )),
            StatusCode::UNSUPPORTED_MEDIA_TYPE => Err(ApiError::new(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                "Content-Type must be application/json",
            )),
            StatusCode::BAD_REQUEST => Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "malformed_json",
                "request body is not valid JSON",
            )),
            _ => Err(ApiError::invalid("request body is invalid")),
        },
    }
}

fn idempotency_key(headers: &HeaderMap) -> Result<String, ApiError> {
    let mut values = headers.get_all("idempotency-key").iter();
    let key = values
        .next()
        .and_then(|value| value.to_str().ok())
        .filter(|key| tackt_primitives::ledger::valid_idempotency_key(key));
    if values.next().is_some() {
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_idempotency_key",
            "one valid Idempotency-Key header is required",
        ));
    }
    let Some(key) = key else {
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_idempotency_key",
            "one valid Idempotency-Key header is required",
        ));
    };
    Ok(key.to_owned())
}

fn non_nil(value: Uuid, field: &'static str) -> Result<(), ApiError> {
    if value.is_nil() {
        Err(ApiError::invalid(match field {
            "user_id" => "user_id must not be nil",
            "reservation_id" => "reservation_id must not be nil",
            _ => "identifier must not be nil",
        }))
    } else {
        Ok(())
    }
}

async fn execute<R: LedgerRepository>(repository: &R, command: LedgerCommand) -> Response {
    // Read the audit identity before execution consumes the command.
    let key = command.header().idempotency_key.clone();
    let kind = command.kind();
    let executed = repository.execute(command).await;
    match outcome::record(&key, kind, executed) {
        Ok(result) => committed(result),
        Err(LedgerError::InvalidQuantity) => rejected(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_quantity",
            "quantity must be a positive atomic-unit integer",
        ),
        Err(LedgerError::InsufficientFunds) => rejected(
            StatusCode::CONFLICT,
            "insufficient_funds",
            "available balance is insufficient",
        ),
        Err(LedgerError::IdempotencyConflict) => rejected(
            StatusCode::CONFLICT,
            "idempotency_conflict",
            "idempotency key was already used for different contents",
        ),
        Err(LedgerError::Conflict(_) | LedgerError::MissingApproval) => rejected(
            StatusCode::CONFLICT,
            "command_conflict",
            "command conflicts with current ledger state",
        ),
        Err(LedgerError::Storage(_) | LedgerError::Integrity(_)) => {
            ApiError::unavailable().into_response()
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
enum CommandResponse {
    Committed {
        transaction_id: Uuid,
        recorded_at: DateTime<Utc>,
        replayed: bool,
    },
    Rejected {
        code: &'static str,
        message: &'static str,
    },
}

fn committed(result: CommandResult) -> Response {
    Json(CommandResponse::Committed {
        transaction_id: result.transaction_id,
        recorded_at: result.recorded_at,
        replayed: result.duplicate,
    })
    .into_response()
}

fn rejected(status: StatusCode, code: &'static str, message: &'static str) -> Response {
    (status, Json(CommandResponse::Rejected { code, message })).into_response()
}

struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: &'static str,
    envelope: ErrorEnvelopeKind,
}

enum ErrorEnvelopeKind {
    Rejection,
    Request { retryable: bool },
}

impl ApiError {
    const fn new(status: StatusCode, code: &'static str, message: &'static str) -> Self {
        Self {
            status,
            code,
            message,
            envelope: ErrorEnvelopeKind::Request { retryable: false },
        }
    }

    const fn invalid(message: &'static str) -> Self {
        Self::new(StatusCode::UNPROCESSABLE_ENTITY, "invalid_request", message)
    }

    const fn invalid_quantity() -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            code: "invalid_quantity",
            message: "quantity must be a positive atomic-unit integer",
            envelope: ErrorEnvelopeKind::Rejection,
        }
    }

    const fn reserved_namespace() -> Self {
        Self::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "reserved_reference",
            "Idempotency-Key and external_reference must not use the reserved observation prefix",
        )
    }

    const fn invalid_idempotency() -> Self {
        Self::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_idempotency_key",
            "Idempotency-Key must equal core_authorization_id",
        )
    }

    const fn unavailable() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "temporarily_unavailable",
            message: "command outcome is unknown; retry with the same idempotency key",
            envelope: ErrorEnvelopeKind::Request { retryable: true },
        }
    }
}

#[derive(Serialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    code: &'static str,
    message: &'static str,
    retryable: bool,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let ErrorEnvelopeKind::Request { retryable } = self.envelope else {
            return rejected(self.status, self.code, self.message);
        };
        (
            self.status,
            Json(ErrorEnvelope {
                error: ErrorBody {
                    code: self.code,
                    message: self.message,
                    retryable,
                },
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use axum::{body::Body, http::Request};
    use chrono::TimeZone;
    use serde_json::Value;
    use std::sync::Mutex;
    use tower::ServiceExt;

    const TOKEN: &str = "0123456789abcdef0123456789abcdef";
    const USER: &str = "11111111-1111-4111-8111-111111111111";

    #[derive(Clone, Copy)]
    enum Outcome {
        Commit,
        Slow,
        InvalidQuantity,
        InsufficientFunds,
        IdempotencyConflict,
    }

    struct StubRepository {
        outcome: Outcome,
        command: Mutex<Option<LedgerCommand>>,
    }

    #[async_trait]
    impl LedgerRepository for StubRepository {
        async fn execute(&self, command: LedgerCommand) -> Result<CommandResult, LedgerError> {
            *self.command.lock().unwrap() = Some(command);
            if let Outcome::Slow = self.outcome {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            match self.outcome {
                Outcome::Commit | Outcome::Slow => Ok(CommandResult {
                    transaction_id: Uuid::nil(),
                    duplicate: false,
                    recorded_at: Utc.timestamp_opt(1, 0).unwrap(),
                }),
                Outcome::InvalidQuantity => Err(LedgerError::InvalidQuantity),
                Outcome::InsufficientFunds => Err(LedgerError::InsufficientFunds),
                Outcome::IdempotencyConflict => Err(LedgerError::IdempotencyConflict),
            }
        }
    }

    fn app(outcome: Outcome) -> (Router, Arc<StubRepository>) {
        app_with_config(
            outcome,
            FinancialHttpConfig::new(TOKEN.into(), 16 * 1024, 64).unwrap(),
        )
    }

    fn app_with_config(
        outcome: Outcome,
        config: FinancialHttpConfig,
    ) -> (Router, Arc<StubRepository>) {
        let repository = Arc::new(StubRepository {
            outcome,
            command: Mutex::new(None),
        });
        let app = router(repository.clone(), || async { true }, config);
        (app, repository)
    }

    async fn operational_status(ready: bool, path: &str) -> StatusCode {
        let repository = Arc::new(StubRepository {
            outcome: Outcome::Commit,
            command: Mutex::new(None),
        });
        let config = FinancialHttpConfig::new(TOKEN.into(), 16 * 1024, 64).unwrap();
        router(repository, move || async move { ready }, config)
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status()
    }

    fn body(quantity: &str) -> String {
        format!(
            r#"{{
                "header":{{
                    "chain_id":1,
                    "effective_at":"2026-08-24T12:00:00Z",
                    "authorization":{{
                        "core_action_id":"action-1",
                        "core_authorization_id":"credit-draw-1",
                        "risk_evaluation_id":"evaluation-1",
                        "reason":"approved credit draw"
                    }}
                }},
                "user_id":"{USER}",
                "quantity":"{quantity}",
                "destination":"ethereum_available"
            }}"#
        )
    }

    fn request_to(path: &str, body: String) -> Request<Body> {
        Request::post(path)
            .header(AUTHORIZATION, format!("Bearer {TOKEN}"))
            .header("idempotency-key", "credit-draw-1")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap()
    }

    fn request(body: String) -> Request<Body> {
        request_to("/internal/v1/credit-draws", body)
    }

    async fn json(response: Response) -> Value {
        let bytes = axum::body::to_bytes(response.into_body(), 32 * 1024)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn operational_and_financial_routes_stay_separate() {
        let (app, _) = app(Outcome::Commit);
        assert_eq!(
            app.clone()
                .oneshot(Request::get("/health").body(Body::empty()).unwrap())
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            app.clone()
                .oneshot(Request::post("/v1/commands").body(Body::empty()).unwrap())
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        for path in [format!("/v1/users/{USER}/balances"), "/v1/export".into()] {
            assert_eq!(
                app.clone()
                    .oneshot(Request::get(path).body(Body::empty()).unwrap())
                    .await
                    .unwrap()
                    .status(),
                StatusCode::NOT_FOUND
            );
        }
        assert_eq!(
            app.oneshot(
                Request::post("/internal/v1/credit-draws")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status(),
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn operational_endpoints_report_liveness_and_readiness() {
        assert_eq!(operational_status(false, "/health").await, StatusCode::OK);
        assert_eq!(operational_status(true, "/ready").await, StatusCode::OK);
        assert_eq!(
            operational_status(false, "/ready").await,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn stalled_readiness_returns_service_unavailable() {
        let app = operational_router(std::future::pending::<bool>, Duration::from_millis(1));

        let response = app
            .oneshot(Request::get("/ready").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn authenticated_request_derives_audit_actor_and_evidence() {
        let (app, repository) = app(Outcome::Commit);
        let response = app.oneshot(request(body("10"))).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(json(response).await["outcome"], "committed");
        let command = repository.command.lock().unwrap().take().unwrap();
        let header = command.header();
        assert_eq!(header.audit.actor, CORE_ACTOR);
        assert!(header.audit.attributes.is_empty());
        let authorization = header
            .audit
            .authorization
            .as_ref()
            .expect("core authorization evidence");
        assert_eq!(authorization.core_action_id(), "action-1");
        assert_eq!(authorization.core_authorization_id(), "credit-draw-1");
        assert_eq!(authorization.risk_evaluation_id(), "evaluation-1");
    }

    #[tokio::test]
    async fn every_financial_route_accepts_its_typed_request_without_postgres() {
        let (app, repository) = app(Outcome::Commit);
        let order = body("10").replace(
            r#""destination":"ethereum_available""#,
            r#""reservation_id":"22222222-2222-4222-8222-222222222222""#,
        );
        let response = app
            .clone()
            .oneshot(request_to("/internal/v1/order-reservations", order))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let withdrawal = body("10").replace(
            r#""destination":"ethereum_available""#,
            r#""reservation_id":"33333333-3333-4333-8333-333333333333","asset":"USDC""#,
        );
        let response = app
            .clone()
            .oneshot(request_to(
                "/internal/v1/withdrawal-reservations",
                withdrawal,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let action = "44444444-4444-4444-8444-444444444444";
        let create = body("10")
            .replace("action-1", action)
            .replace(
                r#""destination":"ethereum_available""#,
                &format!(
                    r#""reservation_id":"{action}","destination":"hyperliquid_margin","funding":"credit""#
                ),
            );
        let response = app
            .clone()
            .oneshot(request_to("/internal/v1/custody-exit-reservations", create))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let transition = body("10").replace("action-1", action).replace(
            r#""destination":"ethereum_available""#,
            &format!(
                r#""reservation_id":"{action}","asset":"USDC","destination":"hyperliquid_margin""#
            ),
        );
        for path in [
            "/internal/v1/custody-exit-reservations/consume",
            "/internal/v1/custody-exit-reservations/release",
        ] {
            let response = app
                .clone()
                .oneshot(request_to(path, transition.clone()))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }
        assert!(repository.command.lock().unwrap().is_some());
    }

    #[tokio::test]
    async fn quantities_accept_json_integers_and_reject_other_json_types() {
        let (app, repository) = app(Outcome::Commit);
        let integer = body("10").replace(r#""quantity":"10""#, r#""quantity":10"#);
        assert_eq!(
            app.clone()
                .oneshot(request(integer))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        repository.command.lock().unwrap().take();

        for invalid in ["1.5", "true", "{}"] {
            let invalid =
                body("10").replace(r#""quantity":"10""#, &format!(r#""quantity":{invalid}"#));
            let response = app.clone().oneshot(request(invalid)).await.unwrap();
            assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
            assert_eq!(json(response).await["code"], "invalid_quantity");
            assert!(repository.command.lock().unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn deterministic_rejections_have_stable_codes() {
        for (outcome, status, code) in [
            (
                Outcome::InvalidQuantity,
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_quantity",
            ),
            (
                Outcome::InsufficientFunds,
                StatusCode::CONFLICT,
                "insufficient_funds",
            ),
            (
                Outcome::IdempotencyConflict,
                StatusCode::CONFLICT,
                "idempotency_conflict",
            ),
        ] {
            let (app, _) = app(outcome);
            let response = app.oneshot(request(body("10"))).await.unwrap();
            assert_eq!(response.status(), status);
            let response = json(response).await;
            assert_eq!(response["outcome"], "rejected");
            assert_eq!(response["code"], code);
        }

        let (app, repository) = app(Outcome::Commit);
        let malformed = app.oneshot(request(body("not-an-integer"))).await.unwrap();
        assert_eq!(malformed.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let malformed = json(malformed).await;
        assert_eq!(malformed["outcome"], "rejected");
        assert_eq!(malformed["code"], "invalid_quantity");
        assert!(repository.command.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn authentication_and_idempotency_fail_closed() {
        let (app, repository) = app(Outcome::Commit);
        let mut wrong = request(body("10"));
        wrong
            .headers_mut()
            .insert(AUTHORIZATION, "Bearer wrong".parse().unwrap());
        assert_eq!(
            app.clone().oneshot(wrong).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );

        let mut duplicate_auth = request(body("10"));
        duplicate_auth
            .headers_mut()
            .append(AUTHORIZATION, format!("Bearer {TOKEN}").parse().unwrap());
        assert_eq!(
            app.clone().oneshot(duplicate_auth).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );

        let mut duplicate_key = request(body("10"));
        duplicate_key
            .headers_mut()
            .append("idempotency-key", "credit-draw-1".parse().unwrap());
        assert_eq!(
            app.clone().oneshot(duplicate_key).await.unwrap().status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );

        let mut rebound = request(body("10"));
        rebound
            .headers_mut()
            .insert("idempotency-key", "different-key".parse().unwrap());
        let rebound = app.oneshot(rebound).await.unwrap();
        assert_eq!(rebound.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            json(rebound).await["error"]["code"],
            "invalid_idempotency_key"
        );
        assert!(repository.command.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn reserved_observation_namespace_is_refused() {
        let (app, repository) = app(Outcome::Commit);
        let prefix = tackt_primitives::OBSERVATION_ID_PREFIX;
        let reserved = format!("{prefix}00ff:pending");

        let mut claimed_key = request(body("10").replace("credit-draw-1", &reserved));
        claimed_key
            .headers_mut()
            .insert("idempotency-key", reserved.parse().unwrap());
        let refused = app.clone().oneshot(claimed_key).await.unwrap();
        assert_eq!(refused.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json(refused).await["error"]["code"], "reserved_reference");

        let claimed_reference = body("10").replace(
            r#""chain_id":1,"#,
            &format!(r#""chain_id":1,"external_reference":"{reserved}","#),
        );
        let refused = app.oneshot(request(claimed_reference)).await.unwrap();
        assert_eq!(refused.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json(refused).await["error"]["code"], "reserved_reference");

        assert!(repository.command.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn request_shape_and_limits_have_typed_errors() {
        let (app, _) = app(Outcome::Commit);
        let mut missing_type = request(body("10"));
        missing_type.headers_mut().remove("content-type");
        let missing_type = app.clone().oneshot(missing_type).await.unwrap();
        assert_eq!(missing_type.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_eq!(
            json(missing_type).await["error"]["code"],
            "unsupported_media_type"
        );

        let malformed = app.clone().oneshot(request("{".into())).await.unwrap();
        assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
        assert_eq!(json(malformed).await["error"]["code"], "malformed_json");

        let unknown = body("10").replace(
            &format!(r#""user_id":"{USER}""#),
            &format!(r#""unknown":1,"user_id":"{USER}""#),
        );
        let unknown = app.oneshot(request(unknown)).await.unwrap();
        assert_eq!(unknown.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let config = FinancialHttpConfig::new(TOKEN.into(), 128, 64).unwrap();
        let (limited, _) = app_with_config(Outcome::Commit, config);
        let limited = limited.oneshot(request(body("10"))).await.unwrap();
        assert_eq!(limited.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(json(limited).await["error"]["code"], "request_too_large");
    }

    #[tokio::test]
    async fn timeout_is_retryable_and_typed() {
        let mut config = FinancialHttpConfig::new(TOKEN.into(), 16 * 1024, 64).unwrap();
        config.timeout = Duration::from_millis(1);
        let (app, _) = app_with_config(Outcome::Slow, config);
        let response = app.oneshot(request(body("10"))).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let response = json(response).await;
        assert_eq!(response["error"]["code"], "temporarily_unavailable");
        assert_eq!(response["error"]["retryable"], true);
    }

    #[test]
    fn financial_config_fails_closed() {
        assert!(FinancialHttpConfig::new("short".into(), 1, 1).is_err());
        assert!(FinancialHttpConfig::new(TOKEN.into(), 0, 1).is_err());
        assert!(FinancialHttpConfig::new(TOKEN.into(), 1, 0).is_err());
        assert!(
            FinancialHttpConfig::new(TOKEN.into(), MAX_CONFIGURABLE_BODY_BYTES + 1, 1).is_err()
        );
        assert!(FinancialHttpConfig::new(TOKEN.into(), 1, MAX_CONFIGURABLE_IN_FLIGHT + 1).is_err());
    }
}

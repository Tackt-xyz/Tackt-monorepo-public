use crate::auth::{OperatorAuth, UserAuth, require_operator};
use crate::openapi::{CoreUserSchema, CreateCoreUserSchema, UserStatusSchema, routes};
use axum::{
    Json, Router,
    extract::{Path, Query, Request, State},
    http::{
        StatusCode,
        header::{CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE},
    },
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use tackt_nats::{NatsClient, UserEventKind};
use tackt_postgres::{PgRepository, UserRepositoryError};
use tackt_primitives::{CoreUser, CreateCoreUser, Source, UserStatus};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

#[derive(Clone)]
pub struct ApiState {
    pub repo: Arc<PgRepository>,
    pub prices: crate::prices::PriceCache,
    /// Publishes user-lifecycle events after commits. Optional: without NATS
    /// the API works unchanged and consumers rely on database polling.
    pub events: Option<Arc<NatsClient>>,
    pub user_auth: UserAuth,
    pub user_signup_enabled: bool,
    pub beta_invite_code: Option<crate::beta_access::BetaInviteCode>,
}

pub fn router(
    repo: Arc<PgRepository>,
    operator_token: String,
    events: Option<Arc<NatsClient>>,
    user_auth: UserAuth,
    user_signup_enabled: bool,
    beta_invite_code: Option<crate::beta_access::BetaInviteCode>,
    prices: crate::prices::PriceCache,
) -> Router {
    let origins = user_auth.origins();
    tackt_telemetry::initialise_metrics();
    let state = ApiState {
        repo,
        prices,
        events,
        user_auth,
        user_signup_enabled,
        beta_invite_code,
    };
    let (app, _) = documented_routes(OperatorAuth::new(operator_token));
    app.layer(middleware::from_fn(json_error_envelope))
        .layer(middleware::from_fn_with_state(
            origins.clone(),
            crate::auth::http::protect_origin,
        ))
        .layer(crate::auth::http::cors(origins))
        .layer(middleware::from_fn(crate::auth::http::observe))
        .with_state(state)
}
pub(crate) fn documented_routes(
    operator: OperatorAuth,
) -> (Router<ApiState>, utoipa::openapi::OpenApi) {
    let (operator_routes, mut spec) = routes!(
        create_user => __path_create_user,
        list_users => __path_list_users,
        get_user => __path_get_user,
        update_status => __path_update_status,
        replace_account => __path_replace_account,
        crate::auth::routes::link_identity => crate::auth::routes::__path_link_identity,
    );
    let operator_routes =
        operator_routes.route_layer(middleware::from_fn_with_state(operator, require_operator));
    let (auth, auth_spec) = crate::auth::router();
    let (beta, beta_spec) = crate::beta_access::router();
    let (summary, summary_spec) = crate::account_summary::router();
    let (activity, activity_spec) = crate::activity::router();
    let (catalog, catalog_spec) = crate::catalog::router();
    let (risk_history, risk_history_spec) = crate::risk_history::router();
    let (app, operations_spec) =
        routes!(health => __path_health, ready => __path_ready, metrics => __path_metrics);
    spec.merge(auth_spec);
    spec.merge(beta_spec);
    spec.merge(summary_spec);
    spec.merge(activity_spec);
    spec.merge(catalog_spec);
    spec.merge(risk_history_spec);
    spec.merge(operations_spec);
    (
        app.merge(operator_routes)
            .merge(auth)
            .merge(beta)
            .merge(summary)
            .merge(activity)
            .merge(catalog)
            .merge(risk_history),
        spec,
    )
}

#[utoipa::path(get, path = "/metrics", responses((status = 200, body = String, content_type = "text/plain; version=0.0.4")), tag = "Operations")]
async fn metrics() -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "text/plain; version=0.0.4")],
        tackt_telemetry::initialise_metrics().render(),
    )
}

#[derive(Serialize, ToSchema)]
struct Readiness {
    ready: bool,
}

#[utoipa::path(get, path = "/ready", responses((status = 200, body = Readiness)), tag = "Operations")]
async fn ready(State(state): State<ApiState>) -> Response {
    // Optional lifecycle publishing is an acceleration channel; a NATS outage
    // must not make committed user operations fail or remove this replica.
    if state.repo.is_ready().await {
        Json(Readiness { ready: true }).into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": {
            "code": "DATABASE_UNAVAILABLE", "message": "The service is temporarily unavailable. Please try again shortly.", "sanitized": true
        }}))).into_response()
    }
}

#[utoipa::path(get, path = "/health", responses((status = 200, description = "Alive; empty body")), tag = "Operations")]
async fn health() -> StatusCode {
    StatusCode::OK
}

/// Guarantees every error response carries the `{"error":{code,message}}`
/// envelope. Handler errors already produce it, but axum extractor rejections
/// (unknown path segments, malformed JSON bodies, method mismatches) emit plain
/// text; this outermost layer rewraps those so clients can rely on one format.
pub(crate) async fn json_error_envelope(request: Request, next: Next) -> Response {
    let response = next.run(request).await;
    let status = if response.status() == StatusCode::UNPROCESSABLE_ENTITY {
        StatusCode::BAD_REQUEST
    } else {
        response.status()
    };
    if !status.is_client_error() && !status.is_server_error() {
        return response;
    }
    let already_json = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/json"));
    if already_json {
        return response;
    }
    // Only the representation changes: non-body headers such as Allow and
    // WWW-Authenticate must survive the rewrap.
    let (parts, _) = response.into_parts();
    let message = match status {
        StatusCode::BAD_REQUEST => "Invalid request. Check the request body and parameters.",
        StatusCode::NOT_FOUND => "The requested resource was not found.",
        StatusCode::METHOD_NOT_ALLOWED => "This request method is not supported.",
        StatusCode::UNSUPPORTED_MEDIA_TYPE => "Send the request body as application/json.",
        StatusCode::PAYLOAD_TOO_LARGE => "The request body is too large.",
        _ if status.is_server_error() => "Something went wrong. Please try again shortly.",
        _ => "The request could not be completed.",
    };
    let code = match status {
        StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => "VALIDATION_ERROR",
        StatusCode::UNAUTHORIZED => "UNAUTHORIZED",
        StatusCode::NOT_FOUND => "NOT_FOUND",
        StatusCode::METHOD_NOT_ALLOWED => "METHOD_NOT_ALLOWED",
        StatusCode::UNSUPPORTED_MEDIA_TYPE => "UNSUPPORTED_MEDIA_TYPE",
        _ if status.is_server_error() => "INTERNAL_ERROR",
        _ => "REQUEST_ERROR",
    };
    let mut body = json!({"error":{"code":code,"message":message,"sanitized":true}});
    if status == StatusCode::BAD_REQUEST {
        body["error"]["details"] =
            json!([{ "field": "request", "message": "Check the request body and parameters." }]);
    }
    let mut rewrapped = (status, Json(body)).into_response();
    for (name, value) in &parts.headers {
        if name != CONTENT_TYPE && name != CONTENT_LENGTH && name != CONTENT_ENCODING {
            rewrapped.headers_mut().append(name, value.clone());
        }
    }
    rewrapped
}

/// Publish a lifecycle event after a committed mutation. Failures are logged
/// and never fail the response: the stream is an acceleration channel, and a
/// publish lost here (or a crash between commit and publish) is bounded by
/// the indexer's periodic re-read of the users table.
pub(crate) async fn publish_event(state: &ApiState, kind: UserEventKind) {
    let Some(events) = &state.events else { return };
    let request_id = crate::auth::http::request_id();
    if let Err(error) = events
        .publish_user_event_with_request_id(
            kind,
            request_id.as_ref().and_then(|id| id.to_str().ok()),
        )
        .await
    {
        tracing::warn!(%error, "user lifecycle event publish failed");
    }
}

#[derive(Debug)]
pub(crate) enum ApiError {
    Validation(String),
    Repository(UserRepositoryError),
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            Self::Validation(m) => (StatusCode::BAD_REQUEST, "VALIDATION_ERROR", m),
            Self::Repository(UserRepositoryError::NotFound) => {
                (StatusCode::NOT_FOUND, "NOT_FOUND", "user not found".into())
            }
            Self::Repository(UserRepositoryError::IdempotencyConflict) => (
                StatusCode::CONFLICT,
                "IDEMPOTENCY_CONFLICT",
                "external_id already exists with different immutable creation data".into(),
            ),
            Self::Repository(UserRepositoryError::AssociationConflict) => (
                StatusCode::CONFLICT,
                "ASSOCIATION_CONFLICT",
                "account is associated with another user".into(),
            ),
            Self::Repository(UserRepositoryError::IdentityConflict) => (
                StatusCode::CONFLICT,
                "IDENTITY_CONFLICT",
                "external identity is already linked to another user".into(),
            ),
            Self::Repository(UserRepositoryError::InvalidIdentity) => (
                StatusCode::BAD_REQUEST,
                "VALIDATION_ERROR",
                "issuer and subject must be non-empty, bounded, and contain no control characters"
                    .into(),
            ),
            Self::Repository(e @ UserRepositoryError::InvalidTransition(..)) => {
                (StatusCode::CONFLICT, "INVALID_TRANSITION", e.to_string())
            }
            Self::Repository(e) if e.is_unavailable() => {
                tracing::error!("database unavailable");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "DATABASE_UNAVAILABLE",
                    "The service is temporarily unavailable. Please try again shortly.".into(),
                )
            }
            Self::Repository(e) => {
                tracing::error!(error=%e,"request failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_ERROR",
                    "internal error".into(),
                )
            }
        };
        (
            status,
            Json(json!({"error":{"code":code,"message":message,"sanitized":true}})),
        )
            .into_response()
    }
}
impl From<UserRepositoryError> for ApiError {
    fn from(e: UserRepositoryError) -> Self {
        Self::Repository(e)
    }
}

fn validate_metadata(value: &Value) -> Result<(), ApiError> {
    if !value.is_object() {
        return Err(ApiError::Validation(
            "metadata must be a JSON object".into(),
        ));
    }
    const FORBIDDEN: &[&str] = &[
        "secret",
        "password",
        "private_key",
        "signing_key",
        "credential",
        "mnemonic",
        "seed_phrase",
    ];
    fn inspect(value: &Value) -> bool {
        match value {
            Value::Object(map) => map.iter().any(|(key, value)| {
                FORBIDDEN
                    .iter()
                    .any(|term| key.to_ascii_lowercase().contains(term))
                    || inspect(value)
            }),
            Value::Array(values) => values.iter().any(inspect),
            _ => false,
        }
    }
    if inspect(value) {
        return Err(ApiError::Validation(
            "metadata must not contain secrets or signing credentials".into(),
        ));
    }
    Ok(())
}

#[derive(Serialize, ToSchema)]
struct CreatedUser {
    #[schema(value_type = CoreUserSchema)]
    user: CoreUser,
    created: bool,
}
#[utoipa::path(post, path = "/v1/users", request_body = CreateCoreUserSchema, responses((status = 200, body = CreatedUser), (status = 201, body = CreatedUser)), security(("OperatorBearer" = [])), tag = "Operators")]
async fn create_user(
    State(s): State<ApiState>,
    Json(mut input): Json<CreateCoreUser>,
) -> Result<(StatusCode, Json<CreatedUser>), ApiError> {
    input.external_id = input.external_id.trim().to_owned();
    if input.external_id.is_empty() || input.external_id.len() > 255 {
        return Err(ApiError::Validation(
            "external_id must contain 1-255 characters".into(),
        ));
    }
    validate_metadata(&input.metadata)?;
    let (user, created) = s
        .repo
        .create_core_user(input, &crate::signing::generate_user_signing_keys())
        .await?;
    if created {
        publish_event(&s, UserEventKind::Created { user: user.clone() }).await;
    }
    Ok((
        if created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(CreatedUser { user, created }),
    ))
}
#[utoipa::path(get, path = "/v1/users/{id}", params(("id" = Uuid, Path)), responses((status = 200, body = CoreUserSchema)), security(("OperatorBearer" = [])), tag = "Operators")]
async fn get_user(
    State(s): State<ApiState>,
    Path(id): Path<Uuid>,
) -> Result<Json<CoreUser>, ApiError> {
    Ok(Json(s.repo.core_user(id).await?))
}
#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct ListQuery {
    /// Clamped to 1–200.
    #[param(default = 50)]
    limit: Option<u32>,
    cursor: Option<String>,
}
#[derive(Serialize, ToSchema)]
struct UserPage {
    #[schema(value_type = Vec<CoreUserSchema>)]
    data: Vec<CoreUser>,
    #[schema(required = true)]
    next_cursor: Option<String>,
}
#[utoipa::path(get, path = "/v1/users", params(ListQuery), responses((status = 200, body = UserPage)), security(("OperatorBearer" = [])), tag = "Operators")]
async fn list_users(
    State(s): State<ApiState>,
    Query(q): Query<ListQuery>,
) -> Result<Json<UserPage>, ApiError> {
    let limit = q.limit.unwrap_or(50).clamp(1, 200);
    let after = q.cursor.as_deref().map(parse_cursor).transpose()?;
    let data = s.repo.list_core_users(limit, after).await?;
    let next_cursor = if data.len() as u32 == limit {
        data.last().map(encode_cursor)
    } else {
        None
    };
    Ok(Json(UserPage { data, next_cursor }))
}

/// Opaque keyset cursor over the listing order `(created_at, id)`. Timestamps
/// round-trip losslessly because PostgreSQL stores microsecond precision.
fn encode_cursor(user: &CoreUser) -> String {
    format!("{}.{}", user.created_at.timestamp_micros(), user.id)
}
fn parse_cursor(value: &str) -> Result<(DateTime<Utc>, Uuid), ApiError> {
    let invalid = || ApiError::Validation("cursor is not a valid pagination token".into());
    let (micros, id) = value.split_once('.').ok_or_else(invalid)?;
    let created_at = micros
        .parse()
        .ok()
        .and_then(DateTime::from_timestamp_micros)
        .ok_or_else(invalid)?;
    Ok((created_at, id.parse().map_err(|_| invalid())?))
}
#[derive(Deserialize, ToSchema)]
struct StatusRequest {
    #[schema(value_type = UserStatusSchema)]
    status: UserStatus,
}
#[utoipa::path(put, path = "/v1/users/{id}/status", params(("id" = Uuid, Path)), request_body = StatusRequest, responses((status = 200, body = CoreUserSchema)), security(("OperatorBearer" = [])), tag = "Operators")]
async fn update_status(
    State(s): State<ApiState>,
    Path(id): Path<Uuid>,
    Json(req): Json<StatusRequest>,
) -> Result<Json<CoreUser>, ApiError> {
    let (user, changed) = s.repo.update_user_status(id, req.status).await?;
    // A no-op request (already at the target status) publishes nothing.
    if changed {
        publish_event(
            &s,
            UserEventKind::StatusChanged {
                user_id: user.id,
                status: user.status,
            },
        )
        .await;
    }
    Ok(Json(user))
}
#[derive(Deserialize, ToSchema)]
struct AccountRequest {
    account: String,
}
#[utoipa::path(put, path = "/v1/users/{id}/accounts/{source}", params(("id" = Uuid, Path), ("source" = crate::openapi::SourceSchema, Path)), request_body = AccountRequest, responses((status = 200, body = CoreUserSchema)), security(("OperatorBearer" = [])), tag = "Operators")]
async fn replace_account(
    State(s): State<ApiState>,
    Path((id, source)): Path<(Uuid, Source)>,
    Json(req): Json<AccountRequest>,
) -> Result<Json<CoreUser>, ApiError> {
    let account = normalize_account(source, &req.account)?;
    let user = s
        .repo
        .replace_user_account(id, source, account.clone())
        .await?;
    // Published unconditionally: the consumer action (resubscribe + poll) is
    // idempotent, so a replayed association costs nothing.
    publish_event(
        &s,
        UserEventKind::AccountAssociated {
            user_id: user.id,
            source,
            account,
        },
    )
    .await;
    Ok(Json(user))
}

/// Per-source account validation; new sources add an arm with their own rules.
fn normalize_account(source: Source, value: &str) -> Result<String, ApiError> {
    match source {
        Source::Ethereum | Source::Hyperliquid => normalize_evm_address(value),
    }
}

fn normalize_evm_address(value: &str) -> Result<String, ApiError> {
    let value = value.trim();
    if value.len() != 42
        || !value.starts_with("0x")
        || !value[2..].bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(ApiError::Validation(
            "account must be a 20-byte 0x-prefixed hexadecimal address".into(),
        ));
    }
    Ok(value.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn normalizes_address() {
        assert_eq!(
            normalize_evm_address(" 0xAABBccDDeeFF0011223344556677889900AAbbCC ").unwrap(),
            "0xaabbccddeeff0011223344556677889900aabbcc"
        );
    }
    #[test]
    fn cursor_round_trips() {
        let user = CoreUser {
            id: Uuid::new_v4(),
            external_id: "tester".into(),
            status: UserStatus::Pending,
            beta_access: false,
            accounts: Default::default(),
            metadata: json!({}),
            created_at: DateTime::from_timestamp_micros(1_722_945_600_123_456).unwrap(),
            updated_at: DateTime::from_timestamp_micros(1_722_945_600_123_456).unwrap(),
        };
        assert_eq!(
            parse_cursor(&encode_cursor(&user)).unwrap(),
            (user.created_at, user.id)
        );
        for cursor in ["", "not-a-cursor", "123", "abc.def", "123.not-a-uuid"] {
            assert!(parse_cursor(cursor).is_err(), "{cursor}");
        }
    }
    #[test]
    fn rejects_malformed_address() {
        for v in [
            "",
            "aabbccddeeff0011223344556677889900aabbcc",
            "0x1234",
            "0xZZbbccddeeff0011223344556677889900aabbcc",
        ] {
            assert!(normalize_evm_address(v).is_err(), "{v}");
        }
    }
}

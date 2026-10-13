//! Operational endpoints and authenticated synchronous risk evaluation.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{StatusCode, header::AUTHORIZATION};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use metrics_exporter_prometheus::PrometheusHandle;
use risk_engine::Error;
use serde::Serialize;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::sync::{mpsc, oneshot};

use super::HostState;
use super::engine::EvaluationMessage;
use super::wire::RiskEvaluationRequest;

const MAX_EVALUATION_BYTES: usize = 1024 * 1024;
const EVALUATION_TIMEOUT: Duration = Duration::from_secs(5);
const METRIC_EVALUATIONS_TOTAL: &str = "risk_http_evaluations_total";
const METRIC_EVALUATION_DURATION_SECONDS: &str = "risk_http_evaluation_duration_seconds";

#[derive(Clone)]
struct HttpState {
    host: Arc<HostState>,
    metrics: PrometheusHandle,
    evaluations: mpsc::Sender<EvaluationMessage>,
}

#[derive(Clone, Copy)]
struct InternalAuth([u8; 32]);

pub fn router(
    host: Arc<HostState>,
    metrics: PrometheusHandle,
    evaluations: mpsc::Sender<EvaluationMessage>,
    internal_token_hash: [u8; 32],
) -> Router {
    let evaluation = Router::new()
        .route("/internal/v1/risk/evaluations", post(evaluate))
        .route_layer(DefaultBodyLimit::max(MAX_EVALUATION_BYTES))
        .route_layer(middleware::from_fn_with_state(
            InternalAuth(internal_token_hash),
            require_internal_auth,
        ));
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(render_metrics))
        .merge(evaluation)
        .with_state(HttpState {
            host,
            metrics,
            evaluations,
        })
}

async fn require_internal_auth(
    State(expected): State<InternalAuth>,
    request: Request,
    next: Next,
) -> Response {
    let mut values = request.headers().get_all(AUTHORIZATION).iter();
    let Some(token) = values
        .next()
        .and_then(|value| value.to_str().ok())
        .and_then(bearer_token)
    else {
        return api_error(StatusCode::UNAUTHORIZED, "UNAUTHORIZED", "unauthorized");
    };
    if values.next().is_some() || token.is_empty() {
        return api_error(StatusCode::UNAUTHORIZED, "UNAUTHORIZED", "unauthorized");
    }
    let received: [u8; 32] = Sha256::digest(token.as_bytes()).into();
    if !bool::from(expected.0.ct_eq(&received)) {
        return api_error(StatusCode::UNAUTHORIZED, "UNAUTHORIZED", "unauthorized");
    }
    next.run(request).await
}

fn bearer_token(value: &str) -> Option<&str> {
    let (scheme, token) = value.split_once(' ')?;
    (scheme.eq_ignore_ascii_case("bearer")
        && !token.is_empty()
        && !token.bytes().any(|byte| byte.is_ascii_whitespace()))
    .then_some(token)
}

/// Liveness stays up during recovery so orchestrators do not kill a replay.
async fn health() -> StatusCode {
    StatusCode::OK
}

/// Readiness requires attached consumers, a healthy processor, and a successful Ledger input read.
async fn ready(State(state): State<HttpState>) -> StatusCode {
    if available(&state) && state.host.ledger.ready.load(Ordering::SeqCst) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn evaluate(
    State(state): State<HttpState>,
    payload: Result<Json<RiskEvaluationRequest>, JsonRejection>,
) -> Response {
    let started = Instant::now();
    let response = evaluate_inner(&state, payload).await;
    metrics::counter!(
        METRIC_EVALUATIONS_TOTAL,
        "status" => response.status().as_u16().to_string()
    )
    .increment(1);
    metrics::histogram!(METRIC_EVALUATION_DURATION_SECONDS).record(started.elapsed().as_secs_f64());
    response
}

async fn evaluate_inner(
    state: &HttpState,
    payload: Result<Json<RiskEvaluationRequest>, JsonRejection>,
) -> Response {
    if !available(state) {
        return api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "ENGINE_UNAVAILABLE",
            "risk engine is not ready",
        );
    }
    let Json(request) = match payload {
        Ok(request) => request,
        Err(error) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "MALFORMED_REQUEST",
                &error.body_text(),
            );
        }
    };
    let request = request.into();
    let (response, receiver) = oneshot::channel();
    match state
        .evaluations
        .try_send(EvaluationMessage { request, response })
    {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            return api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "ENGINE_BUSY",
                "risk engine evaluation queue is full",
            );
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            return api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "ENGINE_UNAVAILABLE",
                "risk engine evaluation queue is closed",
            );
        }
    }
    match tokio::time::timeout(EVALUATION_TIMEOUT, receiver).await {
        Ok(Ok(Ok(evaluation))) => Json(evaluation).into_response(),
        Ok(Ok(Err(error))) => engine_error(&error),
        Ok(Err(_closed)) => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "ENGINE_UNAVAILABLE",
            "risk engine stopped before evaluating the request",
        ),
        Err(_elapsed) => api_error(
            StatusCode::GATEWAY_TIMEOUT,
            "EVALUATION_TIMEOUT",
            "risk evaluation timed out",
        ),
    }
}

fn available(state: &HttpState) -> bool {
    state.host.ready.load(Ordering::SeqCst)
        && !state.host.poisoned.load(Ordering::SeqCst)
        && !state.host.draining.load(Ordering::SeqCst)
}

async fn render_metrics(State(state): State<HttpState>) -> impl IntoResponse {
    state.metrics.render()
}

fn engine_error(error: &Error) -> Response {
    let (status, code) = match error {
        Error::UnknownAccountSubject { .. } => (StatusCode::NOT_FOUND, "ACCOUNT_NOT_FOUND"),
        Error::EvaluationAccountVersionMismatch { .. } => {
            (StatusCode::CONFLICT, "ACCOUNT_VERSION_MISMATCH")
        }
        Error::ProcessorPoisoned => (StatusCode::SERVICE_UNAVAILABLE, "ENGINE_UNAVAILABLE"),
        Error::EvaluationContract { .. } => {
            tracing::error!(%error, "risk evaluation output violated its contract");
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "internal error",
            );
        }
        _ => {
            tracing::warn!(%error, "risk evaluation input rejected");
            return api_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "INVALID_EVALUATION_INPUT",
                "invalid evaluation input",
            );
        }
    };
    api_error(status, code, &error.to_string())
}

#[derive(Serialize)]
struct ErrorEnvelope<'message> {
    error: ErrorBody<'message>,
}

#[derive(Serialize)]
struct ErrorBody<'message> {
    code: &'static str,
    message: &'message str,
}

fn api_error(status: StatusCode, code: &'static str, message: &str) -> Response {
    (
        status,
        Json(ErrorEnvelope {
            error: ErrorBody { code, message },
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use metrics_exporter_prometheus::PrometheusBuilder;
    use risk_engine::processing::ProjectedAccount;
    use serde_json::Value;
    use tackt_primitives::accounts::AccountSubject;
    use tackt_primitives::risk::{
        EvaluationReason, EvaluationVersions, RiskClassification, RiskEvaluation, RiskMeasurements,
    };
    use tower::ServiceExt;
    use uuid::Uuid;

    use super::*;

    const TOKEN: &str = "test-risk-engine-token-at-least-24-bytes";

    fn subject() -> AccountSubject {
        AccountSubject::new(Uuid::from_u128(1)).expect("subject")
    }

    fn request() -> RiskEvaluationRequest {
        RiskEvaluationRequest {
            action_id: Uuid::from_u128(2),
            account: subject(),
            account_version: 5,
            input_version: 6,
            projected_account: ProjectedAccount::default(),
        }
    }

    fn evaluation() -> RiskEvaluation {
        RiskEvaluation::new(
            Uuid::from_u128(2),
            subject(),
            [4; 32],
            RiskClassification::DataIncomplete,
            EvaluationReason::MissingInput,
            RiskMeasurements::new(0, 0, 0, i128::MAX, i128::MAX).expect("measurements"),
            1,
            EvaluationVersions {
                account_version: 5,
                input_version: 6,
                processing_epoch: 0,
                ethereum_price_sequence: None,
                hyperliquid_price_sequence: None,
            },
        )
        .expect("evaluation")
    }

    fn app(ready: bool, evaluations: mpsc::Sender<EvaluationMessage>) -> Router {
        let state = Arc::new(HostState::default());
        state.ready.store(ready, Ordering::SeqCst);
        state.ledger.ready.store(ready, Ordering::SeqCst);
        let metrics = PrometheusBuilder::new().build_recorder().handle();
        router(
            state,
            metrics,
            evaluations,
            Sha256::digest(TOKEN.as_bytes()).into(),
        )
    }

    fn post(body: impl Into<Body>, authenticated: bool) -> Request<Body> {
        let mut request = Request::builder()
            .method("POST")
            .uri("/internal/v1/risk/evaluations")
            .header("content-type", "application/json");
        if authenticated {
            request = request.header("authorization", format!("Bearer {TOKEN}"));
        }
        request.body(body.into()).expect("request")
    }

    async fn json(response: Response) -> Value {
        serde_json::from_slice(
            &to_bytes(response.into_body(), MAX_EVALUATION_BYTES)
                .await
                .expect("response body"),
        )
        .expect("JSON response")
    }

    #[tokio::test]
    async fn readiness_tracks_ledger_refresh_failure_and_recovery() {
        let state = Arc::new(HostState::default());
        state.ready.store(true, Ordering::SeqCst);
        let (sender, _receiver) = mpsc::channel(1);
        let app = router(
            state.clone(),
            PrometheusBuilder::new().build_recorder().handle(),
            sender,
            Sha256::digest(TOKEN.as_bytes()).into(),
        );
        for (loaded, expected) in [
            (false, StatusCode::SERVICE_UNAVAILABLE),
            (true, StatusCode::OK),
            (false, StatusCode::SERVICE_UNAVAILABLE),
            (true, StatusCode::OK),
        ] {
            state.ledger.ready.store(loaded, Ordering::SeqCst);
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri("/ready")
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(response.status(), expected);
        }
    }

    #[tokio::test]
    async fn operational_routes_are_public_and_evaluation_requires_auth() {
        let (sender, _receiver) = mpsc::channel::<EvaluationMessage>(1);
        let response = app(true, sender.clone())
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);

        let response = app(true, sender)
            .oneshot(post(
                serde_json::to_vec(&request()).expect("serialize request"),
                false,
            ))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(json(response).await["error"]["code"], "UNAUTHORIZED");

        let (sender, _receiver) = mpsc::channel::<EvaluationMessage>(1);
        let mut duplicate = post(
            serde_json::to_vec(&request()).expect("serialize request"),
            true,
        );
        duplicate.headers_mut().append(
            AUTHORIZATION,
            format!("Bearer {TOKEN}").parse().expect("header"),
        );
        let response = app(true, sender)
            .oneshot(duplicate)
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn data_incomplete_is_a_successful_typed_evaluation() {
        let (sender, mut receiver) = mpsc::channel::<EvaluationMessage>(1);
        tokio::spawn(async move {
            let message = receiver.recv().await.expect("evaluation request");
            message.response.send(Ok(evaluation())).expect("receiver");
        });
        let response = app(true, sender)
            .oneshot(post(
                serde_json::to_vec(&request()).expect("serialize request"),
                true,
            ))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), MAX_EVALUATION_BYTES)
            .await
            .expect("response body");
        assert!(
            !body
                .windows(b"approved".len())
                .any(|part| part == b"approved")
        );
        let parsed: RiskEvaluation = serde_json::from_slice(&body).expect("body");
        assert_eq!(parsed.classification(), RiskClassification::DataIncomplete);
    }

    #[tokio::test]
    async fn malformed_stale_and_unready_requests_fail_closed() {
        let (sender, mut receiver) = mpsc::channel::<EvaluationMessage>(1);
        let malformed = app(true, sender.clone())
            .oneshot(post("{", true))
            .await
            .expect("response");
        assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
        assert_eq!(json(malformed).await["error"]["code"], "MALFORMED_REQUEST");

        let mut unknown = serde_json::to_value(request()).expect("serialize request");
        unknown["risk_registry"]["future_field"] = serde_json::json!(true);
        let unknown = app(true, sender.clone())
            .oneshot(post(serde_json::to_vec(&unknown).expect("serialize"), true))
            .await
            .expect("response");
        assert_eq!(unknown.status(), StatusCode::BAD_REQUEST);
        assert_eq!(json(unknown).await["error"]["code"], "MALFORMED_REQUEST");

        tokio::spawn(async move {
            let message = receiver.recv().await.expect("evaluation request");
            message
                .response
                .send(Err(Error::EvaluationAccountVersionMismatch {
                    subject: subject(),
                    received: 4,
                    committed: 5,
                }))
                .expect("receiver");
        });
        let stale = app(true, sender.clone())
            .oneshot(post(
                serde_json::to_vec(&request()).expect("serialize request"),
                true,
            ))
            .await
            .expect("response");
        assert_eq!(stale.status(), StatusCode::CONFLICT);
        assert_eq!(
            json(stale).await["error"]["code"],
            "ACCOUNT_VERSION_MISMATCH"
        );

        let unavailable = app(false, sender)
            .oneshot(post(
                serde_json::to_vec(&request()).expect("serialize request"),
                true,
            ))
            .await
            .expect("response");
        assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn saturated_evaluation_queue_fails_fast() {
        let (sender, _receiver) = mpsc::channel::<EvaluationMessage>(1);
        let (response, _receiver) = oneshot::channel();
        sender
            .try_send(EvaluationMessage {
                request: request().into(),
                response,
            })
            .expect("fill queue");

        let response = app(true, sender)
            .oneshot(post(
                serde_json::to_vec(&request()).expect("serialize request"),
                true,
            ))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json(response).await["error"]["code"], "ENGINE_BUSY");
    }

    #[tokio::test]
    async fn validation_errors_do_not_expose_dense_account_indexes() {
        let response = engine_error(&Error::AccountOutOfRange { account: 42 });
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = json(response).await;
        assert_eq!(body["error"]["code"], "INVALID_EVALUATION_INPUT");
        assert_eq!(body["error"]["message"], "invalid evaluation input");
        assert!(!body.to_string().contains("42"));
    }
}

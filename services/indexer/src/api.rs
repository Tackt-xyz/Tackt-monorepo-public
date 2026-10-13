use std::sync::Arc;
use std::time::Duration;

use axum::{Router, extract::State, http::StatusCode, routing::get};
use tackt_nats::NatsClient;
use tackt_telemetry::PrometheusHandle;

/// Bound on each per-stream readiness probe so `/ready` answers within a
/// probe-friendly deadline even when the JetStream API hangs.
const READY_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct ApiState {
    pub nats: Arc<NatsClient>,
    /// Streams whose acknowledged publishes this process cannot run without.
    pub required_streams: Vec<String>,
    pub metrics: PrometheusHandle,
}

pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(render_metrics))
        .with_state(state)
}

async fn health() -> StatusCode {
    StatusCode::OK
}

/// Ready only while the acknowledged JetStream publish path is available: the
/// managed connection is established AND the JetStream API answers for every
/// required stream. The publish is the Indexer's only durable side effect —
/// for observations and price evidence alike — so a process that cannot
/// complete it must not receive traffic, even when the core connection is
/// still up (JetStream down, a stream deleted).
///
/// Per-chain sync health is deliberately excluded: a chain's RPC outage leaves
/// the publish path intact, and 503 would only restart-loop the process. It is
/// exposed as the `indexer_evm_*{chain_id}` gauges on `/metrics` instead.
async fn ready(State(state): State<ApiState>) -> StatusCode {
    if !state.nats.is_connected() {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    for stream in &state.required_streams {
        match tokio::time::timeout(READY_PROBE_TIMEOUT, state.nats.stream_available(stream)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(%error, stream, "readiness probe failed");
                return StatusCode::SERVICE_UNAVAILABLE;
            }
            Err(_) => {
                tracing::warn!(stream, "readiness probe timed out");
                return StatusCode::SERVICE_UNAVAILABLE;
            }
        }
    }
    StatusCode::OK
}

async fn render_metrics(State(state): State<ApiState>) -> String {
    state.metrics.render()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    async fn get_status(app: Router, path: &str) -> StatusCode {
        app.oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn ready_fails_closed_while_nats_is_disconnected() {
        // The managed connection retries in the background, so initialise
        // succeeds against an unreachable server with a disconnected client.
        let nats = Arc::new(
            tackt_nats::initialise("nats://127.0.0.1:1")
                .await
                .expect("managed client"),
        );
        let state = ApiState {
            nats,
            required_streams: vec![tackt_nats::subjects::OBSERVATIONS_STREAM.to_string()],
            metrics: tackt_telemetry::initialise_metrics(),
        };
        assert_eq!(
            get_status(router(state.clone()), "/health").await,
            StatusCode::OK,
            "liveness is independent of dependencies"
        );
        assert_eq!(
            get_status(router(state.clone()), "/ready").await,
            StatusCode::SERVICE_UNAVAILABLE
        );

        metrics::counter!("indexer_api_test_total").increment(1);
        let response = router(state)
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        assert!(
            String::from_utf8(body.to_vec())
                .unwrap()
                .contains("indexer_api_test_total"),
            "recorded metrics must appear in the rendered exposition"
        );
    }
}

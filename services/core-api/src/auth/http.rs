use axum::{
    extract::{ConnectInfo, MatchedPath, Request, State},
    http::{HeaderValue, Method, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::{net::SocketAddr, time::Instant};
use tower_http::cors::CorsLayer;
use tracing::Instrument;

use super::AuthError;

tokio::task_local! { static REQUEST_ID: HeaderValue; }

pub(crate) fn request_id() -> Option<HeaderValue> {
    REQUEST_ID.try_with(Clone::clone).ok()
}

pub(crate) fn cors(origins: Vec<HeaderValue>) -> CorsLayer {
    CorsLayer::new()
        .allow_origin(origins)
        .allow_credentials(true)
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([
            header::AUTHORIZATION,
            header::CONTENT_TYPE,
            "x-request-id".parse().unwrap(),
            "x-idempotency-key".parse().unwrap(),
            "x-user-id".parse().unwrap(),
        ])
        .expose_headers(["x-request-id".parse().unwrap()])
        .max_age(std::time::Duration::from_secs(600))
}

pub(crate) async fn protect_origin(
    State(origins): State<Vec<HeaderValue>>,
    request: Request,
    next: Next,
) -> Response {
    if !request.method().is_safe() && request.uri().path().starts_with("/api/") {
        let mut supplied = request.headers().get_all(header::ORIGIN).iter();
        let origin = supplied.next();
        // Cookie writes require an explicit trusted origin. Also reject foreign
        // origins before processing anonymous sign-in requests (login CSRF).
        let invalid = supplied.next().is_some()
            || origin.is_some_and(|origin| !origins.contains(origin))
            || (origin.is_none() && request.headers().contains_key(header::COOKIE));
        if invalid {
            return AuthError::OriginRejected.into_response();
        }
    }
    next.run(request).await
}

pub(crate) async fn observe(mut request: Request, next: Next) -> Response {
    let multiple_ids = request.headers().get_all("x-request-id").iter().count() != 1;
    let request_id = request
        .headers()
        .get("x-request-id")
        .filter(|value| {
            !multiple_ids
                && !value.is_empty()
                && value.len() <= 128
                && value
                    .as_bytes()
                    .iter()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(b))
        })
        .cloned()
        .unwrap_or_else(|| HeaderValue::from_str(&uuid::Uuid::new_v4().to_string()).unwrap());
    request
        .headers_mut()
        .insert("x-request-id", request_id.clone());
    let user_route = request.uri().path().starts_with("/api/");
    let endpoint = request
        .extensions()
        .get::<MatchedPath>()
        .map_or("unmatched", MatchedPath::as_str)
        .to_owned();
    let ip = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|peer| peer.0.ip().to_string())
        .unwrap_or_else(|| "unknown".into());
    let span = tracing::info_span!("http_request", trace_id = request_id.to_str().unwrap_or_default(),
        %ip, method = %request.method(), %endpoint, user_id = tracing::field::Empty);
    REQUEST_ID.scope(request_id.clone(), async move {
        let start = Instant::now();
        let mut response = next.run(request).await;
        let status = response.status().as_u16().to_string();
        metrics::counter!("http_requests_total", "endpoint" => endpoint.clone(), "status" => status.clone()).increment(1);
        metrics::histogram!("http_request_duration_seconds", "endpoint" => endpoint).record(start.elapsed().as_secs_f64());
        tracing::info!(%status, "request completed");
        let headers = response.headers_mut();
        headers.insert("x-request-id", request_id);
        if user_route { headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store")); }
        for (name, value) in [
            ("x-content-type-options", "nosniff"),
            ("x-frame-options", "DENY"),
            ("content-security-policy", "default-src 'self'"),
            ("strict-transport-security", "max-age=31536000"),
        ] {
            headers.insert(name, HeaderValue::from_static(value));
        }
        response
    }.instrument(span)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json, Router,
        body::{Body, to_bytes},
        middleware,
        routing::post,
    };
    use serde::Deserialize;
    use serde_json::{Value, json};
    use tower::ServiceExt;

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Input {
        value: String,
    }

    fn app() -> Router {
        let origins = vec![HeaderValue::from_static("https://app.example")];
        Router::new()
            .route(
                "/api/test",
                post(|Json(input): Json<Input>| async move { Json(json!({"value":input.value})) }),
            )
            .layer(middleware::from_fn(crate::api::json_error_envelope))
            .layer(middleware::from_fn_with_state(
                origins.clone(),
                protect_origin,
            ))
            .layer(cors(origins))
            .layer(middleware::from_fn(observe))
    }

    #[tokio::test]
    async fn errors_keep_security_request_ids_and_curated_messages() {
        for body in [r#"{"value":"secret-value","extra":true}"#, r#"{"value": "#] {
            let response = app()
                .oneshot(
                    Request::post("/api/test")
                        .header("content-type", "application/json")
                        .header("x-request-id", "request-42")
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), 400);
            assert_eq!(response.headers()["x-request-id"], "request-42");
            assert_eq!(response.headers()["x-frame-options"], "DENY");
            assert_eq!(response.headers()["cache-control"], "no-store");
            let bytes = to_bytes(response.into_body(), 4096).await.unwrap();
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body["error"]["sanitized"], true);
            assert_eq!(body["error"]["code"], "VALIDATION_ERROR");
            assert!(!String::from_utf8_lossy(&bytes).contains("secret-value"));
        }
        let response = app()
            .oneshot(Request::get("/missing").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), 404);
        assert!(
            response.headers()["x-request-id"]
                .to_str()
                .unwrap()
                .parse::<uuid::Uuid>()
                .is_ok()
        );
    }

    #[tokio::test]
    async fn cors_and_origin_checks_reject_untrusted_cookie_writes() {
        for origin in [None, Some("https://evil.example"), Some("null")] {
            let mut request = Request::post("/api/test")
                .header("cookie", "__Secure-tackt-access=credential")
                .header("content-type", "application/json");
            if let Some(origin) = origin {
                request = request.header("origin", origin);
            }
            let response = app()
                .oneshot(request.body(Body::from(r#"{"value":"ok"}"#)).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), 403);
            assert!(response.headers().contains_key("x-request-id"));
        }
        let response = app()
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/api/test")
                    .header("origin", "https://app.example")
                    .header("access-control-request-method", "POST")
                    .header("access-control-request-headers", "content-type,x-user-id")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.headers()["access-control-allow-origin"],
            "https://app.example"
        );
        assert_eq!(
            response.headers()["access-control-allow-credentials"],
            "true"
        );
        assert!(response.headers().contains_key("x-request-id"));
    }
}

use axum::{
    Json,
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde_json::json;
use std::sync::Arc;

#[derive(Clone)]
pub struct OperatorAuth {
    bearer_token: Arc<str>,
}

impl OperatorAuth {
    pub fn new(bearer_token: String) -> Self {
        Self {
            bearer_token: bearer_token.into(),
        }
    }
}

/// Authenticated operator context installed by [`require_operator`].
/// Future authentication mechanisms can populate this type without changing handlers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperatorIdentity {
    pub actor_id: String,
}

pub async fn require_operator(
    State(auth): State<OperatorAuth>,
    mut request: Request,
    next: Next,
) -> Result<Response, AuthError> {
    let identity = authenticate(request.headers(), &auth.bearer_token)?;
    request.extensions_mut().insert(identity);
    Ok(next.run(request).await)
}

/// Client-supplied operator names longer than this fall back to the default
/// identity rather than reaching logs and future audit records unbounded.
const MAX_ACTOR_ID_LENGTH: usize = 128;

fn authenticate(headers: &HeaderMap, operator_token: &str) -> Result<OperatorIdentity, AuthError> {
    use subtle::ConstantTimeEq;
    let presented = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    let authentic =
        presented.is_some_and(|token| token.as_bytes().ct_eq(operator_token.as_bytes()).into());
    if !authentic {
        return Err(AuthError);
    }
    Ok(OperatorIdentity {
        actor_id: headers
            .get("x-operator-id")
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| {
                !value.is_empty()
                    && value.len() <= MAX_ACTOR_ID_LENGTH
                    && !value.chars().any(char::is_control)
            })
            .unwrap_or("operator")
            .to_owned(),
    })
}

#[derive(Debug)]
pub struct AuthError;

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": {
                "code": "UNAUTHORIZED",
                "message": "Valid operator credentials are required.",
                "sanitized": true
            }})),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        body::Body,
        extract::Extension,
        http::{HeaderValue, Request},
        middleware,
        routing::get,
    };
    use tower::ServiceExt;

    #[test]
    fn requires_the_operator_bearer_token() {
        let mut headers = HeaderMap::new();
        assert!(authenticate(&headers, "expected-token").is_err());
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer wrong-token"),
        );
        assert!(authenticate(&headers, "expected-token").is_err());
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer expected-token"),
        );
        headers.insert("x-operator-id", HeaderValue::from_static("operator-42"));
        assert_eq!(
            authenticate(&headers, "expected-token").unwrap(),
            OperatorIdentity {
                actor_id: "operator-42".into()
            }
        );
    }

    #[tokio::test]
    async fn middleware_rejects_unauthenticated_requests_and_installs_identity() {
        let app =
            Router::new()
                .route(
                    "/protected",
                    get(
                        |Extension(operator): Extension<OperatorIdentity>| async move {
                            operator.actor_id
                        },
                    ),
                )
                .route_layer(middleware::from_fn_with_state(
                    OperatorAuth::new("expected-token".into()),
                    require_operator,
                ));
        let unauthenticated = app
            .clone()
            .oneshot(Request::get("/protected").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
        let authenticated = app
            .oneshot(
                Request::get("/protected")
                    .header("authorization", "Bearer expected-token")
                    .header("x-operator-id", "operator-42")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(authenticated.status(), StatusCode::OK);
        assert_eq!(
            axum::body::to_bytes(authenticated.into_body(), 128)
                .await
                .unwrap(),
            "operator-42"
        );
    }
}

pub(crate) mod cookies;
mod evm;
pub(crate) mod http;
mod operator;
mod provider;
pub(crate) mod routes;
mod solana;
mod tokens;

pub use operator::{OperatorAuth, OperatorIdentity, require_operator};
pub use routes::router;

use axum::{
    Json,
    extract::FromRequestParts,
    http::{HeaderMap, StatusCode, header::WWW_AUTHENTICATE, request::Parts},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc};
use tackt_postgres::UserRepositoryError;
use tackt_primitives::{CoreUser, UserStatus};
use uuid::Uuid;

use crate::api::ApiState;
use provider::Provider;
use tokens::Tokens;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConfig {
    pub name: String,
    pub issuer: String,
    pub client_id: String,
}

// Secrets deliberately have no Debug implementation.
#[derive(Clone)]
pub struct UserAuthConfig {
    pub issuer: String,
    pub jwt_secret: Vec<u8>,
    pub providers: Vec<ProviderConfig>,
    pub frontend_uri: String,
}

impl UserAuthConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        validate_url(&reqwest::Url::parse(&self.issuer)?)?;
        anyhow::ensure!(
            self.jwt_secret.len() >= 32,
            "auth signing secret must contain at least 32 bytes"
        );
        anyhow::ensure!(
            self.providers.len() <= 8,
            "configure at most eight OIDC providers"
        );
        solana::SolanaAuth::new(&self.frontend_uri)?;
        evm::EvmAuth::new(&self.frontend_uri)?;
        let mut names = std::collections::HashSet::new();
        for provider in &self.providers {
            anyhow::ensure!(
                !matches!(provider.name.as_str(), "solana" | "evm")
                    && !provider.name.is_empty()
                    && provider.name.len() <= 32
                    && provider
                        .name
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
                "invalid login provider name"
            );
            anyhow::ensure!(
                names.insert(&provider.name),
                "duplicate login provider name"
            );
            anyhow::ensure!(
                !provider.client_id.trim().is_empty(),
                "provider client ID is required"
            );
            let issuer = reqwest::Url::parse(&provider.issuer)?;
            validate_url(&issuer)?;
            anyhow::ensure!(
                issuer.query().is_none() && provider.issuer.len() <= 2048,
                "invalid provider issuer"
            );
        }
        Ok(())
    }
}

pub(crate) fn is_loopback_http(url: &reqwest::Url) -> bool {
    url.scheme() == "http"
        && url.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        })
}

fn validate_url(url: &reqwest::Url) -> anyhow::Result<()> {
    anyhow::ensure!(
        url.scheme() == "https" || is_loopback_http(url),
        "auth URLs require HTTPS (loopback HTTP is allowed for development)"
    );
    anyhow::ensure!(
        url.username().is_empty() && url.password().is_none() && url.fragment().is_none(),
        "auth URL must not contain credentials or a fragment"
    );
    Ok(())
}

#[derive(Clone)]
pub struct UserAuth(Arc<AuthState>);

struct AuthState {
    providers: BTreeMap<String, Provider>,
    tokens: Tokens,
    solana: solana::SolanaAuth,
    evm: evm::EvmAuth,
    origins: Vec<axum::http::HeaderValue>,
}

impl UserAuth {
    pub async fn initialise(config: UserAuthConfig) -> anyhow::Result<Self> {
        config.validate()?;
        let mut origins = vec![
            reqwest::Url::parse(&config.issuer)?
                .origin()
                .ascii_serialization(),
        ];
        origins.push(
            reqwest::Url::parse(&config.frontend_uri)?
                .origin()
                .ascii_serialization(),
        );
        origins.sort();
        origins.dedup();
        let origins = origins
            .into_iter()
            .map(|origin| origin.parse())
            .collect::<Result<Vec<_>, _>>()?;
        let solana = solana::SolanaAuth::new(&config.frontend_uri)?;
        let evm = evm::EvmAuth::new(&config.frontend_uri)?;
        let mut providers = BTreeMap::new();
        for provider in config.providers {
            let name = provider.name.clone();
            let provider = Provider::initialise(provider)
                .await
                .map_err(|_| anyhow::anyhow!("failed to initialise OIDC provider {name}"))?;
            providers.insert(name, provider);
        }
        Ok(Self(Arc::new(AuthState {
            providers,
            solana,
            evm,
            origins,
            tokens: Tokens::new(config.issuer, &config.jwt_secret),
        })))
    }

    pub(crate) fn origins(&self) -> Vec<axum::http::HeaderValue> {
        self.0.origins.clone()
    }

    fn provider(&self, name: &str) -> Result<&Provider, AuthError> {
        self.0.providers.get(name).ok_or(AuthError::UnknownProvider)
    }
}

pub(super) struct VerifiedIdentity {
    pub issuer: String,
    pub subject: String,
}

/// Identity verified from a Core access JWT, without database access.
/// Handlers own account-status checks; session revocation is enforced on refresh.
pub struct AuthenticatedUser {
    pub user_id: Uuid,
    pub session_id: Uuid,
}

impl FromRequestParts<ApiState> for AuthenticatedUser {
    type Rejection = AuthError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &ApiState,
    ) -> Result<Self, Self::Rejection> {
        authenticate(&parts.headers, &state.user_auth)
    }
}

fn authenticate(headers: &HeaderMap, auth: &UserAuth) -> Result<AuthenticatedUser, AuthError> {
    let cookie = cookies::get(headers, cookies::ACCESS_COOKIE)?;
    let claims = if headers.contains_key(axum::http::header::AUTHORIZATION) {
        if cookie.is_some() {
            return Err(AuthError::Unauthorized);
        }
        let claims = auth.0.tokens.verify_access(bearer_token(headers)?)?;
        let mut ids = headers.get_all("x-user-id").iter();
        let id = ids
            .next()
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<Uuid>().ok());
        if ids.next().is_some() || id != Some(claims.sub) {
            return Err(AuthError::UserMismatch);
        }
        claims
    } else {
        if headers.contains_key("x-user-id") {
            return Err(AuthError::UserMismatch);
        }
        auth.0
            .tokens
            .verify_access(cookie.as_deref().ok_or(AuthError::Unauthorized)?)?
    };
    tracing::Span::current().record("user_id", tracing::field::display(claims.sub));
    Ok(AuthenticatedUser {
        user_id: claims.sub,
        session_id: claims.sid,
    })
}

pub(crate) fn require_open_user(user: &CoreUser) -> Result<(), AuthError> {
    if user.status == UserStatus::Closed {
        return Err(AuthError::Closed);
    }
    Ok(())
}

fn bearer_token(headers: &HeaderMap) -> Result<&str, AuthError> {
    let mut values = headers.get_all(axum::http::header::AUTHORIZATION).iter();
    let value = values.next().ok_or(AuthError::Unauthorized)?;
    if values.next().is_some() {
        return Err(AuthError::Unauthorized);
    }
    let value = value.to_str().map_err(|_| AuthError::Unauthorized)?;
    let (scheme, token) = value.split_once(' ').ok_or(AuthError::Unauthorized)?;
    if !scheme.eq_ignore_ascii_case("bearer")
        || token.is_empty()
        || token.len() > 16 * 1024
        || token.bytes().any(|b| b.is_ascii_whitespace())
    {
        return Err(AuthError::Unauthorized);
    }
    Ok(token)
}

#[derive(Debug, thiserror::Error, strum::AsRefStr)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum AuthError {
    #[error("The invite code is invalid.")]
    InvalidInviteCode,
    #[error("Beta invite redemption is currently unavailable.")]
    BetaAccessUnavailable,
    #[error("Your sign-in credentials are invalid or expired. Please sign in again.")]
    Unauthorized,
    #[error("This sign-in method is not available.")]
    UnknownProvider,
    #[error("The requested user does not match the authenticated user.")]
    UserMismatch,
    #[error("This request did not come from an allowed application.")]
    OriginRejected,
    #[strum(serialize = "INVALID_AUTH_FLOW")]
    #[error("This sign-in attempt does not match the requested action. Please start again.")]
    InvalidFlow,
    #[error("This sign-in challenge has already been used or expired. Request a new challenge.")]
    ChallengeUsed,
    #[error("Your session has expired. Please sign in again.")]
    SessionExpired,
    #[strum(serialize = "IDENTITY_NOT_REGISTERED")]
    #[error("This sign-in method is not registered. Contact support to request access.")]
    NotRegistered,
    #[error("This sign-in method already belongs to another account.")]
    IdentityConflict,
    #[strum(serialize = "USER_CLOSED")]
    #[error("This account is closed.")]
    Closed,
    #[strum(serialize = "AUTHENTICATION_UNAVAILABLE")]
    #[error("Sign-in is temporarily unavailable. Please try again shortly.")]
    Unavailable,
    #[strum(serialize = "VALIDATION_ERROR")]
    #[error("The authentication request is invalid. Check the supplied fields.")]
    InvalidRequest,
    #[strum(serialize = "VALIDATION_ERROR")]
    #[error("The authentication request is invalid. Check the supplied fields.")]
    InvalidField(&'static str),
    #[strum(serialize = "INTERNAL_ERROR")]
    #[error("Something went wrong. Please try again shortly.")]
    Internal,
}

impl From<UserRepositoryError> for AuthError {
    fn from(error: UserRepositoryError) -> Self {
        match error {
            UserRepositoryError::NotFound => Self::NotRegistered,
            error if error.is_unavailable() => {
                tracing::error!("authentication database unavailable");
                Self::Unavailable
            }
            UserRepositoryError::IdentityConflict => Self::IdentityConflict,
            UserRepositoryError::InvalidIdentity => Self::InvalidRequest,
            _ => {
                tracing::error!("auth persistence operation failed");
                Self::Internal
            }
        }
    }
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let field = match &self {
            Self::InvalidField(field) => Some(*field),
            Self::InvalidRequest => Some("body"),
            _ => None,
        };
        let status = match self {
            Self::Unauthorized | Self::ChallengeUsed | Self::SessionExpired => {
                StatusCode::UNAUTHORIZED
            }
            Self::UserMismatch
            | Self::OriginRejected
            | Self::NotRegistered
            | Self::Closed
            | Self::InvalidInviteCode => StatusCode::FORBIDDEN,
            Self::UnknownProvider => StatusCode::NOT_FOUND,
            Self::InvalidFlow | Self::InvalidRequest | Self::InvalidField(_) => {
                StatusCode::BAD_REQUEST
            }
            Self::IdentityConflict => StatusCode::CONFLICT,
            Self::Unavailable | Self::BetaAccessUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let code = self.as_ref();
        if status.is_server_error() {
            tracing::error!(code, success = false, "authentication service failed");
        } else {
            tracing::warn!(code, success = false, "authentication failed");
        }
        let mut body =
            json!({"error": {"code": code, "message": self.to_string(), "sanitized": true}});
        if let Some(field) = field {
            body["error"]["details"] = json!([{ "field": field, "message": "Invalid value." }]);
        }
        let mut response = (status, Json(body)).into_response();
        if status == StatusCode::UNAUTHORIZED {
            response.headers_mut().insert(
                WWW_AUTHENTICATE,
                axum::http::HeaderValue::from_static("Bearer realm=\"tackt-user-api\""),
            );
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderValue, header::AUTHORIZATION};

    #[tokio::test]
    async fn access_authentication_verifies_identity_without_database_state() {
        let secret = uuid::Uuid::new_v4().to_string();
        let auth = UserAuth::initialise(UserAuthConfig {
            issuer: "https://api.example".into(),
            jwt_secret: secret.as_bytes().to_vec(),
            providers: vec![],
            frontend_uri: "https://app.example".into(),
        })
        .await
        .unwrap();
        // Neither ID has a stored user or session; authentication needs only the JWT.
        let user_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let expiry = chrono::Utc::now().timestamp() + tokens::ACCESS_SECONDS;
        let token = auth.0.tokens.access(user_id, session_id, expiry).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::COOKIE,
            format!("{}={token}", cookies::ACCESS_COOKIE)
                .parse()
                .unwrap(),
        );
        let identity = authenticate(&headers, &auth).unwrap();
        assert_eq!(identity.user_id, user_id);
        assert_eq!(identity.session_id, session_id);

        headers.clear();
        headers.insert("x-user-id", user_id.to_string().parse().unwrap());
        headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());
        let identity = authenticate(&headers, &auth).unwrap();
        assert_eq!(identity.user_id, user_id);
        assert_eq!(identity.session_id, session_id);

        let foreign_secret = uuid::Uuid::new_v4().to_string();
        let foreign_tokens = Tokens::new("https://api.example".into(), foreign_secret.as_bytes());
        let forged = foreign_tokens.access(user_id, session_id, expiry).unwrap();
        headers.insert(AUTHORIZATION, format!("Bearer {forged}").parse().unwrap());
        assert!(matches!(
            authenticate(&headers, &auth),
            Err(AuthError::Unauthorized)
        ));

        let expired = jsonwebtoken::encode(
            &jsonwebtoken::Header {
                typ: Some("at+jwt".into()),
                ..jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256)
            },
            &tokens::AccessClaims {
                sub: user_id,
                sid: session_id,
                iss: "https://api.example".into(),
                aud: "tackt-user-api".into(),
                iat: chrono::Utc::now().timestamp() - tokens::ACCESS_SECONDS - 60,
                exp: chrono::Utc::now().timestamp() - 60,
            },
            &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap();
        headers.insert(AUTHORIZATION, format!("Bearer {expired}").parse().unwrap());
        assert!(matches!(
            authenticate(&headers, &auth),
            Err(AuthError::Unauthorized)
        ));
    }

    #[test]
    fn rejects_missing_duplicate_and_malformed_bearer_headers() {
        let mut headers = HeaderMap::new();
        assert!(bearer_token(&headers).is_err());
        headers.insert(AUTHORIZATION, HeaderValue::from_static("bearer token"));
        assert_eq!(bearer_token(&headers).unwrap(), "token");
        headers.append(AUTHORIZATION, HeaderValue::from_static("Bearer other"));
        assert!(bearer_token(&headers).is_err());
        for value in ["Basic token", "Bearer ", "Bearer two tokens"] {
            headers.clear();
            headers.insert(AUTHORIZATION, HeaderValue::from_str(value).unwrap());
            assert!(bearer_token(&headers).is_err());
        }
    }

    #[test]
    fn validates_provider_configuration_and_signing_secret() {
        let mut config = UserAuthConfig {
            issuer: "https://api.example".into(),
            jwt_secret: vec![1; 32],
            frontend_uri: "https://app.example".into(),
            providers: vec![ProviderConfig {
                name: "google".into(),
                issuer: "https://accounts.google.com".into(),
                client_id: "client".into(),
            }],
        };
        assert!(config.validate().is_ok());
        config.jwt_secret.pop();
        assert!(config.validate().is_err());
        config.jwt_secret.push(1);
        config.providers.push(config.providers[0].clone());
        assert!(config.validate().is_err());
        config.providers.pop();
        config.providers[0].issuer = "http://remote.example".into();
        assert!(config.validate().is_err());
        config.providers[0].issuer = "https://user:password@remote.example".into();
        assert!(config.validate().is_err());
    }
    #[tokio::test]
    async fn both_wallet_methods_are_available_without_oidc_providers() {
        let auth = UserAuth::initialise(UserAuthConfig {
            issuer: "https://api.example".into(),
            jwt_secret: vec![1; 32],
            providers: vec![],
            frontend_uri: "https://app.example".into(),
        })
        .await
        .unwrap();
        assert!(auth.0.providers.is_empty());
        assert!(
            auth.origins()
                .iter()
                .any(|origin| origin == "https://app.example")
        );
    }

    #[test]
    fn wallet_configuration_requires_a_trusted_uri_and_reserves_both_names() {
        let mut config = UserAuthConfig {
            issuer: "https://api.example".into(),
            jwt_secret: vec![1; 32],
            providers: vec![],
            frontend_uri: "https://app.example".into(),
        };
        for uri in [
            "",
            "http://remote.example",
            "https://app.example/?bad=query",
        ] {
            config.frontend_uri = uri.into();
            assert!(config.validate().is_err());
        }
        config.frontend_uri = "https://app.example".into();
        for name in ["solana", "evm"] {
            config.providers = vec![ProviderConfig {
                name: name.into(),
                issuer: "https://provider.example".into(),
                client_id: "client".into(),
            }];
            assert!(config.validate().is_err());
        }
    }
}

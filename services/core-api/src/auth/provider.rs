use axum::body::{Body, to_bytes};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::Utc;
use jsonwebtoken::{
    Algorithm, DecodingKey, Validation, decode, decode_header,
    jwk::{JwkSet, KeyAlgorithm, KeyOperations, PublicKeyUse},
};
use serde::{Deserialize, de::DeserializeOwned};
use subtle::ConstantTimeEq;
use tokio::sync::Mutex;

use super::{AuthError, ProviderConfig, VerifiedIdentity, tokens::IdTokenChallenge};

struct ProviderKeys {
    keys: JwkSet,
    fetched_at: Instant,
}

pub struct Provider {
    pub config: ProviderConfig,
    http: reqwest::Client,
    cached: Mutex<Arc<ProviderKeys>>,
}

impl Provider {
    pub async fn initialise(config: ProviderConfig) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let cached = discover(&config, &http).await?;
        Ok(Self {
            config,
            http,
            cached: Mutex::new(Arc::new(cached)),
        })
    }

    async fn configuration(&self) -> Result<Arc<ProviderKeys>, AuthError> {
        let mut cached = self.cached.lock().await;
        if cached.fetched_at.elapsed() >= Duration::from_secs(300) {
            // Refresh discovery and signing keys together, failing closed if
            // the provider is unavailable after this cache expires.
            *cached = Arc::new(
                discover(&self.config, &self.http)
                    .await
                    .map_err(|_| AuthError::Unavailable)?,
            );
        }
        Ok(Arc::clone(&cached))
    }

    pub async fn verify(
        &self,
        token: &str,
        challenge: &IdTokenChallenge,
    ) -> Result<VerifiedIdentity, AuthError> {
        let configuration = self.configuration().await?;
        self.verify_id_token(token, &challenge.nonce.to_string(), &configuration.keys)
    }

    fn verify_id_token(
        &self,
        token: &str,
        nonce: &str,
        keys: &JwkSet,
    ) -> Result<VerifiedIdentity, AuthError> {
        if token.len() > 16 * 1024 {
            return Err(AuthError::Unauthorized);
        }
        let header = decode_header(token).map_err(|_| AuthError::Unauthorized)?;
        if !matches!(header.alg, Algorithm::RS256 | Algorithm::ES256)
            || header.typ.as_deref().is_some_and(|typ| typ != "JWT")
        {
            return Err(AuthError::Unauthorized);
        }
        let kid = header.kid.ok_or(AuthError::Unauthorized)?;
        let jwk = keys
            .keys
            .iter()
            .find(|key| key.common.key_id.as_deref() == Some(&kid))
            .ok_or(AuthError::Unauthorized)?;
        let expected_algorithm = match header.alg {
            Algorithm::RS256 => KeyAlgorithm::RS256,
            _ => KeyAlgorithm::ES256,
        };
        if jwk
            .common
            .key_algorithm
            .is_some_and(|alg| alg != expected_algorithm)
            || jwk
                .common
                .public_key_use
                .as_ref()
                .is_some_and(|usage| usage != &PublicKeyUse::Signature)
            || jwk
                .common
                .key_operations
                .as_ref()
                .is_some_and(|ops| !ops.contains(&KeyOperations::Verify))
        {
            return Err(AuthError::Unauthorized);
        }
        let key = DecodingKey::from_jwk(jwk).map_err(|_| AuthError::Unauthorized)?;
        let mut validation = Validation::new(header.alg);
        if self.config.issuer == "https://accounts.google.com" {
            validation.set_issuer(&["https://accounts.google.com", "accounts.google.com"]);
        } else {
            validation.set_issuer(&[&self.config.issuer]);
        }
        validation.set_audience(&[&self.config.client_id]);
        validation.set_required_spec_claims(&["iss", "sub", "aud", "iat", "exp"]);
        validation.validate_nbf = true;
        validation.leeway = 30;
        let claims = decode::<IdClaims>(token, &key, &validation)
            .map_err(|_| AuthError::Unauthorized)?
            .claims;
        let now = Utc::now().timestamp();
        let multiple_audiences = claims.aud.as_array().is_some_and(|aud| aud.len() > 1);
        if claims.sub.is_empty()
            || claims.sub.len() > 255
            || claims.sub.chars().any(char::is_control)
            || claims.iat > now + 30
            || claims.exp <= claims.iat
            || claims.exp.saturating_sub(claims.iat) > 3600
            || (multiple_audiences && claims.azp.is_none())
            || claims
                .azp
                .as_ref()
                .is_some_and(|azp| azp != &self.config.client_id)
            || !bool::from(claims.nonce.as_bytes().ct_eq(nonce.as_bytes()))
        {
            return Err(AuthError::Unauthorized);
        }
        Ok(VerifiedIdentity {
            issuer: self.config.issuer.clone(),
            subject: claims.sub,
        })
    }
}

#[derive(Deserialize)]
struct IdClaims {
    sub: String,
    aud: serde_json::Value,
    iat: i64,
    exp: i64,
    nonce: String,
    azp: Option<String>,
}

#[derive(Deserialize)]
struct Discovery {
    issuer: String,
    jwks_uri: String,
}

async fn discover(config: &ProviderConfig, http: &reqwest::Client) -> anyhow::Result<ProviderKeys> {
    let metadata: Discovery = fetch_json(
        http,
        &format!(
            "{}/.well-known/openid-configuration",
            config.issuer.trim_end_matches('/')
        ),
    )
    .await?;
    anyhow::ensure!(
        metadata.issuer == config.issuer,
        "OIDC discovery issuer mismatch"
    );
    let remote = !super::is_loopback_http(&reqwest::Url::parse(&config.issuer)?);
    let jwks_uri = reqwest::Url::parse(&metadata.jwks_uri)?;
    super::validate_url(&jwks_uri)?;
    anyhow::ensure!(
        !remote || jwks_uri.scheme() == "https",
        "remote OIDC signing keys require HTTPS"
    );
    let keys: JwkSet = fetch_json(http, metadata.jwks_uri.as_str()).await?;
    anyhow::ensure!(
        !keys.keys.is_empty() && keys.keys.len() <= 64,
        "invalid OIDC key count"
    );
    let mut ids = std::collections::HashSet::new();
    anyhow::ensure!(
        keys.keys
            .iter()
            .filter_map(|key| key.common.key_id.as_ref())
            .all(|kid| ids.insert(kid)),
        "duplicate OIDC key IDs"
    );
    Ok(ProviderKeys {
        keys,
        fetched_at: Instant::now(),
    })
}

async fn fetch_json<T: DeserializeOwned>(http: &reqwest::Client, url: &str) -> anyhow::Result<T> {
    let mut request = http.get(url);
    if let Some(id) = super::http::request_id() {
        request = request.header("x-request-id", id);
    }
    let response = request.send().await?.error_for_status()?;
    Ok(serde_json::from_slice(&bounded_body(response).await?)?)
}

async fn bounded_body(response: reqwest::Response) -> Result<Vec<u8>, std::io::Error> {
    to_bytes(Body::from_stream(response.bytes_stream()), 256 * 1024)
        .await
        .map(|bytes| bytes.to_vec())
        .map_err(std::io::Error::other)
}

#[cfg(test)]
#[path = "../../tests/support/mod.rs"]
mod mock;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    #[tokio::test]
    async fn provider_body_size_limit_is_enforced() {
        for size in [0, 256 * 1024, 256 * 1024 + 1] {
            let response = axum::http::Response::new(vec![0_u8; size]);
            assert_eq!(
                bounded_body(response.into()).await.is_ok(),
                size <= 256 * 1024
            );
        }
    }

    async fn fixture() -> (mock::MockOidc, Provider) {
        let server = mock::MockOidc::start().await;
        let provider = Provider::initialise(ProviderConfig {
            name: "test".into(),
            issuer: server.issuer.clone(),
            client_id: "test-client".into(),
        })
        .await
        .unwrap();
        (server, provider)
    }

    #[tokio::test]
    async fn verifies_id_tokens_and_preserves_the_subject_identity() {
        let (server, mut provider) = fixture().await;
        let keys = provider.configuration().await.unwrap();
        let token = server.id_token("nonce", "alice", Value::Null);
        let identity = provider
            .verify_id_token(&token, "nonce", &keys.keys)
            .unwrap();
        assert_eq!(identity.issuer, server.issuer);
        assert_eq!(identity.subject, "alice");
        // Google publishes two valid issuer spellings; store one canonical identity.
        provider.config.issuer = "https://accounts.google.com".into();
        for issuer in ["accounts.google.com", "https://accounts.google.com"] {
            let token = server.id_token("nonce", "alice", json!({"iss":issuer}));
            let identity = provider
                .verify_id_token(&token, "nonce", &keys.keys)
                .unwrap();
            assert_eq!(identity.issuer, "https://accounts.google.com");
        }
    }

    #[tokio::test]
    async fn rejects_invalid_signature_nonce_audience_issuer_and_times() {
        let (server, provider) = fixture().await;
        let keys = provider.configuration().await.unwrap();
        let now = Utc::now().timestamp();
        for overrides in [
            json!({"nonce":"wrong"}),
            json!({"nonce":null}),
            json!({"aud":"another-client"}),
            json!({"iss":"https://attacker.example"}),
            json!({"exp":1}),
            json!({"iat":now+60}),
            json!({"exp":now+7200}),
            json!({"nbf":now+60}),
            json!({"sub":""}),
            json!({"sub":"bad\nsubject"}),
            json!({"aud":["test-client","other"]}),
            json!({"azp":"other"}),
            json!({"invalid_signature":true}),
            json!({"kid":"unknown"}),
        ] {
            let token = server.id_token("nonce", "alice", overrides.clone());
            assert!(
                matches!(
                    provider.verify_id_token(&token, "nonce", &keys.keys),
                    Err(AuthError::Unauthorized)
                ),
                "accepted {overrides}"
            );
        }
        for token in ["not-a-jwt".to_owned(), "x".repeat(16 * 1024 + 1)] {
            assert!(
                provider
                    .verify_id_token(&token, "nonce", &keys.keys)
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn refreshes_expired_keys_and_fails_closed_during_outages() {
        let (server, provider) = fixture().await;
        Arc::get_mut(&mut *provider.cached.lock().await)
            .unwrap()
            .fetched_at = Instant::now() - Duration::from_secs(301);
        assert!(provider.configuration().await.is_ok());
        Arc::get_mut(&mut *provider.cached.lock().await)
            .unwrap()
            .fetched_at = Instant::now() - Duration::from_secs(301);
        drop(server);
        assert!(matches!(
            provider.configuration().await,
            Err(AuthError::Unavailable)
        ));
    }
}

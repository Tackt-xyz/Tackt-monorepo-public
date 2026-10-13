use axum::{Json, Router, extract::State, routing::get};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::Utc;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use p256::{
    SecretKey,
    elliptic_curve::{rand_core::OsRng, sec1::ToEncodedPoint},
    pkcs8::EncodePrivateKey,
};
use serde_json::{Value, json};
use std::sync::Arc;

pub struct MockOidc {
    pub issuer: String,
    state: Arc<MockState>,
    task: tokio::task::JoinHandle<()>,
}
struct MockState {
    issuer: String,
    signing: EncodingKey,
    jwks: Value,
}
impl Drop for MockOidc {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl MockOidc {
    pub async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let issuer = format!("http://{}", listener.local_addr().unwrap());
        let private = SecretKey::random(&mut OsRng);
        let public = private.public_key().to_encoded_point(false);
        let state = Arc::new(MockState {
            issuer: issuer.clone(),
            signing: EncodingKey::from_ec_der(private.to_pkcs8_der().unwrap().as_bytes()),
            jwks: json!({"keys": [{"kty":"EC", "crv":"P-256", "kid":"test-key", "use":"sig", "alg":"ES256",
                "x":URL_SAFE_NO_PAD.encode(public.x().unwrap()), "y":URL_SAFE_NO_PAD.encode(public.y().unwrap())}]}),
        });
        let app = Router::new()
            .route(
                "/.well-known/openid-configuration",
                get(|State(s): State<Arc<MockState>>| async move {
                    Json(json!({"issuer":s.issuer, "jwks_uri":format!("{}/jwks",s.issuer)}))
                }),
            )
            .route(
                "/jwks",
                get(|State(s): State<Arc<MockState>>| async move { Json(s.jwks.clone()) }),
            )
            .with_state(state.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            issuer,
            state,
            task,
        }
    }

    pub fn id_token(&self, nonce: &str, subject: &str, overrides: Value) -> String {
        let now = Utc::now().timestamp();
        let mut claims = json!({"iss":self.issuer, "sub":subject, "aud":"test-client", "iat":now, "exp":now+300, "nonce":nonce});
        if let Some(overrides) = overrides.as_object() {
            for (key, value) in overrides {
                claims[key] = value.clone();
            }
        }
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(claims["kid"].as_str().unwrap_or("test-key").into());
        if claims["invalid_signature"].as_bool() == Some(true) {
            let other = SecretKey::random(&mut OsRng);
            let wrong_key = EncodingKey::from_ec_der(other.to_pkcs8_der().unwrap().as_bytes());
            encode(&header, &claims, &wrong_key).unwrap()
        } else {
            encode(&header, &claims, &self.state.signing).unwrap()
        }
    }
}

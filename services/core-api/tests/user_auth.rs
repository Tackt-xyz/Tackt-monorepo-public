mod support;

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{HeaderMap, Request, StatusCode},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use ed25519_dalek::Signer;
use serde_json::{Value, json};
use std::sync::Arc;
use support::MockOidc;
use tackt_core_api::{
    api,
    auth::{ProviderConfig, UserAuth, UserAuthConfig},
};
use tackt_postgres::PgRepository;
use tackt_primitives::UserStatus;
use tower::ServiceExt;

const OPERATOR: &str = "test-operator-token-for-auth";
const SESSIONS: &str = "/api/v1/auth/sessions";
const IDENTITIES: &str = "/api/v1/users/me/identities";
const CURRENT_USER: &str = "/api/v1/users/me";
const REFRESH: &str = "/api/v1/auth/session-tokens";
const LOGOUT: &str = "/api/v1/users/me/sessions/current";
const BETA_ACCESS: &str = "/api/v1/users/me/beta-access";

#[tokio::test]
async fn current_user_returns_only_the_authenticated_users_provisioned_addresses() {
    let (repo, _, app) = setup(vec![]).await;
    browser(&app, "GET", CURRENT_USER, None, Value::Null)
        .await
        .expect(StatusCode::UNAUTHORIZED);

    let mut profiles = Vec::new();
    for login_wallet in [Wallet::Solana(wallet()), Wallet::evm()] {
        let proof = login_wallet.proof(&app, None).await;
        let login = browser(&app, "POST", SESSIONS, None, proof).await;
        login.expect(StatusCode::CREATED);
        let cookies = login.cookies();
        let user_id = login.body["user"]["id"].as_str().unwrap().parse().unwrap();
        let addresses = repo.user_wallet_addresses(user_id).await.unwrap().unwrap();
        let profile = browser(&app, "GET", CURRENT_USER, Some(&cookies), Value::Null).await;
        profile.expect(StatusCode::OK);
        assert_eq!(profile.headers["cache-control"], "no-store");
        let mut expected = login.body["user"].clone();
        expected["solanaAddress"] = json!(addresses.solana);
        expected["evmAddress"] = json!(addresses.ethereum);
        // Exact shape also excludes private keys, metadata, and login identity subjects.
        assert_eq!(profile.body, expected);

        let linked = Wallet::evm().proof(&app, Some(&cookies)).await;
        browser(&app, "POST", IDENTITIES, Some(&cookies), linked)
            .await
            .expect(StatusCode::CREATED);
        let after_link = browser(&app, "GET", CURRENT_USER, Some(&cookies), Value::Null).await;
        after_link.expect(StatusCode::OK);
        assert_eq!(after_link.body["solanaAddress"], expected["solanaAddress"]);
        assert_eq!(after_link.body["evmAddress"], expected["evmAddress"]);

        repo.update_user_status(user_id, UserStatus::Closed)
            .await
            .unwrap();
        browser(&app, "GET", CURRENT_USER, Some(&cookies), Value::Null)
            .await
            .expect(StatusCode::FORBIDDEN);
        profiles.push(profile.body);
    }
    assert_ne!(profiles[0]["solanaAddress"], profiles[1]["solanaAddress"]);
    assert_ne!(profiles[0]["evmAddress"], profiles[1]["evmAddress"]);
}

#[tokio::test]
async fn current_user_returns_null_addresses_until_legacy_addresses_are_backfilled() {
    let (repo, _, app) = setup(vec![]).await;
    let mut keys = tackt_core_api::signing::generate_user_signing_keys();
    let addresses = keys.addresses.take().unwrap();
    let (user, _) = repo
        .create_core_user(
            tackt_primitives::CreateCoreUser {
                external_id: uuid::Uuid::new_v4().to_string(),
                metadata: json!({}),
            },
            &keys,
        )
        .await
        .unwrap();
    let login_wallet = wallet();
    let subject = bs58::encode(login_wallet.verifying_key().as_bytes()).into_string();
    repo.link_core_user_identity(user.id, "urn:tackt:auth:solana", &subject)
        .await
        .unwrap();
    let proof = solana_proof(&app, &login_wallet, None).await;
    let login = browser(&app, "POST", SESSIONS, None, proof).await;
    login.expect(StatusCode::CREATED);
    let cookies = login.cookies();
    let profile = browser(&app, "GET", CURRENT_USER, Some(&cookies), Value::Null).await;
    profile.expect(StatusCode::OK);
    let mut expected = login.body["user"].clone();
    expected["solanaAddress"] = Value::Null;
    expected["evmAddress"] = Value::Null;
    assert_eq!(profile.body, expected);

    tackt_core_api::signing::backfill_wallet_addresses(&repo)
        .await
        .unwrap();
    let profile = browser(&app, "GET", CURRENT_USER, Some(&cookies), Value::Null).await;
    profile.expect(StatusCode::OK);
    expected["solanaAddress"] = json!(addresses.solana);
    expected["evmAddress"] = json!(addresses.ethereum);
    assert_eq!(profile.body, expected);
}

#[tokio::test]
async fn registry_catalogs_require_authentication_and_include_unheld_assets_and_markets() {
    let (_, _, app) = setup(vec![]).await;
    let wallet = Wallet::Solana(wallet());
    let proof = wallet.proof(&app, None).await;
    let login = browser(&app, "POST", SESSIONS, None, proof).await;
    login.expect(StatusCode::CREATED);
    let registry = tackt_primitives::assets::registry();
    for (path, key, count) in [
        ("/api/v1/assets", "assets", registry.catalog().len()),
        ("/api/v1/markets", "markets", registry.perp_markets().len()),
    ] {
        browser(&app, "GET", path, None, Value::Null)
            .await
            .expect(StatusCode::UNAUTHORIZED);
        let response = browser(&app, "GET", path, Some(&login.cookies()), Value::Null).await;
        response.expect(StatusCode::OK);
        assert_eq!(response.body[key].as_array().unwrap().len(), count);
        assert_eq!(response.headers["cache-control"], "no-store");
        browser(&app, "POST", path, Some(&login.cookies()), json!({}))
            .await
            .expect(StatusCode::METHOD_NOT_ALLOWED);
    }
}

#[tokio::test]
async fn risk_history_reads_only_authenticated_open_users_and_reports_missing_data() {
    let (repo, _, app) = setup(vec![]).await;
    let path = "/api/v1/users/me/risk-health";
    let history_path = format!("{path}/history");
    for route in [path, &history_path] {
        browser(&app, "GET", route, None, Value::Null)
            .await
            .expect(StatusCode::UNAUTHORIZED);
    }
    let alice = Wallet::Solana(wallet());
    let login = browser(&app, "POST", SESSIONS, None, alice.proof(&app, None).await).await;
    login.expect(StatusCode::CREATED);
    let cookies = login.cookies();
    let user: uuid::Uuid = login.body["user"]["id"].as_str().unwrap().parse().unwrap();
    let empty = browser(&app, "GET", path, Some(&cookies), Value::Null).await;
    empty.expect(StatusCode::OK);
    assert!(empty.body["latest"].is_null());
    assert_eq!(empty.body["metric"], "pooled_margin_health");
    assert_eq!(empty.body["inputFreshness"], "unknown");
    assert_eq!(empty.headers["cache-control"], "no-store");
    let at = chrono::DateTime::from_timestamp_micros(chrono::Utc::now().timestamp_micros())
        .unwrap()
        - chrono::Duration::minutes(1);
    let sample = tackt_postgres::test_support::risk_health_sample(user, at);
    repo.record_risk_health(&sample).await.unwrap();
    let mut second = sample.clone();
    second.sample_id = uuid::Uuid::new_v4();
    second.sampled_at += chrono::Duration::seconds(1);
    repo.record_risk_health(&second).await.unwrap();
    let latest = browser(&app, "GET", path, Some(&cookies), Value::Null).await;
    latest.expect(StatusCode::OK);
    assert_eq!(latest.body.as_object().unwrap().len(), 3);
    assert_eq!(
        latest.body["latest"],
        serde_json::json!({
            "sampledAt": second.sampled_at,
            "health": "170141183460469231731687303715884.105726",
            "classification": second.classification,
        })
    );
    let history = browser(
        &app,
        "GET",
        &format!("{history_path}?limit=1"),
        Some(&cookies),
        Value::Null,
    )
    .await;
    history.expect(StatusCode::OK);
    assert_eq!(history.body["samples"].as_array().unwrap().len(), 1);
    assert_eq!(history.body.as_object().unwrap().len(), 4);
    assert_eq!(history.body["samples"][0], latest.body["latest"]);
    assert_eq!(history.body["truncated"], true);
    browser(
        &app,
        "GET",
        &format!("{history_path}?limit=2001"),
        Some(&cookies),
        Value::Null,
    )
    .await
    .expect(StatusCode::BAD_REQUEST);
    let bob = Wallet::Solana(wallet());
    let other = browser(&app, "POST", SESSIONS, None, bob.proof(&app, None).await).await;
    let isolated = browser(&app, "GET", path, Some(&other.cookies()), Value::Null).await;
    isolated.expect(StatusCode::OK);
    assert!(isolated.body["latest"].is_null());
    repo.update_user_status(user, UserStatus::Closed)
        .await
        .unwrap();
    for route in [path, &history_path] {
        browser(&app, "GET", route, Some(&cookies), Value::Null)
            .await
            .expect(StatusCode::FORBIDDEN);
    }
}

#[tokio::test]
async fn beta_invites_enroll_only_the_authenticated_open_user() {
    use tackt_core_api::beta_access::BetaInviteCode;

    let (repo, auth, disabled) = setup(vec![]).await;
    let invite = uuid::Uuid::new_v4().to_string();
    let app = api::router(
        repo.clone(),
        OPERATOR.into(),
        None,
        auth.clone(),
        true,
        Some(BetaInviteCode::new(invite.as_str()).unwrap()),
        tackt_core_api::prices::PriceCache::default(),
    );
    let alice = Wallet::Solana(wallet());
    let proof = alice.proof(&app, None).await;
    let login = browser(&app, "POST", SESSIONS, None, proof).await;
    login.expect(StatusCode::CREATED);
    assert_eq!(login.body["user"]["betaAccess"], false);
    let cookies = login.cookies();
    let user_id = login.body["user"]["id"].as_str().unwrap().parse().unwrap();
    let keys = repo.user_signing_keys(user_id).await.unwrap().unwrap();
    k256::SecretKey::from_slice(&keys.ethereum_private_key).unwrap();
    let valid = json!({"inviteCode": invite.as_str()});
    let redeem = async |app: &Router, cookies: Option<&str>, input: Value| {
        browser(app, "POST", BETA_ACCESS, cookies, input).await
    };

    redeem(&app, None, valid.clone())
        .await
        .expect(StatusCode::UNAUTHORIZED);
    let unavailable = redeem(&disabled, Some(&cookies), valid.clone()).await;
    unavailable.expect(StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(unavailable.body["error"]["code"], "BETA_ACCESS_UNAVAILABLE");

    let wrong = uuid::Uuid::new_v4().to_string();
    let rejected = redeem(&app, Some(&cookies), json!({"inviteCode": wrong.as_str()})).await;
    rejected.expect(StatusCode::FORBIDDEN);
    assert_eq!(rejected.body["error"]["code"], "INVALID_INVITE_CODE");
    assert!(!rejected.body.to_string().contains(wrong.as_str()));
    for malformed in [
        json!({}),
        json!({"inviteCode":""}),
        json!({"inviteCode":"x".repeat(129)}),
        json!({"inviteCode":invite.as_str(),"userId":uuid::Uuid::new_v4()}),
    ] {
        redeem(&app, Some(&cookies), malformed)
            .await
            .expect(StatusCode::BAD_REQUEST);
    }
    request(
        &app,
        "POST",
        BETA_ACCESS,
        &[("cookie", &cookies), ("origin", "https://evil.example")],
        valid.clone(),
    )
    .await
    .expect(StatusCode::FORBIDDEN);
    assert!(!repo.core_user(user_id).await.unwrap().beta_access);

    let granted = redeem(&app, Some(&cookies), valid.clone()).await;
    granted.expect(StatusCode::OK);
    assert_eq!(granted.body, json!({"betaAccess":true}));
    let enrolled = repo.core_user(user_id).await.unwrap();
    assert!(enrolled.beta_access);
    redeem(&app, Some(&cookies), valid.clone())
        .await
        .expect(StatusCode::OK);
    assert_eq!(
        repo.core_user(user_id).await.unwrap().updated_at,
        enrolled.updated_at
    );
    let profile = browser(&app, "GET", CURRENT_USER, Some(&cookies), Value::Null).await;
    profile.expect(StatusCode::OK);
    assert_eq!(profile.body["betaAccess"], true);
    let rotated = browser(&app, "POST", REFRESH, Some(&cookies), json!({})).await;
    rotated.expect(StatusCode::CREATED);
    assert_eq!(rotated.body["user"]["betaAccess"], true);
    let proof = alice.proof(&app, None).await;
    let relogin = browser(&app, "POST", SESSIONS, None, proof).await;
    assert_eq!(relogin.body["user"]["betaAccess"], true);
    let relogin_keys = repo.user_signing_keys(user_id).await.unwrap().unwrap();
    assert!(keys.ethereum_private_key == relogin_keys.ethereum_private_key);
    assert!(keys.solana_private_key == relogin_keys.solana_private_key);
    // Removing the deployment's code does not reset previously granted access.
    let profile = browser(
        &disabled,
        "GET",
        CURRENT_USER,
        Some(&relogin.cookies()),
        Value::Null,
    )
    .await;
    assert_eq!(profile.body["betaAccess"], true);

    let bob = Wallet::evm();
    let proof = bob.proof(&app, None).await;
    let other = browser(&app, "POST", SESSIONS, None, proof).await;
    assert_eq!(other.body["user"]["betaAccess"], false);
    let other_id = other.body["user"]["id"].as_str().unwrap().parse().unwrap();
    repo.update_user_status(other_id, UserStatus::Closed)
        .await
        .unwrap();
    for router in [&app, &disabled] {
        for input in [
            valid.clone(),
            json!({"inviteCode":wrong.as_str()}),
            json!({"inviteCode":""}),
            json!({"inviteCode":"x".repeat(129)}),
        ] {
            let rejected = redeem(router, Some(&other.cookies()), input).await;
            rejected.expect(StatusCode::FORBIDDEN);
            assert_eq!(rejected.body["error"]["code"], "USER_CLOSED");
        }
    }
    assert!(!repo.core_user(other_id).await.unwrap().beta_access);
}

struct Reply {
    status: StatusCode,
    body: Value,
    headers: HeaderMap,
}

impl Reply {
    fn expect(&self, status: StatusCode) -> &Self {
        assert_eq!(self.status, status, "{}", self.body);
        self
    }

    fn credential(&self, name: &str) -> String {
        let prefix = format!("__Secure-tackt-{name}=");
        self.headers
            .get_all("set-cookie")
            .iter()
            .find_map(|value| {
                let value = value.to_str().unwrap();
                let token = value.strip_prefix(&prefix)?.split(';').next().unwrap();
                for attribute in ["Path=/api", "HttpOnly", "Secure", "SameSite=Strict"] {
                    assert!(value.split("; ").any(|part| part == attribute));
                }
                Some(token.to_owned())
            })
            .expect("session response must set both auth cookies")
    }

    fn cookies(&self) -> String {
        format!(
            "__Secure-tackt-access={}; __Secure-tackt-refresh={}",
            self.credential("access"),
            self.credential("refresh")
        )
    }
}

async fn request(
    app: &Router,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Value,
) -> Reply {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    assert!(headers.contains_key("x-request-id"));
    assert_eq!(headers["x-content-type-options"], "nosniff");
    let bytes = to_bytes(response.into_body(), 100_000).await.unwrap();
    let body: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    if status.is_client_error() || status.is_server_error() {
        assert_eq!(body["error"]["sanitized"], true, "{body}");
    }
    // Credentials must be delivered through cookies, never the JSON profile.
    assert!(body.get("accessToken").is_none());
    assert!(body.get("refreshToken").is_none());
    Reply {
        status,
        body,
        headers,
    }
}

async fn browser(
    app: &Router,
    method: &str,
    path: &str,
    cookies: Option<&str>,
    body: Value,
) -> Reply {
    let mut headers = vec![("origin", "https://app.example")];
    if let Some(cookies) = cookies {
        headers.push(("cookie", cookies));
    }
    request(app, method, path, &headers, body).await
}

async fn setup(providers: Vec<ProviderConfig>) -> (Arc<PgRepository>, UserAuth, Router) {
    let url = std::env::var("DATABASE_URL")
        .expect("start deploy/compose.yaml and set DATABASE_URL for auth integration tests");
    let repo = Arc::new(PgRepository::initialise(&url, 5).await.unwrap());
    let auth = UserAuth::initialise(UserAuthConfig {
        issuer: "https://api.example".into(),
        frontend_uri: "https://app.example".into(),
        jwt_secret: uuid::Uuid::new_v4().to_string().as_bytes().to_vec(),
        providers,
    })
    .await
    .unwrap();
    let app = api::router(
        repo.clone(),
        OPERATOR.into(),
        None,
        auth.clone(),
        true,
        None,
        tackt_core_api::prices::PriceCache::default(),
    );
    (repo, auth, app)
}

fn provider_config(provider: &MockOidc, name: &str) -> ProviderConfig {
    ProviderConfig {
        name: name.into(),
        issuer: provider.issuer.clone(),
        client_id: "test-client".into(),
    }
}

async fn oidc_proof(
    app: &Router,
    provider: &MockOidc,
    name: &str,
    subject: &str,
    cookies: Option<&str>,
) -> Value {
    let path = match cookies {
        Some(_) => format!("{IDENTITIES}/{name}/challenges"),
        None => format!("/api/v1/auth/providers/{name}/challenges"),
    };
    let flow = browser(app, "POST", &path, cookies, json!({})).await;
    flow.expect(StatusCode::CREATED);
    let id_token = provider.id_token(flow.body["nonce"].as_str().unwrap(), subject, Value::Null);
    json!({"method":"oidc", "idToken":id_token, "challengeToken":flow.body["challengeToken"]})
}

fn wallet() -> ed25519_dalek::SigningKey {
    use p256::elliptic_curve::rand_core::{OsRng, RngCore};
    let mut seed = [0; 32];
    OsRng.fill_bytes(&mut seed);
    ed25519_dalek::SigningKey::from_bytes(&seed)
}

async fn solana_proof(
    app: &Router,
    wallet: &ed25519_dalek::SigningKey,
    cookies: Option<&str>,
) -> Value {
    let path = match cookies {
        Some(_) => format!("{IDENTITIES}/solana/challenges"),
        None => "/api/v1/auth/providers/solana/challenges".into(),
    };
    let address = bs58::encode(wallet.verifying_key().as_bytes()).into_string();
    let challenge = browser(app, "POST", &path, cookies, json!({"address":address})).await;
    challenge.expect(StatusCode::CREATED);
    assert_eq!(challenge.body["signInInput"]["address"], address);
    let signature = STANDARD.encode(
        wallet
            .sign(challenge.body["message"].as_str().unwrap().as_bytes())
            .to_bytes(),
    );
    json!({"method":"solana", "challengeToken":challenge.body["challengeToken"], "signature":signature})
}

enum Wallet {
    Solana(ed25519_dalek::SigningKey),
    Evm(k256::ecdsa::SigningKey),
}

impl Wallet {
    fn evm() -> Self {
        Self::Evm(k256::ecdsa::SigningKey::random(
            &mut k256::elliptic_curve::rand_core::OsRng,
        ))
    }

    async fn proof(&self, app: &Router, cookies: Option<&str>) -> Value {
        match self {
            Self::Solana(wallet) => solana_proof(app, wallet, cookies).await,
            Self::Evm(wallet) => evm_proof(app, wallet, cookies).await,
        }
    }
}

async fn evm_proof(app: &Router, wallet: &k256::ecdsa::SigningKey, cookies: Option<&str>) -> Value {
    use sha3::{Digest, Keccak256};
    let path = match cookies {
        Some(_) => format!("{IDENTITIES}/evm/challenges"),
        None => "/api/v1/auth/providers/evm/challenges".into(),
    };
    let point = wallet.verifying_key().to_encoded_point(false);
    let address = format!(
        "0x{}",
        hex::encode(&Keccak256::digest(&point.as_bytes()[1..])[12..])
    );
    let challenge = browser(app, "POST", &path, cookies, json!({"address":address})).await;
    challenge.expect(StatusCode::CREATED);
    assert_eq!(
        challenge.body["signInInput"]["address"]
            .as_str()
            .unwrap()
            .to_ascii_lowercase(),
        address
    );
    assert_eq!(challenge.body["signInInput"]["chainId"], 1);
    let message = challenge.body["message"].as_str().unwrap();
    let digest = Keccak256::new()
        .chain_update(format!("\x19Ethereum Signed Message:\n{}", message.len()))
        .chain_update(message.as_bytes());
    let (signature, recovery) = wallet.sign_digest_recoverable(digest).unwrap();
    let signature = format!(
        "0x{}{:02x}",
        hex::encode(signature.to_bytes()),
        recovery.to_byte() + 27
    );
    json!({"method":"evm", "challengeToken":challenge.body["challengeToken"], "signature":signature})
}

#[tokio::test]
async fn multiple_login_methods_share_one_user_without_implicit_merging() {
    let first = MockOidc::start().await;
    let second = MockOidc::start().await;
    let (_, _, app) = setup(vec![
        provider_config(&first, "first"),
        provider_config(&second, "second"),
    ])
    .await;
    let proof = oidc_proof(&app, &first, "first", "alice", None).await;
    let mut wrong = proof.clone();
    wrong["idToken"] = json!(first.id_token("other-attempt", "alice", Value::Null));
    browser(&app, "POST", SESSIONS, None, wrong)
        .await
        .expect(StatusCode::UNAUTHORIZED);
    let login = browser(&app, "POST", SESSIONS, None, proof.clone()).await;
    login.expect(StatusCode::CREATED);
    browser(&app, "POST", SESSIONS, None, proof)
        .await
        .expect(StatusCode::UNAUTHORIZED);
    let cookies = login.cookies();
    let user_id = login.body["user"]["id"].clone();
    assert!(login.body["user"]["createdAt"].is_i64());
    assert!(login.body["expiresAt"].as_i64().unwrap() > 1_000_000_000_000);
    assert!(login.body["user"].get("metadata").is_none());
    assert_eq!(login.headers["cache-control"], "no-store");

    let proof = oidc_proof(&app, &second, "second", "alice-other-login", Some(&cookies)).await;
    browser(&app, "POST", SESSIONS, Some(&cookies), proof.clone())
        .await
        .expect(StatusCode::BAD_REQUEST);
    let linked = browser(&app, "POST", IDENTITIES, Some(&cookies), proof).await;
    linked.expect(StatusCode::CREATED);
    assert_eq!(linked.body["userId"], user_id);
    let proof = oidc_proof(&app, &second, "second", "alice-other-login", None).await;
    let other_login = browser(&app, "POST", SESSIONS, None, proof).await;
    other_login.expect(StatusCode::CREATED);
    assert_eq!(other_login.body["user"]["id"], user_id);

    let wallet = wallet();
    let proof = solana_proof(&app, &wallet, Some(&cookies)).await;
    browser(&app, "POST", IDENTITIES, None, proof.clone())
        .await
        .expect(StatusCode::UNAUTHORIZED);
    let linked = browser(&app, "POST", IDENTITIES, Some(&cookies), proof.clone()).await;
    linked.expect(StatusCode::CREATED);
    assert_eq!(linked.body["userId"], user_id);
    browser(&app, "POST", IDENTITIES, Some(&cookies), proof)
        .await
        .expect(StatusCode::UNAUTHORIZED);
    let proof = solana_proof(&app, &wallet, None).await;
    let wallet_login = browser(&app, "POST", SESSIONS, None, proof).await;
    wallet_login.expect(StatusCode::CREATED);
    assert_eq!(wallet_login.body["user"]["id"], user_id);

    let evm_wallet = Wallet::evm();
    let proof = evm_wallet.proof(&app, Some(&cookies)).await;
    browser(&app, "POST", SESSIONS, Some(&cookies), proof.clone())
        .await
        .expect(StatusCode::BAD_REQUEST);
    browser(
        &app,
        "POST",
        IDENTITIES,
        Some(&other_login.cookies()),
        proof.clone(),
    )
    .await
    .expect(StatusCode::UNAUTHORIZED);
    let linked = browser(&app, "POST", IDENTITIES, Some(&cookies), proof).await;
    linked.expect(StatusCode::CREATED);
    assert_eq!(linked.body["userId"], user_id);
    assert_eq!(linked.body["issuer"], "urn:tackt:auth:evm");
    let subject = linked.body["subject"].as_str().unwrap();
    assert_eq!(subject, subject.to_ascii_lowercase());
    let proof = evm_wallet.proof(&app, None).await;
    browser(&app, "POST", IDENTITIES, Some(&cookies), proof.clone())
        .await
        .expect(StatusCode::BAD_REQUEST);
    let evm_login = browser(&app, "POST", SESSIONS, None, proof).await;
    evm_login.expect(StatusCode::CREATED);
    assert_eq!(evm_login.body["user"]["id"], user_id);

    let mut path = format!("{IDENTITIES}?limit=1");
    let mut identities = Vec::new();
    loop {
        let page = browser(&app, "GET", &path, Some(&cookies), Value::Null).await;
        page.expect(StatusCode::OK);
        identities.extend(page.body["items"].as_array().unwrap().iter().cloned());
        if page.body["hasMore"] == false {
            assert!(page.body["nextCursor"].is_null());
            break;
        }
        path = format!(
            "{IDENTITIES}?limit=1&cursor={}",
            page.body["nextCursor"].as_str().unwrap()
        );
    }
    assert_eq!(identities.len(), 4);
    assert!(
        identities
            .iter()
            .any(|identity| identity["provider"] == "evm")
    );
    assert!(
        identities
            .iter()
            .any(|identity| identity["provider"] == "solana")
    );
    browser(
        &app,
        "GET",
        &format!("{IDENTITIES}?limit=101"),
        Some(&cookies),
        Value::Null,
    )
    .await
    .expect(StatusCode::BAD_REQUEST);
    browser(
        &app,
        "GET",
        &format!("{IDENTITIES}?cursor=invalid"),
        Some(&cookies),
        Value::Null,
    )
    .await
    .expect(StatusCode::BAD_REQUEST);

    let proof = oidc_proof(&app, &first, "first", "bob", None).await;
    let bob = browser(&app, "POST", SESSIONS, None, proof).await;
    let bob_cookies = bob.cookies();
    let proof = oidc_proof(
        &app,
        &second,
        "second",
        "alice-other-login",
        Some(&bob_cookies),
    )
    .await;
    browser(&app, "POST", IDENTITIES, Some(&bob_cookies), proof)
        .await
        .expect(StatusCode::CONFLICT);
    let proof = solana_proof(&app, &wallet, Some(&bob_cookies)).await;
    browser(&app, "POST", IDENTITIES, Some(&bob_cookies), proof)
        .await
        .expect(StatusCode::CONFLICT);
    let proof = evm_wallet.proof(&app, Some(&bob_cookies)).await;
    browser(&app, "POST", IDENTITIES, Some(&bob_cookies), proof)
        .await
        .expect(StatusCode::CONFLICT);
    let proof = oidc_proof(&app, &second, "second", "unclaimed", Some(&cookies)).await;
    browser(&app, "POST", IDENTITIES, Some(&bob_cookies), proof)
        .await
        .expect(StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn solana_proofs_rotation_and_revocation_have_one_winner() {
    wallet_proofs_rotation_and_revocation_have_one_winner(Wallet::Solana(wallet())).await;
}

#[tokio::test]
async fn evm_proofs_rotation_and_revocation_have_one_winner() {
    wallet_proofs_rotation_and_revocation_have_one_winner(Wallet::evm()).await;
}

async fn wallet_proofs_rotation_and_revocation_have_one_winner(wallet: Wallet) {
    let (repo, auth, app) = setup(vec![]).await;
    let closed_signup = api::router(
        repo.clone(),
        OPERATOR.into(),
        None,
        auth,
        false,
        None,
        tackt_core_api::prices::PriceCache::default(),
    );
    let proof = wallet.proof(&closed_signup, None).await;
    browser(&closed_signup, "POST", SESSIONS, None, proof)
        .await
        .expect(StatusCode::FORBIDDEN);
    let providers = browser(&app, "GET", "/api/v1/auth/providers", None, Value::Null).await;
    assert_eq!(
        providers.body,
        json!({"items":["evm", "solana"], "hasMore":false, "nextCursor":null})
    );
    let proof = wallet.proof(&app, None).await;
    let mut invalid = proof.clone();
    invalid["signature"] = json!("invalid");
    browser(&app, "POST", SESSIONS, None, invalid)
        .await
        .expect(StatusCode::UNAUTHORIZED);
    let (a, b) = tokio::join!(
        browser(&app, "POST", SESSIONS, None, proof.clone()),
        browser(&app, "POST", SESSIONS, None, proof)
    );
    let (login, replay) = if a.status == StatusCode::CREATED {
        (a, b)
    } else {
        (b, a)
    };
    login.expect(StatusCode::CREATED);
    replay.expect(StatusCode::UNAUTHORIZED);
    assert_eq!(replay.body["error"]["code"], "CHALLENGE_USED");
    let cookies = login.cookies();
    let proof = wallet.proof(&app, None).await;
    let other = browser(&app, "POST", SESSIONS, None, proof).await;
    assert_eq!(other.body["user"]["id"], login.body["user"]["id"]);
    let other_cookies = other.cookies();
    let (a, b) = tokio::join!(
        browser(&app, "POST", REFRESH, Some(&cookies), json!({})),
        browser(&app, "POST", REFRESH, Some(&cookies), json!({}))
    );
    let (rotated, reused) = if a.status == StatusCode::CREATED {
        (a, b)
    } else {
        (b, a)
    };
    rotated.expect(StatusCode::CREATED);
    reused.expect(StatusCode::UNAUTHORIZED);
    assert_ne!(rotated.credential("refresh"), login.credential("refresh"));
    let rotated_cookies = rotated.cookies();
    let link_proof = wallet.proof(&app, Some(&rotated_cookies)).await;
    let logout = browser(&app, "DELETE", LOGOUT, Some(&rotated_cookies), Value::Null).await;
    logout.expect(StatusCode::OK);
    assert!(logout.credential("access").is_empty());
    assert!(logout.credential("refresh").is_empty());
    for session in [&cookies, &rotated_cookies] {
        browser(&app, "GET", CURRENT_USER, Some(session), Value::Null)
            .await
            .expect(StatusCode::OK);
    }
    browser(&app, "POST", REFRESH, Some(&rotated_cookies), json!({}))
        .await
        .expect(StatusCode::UNAUTHORIZED);
    // Logout blocks refresh, but the issued access JWT still authorizes requests.
    browser(&app, "POST", IDENTITIES, Some(&rotated_cookies), link_proof)
        .await
        .expect(StatusCode::OK);
    browser(&app, "GET", CURRENT_USER, Some(&other_cookies), Value::Null)
        .await
        .expect(StatusCode::OK);
    let closed_link_proof = wallet.proof(&app, Some(&other_cookies)).await;
    repo.update_user_status(
        login.body["user"]["id"].as_str().unwrap().parse().unwrap(),
        UserStatus::Closed,
    )
    .await
    .unwrap();
    browser(&app, "GET", CURRENT_USER, Some(&other_cookies), Value::Null)
        .await
        .expect(StatusCode::FORBIDDEN);
    browser(&app, "GET", IDENTITIES, Some(&other_cookies), Value::Null)
        .await
        .expect(StatusCode::FORBIDDEN);
    browser(
        &app,
        "POST",
        IDENTITIES,
        Some(&other_cookies),
        closed_link_proof,
    )
    .await
    .expect(StatusCode::FORBIDDEN);
    browser(&app, "POST", REFRESH, Some(&other_cookies), json!({}))
        .await
        .expect(StatusCode::FORBIDDEN);
    let proof = wallet.proof(&app, None).await;
    browser(&app, "POST", SESSIONS, None, proof)
        .await
        .expect(StatusCode::FORBIDDEN);
    let nonce = uuid::Uuid::new_v4();
    let expiry = chrono::Utc::now() - chrono::Duration::seconds(1);
    assert!(!repo.consume_auth_challenge(nonce, expiry).await.unwrap());
    assert!(!repo.consume_auth_challenge(nonce, expiry).await.unwrap());
}

#[tokio::test]
async fn solana_cookies_origins_and_service_user_ids_preserve_the_auth_boundary() {
    cookies_origins_and_service_user_ids_preserve_the_auth_boundary(Wallet::Solana(wallet())).await;
}

#[tokio::test]
async fn evm_cookies_origins_and_service_user_ids_preserve_the_auth_boundary() {
    cookies_origins_and_service_user_ids_preserve_the_auth_boundary(Wallet::evm()).await;
}

async fn cookies_origins_and_service_user_ids_preserve_the_auth_boundary(wallet: Wallet) {
    let (_, _, app) = setup(vec![]).await;
    let proof = wallet.proof(&app, None).await;
    let login = browser(&app, "POST", SESSIONS, None, proof).await;
    login.expect(StatusCode::CREATED);
    let cookies = login.cookies();
    let access = format!("Bearer {}", login.credential("access"));
    let id = login.body["user"]["id"].as_str().unwrap();
    browser(&app, "GET", CURRENT_USER, Some(&cookies), Value::Null)
        .await
        .expect(StatusCode::OK);
    request(
        &app,
        "GET",
        CURRENT_USER,
        &[("authorization", &access), ("x-user-id", id)],
        Value::Null,
    )
    .await
    .expect(StatusCode::OK);
    request(
        &app,
        "GET",
        CURRENT_USER,
        &[("authorization", &access)],
        Value::Null,
    )
    .await
    .expect(StatusCode::FORBIDDEN);
    request(
        &app,
        "GET",
        CURRENT_USER,
        &[
            ("authorization", &access),
            ("x-user-id", &uuid::Uuid::new_v4().to_string()),
        ],
        Value::Null,
    )
    .await
    .expect(StatusCode::FORBIDDEN);
    request(
        &app,
        "GET",
        CURRENT_USER,
        &[
            ("authorization", &access),
            ("x-user-id", id),
            ("cookie", &cookies),
        ],
        Value::Null,
    )
    .await
    .expect(StatusCode::UNAUTHORIZED);
    request(
        &app,
        "GET",
        "/v1/users",
        &[("authorization", &access)],
        Value::Null,
    )
    .await
    .expect(StatusCode::UNAUTHORIZED);
    request(
        &app,
        "GET",
        CURRENT_USER,
        &[("authorization", &format!("Bearer {OPERATOR}"))],
        Value::Null,
    )
    .await
    .expect(StatusCode::UNAUTHORIZED);
    request(&app, "POST", REFRESH, &[("cookie", &cookies)], json!({}))
        .await
        .expect(StatusCode::FORBIDDEN);
    request(
        &app,
        "POST",
        REFRESH,
        &[("cookie", &cookies), ("origin", "https://evil.example")],
        json!({}),
    )
    .await
    .expect(StatusCode::FORBIDDEN);
    browser(
        &app,
        "POST",
        REFRESH,
        Some(&cookies),
        json!({"refreshToken":login.credential("refresh")}),
    )
    .await
    .expect(StatusCode::BAD_REQUEST);
    let duplicate = format!(
        "{cookies}; __Secure-tackt-access={}",
        login.credential("access")
    );
    browser(&app, "GET", CURRENT_USER, Some(&duplicate), Value::Null)
        .await
        .expect(StatusCode::UNAUTHORIZED);
    for body in [
        json!({"method":"solana", "challenge_token":"old-case", "signature":"x"}),
        json!({"method":"solana", "challengeToken":"x", "signature":"x", "extra":true}),
    ] {
        browser(&app, "POST", SESSIONS, None, body)
            .await
            .expect(StatusCode::BAD_REQUEST);
    }
    let refreshed = request(
        &app,
        "POST",
        REFRESH,
        &[],
        json!({"refreshToken":login.credential("refresh")}),
    )
    .await;
    refreshed.expect(StatusCode::CREATED);
}

#[tokio::test]
async fn id_token_challenges_reject_old_proofs_and_allow_only_one_concurrent_session() {
    let provider = MockOidc::start().await;
    let (_, _, app) = setup(vec![provider_config(&provider, "google")]).await;
    browser(
        &app,
        "POST",
        SESSIONS,
        None,
        json!({"method":"oidc", "code":"removed", "state":"removed", "flowToken":"removed"}),
    )
    .await
    .expect(StatusCode::BAD_REQUEST);
    let proof = oidc_proof(&app, &provider, "google", "concurrent", None).await;
    let other = oidc_proof(&app, &provider, "google", "concurrent", None).await;
    let mut substituted = proof.clone();
    substituted["challengeToken"] = other["challengeToken"].clone();
    browser(&app, "POST", SESSIONS, None, substituted)
        .await
        .expect(StatusCode::UNAUTHORIZED);
    let (one, two) = tokio::join!(
        browser(&app, "POST", SESSIONS, None, proof.clone()),
        browser(&app, "POST", SESSIONS, None, proof)
    );
    let mut statuses = [one.status.as_u16(), two.status.as_u16()];
    statuses.sort();
    assert_eq!(statuses, [201, 401]);
    let rejected = if one.status == StatusCode::UNAUTHORIZED {
        one
    } else {
        two
    };
    assert_eq!(rejected.body["error"]["code"], "CHALLENGE_USED");
}

#[tokio::test]
async fn account_page_reads_are_authenticated_isolated_and_paginated() {
    let (repo, auth, app) = setup(vec![]).await;
    let summary = "/api/v1/users/me/account-summary";
    let activity = "/api/v1/users/me/activity";
    for path in [summary, activity] {
        browser(&app, "GET", path, None, Value::Null)
            .await
            .expect(StatusCode::UNAUTHORIZED);
    }
    let alice = Wallet::Solana(wallet());
    let login = browser(&app, "POST", SESSIONS, None, alice.proof(&app, None).await).await;
    login.expect(StatusCode::CREATED);
    let cookies = login.cookies();
    browser(
        &app,
        "GET",
        "/api/v1/users/me/balances",
        Some(&cookies),
        Value::Null,
    )
    .await
    .expect(StatusCode::NOT_FOUND);
    let user: uuid::Uuid = login.body["user"]["id"].as_str().unwrap().parse().unwrap();
    let bob = Wallet::Solana(wallet());
    let bob_login = browser(&app, "POST", SESSIONS, None, bob.proof(&app, None).await).await;
    bob_login.expect(StatusCode::CREATED);
    let other = bob_login.body["user"]["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let empty = browser(&app, "GET", summary, Some(&cookies), Value::Null).await;
    empty.expect(StatusCode::OK);
    assert_eq!(empty.body["assets"], json!([]));
    assert_eq!(empty.body["netEquityUsd"], "0.00000000");
    assert_eq!(empty.body["complete"], true);
    let ids = tackt_postgres::test_support::account_page_fixture(&repo, user, other).await;
    let cold = browser(&app, "GET", summary, Some(&cookies), Value::Null).await;
    cold.expect(StatusCode::OK);
    assert_eq!(cold.headers["cache-control"], "no-store");
    assert_eq!(cold.body["userId"], user.to_string());
    assert_eq!(cold.body["complete"], false);
    assert!(cold.body["netEquityUsd"].is_null());
    assert_eq!(cold.body["outstandingDebtUsd"], "2000.00000000");
    assert_eq!(cold.body["missingPriceAssetIds"], json!(["ETH"]));
    let usdc = cold.body["assets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|asset| asset["assetId"] == "USDC")
        .unwrap();
    assert_eq!(usdc["priceUsd"], "1.00000000");
    assert!(usdc["priceAsOf"].is_null());
    let mut cursor = None;
    let mut seen = Vec::new();
    loop {
        let path = cursor.as_ref().map_or_else(
            || format!("{activity}?limit=1"),
            |cursor| format!("{activity}?limit=1&cursor={cursor}"),
        );
        let page = browser(&app, "GET", &path, Some(&cookies), Value::Null).await;
        page.expect(StatusCode::OK);
        assert_eq!(page.headers["cache-control"], "no-store");
        assert_eq!(page.body["items"].as_array().unwrap().len(), 1);
        let item = &page.body["items"][0];
        seen.push(item["id"].as_str().unwrap().parse::<uuid::Uuid>().unwrap());
        if item["commandKind"] == "draw_credit" {
            assert_eq!(item["changes"].as_array().unwrap().len(), 2);
            assert_eq!(item["type"], "credit");
        }
        cursor = page.body["nextCursor"].as_str().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(seen, ids);
    for query in [
        "limit=0",
        "limit=101",
        "cursor=broken",
        "limit=nope",
        "userId=someone",
    ] {
        browser(
            &app,
            "GET",
            &format!("{activity}?{query}"),
            Some(&cookies),
            Value::Null,
        )
        .await
        .expect(StatusCode::BAD_REQUEST);
    }
    let bearer = format!("Bearer {}", login.credential("access"));
    for path in [summary, activity] {
        request(
            &app,
            "GET",
            path,
            &[
                ("authorization", &bearer),
                ("x-user-id", &other.to_string()),
            ],
            Value::Null,
        )
        .await
        .expect(StatusCode::FORBIDDEN);
        browser(&app, "POST", path, Some(&cookies), json!({}))
            .await
            .expect(StatusCode::METHOD_NOT_ALLOWED);
    }
    // Exercise the actual price-feed -> cache -> HTTP response path using Compose NATS.
    let nats_url = std::env::var("NATS_URL").expect("Compose NATS_URL required");
    let client = Arc::new(tackt_nats::initialise(&nats_url).await.unwrap());
    let id = uuid::Uuid::new_v4().simple().to_string();
    let stream = tackt_nats::StreamSpec {
        name: format!("ACCOUNT_PAGE_{id}"),
        subjects: vec![format!("test.account.page.{id}")],
        ..tackt_nats::subjects::price_aggregate_stream()
    };
    client.ensure_stream(&stream).await.unwrap();
    let (prices, feed) =
        tackt_core_api::prices::PriceCache::initialise(client.clone(), stream.clone())
            .await
            .unwrap();
    let feed_task = tokio::spawn(feed.run());
    {
        let (asset, dollars) = (tackt_primitives::ledger::LedgerAsset::Eth, 3000);
        let price = tackt_nats::aggregates::AggregatePrice::new(
            asset,
            tackt_nats::PriceQuote::new(dollars * 100_000_000, 8).unwrap(),
            1,
            chrono::Utc::now(),
            vec![tackt_nats::aggregates::AggregateSource {
                authority: tackt_nats::PriceAuthority::Binance,
                evidence_id: format!("priceev_{}", "a".repeat(64)),
                effective_weight_bps: 10_000,
            }],
        )
        .unwrap();
        client
            .publish_aggregate_price(&stream, &price)
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while prices.get_latest(asset).is_none() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    let priced_app = api::router(
        repo.clone(),
        OPERATOR.into(),
        None,
        auth,
        true,
        None,
        prices,
    );
    let valued = browser(&priced_app, "GET", summary, Some(&cookies), Value::Null).await;
    valued.expect(StatusCode::OK);
    assert_eq!(valued.body["complete"], true);
    assert_eq!(valued.body["netEquityUsd"], "17000.00000000");
    assert_eq!(valued.body["creditLimitUsd"], "14450.00000000");
    assert_eq!(valued.body["outstandingDebtUsd"], "2000.00000000");
    assert_eq!(valued.body["availableCreditUsd"], "12450.00000000");
    for asset in valued.body["assets"].as_array().unwrap() {
        assert_eq!(asset["ltv"], "0.85");
        assert_eq!(asset["liquidationThreshold"], "0.90");
    }
    feed_task.abort();
    let _ = feed_task.await;
    client.delete_stream(&stream.name).await.unwrap();
    repo.update_user_status(user, UserStatus::Closed)
        .await
        .unwrap();
    for path in [summary, activity] {
        browser(&app, "GET", path, Some(&cookies), Value::Null)
            .await
            .expect(StatusCode::FORBIDDEN);
    }
}

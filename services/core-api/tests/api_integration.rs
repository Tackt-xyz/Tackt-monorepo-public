use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use std::sync::Arc;
use tackt_postgres::PgRepository;
use tower::ServiceExt;
use uuid::Uuid;

const TOKEN: &str = "integration-test-operator-token";

async fn user_auth() -> tackt_core_api::auth::UserAuth {
    tackt_core_api::auth::UserAuth::initialise(tackt_core_api::auth::UserAuthConfig {
        issuer: "https://api.example".into(),
        frontend_uri: "https://app.example".into(),
        jwt_secret: uuid::Uuid::new_v4().to_string().as_bytes().to_vec(),
        providers: vec![],
    })
    .await
    .unwrap()
}

/// 40 hexadecimal characters unique to this run, sized for an EVM address body.
fn random_hex_40() -> String {
    let mut hex = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    hex.truncate(40);
    hex
}

/// Reads a gating env var; skips locally, hard-fails when missing in CI.
fn gated_env(var: &str) -> Option<String> {
    std::env::var(var).ok().or_else(|| {
        assert!(
            std::env::var_os("CI").is_none(),
            "{var} is unset in CI; refusing to silently skip integration tests"
        );
        eprintln!("skipping: set {var} to run integration tests");
        None
    })
}

async fn request(app: &Router, method: &str, uri: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("x-operator-id", "api-integration-test")
                .header("x-request-id", Uuid::new_v4().to_string())
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1_000_000).await.unwrap();
    // Every response, including extractor rejections, must be JSON: the router's
    // error-envelope layer guarantees it, and this helper enforces it.
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("API responses must be JSON")
    };
    (status, value)
}

#[tokio::test]
async fn operator_user_api_supports_the_full_provisioning_lifecycle() {
    let Some(database_url) = gated_env("DATABASE_URL") else {
        return;
    };
    let repo = Arc::new(
        PgRepository::initialise(&database_url, 5)
            .await
            .expect("clean migrations"),
    );
    let app = tackt_core_api::api::router(
        repo.clone(),
        TOKEN.into(),
        None,
        user_auth().await,
        false,
        None,
        tackt_core_api::prices::PriceCache::default(),
    );

    let unauthorized = app
        .clone()
        .oneshot(Request::get("/v1/users").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let external_id = format!("api-test-{}", Uuid::new_v4());
    let create_body = json!({"external_id": external_id});
    let (status, created) = request(&app, "POST", "/v1/users", create_body.clone()).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(created["created"], true);
    assert_eq!(created["user"]["status"], "pending");
    assert_eq!(created["user"]["beta_access"], false);
    let id = created["user"]["id"].as_str().unwrap();
    let keys = repo
        .user_signing_keys(id.parse().unwrap())
        .await
        .unwrap()
        .unwrap();
    k256::SecretKey::from_slice(&keys.ethereum_private_key).unwrap();
    assert!(created["user"].get("ethereum_private_key").is_none());
    assert!(created["user"].get("solana_private_key").is_none());

    let identity = json!({
        "issuer": "https://identity.example.com/",
        "subject": format!("operator-enrolled-{}", Uuid::new_v4()),
    });
    let (status, linked) = request(
        &app,
        "POST",
        &format!("/v1/users/{id}/identities"),
        identity.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(linked["user_id"], id.to_owned());
    assert!(linked.get("created").is_none());
    let (status, linked) = request(
        &app,
        "POST",
        &format!("/v1/users/{id}/identities"),
        identity.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(linked["user_id"], id.to_owned());

    let (status, retried) = request(&app, "POST", "/v1/users", create_body).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(retried["created"], false);
    let retried_keys = repo
        .user_signing_keys(id.parse().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(keys.ethereum_private_key == retried_keys.ethereum_private_key);
    assert!(keys.solana_private_key == retried_keys.solana_private_key);
    assert_eq!(retried["user"]["id"], id);

    let (status, fetched) = request(&app, "GET", &format!("/v1/users/{id}"), Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fetched["external_id"], external_id);

    // Walk the keyset pages until the created user appears; the shared test
    // database accumulates users across runs.
    let mut uri = "/v1/users?limit=200".to_owned();
    let mut found = false;
    loop {
        let (status, listed) = request(&app, "GET", &uri, Value::Null).await;
        assert_eq!(status, StatusCode::OK);
        found = found
            || listed["data"]
                .as_array()
                .unwrap()
                .iter()
                .any(|user| user["id"] == id);
        match listed["next_cursor"].as_str() {
            Some(cursor) if !found => uri = format!("/v1/users?limit=200&cursor={cursor}"),
            _ => break,
        }
    }
    assert!(found, "created user must appear in the listing");

    let (status, active) = request(
        &app,
        "PUT",
        &format!("/v1/users/{id}/status"),
        json!({"status":"active"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(active["status"], "active");

    let (status, rejected) = request(
        &app,
        "PUT",
        &format!("/v1/users/{id}/status"),
        json!({"status":"pending"}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(rejected["error"]["code"], "INVALID_TRANSITION");

    // Unique per run: accounts persist in the shared test database, and a fixed
    // wallet would collide with the association a previous run left behind.
    // Uppercased so the response still proves normalization.
    let wallet = format!("0x{}", random_hex_40().to_ascii_uppercase());
    let normalized_wallet = wallet.to_ascii_lowercase();
    let (status, ethereum) = request(
        &app,
        "PUT",
        &format!("/v1/users/{id}/accounts/ethereum"),
        json!({"account":wallet.clone()}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ethereum["accounts"]["ethereum"], normalized_wallet);

    let hyperliquid = format!("0x{}", random_hex_40());
    let (status, associated) = request(
        &app,
        "PUT",
        &format!("/v1/users/{id}/accounts/hyperliquid"),
        json!({"account":hyperliquid.clone()}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(associated["accounts"]["hyperliquid"], hyperliquid);

    let (status, second) = request(
        &app,
        "POST",
        "/v1/users",
        json!({"external_id":format!("api-test-{}",Uuid::new_v4())}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let second_id = second["user"]["id"].as_str().unwrap();
    let (status, identity_conflict) = request(
        &app,
        "POST",
        &format!("/v1/users/{second_id}/identities"),
        identity,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(identity_conflict["error"]["code"], "IDENTITY_CONFLICT");
    let (status, conflict) = request(
        &app,
        "PUT",
        &format!("/v1/users/{second_id}/accounts/ethereum"),
        json!({"account":normalized_wallet}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(conflict["error"]["code"], "ASSOCIATION_CONFLICT");

    let (status, _) = request(
        &app,
        "PUT",
        &format!("/v1/users/{id}/accounts/solana"),
        json!({"account":hyperliquid}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// Mutations publish lifecycle events onto TACKT_USERS, and a no-op status
/// update publishes nothing. Requires both PostgreSQL and NATS.
#[tokio::test]
async fn mutations_publish_lifecycle_events() {
    let Some(database_url) = gated_env("DATABASE_URL") else {
        return;
    };
    let Some(nats_url) = gated_env("NATS_URL") else {
        return;
    };
    let repo = Arc::new(
        PgRepository::initialise(&database_url, 5)
            .await
            .expect("clean migrations"),
    );
    let client = tackt_nats::initialise(&nats_url).await.expect("connect");
    client
        .ensure_stream(&tackt_nats::subjects::users_stream())
        .await
        .expect("users stream");
    // Ephemeral new-only consumer, attached before the mutations it observes
    // — the production consumption model, and nothing leaks onto the shared
    // stream.
    let mut consumer = client.user_events().await.expect("consumer");
    let app = tackt_core_api::api::router(
        repo,
        TOKEN.into(),
        Some(Arc::new(client.clone())),
        user_auth().await,
        false,
        None,
        tackt_core_api::prices::PriceCache::default(),
    );

    let external_id = format!("event-test-{}", Uuid::new_v4());
    let (status, created) = request(
        &app,
        "POST",
        "/v1/users",
        json!({"external_id": external_id}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let id = created["user"]["id"].as_str().unwrap().to_owned();
    let (status, _) = request(
        &app,
        "PUT",
        &format!("/v1/users/{id}/status"),
        json!({"status":"active"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // Repeat: a no-op transition must not publish a second StatusChanged.
    let (status, _) = request(
        &app,
        "PUT",
        &format!("/v1/users/{id}/status"),
        json!({"status":"active"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let wallet = format!("0x{}", random_hex_40());
    let (status, _) = request(
        &app,
        "PUT",
        &format!("/v1/users/{id}/accounts/ethereum"),
        json!({"account":wallet}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // The stream is shared across concurrent runs: filter to this test's
    // user. Keep draining briefly after the third event — a suppressed no-op
    // StatusChanged must not surface as a fourth.
    let mut seen = Vec::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while std::time::Instant::now() < deadline && seen.len() < 4 {
        let wait = if seen.len() < 3 { 2 } else { 1 };
        let Ok(Ok(delivery)) =
            tokio::time::timeout(std::time::Duration::from_secs(wait), consumer.next()).await
        else {
            if seen.len() >= 3 {
                break;
            }
            continue;
        };
        let event = delivery.message().clone();
        delivery.ack().await.expect("ack");
        let matches_user = match &event.kind {
            tackt_nats::UserEventKind::Created { user } => user.id.to_string() == id,
            tackt_nats::UserEventKind::StatusChanged { user_id, .. }
            | tackt_nats::UserEventKind::AccountAssociated { user_id, .. } => {
                user_id.to_string() == id
            }
        };
        if matches_user {
            seen.push(event.kind);
        }
    }
    assert_eq!(
        seen.len(),
        3,
        "expected exactly created/status/account events: {seen:?}"
    );
    assert!(
        matches!(&seen[0], tackt_nats::UserEventKind::Created { user } if user.status == tackt_primitives::UserStatus::Pending)
    );
    assert!(
        matches!(&seen[1], tackt_nats::UserEventKind::StatusChanged { status, .. } if *status == tackt_primitives::UserStatus::Active)
    );
    assert!(
        matches!(&seen[2], tackt_nats::UserEventKind::AccountAssociated { source, account, .. }
            if *source == tackt_primitives::Source::Ethereum && *account == wallet)
    );
}

#[tokio::test]
async fn custody_wallets_support_address_lookup_backfill_and_transaction_signing() {
    use base64::{Engine, engine::general_purpose::STANDARD};
    use ed25519_dalek::Verifier;
    use solana_message::{Message, MessageHeader, VersionedMessage};
    use solana_transaction::versioned::VersionedTransaction;
    use tackt_core_api::signing::{
        SigningError, backfill_wallet_addresses, evm, generate_user_signing_keys, hypercore, svm,
    };

    let Some(url) = gated_env("DATABASE_URL") else {
        return;
    };
    let repo = Arc::new(PgRepository::initialise(&url, 5).await.unwrap());
    let app = tackt_core_api::api::router(
        repo.clone(),
        TOKEN.into(),
        None,
        user_auth().await,
        false,
        None,
        tackt_core_api::prices::PriceCache::default(),
    );
    let mut keys = generate_user_signing_keys();
    let expected = keys.addresses.take().unwrap();
    let (user, _) = repo
        .create_core_user(
            tackt_primitives::CreateCoreUser {
                external_id: format!("legacy-wallet-{}", Uuid::new_v4()),
                metadata: json!({}),
            },
            &keys,
        )
        .await
        .unwrap();
    backfill_wallet_addresses(&repo).await.unwrap();
    backfill_wallet_addresses(&repo).await.unwrap();
    let persisted = repo.user_signing_keys(user.id).await.unwrap().unwrap();
    assert!(keys.ethereum_private_key == persisted.ethereum_private_key);
    assert!(keys.solana_private_key == persisted.solana_private_key);
    assert_eq!(persisted.addresses.as_ref(), Some(&expected));

    use tackt_postgres::WalletAddress;
    assert_eq!(
        repo.user_wallet_addresses(user.id).await.unwrap(),
        Some(expected.clone())
    );
    for address in [
        WalletAddress::Ethereum(&expected.ethereum),
        WalletAddress::Solana(&expected.solana),
    ] {
        assert_eq!(
            repo.user_id_by_wallet_address(address).await.unwrap(),
            Some(user.id)
        );
    }
    for path in [
        format!("/v1/users/{}/wallets", user.id),
        format!("/v1/wallets/ethereum/{}/user", expected.ethereum),
    ] {
        let (status, _) = request(&app, "GET", &path, Value::Null).await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "wallet lookup must remain internal"
        );
    }

    let ethereum_request = || evm::EthereumTransaction {
        chain_id: 11155111,
        nonce: 0,
        gas_limit: 21000,
        max_fee_per_gas: "2000000000".into(),
        max_priority_fee_per_gas: "1000000000".into(),
        to: Some("0x1111111111111111111111111111111111111111".into()),
        value: "1".into(),
        data: "0x".into(),
    };
    let signed = evm::sign(&repo, user.id, ethereum_request()).await.unwrap();
    assert_eq!(signed.address, expected.ethereum);
    assert!(signed.signed_transaction.starts_with("0x02"));
    assert!(matches!(
        evm::sign(&repo, Uuid::new_v4(), ethereum_request()).await,
        Err(SigningError::MissingKeys)
    ));

    let signed = hypercore::sign(
        &repo,
        user.id,
        hypercore::HypercoreTransaction {
            action: hypercore::Action::L1(hypercore::L1Action::Noop),
            network: hypercore::Network::Testnet,
            nonce: 1677777606040,
            vault_address: None,
            expires_after: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(signed.address, expected.ethereum);
    assert_eq!(signed.payload.nonce, 1677777606040);
    assert!(matches!(signed.payload.signature.v, 27 | 28));
    let (status, _) = request(
        &app,
        "POST",
        &format!("/v1/users/{}/signed-transactions", user.id),
        json!({}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "signing must not be exposed through HTTP"
    );

    let public = ed25519_dalek::SigningKey::from_bytes(&keys.solana_private_key).verifying_key();
    let unsigned = VersionedTransaction {
        signatures: vec![[0; 64].into()],
        message: VersionedMessage::Legacy(Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![public.to_bytes().into()],
            ..Default::default()
        }),
    };
    let signed = svm::sign(
        &repo,
        user.id,
        &STANDARD.encode(wincode::serialize(&unsigned).unwrap()),
    )
    .await
    .unwrap();
    assert_eq!(signed.address, expected.solana);
    let signed: VersionedTransaction =
        wincode::deserialize_exact(&STANDARD.decode(signed.signed_transaction).unwrap()).unwrap();
    public
        .verify(
            &wincode::serialize(&signed.message).unwrap(),
            &ed25519_dalek::Signature::from_slice(signed.signatures[0].as_ref()).unwrap(),
        )
        .unwrap();
    assert_eq!(signed.message, unsigned.message);
}

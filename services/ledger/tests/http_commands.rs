//! Core HTTP → committed Ledger transaction, including lost-response retry.

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header::AUTHORIZATION},
    response::Response,
};
use serde_json::{Value, json};
use std::sync::Arc;
use tackt_ledger::FinancialHttpConfig;
use tackt_ledger::Ledger;
use tackt_postgres::{PgRepository, test_support::TestDatabase};
use tackt_primitives::ledger::{BalanceBucket, BalanceTimeBasis, LedgerAsset, LedgerBalance};
use tower::ServiceExt;
use uuid::Uuid;

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

async fn environment() -> Option<(Arc<Ledger>, TestDatabase)> {
    let (database_url, database) = TestDatabase::create().await?;
    Some((
        Arc::new(Ledger::new(
            PgRepository::initialise(&database_url, 5)
                .await
                .expect("clean migrations"),
        )),
        database,
    ))
}

fn app(repository: Arc<Ledger>) -> Router {
    tackt_ledger::router(
        repository,
        || async { true },
        FinancialHttpConfig::new(TOKEN.into(), 16 * 1024, 64).unwrap(),
    )
}

fn header(tag: &str) -> Value {
    json!({
        "chain_id": 1,
        "effective_at": chrono::Utc::now(),
        "authorization": {
            "core_action_id": format!("action-{tag}"),
            "core_authorization_id": tag,
            "risk_evaluation_id": format!("evaluation-{tag}"),
            "reason": "integration test authorization"
        }
    })
}

fn custody_exit_header(action_id: Uuid, tag: &str) -> Value {
    json!({
        "chain_id": 1,
        "effective_at": chrono::Utc::now(),
        "authorization": {
            "core_action_id": action_id,
            "core_authorization_id": tag,
            "risk_evaluation_id": format!("evaluation-{tag}"),
            "reason": "integration test custody-exit authorization"
        }
    })
}

fn request(path: &str, key: &str, body: &Value) -> Request<Body> {
    Request::post(path)
        .header(AUTHORIZATION, format!("Bearer {TOKEN}"))
        .header("idempotency-key", key)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

async fn response_json(response: Response) -> Value {
    let bytes = axum::body::to_bytes(response.into_body(), 32 * 1024)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn balance(values: &[LedgerBalance], bucket: BalanceBucket) -> i128 {
    values
        .iter()
        .find(|value| value.asset == LedgerAsset::Usdc && value.bucket == bucket)
        .map_or(0, |value| value.quantity)
}

#[tokio::test]
async fn typed_http_commands_commit_once_and_reject_stably() {
    let Some((repository, database)) = environment().await else {
        return;
    };
    let app = app(repository.clone());
    let run = Uuid::new_v4().simple().to_string();
    let user = Uuid::new_v4();
    let draw_key = format!("http-draw-{run}");
    let draw = json!({
        "header": header(&draw_key),
        "user_id": user,
        "quantity": "100",
        "destination": "ethereum_available"
    });

    // Simulate a client losing the response after Ledger completed the commit.
    let lost = app
        .clone()
        .oneshot(request("/internal/v1/credit-draws", &draw_key, &draw))
        .await
        .unwrap();
    assert_eq!(lost.status(), StatusCode::OK);
    drop(lost);
    let after_lost = repository
        .balances(user, None, BalanceTimeBasis::Recorded)
        .await
        .unwrap();
    assert_eq!(balance(&after_lost, BalanceBucket::Available), 100);

    let replay = app
        .clone()
        .oneshot(request("/internal/v1/credit-draws", &draw_key, &draw))
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    let replay = response_json(replay).await;
    assert_eq!(replay["outcome"], "committed");
    assert_eq!(replay["replayed"], true);
    assert!(replay["transaction_id"].as_str().is_some());

    let rebound = app
        .clone()
        .oneshot(request(
            "/internal/v1/credit-draws",
            &format!("different-{draw_key}"),
            &draw,
        ))
        .await
        .unwrap();
    assert_eq!(rebound.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        response_json(rebound).await["error"]["code"],
        "invalid_idempotency_key"
    );
    let after_rebound = repository
        .balances(user, None, BalanceTimeBasis::Recorded)
        .await
        .unwrap();
    assert_eq!(balance(&after_rebound, BalanceBucket::Available), 100);

    let mut conflict = draw.clone();
    conflict["quantity"] = json!("101");
    let conflict = app
        .clone()
        .oneshot(request("/internal/v1/credit-draws", &draw_key, &conflict))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::CONFLICT);
    assert_eq!(
        response_json(conflict).await["code"],
        "idempotency_conflict"
    );

    let zero_key = format!("http-zero-{run}");
    let zero = json!({
        "header": header(&zero_key),
        "user_id": user,
        "quantity": "0",
        "destination": "ethereum_available"
    });
    let zero = app
        .clone()
        .oneshot(request("/internal/v1/credit-draws", &zero_key, &zero))
        .await
        .unwrap();
    assert_eq!(zero.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(response_json(zero).await["code"], "invalid_quantity");

    let order_key = format!("http-order-{run}");
    let order = json!({
        "header": header(&order_key),
        "user_id": user,
        "reservation_id": Uuid::new_v4(),
        "quantity": "40"
    });
    let order = app
        .clone()
        .oneshot(request(
            "/internal/v1/order-reservations",
            &order_key,
            &order,
        ))
        .await
        .unwrap();
    assert_eq!(order.status(), StatusCode::OK);

    let withdrawal_key = format!("http-withdrawal-{run}");
    let withdrawal = json!({
        "header": header(&withdrawal_key),
        "user_id": user,
        "reservation_id": Uuid::new_v4(),
        "asset": "USDC",
        "quantity": "60"
    });
    let withdrawal = app
        .clone()
        .oneshot(request(
            "/internal/v1/withdrawal-reservations",
            &withdrawal_key,
            &withdrawal,
        ))
        .await
        .unwrap();
    assert_eq!(withdrawal.status(), StatusCode::OK);

    let insufficient_key = format!("http-insufficient-{run}");
    let insufficient = json!({
        "header": header(&insufficient_key),
        "user_id": user,
        "reservation_id": Uuid::new_v4(),
        "quantity": "1"
    });
    let insufficient = app
        .oneshot(request(
            "/internal/v1/order-reservations",
            &insufficient_key,
            &insufficient,
        ))
        .await
        .unwrap();
    assert_eq!(insufficient.status(), StatusCode::CONFLICT);
    assert_eq!(
        response_json(insufficient).await["code"],
        "insufficient_funds"
    );

    let balances = repository
        .balances(user, None, BalanceTimeBasis::Recorded)
        .await
        .unwrap();
    assert_eq!(balance(&balances, BalanceBucket::Available), 0);
    assert_eq!(balance(&balances, BalanceBucket::OrderReserved), 40);
    assert_eq!(balance(&balances, BalanceBucket::WithdrawalReserved), 60);
    assert_eq!(balance(&balances, BalanceBucket::PrincipalLiability), -100);
    drop(repository);
    database.cleanup().await;
}

#[tokio::test]
async fn custody_exit_http_holds_credit_and_resolves_by_stored_destination() {
    let Some((repository, database)) = environment().await else {
        return;
    };
    let app = app(repository.clone());
    let user = Uuid::new_v4();

    let action = Uuid::new_v4();
    let create_key = format!("{action}:create");
    let create = json!({
        "header": custody_exit_header(action, &create_key),
        "user_id": user,
        "reservation_id": action,
        "quantity": "75",
        "destination": "hyperliquid_margin",
        "funding": "credit"
    });
    let created = app
        .clone()
        .oneshot(request(
            "/internal/v1/custody-exit-reservations",
            &create_key,
            &create,
        ))
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::OK);
    let balances = repository
        .balances(user, None, BalanceTimeBasis::Recorded)
        .await
        .unwrap();
    assert_eq!(balance(&balances, BalanceBucket::Available), 0);
    assert_eq!(balance(&balances, BalanceBucket::Reserved), 75);
    assert_eq!(balance(&balances, BalanceBucket::HyperliquidMargin), 0);
    assert_eq!(balance(&balances, BalanceBucket::PrincipalLiability), -75);

    let wrong_key = format!("{action}:wrong-destination");
    let wrong = json!({
        "header": custody_exit_header(action, &wrong_key),
        "user_id": user,
        "reservation_id": action,
        "asset": "USDC",
        "quantity": "75",
        "destination": "custody_outflow"
    });
    let wrong = app
        .clone()
        .oneshot(request(
            "/internal/v1/custody-exit-reservations/consume",
            &wrong_key,
            &wrong,
        ))
        .await
        .unwrap();
    assert_eq!(wrong.status(), StatusCode::CONFLICT);
    assert_eq!(response_json(wrong).await["code"], "command_conflict");

    let consume_key = format!("{action}:consume");
    let consume = json!({
        "header": custody_exit_header(action, &consume_key),
        "user_id": user,
        "reservation_id": action,
        "asset": "USDC",
        "quantity": "75",
        "destination": "hyperliquid_margin"
    });
    for replayed in [false, true] {
        let response = app
            .clone()
            .oneshot(request(
                "/internal/v1/custody-exit-reservations/consume",
                &consume_key,
                &consume,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response_json(response).await["replayed"], replayed);
    }
    let balances = repository
        .balances(user, None, BalanceTimeBasis::Recorded)
        .await
        .unwrap();
    assert_eq!(balance(&balances, BalanceBucket::Reserved), 0);
    assert_eq!(balance(&balances, BalanceBucket::HyperliquidMargin), 75);

    drop(repository);
    database.cleanup().await;
}

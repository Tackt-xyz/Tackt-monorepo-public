use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use axum::{Json, Router, extract::State, http::HeaderMap, routing::post};
use chrono::Utc;
use serde_json::{Value, json};
use tackt_core_api::orchestration::{
    ActionExecution, ActionOrchestrator, ActionProjectionProvider, CustodyFunding,
    ExecutionOutcome, FinancialHttpClient, FinancialHttpConfig, ProjectionError,
    RiskEvaluationRequest,
};
use tackt_postgres::{NewCoreAction, PgRepository};
use tackt_primitives::{
    CoreActionKind, CoreActionState, CreateCoreUser,
    accounts::AccountSubject,
    assets::AssetReference,
    ledger::{CustodyExitDestination, LedgerAsset},
    risk::{
        EvaluationReason, EvaluationVersions, RiskClassification, RiskEvaluation, RiskMeasurements,
    },
};
use uuid::Uuid;

const TOKEN: &str = "financial-integration-token-32-bytes";

struct FixedProjection {
    account: AccountSubject,
}

#[async_trait]
impl ActionProjectionProvider for FixedProjection {
    async fn execution_for(
        &self,
        action: &tackt_postgres::CoreAction,
    ) -> Result<ActionExecution, ProjectionError> {
        Ok(ActionExecution {
            risk: RiskEvaluationRequest::new(
                action.id,
                self.account,
                5,
                6,
                json!({
                    "ethereum_balances":[],
                    "hypercore_balances":[],
                    "margin_domains":[],
                    "perp_positions":[]
                }),
            )
            .expect("projection"),
            chain_id: 1,
            custody_funding: CustodyFunding::Available,
        })
    }
}

#[derive(Clone)]
struct LedgerState {
    calls: Arc<AtomicUsize>,
    keys: Arc<Mutex<Vec<String>>>,
    stall_first: bool,
}

async fn ledger(
    State(state): State<LedgerState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Json<Value> {
    assert!(
        body["header"]["authorization"]["risk_evaluation_id"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );
    let key = headers
        .get("idempotency-key")
        .expect("idempotency key")
        .to_str()
        .expect("ASCII key")
        .to_owned();
    state.keys.lock().expect("keys").push(key);
    let call = state.calls.fetch_add(1, Ordering::SeqCst);
    if call == 0 && state.stall_first {
        std::future::pending::<()>().await;
    }
    Json(json!({
        "outcome":"committed",
        "transaction_id":"22222222-2222-4222-8222-222222222222",
        "recorded_at":Utc::now(),
        "replayed":call > 0
    }))
}

async fn serve(app: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock");
    let address = listener.local_addr().expect("mock address");
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve mock");
    });
    (format!("http://{address}/"), task)
}

fn evaluation(action_id: Uuid, account: AccountSubject) -> RiskEvaluation {
    RiskEvaluation::new(
        action_id,
        account,
        [4; 32],
        RiskClassification::Healthy,
        EvaluationReason::AboveWarningThreshold,
        RiskMeasurements::new(1_000, 100, 10, 900, 90).expect("measurements"),
        0,
        EvaluationVersions {
            account_version: 5,
            input_version: 6,
            processing_epoch: 8,
            ethereum_price_sequence: Some(9),
            hyperliquid_price_sequence: Some(10),
        },
    )
    .expect("evaluation")
}

fn denied_evaluation(action_id: Uuid, account: AccountSubject) -> RiskEvaluation {
    RiskEvaluation::new(
        action_id,
        account,
        [5; 32],
        RiskClassification::Watch,
        EvaluationReason::HealthBelowWarning,
        RiskMeasurements::new(1_000, 100, 10, 900, 90).expect("measurements"),
        0,
        EvaluationVersions {
            account_version: 5,
            input_version: 6,
            processing_epoch: 8,
            ethereum_price_sequence: Some(9),
            hyperliquid_price_sequence: Some(10),
        },
    )
    .expect("denied evaluation")
}

async fn repository() -> Option<(Arc<PgRepository>, Uuid)> {
    let url = std::env::var("DATABASE_URL").ok().or_else(|| {
        assert!(
            std::env::var_os("CI").is_none(),
            "DATABASE_URL is unset in CI; refusing to skip action orchestration tests"
        );
        eprintln!("skipping: set DATABASE_URL to run action orchestration tests");
        None
    })?;
    let repo = Arc::new(PgRepository::initialise(&url, 5).await.expect("repository"));
    let external_id = format!("tac-34-{}", Uuid::new_v4());
    let (user, _) = repo
        .create_core_user(
            CreateCoreUser {
                external_id,
                metadata: json!({}),
            },
            &tackt_core_api::signing::generate_user_signing_keys(),
        )
        .await
        .expect("user");
    Some((repo, user.id))
}

#[tokio::test]
async fn timeout_retries_the_same_ledger_authorization_and_converges() {
    let Some((repo, user_id)) = repository().await else {
        return;
    };
    let action_id = Uuid::new_v4();
    repo.create_core_action(NewCoreAction {
        id: action_id,
        user_id,
        kind: CoreActionKind::CustodyExit(CustodyExitDestination::CustodyOutflow),
        asset: AssetReference::new(LedgerAsset::Usdc),
        quantity: 1_000_000,
        external_references: Default::default(),
    })
    .await
    .expect("action");
    let account = AccountSubject::new(user_id).expect("subject");
    let evaluation = evaluation(action_id, account);
    let expected_evaluation_id = evaluation.evaluation_id().to_owned();
    let risk_value = serde_json::to_value(evaluation).expect("risk JSON");
    let risk_calls = Arc::new(AtomicUsize::new(0));
    let risk_calls_for_server = risk_calls.clone();
    let (risk_url, risk_task) = serve(Router::new().route(
        "/internal/v1/risk/evaluations",
        post(move || {
            risk_calls_for_server.fetch_add(1, Ordering::SeqCst);
            let response = risk_value.clone();
            async move { Json(response) }
        }),
    ))
    .await;
    let ledger_state = LedgerState {
        calls: Arc::new(AtomicUsize::new(0)),
        keys: Arc::new(Mutex::new(Vec::new())),
        stall_first: true,
    };
    let (ledger_url, ledger_task) = serve(
        Router::new()
            .route("/internal/v1/custody-exit-reservations", post(ledger))
            .with_state(ledger_state.clone()),
    )
    .await;
    let config = FinancialHttpConfig::new(
        &risk_url,
        TOKEN.into(),
        &ledger_url,
        TOKEN.into(),
        Duration::from_millis(500),
    )
    .expect("config");
    let orchestrator = ActionOrchestrator::new(
        repo.clone(),
        FinancialHttpClient::new(config).expect("client"),
        Arc::new(FixedProjection { account }),
    );

    assert!(orchestrator.execute(action_id).await.is_err());
    assert_eq!(
        repo.core_action(action_id).await.expect("pending").state,
        CoreActionState::Pending
    );
    let outcome = orchestrator.execute(action_id).await.expect("replay");
    assert!(matches!(
        outcome,
        ExecutionOutcome::Reserved {
            state: CoreActionState::WaitingForSigning,
            replayed: true,
            ..
        }
    ));
    {
        let keys = ledger_state.keys.lock().expect("keys");
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], keys[1]);
    }
    let action = repo.core_action(action_id).await.expect("action");
    assert_eq!(
        action
            .evaluation_evidence
            .as_ref()
            .expect("evaluation")
            .evaluation_id,
        expected_evaluation_id
    );
    assert_eq!(
        repo.action_ledger_requests(action_id).await.unwrap().len(),
        1
    );
    assert_eq!(risk_calls.load(Ordering::SeqCst), 1);

    risk_task.abort();
    ledger_task.abort();
}

#[tokio::test]
async fn non_healthy_risk_fails_the_action_without_contacting_ledger() {
    let Some((repo, user_id)) = repository().await else {
        return;
    };
    let action_id = Uuid::new_v4();
    repo.create_core_action(NewCoreAction {
        id: action_id,
        user_id,
        kind: CoreActionKind::CustodyExit(CustodyExitDestination::CustodyOutflow),
        asset: AssetReference::new(LedgerAsset::Usdc),
        quantity: 1_000_000,
        external_references: Default::default(),
    })
    .await
    .expect("action");
    let account = AccountSubject::new(user_id).expect("subject");
    let risk_value =
        serde_json::to_value(denied_evaluation(action_id, account)).expect("risk JSON");
    let (risk_url, risk_task) = serve(Router::new().route(
        "/internal/v1/risk/evaluations",
        post(move || {
            let response = risk_value.clone();
            async move { Json(response) }
        }),
    ))
    .await;
    let ledger_state = LedgerState {
        calls: Arc::new(AtomicUsize::new(0)),
        keys: Arc::new(Mutex::new(Vec::new())),
        stall_first: false,
    };
    let (ledger_url, ledger_task) = serve(
        Router::new()
            .route("/internal/v1/custody-exit-reservations", post(ledger))
            .with_state(ledger_state.clone()),
    )
    .await;
    let config = FinancialHttpConfig::new(
        &risk_url,
        TOKEN.into(),
        &ledger_url,
        TOKEN.into(),
        Duration::from_secs(1),
    )
    .expect("config");
    let orchestrator = ActionOrchestrator::new(
        repo.clone(),
        FinancialHttpClient::new(config).expect("client"),
        Arc::new(FixedProjection { account }),
    );

    assert!(matches!(
        orchestrator.execute(action_id).await.expect("denied"),
        ExecutionOutcome::RiskDenied {
            state: CoreActionState::Failed,
            classification: RiskClassification::Watch
        }
    ));
    assert_eq!(ledger_state.calls.load(Ordering::SeqCst), 0);
    assert!(
        repo.core_action(action_id)
            .await
            .expect("action")
            .evaluation_evidence
            .is_some()
    );

    risk_task.abort();
    ledger_task.abort();
}

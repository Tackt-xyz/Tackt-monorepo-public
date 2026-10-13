use std::collections::BTreeMap;

use sqlx::{Executor, PgPool};
use tackt_postgres::{
    ActionLedgerStep, ActionRepositoryError, ActionTransitionRequest, CoreActionKind,
    CoreActionState, NewCoreAction, PgRepository,
};
use tackt_primitives::{
    CreateCoreUser,
    accounts::AccountSubject,
    assets::AssetReference,
    ledger::{CustodyExitDestination, LedgerAsset},
    risk::{
        EvaluationReason, EvaluationVersions, RecommendationReason, RiskClassification,
        RiskEvaluation, RiskMeasurements, RiskRecommendation,
    },
};
use url::Url;
use uuid::Uuid;

struct TestDatabase {
    base_url: String,
    name: String,
    armed: bool,
}

impl TestDatabase {
    async fn cleanup(mut self, admin: &PgPool) {
        admin
            .execute(format!("DROP DATABASE {} WITH (FORCE)", self.name).as_str())
            .await
            .expect("drop test database");
        self.armed = false;
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let base = self.base_url.clone();
        let name = self.name.clone();
        let _ = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("cleanup runtime");
            runtime.block_on(async move {
                if let Ok(pool) = PgPool::connect(&base).await {
                    let _ = pool
                        .execute(format!("DROP DATABASE {name} WITH (FORCE)").as_str())
                        .await;
                }
            });
        })
        .join();
    }
}

async fn database() -> Option<(String, PgPool, TestDatabase)> {
    let Ok(base) =
        std::env::var("DATABASE_URL").or_else(|_| std::env::var("CORE_API_TEST_DATABASE_URL"))
    else {
        assert!(
            std::env::var_os("CI").is_none(),
            "DATABASE_URL is unset in CI; refusing to skip Core action integration tests"
        );
        eprintln!("skipping: set DATABASE_URL to run PostgreSQL integration tests");
        return None;
    };
    let admin = PgPool::connect(&base).await.expect("connect test database");
    let name = format!("tackt_actions_test_{}", Uuid::new_v4().simple());
    admin
        .execute(format!("CREATE DATABASE {name}").as_str())
        .await
        .expect("create isolated test database");
    let mut url = Url::parse(&base).expect("database URL");
    url.set_path(&format!("/{name}"));
    Some((
        url.to_string(),
        admin,
        TestDatabase {
            base_url: base,
            name,
            armed: true,
        },
    ))
}

async fn user(repo: &PgRepository, external_id: &str) -> Uuid {
    repo.create_core_user(
        CreateCoreUser {
            external_id: external_id.into(),
            metadata: serde_json::json!({}),
        },
        &test_signing_keys(),
    )
    .await
    .expect("create user")
    .0
    .id
}

/// The transition rule intentionally exists in both primitives and PostgreSQL: primitives rejects
/// bad requests early, while the live constraint protects the append-only audit log from every
/// writer. Exercise the migrated catalog rather than migration text so deployed-schema drift fails.
async fn live_transition_constraint_matches_primitives(
    repo: &PgRepository,
    pool: &PgPool,
    user_id: Uuid,
) {
    let action_id = Uuid::new_v4();
    repo.create_core_action(action(user_id, action_id, CoreActionKind::VenueOrder))
        .await
        .expect("create matrix action");

    for from in std::iter::once(None).chain(CoreActionState::ALL.map(Some)) {
        for to in CoreActionState::ALL {
            let result = sqlx::query(
                "INSERT INTO core_action_transitions \
                 (transition_id,action_id,from_state,to_state,reason) \
                 VALUES($1,$2,$3::core_action_state,$4::core_action_state,'constraint parity')",
            )
            .bind(Uuid::new_v4())
            .bind(action_id)
            .bind(from.map(CoreActionState::as_str))
            .bind(to.as_str())
            .execute(pool)
            .await;
            let expected = from.map_or(to == CoreActionState::Pending, |state| {
                state.can_transition_to(to)
            });
            match (expected, result) {
                (true, Ok(done)) => assert_eq!(done.rows_affected(), 1),
                (false, Err(error)) => {
                    let database_error = error
                        .as_database_error()
                        .expect("invalid transition must fail at the database constraint");
                    assert_eq!(database_error.code().as_deref(), Some("23514"));
                    assert_eq!(
                        database_error.constraint(),
                        Some("core_action_transitions_state_transition_valid")
                    );
                }
                (true, Err(error)) => {
                    panic!("live transition constraint rejected valid {from:?} -> {to:?}: {error}")
                }
                (false, Ok(_)) => {
                    panic!("live transition constraint accepted invalid {from:?} -> {to:?}")
                }
            }
        }
    }
}

async fn active_scan_matches_primitives(repo: &PgRepository, pool: &PgPool, user_id: Uuid) {
    let mut matrix_ids = Vec::new();
    let mut expected = Vec::new();
    for state in CoreActionState::ALL {
        let id = Uuid::new_v4();
        matrix_ids.push(id);
        repo.create_core_action(action(user_id, id, CoreActionKind::VenueOrder))
            .await
            .expect("create active-state matrix action");
        sqlx::query("UPDATE core_actions SET state=$2::core_action_state WHERE id=$1")
            .bind(id)
            .bind(state.as_str())
            .execute(pool)
            .await
            .expect("set matrix action state");
        if state.is_active() {
            expected.push(id);
        }
    }

    let mut actual = repo
        .active_core_actions(200, None)
        .await
        .expect("scan active-state matrix")
        .into_iter()
        .map(|action| action.id)
        .filter(|id| matrix_ids.contains(id))
        .collect::<Vec<_>>();
    expected.sort_unstable();
    actual.sort_unstable();
    assert_eq!(actual, expected);
}

fn action(user_id: Uuid, id: Uuid, kind: CoreActionKind) -> NewCoreAction {
    NewCoreAction {
        id,
        user_id,
        kind,
        asset: AssetReference::new(LedgerAsset::Usdc),
        quantity: 1_000_000,
        external_references: BTreeMap::from([("client_request".into(), id.to_string())]),
    }
}

fn transition(
    action_id: Uuid,
    transition_id: Uuid,
    expected_state: CoreActionState,
    next_state: CoreActionState,
) -> ActionTransitionRequest {
    ActionTransitionRequest {
        transition_id,
        action_id,
        expected_state,
        next_state,
        reason: "integration test".into(),
        risk_evidence: None,
        external_references: BTreeMap::new(),
    }
}

#[tokio::test]
async fn actions_are_durable_serialized_and_idempotent() {
    let Some((url, admin, database)) = database().await else {
        return;
    };
    let repo = PgRepository::initialise(&url, 8)
        .await
        .expect("clean migrations");
    PgRepository::initialise(&url, 2)
        .await
        .expect("repeated initialization");
    let user_id = user(&repo, "actions-user").await;

    let first_id = Uuid::new_v4();
    let second_id = Uuid::new_v4();
    let first = action(
        user_id,
        first_id,
        CoreActionKind::CustodyExit(CustodyExitDestination::CustodyOutflow),
    );
    let second = action(
        user_id,
        second_id,
        CoreActionKind::CustodyExit(CustodyExitDestination::HyperliquidMargin),
    );
    let (left, right) = tokio::join!(
        repo.create_core_action(first.clone()),
        repo.create_core_action(second)
    );
    let (created, blocked_id) = match (left, right) {
        (Ok((action, true)), Err(ActionRepositoryError::ActiveCustodyExit)) => (action, second_id),
        (Err(ActionRepositoryError::ActiveCustodyExit), Ok((action, true))) => (action, first_id),
        results => panic!("expected exactly one active custody exit, got {results:?}"),
    };
    let (duplicate, inserted) = repo
        .create_core_action(action(user_id, created.id, created.kind))
        .await
        .expect("replay create");
    assert!(!inserted);
    assert_eq!(duplicate.id, created.id);

    let risk = RiskRecommendation::new(
        AccountSubject::new(user_id).expect("account subject"),
        RiskClassification::Healthy,
        RiskClassification::Watch,
        RecommendationReason::HealthBelowWarning,
        RiskMeasurements::new(100, 0, 50, 1_100_000, 1_100_000).expect("measurements"),
        3,
        4,
    )
    .expect("risk recommendation");
    let transition_id = Uuid::new_v4();
    let mut hold_committed = transition(
        created.id,
        transition_id,
        CoreActionState::Pending,
        CoreActionState::WaitingForSigning,
    );
    hold_committed.risk_evidence = Some(risk.clone());
    hold_committed.external_references =
        BTreeMap::from([("ledger_transaction".into(), "ledger-1".into())]);
    let transitioned = repo
        .transition_core_action(hold_committed.clone())
        .await
        .expect("legal transition");
    assert!(!transitioned.duplicate);
    assert_eq!(
        transitioned
            .action
            .risk_evidence
            .as_ref()
            .map(|evidence| evidence.recommendation_id.as_str()),
        Some(risk.recommendation_id())
    );
    assert_eq!(
        transitioned
            .action
            .external_references
            .get("ledger_transaction")
            .map(String::as_str),
        Some("ledger-1")
    );
    assert!(
        repo.transition_core_action(hold_committed)
            .await
            .expect("transition replay")
            .duplicate
    );
    assert!(matches!(
        repo.transition_core_action(transition(
            created.id,
            Uuid::new_v4(),
            CoreActionState::Pending,
            CoreActionState::Failed,
        ))
        .await,
        Err(ActionRepositoryError::StaleTransition {
            expected: CoreActionState::Pending,
            actual: CoreActionState::WaitingForSigning,
        })
    ));

    let ledger_request_id = Uuid::new_v4();
    let (ledger_request, inserted) = repo
        .record_action_ledger_request(
            ledger_request_id,
            created.id,
            ActionLedgerStep::ReserveCustodyExit,
            format!("{}:create-hold", created.id),
        )
        .await
        .expect("record Ledger request");
    assert!(inserted);
    assert_eq!(ledger_request.request_id, ledger_request_id);
    assert!(
        !repo
            .record_action_ledger_request(
                ledger_request_id,
                created.id,
                ActionLedgerStep::ReserveCustodyExit,
                format!("{}:create-hold", created.id),
            )
            .await
            .expect("replay Ledger request")
            .1
    );

    let restarted = PgRepository::initialise(&url, 3)
        .await
        .expect("restart repository");
    assert_eq!(
        restarted
            .core_action(created.id)
            .await
            .expect("load after restart")
            .state,
        CoreActionState::WaitingForSigning
    );
    assert_eq!(
        restarted
            .core_action_transitions(created.id)
            .await
            .expect("history")
            .len(),
        2
    );
    assert_eq!(
        restarted
            .action_ledger_requests(created.id)
            .await
            .expect("Ledger requests")
            .len(),
        1
    );
    let restartable = restarted
        .active_core_actions(200, None)
        .await
        .expect("active recovery scan");
    assert_eq!(restartable.len(), 1);
    assert_eq!(restartable[0].id, created.id);

    let externally_pending = restarted
        .transition_core_action(transition(
            created.id,
            Uuid::new_v4(),
            CoreActionState::WaitingForSigning,
            CoreActionState::ExternallyPending,
        ))
        .await
        .expect("external submission")
        .action;
    assert_eq!(externally_pending.state, CoreActionState::ExternallyPending);
    restarted
        .transition_core_action(transition(
            created.id,
            Uuid::new_v4(),
            CoreActionState::ExternallyPending,
            CoreActionState::Succeeded,
        ))
        .await
        .expect("terminal success");
    assert!(
        restarted
            .active_core_actions(200, None)
            .await
            .expect("terminal actions excluded")
            .is_empty()
    );
    let venue_order_id = Uuid::new_v4();
    restarted
        .create_core_action(action(user_id, venue_order_id, CoreActionKind::VenueOrder))
        .await
        .expect("create venue order");
    assert!(matches!(
        restarted
            .record_action_ledger_request(
                Uuid::new_v4(),
                venue_order_id,
                ActionLedgerStep::ReserveCustodyExit,
                format!("{venue_order_id}:invalid-hold"),
            )
            .await,
        Err(ActionRepositoryError::NotCustodyExit)
    ));
    let mut invalid_hyperliquid_exit = action(
        user_id,
        Uuid::new_v4(),
        CoreActionKind::CustodyExit(CustodyExitDestination::HyperliquidMargin),
    );
    invalid_hyperliquid_exit.asset = AssetReference::new(LedgerAsset::Eth);
    assert!(matches!(
        restarted
            .create_core_action(invalid_hyperliquid_exit)
            .await,
        Err(ActionRepositoryError::InvalidInput(message))
            if message == "Hyperliquid custody exits require USDC"
    ));
    restarted
        .create_core_action(action(
            user_id,
            blocked_id,
            CoreActionKind::CustodyExit(CustodyExitDestination::HyperliquidMargin),
        ))
        .await
        .expect("terminal action releases active slot");

    database.cleanup(&admin).await;
}

#[tokio::test]
async fn database_action_invariants_match_primitives() {
    let Some((url, admin, database)) = database().await else {
        return;
    };
    let repo = PgRepository::initialise(&url, 4)
        .await
        .expect("clean migrations");
    let user_id = user(&repo, "action-invariants-user").await;
    let pool = PgPool::connect(&url)
        .await
        .expect("connect migrated database");

    live_transition_constraint_matches_primitives(&repo, &pool, user_id).await;
    active_scan_matches_primitives(&repo, &pool, user_id).await;

    pool.close().await;
    database.cleanup(&admin).await;
}

#[tokio::test]
async fn synchronous_evaluation_round_trips_unbounded_i128_health() {
    let Some((url, admin, database)) = database().await else {
        return;
    };
    let repo = PgRepository::initialise(&url, 4)
        .await
        .expect("clean migrations");
    let user_id = user(&repo, "evaluation-sentinel-user").await;
    let action_id = Uuid::new_v4();
    repo.create_core_action(action(
        user_id,
        action_id,
        CoreActionKind::CustodyExit(CustodyExitDestination::CustodyOutflow),
    ))
    .await
    .expect("create action");
    let evaluation = RiskEvaluation::new(
        action_id,
        AccountSubject::new(user_id).expect("account"),
        [7; 32],
        RiskClassification::Healthy,
        EvaluationReason::AboveWarningThreshold,
        RiskMeasurements::new(1_000, 0, 0, i128::MAX, i128::MAX).expect("measurements"),
        0,
        EvaluationVersions {
            account_version: 5,
            input_version: 6,
            processing_epoch: 7,
            ethereum_price_sequence: None,
            hyperliquid_price_sequence: None,
        },
    )
    .expect("evaluation");

    let (stored, inserted) = repo
        .record_action_evaluation(action_id, &evaluation)
        .await
        .expect("persist sentinel evaluation");
    assert!(inserted);
    assert_eq!(
        stored
            .evaluation_evidence
            .expect("stored evidence")
            .evaluation,
        evaluation
    );

    database.cleanup(&admin).await;
}

#[test]
fn illegal_transitions_fail_before_database_access() {
    let request = transition(
        Uuid::new_v4(),
        Uuid::new_v4(),
        CoreActionState::Pending,
        CoreActionState::Succeeded,
    );
    // The validation rule is also mirrored by the migration constraint.
    assert!(!CoreActionState::Pending.can_transition_to(request.next_state));
}

fn test_signing_keys() -> tackt_postgres::UserSigningKeys {
    fn random_bytes() -> [u8; 32] {
        let mut bytes = [0; 32];
        bytes[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        bytes[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        bytes
    }
    tackt_postgres::UserSigningKeys {
        addresses: None,
        ethereum_private_key: random_bytes(),
        solana_private_key: random_bytes(),
    }
}

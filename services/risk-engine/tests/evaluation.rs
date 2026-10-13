use risk_engine::Error;
use risk_engine::processing::{
    AccountMode, Amount, AssetRegistry, EvaluationInput, Factor, Finality, MAX_EVALUATION_RECORDS,
    MarginMode, ProjectedAccount, ProjectedEthereumBalance, ProjectedHyperliquidSnapshot,
    ProjectedMarginDomain, RiskPolicy, RiskProcessor, Value,
};
use std::collections::BTreeMap;
use tackt_primitives::accounts::AccountSubject;
use tackt_primitives::risk::RiskClassification;
use uuid::Uuid;

fn subject(value: u128) -> AccountSubject {
    AccountSubject::new(Uuid::from_u128(value)).expect("non-nil subject")
}

fn registry() -> AssetRegistry {
    AssetRegistry::bundled().expect("bundled registry")
}

fn processor() -> RiskProcessor {
    let registry = registry();
    let generation = risk_engine::processing::PortfolioGeneration::empty(5)
        .with_account_subjects([(0, subject(1))].into_iter().collect());
    RiskProcessor::new(
        1,
        1,
        RiskPolicy::new(Factor::from_raw(1_200_000), Factor::from_raw(1_000_000)).expect("policy"),
        registry,
        &generation,
    )
    .expect("processor")
}

fn processor_with_subject_at(account: u32) -> RiskProcessor {
    let registry = registry();
    let mut subjects = BTreeMap::new();
    for index in 0..=2 {
        let assigned = if index == account {
            subject(1)
        } else {
            subject(u128::from(index) + 10)
        };
        subjects.insert(index, assigned);
    }
    let generation =
        risk_engine::processing::PortfolioGeneration::empty(5).with_account_subjects(subjects);
    RiskProcessor::new(
        3,
        1,
        RiskPolicy::new(Factor::from_raw(1_200_000), Factor::from_raw(1_000_000)).expect("policy"),
        registry,
        &generation,
    )
    .expect("processor")
}

fn request(snapshot: Option<(i128, i128)>) -> EvaluationInput {
    let _registry = registry();
    EvaluationInput {
        action_id: Uuid::from_u128(9),
        account: subject(1),
        account_version: 5,
        input_version: 7,
        projected_account: ProjectedAccount {
            margin_domains: vec![ProjectedMarginDomain {
                domain: 1,
                account_mode: AccountMode::Unified,
                margin_mode: MarginMode::Cross,
                snapshot: snapshot.map(|(account_value, maintenance)| {
                    ProjectedHyperliquidSnapshot {
                        account_value: Value::from_raw(account_value),
                        maintenance: Value::from_raw(maintenance),
                        source_sequence: 1,
                    }
                }),
                source_version: 1,
            }],
            ..ProjectedAccount::default()
        },
    }
}

#[test]
fn evaluation_is_deterministic_read_only_and_covers_the_taxonomy() {
    let processor = processor();
    let before = processor.account(0).expect("snapshot");
    for (snapshot, expected) in [
        (Some((200, 100)), RiskClassification::Healthy),
        (Some((110, 100)), RiskClassification::Watch),
        (Some((90, 100)), RiskClassification::Liquidatable),
        (None, RiskClassification::DataIncomplete),
    ] {
        let first = processor.evaluate(request(snapshot)).expect("evaluation");
        let second = processor.evaluate(request(snapshot)).expect("repeat");
        assert_eq!(first, second);
        assert_eq!(first.classification(), expected);
        assert_eq!(first.versions().account_version, 5);
        assert_eq!(first.versions().input_version, 7);
        assert_eq!(first.uncertain_inputs() > 0, snapshot.is_none());
        let round_trip: tackt_primitives::risk::RiskEvaluation =
            serde_json::from_slice(&serde_json::to_vec(&first).expect("serialize evaluation"))
                .expect("validated evaluation round trip");
        assert_eq!(round_trip, first);
    }
    assert_eq!(processor.account(0).expect("unchanged snapshot"), before);
    assert_eq!(processor.last_committed_epoch(), 0);
}

#[test]
fn evaluation_fails_closed_on_identity_and_version_mismatches() {
    let processor = processor();

    let mut candidate = request(Some((200, 100)));
    candidate.action_id = Uuid::nil();
    assert_eq!(
        processor.evaluate(candidate),
        Err(Error::InvalidEvaluationActionId)
    );

    let mut candidate = request(Some((200, 100)));
    candidate.account = subject(2);
    assert!(matches!(
        processor.evaluate(candidate),
        Err(Error::UnknownAccountSubject { .. })
    ));

    let mut candidate = request(Some((200, 100)));
    candidate.account_version -= 1;
    assert!(matches!(
        processor.evaluate(candidate),
        Err(Error::EvaluationAccountVersionMismatch { .. })
    ));

    let mut candidate = request(Some((200, 100)));
    candidate.input_version = 0;
    assert_eq!(
        processor.evaluate(candidate),
        Err(Error::InvalidEvaluationInputVersion)
    );
}

#[test]
fn projection_order_does_not_change_the_evaluation() {
    let processor = processor();
    let mut first = request(Some((200, 100)));
    first
        .projected_account
        .margin_domains
        .push(ProjectedMarginDomain {
            domain: 2,
            account_mode: AccountMode::Unified,
            margin_mode: MarginMode::Cross,
            snapshot: Some(ProjectedHyperliquidSnapshot {
                account_value: Value::from_raw(300),
                maintenance: Value::from_raw(100),
                source_sequence: 2,
            }),
            source_version: 2,
        });
    let mut reordered = first.clone();
    reordered.projected_account.margin_domains.reverse();

    let first = processor.evaluate(first).expect("evaluation");
    let reordered = processor.evaluate(reordered).expect("reordered evaluation");
    assert_eq!(first, reordered);
}

#[test]
fn evaluation_evidence_is_independent_of_private_account_index() {
    let candidate = request(Some((200, 100)));
    let at_zero = processor_with_subject_at(0)
        .evaluate(candidate.clone())
        .expect("evaluation at index zero");
    let at_two = processor_with_subject_at(2)
        .evaluate(candidate)
        .expect("evaluation at index two");

    assert_eq!(at_zero.input_digest(), at_two.input_digest());
    assert_eq!(at_zero.evaluation_id(), at_two.evaluation_id());
}

#[test]
fn evaluation_evidence_binds_the_input_revision() {
    let processor = processor();
    let version_one = request(Some((200, 100)));
    let mut version_two = version_one.clone();
    version_two.input_version += 1;

    let version_one = processor.evaluate(version_one).expect("version one");
    let version_two = processor.evaluate(version_two).expect("version two");
    assert_ne!(version_one.input_digest(), version_two.input_digest());
    assert_ne!(version_one.evaluation_id(), version_two.evaluation_id());
}

#[test]
fn evaluation_record_count_is_bounded_before_compilation() {
    let processor = processor();
    let mut candidate = request(None);
    candidate.projected_account = ProjectedAccount::default();
    candidate.projected_account.ethereum_balances = vec![
        ProjectedEthereumBalance {
            asset: 0,
            amount: Amount::from_raw(1),
            finality: Finality::Finalized,
            position: 0,
            source_version: 1,
        };
        MAX_EVALUATION_RECORDS + 1
    ];
    assert_eq!(
        processor.evaluate(candidate),
        Err(Error::EvaluationRecordLimit {
            received: MAX_EVALUATION_RECORDS + 1,
            maximum: MAX_EVALUATION_RECORDS,
        })
    );
}

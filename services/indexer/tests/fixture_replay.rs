use tackt_indexer::{
    MemoryObservationSink, MemoryRepository, ObservationSink, Repository, model::*,
};
use tackt_nats::{ObservationEvent, PublishOutcome};
use uuid::Uuid;

fn fixture() -> Vec<Observation> {
    serde_json::from_str(include_str!("fixtures/observations.json")).unwrap()
}

/// The durable publish is the Indexer's only record of an observation; replaying a
/// source must resolve to the identities already in the stream, not new records.
#[tokio::test]
async fn fixture_replay_is_deterministic_and_idempotent() {
    let sink = MemoryObservationSink::default();
    for observation in fixture() {
        if observation.source == Source::Ethereum {
            let mut header = observation.clone();
            header.event_id = "fixture_block".into();
            header.kind = EventKind::EthereumBlock;
            header.account = None;
            let tip = header.block_ref().unwrap();
            let event = ObservationEvent::with_batch(
                header,
                tackt_nats::EvmBatch {
                    chain_id: 1,
                    previous_sequence: 0,
                    finalized: tip.clone(),
                    history: vec![tip],
                    revert_from: None,
                    observations: vec![observation],
                },
            );
            assert_eq!(sink.publish_batch(&event).await.unwrap(), 1);
            assert_eq!(sink.publish_batch(&event).await.unwrap(), 1);
        } else {
            let event = ObservationEvent::new(observation);
            assert_eq!(sink.publish(&event).await.unwrap(), PublishOutcome::Stored);
            assert_eq!(
                sink.publish(&event).await.unwrap(),
                PublishOutcome::Duplicate
            );
        }
    }
    assert_eq!(sink.published().len(), 2);
}

#[tokio::test]
async fn only_registered_user_accounts_are_attributable() {
    let repo = MemoryRepository::default();
    let u = User {
        id: Uuid::new_v4(),
        external_id: "tester-1".into(),
        enabled: true,
        ethereum_addresses: vec!["0xabc".into()],
        hyperliquid_accounts: vec!["0xdef".into()],
    };
    repo.insert_user(u.clone()).await;
    assert_eq!(
        repo.user_by_account(Source::Ethereum, "0xabc")
            .await
            .unwrap(),
        Some(u.id)
    );
    assert_eq!(
        repo.user_by_account(Source::Ethereum, "0xunknown")
            .await
            .unwrap(),
        None
    );
}

#[test]
fn normalized_ids_are_stable_and_kind_scoped() {
    let a = Observation::stable_id(
        &Source::Ethereum,
        &EventKind::EthTransfer,
        &["1", "0xtx", "0"],
    );
    let b = Observation::stable_id(
        &Source::Ethereum,
        &EventKind::EthTransfer,
        &["1", "0xtx", "0"],
    );
    let other = Observation::stable_id(
        &Source::Ethereum,
        &EventKind::Erc20Transfer,
        &["1", "0xtx", "0"],
    );
    assert_eq!(a, b);
    assert_ne!(a, other);
}

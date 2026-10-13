use chrono::Utc;
use serde_json::json;
use tackt_ledger::Ledger;
use tackt_nats::ObservationEvent;
use tackt_postgres::{PgRepository, Repository, test_support::TestDatabase};
use tackt_primitives::{
    Observation, Source,
    ledger::{BalanceBucket, BalanceTimeBasis, LedgerAsset},
};
use uuid::Uuid;

fn transfer(user: Uuid, id: &str, quantity: &str) -> Observation {
    serde_json::from_value(json!({
        "event_id":id,"source":"ethereum","kind":"eth_transfer",
        "account":tackt_primitives::accounts::SourceAccount::new(
            tackt_primitives::accounts::AccountSubject::new(user).unwrap(), Source::Ethereum,
            "0x1111111111111111111111111111111111111111").unwrap(),
        "asset":tackt_primitives::assets::AssetReference::new(LedgerAsset::Eth),
        "quantity":quantity,"source_timestamp":Utc::now(),"observed_at":Utc::now(),
        "block_number":100,"block_hash":"old","confirmations":1,"canonical":true,"finalized":false,
        "raw":{"from":"0x2222222222222222222222222222222222222222","to":"0x1111111111111111111111111111111111111111"},
        "provenance":{"endpoint":"fixture","method":"fixture","source_id":id}
    })).unwrap()
}

fn batch(
    previous_sequence: u64,
    hash: &str,
    revert_from: Option<u64>,
    facts: Vec<Observation>,
) -> ObservationEvent {
    use tackt_primitives::chain::BlockRef;
    let mut block = transfer(Uuid::new_v4(), &format!("obs_block_{hash}"), "0x0");
    block.kind = tackt_primitives::EventKind::EthereumBlock;
    block.account = None;
    block.asset = None;
    block.quantity = None;
    block.block_hash = Some(hash.into());
    let finalized = BlockRef {
        number: 99,
        hash: "anchor".into(),
    };
    ObservationEvent::with_batch(
        block,
        tackt_nats::EvmBatch {
            chain_id: 1,
            previous_sequence,
            finalized: finalized.clone(),
            history: vec![
                finalized,
                BlockRef {
                    number: 100,
                    hash: hash.into(),
                },
            ],
            revert_from,
            observations: facts,
        },
    )
}

/// One complete EVM batch on the fixture stream.
async fn observe_transfer(
    ledger: &Ledger,
    sequence: u64,
    observation: &Observation,
) -> anyhow::Result<()> {
    ledger
        .observe(
            "fixture",
            sequence,
            &batch(sequence - 1, "old", None, vec![observation.clone()]),
        )
        .await
}

#[tokio::test]
async fn batches_rebook_old_hashes_without_duplicate_credit() {
    let Some((url, database)) = TestDatabase::create().await else {
        return;
    };
    let storage = PgRepository::initialise(&url, 4).await.unwrap();
    let ledger = Ledger::new(storage.clone());
    let user = Uuid::new_v4();
    let a = transfer(user, "obs_a", "0x7");
    observe_transfer(&ledger, 1, &a).await.unwrap();
    // A failed finalization retains the pending phase plus review detail.
    let mut write = storage.begin_ledger_write().await.unwrap();
    let pending = write.deposit(&a.event_id).await.unwrap().unwrap();
    write
        .store_deposit(&pending, Some("finalization needs review"))
        .await
        .unwrap();
    write.commit().await.unwrap();
    // A repeated fact in another batch must not credit it twice.
    ledger
        .observe("fixture", 2, &batch(1, "old", None, vec![a.clone()]))
        .await
        .unwrap();
    assert_eq!(ledger.export().await.unwrap().transactions.len(), 1);
    let mut b = transfer(user, "obs_b", "0x9");
    b.block_hash = Some("b".into());
    ledger
        .observe("fixture", 3, &batch(2, "b", Some(100), vec![b.clone()]))
        .await
        .unwrap();
    let back = batch(3, "old", Some(100), vec![a.clone()]);
    ledger.observe("fixture", 4, &back).await.unwrap();
    ledger.observe("fixture", 4, &back).await.unwrap();
    assert_eq!(ledger.export().await.unwrap().transactions.len(), 5);
    let balance = ledger
        .balances(user, None, BalanceTimeBasis::Recorded)
        .await
        .unwrap();
    assert_eq!(
        balance
            .iter()
            .find(|b| b.bucket == BalanceBucket::Pending)
            .unwrap()
            .quantity,
        7
    );
    let facts = storage.observations(user, None, 10).await.unwrap();
    assert!(
        facts
            .iter()
            .find(|o| o.event_id == a.event_id)
            .unwrap()
            .canonical
    );
    assert!(
        !facts
            .iter()
            .find(|o| o.event_id == b.event_id)
            .unwrap()
            .canonical
    );
    let mut write = storage.begin_ledger_write().await.unwrap();
    let state = write.chain_state(1).await.unwrap().unwrap();
    assert_eq!(state.tip.hash, "old");
    assert_eq!(state.sequence, 4);
    drop(write);
    // A stale update under a new delivery sequence must not reverse/rebook again.
    assert!(ledger.observe("fixture", 5, &back).await.is_err());
    assert_eq!(storage.observation_sequence("fixture").await.unwrap(), 4);
    database.cleanup().await;
}

#[tokio::test]
async fn block_storage_failure_and_failed_reversal_leave_cursor_and_facts_unchanged() {
    use tackt_ledger::LedgerRepository;
    use tackt_primitives::ledger::{AuditMetadata, CommandHeader, GainLoss, LedgerCommand};
    let Some((url, database)) = TestDatabase::create().await else {
        return;
    };
    let storage = PgRepository::initialise(&url, 4).await.unwrap();
    let ledger = Ledger::new(storage.clone());
    let user = Uuid::new_v4();
    let good = transfer(user, "obs_good", "0x7");
    let mut bad = transfer(user, "obs_bad", "0x1");
    bad.block_number = Some(u64::MAX);
    assert!(
        ledger
            .observe(
                "fixture",
                1,
                &batch(0, "old", None, vec![good.clone(), bad])
            )
            .await
            .is_err()
    );
    assert_eq!(storage.observation_sequence("fixture").await.unwrap(), 0);
    assert!(
        storage
            .observations(user, None, 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(ledger.export().await.unwrap().transactions.is_empty());
    ledger
        .observe("fixture", 1, &batch(0, "old", None, vec![good]))
        .await
        .unwrap();

    // Fault injection through an explicitly approved adjustment, not a deposit lifecycle command.
    // The observation still records pending, so its required reversal must fail closed.
    ledger
        .execute(LedgerCommand::ControlledAdjustment {
            header: CommandHeader {
                chain_id: 1,
                idempotency_key: "fixture-drain".into(),
                external_reference: None,
                effective_at: Utc::now(),
                audit: AuditMetadata {
                    actor: "test".into(),
                    reason: "reversal failure fixture".into(),
                    risk_snapshot_id: None,
                    authorization: None,
                    attributes: Default::default(),
                },
            },
            user_id: user,
            asset: LedgerAsset::Eth,
            quantity: 7,
            bucket: BalanceBucket::Pending,
            direction: GainLoss::Loss,
            approval_reference: "test-fault-injection".into(),
        })
        .await
        .unwrap();
    let before = ledger.export().await.unwrap().transactions.len();
    assert!(
        ledger
            .observe("fixture", 2, &batch(1, "replacement", Some(100), vec![]))
            .await
            .is_err()
    );
    assert_eq!(storage.observation_sequence("fixture").await.unwrap(), 1);
    assert_eq!(ledger.export().await.unwrap().transactions.len(), before);
    assert!(storage.observations(user, None, 10).await.unwrap()[0].canonical);
    database.cleanup().await;
}

#[tokio::test]
async fn durable_restart_preserves_cursor_and_rejects_recreated_stream_or_old_database() {
    let Ok(nats_url) = std::env::var("NATS_URL") else {
        assert!(
            std::env::var_os("CI").is_none(),
            "NATS_URL is required in CI"
        );
        eprintln!("skipping: NATS_URL must point at the Compose broker");
        return;
    };
    let Some((url, database)) = TestDatabase::create().await else {
        return;
    };
    let storage = PgRepository::initialise(&url, 4).await.unwrap();
    let ledger = Ledger::new(storage.clone());
    let client = tackt_nats::initialise(&nats_url).await.unwrap();
    let feed =
        tackt_nats::ObservationFeed::isolated(&format!("test_resume_{}", Uuid::new_v4().simple()))
            .unwrap();
    let stream = &feed.stream().name;
    client.ensure_stream(feed.stream()).await.unwrap();
    client
        .provision_ledger_consumer(feed.stream())
        .await
        .unwrap();
    let lease = storage.acquire_observer().await.unwrap();
    let mut consumer = ledger.observation_consumer(&client, &feed).await.unwrap();
    let identity = consumer.stream_created_at().to_owned();
    let user = Uuid::new_v4();
    for sequence in 1..=2 {
        let event = batch(
            sequence - 1,
            "old",
            None,
            vec![transfer(user, &format!("resume_{sequence}"), "0x7")],
        );
        client.publish_evm_batch(&feed, &event).await.unwrap();
        let delivery =
            tokio::time::timeout(std::time::Duration::from_secs(5), consumer.next_strict())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(delivery.stream_sequence(), sequence);
        ledger
            .observe(stream, sequence, delivery.message())
            .await
            .unwrap();
        delivery.ack().await.unwrap();
        drop(consumer);
        consumer = ledger.observation_consumer(&client, &feed).await.unwrap();
        assert_eq!(consumer.stream_created_at(), identity);
    }
    drop(consumer);
    assert_eq!(ledger.export().await.unwrap().transactions.len(), 2);

    // Simulate restoring a database backup behind the durable's acknowledged position.
    let mut write = storage.begin_ledger_write().await.unwrap();
    write.store_observation_sequence(stream, 0).await.unwrap();
    write.commit().await.unwrap();
    let error = ledger
        .observation_consumer(&client, &feed)
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("ahead of database cursor"));
    let mut write = storage.begin_ledger_write().await.unwrap();
    write.store_observation_sequence(stream, 2).await.unwrap();
    write.commit().await.unwrap();

    // A replacement can already have more messages than the old cursor; its identity still differs.
    client.delete_stream(stream).await.unwrap();
    client.ensure_stream(feed.stream()).await.unwrap();
    client
        .provision_ledger_consumer(feed.stream())
        .await
        .unwrap();
    for sequence in 1..=3 {
        let event = batch(
            sequence - 1,
            "old",
            None,
            vec![transfer(user, &format!("replacement_{sequence}"), "0x7")],
        );
        client.publish_evm_batch(&feed, &event).await.unwrap();
    }
    let error = ledger
        .observation_consumer(&client, &feed)
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("identity missing or changed"));
    assert_eq!(storage.observation_sequence(stream).await.unwrap(), 2);
    assert_eq!(ledger.export().await.unwrap().transactions.len(), 2);
    client.delete_stream(stream).await.unwrap();
    drop(lease);
    database.cleanup().await;
}

#[tokio::test]
async fn observation_accounting_is_atomic_and_replay_safe() {
    let Some((url, database)) = TestDatabase::create().await else {
        return;
    };
    let storage = PgRepository::initialise(&url, 4).await.unwrap();
    let ledger = Ledger::new(storage.clone());
    let user = Uuid::new_v4();
    let observation = transfer(user, "obs_atomic", "0x7");
    observe_transfer(&ledger, 1, &observation).await.unwrap();
    observe_transfer(&ledger, 1, &observation).await.unwrap();
    observe_transfer(&ledger, 2, &observation).await.unwrap();
    assert_eq!(storage.observation_sequence("fixture").await.unwrap(), 2);
    assert_eq!(storage.observations(user, None, 10).await.unwrap().len(), 1);
    assert_eq!(ledger.export().await.unwrap().transactions.len(), 1);
    let balances = ledger
        .balances(user, None, BalanceTimeBasis::Recorded)
        .await
        .unwrap();
    assert_eq!(
        balances
            .iter()
            .find(|balance| balance.bucket == BalanceBucket::Pending)
            .unwrap()
            .quantity,
        7
    );

    // A failed input cannot advance the cursor or partially append an observation/journal.
    let mut bad = transfer(user, "obs_storage_failure", "0x9");
    bad.block_number = Some(u64::MAX);
    assert!(observe_transfer(&ledger, 3, &bad).await.is_err());
    assert_eq!(storage.observation_sequence("fixture").await.unwrap(), 2);
    assert_eq!(storage.observations(user, None, 10).await.unwrap().len(), 1);
    assert_eq!(ledger.export().await.unwrap().transactions.len(), 1);
    assert!(observe_transfer(&ledger, 4, &observation).await.is_err());

    // Unvalued/large quantities are facts, not invalid accounting requests.
    let mut unknown = transfer(user, "obs_unknown", &format!("0x{}", "f".repeat(64)));
    unknown.asset = None;
    unknown.external_asset = Some("MEME".into());
    observe_transfer(&ledger, 3, &unknown).await.unwrap();
    let mut unreadable = transfer(user, "obs_no_parties", "0x9");
    unreadable.raw = json!({});
    observe_transfer(&ledger, 4, &unreadable).await.unwrap();
    assert_eq!(storage.observations(user, None, 10).await.unwrap().len(), 3);
    assert_eq!(ledger.export().await.unwrap().transactions.len(), 1);
    database.cleanup().await;
}

#[tokio::test]
async fn accounting_overflow_preserves_observation_without_partial_postings() {
    let Some((url, database)) = TestDatabase::create().await else {
        return;
    };
    let storage = PgRepository::initialise(&url, 4).await.unwrap();
    let ledger = Ledger::new(storage.clone());
    let user = Uuid::new_v4();
    let full = transfer(user, "obs_full", &format!("0x{:x}", u128::MAX));
    observe_transfer(&ledger, 1, &full).await.unwrap();
    let overflow = transfer(user, "obs_overflow", "0x1");
    observe_transfer(&ledger, 2, &overflow).await.unwrap();
    assert_eq!(storage.observation_sequence("fixture").await.unwrap(), 2);
    assert_eq!(storage.observations(user, None, 10).await.unwrap().len(), 2);
    let export = ledger.export().await.unwrap();
    assert_eq!(export.transactions.len(), 1);
    assert_eq!(export.transactions[0].postings.len(), 2);
    let mut write = storage.begin_ledger_write().await.unwrap();
    let parked = write.deposits(1, 0, 100, &["new"]).await.unwrap();
    assert_eq!(parked.len(), 1);
    assert_eq!(parked[0].event_id, overflow.event_id);
    drop(write);
    database.cleanup().await;
}

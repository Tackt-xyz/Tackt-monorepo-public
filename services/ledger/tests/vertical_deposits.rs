//! Real Indexer adapter -> JetStream -> Ledger -> Postgres, with simulated RPC
//! evidence. Run only against the disposable repository Compose stack.

use axum::{Json, Router, extract::State, routing::post};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tackt_indexer::{
    Indexer, NatsObservationSink,
    source::{evm::adapter::EthereumAdapter, hyperliquid::adapter::HyperliquidAdapter},
};
use tackt_ledger::Ledger;
use tackt_nats::{NatsClient, ObservationEvent, ObservationFeed};
use tackt_postgres::{PgRepository, Repository, test_support::TestDatabase};
use tackt_primitives::{
    CreateCoreUser, Source,
    ledger::{BalanceBucket, BalanceTimeBasis, LedgerAsset},
};
use uuid::Uuid;

const MEMECOIN: &str = "0xcccccccccccccccccccccccccccccccccccccccc";

struct Chain {
    run: String,
    head: u64,
    finalized: u64,
    fork: bool,
    recipient: String,
}

impl Chain {
    fn hash(&self, height: u64) -> String {
        let fork = u8::from(self.fork && height >= 100);
        format!("0x{}{:031x}{fork:x}", self.run, height)
    }
    fn tx_hash(&self, tag: u8) -> String {
        format!("0x{}{:032x}", self.run, tag)
    }
    fn token(&self) -> &str {
        LedgerAsset::Usdc.info().erc20_address().unwrap()
    }
    fn transfers(&self) -> Vec<Value> {
        vec![
            json!({"hash":self.tx_hash(1),"from":"0x2222222222222222222222222222222222222222","to":self.recipient,"value":"0x7"}),
            json!({"hash":self.tx_hash(2),"from":"0x2222222222222222222222222222222222222222","to":self.token(),"value":"0x0"}),
            // Failed native transfer: its intended value must never be credited.
            json!({"hash":self.tx_hash(3),"from":"0x2222222222222222222222222222222222222222","to":self.recipient,"value":"0x1000"}),
            json!({"hash":self.tx_hash(4),"from":"0x2222222222222222222222222222222222222222","to":MEMECOIN,"value":"0x0"}),
        ]
    }
}

async fn rpc(State(chain): State<Arc<Mutex<Chain>>>, Json(request): Json<Value>) -> Json<Value> {
    let chain = chain.lock().unwrap();
    let result = match request["method"].as_str().unwrap() {
        "eth_chainId" => json!("0x1"),
        "eth_blockNumber" => json!(format!("0x{:x}", chain.head)),
        "eth_getBlockByNumber" if request["params"][0] == "finalized" => {
            json!({"number":format!("0x{:x}",chain.finalized),"hash":chain.hash(chain.finalized),"parentHash":chain.hash(chain.finalized-1)})
        }
        "eth_getBlockByNumber" => {
            let height = u64::from_str_radix(
                request["params"][0]
                    .as_str()
                    .unwrap()
                    .trim_start_matches("0x"),
                16,
            )
            .unwrap();
            json!({"number":format!("0x{height:x}"),"hash":chain.hash(height),"parentHash":chain.hash(height-1),"timestamp":"0x68000000","transactions":if height==100 { chain.transfers() } else { vec![] }})
        }
        "eth_getTransactionReceipt" => {
            json!({"blockHash":chain.hash(100),"status":if request["params"][0] == chain.tx_hash(3) { "0x0" } else { "0x1" }})
        }
        "eth_getLogs" if request["params"][0]["blockHash"] == chain.hash(100) => json!([{
            "address":chain.token(),"transactionHash":chain.tx_hash(2),"blockHash":chain.hash(100),"logIndex":"0x0",
            "topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                format!("0x{:0>64}","2222222222222222222222222222222222222222"),
                format!("0x{:0>64}",chain.recipient.trim_start_matches("0x"))],
            "data":format!("0x{:064x}",20u128)
        }, {
            "address":MEMECOIN,"transactionHash":chain.tx_hash(4),"blockHash":chain.hash(100),"logIndex":"0x0",
            "topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                format!("0x{:0>64}","2222222222222222222222222222222222222222"),
                format!("0x{:0>64}",chain.recipient.trim_start_matches("0x"))],
            "data":format!("0x{}", "f".repeat(64))
        }]),
        "eth_getLogs" => json!([]),
        method => panic!("unexpected simulated RPC call {method}"),
    };
    Json(json!({"jsonrpc":"2.0","id":request["id"],"result":result}))
}

async fn spawn_worker(
    repository: &PgRepository,
    client: &NatsClient,
    feed: &ObservationFeed,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    let lease = repository.acquire_observer().await.unwrap();
    let ledger = Arc::new(Ledger::new(repository.clone()));
    let consumer = ledger.observation_consumer(client, feed).await.unwrap();
    tokio::spawn(tackt_ledger::observations::consume(
        ledger,
        feed.clone(),
        consumer,
        lease,
    ))
}

async fn indexer(
    repository: Arc<PgRepository>,
    client: Arc<NatsClient>,
    feed: ObservationFeed,
    endpoint: &str,
) -> Indexer<PgRepository, NatsObservationSink> {
    let sink = NatsObservationSink::initialise_on(client, feed)
        .await
        .unwrap();
    let indexer = Indexer::new(
        repository.clone(),
        sink,
        vec![EthereumAdapter::new(endpoint.into(), 1)],
        HyperliquidAdapter::new("http://127.0.0.1:1", "simulated-unused-token").unwrap(),
    );
    indexer
        .users()
        .reconcile(repository.as_ref())
        .await
        .unwrap();
    indexer
}

async fn eventually(description: &str, mut ready: impl AsyncFnMut() -> bool) {
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        while !ready().await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(result.is_ok(), "timed out: {description}");
}

async fn drained(repository: &PgRepository, client: &NatsClient, feed: &ObservationFeed) {
    let count = client
        .stream_message_count(&feed.stream().name)
        .await
        .unwrap();
    eventually("Ledger drains every published observation", async || {
        repository
            .observation_sequence(&feed.stream().name)
            .await
            .unwrap()
            == count
    })
    .await;
}

async fn balances(repository: &PgRepository, user: Uuid, pending: i128, available: i128) {
    eventually(
        "exact ETH and USDC pending/available balances",
        async || {
            let values = Ledger::new(repository.clone())
                .balances(user, None, BalanceTimeBasis::Recorded)
                .await
                .unwrap();
            let quantity = |asset, bucket| {
                values
                    .iter()
                    .find(|b| b.asset == asset && b.bucket == bucket)
                    .map_or(0, |b| b.quantity)
            };
            quantity(LedgerAsset::Eth, BalanceBucket::Pending) == 7 * pending
                && quantity(LedgerAsset::Usdc, BalanceBucket::Pending) == 20 * pending
                && quantity(LedgerAsset::Eth, BalanceBucket::Available) == 7 * available
                && quantity(LedgerAsset::Usdc, BalanceBucket::Available) == 20 * available
        },
    )
    .await;
}

#[tokio::test]
async fn indexer_deposits_survive_reorg_replay_and_ledger_restart() {
    tackt_telemetry::initialise();
    let Ok(nats_url) = std::env::var("NATS_URL") else {
        assert!(
            std::env::var_os("CI").is_none(),
            "NATS_URL is required in CI"
        );
        eprintln!("skipping: NATS_URL must point at the Compose broker");
        return;
    };
    let Some((database_url, database)) = TestDatabase::create().await else {
        return;
    };
    let admin = Arc::new(PgRepository::initialise(&database_url, 5).await.unwrap());
    let repository = Arc::new(PgRepository::connect(&database_url, 5).await.unwrap());
    let client = Arc::new(tackt_nats::initialise(&nats_url).await.unwrap());
    let run = Uuid::new_v4().simple().to_string();
    let recipient = format!("0x{run}00000001");
    let (user, _) = admin
        .create_core_user(
            CreateCoreUser {
                external_id: format!("simulated-deposit-test-{run}"),
                metadata: json!({"simulated":true}),
            },
            &test_signing_keys(),
        )
        .await
        .unwrap();
    admin
        .replace_user_account(user.id, Source::Ethereum, recipient.clone())
        .await
        .unwrap();
    let chain = Arc::new(Mutex::new(Chain {
        run: run.clone(),
        head: 99,
        finalized: 98,
        fork: false,
        recipient,
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/", post(rpc))
        .with_state(chain.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let feed = ObservationFeed::isolated(&format!("test_ledger_{run}")).unwrap();
    client.ensure_stream(feed.stream()).await.unwrap();
    client.ensure_hyperliquid_resume(&feed).await.unwrap();
    client
        .provision_ledger_consumer(feed.stream())
        .await
        .unwrap();
    let producer = indexer(admin.clone(), client.clone(), feed.clone(), &endpoint).await;
    let worker = spawn_worker(&repository, &client, &feed).await;
    producer.poll_evm(1).await.unwrap();
    chain.lock().unwrap().head = 100;
    producer.poll_evm(1).await.unwrap();
    drained(&repository, &client, &feed).await;
    balances(&repository, user.id, 1, 0).await;
    chain.lock().unwrap().head = 101;
    producer.poll_evm(1).await.unwrap();
    drained(&repository, &client, &feed).await;
    balances(&repository, user.id, 1, 0).await;
    let recorded = admin
        .observations(user.id, Some(Source::Ethereum), 20)
        .await
        .unwrap();
    assert_eq!(recorded.len(), 11, "all account observations are persisted");
    let memecoin = recorded
        .iter()
        .find(|observation| observation.external_asset.as_deref() == Some(MEMECOIN))
        .unwrap();
    assert!(memecoin.asset.is_none());
    assert_eq!(memecoin.quantity, Some(format!("0x{}", "f".repeat(64))));
    assert!(
        recorded
            .iter()
            .any(|observation| observation.kind == tackt_primitives::EventKind::EthereumReceipt)
    );
    let original: Vec<_> = recorded
        .into_iter()
        .filter(|observation| observation.asset.is_some())
        .collect();
    assert_eq!(
        original.len(),
        2,
        "only supported successful transfers enter accounting"
    );

    {
        let mut state = chain.lock().unwrap();
        state.head = 102;
        state.fork = true;
    }
    producer.poll_evm(1).await.unwrap();
    drained(&repository, &client, &feed).await;
    let export = Ledger::new(repository.as_ref().clone())
        .export()
        .await
        .unwrap();
    for observation in &original {
        assert!(export.transactions.iter().any(|entry| {
            entry
                .external_reference
                .as_deref()
                .is_some_and(|reference| {
                    reference.starts_with(&format!("{}:rejected:", observation.event_id))
                })
        }));
    }
    balances(&repository, user.id, 1, 0).await;

    // Same-height return to a previously seen block must reverse B and rebook A.
    chain.lock().unwrap().fork = false;
    let restarted = indexer(admin.clone(), client.clone(), feed.clone(), &endpoint).await;
    restarted.poll_evm(1).await.unwrap();
    drained(&repository, &client, &feed).await;
    balances(&repository, user.id, 1, 0).await;
    let facts = repository
        .observations(user.id, Some(Source::Ethereum), 100)
        .await
        .unwrap();
    for observation in &original {
        assert!(
            facts
                .iter()
                .find(|fact| fact.event_id == observation.event_id)
                .unwrap()
                .canonical
        );
    }

    // Finality alone settles the already recorded deposits without new account facts.
    chain.lock().unwrap().finalized = 100;
    restarted.poll_evm(1).await.unwrap();
    let (_, update) = client.last_evm_event(&feed, 1).await.unwrap().unwrap();
    assert!(update.batch.unwrap().observations.is_empty());
    drained(&repository, &client, &feed).await;
    balances(&repository, user.id, 0, 1).await;

    worker.abort();
    let _ = worker.await;
    // Allow the closed lease session to reach Postgres, then resume from its cursor.
    tokio::time::sleep(Duration::from_millis(100)).await;
    {
        let mut state = chain.lock().unwrap();
        state.head = 103;
        state.finalized = 102;
    }
    let restarted_producer = indexer(admin.clone(), client.clone(), feed.clone(), &endpoint).await;
    restarted_producer.poll_evm(1).await.unwrap();
    let worker = spawn_worker(&repository, &client, &feed).await;
    drained(&repository, &client, &feed).await;
    balances(&repository, user.id, 0, 1).await;

    // The same consumer records trusted non-EVM observations without a whitelist.
    let mut hyperliquid = original[0].clone();
    hyperliquid.event_id = format!("obs_hyperliquid_{run}");
    hyperliquid.source = Source::Hyperliquid;
    hyperliquid.kind = tackt_primitives::EventKind::HyperliquidBalance;
    hyperliquid.account = Some(
        tackt_primitives::accounts::SourceAccount::new(
            tackt_primitives::accounts::AccountSubject::new(user.id).unwrap(),
            Source::Hyperliquid,
            hyperliquid.account.as_ref().unwrap().external_id(),
        )
        .unwrap(),
    );
    hyperliquid.asset = None;
    hyperliquid.external_asset = Some("MEME".into());
    hyperliquid.quantity = Some("123456789.0123456789".into());
    hyperliquid.block_number = None;
    hyperliquid.block_hash = None;
    hyperliquid.raw = json!({"coin":"MEME","total":"123456789.0123456789"});
    client
        .publish_observation_to_feed(&feed, &ObservationEvent::new(hyperliquid.clone()))
        .await
        .unwrap();
    drained(&repository, &client, &feed).await;
    let stored = repository
        .observations(user.id, Some(Source::Hyperliquid), 10)
        .await
        .unwrap();
    assert_eq!(stored, vec![hyperliquid.clone()]);

    // Inclusive Hyperliquid replay may arrive under a fresh broker sequence
    // after its duplicate window. Ledger still records the fill exactly once.
    let mut fill = hyperliquid;
    fill.event_id = format!("obs_hyperliquid_fill_{run}");
    fill.kind = tackt_primitives::EventKind::HyperliquidFill;
    fill.block_number = Some(42);
    fill.provenance.method = "StreamDataBytes/TRADES".into();
    fill.quantity = Some("0.2".into());
    let fill = ObservationEvent::new(fill);
    client
        .publish_observation_to_feed(&feed, &fill)
        .await
        .unwrap();
    drained(&repository, &client, &feed).await;
    client
        .publish_durable(
            &feed.hyperliquid_subject(tackt_nats::HyperliquidObservationStream::Trades),
            None,
            &fill,
        )
        .await
        .unwrap();
    drained(&repository, &client, &feed).await;
    let stored = repository
        .observations(user.id, Some(Source::Hyperliquid), 10)
        .await
        .unwrap();
    assert_eq!(
        stored.len(),
        2,
        "one snapshot and one fill, despite redelivery"
    );
    assert_eq!(
        stored
            .iter()
            .filter(|row| row.event_id == fill.observation.event_id)
            .count(),
        1
    );

    // Retrying an already committed delivery is also safe before its ack arrives.
    let sequence = repository
        .observation_sequence(&feed.stream().name)
        .await
        .unwrap();
    let (_, last) = client.last_evm_event(&feed, 1).await.unwrap().unwrap();
    Ledger::new(repository.as_ref().clone())
        .observe(&feed.stream().name, sequence, &last)
        .await
        .unwrap();
    balances(&repository, user.id, 0, 1).await;
    worker.abort();
    let _ = worker.await;
    server.abort();
    let _ = server.await;
    client
        .delete_stream(&feed.hyperliquid_resume_stream())
        .await
        .unwrap();
    client.delete_stream(&feed.stream().name).await.unwrap();
    drop(repository);
    drop(admin);
    database.cleanup().await;
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

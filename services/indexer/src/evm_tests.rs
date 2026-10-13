use super::*;
use crate::sink::MemoryObservationSink;
use axum::{Json, Router, extract::State, routing::post};
use serde_json::{Value, json};
use std::sync::Mutex;
use tackt_postgres::MemoryRepository;

#[derive(Default)]
struct Chain {
    head: u64,
    finalized: u64,
    fork: u64,
    requests: Vec<Value>,
    header_fault: Option<Value>,
}
impl Chain {
    fn hash(&self, number: u64) -> String {
        format!(
            "0x{number:062x}{:02x}",
            if number > 10 { self.fork } else { 0 }
        )
    }
}

async fn rpc(State(chain): State<Arc<Mutex<Chain>>>, Json(request): Json<Value>) -> Json<Value> {
    let mut chain = chain.lock().unwrap();
    chain.requests.push(request.clone());
    let mut result = match request["method"].as_str().unwrap() {
        "eth_blockNumber" => json!(format!("0x{:x}", chain.head)),
        "eth_chainId" => json!("0x1"),
        "eth_getLogs" => json!([]),
        "eth_getBlockByNumber" => {
            let tag = request["params"][0].as_str().unwrap();
            let number = if tag == "finalized" {
                chain.finalized
            } else {
                u64::from_str_radix(tag.trim_start_matches("0x"), 16).unwrap()
            };
            json!({"number":format!("0x{number:x}"),"hash":chain.hash(number),
                "parentHash":chain.hash(number.saturating_sub(1)),"timestamp":"0x1","transactions":[]})
        }
        method => panic!("unexpected method {method}"),
    };
    let tip_reads = chain
        .requests
        .iter()
        .filter(|call| {
            call["method"] == "eth_getBlockByNumber"
                && call["params"][0] == format!("0x{:x}", chain.head)
                && call["params"][1] == false
        })
        .count();
    if request["method"] == "eth_getBlockByNumber"
        && request["params"][0] == format!("0x{:x}", chain.head)
        && request["params"][1] == false
        && tip_reads == 2
        && let Some(fault) = &chain.header_fault
    {
        result = fault.clone();
    }
    Json(json!({"jsonrpc":"2.0","id":1,"result":result}))
}

fn assert_headers_only(chain: &Chain) {
    assert!(
        chain
            .requests
            .iter()
            .any(|call| call["method"] == "eth_getBlockByNumber")
    );
    for call in &chain.requests {
        match call["method"].as_str().unwrap() {
            "eth_chainId" | "eth_blockNumber" => {}
            "eth_getBlockByNumber" => assert_eq!(call["params"][1], false),
            _ => panic!("unexpected account-evidence RPC: {call}"),
        }
    }
}

fn indexer(
    endpoint: &str,
    sink: MemoryObservationSink,
) -> Indexer<MemoryRepository, MemoryObservationSink> {
    Indexer::new(
        Arc::new(MemoryRepository::default()),
        sink,
        vec![EthereumAdapter::new(endpoint.into(), 1)],
        HyperliquidAdapter::new("http://127.0.0.1:1", "test-token").unwrap(),
    )
}

#[tokio::test]
async fn durable_batches_resume_reorg_return_to_old_hash_and_advance_finality() {
    let chain = Arc::new(Mutex::new(Chain {
        head: 10,
        finalized: 10,
        fork: 0,
        ..Default::default()
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/", post(rpc))
        .with_state(chain.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let sink = MemoryObservationSink::default();
    let producer = indexer(&endpoint, sink.clone());
    assert_eq!(producer.poll_evm(1).await.unwrap().published, 1);
    chain.lock().unwrap().head = 13;
    assert_eq!(producer.poll_evm(1).await.unwrap().published, 3);
    assert_eq!(sink.published().len(), 4, "one message per complete block");
    let original = sink
        .published()
        .last()
        .unwrap()
        .observation
        .event_id
        .clone();
    assert_eq!(producer.poll_evm(1).await.unwrap().published, 0);

    // Restart during an ordinary fork: the retained batch supplies the whole window.
    chain.lock().unwrap().fork = 1;
    let restarted = indexer(&endpoint, sink.clone());
    assert_eq!(restarted.poll_evm(1).await.unwrap().published, 3);
    let events = sink.published();
    assert_eq!(events[4].batch.as_ref().unwrap().revert_from, Some(11));
    assert_eq!(events[5].batch.as_ref().unwrap().revert_from, None);
    chain.lock().unwrap().fork = 0;
    assert_eq!(restarted.poll_evm(1).await.unwrap().published, 3);
    let events = sink.published();
    assert_eq!(
        events.last().unwrap().observation.event_id,
        original,
        "A -> B -> A retains fact identity"
    );
    assert_ne!(
        events[3].batch,
        events.last().unwrap().batch,
        "but is a new chain update"
    );

    // A shorter head explicitly rolls back, even without replacement blocks.
    chain.lock().unwrap().requests.clear();
    chain.lock().unwrap().head = 11;
    assert!(restarted.poll_evm(1).await.unwrap().caught_up);
    let last = sink.last_evm_event(1).await.unwrap().unwrap().1;
    assert_eq!(last.batch.as_ref().unwrap().revert_from, Some(12));
    assert_eq!(last.observation.block_number, Some(11));
    assert_headers_only(&chain.lock().unwrap());
    chain.lock().unwrap().head = 13;
    assert_eq!(restarted.poll_evm(1).await.unwrap().published, 2);

    // A new finality boundary does not need a new head to reach Ledger.
    chain.lock().unwrap().requests.clear();
    chain.lock().unwrap().finalized = 12;
    restarted.poll_evm(1).await.unwrap();
    let last = sink.last_evm_event(1).await.unwrap().unwrap().1;
    let batch = last.batch.unwrap();
    assert_eq!(batch.finalized.number, 12);
    assert_eq!(
        batch.history.iter().map(|b| b.number).collect::<Vec<_>>(),
        vec![12, 13]
    );
    assert!(batch.observations.is_empty());
    assert!(last.observation.raw.get("transactions").is_none());
    assert_headers_only(&chain.lock().unwrap());
    let count = sink.published().len();
    chain.lock().unwrap().fork = 1;
    assert!(
        restarted
            .poll_evm(1)
            .await
            .unwrap_err()
            .to_string()
            .contains("previously finalized")
    );
    assert_eq!(sink.published().len(), count);
    server.abort();
}

#[tokio::test]
async fn reorg_may_replace_blocks_that_have_since_become_finalized() {
    let chain = Arc::new(Mutex::new(Chain {
        head: 12,
        finalized: 10,
        fork: 0,
        ..Default::default()
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/", post(rpc))
        .with_state(chain.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let sink = MemoryObservationSink::default();
    let producer = indexer(&endpoint, sink.clone());
    producer.poll_evm(1).await.unwrap();
    {
        let mut chain = chain.lock().unwrap();
        chain.fork = 1;
        chain.finalized = 12;
    }
    assert_eq!(producer.poll_evm(1).await.unwrap().published, 2);
    assert_eq!(
        sink.published()[1].batch.as_ref().unwrap().revert_from,
        Some(11)
    );
    assert_eq!(
        sink.published()
            .last()
            .unwrap()
            .batch
            .as_ref()
            .unwrap()
            .finalized
            .number,
        12
    );
    server.abort();
}

#[tokio::test]
async fn header_refresh_failure_never_advances_the_checkpoint() {
    let chain = Arc::new(Mutex::new(Chain {
        head: 12,
        finalized: 10,
        ..Default::default()
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/", post(rpc))
        .with_state(chain.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let sink = MemoryObservationSink::default();
    let producer = indexer(&endpoint, sink.clone());
    producer.poll_evm(1).await.unwrap();
    let checkpoint = sink.last_evm_event(1).await.unwrap();
    let mut valid = checkpoint.as_ref().unwrap().1.observation.raw.clone();
    valid["transactions"] = json!(["transaction-hash"]);
    let mut wrong_hash = valid.clone();
    wrong_hash["hash"] = json!("another-branch");
    let mut wrong_height = valid.clone();
    wrong_height["number"] = json!("0xd");
    let mut no_timestamp = valid.clone();
    no_timestamp.as_object_mut().unwrap().remove("timestamp");
    for fault in [wrong_hash, wrong_height, no_timestamp, Value::Null] {
        {
            let mut state = chain.lock().unwrap();
            state.finalized = 11;
            state.requests.clear();
            state.header_fault = Some(fault);
        }
        assert!(producer.poll_evm(1).await.is_err());
        assert_eq!(sink.last_evm_event(1).await.unwrap(), checkpoint);
        assert_headers_only(&chain.lock().unwrap());
    }
    {
        let mut state = chain.lock().unwrap();
        state.requests.clear();
        state.header_fault = Some(valid);
    }
    // A new Indexer resumes from the unchanged durable checkpoint and retries.
    indexer(&endpoint, sink.clone()).poll_evm(1).await.unwrap();
    let (sequence, update) = sink.last_evm_event(1).await.unwrap().unwrap();
    assert_eq!(sequence, 2);
    assert_eq!(update.batch.as_ref().unwrap().finalized.number, 11);
    assert_eq!(
        update.observation.event_id,
        checkpoint.unwrap().1.observation.event_id
    );
    assert!(update.observation.raw.get("transactions").is_none());
    assert_headers_only(&chain.lock().unwrap());
    assert_eq!(producer.poll_evm(1).await.unwrap().published, 0);
    assert_eq!(sink.published().len(), 2);
    server.abort();
}

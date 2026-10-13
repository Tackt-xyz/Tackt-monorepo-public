use anyhow::{Context, Result, bail};
use chrono::{TimeZone, Utc};
use reqwest::Client;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tackt_primitives::chain::BlockRef;
use tackt_primitives::*;
use uuid::Uuid;

pub struct EvmHeader {
    pub block: BlockRef,
    pub parent_hash: String,
}

/// One block's evidence: the header fact (transaction bodies removed) and the
/// account facts read from it.
pub struct EvmBlock {
    pub header: EvmHeader,
    pub block: Observation,
    pub facts: Vec<Observation>,
}

const TRANSFER_TOPIC: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

#[derive(Clone)]
pub struct EthereumAdapter {
    client: Client,
    endpoint: String,
    chain_id: u64,
}

impl EthereumAdapter {
    pub fn new(endpoint: String, chain_id: u64) -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(15))
                .connect_timeout(Duration::from_secs(5))
                .build()
                .expect("reqwest client"),
            endpoint,
            chain_id,
        }
    }
    pub fn configured_chain_id(&self) -> u64 {
        self.chain_id
    }
    async fn rpc(&self, method: &str, params: Value) -> Result<Value> {
        let body = json!({"jsonrpc":"2.0","id":1,"method":method,"params":params});
        let response = self
            .client
            .post(&self.endpoint)
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;
        if let Some(e) = response.get("error") {
            bail!("ethereum RPC {method}: {e}")
        }
        response
            .get("result")
            .cloned()
            .context("ethereum RPC missing result")
    }
    pub async fn chain_id(&self) -> Result<u64> {
        parse_hex(
            self.rpc("eth_chainId", json!([]))
                .await?
                .as_str()
                .context("chain id not string")?,
        )
    }
    pub async fn head(&self) -> Result<u64> {
        parse_hex(
            self.rpc("eth_blockNumber", json!([]))
                .await?
                .as_str()
                .context("block number not string")?,
        )
    }

    pub async fn header(&self, number: u64) -> Result<EvmHeader> {
        let header = self.read_header(&format!("0x{number:x}")).await?;
        anyhow::ensure!(
            header.block.number == number,
            "RPC returned wrong block height"
        );
        Ok(header)
    }

    pub async fn finalized_header(&self) -> Result<EvmHeader> {
        self.read_header("finalized").await
    }

    async fn read_header(&self, tag: &str) -> Result<EvmHeader> {
        let raw = self
            .rpc("eth_getBlockByNumber", json!([tag, false]))
            .await?;
        header_from(&raw)
    }

    /// Bootstrap the connected suffix once; subsequent batches carry it durably.
    pub async fn history(&self, from: u64, through: u64) -> Result<Vec<BlockRef>> {
        let mut history = Vec::new();
        let mut expected = None;
        for number in (from..=through).rev() {
            let header = self.header(number).await?;
            if let Some(hash) = expected {
                anyhow::ensure!(
                    header.block.hash == hash,
                    "RPC branch changed during bootstrap; retry"
                );
            }
            expected = Some(header.parent_hash);
            history.push(header.block);
        }
        history.reverse();
        Ok(history)
    }

    /// Refresh an already processed canonical tip without fetching account evidence.
    pub async fn block_header(
        &self,
        expected: &BlockRef,
        finalized_head: u64,
    ) -> Result<Observation> {
        let raw = self
            .rpc(
                "eth_getBlockByNumber",
                json!([format!("0x{:x}", expected.number), false]),
            )
            .await?;
        let (header, block) =
            self.block_observation(raw, expected.number, expected.number, finalized_head)?;
        anyhow::ensure!(
            header.block == *expected,
            "RPC branch changed before header publication; retry"
        );
        Ok(block)
    }

    fn block_observation(
        &self,
        mut raw: Value,
        number: u64,
        head: u64,
        finalized_head: u64,
    ) -> Result<(EvmHeader, Observation)> {
        if raw.is_null() {
            bail!("block {number} unavailable")
        }
        // Block hash and timestamp are consensus-mandatory and participate in event
        // identity and reorg detection; a response omitting them is a broken node and
        // must fail the poll rather than fabricate a canonical row.
        let header = header_from(&raw).with_context(|| format!("block {number}"))?;
        anyhow::ensure!(
            header.block.number == number,
            "RPC returned wrong block height"
        );
        let hash = header.block.hash.clone();
        let timestamp = Utc
            .timestamp_opt(
                parse_hex(
                    raw["timestamp"]
                        .as_str()
                        .with_context(|| format!("block {number} missing timestamp"))?,
                )? as i64,
                0,
            )
            .single()
            .context("invalid block timestamp")?;
        let confirmations = head.saturating_sub(number) + 1;
        // Consensus finality, not a confirmation-depth heuristic: reverting a
        // finalized block requires slashing a third of all staked ETH, so this
        // is the bar irreversible downstream actions (lending, withdrawals)
        // may rely on.
        let finalized = number <= finalized_head;
        let provenance = |id: String, method: &str| Provenance {
            endpoint: self.endpoint.clone(),
            method: method.into(),
            source_id: id,
        };
        // Neither full-block nor header-only facts retain transaction payloads.
        if let Some(raw) = raw.as_object_mut() {
            raw.remove("transactions");
        }
        let block = Observation {
            event_id: Observation::stable_id(
                &Source::Ethereum,
                &EventKind::EthereumBlock,
                &[&self.chain_id.to_string(), &hash],
            ),
            source: Source::Ethereum,
            kind: EventKind::EthereumBlock,
            account: None,
            asset: None,
            external_asset: None,
            quantity: None,
            source_timestamp: timestamp,
            observed_at: Utc::now(),
            block_number: Some(number),
            block_hash: Some(hash.clone()),
            confirmations: Some(confirmations),
            canonical: true,
            finalized,
            raw,
            provenance: provenance(hash.clone(), "eth_getBlockByNumber"),
        };
        Ok((header, block))
    }

    pub async fn block(
        &self,
        number: u64,
        users: &HashMap<String, Uuid>,
        head: u64,
        finalized_head: u64,
    ) -> Result<EvmBlock> {
        let mut raw = self
            .rpc(
                "eth_getBlockByNumber",
                json!([format!("0x{number:x}"), true]),
            )
            .await?;
        let transactions = raw
            .get_mut("transactions")
            .map(Value::take)
            .unwrap_or(Value::Null);
        let (header, block) = self.block_observation(raw, number, head, finalized_head)?;
        let hash = header.block.hash.clone();
        let timestamp = block.source_timestamp;
        let confirmations = head.saturating_sub(number) + 1;
        let finalized = block.finalized;
        let mut out = Vec::new();
        // Native and token paths share one transaction/receipt fact per block.
        let mut emitted: HashSet<String> = HashSet::new();
        for tx in transactions.as_array().into_iter().flatten() {
            let Some(from) = tx["from"].as_str().map(str::to_lowercase) else {
                tracing::warn!(block = number, "skipping transaction without a sender");
                continue;
            };
            // Contract creations legitimately carry no `to`.
            let to = tx["to"].as_str().unwrap_or_default().to_lowercase();
            let Some((uid, account)) = users
                .get(&to)
                .map(|uid| (*uid, to.as_str()))
                .or_else(|| users.get(&from).map(|uid| (*uid, from.as_str())))
            else {
                continue;
            };
            let Some(txh) = tx["hash"].as_str().map(str::to_lowercase) else {
                tracing::warn!(block = number, "skipping transaction without a hash");
                continue;
            };
            let receipt = self
                .rpc("eth_getTransactionReceipt", json!([tx["hash"]]))
                .await?;
            let receipt_status = parse_hex(
                receipt["status"]
                    .as_str()
                    .context("watched transaction receipt is missing execution status")?,
            )?;
            ensure_receipt_block(&receipt, &hash)?;
            anyhow::ensure!(
                receipt_status <= 1,
                "watched transaction receipt has invalid execution status"
            );
            push_observation(
                &mut out,
                &txh,
                observation(
                    &self.endpoint,
                    self.chain_id,
                    EventKind::EthereumTransaction,
                    uid,
                    account,
                    None,
                    None,
                    timestamp,
                    number,
                    &hash,
                    confirmations,
                    finalized,
                    tx.clone(),
                    &txh,
                    "eth_getBlockByNumber",
                ),
            );
            if receipt_status == 1
                && tx["value"]
                    .as_str()
                    .is_some_and(|v| v != "0x0" && v != "0x")
            {
                push_observation(
                    &mut out,
                    &txh,
                    observation(
                        &self.endpoint,
                        self.chain_id,
                        EventKind::EthTransfer,
                        uid,
                        account,
                        Some("ETH"),
                        tx["value"].as_str(),
                        timestamp,
                        number,
                        &hash,
                        confirmations,
                        finalized,
                        tx.clone(),
                        &format!("{txh}:value"),
                        "eth_getBlockByNumber",
                    ),
                );
            }
            push_observation(
                &mut out,
                &txh,
                observation(
                    &self.endpoint,
                    self.chain_id,
                    EventKind::EthereumReceipt,
                    uid,
                    account,
                    None,
                    None,
                    timestamp,
                    number,
                    &hash,
                    confirmations,
                    finalized,
                    receipt.clone(),
                    &txh,
                    "eth_getTransactionReceipt",
                ),
            );
            emitted.insert(txh);
        }
        // ERC-20 transactions target the token contract, not the user's address. Query
        // Transfer logs for the block, then retain only logs involving registered users.
        // This also covers Transfer logs inside sender-matched transactions, so the
        // receipt-log scan that used to duplicate this work is gone.
        let bodies: HashMap<String, &Value> = transactions
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|tx| Some((tx["hash"].as_str()?.to_lowercase(), tx)))
            .collect();
        let logs = self
            .rpc(
                "eth_getLogs",
                json!([{"blockHash":hash,"topics":[TRANSFER_TOPIC]}]),
            )
            .await?;
        for log in logs.as_array().into_iter().flatten() {
            let Some(topics) = log["topics"].as_array() else {
                continue;
            };
            if topics.len() < 3 {
                continue;
            }
            let Some((from, to)) = topics[1]
                .as_str()
                .and_then(topic_address)
                .zip(topics[2].as_str().and_then(topic_address))
            else {
                tracing::warn!(
                    block = number,
                    "skipping transfer log with malformed party topics"
                );
                continue;
            };
            let Some((uid, account)) = users
                .get(&to)
                .map(|uid| (*uid, to.as_str()))
                .or_else(|| users.get(&from).map(|uid| (*uid, from.as_str())))
            else {
                continue;
            };
            // Both participate in the observation's source identity; empty values
            // would collapse distinct transfers onto one event id.
            let Some(tx_hash) = log["transactionHash"].as_str() else {
                tracing::warn!(
                    block = number,
                    "skipping transfer log without transaction hash"
                );
                continue;
            };
            let Some(log_index) = log["logIndex"].as_str() else {
                tracing::warn!(
                    block = number,
                    tx_hash,
                    "skipping transfer log without log index"
                );
                continue;
            };
            let source_id = format!("{tx_hash}:{log_index}");
            push_observation(
                &mut out,
                &source_id,
                observation(
                    &self.endpoint,
                    self.chain_id,
                    EventKind::Erc20Transfer,
                    uid,
                    account,
                    log["address"].as_str(),
                    log["data"].as_str(),
                    timestamp,
                    number,
                    &hash,
                    confirmations,
                    finalized,
                    log.clone(),
                    &source_id,
                    "eth_getLogs",
                ),
            );
            let txh = tx_hash.to_lowercase();
            if !emitted.insert(txh.clone()) {
                continue;
            }
            // The tx body is already in the full-block response; only the receipt
            // needs a fetch, and only once per transaction.
            let tx = bodies
                .get(&txh)
                .context("transfer log transaction is absent from its block; retry")?;
            push_observation(
                &mut out,
                &txh,
                observation(
                    &self.endpoint,
                    self.chain_id,
                    EventKind::EthereumTransaction,
                    uid,
                    account,
                    None,
                    None,
                    timestamp,
                    number,
                    &hash,
                    confirmations,
                    finalized,
                    (*tx).clone(),
                    &txh,
                    "eth_getBlockByNumber",
                ),
            );
            let receipt = self
                .rpc("eth_getTransactionReceipt", json!([tx_hash]))
                .await?;
            ensure_receipt_block(&receipt, &hash)?;
            push_observation(
                &mut out,
                &txh,
                observation(
                    &self.endpoint,
                    self.chain_id,
                    EventKind::EthereumReceipt,
                    uid,
                    account,
                    None,
                    None,
                    timestamp,
                    number,
                    &hash,
                    confirmations,
                    finalized,
                    receipt,
                    &txh,
                    "eth_getTransactionReceipt",
                ),
            );
        }
        anyhow::ensure!(
            self.header(number).await?.block.hash == hash,
            "RPC branch changed while reading block; retry"
        );
        Ok(EvmBlock {
            header,
            block,
            facts: out,
        })
    }
}

/// Header coordinates from an `eth_getBlockByNumber` result, hashes lowercased.
fn header_from(raw: &Value) -> Result<EvmHeader> {
    Ok(EvmHeader {
        block: BlockRef {
            number: parse_hex(raw["number"].as_str().context("header missing number")?)?,
            hash: raw["hash"]
                .as_str()
                .context("header missing hash")?
                .to_lowercase(),
        },
        parent_hash: raw["parentHash"]
            .as_str()
            .context("header missing parentHash")?
            .to_lowercase(),
    })
}

#[allow(clippy::too_many_arguments)]
fn observation(
    endpoint: &str,
    chain_id: u64,
    kind: EventKind,
    user: Uuid,
    account: &str,
    asset: Option<&str>,
    quantity: Option<&str>,
    ts: chrono::DateTime<Utc>,
    block: u64,
    hash: &str,
    confirmations: u64,
    finalized: bool,
    raw: Value,
    source_id: &str,
    method: &str,
) -> Result<Observation> {
    let subject = tackt_primitives::accounts::AccountSubject::new(user)?;
    let account =
        tackt_primitives::accounts::SourceAccount::new(subject, Source::Ethereum, account)?;
    let normalized_asset = match kind {
        EventKind::EthTransfer => Some(tackt_primitives::assets::AssetReference::new(
            tackt_primitives::assets::native_asset(chain_id)?.asset,
        )),
        EventKind::Erc20Transfer => asset
            .and_then(|address| tackt_primitives::assets::by_erc20_contract(chain_id, address).ok())
            .map(|info| tackt_primitives::assets::AssetReference::new(info.asset)),
        _ => None,
    };
    let observation = Observation {
        // The block hash participates in the identity so a transaction orphaned by a
        // reorg and re-included in a replacement block creates a fresh canonical row
        // instead of being dropped by ON CONFLICT DO NOTHING.
        event_id: Observation::stable_id(&Source::Ethereum, &kind, &[hash, source_id]),
        source: Source::Ethereum,
        kind,
        account: Some(account),
        asset: normalized_asset,
        external_asset: asset.map(str::to_ascii_lowercase),
        quantity: quantity.map(Into::into),
        source_timestamp: ts,
        observed_at: Utc::now(),
        block_number: Some(block),
        block_hash: Some(hash.into()),
        confirmations: Some(confirmations),
        canonical: true,
        finalized,
        raw,
        provenance: Provenance {
            endpoint: endpoint.into(),
            method: method.into(),
            source_id: source_id.into(),
        },
    };
    Ok(observation)
}
fn parse_hex(v: &str) -> Result<u64> {
    u64::from_str_radix(v.trim_start_matches("0x"), 16)
        .with_context(|| format!("invalid hex integer {v}"))
}

fn ensure_receipt_block(receipt: &Value, hash: &str) -> Result<()> {
    anyhow::ensure!(
        receipt["blockHash"]
            .as_str()
            .is_some_and(|value| value.eq_ignore_ascii_case(hash)),
        "receipt belongs to a different block; retry"
    );
    Ok(())
}
fn push_observation(out: &mut Vec<Observation>, source_id: &str, result: Result<Observation>) {
    match result {
        Ok(observation) => out.push(observation),
        Err(error) => tracing::warn!(%error, source_id, "quarantining invalid EVM observation"),
    }
}

fn topic_address(v: &str) -> Option<String> {
    let digits = v.strip_prefix("0x").or_else(|| v.strip_prefix("0X"))?;
    (digits.len() == 64 && digits.chars().all(|c| c.is_ascii_hexdigit()))
        .then(|| format!("0x{}", digits[24..].to_ascii_lowercase()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, extract::State, routing::post};

    #[tokio::test]
    async fn native_deposits_require_a_successful_receipt() {
        const RECIPIENT: &str = "0x1111111111111111111111111111111111111111";
        async fn rpc(
            State(status): State<&'static str>,
            Json(request): Json<Value>,
        ) -> Json<Value> {
            let result = match request["method"].as_str().unwrap() {
                "eth_getBlockByNumber" => json!({
                    "number":"0x64","hash":"0xabc","parentHash":"0xparent","timestamp":"0x68000000",
                    "transactions":[{"hash":"0xdeposit","from":"0x2222222222222222222222222222222222222222","to":RECIPIENT,"value":"0x7"}]
                }),
                "eth_getTransactionReceipt" => json!({"status":status,"blockHash":"0xabc"}),
                "eth_getLogs" => json!([]),
                method => panic!("unexpected method {method}"),
            };
            Json(json!({"jsonrpc":"2.0","id":request["id"],"result":result}))
        }
        for (status, expected) in [("0x0", 0), ("0x1", 1)] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let app = Router::new().route("/", post(rpc)).with_state(status);
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let adapter = EthereumAdapter::new(endpoint, 1);
            let observations = adapter
                .block(
                    100,
                    &HashMap::from([(RECIPIENT.into(), uuid::Uuid::from_u128(1))]),
                    102,
                    100,
                )
                .await
                .unwrap();
            assert_eq!(
                observations
                    .facts
                    .iter()
                    .filter(|observation| observation.kind == EventKind::EthTransfer)
                    .count(),
                expected,
                "receipt status {status}"
            );
            assert!(
                observations
                    .facts
                    .iter()
                    .any(|observation| observation.kind == EventKind::EthereumReceipt)
            );
            server.abort();
        }
    }
    #[test]
    fn extracts_topic_address() {
        assert_eq!(
            topic_address("0x000000000000000000000000abc0000000000000000000000000000000000123"),
            Some("0xabc0000000000000000000000000000000000123".into())
        );
        assert_eq!(topic_address("0x1234"), None);
    }

    #[test]
    fn receipts_must_belong_to_the_block_being_published() {
        assert!(ensure_receipt_block(&json!({"blockHash":"0xABC"}), "0xabc").is_ok());
        assert!(ensure_receipt_block(&json!({"blockHash":"0xdef"}), "0xabc").is_err());
        assert!(ensure_receipt_block(&json!({}), "0xabc").is_err());
    }
}

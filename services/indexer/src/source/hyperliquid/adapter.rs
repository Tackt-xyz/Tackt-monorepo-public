use super::stream::validate_decimal;
use anyhow::{Context, Result, bail};
use chrono::{DateTime, TimeZone, Utc};
use futures_util::StreamExt;
use reqwest::Client;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};
use tackt_primitives::*;
use uuid::Uuid;

pub const PORTFOLIO_BATCH_LIMIT: usize = 500;
// Bounds JSON-RPC memory for portfolio batches and multi-block recovery pages.
const MAX_RPC_RESPONSE_BYTES: usize = 32 * 1024 * 1024;
// Single-block fetches match the gRPC stream's per-block cap so any streamable
// block stays recoverable.
const MAX_SINGLE_BLOCK_RESPONSE_BYTES: usize = 100 * 1024 * 1024;

/// Oversized RPC response; recovery reacts by shrinking its page size.
#[derive(Debug)]
pub struct ResponseTooLarge;

impl std::fmt::Display for ResponseTooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Hyperliquid RPC response exceeds the size cap")
    }
}

impl std::error::Error for ResponseTooLarge {}

pub struct PortfolioBatch {
    pub observations: Vec<Observation>,
    pub failed_accounts: Vec<String>,
}

#[derive(Clone)]
pub struct HyperliquidAdapter {
    client: Client,
    rpc_endpoint: url::Url,
    provenance_endpoint: String,
}

impl HyperliquidAdapter {
    pub fn new(grpc_url: &str, token: &str) -> Result<Self> {
        let mut rpc_endpoint = url::Url::parse(grpc_url).context("invalid Hyperliquid gRPC URL")?;
        if rpc_endpoint.scheme() == "https" {
            rpc_endpoint
                .set_port(None)
                .map_err(|_| anyhow::anyhow!("invalid Hyperliquid gRPC port"))?;
        }
        rpc_endpoint.set_path("");
        rpc_endpoint
            .path_segments_mut()
            .map_err(|_| anyhow::anyhow!("Hyperliquid RPC URL cannot contain path segments"))?
            .push(token)
            .push("hypercore");
        Ok(Self {
            client: Client::builder()
                .timeout(Duration::from_secs(30))
                .connect_timeout(Duration::from_secs(5))
                .build()
                .context("build Hyperliquid HTTP client")?,
            rpc_endpoint,
            provenance_endpoint: grpc_url.to_owned(),
        })
    }

    async fn rpc(&self, method: &str, params: Value, limit: usize) -> Result<Value> {
        let response = self
            .client
            .post(self.rpc_endpoint.clone())
            .json(&json!({"jsonrpc":"2.0","method":method,"params":params,"id":1}))
            .send()
            .await
            .map_err(reqwest::Error::without_url)
            .with_context(|| format!("Hyperliquid {method} request failed"))?;
        let response = response
            .error_for_status()
            .map_err(reqwest::Error::without_url)
            .with_context(|| format!("Hyperliquid {method} returned an HTTP error"))?;
        let mut bytes = Vec::new();
        let mut chunks = response.bytes_stream();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk
                .map_err(reqwest::Error::without_url)
                .with_context(|| format!("read Hyperliquid {method} response"))?;
            if bytes.len().saturating_add(chunk.len()) > limit {
                return Err(anyhow::Error::new(ResponseTooLarge).context(format!(
                    "Hyperliquid {method} response exceeds {limit} bytes"
                )));
            }
            bytes.extend_from_slice(&chunk);
        }
        let body: Value = serde_json::from_slice(&bytes)
            .with_context(|| format!("decode Hyperliquid {method} response"))?;
        if let Some(error) = body.get("error") {
            bail!("Hyperliquid {method} RPC error: {error}")
        }
        body.get("result")
            .cloned()
            .with_context(|| format!("Hyperliquid {method} response missing result"))
    }

    pub async fn batch_blocks(&self, stream: &str, from: u64, to: u64) -> Result<Vec<Value>> {
        let limit = if from == to {
            MAX_SINGLE_BLOCK_RESPONSE_BYTES
        } else {
            MAX_RPC_RESPONSE_BYTES
        };
        let result = self
            .rpc(
                "hl_getBatchBlocks",
                json!({"stream":stream,"from":from,"to":to}),
                limit,
            )
            .await?;
        result["blocks"]
            .as_array()
            .cloned()
            .context("Hyperliquid batch blocks response missing blocks")
    }

    pub async fn portfolio_snapshots(&self, accounts: &[(Uuid, String)]) -> Result<PortfolioBatch> {
        if accounts.len() > PORTFOLIO_BATCH_LIMIT {
            bail!(
                "Hyperliquid portfolio batch has {} accounts; maximum is {PORTFOLIO_BATCH_LIMIT}",
                accounts.len()
            )
        }
        if accounts.is_empty() {
            return Ok(PortfolioBatch {
                observations: Vec::new(),
                failed_accounts: Vec::new(),
            });
        }
        let owners: HashMap<_, _> = accounts
            .iter()
            .map(|(user, account)| (account.to_lowercase(), *user))
            .collect();
        let result = self
            .rpc(
                "hl_batchPortfolioStates",
                json!({"users":owners.keys().collect::<Vec<_>>() }),
                MAX_RPC_RESPONSE_BYTES,
            )
            .await?;
        let states = result["successful_states"]
            .as_array()
            .context("Hyperliquid portfolio response missing successful_states")?;
        let mut observations = Vec::new();
        let mut returned = HashSet::new();
        let mut seen = HashSet::new();
        let mut failed_accounts = HashSet::new();
        for state in states {
            let tuple = state
                .as_array()
                .filter(|tuple| tuple.len() == 2)
                .context("Hyperliquid portfolio state must be [address, data]")?;
            let account = tuple[0]
                .as_str()
                .context("Hyperliquid portfolio state missing address")?
                .to_lowercase();
            let user = owners
                .get(&account)
                .copied()
                .with_context(|| format!("Hyperliquid returned unrequested account {account}"))?;
            if !seen.insert(account.clone()) {
                bail!("Hyperliquid returned duplicate portfolio state for {account}")
            }
            let clearinghouse = &tuple[1]["clearinghouseState"];
            if !clearinghouse.is_object() {
                bail!("Hyperliquid portfolio state for {account} missing clearinghouseState")
            }
            match self.state_observations(user, &account, clearinghouse) {
                Ok(account_observations) => {
                    observations.extend(account_observations);
                    returned.insert(account);
                }
                Err(error) => {
                    tracing::warn!(%error, %account, "quarantining invalid Hyperliquid account snapshot");
                    failed_accounts.insert(account);
                }
            }
        }
        let failed_wallets = result["failed_wallets"]
            .as_array()
            .context("Hyperliquid portfolio response missing failed_wallets")?;
        for account in failed_wallets {
            failed_accounts.insert(
                account
                    .as_str()
                    .context("Hyperliquid failed wallet must be a string")?
                    .to_lowercase(),
            );
        }
        if let Some(unrequested) = failed_accounts
            .iter()
            .find(|account| !owners.contains_key(*account))
        {
            bail!("Hyperliquid returned unrequested failed account {unrequested}")
        }
        if let Some(conflict) = failed_accounts
            .iter()
            .find(|account| returned.contains(*account))
        {
            bail!("Hyperliquid returned {conflict} as both successful and failed")
        }
        failed_accounts.extend(
            owners
                .keys()
                .filter(|account| !returned.contains(*account))
                .cloned(),
        );
        let failed_accounts = failed_accounts.into_iter().collect();
        Ok(PortfolioBatch {
            observations,
            failed_accounts,
        })
    }

    fn state_observations(
        &self,
        user: Uuid,
        account: &str,
        state: &Value,
    ) -> Result<Vec<Observation>> {
        let mut observations = vec![];
        let snapshot_time = state["time"]
            .as_i64()
            .context("clearinghouseState missing time")?;
        let summary = state["marginSummary"]
            .as_object()
            .context("clearinghouseState missing marginSummary")?;
        let account_value = validate_decimal(
            summary
                .get("accountValue")
                .and_then(Value::as_str)
                .context("marginSummary missing accountValue")?,
            "marginSummary accountValue",
        )?;
        observations.push(self.observation(
            user,
            account,
            EventKind::HyperliquidBalance,
            "USD",
            Some(account_value),
            state.clone(),
            "marginSummary",
            snapshot_time,
        )?);
        let positions = state["assetPositions"]
            .as_array()
            .context("clearinghouseState missing assetPositions")?;
        for position in positions {
            let position_data = position["position"]
                .as_object()
                .context("asset position missing position")?;
            let coin = position_data
                .get("coin")
                .and_then(Value::as_str)
                .filter(|coin| !coin.is_empty())
                .context("position missing coin")?;
            let size = validate_decimal(
                position_data
                    .get("szi")
                    .and_then(Value::as_str)
                    .context("position missing szi")?,
                "position szi",
            )?;
            observations.push(self.observation(
                user,
                account,
                EventKind::HyperliquidPosition,
                coin,
                Some(size),
                position.clone(),
                &format!("position:{coin}"),
                snapshot_time,
            )?);
        }
        Ok(observations)
    }

    #[allow(clippy::too_many_arguments)]
    fn observation(
        &self,
        user: Uuid,
        account: &str,
        kind: EventKind,
        asset: &str,
        quantity: Option<&str>,
        raw: Value,
        identity: &str,
        timestamp_millis: i64,
    ) -> Result<Observation> {
        let source_timestamp: DateTime<Utc> =
            Utc.timestamp_millis_opt(timestamp_millis)
                .single()
                .with_context(|| format!("unrepresentable timestamp {timestamp_millis}"))?;
        let source_id = format!("{identity}:{timestamp_millis}");
        let subject = tackt_primitives::accounts::AccountSubject::new(user)?;
        let account =
            tackt_primitives::accounts::SourceAccount::new(subject, Source::Hyperliquid, account)?;
        let observation = Observation {
            event_id: Observation::stable_id(
                &Source::Hyperliquid,
                &kind,
                &[account.external_id(), &source_id],
            ),
            source: Source::Hyperliquid,
            kind,
            account: Some(account),
            asset: None,
            external_asset: Some(asset.into()),
            quantity: quantity.map(Into::into),
            source_timestamp,
            observed_at: Utc::now(),
            block_number: None,
            block_hash: None,
            confirmations: None,
            canonical: true,
            finalized: true,
            raw,
            provenance: Provenance {
                endpoint: self.provenance_endpoint.clone(),
                method: "hl_batchPortfolioStates".into(),
                source_id,
            },
        };
        Ok(observation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, routing::post};
    use std::sync::{Arc, Mutex};

    #[test]
    fn derives_hypercore_url_without_exposing_it_as_provenance() {
        let adapter = HyperliquidAdapter::new("https://example.com:10000", "secret").unwrap();
        assert_eq!(
            adapter.rpc_endpoint.as_str(),
            "https://example.com/secret/hypercore"
        );
        assert_eq!(adapter.provenance_endpoint, "https://example.com:10000");
    }

    #[tokio::test]
    async fn batches_portfolios_and_preserves_snapshot_ids() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let app = Router::new().route(
            "/secret/hypercore",
            post(move |Json(request): Json<Value>| {
                captured.lock().unwrap().push(request);
                async {
                    Json(json!({
                        "jsonrpc":"2.0", "id":1,
                        "result":{
                            "successful_states":[[
                                "0x1111111111111111111111111111111111111111",
                                {"clearinghouseState":{
                                    "time":1_700_000_000_000_i64,
                                    "marginSummary":{"accountValue":"123.45"},
                                    "assetPositions":[{"position":{"coin":"BTC","szi":"0.25"}}]
                                }}
                            ]],
                            "failed_wallets":[]
                        }
                    }))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let user = Uuid::new_v4();
        let adapter = HyperliquidAdapter::new(&format!("http://{address}"), "secret").unwrap();
        let batch = adapter
            .portfolio_snapshots(&[(user, "0x1111111111111111111111111111111111111111".into())])
            .await
            .unwrap();
        assert!(batch.failed_accounts.is_empty());
        assert_eq!(batch.observations.len(), 2);
        assert_eq!(batch.observations[0].quantity.as_deref(), Some("123.45"));
        assert_eq!(
            batch.observations[0].provenance.source_id,
            "marginSummary:1700000000000"
        );
        assert_eq!(
            batch.observations[1].provenance.source_id,
            "position:BTC:1700000000000"
        );
        let request = &requests.lock().unwrap()[0];
        assert_eq!(request["method"], "hl_batchPortfolioStates");
        assert_eq!(request["params"]["users"].as_array().unwrap().len(), 1);
        server.abort();
    }
}

pub mod adapter;

use crate::backoff::RestartBackoff;
use crate::engine::Indexer;
use crate::sink::ObservationSink;
use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tackt_postgres::Repository;
use tokio::{net::TcpStream, sync::Notify, time::Instant};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Ethereum normally produces a head roughly every twelve seconds. A minute without
/// one is treated as a stale signal channel and forces a reconnect.
const HEAD_IDLE_LIMIT: Duration = Duration::from_secs(60);
/// Cheap HTTP safety net for a live socket that somehow misses a notification.
const RECONCILE_INTERVAL: Duration = Duration::from_secs(5 * 60);
type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Runs a capacity-one WebSocket wake channel beside the authoritative HTTP
/// reconciler for one EVM chain. The socket never blocks on RPC or database
/// work, and bursty heads coalesce into at most one follow-up reconciliation.
pub async fn supervise<R: Repository, S: ObservationSink>(
    url: String,
    indexer: Indexer<R, S>,
    chain_id: u64,
) {
    let wake = Arc::new(Notify::new());
    tokio::join!(
        supervise_connections(url, wake.clone()),
        reconcile(indexer, wake, chain_id),
    );
}

async fn reconcile<R: Repository, S: ObservationSink>(
    indexer: Indexer<R, S>,
    wake: Arc<Notify>,
    chain_id: u64,
) {
    // Start immediately even when the WebSocket endpoint is temporarily unavailable.
    poll(&indexer, chain_id).await;
    let mut fallback = tokio::time::interval_at(
        tokio::time::Instant::now() + RECONCILE_INTERVAL,
        RECONCILE_INTERVAL,
    );
    fallback.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = wake.notified() => poll(&indexer, chain_id).await,
            _ = fallback.tick() => poll(&indexer, chain_id).await,
        }
    }
}

async fn poll<R: Repository, S: ObservationSink>(indexer: &Indexer<R, S>, chain_id: u64) {
    let mut total = 0;
    loop {
        match indexer.poll_evm(chain_id).await {
            Ok(result) => {
                total += result.published;
                if result.caught_up {
                    tracing::info!(published = total, chain_id, "EVM chain synchronized");
                    return;
                }
                tokio::task::yield_now().await;
            }
            Err(error) => {
                tracing::error!(%error, chain_id, "EVM reconciliation failed");
                return;
            }
        }
    }
}

async fn supervise_connections(url: String, wake: Arc<Notify>) {
    let mut backoff = RestartBackoff::new(Duration::from_secs(1), Duration::from_secs(30));
    loop {
        let started = Instant::now();
        if let Err(error) = connection(&url, &wake).await {
            tracing::warn!(%error, "Ethereum websocket cycle failed");
        }
        tokio::time::sleep(backoff.next_delay(started.elapsed())).await;
    }
}

async fn connection(url: &str, wake: &Notify) -> Result<()> {
    let (mut socket, _) = tokio::time::timeout(HANDSHAKE_TIMEOUT, connect_async(url))
        .await
        .context("Ethereum websocket connect timeout")??;
    let subscription = tokio::time::timeout(HANDSHAKE_TIMEOUT, subscribe(&mut socket))
        .await
        .context("Ethereum websocket subscribe timeout")??;
    // Reconcile only after the subscription is installed, closing the
    // subscribe/snapshot race. Notify coalesces with the immediate startup poll.
    wake.notify_one();
    loop {
        tokio::time::timeout(HEAD_IDLE_LIMIT, next_head(&mut socket, &subscription))
            .await
            .context("Ethereum newHeads subscription idle")??;
        wake.notify_one();
    }
}

async fn subscribe(socket: &mut Socket) -> Result<String> {
    socket
        .send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newHeads"]
            })
            .to_string()
            .into(),
        ))
        .await
        .context("Ethereum newHeads subscribe")?;
    loop {
        match socket.next().await {
            Some(Ok(Message::Text(text))) => {
                let value: Value = serde_json::from_str(text.as_ref())?;
                if value["id"] == 1 {
                    if let Some(error) = value.get("error") {
                        bail!("Ethereum newHeads subscribe: {error}")
                    }
                    return value["result"]
                        .as_str()
                        .map(str::to_owned)
                        .context("Ethereum subscription response missing result");
                }
            }
            Some(Ok(Message::Ping(payload))) => socket.send(Message::Pong(payload)).await?,
            Some(Ok(Message::Close(_))) | None => {
                bail!("Ethereum websocket closed during subscribe")
            }
            Some(Err(error)) => return Err(error).context("Ethereum websocket read"),
            Some(Ok(_)) => {}
        }
    }
}

async fn next_head(socket: &mut Socket, subscription: &str) -> Result<()> {
    loop {
        match socket.next().await {
            Some(Ok(Message::Text(text))) => {
                let value: Value = serde_json::from_str(text.as_ref())?;
                if is_head(&value, subscription)? {
                    return Ok(());
                }
            }
            Some(Ok(Message::Ping(payload))) => socket.send(Message::Pong(payload)).await?,
            Some(Ok(Message::Close(_))) | None => bail!("Ethereum websocket closed"),
            Some(Err(error)) => return Err(error).context("Ethereum websocket read"),
            Some(Ok(_)) => {}
        }
    }
}

fn is_head(value: &Value, subscription: &str) -> Result<bool> {
    if value["method"] != "eth_subscription"
        || value["params"]["subscription"].as_str() != Some(subscription)
    {
        return Ok(false);
    }
    let number = value["params"]["result"]["number"]
        .as_str()
        .context("Ethereum newHeads notification missing number")?;
    u64::from_str_radix(number.trim_start_matches("0x"), 16)
        .with_context(|| format!("invalid Ethereum head number {number}"))?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;
    use tokio_tungstenite::accept_async;

    #[tokio::test]
    async fn subscribes_then_wakes_only_for_matching_heads() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (send_unrelated, receive_unrelated) = oneshot::channel();
        let (unrelated_processed, wait_until_processed) = oneshot::channel();
        let (send_matching, receive_matching) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let request = socket.next().await.unwrap().unwrap();
            let Message::Text(request) = request else {
                panic!("expected text subscription request")
            };
            let request: Value = serde_json::from_str(request.as_ref()).unwrap();
            assert_eq!(request["method"], "eth_subscribe");
            assert_eq!(request["params"], json!(["newHeads"]));
            socket
                .send(Message::Text(
                    json!({"jsonrpc":"2.0", "id":1, "result":"0xabc"})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();

            receive_unrelated.await.unwrap();
            socket
                .send(Message::Text(
                    json!({
                        "method":"eth_subscription",
                        "params": {
                            "subscription":"0xother",
                            "result":{"number":"0x10"}
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            socket.send(Message::Ping(vec![1].into())).await.unwrap();
            loop {
                if matches!(socket.next().await.unwrap().unwrap(), Message::Pong(_)) {
                    break;
                }
            }
            unrelated_processed.send(()).unwrap();

            receive_matching.await.unwrap();
            socket
                .send(Message::Text(
                    json!({
                        "method":"eth_subscription",
                        "params": {
                            "subscription":"0xabc",
                            "result":{"number":"0x11"}
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            socket.send(Message::Close(None)).await.unwrap();
        });
        let wake = Arc::new(Notify::new());
        let client_wake = wake.clone();
        let client =
            tokio::spawn(async move { connection(&format!("ws://{address}"), &client_wake).await });

        tokio::time::timeout(Duration::from_secs(1), wake.notified())
            .await
            .expect("subscribe should wake reconciliation");
        send_unrelated.send(()).unwrap();
        wait_until_processed.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(1), wake.notified())
                .await
                .is_err(),
            "unrelated subscription must not wake reconciliation"
        );
        send_matching.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), wake.notified())
            .await
            .expect("matching head should wake reconciliation");
        assert!(client.await.unwrap().is_err());
        server.await.unwrap();
    }

    #[test]
    fn accepts_matching_head() {
        let value = json!({
            "jsonrpc": "2.0",
            "method": "eth_subscription",
            "params": {"subscription": "0xabc", "result": {"number": "0x10"}}
        });
        assert!(is_head(&value, "0xabc").unwrap());
    }

    #[test]
    fn ignores_other_subscriptions() {
        let value = json!({
            "method": "eth_subscription",
            "params": {"subscription": "0xdef", "result": {"number": "0x10"}}
        });
        assert!(!is_head(&value, "0xabc").unwrap());
    }

    #[test]
    fn rejects_malformed_matching_head() {
        let value = json!({
            "method": "eth_subscription",
            "params": {"subscription": "0xabc", "result": {"number": "latest"}}
        });
        assert!(is_head(&value, "0xabc").is_err());
    }
}

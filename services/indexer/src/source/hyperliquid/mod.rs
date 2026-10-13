pub mod adapter;
pub mod stream;

use self::{
    adapter::ResponseTooLarge,
    stream::{HyperliquidStream, normalize_block},
};
use crate::Indexer;
use crate::sink::ObservationSink;
use anyhow::{Context, Result, bail};
use std::{
    collections::HashMap,
    io::{Cursor, Read},
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tackt_postgres::Repository;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    Request,
    codec::CompressionEncoding,
    metadata::MetadataValue,
    transport::{Channel, Endpoint},
};
use uuid::Uuid;

pub mod proto {
    tonic::include_proto!("hyperliquid");
}

use proto::{
    Ping, PingRequest, StreamSubscribe, StreamType, SubscribeRequest,
    streaming_client::StreamingClient, subscribe_bytes_update, subscribe_request,
};

const PING_INTERVAL: Duration = Duration::from_secs(30);
const READ_IDLE_TIMEOUT: Duration = Duration::from_secs(90);
const MAX_MESSAGE_BYTES: usize = 100 * 1024 * 1024;
const BACKFILL_BLOCKS_PER_REQUEST: u64 = 200;
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];

/// Process-local completed-block progress for reconnects. A fresh process seeds
/// each feed from its broker-maintained KV observation and replays that block.
#[derive(Clone, Default)]
pub struct StreamCursors(Arc<RwLock<HashMap<&'static str, u64>>>);

impl StreamCursors {
    fn get(&self, stream: HyperliquidStream) -> Option<u64> {
        self.0
            .read()
            .expect("Hyperliquid stream cursor lock")
            .get(stream.name())
            .copied()
    }

    fn advance(&self, stream: HyperliquidStream, expected: Option<u64>, block: u64) -> Result<()> {
        let mut cursors = self.0.write().expect("Hyperliquid stream cursor lock");
        let current = cursors.get(stream.name()).copied();
        if current != expected {
            bail!(
                "Hyperliquid {} in-memory cursor changed from {expected:?} to {current:?}",
                stream.name()
            )
        }
        if expected.is_some_and(|expected| block < expected) {
            bail!(
                "Hyperliquid {} block {block} precedes in-memory cursor {expected:?}",
                stream.name()
            )
        }
        cursors.insert(stream.name(), block);
        Ok(())
    }
}

pub type AccountRegistry = Arc<AccountRegistryState>;

#[derive(Default, PartialEq, Eq)]
struct RegisteredAccounts {
    owners: HashMap<String, Uuid>,
    active_since: HashMap<String, chrono::DateTime<chrono::Utc>>,
}

pub struct AccountRegistryState {
    accounts: RwLock<Arc<RegisteredAccounts>>,
    generation: AtomicU64,
    replayed_generation: AtomicU64,
}

impl Default for AccountRegistryState {
    fn default() -> Self {
        Self {
            accounts: RwLock::new(Arc::new(RegisteredAccounts::default())),
            generation: AtomicU64::new(0),
            replayed_generation: AtomicU64::new(0),
        }
    }
}

impl AccountRegistryState {
    fn snapshot(&self) -> Arc<RegisteredAccounts> {
        self.accounts
            .read()
            .expect("Hyperliquid account registry lock")
            .clone()
    }

    pub fn needs_replay(&self) -> bool {
        self.replayed_generation.load(Ordering::Acquire) < self.generation.load(Ordering::Acquire)
    }

    fn mark_replayed(&self, generation: u64) {
        self.replayed_generation
            .fetch_max(generation, Ordering::AcqRel);
    }

    /// Startup only: stream bootstrap covers the same range, so the first
    /// worker replay is redundant.
    pub fn mark_replayed_current(&self) {
        self.mark_replayed(self.generation.load(Ordering::Acquire));
    }
}

pub struct AccountRefresh {
    pub accounts: usize,
    pub changed: bool,
}

pub async fn refresh_accounts<R: Repository>(
    repository: &R,
    registry: &AccountRegistry,
) -> Result<AccountRefresh> {
    let mut accounts = RegisteredAccounts::default();
    for registration in repository.hyperliquid_account_registrations().await? {
        let account = registration.account.to_lowercase();
        accounts
            .owners
            .insert(account.clone(), registration.user_id);
        accounts
            .active_since
            .insert(account, registration.active_since);
    }
    let count = accounts.owners.len();
    let mut current = registry
        .accounts
        .write()
        .expect("Hyperliquid account registry lock");
    let changed = **current != accounts;
    if changed {
        *current = Arc::new(accounts);
        registry.generation.fetch_add(1, Ordering::AcqRel);
    }
    Ok(AccountRefresh {
        accounts: count,
        changed,
    })
}

pub async fn validate_connection(grpc_url: &str, token: &str) -> Result<()> {
    authenticated_channel(grpc_url, token).await?;
    Ok(())
}

async fn authenticated_channel(
    grpc_url: &str,
    token: &str,
) -> Result<(Channel, MetadataValue<tonic::metadata::Ascii>)> {
    let endpoint = Endpoint::new(grpc_url.to_owned())?
        .connect_timeout(Duration::from_secs(10))
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_timeout(Duration::from_secs(10))
        .keep_alive_while_idle(true);
    let channel = endpoint
        .connect()
        .await
        .context("connect QuickNode Hyperliquid gRPC")?;
    let mut metadata = MetadataValue::try_from(token)
        .context("HYPERLIQUID_GRPC_TOKEN is not valid gRPC metadata")?;
    metadata.set_sensitive(true);
    let mut validation = client(channel.clone());
    let mut ping = Request::new(PingRequest { count: 1 });
    ping.metadata_mut().insert("x-token", metadata.clone());
    tokio::time::timeout(Duration::from_secs(15), validation.ping(ping))
        .await
        .context("QuickNode Hyperliquid gRPC ping timed out")??;
    Ok((channel, metadata))
}

pub async fn supervise<R: Repository + Clone, S: ObservationSink>(
    grpc_url: String,
    token: String,
    initial_lookback_blocks: u64,
    indexer: Indexer<R, S>,
    accounts: AccountRegistry,
    cursors: StreamCursors,
    dirty_accounts: broadcast::Sender<String>,
) -> Result<()> {
    let (channel, metadata) = authenticated_channel(&grpc_url, &token).await?;
    let trades = supervise_stream(
        HyperliquidStream::Trades,
        channel.clone(),
        metadata.clone(),
        grpc_url.clone(),
        initial_lookback_blocks,
        indexer.clone(),
        accounts.clone(),
        cursors.clone(),
        dirty_accounts.clone(),
    );
    let events = supervise_stream(
        HyperliquidStream::Events,
        channel,
        metadata,
        grpc_url,
        initial_lookback_blocks,
        indexer,
        accounts,
        cursors,
        dirty_accounts,
    );
    let (trades_result, events_result) = tokio::join!(trades, events);
    trades_result?;
    events_result?;
    bail!("QuickNode Hyperliquid gRPC supervisors ended")
}

#[allow(clippy::too_many_arguments)]
async fn supervise_stream<R: Repository + Clone, S: ObservationSink>(
    stream: HyperliquidStream,
    channel: Channel,
    metadata: MetadataValue<tonic::metadata::Ascii>,
    endpoint: String,
    initial_lookback_blocks: u64,
    indexer: Indexer<R, S>,
    accounts: AccountRegistry,
    cursors: StreamCursors,
    dirty_accounts: broadcast::Sender<String>,
) -> Result<()> {
    let mut backoff = Duration::from_secs(1);
    loop {
        let started = std::time::Instant::now();
        let result = run_stream(
            stream,
            channel.clone(),
            metadata.clone(),
            endpoint.clone(),
            initial_lookback_blocks,
            indexer.clone(),
            accounts.clone(),
            cursors.clone(),
            dirty_accounts.clone(),
        )
        .await;
        match result {
            Ok(()) => tracing::warn!(stream = stream.name(), "Hyperliquid gRPC stream ended"),
            Err(error) => tracing::error!(
                stream = stream.name(),
                %error,
                "Hyperliquid gRPC stream failed; its in-memory cursor remains fail-closed"
            ),
        }
        if started.elapsed() > Duration::from_secs(60) {
            backoff = Duration::from_secs(1);
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

fn client(channel: Channel) -> StreamingClient<Channel> {
    StreamingClient::new(channel)
        .accept_compressed(CompressionEncoding::Zstd)
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
}

#[allow(clippy::too_many_arguments)]
async fn run_stream<R: Repository, S: ObservationSink>(
    stream: HyperliquidStream,
    channel: Channel,
    metadata: MetadataValue<tonic::metadata::Ascii>,
    endpoint: String,
    initial_lookback_blocks: u64,
    indexer: Indexer<R, S>,
    accounts: AccountRegistry,
    cursors: StreamCursors,
    dirty_accounts: broadcast::Sender<String>,
) -> Result<()> {
    let mut cursor = cursors.get(stream);
    if cursor.is_none()
        && let Some(block) = indexer
            .hyperliquid_resume_block(stream.observation_stream())
            .await?
    {
        // KV records one observation, not completion of its whole block. Rewind
        // one so the normal gap-replay path includes any unpublished remainder.
        let previous = block
            .checked_sub(1)
            .context("Hyperliquid resume block must be positive")?;
        cursors.advance(stream, None, previous)?;
        cursor = Some(previous);
        tracing::info!(
            stream = stream.name(),
            block,
            "restored Hyperliquid resume block; replaying inclusively"
        );
    }
    let (requests, request_stream) = request_stream(stream);
    let mut request = Request::new(request_stream);
    request.metadata_mut().insert("x-token", metadata);
    let mut responses = client(channel)
        .stream_data_bytes(request)
        .await
        .with_context(|| format!("subscribe to Hyperliquid {} stream", stream.name()))?
        .into_inner();
    tracing::info!(
        stream = stream.name(),
        cursor,
        "Hyperliquid gRPC stream subscribed"
    );
    loop {
        let update = tokio::time::timeout(READ_IDLE_TIMEOUT, responses.message())
            .await
            .with_context(|| format!("Hyperliquid {} stream read idle", stream.name()))??
            .with_context(|| format!("Hyperliquid {} stream closed", stream.name()))?;
        match update.update {
            Some(subscribe_bytes_update::Update::Pong(_)) => {}
            Some(subscribe_bytes_update::Update::Data(data)) => {
                if cursor.is_none() && initial_lookback_blocks > 0 {
                    let backfill_from = data.block_number.saturating_sub(initial_lookback_blocks);
                    let bootstrap_cursor = backfill_from.saturating_sub(1);
                    cursors.advance(stream, None, bootstrap_cursor)?;
                    cursor = Some(bootstrap_cursor);
                    tracing::info!(
                        stream = stream.name(),
                        bootstrap_cursor,
                        live_block = data.block_number,
                        "initialized Hyperliquid stream cursor for bounded backfill"
                    );
                }
                if let Some(previous) = cursor {
                    if data.block_number < previous {
                        bail!(
                            "Hyperliquid {} stream block {} is behind in-memory cursor {previous}",
                            stream.name(),
                            data.block_number
                        )
                    }
                    // Any gap — bootstrap or mid-stream skip — is recovered via RPC.
                    if data.block_number > previous.saturating_add(1) {
                        replay_range(
                            stream,
                            previous.saturating_add(1),
                            data.block_number - 1,
                            Some(&cursors),
                            &indexer,
                            &accounts,
                            &endpoint,
                            &dirty_accounts,
                        )
                        .await?;
                        cursor = Some(data.block_number - 1);
                    }
                }
                let payload = decode_payload(&data.data)?;
                cursor = Some(
                    commit_payload(
                        stream,
                        cursor,
                        data.block_number,
                        payload,
                        &indexer,
                        &accounts,
                        &cursors,
                        &endpoint,
                        &dirty_accounts,
                    )
                    .await?,
                );
            }
            None => bail!(
                "Hyperliquid {} stream returned an empty update",
                stream.name()
            ),
        }
        if requests.is_closed() {
            bail!("Hyperliquid {} request stream stopped", stream.name())
        }
    }
}

fn request_stream(
    stream: HyperliquidStream,
) -> (
    mpsc::Sender<SubscribeRequest>,
    ReceiverStream<SubscribeRequest>,
) {
    let (sender, receiver) = mpsc::channel(4);
    let initial = SubscribeRequest {
        request: Some(subscribe_request::Request::Subscribe(StreamSubscribe {
            stream_type: match stream {
                HyperliquidStream::Trades => StreamType::Trades as i32,
                HyperliquidStream::Events => StreamType::Events as i32,
            },
            // QuickNode currently starts at the live tip even when this is set.
            // Durable recovery is handled with hl_getBatchBlocks below.
            start_block: 0,
            filters: HashMap::new(),
            filter_name: format!("tackt_{}", stream.name()),
        })),
    };
    let producer = sender.clone();
    tokio::spawn(async move {
        if producer.send(initial).await.is_err() {
            return;
        }
        let mut interval = tokio::time::interval(PING_INTERVAL);
        interval.tick().await;
        loop {
            interval.tick().await;
            let timestamp = chrono::Utc::now().timestamp_millis();
            if producer
                .send(SubscribeRequest {
                    request: Some(subscribe_request::Request::Ping(Ping { timestamp })),
                })
                .await
                .is_err()
            {
                return;
            }
        }
    });
    (sender, ReceiverStream::new(receiver))
}

/// Reprocess a bounded overlap after the account registry changes. Cursors do
/// not move; event-id dedup makes the replay safe.
pub async fn replay_registry_overlap<R: Repository, S: ObservationSink>(
    lookback_blocks: u64,
    indexer: &Indexer<R, S>,
    accounts: &AccountRegistry,
    cursors: &StreamCursors,
    endpoint: &str,
    dirty_accounts: &broadcast::Sender<String>,
) -> Result<u64> {
    let generation = accounts.generation.load(Ordering::Acquire);
    let mut total = 0;
    if lookback_blocks > 0 {
        for stream in [HyperliquidStream::Trades, HyperliquidStream::Events] {
            let Some(through) = cursors.get(stream) else {
                continue;
            };
            let from = through.saturating_sub(lookback_blocks.saturating_sub(1));
            total += replay_range(
                stream,
                from,
                through,
                None,
                indexer,
                accounts,
                endpoint,
                dirty_accounts,
            )
            .await?;
        }
        tracing::info!(
            published = total,
            "Hyperliquid account-registry overlap replay completed"
        );
    }
    accounts.mark_replayed(generation);
    Ok(total)
}

/// Fetch and persist blocks `from..=through` in pages. With `advance` the
/// shared cursor moves past every page (gap backfill); without it cursors stay
/// untouched. Oversized pages shrink until single blocks fit the RPC cap.
#[allow(clippy::too_many_arguments)]
async fn replay_range<R: Repository, S: ObservationSink>(
    stream: HyperliquidStream,
    from: u64,
    through: u64,
    advance: Option<&StreamCursors>,
    indexer: &Indexer<R, S>,
    accounts: &AccountRegistry,
    endpoint: &str,
    dirty_accounts: &broadcast::Sender<String>,
) -> Result<u64> {
    let mut published = 0;
    let mut cursor = from.saturating_sub(1);
    let mut span = BACKFILL_BLOCKS_PER_REQUEST;
    let mut page_from = from;
    while page_from <= through {
        let page_to = through.min(page_from.saturating_add(span - 1));
        let batch = indexer
            .hyperliquid
            .batch_blocks(stream.name(), page_from, page_to)
            .await;
        let mut blocks = match batch {
            Err(error) if span > 1 && error.downcast_ref::<ResponseTooLarge>().is_some() => {
                span /= 2;
                tracing::warn!(
                    stream = stream.name(),
                    span,
                    "Hyperliquid replay page too large; halving"
                );
                continue;
            }
            batch => batch?,
        };
        blocks.sort_unstable_by_key(|block| block["block_number"].as_u64().unwrap_or_default());
        // A missing historical block is not proof of an empty block. Never
        // advance past unavailable history after restoring a durable position.
        validate_replay_page(&blocks, page_from, page_to)?;
        // Validation establishes the block numbers and their order once.
        for (number, block) in (page_from..=page_to).zip(blocks) {
            if advance.is_some() && number <= cursor {
                continue;
            }
            published += persist_block(
                stream,
                number,
                BlockPayload::Parsed(block),
                indexer,
                accounts,
                endpoint,
                dirty_accounts,
            )
            .await?;
            if let Some(cursors) = advance {
                cursors.advance(stream, Some(cursor), number)?;
                cursor = number;
            }
        }
        if advance.is_some() {
            tracing::info!(
                stream = stream.name(),
                cursor,
                "Hyperliquid stream backfill committed"
            );
        }
        if page_to == through {
            break;
        }
        page_from = page_to.saturating_add(1);
    }
    Ok(published)
}

fn validate_replay_page(blocks: &[serde_json::Value], from: u64, to: u64) -> Result<()> {
    anyhow::ensure!(
        blocks.len() as u64 == to - from + 1,
        "Hyperliquid replay history incomplete for blocks {from}..={to}; refusing to skip the gap"
    );
    for (offset, block) in blocks.iter().enumerate() {
        anyhow::ensure!(
            block["block_number"].as_u64() == Some(from + offset as u64),
            "Hyperliquid replay history is not contiguous for blocks {from}..={to}"
        );
    }
    Ok(())
}

enum BlockPayload {
    Encoded(Vec<u8>),
    Parsed(serde_json::Value),
}

async fn persist_block<R: Repository, S: ObservationSink>(
    stream: HyperliquidStream,
    block: u64,
    payload: BlockPayload,
    indexer: &Indexer<R, S>,
    accounts: &AccountRegistry,
    endpoint: &str,
    dirty_accounts: &broadcast::Sender<String>,
) -> Result<u64> {
    let accounts = accounts.snapshot();
    let endpoint = endpoint.to_owned();
    let normalized = tokio::task::spawn_blocking(move || {
        let mut normalized = match payload {
            BlockPayload::Encoded(bytes) => {
                normalize_block(stream, block, &bytes, &accounts.owners, &endpoint)?
            }
            BlockPayload::Parsed(raw) => {
                stream::normalize_parsed_block(stream, block, &raw, &accounts.owners, &endpoint)?
            }
        };
        retain_active_observations(&mut normalized, &accounts);
        Ok::<_, anyhow::Error>(normalized)
    })
    .await
    .context("Hyperliquid normalization task panicked")??;
    let published = indexer
        .publish_hyperliquid_observations(&normalized.observations)
        .await?;
    for account in normalized.dirty_accounts {
        // Periodic full reconciliation covers sends without a live receiver.
        let _ = dirty_accounts.send(account);
    }
    Ok(published)
}

#[allow(clippy::too_many_arguments)]
async fn commit_payload<R: Repository, S: ObservationSink>(
    stream: HyperliquidStream,
    expected_cursor: Option<u64>,
    block: u64,
    payload: Vec<u8>,
    indexer: &Indexer<R, S>,
    accounts: &AccountRegistry,
    cursors: &StreamCursors,
    endpoint: &str,
    dirty_accounts: &broadcast::Sender<String>,
) -> Result<u64> {
    let published = persist_block(
        stream,
        block,
        BlockPayload::Encoded(payload),
        indexer,
        accounts,
        endpoint,
        dirty_accounts,
    )
    .await?;
    cursors.advance(stream, expected_cursor, block)?;
    tracing::debug!(
        stream = stream.name(),
        block,
        published,
        "Hyperliquid stream block persisted"
    );
    Ok(block)
}

fn retain_active_observations(
    normalized: &mut stream::NormalizedBlock,
    accounts: &RegisteredAccounts,
) {
    normalized.observations.retain(|observation| {
        observation.account.as_ref().is_some_and(|account| {
            accounts
                .active_since
                .get(account.external_id())
                .is_some_and(|active_since| observation.source_timestamp >= *active_since)
        })
    });
}

fn decode_payload(data: &[u8]) -> Result<Vec<u8>> {
    if !data.starts_with(&ZSTD_MAGIC) {
        if data.len() > MAX_MESSAGE_BYTES {
            bail!("Hyperliquid stream payload exceeds {MAX_MESSAGE_BYTES} bytes")
        }
        return Ok(data.to_vec());
    }
    let decoder = zstd::stream::read::Decoder::new(Cursor::new(data))
        .context("open Hyperliquid zstd payload")?;
    let mut payload = Vec::new();
    decoder
        .take((MAX_MESSAGE_BYTES + 1) as u64)
        .read_to_end(&mut payload)
        .context("decompress Hyperliquid stream payload")?;
    if payload.len() > MAX_MESSAGE_BYTES {
        bail!("decompressed Hyperliquid stream payload exceeds {MAX_MESSAGE_BYTES} bytes")
    }
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt as _;

    #[test]
    fn stream_cursors_are_process_local_and_compare_and_swap() {
        let cursors = StreamCursors::default();
        assert_eq!(cursors.get(HyperliquidStream::Trades), None);
        cursors
            .advance(HyperliquidStream::Trades, None, 10)
            .unwrap();
        assert!(
            cursors
                .advance(HyperliquidStream::Trades, None, 11)
                .is_err()
        );
        cursors
            .advance(HyperliquidStream::Trades, Some(10), 12)
            .unwrap();
        assert_eq!(cursors.get(HyperliquidStream::Trades), Some(12));
        assert_eq!(
            StreamCursors::default().get(HyperliquidStream::Trades),
            None
        );
    }

    #[test]
    fn decompresses_zstd_and_accepts_plain_json() {
        let raw = br#"{"block_number":1,"events":[]}"#;
        assert_eq!(decode_payload(raw).unwrap(), raw);
        let compressed = zstd::stream::encode_all(Cursor::new(raw), 1).unwrap();
        assert_eq!(decode_payload(&compressed).unwrap(), raw);
    }

    #[tokio::test]
    async fn account_registry_tracks_replay_across_lifecycle_changes() {
        let repository = tackt_postgres::MemoryRepository::default();
        let registry: AccountRegistry = Arc::new(Default::default());
        let user = tackt_primitives::User {
            id: Uuid::new_v4(),
            external_id: "registry-test".into(),
            enabled: true,
            ethereum_addresses: vec![],
            hyperliquid_accounts: vec!["0x1111111111111111111111111111111111111111".into()],
        };
        repository.insert_user(user.clone()).await;
        let refresh = refresh_accounts(&repository, &registry).await.unwrap();
        assert!(refresh.changed);
        assert!(registry.needs_replay());
        assert_eq!(
            registry
                .snapshot()
                .owners
                .get(&user.hyperliquid_accounts[0]),
            Some(&user.id)
        );

        let generation = registry.generation.load(Ordering::Acquire);
        registry.mark_replayed(generation);
        assert!(!registry.needs_replay());

        let mut disabled = user;
        disabled.enabled = false;
        repository.insert_user(disabled).await;
        let refresh = refresh_accounts(&repository, &registry).await.unwrap();
        assert!(refresh.changed);
        assert!(registry.needs_replay());
        assert!(registry.snapshot().owners.is_empty());
    }

    #[test]
    fn replay_does_not_attribute_activity_before_registration() {
        let account = "0x1111111111111111111111111111111111111111".to_owned();
        let user = Uuid::new_v4();
        let registered = RegisteredAccounts {
            owners: HashMap::from([(account.clone(), user)]),
            active_since: HashMap::from([(
                account.clone(),
                chrono::DateTime::from_timestamp_millis(1_764_867_600_519).unwrap(),
            )]),
        };
        let payload = serde_json::to_vec(&serde_json::json!({
            "block_number": 42,
            "events": [[account, {
                "coin":"BTC", "sz":"0.2", "time":1_764_867_600_518_i64,
                "tid":1081368440085115_u64, "fee":"0", "feeToken":"USDC"
            }]]
        }))
        .unwrap();
        let mut normalized = normalize_block(
            HyperliquidStream::Trades,
            42,
            &payload,
            &registered.owners,
            "fixture",
        )
        .unwrap();
        assert_eq!(normalized.observations.len(), 1);
        retain_active_observations(&mut normalized, &registered);
        assert!(normalized.observations.is_empty());
    }

    #[tokio::test]
    async fn request_stream_subscribes_without_filters_and_then_pings() {
        let (_sender, mut requests) = request_stream(HyperliquidStream::Trades);
        let first = tokio_stream::StreamExt::next(&mut requests).await.unwrap();
        let subscribe = match first.request.unwrap() {
            subscribe_request::Request::Subscribe(subscribe) => subscribe,
            _ => panic!("expected subscription"),
        };
        assert_eq!(subscribe.stream_type, StreamType::Trades as i32);
        assert_eq!(subscribe.start_block, 0);
        assert!(subscribe.filters.is_empty());
        assert_eq!(subscribe.filter_name, "tackt_trades");
    }

    #[derive(Clone, Default)]
    struct MockStreaming {
        stream_types: Arc<std::sync::Mutex<Vec<i32>>>,
        live_blocks: Arc<std::sync::Mutex<HashMap<i32, serde_json::Value>>>,
    }

    type MockStream<T> = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = std::result::Result<T, tonic::Status>> + Send>,
    >;

    #[tonic::async_trait]
    impl proto::streaming_server::Streaming for MockStreaming {
        type StreamDataStream = MockStream<proto::SubscribeUpdate>;

        async fn stream_data(
            &self,
            _request: Request<tonic::Streaming<SubscribeRequest>>,
        ) -> std::result::Result<tonic::Response<Self::StreamDataStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("use StreamDataBytes"))
        }

        type StreamDataBytesStream = MockStream<proto::SubscribeBytesUpdate>;

        async fn stream_data_bytes(
            &self,
            request: Request<tonic::Streaming<SubscribeRequest>>,
        ) -> std::result::Result<tonic::Response<Self::StreamDataBytesStream>, tonic::Status>
        {
            assert_eq!(request.metadata().get("x-token").unwrap(), "secret");
            let mut requests = request.into_inner();
            let first = requests
                .message()
                .await?
                .ok_or_else(|| tonic::Status::invalid_argument("missing subscription"))?;
            let subscribe = match first.request {
                Some(subscribe_request::Request::Subscribe(subscribe)) => subscribe,
                _ => return Err(tonic::Status::invalid_argument("expected subscription")),
            };
            assert!(subscribe.filters.is_empty());
            self.stream_types
                .lock()
                .unwrap()
                .push(subscribe.stream_type);
            let block_number = if subscribe.stream_type == StreamType::Trades as i32 {
                42
            } else {
                43
            };
            let block = self
                .live_blocks
                .lock()
                .unwrap()
                .get(&subscribe.stream_type)
                .cloned()
                .unwrap_or_else(|| {
                    serde_json::json!({
                        "block_number": block_number,
                        "events": []
                    })
                });
            let block_number = block["block_number"].as_u64().unwrap();
            let payload = serde_json::to_vec(&block).unwrap();
            let update = proto::SubscribeBytesUpdate {
                update: Some(subscribe_bytes_update::Update::Data(
                    proto::StreamBytesResponse {
                        block_number,
                        timestamp: 1,
                        data: payload,
                    },
                )),
            };
            let responses = futures_util::stream::once(async move { Ok(update) })
                .chain(futures_util::stream::pending());
            Ok(tonic::Response::new(Box::pin(responses)))
        }

        async fn ping(
            &self,
            request: Request<PingRequest>,
        ) -> std::result::Result<tonic::Response<proto::PingResponse>, tonic::Status> {
            assert_eq!(request.metadata().get("x-token").unwrap(), "secret");
            Ok(tonic::Response::new(proto::PingResponse {
                count: request.into_inner().count,
            }))
        }
    }

    #[tokio::test]
    async fn supervisor_authenticates_and_subscribes_to_only_trades_and_events() {
        use crate::source::{
            evm::adapter::EthereumAdapter, hyperliquid::adapter::HyperliquidAdapter,
        };
        use tackt_postgres::MemoryRepository;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mock = MockStreaming::default();
        let stream_types = mock.stream_types.clone();
        let service = proto::streaming_server::StreamingServer::new(mock)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Zstd);
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let repository = Arc::new(MemoryRepository::default());
        let endpoint = format!("http://{address}");
        let indexer = Indexer::new(
            repository.clone(),
            crate::sink::MemoryObservationSink::default(),
            vec![EthereumAdapter::new("http://127.0.0.1:1".into(), 1)],
            HyperliquidAdapter::new(&endpoint, "secret").unwrap(),
        );
        let accounts: AccountRegistry = Arc::new(Default::default());
        let cursors = StreamCursors::default();
        let observed_cursors = cursors.clone();
        let (dirty, _) = broadcast::channel(4);
        let worker = tokio::spawn(supervise(
            endpoint,
            "secret".into(),
            0,
            indexer,
            accounts,
            cursors,
            dirty,
        ));
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if observed_cursors.get(HyperliquidStream::Trades) == Some(42)
                    && observed_cursors.get(HyperliquidStream::Events) == Some(43)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let mut observed = stream_types.lock().unwrap().clone();
        observed.sort_unstable();
        assert_eq!(
            observed,
            vec![StreamType::Trades as i32, StreamType::Events as i32]
        );
        worker.abort();
        server.abort();
    }

    #[test]
    fn replay_refuses_missing_duplicate_or_misnumbered_blocks() {
        let block = |number| serde_json::json!({"block_number": number, "events": []});
        assert!(validate_replay_page(&[block(10), block(11)], 10, 11).is_ok());
        assert!(validate_replay_page(&[block(11)], 10, 11).is_err());
        assert!(validate_replay_page(&[block(10), block(10)], 10, 11).is_err());
        assert!(validate_replay_page(&[block(10), block(12)], 10, 11).is_err());
    }

    #[tokio::test]
    async fn restart_replays_partial_block_beyond_lookback() {
        use super::adapter::HyperliquidAdapter;
        use axum::{Json, Router, extract::State, routing::post};
        use std::sync::{Mutex, atomic::AtomicUsize};
        use tackt_nats::{HyperliquidObservationStream, ObservationEvent, PublishOutcome};
        use tackt_postgres::MemoryRepository;

        // One acknowledged observation followed by a publish failure models a
        // process dying before the rest of this block reaches durable storage.
        #[derive(Clone, Default)]
        struct PartialSink {
            durable: crate::sink::MemoryObservationSink,
            attempts: Arc<AtomicUsize>,
        }
        impl ObservationSink for PartialSink {
            async fn publish(&self, event: &ObservationEvent) -> Result<PublishOutcome> {
                if self.attempts.fetch_add(1, Ordering::SeqCst) == 1 {
                    bail!("simulated publish failure");
                }
                self.durable.publish(event).await
            }
            async fn publish_batch(&self, event: &ObservationEvent) -> Result<u64> {
                self.durable.publish_batch(event).await
            }
            async fn last_evm_event(&self, chain: u64) -> Result<Option<(u64, ObservationEvent)>> {
                self.durable.last_evm_event(chain).await
            }
            async fn hyperliquid_resume_block(
                &self,
                stream: HyperliquidObservationStream,
            ) -> Result<Option<u64>> {
                self.durable.hyperliquid_resume_block(stream).await
            }
        }
        #[derive(Clone)]
        struct History {
            partial: serde_json::Value,
            ranges: Arc<Mutex<Vec<(u64, u64)>>>,
        }
        async fn rpc(
            State(history): State<History>,
            Json(request): Json<serde_json::Value>,
        ) -> Json<serde_json::Value> {
            assert_eq!(request["method"], "hl_getBatchBlocks");
            assert_eq!(request["params"]["stream"], "trades");
            let from = request["params"]["from"].as_u64().unwrap();
            let to = request["params"]["to"].as_u64().unwrap();
            history.ranges.lock().unwrap().push((from, to));
            let blocks: Vec<_> = (from..=to)
                .map(|height| {
                    if height == 100 {
                        history.partial.clone()
                    } else {
                        serde_json::json!({"block_number": height, "events": []})
                    }
                })
                .collect();
            Json(serde_json::json!({"jsonrpc":"2.0", "id":1, "result":{"blocks":blocks}}))
        }
        let account = "0x1111111111111111111111111111111111111111".to_owned();
        let user = Uuid::new_v4();
        let fill = |tid: u64| {
            serde_json::json!([account, {
                "coin":"BTC", "sz":"0.2", "time":1_764_867_600_518_i64,
                "tid":tid, "fee":"0", "feeToken":"USDC"
            }])
        };
        let partial = serde_json::json!({"block_number":100,"events":[fill(1), fill(2)]});
        let history = History {
            partial: partial.clone(),
            ranges: Arc::new(Mutex::new(Vec::new())),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rpc_url = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/secret/hypercore", post(rpc))
            .with_state(history.clone());
        let http_server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let sink = PartialSink::default();
        let make_indexer = || {
            Indexer::new(
                Arc::new(MemoryRepository::default()),
                sink.clone(),
                vec![],
                HyperliquidAdapter::new(&rpc_url, "secret").unwrap(),
            )
        };
        let accounts: AccountRegistry = Arc::new(Default::default());
        *accounts.accounts.write().unwrap() = Arc::new(RegisteredAccounts {
            owners: HashMap::from([(account.clone(), user)]),
            active_since: HashMap::from([(account, chrono::DateTime::UNIX_EPOCH)]),
        });
        let old_cursors = StreamCursors::default();
        let (dirty, _) = broadcast::channel(4);
        assert!(
            commit_payload(
                HyperliquidStream::Trades,
                None,
                100,
                serde_json::to_vec(&partial).unwrap(),
                &make_indexer(),
                &accounts,
                &old_cursors,
                "fixture",
                &dirty
            )
            .await
            .is_err()
        );
        assert_eq!(old_cursors.get(HyperliquidStream::Trades), None);
        assert_eq!(sink.durable.published().len(), 1);

        let grpc_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let grpc_url = format!("http://{}", grpc_listener.local_addr().unwrap());
        let mock = MockStreaming::default();
        mock.live_blocks.lock().unwrap().insert(
            StreamType::Trades as i32,
            serde_json::json!({"block_number":110,"events":[]}),
        );
        let grpc_server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(proto::streaming_server::StreamingServer::new(mock))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                    grpc_listener,
                ))
                .await
                .unwrap();
        });
        let (channel, metadata) = authenticated_channel(&grpc_url, "secret").await.unwrap();
        let restored = StreamCursors::default();
        let worker = tokio::spawn(run_stream(
            HyperliquidStream::Trades,
            channel,
            metadata,
            grpc_url,
            1,
            make_indexer(),
            accounts,
            restored.clone(),
            dirty,
        ));
        tokio::time::timeout(Duration::from_secs(3), async {
            while restored.get(HyperliquidStream::Trades) != Some(110) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            *history.ranges.lock().unwrap(),
            vec![(100, 109)],
            "recovery must include the partial block, ignoring the one-block bootstrap limit"
        );
        assert_eq!(
            sink.durable.published().len(),
            2,
            "the first fill is a duplicate; the unpublished second fill is recovered"
        );
        worker.abort();
        let _ = worker.await;
        http_server.abort();
        grpc_server.abort();
    }
}

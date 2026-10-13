//! Generic JetStream helpers: stream provisioning, acked publishes, and typed
//! pull consumers. Services never touch `async_nats` types. Delivery is
//! at-least-once: consumers must be idempotent, publishers await server acks.

use std::marker::PhantomData;
use std::time::Duration;

use anyhow::{Context as _, bail};
use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy, PullConsumer, pull};
use async_nats::jetstream::stream::{Config as StreamConfig, DiscardPolicy, RetentionPolicy};
use futures_util::StreamExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::NatsClient;

/// Stream retention. Immutable server-side: [`NatsClient::ensure_stream`]
/// fails hard on a mismatch (manual migration required).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Retention {
    /// Retained to the configured limits; every consumer sees every message.
    Limits,
    /// Deleted once acked; exactly one consumer group per subject filter.
    WorkQueue,
}

/// What the server does when a stream reaches a capacity limit.
///
/// Never age: `max_age` expires the oldest messages under either policy. This
/// decides only what happens when `max_bytes` or `max_messages` is full.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Discard {
    /// Delete the oldest messages to make room for the publish.
    Old,
    /// Reject the publish and keep what is already stored. Streams whose
    /// backlog is unrecoverable evidence use this: a producer that cannot hand
    /// off learns so and fails closed, instead of the broker quietly dropping
    /// messages no consumer has processed yet.
    New,
}

/// Declarative stream configuration. Owned names let tests build unique streams.
#[derive(Clone, Debug)]
pub struct StreamSpec {
    pub name: String,
    pub subjects: Vec<String>,
    pub retention: Retention,
    /// `None` = unlimited.
    pub max_age: Option<Duration>,
    /// `None` = unlimited.
    pub max_messages: Option<i64>,
    /// `None` = unlimited.
    pub max_bytes: Option<i64>,
    /// Behavior when a capacity limit above is reached.
    pub discard: Discard,
    /// Server-side `Nats-Msg-Id` dedupe window. Must be non-zero: the server
    /// rewrites zero to its default, which would make `ensure_stream`'s
    /// config diff fire an update on every boot.
    pub duplicate_window: Duration,
}

impl StreamSpec {
    /// The single subject of a one-subject stream; publish helpers need one.
    pub fn subject(&self) -> &str {
        debug_assert_eq!(
            self.subjects.len(),
            1,
            "stream {} is multi-subject",
            self.name
        );
        &self.subjects[0]
    }

    /// Overlay the spec-owned fields onto `config`, leaving operator-managed
    /// fields (replicas, placement, storage) untouched.
    fn apply_to(&self, config: &mut StreamConfig) {
        config.name = self.name.clone();
        config.subjects = self.subjects.clone();
        config.retention = match self.retention {
            Retention::Limits => RetentionPolicy::Limits,
            Retention::WorkQueue => RetentionPolicy::WorkQueue,
        };
        config.max_age = self.max_age.unwrap_or(Duration::ZERO);
        config.max_messages = self.max_messages.unwrap_or(-1);
        config.max_bytes = self.max_bytes.unwrap_or(-1);
        config.discard = match self.discard {
            Discard::Old => DiscardPolicy::Old,
            Discard::New => DiscardPolicy::New,
        };
        config.duplicate_window = self.duplicate_window;
    }

    fn to_config(&self) -> StreamConfig {
        let mut config = StreamConfig::default();
        self.apply_to(&mut config);
        config
    }
}

/// A capacity limit where any non-positive value means unlimited: JetStream
/// reports an absent `max_bytes` or `max_messages` as `-1`, and older servers
/// have written `0`.
const fn limit_narrows(existing: i64, desired: i64) -> bool {
    if desired <= 0 {
        return false;
    }
    existing <= 0 || desired < existing
}

/// The same rule for `max_age`, where unlimited is [`Duration::ZERO`].
const fn age_narrows(existing: Duration, desired: Duration) -> bool {
    if desired.is_zero() {
        return false;
    }
    existing.is_zero() || desired.as_nanos() < existing.as_nanos()
}

/// Describes the first change from `existing` to `desired` that could destroy
/// messages the stream is currently holding, or `None` when the change only
/// widens what the stream keeps.
///
/// [`NatsClient::ensure_stream`] converges widening automatically because a
/// bigger, longer, or broader stream cannot lose anything. Narrowing is
/// refused for every stream rather than only the fail-closed ones: a limit a
/// deployment shrinks is applied by the server immediately, so a typo in a
/// spec would delete retained messages during a routine boot with no operator
/// in the loop. Deliberate narrowing stays possible — it just has to be done
/// against the server, where the data loss is visible.
fn narrowing_change(existing: &StreamConfig, desired: &StreamConfig) -> Option<String> {
    if existing.retention != desired.retention {
        return Some(format!(
            "retention {:?} -> {:?}",
            existing.retention, desired.retention
        ));
    }
    if existing.discard != desired.discard {
        return Some(format!(
            "discard policy {:?} -> {:?}",
            existing.discard, desired.discard
        ));
    }
    if let Some(dropped) = existing
        .subjects
        .iter()
        .find(|subject| !desired.subjects.contains(subject))
    {
        return Some(format!("subject {dropped} is no longer captured"));
    }
    if age_narrows(existing.max_age, desired.max_age) {
        return Some(format!(
            "max_age {:?} -> {:?}",
            existing.max_age, desired.max_age
        ));
    }
    if limit_narrows(existing.max_bytes, desired.max_bytes) {
        return Some(format!(
            "max_bytes {} -> {}",
            existing.max_bytes, desired.max_bytes
        ));
    }
    if limit_narrows(existing.max_messages, desired.max_messages) {
        return Some(format!(
            "max_messages {} -> {}",
            existing.max_messages, desired.max_messages
        ));
    }
    None
}

/// What the server did with an acknowledged durable publish.
///
/// Both variants mean the message is durable; they differ only in whether this
/// call is what made it so. Producers that publish under a deterministic
/// message id use the distinction for observability — a steady stream of
/// duplicates means a source is replaying — never to decide whether the
/// handoff succeeded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishOutcome {
    /// The server stored this message.
    Stored,
    /// The server matched the message id against one already in the stream's
    /// duplicate window and stored nothing further.
    Duplicate,
}

/// Where a newly created consumer starts reading.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartPosition {
    /// From the first retained message.
    DeliverAll,
    /// Only messages published after the consumer exists. For signal-style
    /// consumers where history is worthless.
    New,
    /// From this stream sequence (inclusive). The consumer is recreated at
    /// this position even if the durable exists: callers persisting their own
    /// cursor must never resume from server state, which can be ahead of it.
    /// Replaying is safe for idempotent consumers; skipping is not.
    BySequence(u64),
}

/// Pull-consumer configuration.
#[derive(Clone, Debug)]
pub struct ConsumerSpec {
    /// `None` = ephemeral: per-instance, auto-cleaned when idle.
    pub durable_name: Option<String>,
    /// Server redelivery timeout for unacked messages.
    pub ack_wait: Duration,
    /// `None` = unlimited redelivery: a message leaves the consumer's view
    /// only through an explicit ack or term. Set a limit only where losing an
    /// unprocessed message is acceptable — a delivery ceiling drops it with no
    /// consumer having decided anything about it.
    pub max_deliveries: Option<u32>,
    pub start: StartPosition,
    /// Bounds outstanding deliveries; ordered accounting consumers use one.
    pub max_ack_pending: Option<i64>,
}

impl ConsumerSpec {
    /// Durable consumer reading from the stream start, 30s ack wait,
    /// unlimited redelivery.
    ///
    /// Binding an existing durable does not converge its configuration
    /// (deliver policy is immutable server-side); config changes require
    /// recreating the durable.
    pub fn durable(name: &str) -> Self {
        Self {
            durable_name: Some(name.to_string()),
            ack_wait: Duration::from_secs(30),
            max_deliveries: None,
            start: StartPosition::DeliverAll,
            max_ack_pending: None,
        }
    }

    /// Ephemeral consumer receiving only new messages; the server removes it
    /// after idle disconnection.
    pub fn ephemeral() -> Self {
        Self {
            durable_name: None,
            ack_wait: Duration::from_secs(30),
            max_deliveries: None,
            start: StartPosition::New,
            max_ack_pending: None,
        }
    }
}

/// Generic typed envelope for payload types owned outside this crate
/// (e.g. the risk streams; see the crate docs).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Envelope<T> {
    pub payload: T,
}

impl<T> Envelope<T> {
    pub fn new(payload: T) -> Self {
        Self { payload }
    }
}

impl NatsClient {
    /// Read-only startup check. Provisioning is an explicit operator action, never a runtime fallback.
    pub async fn require_stream(&self, spec: &StreamSpec) -> anyhow::Result<()> {
        let stream = self
            .jetstream
            .get_stream(&spec.name)
            .await
            .with_context(|| {
                format!(
                    "required stream {} is unavailable; provision it before starting the service",
                    spec.name
                )
            })?;
        let existing = &stream.cached_info().config;
        let mut desired = existing.clone();
        spec.apply_to(&mut desired);
        anyhow::ensure!(
            existing == &desired,
            "required stream {} has incompatible configuration; apply the deployment configuration before startup",
            spec.name
        );
        Ok(())
    }

    /// Create the stream if absent, else converge only the spec-owned fields
    /// (subjects, limits, discard policy, dedupe window). Operator-managed
    /// fields — replicas, placement, storage — are preserved.
    ///
    /// Idempotent: re-running against a stream that already matches performs no
    /// update. Only provisioning tools and test setup may call this. A configuration
    /// this build cannot reach without destroying retained messages — see
    /// [`narrowing_change`] — is a hard error rather than a silent convergence.
    pub async fn ensure_stream(&self, spec: &StreamSpec) -> anyhow::Result<()> {
        // The server rewrites a zero window to its default, which would make
        // the config diff below fire an update on every boot.
        if spec.duplicate_window.is_zero() {
            bail!("stream {} needs a non-zero duplicate_window", spec.name);
        }
        let mut stream = self
            .jetstream
            .get_or_create_stream(spec.to_config())
            .await
            .with_context(|| format!("creating stream {}", spec.name))?;
        let info = stream
            .info()
            .await
            .with_context(|| format!("inspecting stream {}", spec.name))?;
        let mut desired = info.config.clone();
        spec.apply_to(&mut desired);
        if let Some(change) = narrowing_change(&info.config, &desired) {
            bail!(
                "stream {} exists with an incompatible configuration ({change}); applying it \
                 could destroy retained messages, so migrate it manually",
                spec.name,
            );
        }
        if info.config != desired {
            self.jetstream
                .update_stream(&desired)
                .await
                .with_context(|| format!("updating stream {}", spec.name))?;
        }
        Ok(())
    }

    /// Number of retained messages currently in a durable stream.
    pub async fn stream_message_count(&self, name: &str) -> anyhow::Result<u64> {
        let mut stream = self
            .jetstream
            .get_stream(name)
            .await
            .with_context(|| format!("opening stream {name}"))?;
        Ok(stream
            .info()
            .await
            .with_context(|| format!("inspecting stream {name}"))?
            .state
            .messages)
    }

    /// Whether the durable publish path to this stream is available: the
    /// JetStream API answers for the stream right now. This is the probe
    /// behind a producer's readiness endpoint — a live core connection alone
    /// does not prove the durable handoff works, because JetStream can be
    /// down or the stream deleted while the connection stays up.
    ///
    /// Deliberately not probed: capacity rejection on a fail-closed
    /// (discard-new) stream. Whether one more publish fits depends on that
    /// message's size, so the publish path itself is the authority — it fails
    /// the ingestion pass and supervision retries under backoff.
    pub async fn stream_available(&self, name: &str) -> anyhow::Result<()> {
        // get_stream fetches the stream's info: one round trip that fails when
        // JetStream does not answer or the stream does not exist.
        // (get_stream_no_info would not do: it builds a local handle without
        // contacting the server.)
        self.jetstream
            .get_stream(name)
            .await
            .map_err(|error| anyhow::anyhow!("stream {name} unavailable: {error}"))?;
        Ok(())
    }

    /// Delete a stream. Tests and operational tooling.
    pub async fn delete_stream(&self, name: &str) -> anyhow::Result<()> {
        self.jetstream
            .delete_stream(name)
            .await
            .with_context(|| format!("deleting stream {name}"))?;
        Ok(())
    }

    /// JSON publish awaiting the server ack. `dedupe_id` becomes
    /// `Nats-Msg-Id`, letting the broker drop duplicates inside the stream's
    /// dedupe window.
    ///
    /// Returning means the server acknowledged that the message is durable: it
    /// either stored it now or recognized it as one it already holds. An error
    /// means the opposite — nothing was stored, and the caller still owns the
    /// handoff.
    pub async fn publish_durable<T: Serialize>(
        &self,
        subject: &str,
        dedupe_id: Option<&str>,
        message: &T,
    ) -> anyhow::Result<PublishOutcome> {
        self.publish_durable_traced(subject, dedupe_id, message, None)
            .await
    }

    pub(crate) async fn publish_durable_traced<T: Serialize>(
        &self,
        subject: &str,
        dedupe_id: Option<&str>,
        message: &T,
        request_id: Option<&str>,
    ) -> anyhow::Result<PublishOutcome> {
        if let Some(id) = request_id {
            anyhow::ensure!(
                !id.is_empty()
                    && id.len() <= 128
                    && id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b)),
                "invalid request ID"
            );
        }
        // Header insertion panics on control characters; ids are often
        // caller-supplied (e.g. idempotency keys), so reject them as errors
        // under the same rule the ledger enforces before committing.
        if let Some(id) = dedupe_id
            && !tackt_primitives::ledger::valid_idempotency_key(id)
        {
            bail!(
                "dedupe id must be non-blank, at most {} bytes, without control characters",
                tackt_primitives::ledger::MAX_IDEMPOTENCY_KEY_BYTES
            );
        }
        let payload = serde_json::to_vec(message).context("serializing message")?;
        let mut headers = async_nats::HeaderMap::new();
        if let Some(id) = dedupe_id {
            headers.insert(async_nats::header::NATS_MESSAGE_ID, id);
        }
        if let Some(id) = request_id {
            headers.insert("X-Request-ID", id);
        }
        let ack = self
            .jetstream
            .publish_with_headers(subject.to_string(), headers, payload.into())
            .await
            .with_context(|| format!("publishing to {subject}"))?;
        let ack = ack
            .await
            .with_context(|| format!("awaiting publish ack for {subject}"))?;
        Ok(if ack.duplicate {
            PublishOutcome::Duplicate
        } else {
            PublishOutcome::Stored
        })
    }

    /// Require an existing stream, then create (or bind) the pull consumer.
    /// Explicit cursor replay is retained for snapshot-backed consumers such as Risk.
    pub async fn consumer<T: DeserializeOwned>(
        &self,
        stream: &StreamSpec,
        spec: ConsumerSpec,
    ) -> anyhow::Result<Consumer<T>> {
        self.require_stream(stream).await?;
        let js_stream = self
            .jetstream
            .get_stream(&stream.name)
            .await
            .with_context(|| format!("looking up stream {}", stream.name))?;
        let config = pull::Config {
            durable_name: spec.durable_name.clone(),
            ack_wait: spec.ack_wait,
            // Explicit rather than left to the server default, so a server or
            // account policy change cannot start expiring undelivered work.
            max_deliver: spec.max_deliveries.map_or(-1, i64::from),
            max_ack_pending: spec.max_ack_pending.unwrap_or(0),
            // Ephemerals are per-instance; let the server reap idle ones.
            inactive_threshold: if spec.durable_name.is_none() {
                Duration::from_mins(5)
            } else {
                Duration::ZERO
            },
            deliver_policy: match spec.start {
                StartPosition::DeliverAll => DeliverPolicy::All,
                StartPosition::New => DeliverPolicy::New,
                StartPosition::BySequence(sequence) => DeliverPolicy::ByStartSequence {
                    start_sequence: sequence,
                },
            },
            ..pull::Config::default()
        };
        let name = spec.durable_name.as_deref().unwrap_or("(ephemeral)");
        let pull_consumer = match (&spec.durable_name, spec.start) {
            // An explicit position overrides server-side durable state: the
            // durable must actually be deleted before recreation, or the
            // CreateOrUpdate below silently binds the old position. A crash
            // between delete and create is safe — the next boot recreates
            // from the caller's persisted cursor.
            (Some(durable), StartPosition::BySequence(_)) => {
                if let Err(delete_error) = js_stream.delete_consumer(durable).await
                    && js_stream.consumer_info(durable).await.is_ok()
                {
                    bail!("repositioning consumer {durable}: delete failed: {delete_error}");
                }
                js_stream
                    .create_consumer(config)
                    .await
                    .with_context(|| format!("creating consumer {name}"))?
            }
            (Some(durable), _) => js_stream
                .get_or_create_consumer(durable, config)
                .await
                .with_context(|| format!("binding consumer {name}"))?,
            (None, _) => js_stream
                .create_consumer(config)
                .await
                .context("creating ephemeral consumer")?,
        };
        Consumer::open(js_stream, pull_consumer).await
    }

    /// Bind a pre-provisioned durable without creating, updating or resetting it.
    pub async fn bind_consumer<T: DeserializeOwned>(
        &self,
        stream: &StreamSpec,
        spec: ConsumerSpec,
    ) -> anyhow::Result<Consumer<T>> {
        self.require_stream(stream).await?;
        let name = spec
            .durable_name
            .as_deref()
            .context("binding requires a durable name")?;
        let js_stream = self.jetstream.get_stream(&stream.name).await?;
        let consumer: PullConsumer = js_stream.get_consumer(name).await.map_err(|error| {
            anyhow::anyhow!(
                "required consumer {name} on {} is unavailable; provision it before startup: {error}",
                stream.name
            )
        })?;
        let config = &consumer.cached_info().config;
        anyhow::ensure!(
            config.ack_policy == AckPolicy::Explicit
                && config.ack_wait == spec.ack_wait
                && config.max_deliver == spec.max_deliveries.map_or(-1, i64::from)
                && spec
                    .max_ack_pending
                    .is_none_or(|limit| config.max_ack_pending == limit)
                && config.filter_subject.is_empty()
                && config.filter_subjects.is_empty()
                && config.inactive_threshold.is_zero(),
            "required consumer {name} on {} has incompatible delivery configuration; correct it before startup",
            stream.name
        );
        Consumer::open(js_stream, consumer).await
    }
}

/// Typed pull consumer over one stream.
pub struct Consumer<T> {
    stream_name: String,
    stream_created_at: String,
    acknowledged_sequence: u64,
    messages: pull::Stream,
    _message: PhantomData<T>,
}

impl<T: DeserializeOwned> Consumer<T> {
    async fn open(
        mut stream: async_nats::jetstream::stream::Stream,
        consumer: PullConsumer,
    ) -> anyhow::Result<Self> {
        let stream_created_at = stream
            .info()
            .await?
            .created
            .unix_timestamp_nanos()
            .to_string();
        let acknowledged_sequence = consumer.cached_info().ack_floor.stream_sequence;
        let messages = consumer
            .messages()
            .await
            .context("opening consumer message stream")?;
        Ok(Self {
            stream_name: stream.cached_info().config.name.clone(),
            stream_created_at,
            acknowledged_sequence,
            messages,
            _message: PhantomData,
        })
    }
    /// Broker acknowledgement floor when the consumer was bound.
    pub fn acknowledged_sequence(&self) -> u64 {
        self.acknowledged_sequence
    }

    /// JetStream stream creation time in Unix nanoseconds; survives service/broker restarts,
    /// but changes when the stream is deleted and recreated under the same name.
    pub fn stream_created_at(&self) -> &str {
        &self.stream_created_at
    }

    /// Accounting evidence must stop at an undecodable message, never skip it.
    /// The delivery stays unacknowledged on every error so recovery can replay it.
    pub async fn next_strict(&mut self) -> anyhow::Result<Delivery<T>> {
        let message = self
            .messages
            .next()
            .await
            .context("evidence stream ended")??;
        let info = message
            .info()
            .map_err(|error| anyhow::anyhow!("evidence delivery metadata: {error}"))?;
        let handle = AckHandle {
            stream_sequence: info.stream_sequence,
            delivery_count: info.delivered,
            message,
        };
        let decoded: T = serde_json::from_slice(handle.payload())
            .context("undecodable accounting evidence; delivery remains unacknowledged")?;
        Ok(Delivery {
            message: decoded,
            handle,
        })
    }

    /// Next decodable message. Undecodable payloads or
    /// metadata are TERMed, logged, and
    /// skipped. `Err` = the message stream failed; callers rebuild through
    /// their supervision loop.
    pub async fn next(&mut self) -> anyhow::Result<Delivery<T>> {
        loop {
            let message = self
                .messages
                .next()
                .await
                .with_context(|| format!("message stream for {} ended", self.stream_name))?
                .with_context(|| format!("receiving from {}", self.stream_name))?;
            let handle = match message.info() {
                Ok(info) => AckHandle {
                    stream_sequence: info.stream_sequence,
                    delivery_count: info.delivered,
                    message,
                },
                // Never fabricate a sequence: a wrong cursor is worse than a
                // dropped signal.
                Err(error) => {
                    tracing::error!(%error, stream = %self.stream_name, "terminating message with undecodable metadata");
                    let _ = message.ack_with(async_nats::jetstream::AckKind::Term).await;
                    continue;
                }
            };
            let decoded: T = match serde_json::from_slice(handle.payload()) {
                Ok(decoded) => decoded,
                // A failed TERM must not tear down the consumer: skip now,
                // let the server redeliver into the same skip path.
                Err(error) => {
                    tracing::error!(%error, stream = %self.stream_name, sequence = handle.stream_sequence, "terminating undecodable message");
                    if let Err(term_error) = handle.term().await {
                        tracing::warn!(%term_error, stream = %self.stream_name, "term failed");
                    }
                    continue;
                }
            };
            return Ok(Delivery {
                message: decoded,
                handle,
            });
        }
    }
}

/// One at-least-once delivery: decoded message plus ack control.
pub struct Delivery<T> {
    message: T,
    handle: AckHandle,
}

impl<T> Delivery<T> {
    pub fn message(&self) -> &T {
        &self.message
    }

    /// Split into the owned message and the ack handle, for consumers that
    /// move the message while deferring the ack decision.
    pub fn split(self) -> (T, AckHandle) {
        (self.message, self.handle)
    }

    /// JetStream sequence in the stream.
    pub fn stream_sequence(&self) -> u64 {
        self.handle.stream_sequence
    }

    /// 1 = first delivery.
    pub fn delivery_count(&self) -> i64 {
        self.handle.delivery_count
    }

    pub async fn ack(self) -> anyhow::Result<()> {
        self.handle.ack().await
    }

    pub async fn nak(self, redeliver_after: Duration) -> anyhow::Result<()> {
        self.handle.nak(redeliver_after).await
    }

    pub async fn term(self) -> anyhow::Result<()> {
        self.handle.term().await
    }
}

/// What a consumer decided about one delivery. Centralizes the
/// acknowledgement vocabulary so services only map their domain errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disposition {
    /// Done; never redelivered.
    Ack,
    /// Permanently rejected; never redelivered.
    Term,
    /// Retry after the delay; redelivered until acked or termed.
    Nak(Duration),
}

/// Ack control detached from the decoded message. Retains the raw payload so
/// consumers can re-materialize a consumed message (the risk host's
/// poisoned-processor retry).
pub struct AckHandle {
    stream_sequence: u64,
    delivery_count: i64,
    message: async_nats::jetstream::Message,
}

impl AckHandle {
    /// The raw payload as published.
    pub fn payload(&self) -> &[u8] {
        &self.message.payload
    }

    pub async fn finish(self, disposition: Disposition) -> anyhow::Result<()> {
        match disposition {
            Disposition::Ack => self.ack().await,
            Disposition::Term => self.term().await,
            Disposition::Nak(delay) => self.nak(delay).await,
        }
    }

    pub async fn ack(self) -> anyhow::Result<()> {
        self.message
            .ack()
            .await
            .map_err(|error| anyhow::anyhow!("acknowledging message: {error}"))
    }

    pub async fn nak(self, redeliver_after: Duration) -> anyhow::Result<()> {
        self.message
            .ack_with(async_nats::jetstream::AckKind::Nak(Some(redeliver_after)))
            .await
            .map_err(|error| anyhow::anyhow!("nacking message: {error}"))
    }

    pub async fn term(self) -> anyhow::Result<()> {
        self.message
            .ack_with(async_nats::jetstream::AckKind::Term)
            .await
            .map_err(|error| anyhow::anyhow!("terminating message: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> StreamSpec {
        StreamSpec {
            name: "TEST".to_string(),
            subjects: vec!["test.one".to_string(), "test.two".to_string()],
            retention: Retention::Limits,
            max_age: Some(Duration::from_secs(600)),
            max_messages: Some(1_000),
            max_bytes: Some(1_024),
            discard: Discard::Old,
            duplicate_window: Duration::from_secs(120),
        }
    }

    /// What `ensure_stream` compares: the server's view of an existing stream
    /// against that view with the spec overlaid onto it.
    fn change(existing: &StreamSpec, desired: &StreamSpec) -> Option<String> {
        let mut overlaid = existing.to_config();
        desired.apply_to(&mut overlaid);
        narrowing_change(&existing.to_config(), &overlaid)
    }

    #[test]
    fn an_unchanged_spec_converges_silently() {
        assert_eq!(change(&spec(), &spec()), None);
    }

    #[test]
    fn widening_every_limit_converges() {
        let mut wider = spec();
        wider.max_age = Some(Duration::from_secs(1_200));
        wider.max_messages = Some(2_000);
        wider.max_bytes = Some(2_048);
        wider.subjects.push("test.three".to_string());
        assert_eq!(change(&spec(), &wider), None);
        // Removing a limit entirely is the widest change of all.
        let mut unlimited = spec();
        unlimited.max_age = None;
        unlimited.max_messages = None;
        unlimited.max_bytes = None;
        assert_eq!(change(&spec(), &unlimited), None);
    }

    #[test]
    fn narrowing_any_limit_is_refused() {
        for (label, narrow) in [
            ("max_age", {
                let mut narrow = spec();
                narrow.max_age = Some(Duration::from_secs(60));
                narrow
            }),
            ("max_bytes", {
                let mut narrow = spec();
                narrow.max_bytes = Some(512);
                narrow
            }),
            ("max_messages", {
                let mut narrow = spec();
                narrow.max_messages = Some(10);
                narrow
            }),
            ("subject", {
                let mut narrow = spec();
                narrow.subjects.pop();
                narrow
            }),
            ("discard policy", {
                let mut narrow = spec();
                narrow.discard = Discard::New;
                narrow
            }),
            ("retention", {
                let mut narrow = spec();
                narrow.retention = Retention::WorkQueue;
                narrow
            }),
        ] {
            let refused = change(&spec(), &narrow)
                .unwrap_or_else(|| panic!("narrowing {label} must be refused"));
            assert!(
                refused.contains(label),
                "refusal for {label} does not name it: {refused}"
            );
        }
    }

    #[test]
    fn imposing_a_limit_on_an_unlimited_stream_is_narrowing() {
        let mut unlimited = spec();
        unlimited.max_age = None;
        unlimited.max_messages = None;
        unlimited.max_bytes = None;
        // The server reports an absent limit as -1 (older ones as 0); both must
        // read as unlimited, or a first-boot spec would look like a shrink.
        assert!(change(&unlimited, &spec()).is_some());
        let mut zeroed = unlimited.to_config();
        zeroed.max_bytes = 0;
        zeroed.max_messages = 0;
        let mut overlaid = zeroed.clone();
        unlimited.apply_to(&mut overlaid);
        assert_eq!(narrowing_change(&zeroed, &overlaid), None);
    }
}

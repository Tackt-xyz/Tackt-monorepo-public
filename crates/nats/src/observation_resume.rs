//! Broker-maintained Hyperliquid restart positions. The KV values are complete
//! observation envelopes copied from the durable feed, not completed-block markers.
//! Readers must replay the recorded block inclusively and deduplicate by event ID.

use anyhow::{Context, Result, ensure};
use async_nats::jetstream::stream::{
    Config, DiscardPolicy, LastRawMessageErrorKind, RetentionPolicy, Source as StreamSource,
    StorageType, SubjectTransform,
};
use tackt_primitives::{Observation, Source};

use crate::{NatsClient, ObservationEvent, ObservationFeed};

/// Independent upstream feeds whose observations carry restart block heights.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HyperliquidObservationStream {
    Trades,
    Events,
}

impl HyperliquidObservationStream {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trades => "trades",
            Self::Events => "events",
        }
    }

    /// Provenance disambiguates fees emitted by either feed. Portfolio snapshots
    /// have no source block and must never replace an ingestion resume position.
    pub fn from_observation(observation: &Observation) -> Option<Self> {
        if observation.source != Source::Hyperliquid || observation.block_number.is_none() {
            return None;
        }
        match observation.provenance.method.as_str() {
            "StreamDataBytes/TRADES" => Some(Self::Trades),
            "StreamDataBytes/EVENTS" => Some(Self::Events),
            _ => None,
        }
    }
}

impl ObservationFeed {
    pub fn hyperliquid_subject(&self, stream: HyperliquidObservationStream) -> String {
        format!("{}.{}", self.subject(Source::Hyperliquid), stream.as_str())
    }

    pub fn hyperliquid_resume_bucket(&self) -> String {
        format!("{}_HYPERLIQUID_RESUME", self.stream().name)
    }

    pub fn hyperliquid_resume_stream(&self) -> String {
        format!("KV_{}", self.hyperliquid_resume_bucket())
    }

    fn resume_config(&self) -> Config {
        let bucket = self.hyperliquid_resume_bucket();
        let subject_transforms = [
            HyperliquidObservationStream::Trades,
            HyperliquidObservationStream::Events,
        ]
        .into_iter()
        .map(|stream| SubjectTransform {
            source: self.hyperliquid_subject(stream),
            destination: format!("$KV.{bucket}.{}", stream.as_str()),
        })
        .collect();
        Config {
            name: self.hyperliquid_resume_stream(),
            description: Some(
                "Latest durable Hyperliquid observations; replay blocks inclusively".into(),
            ),
            // The source alone writes this bucket; there are no publish subjects.
            sources: Some(vec![StreamSource {
                name: self.stream().name.clone(),
                subject_transforms,
                ..Default::default()
            }]),
            max_messages_per_subject: 1,
            // Only two keys, each at most one upstream message; no age expiry.
            retention: RetentionPolicy::Limits,
            discard: DiscardPolicy::Old,
            storage: StorageType::File,
            allow_direct: true,
            deny_delete: true,
            deny_purge: true,
            ..Default::default()
        }
    }

    fn validate_resume_config(&self, existing: &Config) -> Result<()> {
        ensure!(
            existing.sources == self.resume_config().sources
                && existing.subjects.is_empty()
                && existing.mirror.is_none()
                && existing.max_messages_per_subject == 1
                && existing.max_age.is_zero()
                && existing.retention == RetentionPolicy::Limits
                && existing.discard == DiscardPolicy::Old
                && existing.storage == StorageType::File
                && existing.allow_direct,
            "Hyperliquid resume bucket has incompatible configuration; refusing to replace it"
        );
        Ok(())
    }
}

impl NatsClient {
    /// Explicit deployment/test setup: provision a sourced KV bucket. The Rust
    /// KV builder assumes every source is another KV bucket, so construct its
    /// backing stream here. The normal KV API reads it; no application puts values.
    pub async fn ensure_hyperliquid_resume(&self, feed: &ObservationFeed) -> Result<()> {
        self.require_stream(feed.stream()).await?;
        let upstream = self.jetstream.get_stream(&feed.stream().name).await?;
        let mut config = feed.resume_config();
        config.num_replicas = upstream.cached_info().config.num_replicas;
        let stream = self.jetstream.get_or_create_stream(config).await?;
        feed.validate_resume_config(&stream.cached_info().config)
    }

    /// Read-only startup check. Deployment must provision both streams first.
    pub async fn require_hyperliquid_resume(&self, feed: &ObservationFeed) -> Result<()> {
        self.require_stream(feed.stream()).await?;
        let stream = self.jetstream.get_stream(feed.hyperliquid_resume_stream()).await
            .context("required Hyperliquid resume KV is unavailable; run NATS provisioning before startup")?;
        feed.validate_resume_config(&stream.cached_info().config)
    }

    /// Read a restart block from KV. An asynchronous source may lag safely: an
    /// older observation causes extra replay. A missing key means bootstrap only
    /// if this feed has no observations; otherwise wait for the source to catch up.
    /// Transport/configuration errors propagate rather than appearing as empty KV.
    pub async fn hyperliquid_resume_block(
        &self,
        feed: &ObservationFeed,
        source: HyperliquidObservationStream,
    ) -> Result<Option<u64>> {
        let bucket = self
            .jetstream
            .get_key_value(feed.hyperliquid_resume_bucket())
            .await?;
        if let Some(bytes) = bucket.get(source.as_str()).await? {
            let event: ObservationEvent = serde_json::from_slice(&bytes)?;
            ensure!(
                HyperliquidObservationStream::from_observation(&event.observation) == Some(source),
                "Hyperliquid resume key contains an incompatible observation"
            );
            return Ok(event.observation.block_number);
        }
        let stream = self.jetstream.get_stream(&feed.stream().name).await?;
        match stream
            .get_last_raw_message_by_subject(&feed.hyperliquid_subject(source))
            .await
        {
            Ok(_) => anyhow::bail!(
                "Hyperliquid resume KV has not caught up; retry before starting the feed"
            ),
            Err(error) if error.kind() == LastRawMessageErrorKind::NoMessageFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}

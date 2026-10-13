//! `JetStream` ingress: one supervised consumer task per risk stream, feeding
//! the bounded channel the engine thread drains.

use std::time::Duration;

use risk_engine::ingest::IngestEvent;
use serde::de::DeserializeOwned;
use tackt_nats::{AckHandle, ConsumerSpec, NatsClient, StartPosition, StreamSpec};
use tokio::sync::mpsc::Sender;

use super::wire::{Envelope, StreamKind};

/// Shared by all three ingress consumers; the streams are distinct.
const DURABLE_NAME: &str = "risk-engine";

/// Generous: an in-flight message may sit behind a snapshot write or an
/// egress publish stall; neither should churn redeliveries.
const ACK_WAIT: Duration = Duration::from_mins(5);

const RESTART_DELAY: Duration = Duration::from_secs(5);

/// One decoded ingress message plus what the engine thread needs to ack it
/// (and to re-decode it on the poisoned-processor path).
pub struct IngestMessage {
    pub kind: StreamKind,
    /// Fold cursor key: the full stream name.
    pub stream_name: String,
    pub sequence: u64,
    pub event: IngestEvent,
    pub ack: AckHandle,
}

/// Consume `stream` forever, mapping payloads into ingest events. Exits only
/// when the engine side of the channel is gone (shutdown).
///
/// `start` positions the first successful bind — `BySequence(cursor + 1)`
/// from the recovered snapshot, overriding server durable state, which can be
/// ahead of the persisted cursor (replaying is safe; skipping is not). It is
/// consumed only on a successful bind: a setup failure must retry the same
/// position, never fall back to the server ack floor. After an in-process
/// consumer failure the durable re-binds at the server ack floor, which this
/// lifetime's acks keep aligned with the fold.
pub async fn supervise<T: DeserializeOwned + Send>(
    client: NatsClient,
    stream: StreamSpec,
    kind: StreamKind,
    start: StartPosition,
    channel: Sender<IngestMessage>,
    map: fn(T) -> IngestEvent,
) {
    let mut position = Some(start);
    loop {
        let mut spec = ConsumerSpec::durable(DURABLE_NAME);
        spec.ack_wait = ACK_WAIT;
        spec.start = *position.as_ref().unwrap_or(&StartPosition::DeliverAll);
        match client.consumer::<Envelope<T>>(&stream, spec).await {
            Ok(mut consumer) => {
                position = None;
                loop {
                    match consumer.next().await {
                        Ok(delivery) => {
                            let sequence = delivery.stream_sequence();
                            let (envelope, ack) = delivery.split();
                            let message = IngestMessage {
                                kind,
                                stream_name: stream.name.clone(),
                                sequence,
                                event: map(envelope.payload),
                                ack,
                            };
                            if channel.send(message).await.is_err() {
                                return;
                            }
                        }
                        Err(error) => {
                            tracing::warn!(%error, stream = %stream.name, "ingress consumer failed; rebinding");
                            break;
                        }
                    }
                }
            }
            Err(error) => {
                tracing::warn!(%error, stream = %stream.name, "ingress consumer setup failed");
            }
        }
        tokio::time::sleep(RESTART_DELAY).await;
    }
}

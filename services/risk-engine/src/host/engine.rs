//! The dedicated engine thread: sole owner of the `ApplyLoop`.
//!
//! The processor is single-writer (`&mut self`, internal rayon pool), so one
//! OS thread drives it; the tokio runtime never touches it. Per accepted
//! event: publish every emitted risk event (broker-acked) → record it in the
//! egress ledger → advance the fold cursor → ack the input. A crash between
//! apply and ack redelivers the input, absorbed as an engine duplicate.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use risk_engine::Error;
use risk_engine::persistence::{ApplyLoop, SnapshotStore};
use risk_engine::processing::{EvaluationInput, RiskEvent};
use tackt_primitives::risk::RiskEvaluation;
use tokio::runtime::Handle;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;

use super::HostState;
use super::egress::EgressPublisher;
use super::ingress::IngestMessage;
use super::wire;

/// A deterministically poisoning input proves itself on the second heal;
/// beyond that the wedge must become loud instead of a rebuild livelock.
/// Deliberate fail-stop: redelivery is unbounded and each restart repeats the
/// heals then exits, so a poison pill crash-loops visibly (paced by the
/// orchestrator) rather than being termed — dropping an accepted-validity
/// input would silently diverge risk state from the input streams.
const MAX_HEALS_PER_INPUT: u32 = 2;

enum Processed {
    Accepted,
    Rejected,
    /// Shutdown interrupted an egress publish; the input stays unacked.
    Aborted,
}

/// One read-only evaluation request and its private response channel.
pub struct EvaluationMessage {
    pub request: EvaluationInput,
    pub response: oneshot::Sender<risk_engine::Result<RiskEvaluation>>,
}

enum Received {
    Ingest,
    Evaluation,
    Deadline,
}

/// Everything the engine thread owns for its lifetime.
pub struct EngineTask {
    pub apply_loop: ApplyLoop,
    /// Recovery corrections, published before any live input applies.
    pub corrections: Vec<RiskEvent>,
    pub store: SnapshotStore,
    /// Snapshot cadence: interval or accepted-event count, whichever first.
    /// Rejections don't advance the count.
    pub snapshot_interval: Duration,
    pub snapshot_event_count: u64,
    pub publisher: EgressPublisher,
    pub state: Arc<HostState>,
    /// Runtime handle for blocking on async acks/publishes.
    pub handle: Handle,
    pub ingest: Receiver<IngestMessage>,
    pub evaluations: Receiver<EvaluationMessage>,
    /// Fired once corrections are on the egress stream.
    pub corrections_done: Option<oneshot::Sender<()>>,
    /// Best-effort display capture, separate from durable engine recovery snapshots.
    pub history: Option<risk_engine::egress::history::HistorySampler>,
    pub history_interval: Duration,
}

impl EngineTask {
    /// Runs until the ingest channel closes (shutdown), then writes a final
    /// snapshot. Never snapshots mid-event: this thread is the only caller.
    pub fn run(mut self) {
        // Corrections resynchronize downstream and must precede live traffic;
        // consumers are not created until this fires.
        let corrections = std::mem::take(&mut self.corrections);
        if !self.publish_all(&corrections) {
            return;
        }
        if let Some(done) = self.corrections_done.take() {
            let _ = done.send(());
        }

        let mut accepted: u64 = 0;
        let mut deadline = Instant::now() + self.snapshot_interval;
        let mut history_deadline = Instant::now() + self.history_interval;
        let mut evaluations_open = true;
        loop {
            let next_deadline = if self.history.is_some() {
                deadline.min(history_deadline)
            } else {
                deadline
            };
            let wait = next_deadline.saturating_duration_since(Instant::now());
            let mut ingest_message = None;
            let mut evaluation_message = None;
            let received = self.handle.clone().block_on(async {
                tokio::select! {
                    biased;
                    message = self.ingest.recv() => {
                        ingest_message = message;
                        Received::Ingest
                    },
                    message = self.evaluations.recv(), if evaluations_open => {
                        evaluation_message = message;
                        Received::Evaluation
                    },
                    () = tokio::time::sleep(wait) => Received::Deadline,
                }
            });
            match received {
                Received::Ingest => match ingest_message {
                    Some(message) => match self.process(message) {
                        Processed::Accepted => accepted += 1,
                        Processed::Rejected => {}
                        Processed::Aborted => break,
                    },
                    // Closing ingress is the host's shutdown signal. HTTP
                    // connection tasks can retain evaluation senders, so
                    // waiting for that channel would wedge final snapshotting.
                    None => break,
                },
                Received::Evaluation => match evaluation_message {
                    Some(message) => {
                        if !message.response.is_closed() {
                            let result = self.apply_loop.processor().evaluate(message.request);
                            let _ = message.response.send(result);
                        }
                    }
                    None => evaluations_open = false,
                },
                Received::Deadline => {}
            }
            // Capture between completed events, even when ingress stays continuously busy.
            if let Some(history) = &self.history
                && Instant::now() >= history_deadline
            {
                if self.state.ready.load(Ordering::SeqCst)
                    && !self.state.draining.load(Ordering::SeqCst)
                {
                    history.capture(self.apply_loop.processor(), chrono::Utc::now());
                }
                history_deadline = Instant::now() + self.history_interval;
            }
            if accepted >= self.snapshot_event_count || Instant::now() >= deadline {
                // Idle intervals don't rewrite an unchanged snapshot.
                if accepted > 0 {
                    self.write_snapshot();
                }
                accepted = 0;
                deadline = Instant::now() + self.snapshot_interval;
            }
        }
        self.write_snapshot();
    }

    /// Publish each event with broker ack, recording it in the egress ledger.
    /// `false` = shutdown interrupted; unrecorded events stay unpublished.
    fn publish_all(&mut self, events: &[RiskEvent]) -> bool {
        for event in events {
            let recommendation = match self.apply_loop.recommendation(event) {
                Ok(recommendation) => recommendation,
                Err(error) => {
                    tracing::error!(%error, account = event.account, "engine output violates the public recommendation contract; snapshotting and exiting");
                    self.write_snapshot();
                    std::process::exit(1);
                }
            };
            if !self.publisher.publish_blocking(&recommendation) {
                return false;
            }
            self.apply_loop.record_published(event);
        }
        true
    }

    fn process(&mut self, message: IngestMessage) -> Processed {
        let IngestMessage {
            kind,
            stream_name,
            sequence,
            event,
            ack,
        } = message;
        let mut event = Some(event);
        let mut heals = 0;
        loop {
            match self
                .apply_loop
                .apply(event.take().expect("event is set on every iteration"))
            {
                Ok(events) => {
                    if !self.publish_all(&events) {
                        return Processed::Aborted;
                    }
                    self.apply_loop.acknowledge(&stream_name, sequence);
                    if let Err(error) = self.handle.block_on(ack.ack()) {
                        // Redelivery replays as an engine duplicate; safe.
                        tracing::warn!(%error, stream = %stream_name, sequence, "input ack failed");
                    }
                    return Processed::Accepted;
                }
                // Poisoning reports the processor's health, not the event's
                // validity: heal, publish the rebuild's transitions, re-apply
                // this same event. Never nak — later same-stream messages
                // would apply first and the redelivery would be rejected
                // stale, silently losing the update.
                Err(Error::ProcessorPoisoned) => {
                    self.state.poisoned.store(true, Ordering::SeqCst);
                    heals += 1;
                    tracing::error!(stream = %stream_name, sequence, heals, "processor poisoned; rebuilding");
                    let transitions = match self.apply_loop.rebuild() {
                        Ok(transitions) if heals <= MAX_HEALS_PER_INPUT => transitions,
                        Ok(_) => {
                            tracing::error!(stream = %stream_name, sequence, "input poisons deterministically; snapshotting and exiting");
                            self.write_snapshot();
                            std::process::exit(1);
                        }
                        Err(error) => {
                            // Input stays unacked: it redelivers onto
                            // recovered state after restart.
                            tracing::error!(%error, "rebuild failed; snapshotting and exiting");
                            self.write_snapshot();
                            std::process::exit(1);
                        }
                    };
                    if !self.publish_all(&transitions) {
                        return Processed::Aborted;
                    }
                    self.state.poisoned.store(false, Ordering::SeqCst);
                    // Re-materialize: apply() consumed the event and
                    // IngestEvent is not Clone.
                    match wire::decode(kind, ack.payload()) {
                        Ok(decoded) => event = Some(decoded),
                        Err(error) => {
                            tracing::error!(%error, stream = %stream_name, sequence, "re-decode changed verdict; snapshotting and exiting");
                            self.write_snapshot();
                            std::process::exit(1);
                        }
                    }
                }
                Err(error)
                    if matches!(
                        error,
                        Error::MissingAccountSubject { .. }
                            | Error::DuplicateAccountSubject { .. }
                            | Error::AccountSubjectRebind { .. }
                    ) =>
                {
                    // Permanent identity invariants must never enter the
                    // ordinary "acknowledge and drop" rejection path.
                    tracing::error!(%error, stream = %stream_name, sequence, "account identity invariant violated; snapshotting and exiting");
                    self.write_snapshot();
                    std::process::exit(1);
                }
                // Deterministic rejection: redelivery cannot change the
                // verdict; acknowledge and drop per the persistence contract.
                Err(error) => {
                    tracing::warn!(%error, stream = %stream_name, sequence, "input rejected; acknowledged and dropped");
                    self.apply_loop.acknowledge(&stream_name, sequence);
                    if let Err(ack_error) = self.handle.block_on(ack.ack()) {
                        tracing::warn!(%ack_error, stream = %stream_name, sequence, "rejection ack failed");
                    }
                    return Processed::Rejected;
                }
            }
        }
    }

    fn write_snapshot(&self) {
        match self.store.write_view(&self.apply_loop.snapshot_view()) {
            Ok(path) => tracing::debug!(path = %path.display(), "snapshot written"),
            // The fold is intact; the next cadence retries.
            Err(error) => tracing::error!(%error, "snapshot write failed"),
        }
    }
}

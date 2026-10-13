//! The shared reconnectable WebSocket shell for venue price streams.
//!
//! Every venue connection has the same failure physics — dead TCP that only an idle limit
//! notices, transport pings that need answering, routine server-side disconnects, and the
//! backoff discipline that separates a flapping endpoint from a healthy reconnect — so they
//! are implemented exactly once here and cannot diverge between venues. A venue contributes
//! only what is genuinely its own through [`VenueSession`]: its subscription handshake and its
//! frame format.

use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::backoff::RestartBackoff;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

pub(super) type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Every supported venue emits something — trades, heartbeats, or transport pings — well
/// inside this window, so a silent socket is dead TCP and the fix is a reconnect.
const READ_IDLE_LIMIT: Duration = Duration::from_secs(90);
const RECONNECT_BACKOFF_START: Duration = Duration::from_secs(1);
const RECONNECT_BACKOFF_CAP: Duration = Duration::from_secs(30);
/// The shell's own frame bound, and so every venue's default.
pub(super) const DEFAULT_MAX_FRAME_BYTES: usize = 64 * 1024;

/// An application-level keepalive some venues require: send `payload` as a text frame every
/// `interval`, regardless of inbound traffic — a venue that only wants pings through quiet
/// stretches tolerates scheduled ones too, so one schedule serves both styles. The venue's
/// reply arrives as an ordinary text frame for the session to recognize and drop.
#[derive(Clone, Copy)]
pub(super) struct ClientPing {
    pub interval: Duration,
    pub payload: &'static str,
}

/// What one venue contributes to a supervised connection.
pub(super) trait VenueSession: Send {
    /// Venue name for logs.
    const VENUE: &'static str;

    /// Largest text frame worth handing to the session — anything bigger is not this
    /// subscription and is refused here, before the venue's JSON parsing allocates. Venues
    /// size it to their own protocol; the shell enforcing it means no venue can forget the
    /// bound.
    const MAX_FRAME_BYTES: usize = DEFAULT_MAX_FRAME_BYTES;

    /// Set by venues whose servers drop a client that never speaks (Bybit); `None` where the
    /// server drives the keepalive with transport pings or heartbeats.
    const CLIENT_PING: Option<ClientPing> = None;

    /// Called once per connection, after the handshake and before any frame — the place to
    /// send subscribe requests. Venues whose subscription lives in the URL do nothing.
    fn subscribe(&mut self, socket: &mut Socket) -> impl Future<Output = Result<()>> + Send;

    /// Handles one text frame. An error ends the connection cycle, and the supervisor
    /// reconnects with a clean subscription.
    fn frame(&mut self, text: &str) -> impl Future<Output = Result<()>> + Send;
}

/// Runs connection cycles forever under the service-wide restart backoff, so a venue's
/// routine daily disconnect resumes at full speed while a flapping endpoint is throttled.
/// Never returns.
pub(super) async fn supervise<S: VenueSession>(url: &str, session: &mut S) {
    let mut backoff = RestartBackoff::new(RECONNECT_BACKOFF_START, RECONNECT_BACKOFF_CAP);
    loop {
        let started = Instant::now();
        if let Err(error) = cycle(url, session).await {
            tracing::warn!(%error, venue = S::VENUE, "venue websocket cycle failed");
        }
        tokio::time::sleep(backoff.next_delay(started.elapsed())).await;
    }
}

/// One connection cycle: connect, subscribe, then hand every text frame to the session until
/// the socket fails. Only ever returns an error — every exit is a reason to reconnect.
pub(super) async fn cycle<S: VenueSession>(url: &str, session: &mut S) -> Result<()> {
    let (mut socket, _) = tokio::time::timeout(HANDSHAKE_TIMEOUT, connect_async(url))
        .await
        .with_context(|| format!("{} websocket connect timeout", S::VENUE))??;
    session.subscribe(&mut socket).await?;
    tracing::info!(venue = S::VENUE, "venue websocket connected");
    // The ping schedule outlives individual frames: a busy stream must not postpone a venue's
    // required heartbeat.
    let mut next_ping =
        Instant::now() + S::CLIENT_PING.map_or(READ_IDLE_LIMIT, |ping| ping.interval);
    loop {
        let text = next_text(&mut socket, S::VENUE, S::CLIENT_PING, &mut next_ping).await?;
        if text.len() > S::MAX_FRAME_BYTES {
            bail!(
                "{} frame of {} bytes exceeds {}",
                S::VENUE,
                text.len(),
                S::MAX_FRAME_BYTES
            )
        }
        session.frame(&text).await?;
    }
}

/// Sends one text frame — the socket write venue subscribe handshakes go through, so the
/// transport types stay in the shell. The caller names the operation in the failure.
pub(super) async fn send_text(socket: &mut Socket, text: &str) -> Result<()> {
    socket.send(Message::Text(text.into())).await?;
    Ok(())
}

/// The next text frame, answering transport pings inline, sending the venue's client
/// keepalive on its schedule, and treating closure or [`READ_IDLE_LIMIT`] of silence as
/// failure. A keepalive reply is inbound traffic, so a socket that still answers pings never
/// trips the idle limit.
async fn next_text(
    socket: &mut Socket,
    venue: &'static str,
    ping: Option<ClientPing>,
    next_ping: &mut Instant,
) -> Result<Utf8Bytes> {
    let idle_deadline = Instant::now() + READ_IDLE_LIMIT;
    loop {
        let wait_until = match ping {
            Some(_) => (*next_ping).min(idle_deadline),
            None => idle_deadline,
        };
        match tokio::time::timeout_at(wait_until, socket.next()).await {
            Ok(message) => match message {
                Some(Ok(Message::Text(text))) => return Ok(text),
                Some(Ok(Message::Ping(payload))) => socket
                    .send(Message::Pong(payload))
                    .await
                    .with_context(|| format!("{venue} websocket pong"))?,
                Some(Ok(Message::Close(_))) | None => bail!("{venue} websocket closed"),
                Some(Err(error)) => {
                    return Err(error).with_context(|| format!("{venue} websocket read"));
                }
                Some(Ok(_)) => {}
            },
            Err(_) if Instant::now() >= idle_deadline => {
                bail!("{venue} websocket idle")
            }
            Err(_) => {
                let ping = ping.expect("a wait shorter than the idle limit implies a client ping");
                socket
                    .send(Message::Text(ping.payload.into()))
                    .await
                    .with_context(|| format!("{venue} websocket keepalive"))?;
                *next_ping = Instant::now() + ping.interval;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use tokio_tungstenite::accept_async;

    use super::*;

    struct TestSession {
        frames: mpsc::UnboundedSender<String>,
    }

    impl VenueSession for TestSession {
        const VENUE: &'static str = "Test";

        async fn subscribe(&mut self, _socket: &mut Socket) -> Result<()> {
            Ok(())
        }

        async fn frame(&mut self, text: &str) -> Result<()> {
            self.frames.send(text.to_owned()).context("record frame")
        }
    }

    #[tokio::test]
    async fn a_cycle_delivers_frames_in_order_and_answers_pings() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(tcp).await.unwrap();
            socket.send(Message::Text("one".into())).await.unwrap();
            socket.send(Message::Ping(vec![7].into())).await.unwrap();
            loop {
                if matches!(socket.next().await.unwrap().unwrap(), Message::Pong(_)) {
                    break;
                }
            }
            socket.send(Message::Text("two".into())).await.unwrap();
            socket.send(Message::Close(None)).await.unwrap();
        });
        let (frames, mut received) = mpsc::unbounded_channel();
        let mut session = TestSession { frames };
        let outcome = cycle(&format!("ws://{address}"), &mut session).await;
        assert!(outcome.is_err(), "a closed socket must end the cycle");
        assert_eq!(received.recv().await.unwrap(), "one");
        assert_eq!(received.recv().await.unwrap(), "two");
        server.await.unwrap();
    }

    struct PingingSession {
        frames: mpsc::UnboundedSender<String>,
    }

    impl VenueSession for PingingSession {
        const VENUE: &'static str = "Pinging";
        const CLIENT_PING: Option<ClientPing> = Some(ClientPing {
            interval: Duration::from_millis(50),
            payload: "ping",
        });

        async fn subscribe(&mut self, _socket: &mut Socket) -> Result<()> {
            Ok(())
        }

        async fn frame(&mut self, text: &str) -> Result<()> {
            self.frames.send(text.to_owned()).context("record frame")
        }
    }

    #[tokio::test]
    async fn the_client_keepalive_fires_on_schedule_even_while_frames_flow() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(tcp).await.unwrap();
            // Send data first: inbound traffic must not postpone the ping schedule.
            socket.send(Message::Text("data".into())).await.unwrap();
            loop {
                if let Message::Text(text) = socket.next().await.unwrap().unwrap() {
                    assert_eq!(text.as_str(), "ping");
                    break;
                }
            }
            socket.send(Message::Text("pong".into())).await.unwrap();
            socket.send(Message::Close(None)).await.unwrap();
        });
        let (frames, mut received) = mpsc::unbounded_channel();
        let mut session = PingingSession { frames };
        let outcome = cycle(&format!("ws://{address}"), &mut session).await;
        assert!(outcome.is_err(), "a closed socket must end the cycle");
        assert_eq!(received.recv().await.unwrap(), "data");
        assert_eq!(received.recv().await.unwrap(), "pong");
        server.await.unwrap();
    }

    struct TinyFrameSession {
        frames: mpsc::UnboundedSender<String>,
    }

    impl VenueSession for TinyFrameSession {
        const VENUE: &'static str = "Tiny";
        const MAX_FRAME_BYTES: usize = 8;

        async fn subscribe(&mut self, _socket: &mut Socket) -> Result<()> {
            Ok(())
        }

        async fn frame(&mut self, text: &str) -> Result<()> {
            self.frames.send(text.to_owned()).context("record frame")
        }
    }

    #[tokio::test]
    async fn an_oversized_frame_ends_the_cycle_before_the_session_sees_it() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(tcp).await.unwrap();
            socket
                .send(Message::Text("123456789".into()))
                .await
                .unwrap();
            let _ = socket.next().await;
        });
        let (frames, mut received) = mpsc::unbounded_channel();
        let mut session = TinyFrameSession { frames };
        let outcome = cycle(&format!("ws://{address}"), &mut session).await;
        assert!(outcome.is_err(), "an oversized frame must end the cycle");
        assert!(
            outcome.unwrap_err().to_string().contains("exceeds"),
            "the failure must name the bound"
        );
        assert!(received.try_recv().is_err(), "the session must not see it");
        server.abort();
    }

    #[tokio::test]
    async fn supervision_reconnects_after_a_disconnect() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for frame in ["first", "second"] {
                let (tcp, _) = listener.accept().await.unwrap();
                let mut socket = accept_async(tcp).await.unwrap();
                socket.send(Message::Text(frame.into())).await.unwrap();
                socket.send(Message::Close(None)).await.unwrap();
            }
        });
        let (frames, mut received) = mpsc::unbounded_channel();
        let url = format!("ws://{address}");
        let worker = tokio::spawn(async move {
            let mut session = TestSession { frames };
            supervise(&url, &mut session).await;
        });
        let collected = tokio::time::timeout(Duration::from_secs(10), async {
            (
                received.recv().await.unwrap(),
                received.recv().await.unwrap(),
            )
        })
        .await
        .expect("supervision must reconnect and deliver from the second connection");
        assert_eq!(collected, ("first".to_owned(), "second".to_owned()));
        worker.abort();
        server.await.unwrap();
    }
}

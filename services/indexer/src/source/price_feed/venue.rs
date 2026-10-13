//! What a price venue is, and everything every venue shares.
//!
//! A venue is the contract one adapter fills in over the reconnecting shell in `ws`.
//!
//! Market canonicalization is identity-bearing and lives here for the same reason: the subject
//! comes from the canonical uppercase spelling, never from the raw configuration string, so one
//! market cannot become two sources under two spellings.

use std::marker::PhantomData;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use tackt_nats::prices::{
    MAX_MARKET_SYMBOL_BYTES, MarketSymbol, PriceAuthority, PriceEvidence, PriceSubject,
};

use super::ws::{self, ClientPing, Socket, VenueSession};

/// One price venue's complete contract.
pub(super) trait Venue: Sized + Send + Sync + 'static {
    /// The authority every trade from this venue is attributed to.
    const AUTHORITY: PriceAuthority;

    /// Venue name for logs, errors, and evidence context.
    const NAME: &'static str;

    /// The venue's public WebSocket endpoint: what [`Venue::subscription`] builds on and what
    /// provenance records.
    const ENDPOINT: &'static str;

    /// The venue's pair separator (`/` for Kraken, `-` for Coinbase); `None` where a market is
    /// a single alphanumeric run.
    const SEPARATOR: Option<char>;

    /// Largest text frame worth decoding. The default is the shell's own bound; a venue whose
    /// subscribe-time snapshot is genuinely larger raises it.
    const MAX_FRAME_BYTES: usize = ws::DEFAULT_MAX_FRAME_BYTES;

    /// Set by venues whose servers drop a client that never speaks; `None` where the server
    /// drives the keepalive.
    const CLIENT_PING: Option<ClientPing> = None;

    /// One trade exactly as the venue states it.
    type Trade: Send + Sync;

    /// How this venue subscribes every configured market over one connection.
    #[must_use]
    fn subscription(markets: &[Market<Self>]) -> Subscription;

    /// Decodes one text frame into its trades. Control frames — heartbeats, acknowledgements,
    /// snapshots' envelopes — decode to no trades.
    ///
    /// # Errors
    ///
    /// Fails on an undecodable frame, a rejected subscription, or a channel this build did not
    /// subscribe: the subscription is not what was asked for, and reconnecting re-establishes
    /// a clean one.
    fn decode(frame: &str) -> Result<Vec<Self::Trade>>;

    /// The market spelling a decoded trade names, as it appears in the venue's payloads: the key
    /// `run_venue`'s symbol-keyed lookup pairs a trade with its configured market by.
    fn market_of(trade: &Self::Trade) -> &str;

    /// Normalizes one trade into price evidence. `observed_at` is the indexer's acceptance
    /// instant, passed in so the function stays pure.
    ///
    /// The caller owes `Self::market_of(trade) == market.symbol()`, which `run_venue`'s
    /// symbol-keyed lookup guarantees.
    ///
    /// # Errors
    ///
    /// Fails closed when the price is not an exact positive bounded decimal, when the venue's
    /// timestamp is unrepresentable, or when any evidence-contract check refuses the message.
    fn evidence(
        market: &Market<Self>,
        trade: &Self::Trade,
        observed_at: DateTime<Utc>,
    ) -> Result<PriceEvidence>;
}

/// The two ways a venue expresses one whole subscription: every stream named in the connect URL,
/// leaving no handshake to race, or one request sent over the socket once connected.
pub(super) enum Subscription {
    InUrl(String),
    Frame { url: String, request: String },
}

/// One configured market on one venue: the spelling to subscribe and the subject its trades
/// price.
pub(super) struct Market<V> {
    symbol: String,
    subject: PriceSubject,
    /// `fn() -> V` rather than `V`: the market neither inherits V's auto traits nor loses
    /// covariance in it.
    venue: PhantomData<fn() -> V>,
}

impl<V: Venue> Market<V> {
    /// Validates one configured market spelling.
    ///
    /// # Errors
    ///
    /// Fails closed unless the spelling is uppercase-canonicalizable ASCII alphanumerics
    /// around the venue's separator, within the market-symbol bounds.
    pub(super) fn new(spelling: impl Into<String>) -> Result<Self, MarketError> {
        let spelling: String = spelling.into();
        let Some((symbol, subject)) = canonical(&spelling, V::SEPARATOR) else {
            return Err(MarketError {
                venue: V::NAME,
                spelling,
                separator: V::SEPARATOR,
            });
        };
        Ok(Self {
            symbol,
            subject,
            venue: PhantomData,
        })
    }

    /// The market as it appears in the venue's payloads (`BTCUSDT`, `ETH/USD`, `ETH-USD`).
    #[must_use]
    pub(super) fn symbol(&self) -> &str {
        &self.symbol
    }

    /// What this market's trades price.
    #[must_use]
    pub(super) const fn subject(&self) -> &PriceSubject {
        &self.subject
    }
}

// Derived `Clone` would demand `V: Clone` for a type parameter no value depends on.
impl<V> Clone for Market<V> {
    fn clone(&self) -> Self {
        Self {
            symbol: self.symbol.clone(),
            subject: self.subject.clone(),
            venue: PhantomData,
        }
    }
}

/// A configured market that must not be subscribed.
#[derive(Debug, thiserror::Error)]
pub(super) struct MarketError {
    venue: &'static str,
    spelling: String,
    separator: Option<char>,
}

// Written by hand so a separatorless venue states its own rule instead of rendering `None`.
impl std::fmt::Display for MarketError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (venue, spelling) = (self.venue, &self.spelling);
        match self.separator {
            None => write!(
                formatter,
                "{venue} market {spelling:?} is not 1 to {MAX_MARKET_SYMBOL_BYTES} ASCII \
                 alphanumeric characters"
            ),
            Some(separator) => write!(
                formatter,
                "{venue} market {spelling:?} is not ASCII alphanumerics around exactly one \
                 {separator:?}, within the market-symbol bounds"
            ),
        }
    }
}

/// Canonicalizes one market spelling and derives the subject its trades price.
///
/// `separator` is required to occur exactly once between non-empty alphanumeric sides. The
/// canonical spelling is the uppercased input; the subject always spells the separator `-`,
/// and [`MarketSymbol`]'s bounds apply. Returns `None` for any spelling that must not be
/// subscribed.
fn canonical(raw: &str, separator: Option<char>) -> Option<(String, PriceSubject)> {
    let sides_valid = |base: &str, quote: &str| {
        !base.is_empty()
            && !quote.is_empty()
            && base
                .bytes()
                .chain(quote.bytes())
                .all(|byte| byte.is_ascii_alphanumeric())
    };
    match separator {
        None if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_alphanumeric()) => {
            return None;
        }
        Some(sep) => {
            let (base, quote) = raw.split_once(sep)?;
            if !sides_valid(base, quote) {
                return None;
            }
        }
        None => {}
    }
    let canonical = raw.to_ascii_uppercase();
    let subject_spelling = match separator {
        Some(sep) if sep != '-' => canonical.replace(sep, "-"),
        _ => canonical.clone(),
    };
    let subject = PriceSubject::Market(MarketSymbol::new(subject_spelling).ok()?);
    Some((canonical, subject))
}

/// Sends the venue's subscribe request, if it has one, then decodes each frame and hands every
/// trade to `deliver`.
struct Session<V, F> {
    url: String,
    subscribe_request: Option<String>,
    deliver: F,
    venue: PhantomData<fn() -> V>,
}

impl<V: Venue, F> Session<V, F> {
    /// Resolves the venue's subscription once into the URL to connect and the request, if any,
    /// to send after the handshake.
    fn new(markets: &[Market<V>], deliver: F) -> Self {
        let (url, subscribe_request) = match V::subscription(markets) {
            Subscription::InUrl(url) => (url, None),
            Subscription::Frame { url, request } => (url, Some(request)),
        };
        Self {
            url,
            subscribe_request,
            deliver,
            venue: PhantomData,
        }
    }
}

impl<V, F, Fut> VenueSession for Session<V, F>
where
    V: Venue,
    F: FnMut(V::Trade) -> Fut + Send,
    Fut: Future<Output = Result<()>> + Send,
{
    const VENUE: &'static str = V::NAME;
    const MAX_FRAME_BYTES: usize = V::MAX_FRAME_BYTES;
    const CLIENT_PING: Option<ClientPing> = V::CLIENT_PING;

    async fn subscribe(&mut self, socket: &mut Socket) -> Result<()> {
        let Some(request) = &self.subscribe_request else {
            return Ok(());
        };
        ws::send_text(socket, request)
            .await
            .with_context(|| format!("{} subscribe", V::NAME))
    }

    async fn frame(&mut self, text: &str) -> Result<()> {
        for trade in V::decode(text)? {
            (self.deliver)(trade).await?;
        }
        Ok(())
    }
}

/// Runs one venue's subscription forever, reconnecting on any failure, and delivers each
/// decoded trade to `deliver`.
///
/// There is no gap recovery beyond a venue's own subscribe-time snapshot: this is a
/// current-price feed and the aggregation engine's staleness decay covers a reconnect. A
/// delivery failure fails the whole connection cycle so the shell's backoff throttles
/// ingestion while the durable handoff is down.
///
/// # Errors
///
/// Returns only for an empty market list — a configuration error, not a connection state.
pub(super) async fn supervise<V, F, Fut>(markets: &[Market<V>], deliver: F) -> Result<()>
where
    V: Venue,
    F: FnMut(V::Trade) -> Fut + Send,
    Fut: Future<Output = Result<()>> + Send,
{
    if markets.is_empty() {
        // Unreachable through the service: `run_venue` parks an unconfigured venue instead.
        bail!(
            "{} supervision requires at least one configured market",
            V::NAME
        )
    }
    let mut session = Session::new(markets, deliver);
    let url = session.url.clone();
    ws::supervise(&url, &mut session).await;
    unreachable!("websocket supervision runs forever")
}

/// The venue-agnostic half of every adapter's test suite: each venue instantiates these with
/// its own fixtures, so what a venue's tests actually state is what that venue does
/// differently.
#[cfg(test)]
pub(super) mod harness {
    use chrono::TimeZone;
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::accept_async;
    use tokio_tungstenite::tungstenite::Message;

    use super::*;

    /// The acceptance instant every adapter's fixtures normalize against: later than each
    /// fixture trade, and inside the evidence contract's skew bound.
    #[must_use]
    pub fn observed_at() -> DateTime<Utc> {
        Utc.timestamp_opt(1_800_000_100, 0).single().expect("time")
    }

    /// Replays `frames` from a local server, asserting the session sends exactly the venue's
    /// subscribe request — byte for byte, so the wire itself is under test — and returns every
    /// trade it delivered, in order.
    pub async fn delivers_replayed_frames<V: Venue>(
        markets: &[Market<V>],
        frames: &[String],
    ) -> Vec<V::Trade> {
        let mut delivered = Vec::new();
        let mut session = Session::<V, _>::new(markets, |trade| {
            delivered.push(trade);
            async { Ok(()) }
        });
        let expected = session.subscribe_request.clone();
        let replay = frames.to_vec();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(tcp).await.unwrap();
            if let Some(expected) = expected {
                let Some(Ok(Message::Text(request))) = socket.next().await else {
                    panic!("expected a subscribe request")
                };
                assert_eq!(request.as_str(), expected);
            }
            for frame in replay {
                socket.send(Message::Text(frame.into())).await.unwrap();
            }
            socket.send(Message::Close(None)).await.unwrap();
        });
        let outcome = ws::cycle(&format!("ws://{address}"), &mut session).await;
        assert!(outcome.is_err(), "a closed socket must end the cycle");
        server.await.unwrap();
        delivered
    }

    /// Both spellings of one market must canonicalize to one identity, and every malformed
    /// spelling must be refused.
    pub fn canonicalizes_markets<V: Venue>(
        spellings: &[&str],
        canonical: &str,
        subject: &str,
        malformed: &[&str],
    ) {
        // A venue's name and the authority its evidence carries are one identity, spelled twice.
        assert_eq!(V::NAME.to_ascii_lowercase(), V::AUTHORITY.as_str());
        for spelling in spellings {
            let market = Market::<V>::new(*spelling).expect("valid market");
            assert_eq!(market.symbol(), canonical);
            assert_eq!(
                market.subject(),
                &PriceSubject::Market(MarketSymbol::new(subject).expect("symbol"))
            );
        }
        for spelling in malformed {
            assert!(Market::<V>::new(*spelling).is_err(), "{spelling:?}");
        }
    }

    /// Live smoke test body: connects to the real venue and normalizes its first trade.
    pub async fn live_trade_normalizes<V: Venue>(markets: &[Market<V>]) {
        let mut first = None;
        let mut session = Session::<V, _>::new(markets, |trade| {
            if first.is_none() {
                first = Some(trade);
            }
            async { anyhow::bail!("stop after the first trade") }
        });
        let url = session.url.clone();
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            ws::cycle(&url, &mut session),
        )
        .await;
        let Some(trade) = first else {
            panic!(
                "no live {} trade within 60 seconds; cycle outcome: {outcome:?}",
                V::NAME
            )
        };
        assert_eq!(V::market_of(&trade), markets[0].symbol());
        let evidence = V::evidence(&markets[0], &trade, Utc::now())
            .expect("a live trade must normalize into price evidence");
        assert!(evidence.price().value() > 0);
    }
}

#[cfg(test)]
mod tests {
    use super::super::binance::Binance;
    use super::super::bybit::Bybit;
    use super::super::coinbase::Coinbase;
    use super::super::kraken::Kraken;
    use super::*;

    /// An empty market list is a configuration error, never a connection state.
    #[tokio::test]
    async fn supervision_refuses_an_empty_market_list() {
        async fn refuses<V: Venue>() {
            assert!(
                supervise::<V, _, _>(&[], |_: V::Trade| async { Ok(()) })
                    .await
                    .is_err(),
                "{}",
                V::NAME
            );
        }
        refuses::<Binance>().await;
        refuses::<Kraken>().await;
        refuses::<Bybit>().await;
        refuses::<Coinbase>().await;
    }
}

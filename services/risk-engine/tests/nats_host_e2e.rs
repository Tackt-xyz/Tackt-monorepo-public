//! End-to-end host test: spawns the real `risk-engine` binary against a live
//! `JetStream` server, drives the three ingress streams, and asserts the
//! egress stream, graceful shutdown, and cursor-exact restart behavior.
//!
//! CI supplies Compose `NATS_URL` and `DATABASE_URL`; missing infrastructure is not verified.
//! Each run uses an isolated database, stream prefix, and snapshot directory.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use risk_engine::processing::{
    AMOUNT_SCALE, AccountMode, AccountPortfolio, HyperliquidSnapshot, MarginDomain, MarginMode,
    PRICE_SCALE, PerpPosition, PortfolioGeneration, Price, PriceSourceKind, PriceUpdate,
    SignedAmount, Value,
};
use serde::{Deserialize, Serialize};
use tackt_nats::{NatsClient, recommendations::RiskRecommendationEnvelope, subjects::RiskStreams};
use tackt_postgres::{
    PgRepository, UserRiskHealthRead, UserSigningKeys, test_support::TestDatabase,
};
use tackt_primitives::accounts::AccountSubject;
use tackt_primitives::risk::RiskRecommendation;
use uuid::Uuid;

fn subject(account: u32) -> AccountSubject {
    AccountSubject::new(Uuid::from_u128(u128::from(account) + 1)).expect("non-zero subject")
}

/// Mirrors the host's wire envelope (`src/host/wire.rs`), which is private to
/// the binary. The shape is part of the published contract.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Envelope<T> {
    payload: T,
}

impl<T> Envelope<T> {
    fn new(payload: T) -> Self {
        Self { payload }
    }
}

fn price(price_index: u32, price: i128, sequence: u64) -> PriceUpdate {
    PriceUpdate {
        price_index,
        source: PriceSourceKind::Hyperliquid,
        source_sequence: sequence,
        price: Price::from_raw(price * PRICE_SCALE),
    }
}

fn domain(account: u32, account_value: i128, maintenance: i128, version: u64) -> MarginDomain {
    MarginDomain {
        account,
        domain: 1,
        account_mode: AccountMode::Unified,
        margin_mode: MarginMode::Cross,
        snapshot: Some(HyperliquidSnapshot {
            account_value: Value::from_raw(account_value * PRICE_SCALE),
            maintenance: Value::from_raw(maintenance * PRICE_SCALE),
            source_sequence: version,
        }),
        source_version: version,
    }
}

fn perp(account: u32, size: i128, anchor: i128, version: u64) -> PerpPosition {
    PerpPosition {
        account,
        domain: 1,
        market: 1,
        signed_size: SignedAmount::from_raw(size * AMOUNT_SCALE),
        anchor_price: Some(Price::from_raw(anchor * PRICE_SCALE)),
        position: account + 1,
        source_version: version,
    }
}

/// A four-account reconciliation sweep stamped past the host's first-boot
/// empty generation (version 1).
fn sweep(version: u64) -> PortfolioGeneration {
    let domains = (0..4).map(|account| domain(account, 100, 20, 1)).collect();
    let perps = (0..4)
        .map(|account| perp(account, if account % 2 == 0 { 1 } else { -1 }, 100, 1))
        .collect();
    PortfolioGeneration::new(version, Vec::new(), Vec::new(), domains, perps)
        .with_account_subjects((0..4).map(|account| (account, subject(account))).collect())
}

fn account_patch(account: u32, version: u64) -> AccountPortfolio {
    AccountPortfolio {
        account,
        account_subject: subject(account),
        version,
        ethereum_balances: Vec::new(),
        hypercore_balances: Vec::new(),
        margin_domains: vec![domain(account, 60, 25, 2)],
        perp_positions: vec![perp(account, 2, 110, 2)],
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
}

fn http_status(port: u16, path: &str) -> Option<u16> {
    let mut stream =
        TcpStream::connect_timeout(&([127, 0, 0, 1], port).into(), Duration::from_secs(1)).ok()?;
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).ok()?;
    response
        .split_whitespace()
        .nth(1)
        .and_then(|status| status.parse().ok())
}

/// Kills the child on drop so a panicking assertion never leaks a host.
struct Host(Child);

impl Drop for Host {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_host(
    port: u16,
    prefix: &str,
    snapshot_dir: &Path,
    nats_url: &str,
    database_url: &str,
    history_account: AccountSubject,
) -> Host {
    Host(
        Command::new(env!("CARGO_BIN_EXE_risk-engine"))
            .env("RISK_ENGINE_LISTEN_ADDR", format!("127.0.0.1:{port}"))
            .env("RISK_ENGINE_STREAM_PREFIX", prefix)
            .env("RISK_ENGINE_SNAPSHOT_DIR", snapshot_dir)
            .env("RISK_ENGINE_ACCOUNT_COUNT", "8")
            .env("RISK_ENGINE_SHARD_COUNT", "2")
            .env(
                "RISK_ENGINE_INTERNAL_TOKEN",
                "test-risk-engine-token-at-least-24-bytes",
            )
            .env("NATS_URL", nats_url)
            .env("RISK_ENGINE_NATS_URL", nats_url)
            .env("RISK_ENGINE_DATABASE_URL", database_url)
            .env_remove("RISK_ENGINE_MIGRATION_DATABASE_URL")
            .env("RISK_ENGINE_LEDGER_REFRESH_INTERVAL_SECS", "1")
            .env("RISK_ENGINE_HISTORY_ACCOUNTS", history_account.to_string())
            .env("RISK_ENGINE_HISTORY_INTERVAL_SECS", "1")
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn risk-engine"),
    )
}

fn wait_ready(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if http_status(port, "/ready") == Some(200) {
            return;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    panic!("risk-engine never became ready");
}

fn terminate_gracefully(host: &mut Host) {
    let status = Command::new("kill")
        .args(["-TERM", &host.0.id().to_string()])
        .status()
        .expect("send SIGTERM");
    assert!(status.success(), "SIGTERM delivery failed");
    let exit = host.0.wait().expect("await exit");
    assert!(exit.success(), "graceful shutdown must exit 0: {exit:?}");
}

async fn collect_events(
    consumer: &mut tackt_nats::Consumer<RiskRecommendationEnvelope>,
    quiet: Duration,
) -> Vec<RiskRecommendation> {
    let mut events = Vec::new();
    while let Ok(Ok(delivery)) = tokio::time::timeout(quiet, consumer.next()).await {
        let (envelope, ack) = delivery.split();
        events.push(envelope.payload);
        ack.ack().await.expect("ack event");
    }
    events
}

fn test_nats_url() -> Option<String> {
    let Ok(nats_url) = std::env::var("NATS_URL") else {
        assert!(
            std::env::var_os("CI").is_none(),
            "NATS_URL is unset in CI; refusing to silently skip NATS integration tests"
        );
        eprintln!("skipping: set NATS_URL to run NATS integration tests");
        return None;
    };
    Some(nats_url)
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn host_processes_streams_and_recovers_after_restart() {
    let Some(nats_url) = test_nats_url() else {
        return;
    };
    let Some((database_url, database)) = TestDatabase::create().await else {
        return;
    };
    let repository = PgRepository::initialise(&database_url, 2)
        .await
        .expect("repository");
    let history_account = create_history_user(&repository).await;
    // Unique across parallel CI runs sharing one NATS server.
    let run = format!(
        "{:X}{:X}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos()
    );
    let prefix = format!("E2E{run}");
    let snapshot_dir = std::env::temp_dir().join(format!("risk-host-e2e-{run}"));
    if snapshot_dir.exists() {
        std::fs::remove_dir_all(&snapshot_dir).expect("clean snapshot dir");
    }
    let client = tackt_nats::initialise(&nats_url).await.expect("connect");
    let streams = RiskStreams::with_prefix(&prefix);
    provision_streams(&client, &streams).await;

    // First run: the host boots on an empty generation, then processes a
    // sweep, a patch, and prices published while it is live.
    let port = free_port();
    let mut host = spawn_host(
        port,
        &prefix,
        &snapshot_dir,
        &nats_url,
        &database_url,
        history_account,
    );
    wait_ready(port);
    assert_eq!(http_status(port, "/health"), Some(200));

    let mut generation = sweep(2);
    generation.account_subjects.insert(0, history_account);
    client
        .publish_durable(streams.sweeps.subject(), None, &Envelope::new(generation))
        .await
        .expect("publish sweep");
    // Broker acknowledgement does not order application across separate streams.
    // This user's first sample proves the sweep applied before the newer patch,
    // which would otherwise make the sweep stale if it won the ingress race.
    wait_for_health(&repository, history_account, |_| true).await;
    client
        .publish_durable(
            streams.patches.subject(),
            None,
            &Envelope::new(account_patch(1, 3)),
        )
        .await
        .expect("publish patch");
    client
        .publish_durable(
            streams.prices.subject(),
            None,
            &Envelope::new(price(3, 110, 1)),
        )
        .await
        .expect("publish price");
    client
        .publish_durable(
            streams.prices.subject(),
            None,
            &Envelope::new(price(3, 45, 2)),
        )
        .await
        .expect("publish crash price");

    let mut watcher = client
        .risk_recommendations(&streams.recommendations, "e2e-watch")
        .await
        .expect("recommendations consumer");
    let first_run = collect_events(&mut watcher, Duration::from_secs(10)).await;
    assert!(
        !first_run.is_empty(),
        "processing the sweep/patch/prices must emit risk recommendations"
    );

    let before_restart = wait_for_health(&repository, history_account, |sample| {
        sample.health.is_some()
    })
    .await;

    // Graceful restart: exit 0, a snapshot on disk, no re-broadcast of
    // anything downstream already heard, and cursors past consumed input.
    terminate_gracefully(&mut host);
    let snapshots = std::fs::read_dir(&snapshot_dir)
        .expect("snapshot dir")
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("snapshot-"))
        .count();
    assert!(snapshots > 0, "graceful shutdown must leave a snapshot");

    let mut host = spawn_host(
        port,
        &prefix,
        &snapshot_dir,
        &nats_url,
        &database_url,
        history_account,
    );
    wait_ready(port);
    let after_restart = collect_events(&mut watcher, Duration::from_secs(3)).await;
    assert!(
        after_restart.is_empty(),
        "a clean restart must not re-broadcast: {after_restart:?}"
    );

    let recovered = wait_for_health(&repository, history_account, |sample| {
        sample.sampled_at > before_restart.sampled_at
    })
    .await;
    assert_eq!(recovered.health, before_restart.health);

    // Fresh input still flows: cursors resumed past the old messages while
    // new ones process normally.
    client
        .publish_durable(
            streams.prices.subject(),
            None,
            &Envelope::new(price(3, 105, 3)),
        )
        .await
        .expect("publish post-restart price");
    let post_restart = collect_events(&mut watcher, Duration::from_secs(10)).await;
    assert!(
        !post_restart.is_empty(),
        "a post-restart price crossing thresholds must emit events"
    );

    wait_for_health(&repository, history_account, |sample| {
        sample.health.is_some() && sample.health != recovered.health
    })
    .await;
    terminate_gracefully(&mut host);
    drop(repository);
    drop(watcher);
    database.cleanup().await;
    cleanup_streams(&client, &streams).await;
}

async fn provision_streams(client: &NatsClient, streams: &RiskStreams) {
    for stream in [
        &streams.prices,
        &streams.patches,
        &streams.sweeps,
        &streams.recommendations,
    ] {
        client
            .ensure_stream(stream)
            .await
            .expect("provision isolated test stream");
    }
}

async fn cleanup_streams(client: &NatsClient, streams: &RiskStreams) {
    for spec in [
        &streams.prices,
        &streams.patches,
        &streams.sweeps,
        &streams.recommendations,
    ] {
        let _ = client.delete_stream(&spec.name).await;
    }
}

fn random_key() -> [u8; 32] {
    let mut key = [0; 32];
    key[..16].copy_from_slice(Uuid::new_v4().as_bytes());
    key[16..].copy_from_slice(Uuid::new_v4().as_bytes());
    key
}

async fn create_history_user(repository: &PgRepository) -> AccountSubject {
    let (user, _) = repository
        .create_core_user(
            tackt_primitives::CreateCoreUser {
                external_id: "risk-history-e2e".to_owned(),
                metadata: serde_json::json!({}),
            },
            &UserSigningKeys {
                addresses: None,
                ethereum_private_key: random_key(),
                solana_private_key: random_key(),
            },
        )
        .await
        .expect("history user");
    AccountSubject::new(user.id).expect("subject")
}

async fn wait_for_health(
    repository: &PgRepository,
    account: AccountSubject,
    matches: impl Fn(&tackt_postgres::RiskHealthSample) -> bool,
) -> tackt_postgres::RiskHealthSample {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let UserRiskHealthRead::Available(mut samples) = repository
                .latest_user_risk_health(account.user_id())
                .await
                .expect("read history")
            else {
                panic!("history user must exist");
            };
            if let Some(sample) = samples.pop().filter(&matches) {
                return sample;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the real host must persist the expected timer sample within 30 seconds")
}

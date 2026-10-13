use std::sync::{Once, OnceLock};

use metrics_exporter_prometheus::PrometheusBuilder;
pub use metrics_exporter_prometheus::PrometheusHandle;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::LevelFilter;

/// Install the platform-wide structured logging defaults.
///
/// Logs at `info` and above unless `RUST_LOG` overrides it; invalid `RUST_LOG`
/// directives are dropped individually instead of silencing all output.
/// Repeated calls in the same process are harmless; only the first installs the
/// global subscriber.
pub fn initialise() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let filter = EnvFilter::builder()
            .with_default_directive(LevelFilter::INFO.into())
            .from_env_lossy();
        if let Err(error) = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .try_init()
        {
            eprintln!("tackt-telemetry: failed to install global subscriber: {error}");
        }
    });
}

/// Install the process-wide Prometheus metrics recorder and return its render
/// handle for a `/metrics` endpoint.
///
/// Idempotent: repeated calls in the same process return the same handle.
///
/// # Panics
///
/// Panics if another global metrics recorder was already installed outside this
/// function — a programming error, and per the initialization convention the
/// service must fail startup rather than run without its observability.
pub fn initialise_metrics() -> PrometheusHandle {
    static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
    HANDLE
        .get_or_init(|| {
            PrometheusBuilder::new()
                .install_recorder()
                .expect("install the process-wide Prometheus metrics recorder")
        })
        .clone()
}

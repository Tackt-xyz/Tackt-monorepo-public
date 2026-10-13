//! `PostgreSQL`-backed Ledger inputs for portfolio credit. Health is calculated separately.

use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tackt_postgres::{LedgerBalanceSnapshot, PgRepository};

/// Current authoritative balances, including pending funds, reservations, and booked debt.
/// This host cache is rebuilt from `PostgreSQL` on every startup; it never writes Ledger state.
#[derive(Default)]
pub struct LedgerInputs {
    /// Last complete successful read. A refresh replaces the entire snapshot, including removals.
    pub snapshot: RwLock<Option<LedgerBalanceSnapshot>>,
    /// False after any failed read, even though the last successful inputs remain available.
    pub ready: AtomicBool,
}

impl LedgerInputs {
    /// Reads a complete replacement before publishing it to the host.
    pub async fn refresh(&self, repository: &PgRepository) -> anyhow::Result<()> {
        let result = repository
            .ledger_balance_snapshot()
            .await
            .and_then(|snapshot| {
                let accounts = snapshot.accounts.len();
                let mut current = self
                    .snapshot
                    .write()
                    .map_err(|_| anyhow::anyhow!("Ledger input snapshot lock is poisoned"))?;
                *current = Some(snapshot);
                tracing::debug!(accounts, "refreshed Ledger credit inputs from PostgreSQL");
                Ok(())
            });
        self.ready.store(result.is_ok(), Ordering::SeqCst);
        result
    }
}

/// Periodic complete reads also recover missed changes without a notification/cursor protocol.
/// The initial read has already succeeded before this worker starts.
pub async fn refresh_loop(
    repository: PgRepository,
    state: Arc<super::HostState>,
    interval: Duration,
) {
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick.tick().await;
    loop {
        tick.tick().await;
        if let Err(error) = state.ledger.refresh(&repository).await {
            tracing::warn!(%error, "Ledger input refresh failed; retaining last snapshot and failing readiness");
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::host::HostState;
    use chrono::Utc;
    use tackt_postgres::test_support::TestDatabase;

    use super::*;

    #[tokio::test]
    async fn startup_refresh_failure_preserves_inputs_and_success_replaces_them() {
        let Some((url, database)) = TestDatabase::create().await else {
            return;
        };
        let repository = PgRepository::connect(&url, 2)
            .await
            .expect("connect empty database");
        let inputs = LedgerInputs::default();
        let previous = LedgerBalanceSnapshot {
            as_of: Utc::now(),
            accounts: [(uuid::Uuid::from_u128(1), Vec::new())].into(),
        };
        *inputs.snapshot.write().expect("input lock") = Some(previous.clone());
        inputs.ready.store(true, Ordering::SeqCst);
        assert!(
            inputs.refresh(&repository).await.is_err(),
            "missing schema fails the read"
        );
        assert!(!inputs.ready.load(Ordering::SeqCst));
        assert_eq!(*inputs.snapshot.read().expect("input lock"), Some(previous));

        let repository = PgRepository::initialise(&url, 2)
            .await
            .expect("initialise schema");
        inputs
            .refresh(&repository)
            .await
            .expect("refresh current balances");
        assert!(inputs.ready.load(Ordering::SeqCst));
        assert!(
            inputs
                .snapshot
                .read()
                .expect("input lock")
                .as_ref()
                .expect("snapshot")
                .accounts
                .is_empty(),
            "a full read removes prior accounts"
        );
        database.cleanup().await;
    }

    #[tokio::test]
    async fn timer_refreshes_the_database_snapshot() {
        let Some((url, database)) = TestDatabase::create().await else {
            return;
        };
        let repository = PgRepository::initialise(&url, 2)
            .await
            .expect("initialise schema");
        let state = Arc::new(HostState::default());
        state
            .ledger
            .refresh(&repository)
            .await
            .expect("initial load");
        let initial_time = state
            .ledger
            .snapshot
            .read()
            .expect("input lock")
            .as_ref()
            .expect("snapshot")
            .as_of;
        let worker = tokio::spawn(refresh_loop(
            repository,
            state.clone(),
            Duration::from_millis(20),
        ));
        let refreshed = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let observed = state
                    .ledger
                    .snapshot
                    .read()
                    .expect("input lock")
                    .as_ref()
                    .expect("snapshot")
                    .as_of;
                if observed > initial_time {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        worker.abort();
        let _ = worker.await;
        assert!(
            refreshed.is_ok(),
            "periodic refresh must replace the initial read"
        );
        assert!(state.ledger.ready.load(Ordering::SeqCst));
        database.cleanup().await;
    }
}

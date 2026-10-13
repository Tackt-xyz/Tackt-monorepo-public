//! Process-local user registry.
//!
//! PostgreSQL is authoritative; NATS user-lifecycle events only accelerate
//! convergence, and a periodic full reconciliation bounds drift from lost
//! deliveries. Reads take a brief shared lock to clone an `Arc` snapshot, so
//! the ingestion hot path never contends with PostgreSQL round-trips.

use anyhow::{Result, bail};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};
use tackt_nats::{UserEvent, UserEventKind};
use tackt_postgres::Repository;
use tackt_primitives::User;
use uuid::Uuid;

/// How often the registry is compared against PostgreSQL and repaired.
pub const RECONCILE_INTERVAL: Duration = Duration::from_secs(5 * 60);

type Owners = HashMap<String, Uuid>;

/// Shared handle to the current registration snapshot. Writers rebuild the
/// next snapshot off-lock and swap it in atomically; readers are never blocked
/// by PostgreSQL.
#[derive(Clone, Default)]
pub struct UserRegistry {
    snapshot: Arc<RwLock<Arc<Registrations>>>,
    /// Serializes the PostgreSQL read with the snapshot swap so an older full
    /// query cannot overwrite a newer targeted refresh.
    update: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Clone, Default, PartialEq, Eq)]
pub(crate) struct Registrations {
    users: HashMap<Uuid, User>,
    ethereum: Owners,
}

impl Registrations {
    pub(crate) fn ethereum_owners(&self) -> &Owners {
        &self.ethereum
    }

    fn insert(&mut self, user: User) -> Result<()> {
        if user.enabled {
            for address in &user.ethereum_addresses {
                let address = address.to_lowercase();
                if let Some(owner) = self.ethereum.insert(address.clone(), user.id)
                    && owner != user.id
                {
                    bail!(
                        "Ethereum address {address} is registered to both {owner} and {}",
                        user.id
                    )
                }
            }
        }
        self.users.insert(user.id, user);
        Ok(())
    }

    fn remove(&mut self, user_id: Uuid) {
        if let Some(user) = self.users.remove(&user_id) {
            for address in user.ethereum_addresses {
                self.ethereum.remove(&address.to_lowercase());
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RegistryChange {
    pub users: usize,
    pub ethereum_accounts: usize,
    pub changed: bool,
    pub added: usize,
    pub removed: usize,
    pub reassigned: usize,
}

impl UserRegistry {
    pub(crate) fn registrations(&self) -> Arc<Registrations> {
        self.snapshot
            .read()
            .expect("Indexer user registry lock")
            .clone()
    }

    /// Atomically compares and replaces the complete registry from
    /// PostgreSQL. Reapplying an identical snapshot is an intentional no-op.
    pub async fn reconcile<R: Repository>(&self, repo: &R) -> Result<RegistryChange> {
        let _update_guard = self.update.lock().await;
        let mut next = Registrations::default();
        for user in repo.users().await? {
            next.insert(user)?;
        }
        Ok(self.replace(next))
    }

    /// Re-reads one user's registration after a lifecycle signal. The previous
    /// registration is replaced by the current PostgreSQL row — or removed when
    /// the row is gone — so replacements, restrictions, and duplicate NATS
    /// deliveries are all idempotent.
    pub async fn refresh_user<R: Repository>(
        &self,
        repo: &R,
        user_id: Uuid,
    ) -> Result<RegistryChange> {
        let _update_guard = self.update.lock().await;
        let user = repo.user_registration(user_id).await?;
        if let Some(user) = &user
            && user.id != user_id
        {
            bail!(
                "user registry refresh requested user {user_id} but PostgreSQL returned {}",
                user.id
            )
        }
        let mut next = (*self.registrations()).clone();
        next.remove(user_id);
        if let Some(user) = user {
            next.insert(user)?;
        }
        Ok(self.replace(next))
    }

    /// Applies NATS lifecycle signals by re-reading each user from PostgreSQL.
    /// Failures are logged rather than returned: the periodic reconciler
    /// repairs whatever an event-triggered refresh missed.
    pub async fn apply_events<R: Repository>(
        &self,
        repo: &R,
        users: impl IntoIterator<Item = Uuid>,
    ) {
        for user_id in users {
            match self.refresh_user(repo, user_id).await {
                Ok(change) if change.changed => tracing::info!(
                    %user_id,
                    users = change.users,
                    ethereum_accounts = change.ethereum_accounts,
                    added = change.added,
                    removed = change.removed,
                    reassigned = change.reassigned,
                    "user event updated Indexer user registry"
                ),
                Ok(_) => tracing::debug!(
                    %user_id,
                    "duplicate user event left Indexer user registry unchanged"
                ),
                Err(error) => tracing::warn!(
                    %error,
                    %user_id,
                    "user-event registry update failed; periodic reconciliation will repair it"
                ),
            }
        }
    }

    fn replace(&self, next: Registrations) -> RegistryChange {
        let mut current = self.snapshot.write().expect("Indexer user registry lock");
        let change = compare(&current, &next);
        if change.changed {
            *current = Arc::new(next);
        }
        change
    }
}

/// The user whose registration a lifecycle event affects. Events are only a
/// hint; the registration itself is always re-read from PostgreSQL.
pub fn affected_user(event: &UserEvent) -> Uuid {
    match &event.kind {
        UserEventKind::Created { user } => user.id,
        UserEventKind::StatusChanged { user_id, .. } => *user_id,
        UserEventKind::AccountAssociated { user_id, .. } => *user_id,
    }
}

/// Full reconciliation loop: every [`RECONCILE_INTERVAL`] the registry is
/// compared against PostgreSQL and repaired.
pub async fn reconcile_forever<R: Repository>(registry: UserRegistry, repo: Arc<R>) {
    let start = tokio::time::Instant::now() + RECONCILE_INTERVAL;
    let mut ticker = tokio::time::interval_at(start, RECONCILE_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        match registry.reconcile(repo.as_ref()).await {
            Ok(change) if change.changed => tracing::warn!(
                users = change.users,
                ethereum_accounts = change.ethereum_accounts,
                added = change.added,
                removed = change.removed,
                reassigned = change.reassigned,
                "Indexer user registry mismatch detected and repaired from PostgreSQL"
            ),
            Ok(change) => tracing::debug!(
                users = change.users,
                ethereum_accounts = change.ethereum_accounts,
                "Indexer user registry reconciliation complete"
            ),
            Err(error) => {
                tracing::error!(%error, "Indexer user registry reconciliation failed")
            }
        }
    }
}

fn compare(current: &Registrations, next: &Registrations) -> RegistryChange {
    let added = next
        .ethereum
        .keys()
        .filter(|address| !current.ethereum.contains_key(*address))
        .count();
    let removed = current
        .ethereum
        .keys()
        .filter(|address| !next.ethereum.contains_key(*address))
        .count();
    let reassigned = next
        .ethereum
        .iter()
        .filter(|(address, owner)| {
            current
                .ethereum
                .get(*address)
                .is_some_and(|current_owner| current_owner != *owner)
        })
        .count();
    RegistryChange {
        users: next.users.len(),
        ethereum_accounts: next.ethereum.len(),
        changed: current != next,
        added,
        removed,
        reassigned,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tackt_postgres::MemoryRepository;

    fn user(id: Uuid, enabled: bool, addresses: &[&str]) -> User {
        User {
            id,
            external_id: id.to_string(),
            enabled,
            ethereum_addresses: addresses.iter().map(|value| (*value).into()).collect(),
            hyperliquid_accounts: Vec::new(),
        }
    }

    #[tokio::test]
    async fn postgres_seed_followed_by_duplicate_event_is_idempotent() {
        let repo = MemoryRepository::default();
        let registry = UserRegistry::default();
        let id = Uuid::new_v4();
        repo.insert_user(user(id, true, &["0xAbC"])).await;

        assert!(registry.reconcile(&repo).await.unwrap().changed);
        let duplicate = registry.refresh_user(&repo, id).await.unwrap();

        assert!(!duplicate.changed);
        assert_eq!(duplicate.ethereum_accounts, 1);
        assert_eq!(
            registry.registrations().ethereum_owners().get("0xabc"),
            Some(&id)
        );
    }

    #[tokio::test]
    async fn targeted_refresh_replaces_and_disables_accounts() {
        let repo = MemoryRepository::default();
        let registry = UserRegistry::default();
        let id = Uuid::new_v4();
        repo.insert_user(user(id, true, &["0xold"])).await;
        registry.reconcile(&repo).await.unwrap();

        repo.insert_user(user(id, true, &["0xnew"])).await;
        let replaced = registry.refresh_user(&repo, id).await.unwrap();
        assert_eq!((replaced.added, replaced.removed), (1, 1));
        assert!(
            !registry
                .registrations()
                .ethereum_owners()
                .contains_key("0xold")
        );

        repo.insert_user(user(id, false, &["0xnew"])).await;
        let disabled = registry.refresh_user(&repo, id).await.unwrap();
        assert_eq!(disabled.removed, 1);
        assert!(registry.registrations().ethereum_owners().is_empty());
    }

    #[tokio::test]
    async fn full_reconciliation_reports_and_repairs_mismatch() {
        let stale = MemoryRepository::default();
        let authoritative = MemoryRepository::default();
        let registry = UserRegistry::default();
        let id = Uuid::new_v4();
        stale.insert_user(user(id, true, &["0xstale"])).await;
        authoritative
            .insert_user(user(id, true, &["0xauthoritative"]))
            .await;
        registry.reconcile(&stale).await.unwrap();

        let repaired = registry.reconcile(&authoritative).await.unwrap();
        assert!(repaired.changed);
        assert_eq!((repaired.added, repaired.removed), (1, 1));
        assert_eq!(
            registry
                .registrations()
                .ethereum_owners()
                .get("0xauthoritative"),
            Some(&id)
        );
    }

    #[tokio::test]
    async fn refreshing_a_user_missing_from_postgres_removes_the_registration() {
        let seeded = MemoryRepository::default();
        let registry = UserRegistry::default();
        let id = Uuid::new_v4();
        seeded.insert_user(user(id, true, &["0xabc"])).await;
        registry.reconcile(&seeded).await.unwrap();

        let removed = registry
            .refresh_user(&MemoryRepository::default(), id)
            .await
            .unwrap();
        assert!(removed.changed);
        assert!(registry.registrations().ethereum_owners().is_empty());
    }

    #[tokio::test]
    async fn non_ethereum_changes_update_the_global_registry() {
        let repo = MemoryRepository::default();
        let registry = UserRegistry::default();
        let id = Uuid::new_v4();
        let mut projection = user(id, true, &[]);
        repo.insert_user(projection.clone()).await;
        registry.reconcile(&repo).await.unwrap();

        projection.hyperliquid_accounts.push("0xhyperliquid".into());
        repo.insert_user(projection).await;
        let change = registry.refresh_user(&repo, id).await.unwrap();
        assert!(change.changed);
        assert_eq!(change.ethereum_accounts, 0);
    }
}

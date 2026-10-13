//! PostgreSQL integration-test support shared by workspace crates.

use sqlx::{Executor, PgPool};
use url::Url;
use uuid::Uuid;

/// Display-history fixture with values beyond JavaScript's exact integer range.
pub fn risk_health_sample(
    user: Uuid,
    sampled_at: chrono::DateTime<chrono::Utc>,
) -> crate::RiskHealthSample {
    crate::RiskHealthSample {
        sample_id: Uuid::new_v4(),
        account: tackt_primitives::accounts::AccountSubject::new(user).expect("user"),
        sampled_at,
        classification: tackt_primitives::risk::RiskClassification::Healthy,
        health: Some(i128::MAX - 1),
    }
}

pub struct TestDatabase {
    base_url: String,
    name: String,
    admin: PgPool,
    armed: bool,
    owner_role: Option<String>,
}

impl TestDatabase {
    pub async fn ledger_projection(repository: &crate::PgRepository) -> Vec<(String, i128)> {
        sqlx::query_as::<_, (String, String)>("SELECT a.code,(b.debit_quantity-b.credit_quantity)::text FROM ledger.accounts a JOIN ledger.current_balances b ON b.account_id=a.id")
            .fetch_all(&repository.pool).await.unwrap().into_iter()
            .map(|(code, quantity)| (code, quantity.parse().unwrap())).collect()
    }
    pub async fn create() -> Option<(String, Self)> {
        Self::create_with_owner(false).await
    }

    /// Use only with the disposable Compose administrator connection. The returned
    /// URL assumes a database owner with no superuser or cluster role privileges.
    pub async fn create_unprivileged() -> Option<(String, Self)> {
        Self::create_with_owner(true).await
    }

    async fn create_with_owner(unprivileged: bool) -> Option<(String, Self)> {
        let Ok(base_url) =
            std::env::var("DATABASE_URL").or_else(|_| std::env::var("LEDGER_TEST_DATABASE_URL"))
        else {
            assert!(
                std::env::var_os("CI").is_none(),
                "DATABASE_URL is unset in CI; refusing to skip PostgreSQL integration tests"
            );
            eprintln!(
                "skipping: set DATABASE_URL or LEDGER_TEST_DATABASE_URL to run PostgreSQL integration tests"
            );
            return None;
        };
        let admin = PgPool::connect(&base_url)
            .await
            .expect("connect test administrator database");
        let name = format!("tackt_ledger_test_{}", Uuid::new_v4().simple());
        admin
            .execute(format!("CREATE DATABASE {name}").as_str())
            .await
            .expect("create isolated ledger database");
        let mut url = Url::parse(&base_url).expect("valid PostgreSQL URL");
        url.set_path(&format!("/{name}"));
        let mut database = Self {
            base_url,
            name,
            admin,
            armed: true,
            owner_role: None,
        };
        if unprivileged {
            let role = format!("{}_owner", database.name);
            database
                .admin
                .execute(
                    format!(
                        "CREATE ROLE {role} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT"
                    )
                    .as_str(),
                )
                .await
                .expect("create unprivileged test owner");
            database.owner_role = Some(role.clone());
            database
                .admin
                .execute(format!("ALTER DATABASE {} OWNER TO {role}", database.name).as_str())
                .await
                .expect("assign isolated database to its test owner");
            url.query_pairs_mut()
                .append_pair("options", &format!("-c role={role}"));
        }
        Some((url.to_string(), database))
    }

    pub async fn cleanup(mut self) {
        self.admin
            .execute(format!("DROP DATABASE {} WITH (FORCE)", self.name).as_str())
            .await
            .expect("drop isolated ledger database");
        if let Some(role) = &self.owner_role {
            self.admin
                .execute(format!("DROP ROLE {role}").as_str())
                .await
                .expect("drop isolated test owner");
        }
        self.armed = false;
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let base_url = self.base_url.clone();
        let name = self.name.clone();
        let owner_role = self.owner_role.clone();
        let _ = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build cleanup runtime");
            runtime.block_on(async move {
                if let Ok(pool) = PgPool::connect(&base_url).await {
                    let _ = pool
                        .execute(format!("DROP DATABASE {name} WITH (FORCE)").as_str())
                        .await;
                    if let Some(role) = owner_role {
                        let _ = pool.execute(format!("DROP ROLE {role}").as_str()).await;
                    }
                }
            });
        })
        .join();
    }
}

/// Account-page fixture: one finalized deposit, a credit draw, and a repayment.
/// All entries share a microsecond timestamp to exercise the UUID pagination tie-breaker.
/// A foreign user's posting on the draw verifies that transaction reads filter every leg.
pub async fn account_page_fixture(
    repository: &crate::PgRepository,
    user: Uuid,
    other: Uuid,
) -> Vec<Uuid> {
    use crate::{LedgerAccount, LedgerAccountDefinition, LedgerPostingWrite};
    use chrono::Utc;
    use tackt_primitives::{
        assets,
        ledger::{BalanceBucket, LedgerAsset, PostingSide},
    };
    let at = chrono::DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
    let units = |asset, n| n * 10_u128.pow(u32::from(assets::info(asset).decimals));
    let account = |user_id, asset, bucket: BalanceBucket, debit, credit| LedgerAccount {
        id: Uuid::new_v4(),
        definition: LedgerAccountDefinition {
            code: Uuid::new_v4().to_string(),
            user_id,
            asset,
            bucket: bucket.as_ref().into(),
            allow_negative: false,
        },
        debit,
        credit,
    };
    let accounts = [
        account(
            Some(user),
            LedgerAsset::Eth,
            BalanceBucket::Available,
            units(LedgerAsset::Eth, 4),
            0,
        ),
        account(
            Some(user),
            LedgerAsset::Eth,
            BalanceBucket::Reserved,
            units(LedgerAsset::Eth, 1),
            0,
        ),
        account(
            Some(user),
            LedgerAsset::Usdc,
            BalanceBucket::Available,
            units(LedgerAsset::Usdc, 2500),
            units(LedgerAsset::Usdc, 500),
        ),
        account(
            Some(user),
            LedgerAsset::Usdc,
            BalanceBucket::PrincipalLiability,
            units(LedgerAsset::Usdc, 500),
            units(LedgerAsset::Usdc, 2500),
        ),
        account(
            Some(other),
            LedgerAsset::Usdc,
            BalanceBucket::Available,
            units(LedgerAsset::Usdc, 99),
            0,
        ),
        account(
            None,
            LedgerAsset::Usdc,
            BalanceBucket::Available,
            0,
            units(LedgerAsset::Usdc, 2500),
        ),
    ];
    let mut write = repository.begin_ledger_write().await.unwrap();
    write.store_accounts(&accounts, at).await.unwrap();
    let mut ids = Vec::new();
    let posting = |index: usize, side, quantity| LedgerPostingWrite {
        account_id: accounts[index].id,
        asset: accounts[index].definition.asset,
        side,
        quantity,
    };
    let entries = [
        (
            "finalized",
            vec![posting(0, PostingSide::Debit, units(LedgerAsset::Eth, 4))],
        ),
        (
            "draw_credit",
            vec![
                posting(2, PostingSide::Debit, units(LedgerAsset::Usdc, 2500)),
                posting(3, PostingSide::Credit, units(LedgerAsset::Usdc, 2500)),
                posting(4, PostingSide::Debit, units(LedgerAsset::Usdc, 99)),
                posting(5, PostingSide::Credit, units(LedgerAsset::Usdc, 2500)),
            ],
        ),
        (
            "record_principal_repayment",
            vec![
                posting(2, PostingSide::Credit, units(LedgerAsset::Usdc, 500)),
                posting(3, PostingSide::Debit, units(LedgerAsset::Usdc, 500)),
            ],
        ),
        ("foreign_only", vec![posting(4, PostingSide::Debit, 1)]),
    ];
    for (kind, postings) in entries {
        let id = Uuid::new_v4();
        // Explicit timestamps and deterministic read fixtures, never application accounting.
        sqlx::query("INSERT INTO ledger.transactions(id,command_source,chain_id,command_kind,command_hash,idempotency_key,recorded_at,effective_at,audit) VALUES($1,'evm',1,$2,'fixture',$1::text,$3,$3,'{}')")
            .bind(id).bind(kind).bind(at).execute(&mut *write.0).await.unwrap();
        write.append_postings(id, &postings).await.unwrap();
        if kind != "foreign_only" {
            ids.push(id);
        }
    }
    write.commit().await.unwrap();
    ids.sort_by(|a, b| b.cmp(a));
    ids
}

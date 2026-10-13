use serde_json::json;
use sqlx::{Executor, PgPool};
use tackt_postgres::{PgRepository, UserRepositoryError};
use tackt_primitives::{CreateCoreUser, Source, UserStatus};
use url::Url;
use uuid::Uuid;

struct TestDatabase {
    base_url: String,
    name: String,
    armed: bool,
}
impl TestDatabase {
    async fn cleanup(mut self, admin: &PgPool) {
        admin
            .execute(format!("DROP DATABASE {} WITH (FORCE)", self.name).as_str())
            .await
            .expect("drop test database");
        self.armed = false;
    }
}
impl Drop for TestDatabase {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let base = self.base_url.clone();
        let name = self.name.clone();
        let _ = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                if let Ok(pool) = PgPool::connect(&base).await {
                    let _ = pool
                        .execute(format!("DROP DATABASE {name} WITH (FORCE)").as_str())
                        .await;
                }
            });
        })
        .join();
    }
}
/// Returns `None` when no database is configured so `cargo test --workspace`
/// stays green on machines without PostgreSQL; CI supplies `DATABASE_URL`.
async fn database() -> Option<(String, PgPool, TestDatabase)> {
    let Ok(base) =
        std::env::var("DATABASE_URL").or_else(|_| std::env::var("CORE_API_TEST_DATABASE_URL"))
    else {
        // Skipping is a local-development convenience only; in CI a missing
        // DATABASE_URL must fail loudly, or the whole integration suite silently
        // evaporates while reporting green.
        assert!(
            std::env::var_os("CI").is_none(),
            "DATABASE_URL is unset in CI; refusing to silently skip PostgreSQL integration tests"
        );
        eprintln!("skipping: set DATABASE_URL to run PostgreSQL integration tests");
        return None;
    };
    let admin = PgPool::connect(&base).await.expect("connect test database");
    let name = format!("tackt_users_test_{}", Uuid::new_v4().simple());
    admin
        .execute(format!("CREATE DATABASE {name}").as_str())
        .await
        .expect("create isolated test database");
    let mut url = Url::parse(&base).unwrap();
    url.set_path(&format!("/{name}"));
    Some((
        url.to_string(),
        admin,
        TestDatabase {
            base_url: base,
            name,
            armed: true,
        },
    ))
}

#[tokio::test]
async fn provisioning_is_idempotent_and_associations_are_unique() {
    let Some((url, admin, database)) = database().await else {
        return;
    };
    let repo = PgRepository::initialise(&url, 5)
        .await
        .expect("clean migrations succeed");
    PgRepository::initialise(&url, 2)
        .await
        .expect("migrations repeat safely");
    let input = CreateCoreUser {
        external_id: "tester-001".into(),
        metadata: json!({"cohort":"private-beta"}),
    };
    let keys = test_signing_keys();
    let (user, created) = repo.create_core_user(input.clone(), &keys).await.unwrap();
    assert!(created);
    assert_eq!(user.status, UserStatus::Pending);
    let (duplicate, created) = repo
        .create_core_user(input, &test_signing_keys())
        .await
        .unwrap();
    assert!(!created);
    assert_eq!(duplicate.id, user.id);
    let persisted = repo.user_signing_keys(user.id).await.unwrap().unwrap();
    assert!(persisted.ethereum_private_key == keys.ethereum_private_key);
    assert!(persisted.solana_private_key == keys.solana_private_key);
    assert!(matches!(
        repo.create_core_user(
            CreateCoreUser {
                external_id: "tester-001".into(),
                metadata: json!({"different":true})
            },
            &test_signing_keys()
        )
        .await,
        Err(UserRepositoryError::IdempotencyConflict)
    ));
    let wallet = "0x1111111111111111111111111111111111111111".to_owned();
    let user = repo
        .replace_user_account(user.id, Source::Ethereum, wallet.clone())
        .await
        .unwrap();
    let user = repo
        .replace_user_account(
            user.id,
            Source::Hyperliquid,
            "0x2222222222222222222222222222222222222222".into(),
        )
        .await
        .unwrap();
    let (user, changed) = repo
        .update_user_status(user.id, UserStatus::Active)
        .await
        .unwrap();
    assert!(changed);
    assert_eq!(
        user.accounts.get(&Source::Ethereum).map(String::as_str),
        Some(wallet.as_str())
    );
    assert_eq!(user.status, UserStatus::Active);
    let (profile_user, addresses) = repo.core_user_with_wallet_addresses(user.id).await.unwrap();
    assert_eq!(profile_user, user);
    assert_eq!(addresses, None);
    let registrations = repo.hyperliquid_account_registrations().await.unwrap();
    assert_eq!(registrations.len(), 1);
    assert_eq!(registrations[0].user_id, user.id);
    assert_eq!(
        registrations[0].account,
        "0x2222222222222222222222222222222222222222"
    );
    let (_, repeated) = repo
        .update_user_status(user.id, UserStatus::Active)
        .await
        .unwrap();
    assert!(!repeated, "a no-op status update must report no change");
    let (other, _) = repo
        .create_core_user(
            CreateCoreUser {
                external_id: "tester-002".into(),
                metadata: json!({}),
            },
            &test_signing_keys(),
        )
        .await
        .unwrap();
    assert!(matches!(
        repo.replace_user_account(other.id, Source::Ethereum, wallet)
            .await,
        Err(UserRepositoryError::AssociationConflict)
    ));
    let (other, _) = repo
        .update_user_status(other.id, UserStatus::Closed)
        .await
        .unwrap();
    assert!(matches!(
        repo.update_user_status(other.id, UserStatus::Active).await,
        Err(UserRepositoryError::InvalidTransition(
            UserStatus::Closed,
            UserStatus::Active
        ))
    ));
    use tackt_postgres::Repository;
    assert_eq!(
        repo.user_by_account(Source::Ethereum, &user.accounts[&Source::Ethereum])
            .await
            .unwrap(),
        Some(user.id)
    );
    let page = repo.list_core_users(1, None).await.unwrap();
    assert_eq!(page.len(), 1);
    let next = repo
        .list_core_users(1, Some((page[0].created_at, page[0].id)))
        .await
        .unwrap();
    assert_eq!(next.len(), 1);
    assert_ne!(next[0].id, page[0].id);
    drop(repo);
    database.cleanup(&admin).await;
}

#[tokio::test]
async fn external_identity_bootstrap_is_atomic_and_provider_neutral() {
    let Some((url, admin, database)) = database().await else {
        return;
    };
    let repo = std::sync::Arc::new(
        PgRepository::initialise(&url, 20)
            .await
            .expect("clean migrations succeed"),
    );

    assert!(matches!(
        repo.bootstrap_core_user_identity(
            "https://issuer.example",
            "subject-1",
            false,
            &test_signing_keys()
        )
        .await,
        Err(UserRepositoryError::NotFound)
    ));

    let mut tasks = Vec::new();
    for _ in 0..16 {
        let repo = repo.clone();
        tasks.push(tokio::spawn(async move {
            let keys = test_signing_keys();
            let result = repo
                .bootstrap_core_user_identity("https://issuer.example", "subject-1", true, &keys)
                .await
                .unwrap();
            if result.1 {
                let stored = repo.user_signing_keys(result.0.id).await.unwrap().unwrap();
                assert!(stored.ethereum_private_key == keys.ethereum_private_key);
                assert!(stored.solana_private_key == keys.solana_private_key);
            }
            result
        }));
    }
    let mut ids = Vec::new();
    let mut created = 0;
    for task in tasks {
        let (user, was_created) = task.await.unwrap();
        ids.push(user.id);
        created += usize::from(was_created);
        assert_eq!(user.status, UserStatus::Pending);
        assert!(user.external_id.starts_with("auth:"));
    }
    assert_eq!(created, 1);
    assert!(ids.iter().all(|id| *id == ids[0]));

    let resolved = repo
        .core_user_by_identity("https://issuer.example", "subject-1")
        .await
        .unwrap();
    assert_eq!(resolved.id, ids[0]);
    assert!(matches!(
        repo.core_user_by_identity("https://issuer.example", "Subject-1")
            .await,
        Err(UserRepositoryError::NotFound)
    ));
    assert!(matches!(
        repo.core_user_by_identity("https://ISSUER.example", "subject-1")
            .await,
        Err(UserRepositoryError::NotFound)
    ));
    let (other_issuer, other_created) = repo
        .bootstrap_core_user_identity(
            "https://other-issuer.example",
            "subject-1",
            true,
            &test_signing_keys(),
        )
        .await
        .unwrap();
    assert!(other_created);
    assert_ne!(other_issuer.id, resolved.id);

    assert!(
        repo.link_core_user_identity(
            resolved.id,
            "https://issuer.example",
            "operator-enrolled-subject",
        )
        .await
        .unwrap()
    );
    assert!(
        !repo
            .link_core_user_identity(
                resolved.id,
                "https://issuer.example",
                "operator-enrolled-subject",
            )
            .await
            .unwrap()
    );
    assert_eq!(
        repo.core_user_by_identity("https://issuer.example", "operator-enrolled-subject",)
            .await
            .unwrap()
            .id,
        resolved.id
    );
    assert!(matches!(
        repo.link_core_user_identity(
            other_issuer.id,
            "https://issuer.example",
            "operator-enrolled-subject",
        )
        .await,
        Err(UserRepositoryError::IdentityConflict)
    ));
    assert!(matches!(
        repo.link_core_user_identity(
            Uuid::new_v4(),
            "https://issuer.example",
            "missing-user-subject",
        )
        .await,
        Err(UserRepositoryError::NotFound)
    ));

    for (issuer, subject) in [
        ("", "subject"),
        ("https://issuer.example", ""),
        ("https://issuer.example", "control\ncharacter"),
    ] {
        assert!(matches!(
            repo.bootstrap_core_user_identity(issuer, subject, true, &test_signing_keys())
                .await,
            Err(UserRepositoryError::InvalidIdentity)
        ));
    }

    let pool = PgPool::connect(&url).await.unwrap();
    let identity_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM user_identities WHERE issuer=$1 AND subject=$2")
            .bind("https://issuer.example")
            .bind("subject-1")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(identity_count, 1);
    let user_count: i64 = sqlx::query_scalar("SELECT count(*) FROM users WHERE id=$1")
        .bind(ids[0])
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(user_count, 1);
    let users: i64 = sqlx::query_scalar("SELECT count(*) FROM users")
        .fetch_one(&pool)
        .await
        .unwrap();
    let keys: i64 = sqlx::query_scalar("SELECT count(*) FROM user_signing_keys")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(users, 2, "concurrent losers must leave no candidate users");
    assert_eq!(keys, users, "each new user must have exactly one key pair");

    for statement in [
        "UPDATE user_identities SET subject=subject WHERE issuer='https://issuer.example' AND subject='subject-1'",
        "DELETE FROM user_identities WHERE issuer='https://issuer.example' AND subject='subject-1'",
        "TRUNCATE user_identities",
    ] {
        let error = sqlx::query(statement).execute(&pool).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("user identity mappings are append-only"),
            "unexpected immutability error: {error}"
        );
    }
    assert_eq!(
        repo.core_user_by_identity("https://issuer.example", "subject-1")
            .await
            .unwrap()
            .id,
        ids[0]
    );

    drop(pool);
    drop(repo);
    database.cleanup(&admin).await;
}

fn test_signing_keys() -> tackt_postgres::UserSigningKeys {
    fn random_bytes() -> [u8; 32] {
        let mut bytes = [0; 32];
        bytes[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        bytes[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        bytes
    }
    tackt_postgres::UserSigningKeys {
        addresses: None,
        ethereum_private_key: random_bytes(),
        solana_private_key: random_bytes(),
    }
}

#[tokio::test]
async fn failed_key_storage_rolls_back_both_signup_paths() {
    let Some((url, admin, database)) = database().await else {
        return;
    };
    let repo = PgRepository::initialise(&url, 5).await.unwrap();
    let pool = PgPool::connect(&url).await.unwrap();
    // Force storage failure after the user insert, without changing the production API.
    sqlx::query("ALTER TABLE user_signing_keys ADD CONSTRAINT reject_test_keys CHECK (false)")
        .execute(&pool)
        .await
        .unwrap();
    let input = CreateCoreUser {
        external_id: "rollback-test".into(),
        metadata: json!({}),
    };
    assert!(
        repo.create_core_user(input.clone(), &test_signing_keys())
            .await
            .is_err()
    );
    assert!(
        repo.bootstrap_core_user_identity(
            "https://issuer.example",
            "rollback",
            true,
            &test_signing_keys()
        )
        .await
        .is_err()
    );
    for table in ["users", "user_signing_keys", "user_identities"] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0, "failed provisioning must leave {table} empty");
    }
    sqlx::query("ALTER TABLE user_signing_keys DROP CONSTRAINT reject_test_keys")
        .execute(&pool)
        .await
        .unwrap();
    let (operator, created) = repo
        .create_core_user(input, &test_signing_keys())
        .await
        .unwrap();
    assert!(created);
    let (login, created) = repo
        .bootstrap_core_user_identity(
            "https://issuer.example",
            "rollback",
            true,
            &test_signing_keys(),
        )
        .await
        .unwrap();
    assert!(created);
    assert!(repo.user_signing_keys(operator.id).await.unwrap().is_some());
    assert!(repo.user_signing_keys(login.id).await.unwrap().is_some());
    drop(pool);
    drop(repo);
    database.cleanup(&admin).await;
}

#[tokio::test]
async fn wallet_address_index_is_unique_and_lookup_returns_only_the_owner() {
    use tackt_postgres::{UserWalletAddresses, WalletAddress};
    let Some((url, admin, database)) = database().await else {
        return;
    };
    let repo = PgRepository::initialise(&url, 5).await.unwrap();
    let addresses = UserWalletAddresses {
        ethereum: "0x1111111111111111111111111111111111111111".into(),
        solana: "11111111111111111111111111111111".into(),
    };
    let mut keys = test_signing_keys();
    keys.addresses = Some(addresses.clone());
    let (user, _) = repo
        .create_core_user(
            CreateCoreUser {
                external_id: "indexed-wallet".into(),
                metadata: json!({}),
            },
            &keys,
        )
        .await
        .unwrap();
    assert_eq!(
        repo.user_wallet_addresses(user.id).await.unwrap(),
        Some(addresses.clone())
    );
    let (profile_user, profile_addresses) =
        repo.core_user_with_wallet_addresses(user.id).await.unwrap();
    assert_eq!(profile_user, user);
    assert_eq!(profile_addresses, Some(addresses.clone()));
    assert!(matches!(
        repo.core_user_with_wallet_addresses(Uuid::new_v4()).await,
        Err(UserRepositoryError::NotFound)
    ));
    for address in [
        WalletAddress::Ethereum(&addresses.ethereum),
        WalletAddress::Solana(&addresses.solana),
    ] {
        assert_eq!(
            repo.user_id_by_wallet_address(address).await.unwrap(),
            Some(user.id)
        );
    }
    assert!(
        repo.create_core_user(
            CreateCoreUser {
                external_id: "duplicate-wallet".into(),
                metadata: json!({})
            },
            &keys
        )
        .await
        .is_err()
    );
    assert_eq!(repo.list_core_users(100, None).await.unwrap().len(), 1);
    let pool = PgPool::connect(&url).await.unwrap();
    for query in [
        "UPDATE user_signing_keys SET ethereum_address=NULL WHERE user_id=$1",
        "UPDATE user_signing_keys SET ethereum_address='invalid' WHERE user_id=$1",
        "UPDATE user_signing_keys SET solana_address='invalid' WHERE user_id=$1",
    ] {
        assert!(
            sqlx::query(query)
                .bind(user.id)
                .execute(&pool)
                .await
                .is_err()
        );
    }
    sqlx::query("DELETE FROM user_signing_keys WHERE user_id=$1")
        .bind(user.id)
        .execute(&pool)
        .await
        .unwrap();
    let (profile_user, profile_addresses) =
        repo.core_user_with_wallet_addresses(user.id).await.unwrap();
    assert_eq!(profile_user, user);
    assert_eq!(profile_addresses, None);
    drop(pool);
    drop(repo);
    database.cleanup(&admin).await;
}

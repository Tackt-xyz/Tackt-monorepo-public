#![cfg(feature = "test-support")]

use sqlx::PgPool;
use tackt_postgres::{PgRepository, test_support::TestDatabase};

#[tokio::test]
async fn beta_access_defaults_existing_and_new_users_to_false_and_survives_initialisation() {
    let Some((url, database)) = TestDatabase::create().await else {
        return;
    };
    drop(PgRepository::initialise(&url, 2).await.unwrap());
    let pool = PgPool::connect(&url).await.unwrap();
    sqlx::migrate!().undo(&pool, 6).await.unwrap();
    let old_id: uuid::Uuid = sqlx::query_scalar(
        "INSERT INTO users(external_id) VALUES('before-beta-migration') RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let repo = PgRepository::initialise(&url, 2).await.unwrap();
    assert!(!repo.core_user(old_id).await.unwrap().beta_access);
    assert!(repo.user_signing_keys(old_id).await.unwrap().is_none());
    let (new_user, _) = repo
        .create_core_user(
            tackt_primitives::CreateCoreUser {
                external_id: "after-beta-migration".into(),
                metadata: serde_json::json!({}),
            },
            &test_signing_keys(),
        )
        .await
        .unwrap();
    assert!(!new_user.beta_access);
    repo.grant_beta_access(old_id).await.unwrap();
    drop(repo);
    let repo = PgRepository::initialise(&url, 2).await.unwrap();
    assert!(repo.core_user(old_id).await.unwrap().beta_access);
    assert!(!repo.core_user(new_user.id).await.unwrap().beta_access);
    assert!(matches!(
        repo.grant_beta_access(uuid::Uuid::new_v4()).await,
        Err(tackt_postgres::UserRepositoryError::NotFound)
    ));
    drop(repo);
    pool.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn baseline_initialises_concurrently_and_reapplies_after_each_rollback() {
    let Some((url, database)) = TestDatabase::create().await else {
        return;
    };
    let (first, second) = tokio::join!(
        PgRepository::initialise(&url, 2),
        PgRepository::initialise(&url, 2),
    );
    drop(first.expect("first concurrent initialisation"));
    drop(second.expect("second concurrent initialisation"));

    let pool = PgPool::connect(&url).await.unwrap();
    let migrator = sqlx::migrate!();
    let versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(versions, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);

    // Exercise every feature boundary, including retained roles and pgcrypto on a full reset.
    for target in (0..11).rev() {
        migrator.undo(&pool, target).await.unwrap();
        let applied: i64 = sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(applied, target);
        if target == 0 {
            let remaining: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_tables WHERE schemaname IN ('public', 'ledger') \
                 AND tablename <> '_sqlx_migrations'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(remaining, 0, "rollback must remove all feature tables");
        }
        drop(PgRepository::initialise(&url, 2).await.unwrap());
    }

    pool.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn observation_rollback_preserves_committed_chain_state() {
    let Some((url, database)) = TestDatabase::create().await else {
        return;
    };
    drop(PgRepository::initialise(&url, 2).await.unwrap());
    let pool = PgPool::connect(&url).await.unwrap();
    sqlx::query(
        "INSERT INTO ledger.chain_state \
         (chain_id, block_number, block_hash, finalized_number, finalized_hash, sequence) \
         VALUES (1, 10, 'block', 9, 'finalized', 1)",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Run in a transaction, just as SQLx does, so a rejected rollback stays atomic.
    let mut transaction = pool.begin().await.unwrap();
    let error = sqlx::raw_sql(include_str!(
        "../migrations/0003_ledger_observations.down.sql"
    ))
    .execute(&mut *transaction)
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("observation processing has started")
    );
    transaction.rollback().await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM ledger.chain_state")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);

    pool.close().await;
    database.cleanup().await;
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

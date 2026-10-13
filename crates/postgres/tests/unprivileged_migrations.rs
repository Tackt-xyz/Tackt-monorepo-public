//! Fresh setup and rollback must work without predefined roles or superuser access.
#![cfg(feature = "test-support")]

use sqlx::PgPool;
use tackt_postgres::{PgRepository, test_support::TestDatabase};

#[tokio::test]
async fn database_owner_can_migrate_rollback_and_reapply_without_cluster_privileges() {
    let Some((url, database)) = TestDatabase::create_unprivileged().await else {
        return;
    };
    let owner = PgPool::connect(&url).await.unwrap();
    let elevated: bool = sqlx::query_scalar(
        "SELECT rolsuper OR rolcreaterole OR rolcreatedb FROM pg_roles WHERE rolname=current_user",
    )
    .fetch_one(&owner)
    .await
    .unwrap();
    assert!(
        !elevated,
        "migration connection must use the unprivileged owner"
    );
    let memberships: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_auth_members WHERE member=(SELECT oid FROM pg_roles WHERE rolname=current_user)",
    )
    .fetch_one(&owner)
    .await
    .unwrap();
    assert_eq!(memberships, 0, "test owner must not inherit another role");

    // This covers the functions, triggers and trusted extension as well as Ledger.
    PgRepository::initialise(&url, 2).await.unwrap();
    PgRepository::initialise(&url, 2).await.unwrap();
    let foreign_owners: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='ledger' AND pg_get_userbyid(c.relowner)<>current_user",
    )
    .fetch_one(&owner)
    .await
    .unwrap();
    assert_eq!(
        foreign_owners, 0,
        "Ledger objects must retain their creator as owner"
    );
    let schema_owner: String = sqlx::query_scalar(
        "SELECT pg_get_userbyid(nspowner)::text FROM pg_namespace WHERE nspname='ledger'",
    )
    .fetch_one(&owner)
    .await
    .unwrap();
    let current_user: String = sqlx::query_scalar("SELECT current_user::text")
        .fetch_one(&owner)
        .await
        .unwrap();
    assert_eq!(schema_owner, current_user);

    sqlx::migrate!().undo(&owner, 0).await.unwrap();
    let ledger_exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT FROM pg_namespace WHERE nspname='ledger')")
            .fetch_one(&owner)
            .await
            .unwrap();
    assert!(!ledger_exists);
    PgRepository::initialise(&url, 2).await.unwrap();

    owner.close().await;
    database.cleanup().await;
}

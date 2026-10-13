#![cfg(feature = "test-support")]

use tackt_postgres::{
    LedgerActivityCursor, PgRepository, UserLedgerActivityRead,
    test_support::{TestDatabase, account_page_fixture},
};
use tackt_primitives::UserStatus;
use uuid::Uuid;

#[tokio::test]
async fn activity_is_user_scoped_paginated_without_split_transactions_and_lifecycle_checked() {
    let (url, database) = TestDatabase::create()
        .await
        .expect("Compose DATABASE_URL required");
    let repo = PgRepository::initialise(&url, 2).await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let user: Uuid =
        sqlx::query_scalar("INSERT INTO users(external_id) VALUES('activity-alice') RETURNING id")
            .fetch_one(&pool)
            .await
            .unwrap();
    let other: Uuid =
        sqlx::query_scalar("INSERT INTO users(external_id) VALUES('activity-bob') RETURNING id")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        matches!(repo.user_ledger_activity(user, None, 10).await.unwrap(), UserLedgerActivityRead::Available(items) if items.is_empty())
    );
    assert!(matches!(
        repo.user_ledger_activity(Uuid::new_v4(), None, 10)
            .await
            .unwrap(),
        UserLedgerActivityRead::UserNotFound
    ));
    let ids = account_page_fixture(&repo, user, other).await;
    let mut seen = Vec::new();
    let mut cursor = None;
    loop {
        let UserLedgerActivityRead::Available(items) =
            repo.user_ledger_activity(user, cursor, 1).await.unwrap()
        else {
            panic!("open user")
        };
        if items.is_empty() {
            break;
        }
        assert_eq!(items.len(), 1);
        let item = &items[0];
        seen.push(item.id);
        assert!(!item.postings.is_empty());
        if item.command_kind == "draw_credit" {
            // Neither the foreign user's nor the system's posting is returned.
            assert_eq!(item.postings.len(), 2);
        }
        cursor = Some(LedgerActivityCursor {
            recorded_at: item.recorded_at,
            transaction_id: item.id,
        });
    }
    assert_eq!(seen, ids);
    assert!(repo.user_ledger_activity(user, None, 0).await.is_err());
    assert!(repo.user_ledger_activity(user, None, 102).await.is_err());
    repo.update_user_status(user, UserStatus::Closed)
        .await
        .unwrap();
    assert!(matches!(
        repo.user_ledger_activity(user, None, 10).await.unwrap(),
        UserLedgerActivityRead::UserClosed
    ));
    pool.close().await;
    drop(repo);
    database.cleanup().await;
}

//! Ledger accounting policy lives in the application; database objects use their creator as owner.
#![cfg(feature = "test-support")]

use sqlx::{PgPool, Row};
use tackt_postgres::{PgRepository, UserLedgerBalanceRead, test_support::TestDatabase};
use tackt_primitives::ledger::UserLedgerBalanceSnapshot;
use url::Url;

fn open_snapshot(read: UserLedgerBalanceRead) -> UserLedgerBalanceSnapshot {
    let UserLedgerBalanceRead::Available(snapshot) = read else {
        panic!("expected an open user's balance snapshot, got {read:?}");
    };
    snapshot
}

#[tokio::test]
async fn current_snapshot_preserves_both_sides_and_only_committed_user_balances() {
    use chrono::{TimeZone, Utc};
    use tackt_postgres::{LedgerAccount, LedgerAccountDefinition};
    use tackt_primitives::ledger::{BalanceBucket, LedgerAsset};
    use uuid::Uuid;

    let Some((url, database)) = TestDatabase::create().await else {
        return;
    };
    let repo = PgRepository::initialise(&url, 2).await.unwrap();
    let user = Uuid::new_v4();
    let other_user = Uuid::new_v4();
    assert_eq!(
        repo.user_ledger_balance_snapshot(user).await.unwrap(),
        UserLedgerBalanceRead::UserNotFound
    );
    let admin = PgPool::connect(&url).await.unwrap();
    for id in [user, other_user] {
        sqlx::query("INSERT INTO users(id,external_id) VALUES($1,$2)")
            .bind(id)
            .bind(id.to_string())
            .execute(&admin)
            .await
            .unwrap();
    }
    let empty = open_snapshot(repo.user_ledger_balance_snapshot(user).await.unwrap());
    assert!(empty.balances.is_empty());
    let updated_at = Utc.timestamp_millis_opt(1_700_000_000_123).unwrap();
    let account = |owner, bucket: BalanceBucket, debit, credit| LedgerAccount {
        id: Uuid::new_v4(),
        definition: LedgerAccountDefinition {
            code: format!("{owner:?}:{}:USDC", bucket.as_ref()),
            user_id: owner,
            bucket: bucket.as_ref().to_owned(),
            asset: LedgerAsset::Usdc,
            allow_negative: false,
        },
        debit,
        credit,
    };
    let mut write = repo.begin_ledger_write().await.unwrap();
    write
        .store_accounts(
            &[
                account(
                    Some(user),
                    BalanceBucket::Available,
                    u128::MAX,
                    u128::MAX - 1,
                ),
                account(Some(user), BalanceBucket::PrincipalLiability, 20, 100),
                account(Some(user), BalanceBucket::Reserved, 50, 50),
                account(Some(other_user), BalanceBucket::Available, 900, 0),
                account(None, BalanceBucket::Available, 800, 0),
            ],
            updated_at,
        )
        .await
        .unwrap();
    assert!(
        open_snapshot(repo.user_ledger_balance_snapshot(user).await.unwrap())
            .balances
            .is_empty()
    );
    write.commit().await.unwrap();

    let snapshot = open_snapshot(repo.user_ledger_balance_snapshot(user).await.unwrap());
    assert!(snapshot.as_of >= empty.as_of);
    assert_eq!(snapshot.balances.len(), 3);
    let available = &snapshot.balances[0];
    assert_eq!(available.bucket, BalanceBucket::Available);
    assert_eq!(available.debit_quantity, u128::MAX);
    assert_eq!(available.credit_quantity, u128::MAX - 1);
    let principal = &snapshot.balances[1];
    assert_eq!(principal.bucket, BalanceBucket::PrincipalLiability);
    assert_eq!(
        (principal.debit_quantity, principal.credit_quantity),
        (20, 100)
    );
    let reserved = &snapshot.balances[2];
    assert_eq!(reserved.bucket, BalanceBucket::Reserved);
    assert_eq!(
        (reserved.debit_quantity, reserved.credit_quantity),
        (50, 50)
    );
    assert!(
        snapshot
            .balances
            .iter()
            .all(|balance| balance.updated_at == updated_at)
    );

    // Reproduce the old interleaving without timing-dependent sleeps: a reader observes an
    // open user, another connection commits closure, then the protected read must reject it.
    use tackt_primitives::UserStatus;
    assert_eq!(
        repo.core_user(user).await.unwrap().status,
        UserStatus::Pending
    );
    let writer = PgRepository::connect(&url, 1).await.unwrap();
    writer
        .update_user_status(user, UserStatus::Closed)
        .await
        .unwrap();
    assert_eq!(
        repo.user_ledger_balance_snapshot(user).await.unwrap(),
        UserLedgerBalanceRead::UserClosed
    );
    // The other user's balances are still readable, not confused with the closed account.
    assert_eq!(
        open_snapshot(repo.user_ledger_balance_snapshot(other_user).await.unwrap())
            .balances
            .len(),
        1
    );
    admin.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn every_registered_instrument_keeps_its_own_persisted_balance() {
    use chrono::Utc;
    use tackt_postgres::{LedgerAccount, LedgerAccountDefinition};
    use tackt_primitives::ledger::{BalanceBucket, LedgerAsset};
    use uuid::Uuid;

    let Some((url, database)) = TestDatabase::create().await else {
        return;
    };
    let repository = PgRepository::initialise(&url, 2).await.unwrap();
    let user = Uuid::new_v4();
    let accounts: Vec<_> = LedgerAsset::ALL
        .into_iter()
        .enumerate()
        .map(|(index, asset)| LedgerAccount {
            id: Uuid::new_v4(),
            definition: LedgerAccountDefinition {
                code: format!("onboarding:{}", asset.code()),
                user_id: Some(user),
                bucket: BalanceBucket::Available.as_ref().to_owned(),
                asset,
                allow_negative: false,
            },
            debit: index as u128 + 1,
            credit: 0,
        })
        .collect();
    let mut write = repository.begin_ledger_write().await.unwrap();
    write.store_accounts(&accounts, Utc::now()).await.unwrap();
    write.commit().await.unwrap();
    let snapshot = repository.ledger_balance_snapshot().await.unwrap();
    let balances = &snapshot.accounts[&user];
    assert_eq!(balances.len(), accounts.len());
    for expected in accounts {
        let balance = balances
            .iter()
            .find(|balance| balance.asset == expected.definition.asset)
            .expect("asset survives persistence");
        assert_eq!(balance.bucket, BalanceBucket::Available);
        assert_eq!(balance.quantity, i128::try_from(expected.debit).unwrap());
    }
    database.cleanup().await;
}

#[tokio::test]
async fn ledger_schema_contains_storage_keys_without_accounting_policy() {
    let Some((url, database)) = TestDatabase::create().await else {
        return;
    };
    PgRepository::initialise(&url, 2).await.unwrap();
    PgRepository::initialise(&url, 2).await.unwrap();
    let admin = PgPool::connect(&url).await.unwrap();
    let rules: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_constraint WHERE (connamespace='ledger'::regnamespace OR conrelid='observations'::regclass) AND contype IN ('c','f','u')"
    ).fetch_one(&admin).await.unwrap();
    assert_eq!(rules, 0, "Ledger business constraints must not return");
    let triggers: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid WHERE (c.relnamespace='ledger'::regnamespace OR c.oid='observations'::regclass) AND NOT t.tgisinternal"
    ).fetch_one(&admin).await.unwrap();
    assert_eq!(triggers, 0);
    let enums: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_type WHERE typnamespace='ledger'::regnamespace AND typtype='e'",
    )
    .fetch_one(&admin)
    .await
    .unwrap();
    assert_eq!(enums, 0);
    let catalog: bool = sqlx::query_scalar("SELECT to_regclass('ledger.assets') IS NOT NULL")
        .fetch_one(&admin)
        .await
        .unwrap();
    assert!(
        !catalog,
        "supported assets come only from the embedded registry"
    );

    let mut tx = admin.begin().await.unwrap();
    // These values are deliberately invalid accounting. The authorized storage
    // writer can persist them; only Ledger's command API is allowed to decide.
    sqlx::query("INSERT INTO ledger.accounts(code,bucket,asset) VALUES('not-an-account','not-a-bucket','not-an-asset')")
        .execute(&mut *tx).await.unwrap();
    for _ in 0..2 {
        let row = sqlx::query("INSERT INTO ledger.transactions(command_source,chain_id,command_kind,command_hash,idempotency_key,effective_at,audit,external_reference) VALUES('invalid',0,'invalid','invalid','same-key',now(),'{}','same-reference') RETURNING id")
            .fetch_one(&mut *tx).await.unwrap();
        sqlx::query("INSERT INTO ledger.postings(transaction_id,account_id,asset,side,quantity) VALUES($1,gen_random_uuid(),'unknown','invalid',0)")
            .bind(row.get::<uuid::Uuid,_>("id")).execute(&mut *tx).await.unwrap();
    }
    tx.rollback().await.unwrap();

    admin.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn ledger_snapshot_reads_all_users_from_committed_balance_projections() {
    use chrono::Utc;
    use tackt_postgres::{LedgerAccount, LedgerAccountDefinition};
    use tackt_primitives::ledger::{BalanceBucket, LedgerAsset};
    use uuid::Uuid;

    let Some((url, database)) = TestDatabase::create().await else {
        return;
    };
    let repository = PgRepository::initialise(&url, 2).await.unwrap();
    assert!(
        repository
            .ledger_balance_snapshot()
            .await
            .unwrap()
            .accounts
            .is_empty()
    );
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    let make_account = |user_id, bucket: BalanceBucket, asset, debit, credit| LedgerAccount {
        id: Uuid::new_v4(),
        definition: LedgerAccountDefinition {
            code: Uuid::new_v4().to_string(),
            user_id,
            bucket: bucket.as_ref().to_owned(),
            asset,
            allow_negative: bucket == BalanceBucket::PrincipalLiability,
        },
        debit,
        credit,
    };
    let mut accounts = [
        make_account(
            Some(first),
            BalanceBucket::Pending,
            LedgerAsset::SolSolana,
            10,
            0,
        ),
        make_account(
            Some(first),
            BalanceBucket::Available,
            LedgerAsset::SolSolana,
            0,
            0,
        ),
        make_account(
            Some(first),
            BalanceBucket::PrincipalLiability,
            LedgerAsset::Usdc,
            0,
            800,
        ),
        make_account(
            Some(second),
            BalanceBucket::WithdrawalReserved,
            LedgerAsset::EthBase,
            2,
            0,
        ),
        make_account(None, BalanceBucket::Available, LedgerAsset::Usdc, 800, 0),
    ];
    let mut write = repository.begin_ledger_write().await.unwrap();
    write.store_accounts(&accounts, Utc::now()).await.unwrap();
    write.commit().await.unwrap();
    let mut read_url = Url::parse(&url).unwrap();
    read_url
        .query_pairs_mut()
        .append_pair("options", "-c role=pg_read_all_data");
    let reader = PgRepository::connect(read_url.as_str(), 1).await.unwrap();
    let initial = reader.ledger_balance_snapshot().await.unwrap();
    assert_eq!(initial.accounts.len(), 2);
    assert_eq!(initial.accounts[&first].len(), 3);
    assert_eq!(initial.accounts[&second].len(), 1);
    assert!(
        initial
            .accounts
            .values()
            .flatten()
            .all(|balance| balance.as_of == initial.as_of)
    );
    let quantity = |snapshot: &tackt_postgres::LedgerBalanceSnapshot, bucket| {
        snapshot.accounts[&first]
            .iter()
            .find(|balance| balance.bucket == bucket)
            .unwrap()
            .quantity
    };
    assert_eq!(quantity(&initial, BalanceBucket::PrincipalLiability), -800);
    assert_eq!(quantity(&initial, BalanceBucket::Pending), 10);

    // Uncommitted Ledger changes stay invisible, and readers do not take its writer lock.
    accounts[0].debit = 0;
    accounts[1].debit = 10;
    let mut write = repository.begin_ledger_write().await.unwrap();
    write.store_accounts(&accounts, Utc::now()).await.unwrap();
    let during = reader.ledger_balance_snapshot().await.unwrap();
    assert_eq!(quantity(&during, BalanceBucket::Pending), 10);
    assert_eq!(quantity(&during, BalanceBucket::Available), 0);
    write.commit().await.unwrap();
    let after = reader.ledger_balance_snapshot().await.unwrap();
    assert_eq!(quantity(&after, BalanceBucket::Pending), 0);
    assert_eq!(quantity(&after, BalanceBucket::Available), 10);
    assert_eq!(quantity(&after, BalanceBucket::PrincipalLiability), -800);
    database.cleanup().await;
}

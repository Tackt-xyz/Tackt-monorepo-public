#![cfg(feature = "test-support")]

use chrono::{Duration, Utc};
use sqlx::PgPool;
use tackt_postgres::{
    PgRepository, UserRiskHealthRead,
    test_support::{TestDatabase, risk_health_sample},
};
use tackt_primitives::{UserStatus, risk::RiskClassification};
use uuid::Uuid;

#[tokio::test]
async fn history_is_exact_ordered_bounded_and_scoped_to_open_users() {
    let Some((url, database)) = TestDatabase::create().await else {
        return;
    };
    let repo = PgRepository::initialise(&url, 2).await.unwrap();
    let pool = PgPool::connect(&url).await.unwrap();
    let alice: Uuid =
        sqlx::query_scalar("INSERT INTO users(external_id) VALUES('history-alice') RETURNING id")
            .fetch_one(&pool)
            .await
            .unwrap();
    let bob: Uuid =
        sqlx::query_scalar("INSERT INTO users(external_id) VALUES('history-bob') RETURNING id")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        matches!(repo.latest_user_risk_health(alice).await.unwrap(), UserRiskHealthRead::Available(s) if s.is_empty())
    );
    assert!(matches!(
        repo.latest_user_risk_health(Uuid::new_v4()).await.unwrap(),
        UserRiskHealthRead::UserNotFound
    ));
    let at = chrono::DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
    let first = risk_health_sample(alice, at);
    let mut second = risk_health_sample(alice, at + Duration::minutes(1));
    second.classification = RiskClassification::DataIncomplete;
    second.health = None;
    // Insert out of order; latest is capture time, not arrival time.
    repo.record_risk_health(&second).await.unwrap();
    repo.record_risk_health(&first).await.unwrap();
    assert!(repo.record_risk_health(&first).await.is_err());
    repo.record_risk_health(&risk_health_sample(bob, at + Duration::minutes(2)))
        .await
        .unwrap();

    let UserRiskHealthRead::Available(samples) = repo
        .user_risk_health_history(alice, at, at + Duration::minutes(2), 10)
        .await
        .unwrap()
    else {
        panic!("history");
    };
    assert_eq!(samples, vec![second.clone(), first.clone()]);
    let UserRiskHealthRead::Available(latest) = repo.latest_user_risk_health(alice).await.unwrap()
    else {
        panic!("latest");
    };
    assert_eq!(latest, vec![second]);
    let UserRiskHealthRead::Available(samples) = repo
        .user_risk_health_history(alice, at, at + Duration::minutes(1), 1)
        .await
        .unwrap()
    else {
        panic!("bounded history");
    };
    assert_eq!(samples, vec![first]);
    assert!(
        repo.user_risk_health_history(alice, at, at + Duration::days(32), 10)
            .await
            .is_err()
    );
    assert!(
        repo.user_risk_health_history(alice, at, at + Duration::minutes(1), 2002)
            .await
            .is_err()
    );
    repo.update_user_status(alice, UserStatus::Closed)
        .await
        .unwrap();
    assert!(matches!(
        repo.latest_user_risk_health(alice).await.unwrap(),
        UserRiskHealthRead::UserClosed
    ));
    drop(repo);
    pool.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn numeric_history_preserves_finite_incomplete_and_unbounded_samples() {
    let Some((url, database)) = TestDatabase::create().await else {
        return;
    };
    let repo = PgRepository::initialise(&url, 2).await.unwrap();
    let pool = PgPool::connect(&url).await.unwrap();
    let user: Uuid =
        sqlx::query_scalar("INSERT INTO users(external_id) VALUES('numeric-history') RETURNING id")
            .fetch_one(&pool)
            .await
            .unwrap();
    let at = chrono::DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
    let cases = [
        (RiskClassification::Healthy, Some(i128::MAX - 1)),
        (RiskClassification::Liquidatable, Some(-333334)),
        (RiskClassification::Healthy, None),
        (RiskClassification::Liquidatable, None),
        (RiskClassification::DataIncomplete, None),
    ];
    for (index, &(classification, health)) in cases.iter().enumerate() {
        let mut sample = risk_health_sample(user, at + Duration::seconds(index as i64));
        sample.classification = classification;
        sample.health = health;
        repo.record_risk_health(&sample).await.unwrap();
    }
    let UserRiskHealthRead::Available(samples) = repo
        .user_risk_health_history(user, at, at + Duration::minutes(1), 10)
        .await
        .unwrap()
    else {
        panic!("history");
    };
    assert_eq!(samples.len(), cases.len());
    for (sample, (classification, expected)) in samples.iter().rev().zip(cases) {
        assert_eq!(sample.health, expected);
        assert_eq!(sample.classification, classification);
    }
    let payload_columns: i64 = sqlx::query_scalar("SELECT count(*) FROM information_schema.columns WHERE table_name='risk_health_samples' AND column_name='payload'")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(payload_columns, 0);
    drop(repo);
    pool.close().await;
    database.cleanup().await;
}

use chrono::{Duration, Utc};
use std::collections::HashMap;
use tackt_ledger::{Ledger, LedgerError, LedgerRepository};
use tackt_postgres::{PgRepository, test_support::TestDatabase};
use tackt_primitives::ledger::*;
use uuid::Uuid;

fn header(key: &str, effective_at: chrono::DateTime<Utc>) -> CommandHeader {
    header_on_chain(key, effective_at, 1)
}

fn header_on_chain(key: &str, effective_at: chrono::DateTime<Utc>, chain_id: u64) -> CommandHeader {
    CommandHeader {
        chain_id,
        idempotency_key: key.into(),
        external_reference: Some(format!("external:{key}")),
        effective_at,
        audit: AuditMetadata {
            actor: "integration-test".into(),
            reason: "prove ledger invariant".into(),
            risk_snapshot_id: Some("risk-1".into()),
            authorization: None,
            attributes: Default::default(),
        },
    }
}

/// Credit draws and both reservation kinds are Core-authorized: the ledger refuses them
/// without Core's evidence, whichever ingress submitted them.
fn authorized_header(key: &str, effective_at: chrono::DateTime<Utc>) -> CommandHeader {
    let mut header = header(key, effective_at);
    header.audit.authorization = Some(
        CoreAuthorization::new(
            format!("action-{key}"),
            key.into(),
            format!("recommendation-{key}"),
        )
        .expect("well-formed authorization evidence"),
    );
    header
}

fn custody_exit_header(
    action_id: Uuid,
    key: &str,
    effective_at: chrono::DateTime<Utc>,
) -> CommandHeader {
    let mut header = header(key, effective_at);
    header.audit.authorization = Some(
        CoreAuthorization::new(
            action_id.to_string(),
            key.into(),
            format!("recommendation-{key}"),
        )
        .expect("well-formed custody-exit authorization"),
    );
    header
}

/// Seed independent accounting tests through the existing audited adjustment capability.
fn credit(
    header: CommandHeader,
    user_id: Uuid,
    asset: LedgerAsset,
    quantity: u128,
) -> LedgerCommand {
    LedgerCommand::ControlledAdjustment {
        header,
        user_id,
        asset,
        quantity,
        bucket: BalanceBucket::Available,
        direction: GainLoss::Gain,
        approval_reference: "test-fixture-credit".into(),
    }
}

fn balance(values: &[LedgerBalance], asset: LedgerAsset, bucket: BalanceBucket) -> i128 {
    values
        .iter()
        .find(|v| v.asset == asset && v.bucket == bucket)
        .map_or(0, |v| v.quantity)
}

fn assert_conflict(result: Result<CommandResult, LedgerError>, expected: &str) {
    match result {
        Err(LedgerError::Conflict(message)) => assert!(
            message.contains(expected),
            "expected conflict containing {expected:?}, got {message:?}"
        ),
        other => panic!("expected conflict containing {expected:?}, got {other:?}"),
    }
}

#[tokio::test]
async fn invalid_and_overflowing_quantities_leave_no_partial_journal() {
    let Some((url, database)) = TestDatabase::create().await else {
        return;
    };
    let repo = PgRepository::initialise(&url, 2).await.unwrap();
    let _storage = repo.clone();
    let repo = Ledger::new(repo);
    let user_id = Uuid::new_v4();
    let now = Utc::now();
    let command = |key, quantity| credit(header(key, now), user_id, LedgerAsset::Eth, quantity);
    assert!(matches!(
        repo.execute(command("zero", 0)).await,
        Err(LedgerError::InvalidQuantity)
    ));
    assert!(repo.export().await.unwrap().transactions.is_empty());
    let full = command("full", u128::MAX);
    let first = repo.execute(full.clone()).await.unwrap();
    assert!(matches!(
        repo.execute(command("overflow", 1)).await,
        Err(LedgerError::InvalidQuantity)
    ));
    let replay = repo.execute(full).await.unwrap();
    assert!(replay.duplicate);
    assert_eq!(replay.transaction_id, first.transaction_id);
    let export = repo.export().await.unwrap();
    assert_eq!(export.transactions.len(), 1);
    assert_eq!(export.transactions[0].postings.len(), 2);
    assert!(
        export.transactions[0]
            .postings
            .iter()
            .all(|posting| posting.quantity == u128::MAX)
    );
    database.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ledger_database_invariants_and_complete_reconstruction() {
    let Some((url, database)) = TestDatabase::create().await else {
        return;
    };
    let migrator = PgRepository::initialise(&url, 2)
        .await
        .expect("clean migrations succeed");
    PgRepository::initialise(&url, 2)
        .await
        .expect("repeated initialization is idempotent");
    drop(migrator);
    let repo = PgRepository::connect(&url, 20)
        .await
        .expect("database owner can connect");
    let _storage = repo.clone();
    let repo = Ledger::new(repo);
    let user = Uuid::new_v4();
    let now = Utc::now();

    let mut whitespace_header = header("   ", now);
    whitespace_header.audit.actor = "\t".into();
    assert_conflict(
        repo.execute(credit(whitespace_header, user, LedgerAsset::Eth, 1))
            .await,
        "idempotency key",
    );
    // Full u128-scale precision survives JSON, Rust and numeric(39,0) unchanged.
    let precise = 100_000_000_000_000_000_000_000_000_000u128;
    let credited = credit(
        header("credit", now - Duration::hours(1)),
        user,
        LedgerAsset::Eth,
        precise,
    );
    let invalid_chain = credit(
        header_on_chain("invalid-chain", now, 0),
        user,
        LedgerAsset::Eth,
        1,
    );
    assert_conflict(repo.execute(invalid_chain).await, "positive chain id");
    let first = repo.execute(credited.clone()).await.unwrap();
    let duplicate = repo.execute(credited).await.unwrap();
    assert_eq!(first.transaction_id, duplicate.transaction_id);
    assert!(duplicate.duplicate);
    let changed = credit(
        header("credit", now - Duration::hours(1)),
        user,
        LedgerAsset::Eth,
        1,
    );
    assert!(matches!(
        repo.execute(changed).await,
        Err(LedgerError::IdempotencyConflict)
    ));
    let chain_scoped_customer = Uuid::new_v4();
    let mut chain_one = header_on_chain("chain-one-event", now, 1);
    chain_one.external_reference = Some("shared-event-reference".into());
    let mut chain_two = header_on_chain("chain-two-event", now, 137);
    chain_two.external_reference = Some("shared-event-reference".into());
    for header in [chain_one, chain_two] {
        repo.execute(credit(header, chain_scoped_customer, LedgerAsset::Usdc, 1))
            .await
            .unwrap();
    }
    let mut duplicate_chain_reference = header_on_chain("chain-one-duplicate", now, 1);
    duplicate_chain_reference.external_reference = Some("shared-event-reference".into());
    assert!(
        repo.execute(credit(
            duplicate_chain_reference,
            chain_scoped_customer,
            LedgerAsset::Usdc,
            1
        ))
        .await
        .is_err(),
        "external references are unique within source and chain"
    );
    let concurrent_customer = Uuid::new_v4();
    let concurrent_command = credit(
        header("concurrent-idempotency", now),
        concurrent_customer,
        LedgerAsset::Btc,
        1,
    );
    let (left, right) = tokio::join!(
        repo.execute(concurrent_command.clone()),
        repo.execute(concurrent_command)
    );
    let left = left.unwrap();
    let right = right.unwrap();
    assert_eq!(left.transaction_id, right.transaction_id);
    assert_ne!(left.duplicate, right.duplicate);
    let eth = repo
        .balances(user, None, BalanceTimeBasis::Recorded)
        .await
        .unwrap();
    assert_eq!(
        balance(&eth, LedgerAsset::Eth, BalanceBucket::Available),
        precise as i128
    );
    let effective_history = repo
        .balances(
            user,
            Some(now - Duration::minutes(90)),
            BalanceTimeBasis::Effective,
        )
        .await
        .unwrap();
    assert_eq!(
        balance(
            &effective_history,
            LedgerAsset::Eth,
            BalanceBucket::Available
        ),
        0
    );

    let credited = repo
        .execute(credit(
            header("usdc-credit", now),
            user,
            LedgerAsset::Usdc,
            100,
        ))
        .await
        .unwrap();
    let historical = repo
        .balances(
            user,
            Some(credited.recorded_at - Duration::microseconds(1)),
            BalanceTimeBasis::Recorded,
        )
        .await
        .unwrap();
    assert_eq!(
        balance(&historical, LedgerAsset::Usdc, BalanceBucket::Available),
        0,
        "recorded-time historical balance excludes later commit"
    );

    // Two concurrent reservations cannot both consume the same available amount.
    let a = repo.clone();
    let b = repo.clone();
    let r1 = Uuid::new_v4();
    let r2 = Uuid::new_v4();
    let c1 = LedgerCommand::ReserveCreditForOrder {
        header: authorized_header("reserve-a", now),
        user_id: user,
        reservation_id: r1,
        quantity: 80,
    };
    let c2 = LedgerCommand::ReserveCreditForOrder {
        header: authorized_header("reserve-b", now),
        user_id: user,
        reservation_id: r2,
        quantity: 80,
    };
    let (x, y) = tokio::join!(a.execute(c1), b.execute(c2));
    assert_eq!(usize::from(x.is_ok()) + usize::from(y.is_ok()), 1);
    let reservation = if x.is_ok() { r1 } else { r2 };
    repo.execute(LedgerCommand::ConsumeOrderReservation {
        header: header("consume-part", now),
        user_id: user,
        reservation_id: reservation,
        quantity: 30,
    })
    .await
    .unwrap();
    repo.execute(LedgerCommand::ReleaseOrderReservation {
        header: header("release-rest", now),
        user_id: user,
        reservation_id: reservation,
        quantity: 50,
    })
    .await
    .unwrap();
    let usdc = repo
        .balances(user, None, BalanceTimeBasis::Recorded)
        .await
        .unwrap();
    assert_eq!(
        balance(&usdc, LedgerAsset::Usdc, BalanceBucket::Available),
        70
    );
    assert_eq!(
        balance(&usdc, LedgerAsset::Usdc, BalanceBucket::OrderReserved),
        0
    );
    assert_eq!(
        balance(&usdc, LedgerAsset::Usdc, BalanceBucket::HyperliquidMargin),
        30
    );
    assert!(matches!(
        repo.execute(LedgerCommand::ReleaseOrderReservation {
            header: header("release-too-much", now),
            user_id: user,
            reservation_id: reservation,
            quantity: 1,
        })
        .await,
        Err(LedgerError::InsufficientFunds)
    ));
    assert!(matches!(
        repo.execute(LedgerCommand::ReserveWithdrawal {
            header: authorized_header("overspend", now),
            user_id: user,
            reservation_id: Uuid::new_v4(),
            asset: LedgerAsset::Usdc,
            quantity: 71,
        })
        .await,
        Err(LedgerError::InsufficientFunds)
    ));

    // Withdrawal reservations support partial settlement and release.
    let withdrawal = Uuid::new_v4();
    repo.execute(LedgerCommand::ReserveWithdrawal {
        header: authorized_header("withdraw-reserve", now),
        user_id: user,
        reservation_id: withdrawal,
        asset: LedgerAsset::Usdc,
        quantity: 60,
    })
    .await
    .unwrap();
    repo.execute(LedgerCommand::SettleWithdrawal {
        header: header("withdraw-settle", now),
        user_id: user,
        reservation_id: withdrawal,
        asset: LedgerAsset::Usdc,
        quantity: 25,
    })
    .await
    .unwrap();
    repo.execute(LedgerCommand::ReleaseWithdrawal {
        header: header("withdraw-release", now),
        user_id: user,
        reservation_id: withdrawal,
        asset: LedgerAsset::Usdc,
        quantity: 35,
    })
    .await
    .unwrap();

    // A Core-authorized command is refused without Core's evidence or with a placeholder user,
    // so no ingress can book it on attribution the journal cannot support.
    assert_conflict(
        repo.execute(LedgerCommand::DrawCredit {
            header: header("unauthorized-draw", now),
            user_id: user,
            quantity: 50,
            destination: CreditDestination::EthereumAvailable,
        })
        .await,
        "Core authorization evidence",
    );
    assert_conflict(
        repo.execute(LedgerCommand::DrawCredit {
            header: authorized_header("nil-user-draw", now),
            user_id: Uuid::nil(),
            quantity: 50,
            destination: CreditDestination::EthereumAvailable,
        })
        .await,
        "non-nil user",
    );

    // Principal is USDC-only, does not accrue, and cannot be repaid past zero.
    repo.execute(LedgerCommand::DrawCredit {
        header: authorized_header("draw-credit", now),
        user_id: user,
        quantity: 50,
        destination: CreditDestination::EthereumAvailable,
    })
    .await
    .unwrap();
    repo.execute(LedgerCommand::RecordPrincipalRepayment {
        header: header("repay-principal", now),
        user_id: user,
        quantity: 20,
    })
    .await
    .unwrap();
    assert!(
        repo.execute(LedgerCommand::RecordPrincipalRepayment {
            header: header("over-repay-principal", now),
            user_id: user,
            quantity: 31,
        })
        .await
        .is_err()
    );

    // The remaining reviewed financial templates all commit through the same invariants.
    for command in [
        LedgerCommand::RecordHyperliquidTransfer {
            header: header("hl-transfer-in", now),
            user_id: user,
            quantity: 5,
            direction: TransferDirection::ToHyperliquid,
        },
        LedgerCommand::RecordHyperliquidTransfer {
            header: header("hl-transfer-out", now),
            user_id: user,
            quantity: 5,
            direction: TransferDirection::FromHyperliquid,
        },
        LedgerCommand::RecordRealizedPnl {
            header: header("pnl-gain", now),
            user_id: user,
            quantity: 7,
            direction: GainLoss::Gain,
        },
        LedgerCommand::RecordRealizedPnl {
            header: header("pnl-loss", now),
            user_id: user,
            quantity: 2,
            direction: GainLoss::Loss,
        },
        LedgerCommand::RecordFundingPayment {
            header: header("funding-gain", now),
            user_id: user,
            quantity: 3,
            direction: GainLoss::Gain,
        },
        LedgerCommand::RecordFundingPayment {
            header: header("funding-loss", now),
            user_id: user,
            quantity: 1,
            direction: GainLoss::Loss,
        },
        LedgerCommand::RecordLiquidationProceeds {
            header: header("liquidation-proceeds", now),
            user_id: user,
            quantity: 20,
        },
        LedgerCommand::RecordCustomerSurplus {
            header: header("user-surplus", now),
            user_id: user,
            quantity: 10,
        },
        LedgerCommand::RecordCreditProviderShortfall {
            header: header("provider-shortfall", now),
            user_id: user,
            quantity: 10,
        },
        LedgerCommand::ControlledAdjustment {
            header: header("adjustment-gain", now),
            user_id: user,
            asset: LedgerAsset::Eth,
            bucket: BalanceBucket::Available,
            quantity: 1,
            direction: GainLoss::Gain,
            approval_reference: "two-person-review:42".into(),
        },
        LedgerCommand::ControlledAdjustment {
            header: header("adjustment-loss", now),
            user_id: user,
            asset: LedgerAsset::Eth,
            bucket: BalanceBucket::Available,
            quantity: 1,
            direction: GainLoss::Loss,
            approval_reference: "two-person-review:43".into(),
        },
    ] {
        repo.execute(command).await.unwrap();
    }

    // A reversal is linked, exact, idempotent, and cannot be reversed twice.
    let fee = repo
        .execute(LedgerCommand::RecordVenueFee {
            header: header("fee", now),
            user_id: user,
            quantity: 10,
        })
        .await
        .unwrap();
    let reversal = repo
        .execute(LedgerCommand::ReverseTransaction {
            header: header("fee-reverse", now),
            transaction_id: fee.transaction_id,
        })
        .await
        .unwrap();
    assert_ne!(fee.transaction_id, reversal.transaction_id);
    assert!(
        repo.execute(LedgerCommand::ReverseTransaction {
            header: header("fee-reverse-again", now),
            transaction_id: fee.transaction_id
        })
        .await
        .is_err()
    );

    let export = repo.export().await.unwrap();
    assert_eq!(
        export
            .transactions
            .iter()
            .filter(|tx| tx.command_kind == "controlled_adjustment"
                && tx.audit["approval_reference"]
                    .as_str()
                    .is_some_and(|reference| reference.starts_with("two-person-review:")))
            .count(),
        2
    );
    for tx in export
        .transactions
        .iter()
        .filter(|tx| tx.command_kind == "draw_credit")
    {
        assert_eq!(
            tx.audit["authorization"]["core_authorization_id"],
            tx.idempotency_key
        );
    }
    for tx in &export.transactions {
        let mut totals: HashMap<LedgerAsset, i128> = HashMap::new();
        for posting in &tx.postings {
            *totals.entry(posting.asset).or_default() += match posting.side {
                PostingSide::Debit => posting.quantity as i128,
                PostingSide::Credit => -(posting.quantity as i128),
            };
        }
        assert!(totals.values().all(|value| *value == 0));
    }
    assert!(
        export
            .transactions
            .iter()
            .all(|transaction| transaction.chain_id > 0)
    );
    assert!(
        export
            .transactions
            .iter()
            .any(
                |transaction| transaction.source == LedgerCommandSource::Internal
                    && transaction.chain_id == 137
            )
    );
    assert!(
        export
            .transactions
            .iter()
            .any(
                |transaction| transaction.source == LedgerCommandSource::Hyperliquid
                    && transaction.command_kind == "record_funding_payment"
            )
    );
    assert!(
        export
            .transactions
            .iter()
            .any(
                |transaction| transaction.source == LedgerCommandSource::Internal
                    && transaction.command_kind == "record_principal_repayment"
            )
    );
    let mut rebuilt: HashMap<String, i128> = HashMap::new();
    for transaction in &export.transactions {
        for posting in &transaction.postings {
            *rebuilt.entry(posting.account_code.clone()).or_default() += match posting.side {
                PostingSide::Debit => posting.quantity as i128,
                PostingSide::Credit => -(posting.quantity as i128),
            }
        }
    }
    for (code, quantity) in TestDatabase::ledger_projection(&_storage).await {
        assert_eq!(rebuilt.get(&code).copied().unwrap_or_default(), quantity);
    }

    drop(repo);
    database.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reservation_operations_reject_cross_kind_use() {
    let Some((url, database)) = TestDatabase::create().await else {
        return;
    };
    let repo = PgRepository::initialise(&url, 4)
        .await
        .expect("migrations succeed");
    let _storage = repo.clone();
    let repo = Ledger::new(repo);
    let user = Uuid::new_v4();
    let now = Utc::now();

    repo.execute(LedgerCommand::DrawCredit {
        header: authorized_header("credit", now),
        user_id: user,
        quantity: 1_000,
        destination: CreditDestination::EthereumAvailable,
    })
    .await
    .unwrap();
    let order = Uuid::new_v4();
    let withdrawal = Uuid::new_v4();
    repo.execute(LedgerCommand::ReserveCreditForOrder {
        header: authorized_header("reserve-order", now),
        user_id: user,
        reservation_id: order,
        quantity: 400,
    })
    .await
    .unwrap();
    repo.execute(LedgerCommand::ReserveWithdrawal {
        header: authorized_header("reserve-withdrawal", now),
        user_id: user,
        reservation_id: withdrawal,
        asset: LedgerAsset::Usdc,
        quantity: 300,
    })
    .await
    .unwrap();

    // A reservation may only be spent by commands of its own kind, even when the
    // opposite kind's bucket holds enough balance to cover the movement.
    assert_conflict(
        repo.execute(LedgerCommand::ConsumeOrderReservation {
            header: header("cross-consume", now),
            user_id: user,
            reservation_id: withdrawal,
            quantity: 100,
        })
        .await,
        "invalid or insufficient reservation",
    );
    assert_conflict(
        repo.execute(LedgerCommand::SettleWithdrawal {
            header: header("cross-settle", now),
            user_id: user,
            reservation_id: order,
            asset: LedgerAsset::Usdc,
            quantity: 100,
        })
        .await,
        "invalid or insufficient reservation",
    );
    assert_conflict(
        repo.execute(LedgerCommand::ReleaseWithdrawal {
            header: header("cross-release", now),
            user_id: user,
            reservation_id: order,
            asset: LedgerAsset::Usdc,
            quantity: 100,
        })
        .await,
        "invalid or insufficient reservation",
    );

    // Rejected cross-kind commands leave both reservations and buckets untouched.
    let balances = repo
        .balances(user, None, BalanceTimeBasis::Recorded)
        .await
        .unwrap();
    assert_eq!(
        balance(&balances, LedgerAsset::Usdc, BalanceBucket::OrderReserved),
        400
    );
    assert_eq!(
        balance(
            &balances,
            LedgerAsset::Usdc,
            BalanceBucket::WithdrawalReserved
        ),
        300
    );

    // Same-kind commands still succeed against their own reservations.
    repo.execute(LedgerCommand::ConsumeOrderReservation {
        header: header("consume", now),
        user_id: user,
        reservation_id: order,
        quantity: 100,
    })
    .await
    .unwrap();
    repo.execute(LedgerCommand::SettleWithdrawal {
        header: header("settle", now),
        user_id: user,
        reservation_id: withdrawal,
        asset: LedgerAsset::Usdc,
        quantity: 100,
    })
    .await
    .unwrap();

    drop(repo);
    database.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn custody_exit_reservations_are_atomic_terminal_and_destination_bound() {
    let Some((url, database)) = TestDatabase::create().await else {
        return;
    };
    let repo = PgRepository::initialise(&url, 4)
        .await
        .expect("migrations succeed");
    let _storage = repo.clone();
    let repo = Ledger::new(repo);
    let user = Uuid::new_v4();
    let now = Utc::now();

    repo.execute(LedgerCommand::DrawCredit {
        header: authorized_header("custody-exit-seed", now),
        user_id: user,
        quantity: 200,
        destination: CreditDestination::EthereumAvailable,
    })
    .await
    .unwrap();

    let withdrawal = Uuid::new_v4();
    repo.execute(LedgerCommand::ReserveCustodyExit {
        header: custody_exit_header(withdrawal, "custody-exit-reserve", now),
        user_id: user,
        reservation_id: withdrawal,
        asset: LedgerAsset::Usdc,
        quantity: 100,
        destination: CustodyExitDestination::CustodyOutflow,
    })
    .await
    .unwrap();
    assert_conflict(
        repo.execute(LedgerCommand::ConsumeCustodyExit {
            header: custody_exit_header(withdrawal, "custody-exit-wrong-destination", now),
            user_id: user,
            reservation_id: withdrawal,
            asset: LedgerAsset::Usdc,
            quantity: 100,
            destination: CustodyExitDestination::HyperliquidMargin,
        })
        .await,
        "invalid custody-exit reservation transition",
    );
    let consume = LedgerCommand::ConsumeCustodyExit {
        header: custody_exit_header(withdrawal, "custody-exit-consume", now),
        user_id: user,
        reservation_id: withdrawal,
        asset: LedgerAsset::Usdc,
        quantity: 100,
        destination: CustodyExitDestination::CustodyOutflow,
    };
    let consumed = repo.execute(consume.clone()).await.unwrap();
    assert!(repo.execute(consume).await.unwrap().duplicate);
    assert_conflict(
        repo.execute(LedgerCommand::ReverseTransaction {
            header: header("custody-exit-consume-reverse", now),
            transaction_id: consumed.transaction_id,
        })
        .await,
        "custody-exit transactions cannot be reversed",
    );
    // A consumed hold is terminal, so a second transition must be refused. Which guard reports it
    // is deliberately not pinned: `reserved` is one bucket per (user, asset) shared by every hold,
    // so this release is caught by the reservation state machine while another hold still funds
    // that bucket, and by the balance backstop when none does. `ledger_database_invariants_and_
    // complete_reconstruction` pins `InsufficientFunds` for the same situation on an order
    // reservation, so pinning the transition message here would assert the opposite contract.
    let refused = repo
        .execute(LedgerCommand::ReleaseCustodyExit {
            header: custody_exit_header(withdrawal, "custody-exit-release-after-consume", now),
            user_id: user,
            reservation_id: withdrawal,
            asset: LedgerAsset::Usdc,
            quantity: 100,
            destination: CustodyExitDestination::CustodyOutflow,
        })
        .await;
    assert!(
        matches!(
            refused,
            Err(LedgerError::Conflict(_) | LedgerError::InsufficientFunds)
        ),
        "a consumed custody exit must refuse a second terminal transition, got {refused:?}"
    );

    let margin = Uuid::new_v4();
    repo.execute(LedgerCommand::DrawCreditToCustodyExit {
        header: custody_exit_header(margin, "custody-exit-credit", now),
        user_id: user,
        reservation_id: margin,
        quantity: 50,
        destination: CustodyExitDestination::HyperliquidMargin,
    })
    .await
    .unwrap();
    let before_margin = repo
        .balances(user, None, BalanceTimeBasis::Recorded)
        .await
        .unwrap();
    assert_eq!(
        balance(&before_margin, LedgerAsset::Usdc, BalanceBucket::Available),
        100
    );
    assert_eq!(
        balance(&before_margin, LedgerAsset::Usdc, BalanceBucket::Reserved),
        50
    );
    assert_eq!(
        balance(
            &before_margin,
            LedgerAsset::Usdc,
            BalanceBucket::HyperliquidMargin
        ),
        0
    );
    assert_eq!(
        balance(
            &before_margin,
            LedgerAsset::Usdc,
            BalanceBucket::PrincipalLiability
        ),
        -250
    );
    repo.execute(LedgerCommand::ConsumeCustodyExit {
        header: custody_exit_header(margin, "custody-exit-margin-consume", now),
        user_id: user,
        reservation_id: margin,
        asset: LedgerAsset::Usdc,
        quantity: 50,
        destination: CustodyExitDestination::HyperliquidMargin,
    })
    .await
    .unwrap();

    let released = Uuid::new_v4();
    repo.execute(LedgerCommand::ReserveCustodyExit {
        header: custody_exit_header(released, "custody-exit-reserve-release", now),
        user_id: user,
        reservation_id: released,
        asset: LedgerAsset::Usdc,
        quantity: 40,
        destination: CustodyExitDestination::CustodyOutflow,
    })
    .await
    .unwrap();
    repo.execute(LedgerCommand::ReleaseCustodyExit {
        header: custody_exit_header(released, "custody-exit-release", now),
        user_id: user,
        reservation_id: released,
        asset: LedgerAsset::Usdc,
        quantity: 40,
        destination: CustodyExitDestination::CustodyOutflow,
    })
    .await
    .unwrap();

    let balances = repo
        .balances(user, None, BalanceTimeBasis::Recorded)
        .await
        .unwrap();
    assert_eq!(
        balance(&balances, LedgerAsset::Usdc, BalanceBucket::Available),
        100
    );
    assert_eq!(
        balance(&balances, LedgerAsset::Usdc, BalanceBucket::Reserved),
        0
    );
    assert_eq!(
        balance(
            &balances,
            LedgerAsset::Usdc,
            BalanceBucket::HyperliquidMargin
        ),
        50
    );
    let mut write = _storage.begin_ledger_write().await.unwrap();
    let mut states = Vec::new();
    for id in [withdrawal, margin, released] {
        let state = write.reservation(id).await.unwrap().unwrap();
        states.push((id, state.status));
    }
    write.commit().await.unwrap();
    assert!(
        states
            .iter()
            .any(|row| row.0 == withdrawal && row.1 == "consumed")
    );
    assert!(
        states
            .iter()
            .any(|row| row.0 == margin && row.1 == "consumed")
    );
    assert!(
        states
            .iter()
            .any(|row| row.0 == released && row.1 == "released")
    );

    drop(repo);
    database.cleanup().await;
}

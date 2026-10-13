use risk_engine::Error;
use risk_engine::processing::{
    AMOUNT_SCALE, AccountMode, AccountPortfolio, AccountSnapshot, Amount, AssetRegistry,
    EthereumBalance, FACTOR_SCALE, Factor, Finality, HyperCoreBalance, HyperliquidSnapshot,
    MarginDomain, MarginMode, PRICE_SCALE, PerpPosition, PortfolioGeneration, Price,
    PriceSourceKind, PriceUpdate, RiskPolicy, RiskProcessor, RiskState, SignedAmount, Value,
    VenueKind,
};
use std::collections::BTreeSet;
use tackt_primitives::accounts::AccountSubject;
use uuid::Uuid;

fn subject(account: u32) -> AccountSubject {
    AccountSubject::new(Uuid::from_u128(u128::from(account) + 1)).expect("non-zero subject")
}

fn registry() -> AssetRegistry {
    AssetRegistry::bundled().expect("bundled registry")
}

fn generation(
    version: u64,
    ethereum_balances: Vec<EthereumBalance>,
    hypercore_balances: Vec<HyperCoreBalance>,
    margin_domains: Vec<MarginDomain>,
    perp_positions: Vec<PerpPosition>,
) -> PortfolioGeneration {
    let accounts: BTreeSet<u32> = ethereum_balances
        .iter()
        .map(|record| record.account)
        .chain(hypercore_balances.iter().map(|record| record.account))
        .chain(margin_domains.iter().map(|record| record.account))
        .chain(perp_positions.iter().map(|record| record.account))
        .collect();
    PortfolioGeneration::new(
        version,
        ethereum_balances,
        hypercore_balances,
        margin_domains,
        perp_positions,
    )
    .with_account_subjects(
        accounts
            .into_iter()
            .map(|account| (account, subject(account)))
            .collect(),
    )
}

fn policy() -> RiskPolicy {
    RiskPolicy::new(Factor::from_raw(1_200_000), Factor::from_raw(FACTOR_SCALE))
        .expect("valid policy")
}

fn update(price_index: u32, source: PriceSourceKind, price: i128, sequence: u64) -> PriceUpdate {
    PriceUpdate {
        price_index,
        source,
        source_sequence: sequence,
        price: Price::from_raw(price * PRICE_SCALE),
    }
}

fn domain(
    account: u32,
    domain: u32,
    margin_mode: MarginMode,
    account_value: i128,
    maintenance: i128,
) -> MarginDomain {
    MarginDomain {
        account,
        domain,
        account_mode: AccountMode::Standard,
        margin_mode,
        snapshot: Some(HyperliquidSnapshot {
            account_value: Value::from_raw(account_value * PRICE_SCALE),
            maintenance: Value::from_raw(maintenance * PRICE_SCALE),
            source_sequence: 1,
        }),
        source_version: 1,
    }
}

fn perp(account: u32, domain: u32, market: u32, size: i128, anchor: i128) -> PerpPosition {
    PerpPosition {
        account,
        domain,
        market,
        signed_size: SignedAmount::from_raw(size * AMOUNT_SCALE),
        anchor_price: Some(Price::from_raw(anchor * PRICE_SCALE)),
        position: account * 10 + domain * 2 + market,
        source_version: 1,
    }
}

fn snapshots(processor: &RiskProcessor) -> Vec<AccountSnapshot> {
    (0..processor.account_count())
        .map(|account| {
            processor
                .account(u32::try_from(account).expect("test account fits"))
                .expect("account exists")
        })
        .collect()
}

#[test]
fn account_subject_bindings_are_required_unique_and_permanent() {
    let mut missing = PortfolioGeneration::new(
        1,
        Vec::new(),
        Vec::new(),
        vec![domain(0, 1, MarginMode::Cross, 100, 20)],
        Vec::new(),
    );
    assert!(matches!(
        RiskProcessor::new(2, 1, policy(), registry(), &missing),
        Err(Error::MissingAccountSubject { account: 0 })
    ));

    missing.account_subjects.insert(0, subject(0));
    missing.account_subjects.insert(1, subject(0));
    assert!(matches!(
        RiskProcessor::new(2, 1, policy(), registry(), &missing),
        Err(Error::DuplicateAccountSubject { .. })
    ));

    let installed = generation(
        1,
        Vec::new(),
        Vec::new(),
        vec![domain(0, 1, MarginMode::Cross, 100, 20)],
        Vec::new(),
    );
    let mut processor =
        RiskProcessor::new(2, 1, policy(), registry(), &installed).expect("valid generation");
    let rebound = AccountPortfolio {
        account_subject: subject(1),
        ..patch(
            0,
            2,
            Vec::new(),
            vec![domain(0, 1, MarginMode::Cross, 100, 20)],
            Vec::new(),
        )
    };
    assert!(matches!(
        processor.apply_account_patch(&rebound),
        Err(Error::AccountSubjectRebind { account: 0, .. })
    ));
}

#[test]
fn ethereum_and_hyperliquid_sequences_are_independent_and_source_bound() {
    let mut processor =
        RiskProcessor::new(1, 1, policy(), registry(), &PortfolioGeneration::empty(1))
            .expect("processor");

    let ethereum = update(2, PriceSourceKind::Ethereum, 1, 10);
    let hyperliquid = update(1, PriceSourceKind::Hyperliquid, 1, 1);
    assert_eq!(
        processor
            .apply_price_update(ethereum)
            .expect("Ethereum update")
            .epoch,
        1
    );
    assert_eq!(
        processor
            .apply_price_update(hyperliquid)
            .expect("Hyperliquid update")
            .epoch,
        2
    );
    assert_eq!(
        processor
            .apply_price_update(ethereum)
            .expect("duplicate")
            .epoch,
        1
    );
    assert!(matches!(
        processor.apply_price_update(update(2, PriceSourceKind::Ethereum, 2, 9)),
        Err(Error::StaleSourceSequence { .. })
    ));
    assert!(matches!(
        processor.apply_price_update(update(2, PriceSourceKind::Ethereum, 2, 10)),
        Err(Error::SourceSequenceConflict { .. })
    ));
    assert!(matches!(
        processor.apply_price_update(update(2, PriceSourceKind::Hyperliquid, 1, 2)),
        Err(Error::PriceSourceMismatch { .. })
    ));
}

#[test]
fn finalized_ethereum_and_hypercore_assets_with_the_same_symbol_stay_separate() {
    let generation = generation(
        1,
        vec![
            EthereumBalance {
                account: 0,
                asset: 2,
                amount: Amount::from_raw(10 * AMOUNT_SCALE),
                finality: Finality::Finalized,
                position: 1,
                source_version: 1,
            },
            EthereumBalance {
                account: 0,
                asset: 2,
                amount: Amount::from_raw(100 * AMOUNT_SCALE),
                finality: Finality::InFlight,
                position: 2,
                source_version: 1,
            },
        ],
        vec![HyperCoreBalance {
            account: 0,
            asset: 1,
            amount: Amount::from_raw(20 * AMOUNT_SCALE),
            included_in_account_value: false,
            position: 3,
            source_version: 1,
        }],
        Vec::new(),
        Vec::new(),
    );
    let mut processor =
        RiskProcessor::new(1, 1, policy(), registry(), &generation).expect("processor");

    processor
        .apply_price_update(update(2, PriceSourceKind::Ethereum, 1, 1))
        .expect("Ethereum price");
    assert_eq!(
        processor.account(0).expect("account").collateral_value,
        Value::from_raw(9 * PRICE_SCALE)
    );
    assert_eq!(
        processor.account(0).expect("account").risk_state,
        RiskState::DataIncomplete
    );

    processor
        .apply_price_update(update(1, PriceSourceKind::Hyperliquid, 1, 1))
        .expect("Hyperliquid price");
    let account = processor.account(0).expect("account");
    assert_eq!(account.collateral_value, Value::from_raw(29 * PRICE_SCALE));
    assert_eq!(account.risk_state, RiskState::Healthy);
}

#[test]
fn unified_hypercore_spot_is_not_double_counted() {
    let included = |included_in_account_value| {
        generation(
            1,
            Vec::new(),
            vec![HyperCoreBalance {
                account: 0,
                asset: 1,
                amount: Amount::from_raw(10 * AMOUNT_SCALE),
                included_in_account_value,
                position: 1,
                source_version: 1,
            }],
            vec![MarginDomain {
                account_mode: AccountMode::Unified,
                ..domain(0, 1, MarginMode::Cross, 100, 10)
            }],
            Vec::new(),
        )
    };
    let unified = RiskProcessor::new(1, 1, policy(), registry(), &included(true)).expect("unified");
    let mut standalone =
        RiskProcessor::new(1, 1, policy(), registry(), &included(false)).expect("standalone");
    standalone
        .apply_price_update(update(1, PriceSourceKind::Hyperliquid, 1, 1))
        .expect("spot price");

    assert_eq!(
        unified.account(0).expect("unified").collateral_value,
        Value::from_raw(100 * PRICE_SCALE)
    );
    assert_eq!(
        standalone.account(0).expect("standalone").collateral_value,
        Value::from_raw(110 * PRICE_SCALE)
    );
}

#[test]
fn long_and_short_perps_move_account_value_and_maintenance() {
    let generation = generation(
        1,
        Vec::new(),
        Vec::new(),
        vec![
            domain(0, 1, MarginMode::Cross, 100, 10),
            domain(1, 1, MarginMode::Cross, 100, 10),
        ],
        vec![perp(0, 1, 1, 1, 100), perp(1, 1, 1, -1, 100)],
    );
    let mut processor =
        RiskProcessor::new(2, 2, policy(), registry(), &generation).expect("processor");
    processor
        .apply_price_update(update(3, PriceSourceKind::Hyperliquid, 110, 1))
        .expect("mark price");

    let long = processor
        .margin_domain(0, 1)
        .expect("query")
        .expect("long domain");
    let short = processor
        .margin_domain(1, 1)
        .expect("query")
        .expect("short domain");
    assert_eq!(long.account_value, Value::from_raw(110 * PRICE_SCALE));
    assert_eq!(short.account_value, Value::from_raw(90 * PRICE_SCALE));
    // Base-tier BTC maintenance rate is 1.25%: 10 + |1| × 10 × 1.25%.
    assert_eq!(long.maintenance, Value::from_raw(1_012_500_000));
    assert_eq!(short.maintenance, long.maintenance);
}

#[test]
fn full_account_value_is_collateral_and_maintenance_is_an_independent_boundary() {
    let generation = generation(
        1,
        Vec::new(),
        Vec::new(),
        vec![
            domain(0, 1, MarginMode::Cross, 100, 90),
            domain(1, 1, MarginMode::Cross, -20, 10),
        ],
        Vec::new(),
    );
    let processor = RiskProcessor::new(2, 1, policy(), registry(), &generation).expect("processor");
    let account = processor.account(0).expect("account");

    assert_eq!(account.collateral_value, Value::from_raw(100 * PRICE_SCALE));
    assert_eq!(account.debt_value, Value::zero());
    assert_eq!(account.risk_state, RiskState::Watch);
    assert_eq!(
        processor
            .margin_domain(0, 1)
            .expect("query")
            .expect("domain")
            .risk_state,
        RiskState::Watch
    );
    let negative = processor.account(1).expect("negative account value");
    assert_eq!(negative.collateral_value, Value::zero());
    assert_eq!(negative.debt_value, Value::from_raw(20 * PRICE_SCALE));
    assert_eq!(negative.risk_state, RiskState::Liquidatable);
}

#[test]
fn snapshot_replacement_discards_previously_applied_local_deltas() {
    let first = generation(
        1,
        Vec::new(),
        Vec::new(),
        vec![domain(0, 1, MarginMode::Cross, 100, 10)],
        vec![perp(0, 1, 1, 1, 100)],
    );
    let mut processor = RiskProcessor::new(1, 1, policy(), registry(), &first).expect("processor");
    processor
        .apply_price_update(update(3, PriceSourceKind::Hyperliquid, 110, 1))
        .expect("mark");
    assert_eq!(
        processor
            .margin_domain(0, 1)
            .expect("query")
            .expect("domain")
            .account_value,
        Value::from_raw(110 * PRICE_SCALE)
    );

    let mut replacement_domain = domain(0, 1, MarginMode::Cross, 150, 12);
    replacement_domain.source_version = 2;
    replacement_domain
        .snapshot
        .as_mut()
        .expect("snapshot")
        .source_sequence = 2;
    let mut replacement_position = perp(0, 1, 1, 1, 110);
    replacement_position.source_version = 2;
    processor
        .install_portfolio_generation(&generation(
            2,
            Vec::new(),
            Vec::new(),
            vec![replacement_domain],
            vec![replacement_position],
        ))
        .expect("replacement");

    let replaced = processor
        .margin_domain(0, 1)
        .expect("query")
        .expect("domain");
    assert_eq!(replaced.account_value, Value::from_raw(150 * PRICE_SCALE));
    assert_eq!(replaced.maintenance, Value::from_raw(12 * PRICE_SCALE));
}

#[test]
fn domains_roll_up_to_overall_account_health_and_missing_inputs_remain_incomplete() {
    let generation = generation(
        1,
        Vec::new(),
        Vec::new(),
        vec![
            domain(0, 1, MarginMode::Cross, 100, 10),
            domain(0, 2, MarginMode::Hip3, 10, 10),
            MarginDomain {
                account: 1,
                domain: 1,
                account_mode: AccountMode::Standard,
                margin_mode: MarginMode::Isolated,
                snapshot: None,
                source_version: 1,
            },
        ],
        Vec::new(),
    );
    let processor = RiskProcessor::new(2, 1, policy(), registry(), &generation).expect("processor");

    assert_eq!(
        processor.account(0).expect("account").risk_state,
        RiskState::Healthy
    );
    assert_eq!(
        processor
            .margin_domain(0, 1)
            .expect("query")
            .expect("default")
            .risk_state,
        RiskState::Healthy
    );
    assert_eq!(
        processor
            .margin_domain(0, 2)
            .expect("query")
            .expect("HIP-3")
            .risk_state,
        RiskState::Liquidatable
    );
    assert_eq!(
        processor.account(1).expect("missing snapshot").risk_state,
        RiskState::DataIncomplete
    );
}

#[test]
fn ethereum_balance_supports_overall_health_without_moving_assets() {
    let generation = generation(
        1,
        vec![EthereumBalance {
            account: 0,
            asset: 2,
            amount: Amount::from_raw(100 * AMOUNT_SCALE),
            finality: Finality::Finalized,
            position: 1,
            source_version: 1,
        }],
        Vec::new(),
        vec![domain(0, 1, MarginMode::Cross, 10, 10)],
        Vec::new(),
    );
    let mut processor =
        RiskProcessor::new(1, 1, policy(), registry(), &generation).expect("processor");
    assert_eq!(
        processor
            .account(0)
            .expect("missing Ethereum price")
            .risk_state,
        RiskState::DataIncomplete
    );

    processor
        .apply_price_update(update(2, PriceSourceKind::Ethereum, 1, 1))
        .expect("Ethereum price");

    let account = processor.account(0).expect("overall account");
    assert_eq!(account.collateral_value, Value::from_raw(100 * PRICE_SCALE));
    assert_eq!(account.maintenance_value, Value::from_raw(10 * PRICE_SCALE));
    assert_eq!(account.health, Factor::from_raw(10 * FACTOR_SCALE));
    assert_eq!(account.risk_state, RiskState::Healthy);
    assert_eq!(
        processor
            .margin_domain(0, 1)
            .expect("query")
            .expect("domain")
            .risk_state,
        RiskState::Liquidatable
    );
}

#[test]
fn results_are_identical_with_one_and_eight_workers() {
    let domains = (0..64)
        .map(|account| domain(account, 1, MarginMode::Cross, 100, 20))
        .collect::<Vec<_>>();
    let positions = (0..64)
        .map(|account| perp(account, 1, 1, if account % 2 == 0 { 1 } else { -1 }, 100))
        .collect::<Vec<_>>();
    let generation = generation(1, Vec::new(), Vec::new(), domains, positions);
    let mut one = RiskProcessor::new(64, 1, policy(), registry(), &generation).expect("one");
    let mut eight = RiskProcessor::new(64, 8, policy(), registry(), &generation).expect("eight");

    for (price, sequence) in [(110, 1), (80, 2), (105, 3)] {
        let update = update(3, PriceSourceKind::Hyperliquid, price, sequence);
        assert_eq!(
            one.apply_price_update(update).expect("one").events,
            eight.apply_price_update(update).expect("eight").events
        );
    }
    assert_eq!(snapshots(&one), snapshots(&eight));
    for account in 0..64 {
        assert_eq!(
            one.venue_margin(account, VenueKind::HyperliquidPerps)
                .expect("one venue"),
            eight
                .venue_margin(account, VenueKind::HyperliquidPerps)
                .expect("eight venue"),
        );
    }
}

#[test]
fn anchorless_perp_positions_match_between_incremental_updates_and_rebuild() {
    let mut anchorless = perp(0, 1, 1, 1, 100);
    anchorless.anchor_price = None;
    let generation = generation(
        1,
        Vec::new(),
        Vec::new(),
        vec![domain(0, 1, MarginMode::Cross, 100, 10)],
        vec![anchorless],
    );
    let mut processor =
        RiskProcessor::new(1, 1, policy(), registry(), &generation).expect("processor");
    processor
        .apply_price_update(update(3, PriceSourceKind::Hyperliquid, 100, 1))
        .expect("first mark");
    processor
        .apply_price_update(update(3, PriceSourceKind::Hyperliquid, 110, 2))
        .expect("second mark");

    let incremental = processor
        .margin_domain(0, 1)
        .expect("query")
        .expect("domain");
    // The missing anchor keeps the position out of valuation and the domain data-incomplete;
    // the first observed price must not become an implicit anchor.
    assert_eq!(
        incremental.account_value,
        Value::from_raw(100 * PRICE_SCALE)
    );
    assert_eq!(incremental.risk_state, RiskState::DataIncomplete);
    let incremental_account = processor.account(0).expect("account");

    processor.rebuild().expect("rebuild");
    assert_eq!(
        processor
            .margin_domain(0, 1)
            .expect("query")
            .expect("domain"),
        incremental
    );
    let rebuilt_account = processor.account(0).expect("account");
    assert_eq!(
        rebuilt_account.collateral_value,
        incremental_account.collateral_value
    );
    assert_eq!(rebuilt_account.risk_state, incremental_account.risk_state);
}

#[test]
fn generation_installs_report_risk_state_transitions() {
    let first = generation(
        1,
        Vec::new(),
        Vec::new(),
        vec![domain(0, 1, MarginMode::Cross, 100, 10)],
        Vec::new(),
    );
    let mut processor = RiskProcessor::new(1, 1, policy(), registry(), &first).expect("processor");
    assert_eq!(
        processor.account(0).expect("account").risk_state,
        RiskState::Healthy
    );

    let collapsed = || {
        let mut collapsed = domain(0, 1, MarginMode::Cross, 5, 10);
        collapsed.source_version = 2;
        collapsed
            .snapshot
            .as_mut()
            .expect("snapshot")
            .source_sequence = 2;
        generation(2, Vec::new(), Vec::new(), vec![collapsed], Vec::new())
    };
    let install = processor
        .install_portfolio_generation(&collapsed())
        .expect("install");

    assert!(install.installed);
    assert_eq!(install.events.len(), 1);
    assert_eq!(install.events[0].previous_state, RiskState::Healthy);
    assert_eq!(install.events[0].current_state, RiskState::Liquidatable);

    let repeat = processor
        .install_portfolio_generation(&collapsed())
        .expect("identical generation");
    assert!(!repeat.installed);
    assert!(repeat.events.is_empty());
}

#[test]
fn same_version_generations_with_lane_invisible_differences_conflict() {
    let mut processor =
        RiskProcessor::new(1, 1, policy(), registry(), &PortfolioGeneration::empty(1))
            .expect("processor");

    // An in-flight balance compiles to no lane records, so this generation and an empty
    // one are indistinguishable in the compiled index; only the content digest differs.
    let in_flight = generation(
        2,
        vec![EthereumBalance {
            account: 0,
            asset: 2,
            amount: Amount::from_raw(10 * AMOUNT_SCALE),
            finality: Finality::InFlight,
            position: 1,
            source_version: 1,
        }],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    assert!(
        processor
            .install_portfolio_generation(&in_flight)
            .expect("install")
            .installed
    );

    assert!(matches!(
        processor.install_portfolio_generation(&generation(
            2,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new()
        )),
        Err(Error::PortfolioGenerationConflict { generation: 2 })
    ));
}

#[test]
fn same_version_reorder_of_equal_sort_key_records_is_a_duplicate() {
    let mut processor =
        RiskProcessor::new(1, 1, policy(), registry(), &PortfolioGeneration::empty(1))
            .expect("processor");

    let balance = |position: u32, amount: i128| EthereumBalance {
        account: 0,
        asset: 2,
        amount: Amount::from_raw(amount * AMOUNT_SCALE),
        finality: Finality::Finalized,
        position,
        source_version: 1,
    };
    // Two finalized balances of one asset share the (price, shard, account) lane sort
    // key, so compiled lane order follows input order; the digest must still identify
    // the reordered resend as the same content.
    assert!(
        processor
            .install_portfolio_generation(&generation(
                2,
                vec![balance(1, 10), balance(2, 20)],
                Vec::new(),
                Vec::new(),
                Vec::new()
            ))
            .expect("install")
            .installed
    );

    let repeat = processor
        .install_portfolio_generation(&generation(
            2,
            vec![balance(2, 20), balance(1, 10)],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ))
        .expect("identical content reordered");
    assert!(!repeat.installed);
    assert!(repeat.events.is_empty());
}

#[test]
fn included_hypercore_balance_requires_a_margin_domain() {
    let generation = generation(
        1,
        Vec::new(),
        vec![HyperCoreBalance {
            account: 0,
            asset: 1,
            amount: Amount::from_raw(10 * AMOUNT_SCALE),
            included_in_account_value: true,
            position: 1,
            source_version: 1,
        }],
        Vec::new(),
        Vec::new(),
    );

    assert!(matches!(
        RiskProcessor::new(1, 1, policy(), registry(), &generation),
        Err(Error::IncludedBalanceWithoutMarginDomain { .. })
    ));
}

fn patch(
    account: u32,
    version: u64,
    hypercore_balances: Vec<HyperCoreBalance>,
    margin_domains: Vec<MarginDomain>,
    perp_positions: Vec<PerpPosition>,
) -> AccountPortfolio {
    AccountPortfolio {
        account,
        account_subject: subject(account),
        version,
        ethereum_balances: Vec::new(),
        hypercore_balances,
        margin_domains,
        perp_positions,
    }
}

fn comparable(snapshot: &AccountSnapshot) -> impl PartialEq + std::fmt::Debug + use<> {
    (
        snapshot.account,
        snapshot.collateral_value,
        snapshot.debt_value,
        snapshot.maintenance_value,
        snapshot.health,
        snapshot.minimum_margin_health,
        snapshot.risk_state,
        snapshot.uncertain_inputs,
    )
}

#[test]
fn account_patches_apply_and_report_transitions() {
    let first = generation(
        1,
        Vec::new(),
        Vec::new(),
        vec![domain(0, 1, MarginMode::Cross, 100, 10)],
        Vec::new(),
    );
    let mut processor = RiskProcessor::new(1, 1, policy(), registry(), &first).expect("processor");
    let baseline_epoch = processor.last_committed_epoch();

    let mut collapsed = domain(0, 1, MarginMode::Cross, 5, 10);
    collapsed.source_version = 2;
    collapsed
        .snapshot
        .as_mut()
        .expect("snapshot")
        .source_sequence = 2;
    let outcome = processor
        .apply_account_patch(&patch(0, 2, Vec::new(), vec![collapsed], Vec::new()))
        .expect("patch");

    assert_eq!(outcome.epoch, baseline_epoch + 1);
    assert_eq!(
        outcome.status,
        risk_engine::processing::EpochStatus::Applied
    );
    assert_eq!(outcome.events.len(), 1);
    assert_eq!(outcome.events[0].previous_state, RiskState::Healthy);
    assert_eq!(outcome.events[0].current_state, RiskState::Liquidatable);
    assert_eq!(
        processor
            .margin_domain(0, 1)
            .expect("query")
            .expect("domain")
            .account_value,
        Value::from_raw(5 * PRICE_SCALE)
    );
}

#[test]
fn account_patches_enforce_versioning() {
    let first = generation(
        1,
        Vec::new(),
        Vec::new(),
        vec![domain(0, 1, MarginMode::Cross, 100, 10)],
        Vec::new(),
    );
    let mut processor = RiskProcessor::new(1, 1, policy(), registry(), &first).expect("processor");

    let updated = |version: u64, account_value: i128| {
        let mut updated = domain(0, 1, MarginMode::Cross, account_value, 10);
        updated.source_version = 2;
        updated.snapshot.as_mut().expect("snapshot").source_sequence = 2;
        patch(0, version, Vec::new(), vec![updated], Vec::new())
    };

    let applied = processor
        .apply_account_patch(&updated(3, 50))
        .expect("patch");
    assert!(matches!(
        processor.apply_account_patch(&updated(2, 50)),
        Err(Error::StaleAccountPatch { .. })
    ));
    let duplicate = processor
        .apply_account_patch(&updated(3, 50))
        .expect("duplicate");
    assert_eq!(
        duplicate.status,
        risk_engine::processing::EpochStatus::Duplicate
    );
    assert_eq!(duplicate.epoch, applied.epoch);
    assert!(duplicate.events.is_empty());

    assert!(matches!(
        processor.apply_account_patch(&updated(3, 60)),
        Err(Error::AccountPatchConflict { .. })
    ));
}

#[test]
fn patches_match_full_install_of_identical_content() {
    let base_domains = vec![
        domain(0, 1, MarginMode::Cross, 100, 20),
        domain(1, 1, MarginMode::Cross, 100, 20),
    ];
    let base_perps = vec![perp(0, 1, 1, 1, 100), perp(1, 1, 1, -1, 100)];
    let mut patched = RiskProcessor::new(
        2,
        2,
        policy(),
        registry(),
        &generation(1, Vec::new(), Vec::new(), base_domains, base_perps),
    )
    .expect("patched processor");

    patched
        .apply_price_update(update(3, PriceSourceKind::Hyperliquid, 110, 1))
        .expect("price");

    // Replace account 0's whole portfolio: bigger position, fresh snapshot, spot balance.
    let mut new_domain = domain(0, 1, MarginMode::Cross, 150, 30);
    new_domain.source_version = 2;
    new_domain
        .snapshot
        .as_mut()
        .expect("snapshot")
        .source_sequence = 2;
    let mut new_perp = perp(0, 1, 1, 2, 110);
    new_perp.source_version = 2;
    let spot = HyperCoreBalance {
        account: 0,
        asset: 1,
        amount: Amount::from_raw(10 * AMOUNT_SCALE),
        included_in_account_value: false,
        position: 100,
        source_version: 2,
    };
    patched
        .apply_account_patch(&patch(
            0,
            2,
            vec![spot.clone()],
            vec![new_domain.clone()],
            vec![new_perp.clone()],
        ))
        .expect("patch");

    patched
        .apply_price_update(update(3, PriceSourceKind::Hyperliquid, 90, 2))
        .expect("second price");
    patched
        .apply_price_update(update(1, PriceSourceKind::Hyperliquid, 1, 3))
        .expect("spot price");

    // Oracle: one full install of the final content, replaying the same prices.
    let final_generation = generation(
        2,
        Vec::new(),
        vec![spot],
        vec![new_domain, domain(1, 1, MarginMode::Cross, 100, 20)],
        vec![new_perp, perp(1, 1, 1, -1, 100)],
    );
    let mut oracle =
        RiskProcessor::new(2, 2, policy(), registry(), &final_generation).expect("oracle");
    for (index, sequence) in [(3, 1_u64), (3, 2), (1, 3)] {
        let price = match (index, sequence) {
            (3, 1) => 110,
            (3, 2) => 90,
            _ => 1,
        };
        oracle
            .apply_price_update(update(index, PriceSourceKind::Hyperliquid, price, sequence))
            .expect("oracle price");
    }

    for account in 0..2 {
        assert_eq!(
            comparable(&patched.account(account).expect("patched")),
            comparable(&oracle.account(account).expect("oracle")),
            "account {account} diverged from the full-install oracle"
        );
        assert_eq!(
            patched.margin_domain(account, 1).expect("patched"),
            oracle.margin_domain(account, 1).expect("oracle"),
        );
        assert_eq!(
            patched
                .venue_margin(account, VenueKind::HyperliquidPerps)
                .expect("patched venue"),
            oracle
                .venue_margin(account, VenueKind::HyperliquidPerps)
                .expect("oracle venue"),
        );
    }

    // A rebuild of the patched processor must be a content no-op: the compiled index is
    // the source of truth and already reflects every patch. (Rebuilds re-evaluate every
    // account, so state versions advance; the values must not.)
    let before: Vec<_> = snapshots(&patched).iter().map(comparable).collect();
    let events = patched.rebuild().expect("rebuild");
    assert!(events.is_empty());
    let after: Vec<_> = snapshots(&patched).iter().map(comparable).collect();
    assert_eq!(after, before);
}

#[test]
fn empty_patch_removes_account_and_uncertainty_balances() {
    let first = generation(
        1,
        Vec::new(),
        vec![HyperCoreBalance {
            account: 0,
            asset: 1,
            amount: Amount::from_raw(10 * AMOUNT_SCALE),
            included_in_account_value: false,
            position: 1,
            source_version: 1,
        }],
        Vec::new(),
        Vec::new(),
    );
    let mut processor = RiskProcessor::new(1, 1, policy(), registry(), &first).expect("processor");
    assert_eq!(
        processor.account(0).expect("account").risk_state,
        RiskState::DataIncomplete
    );

    // A patch while the price is still missing keeps the account uncertain.
    let doubled = HyperCoreBalance {
        amount: Amount::from_raw(20 * AMOUNT_SCALE),
        position: 2,
        source_version: 2,
        ..first_balance()
    };
    processor
        .apply_account_patch(&patch(0, 2, vec![doubled], Vec::new(), Vec::new()))
        .expect("patch");
    let account = processor.account(0).expect("account");
    assert_eq!(account.risk_state, RiskState::DataIncomplete);
    assert_eq!(account.uncertain_inputs, 1);

    // First price arrival pays the uncertainty off exactly.
    processor
        .apply_price_update(update(1, PriceSourceKind::Hyperliquid, 1, 1))
        .expect("price");
    let account = processor.account(0).expect("account");
    assert_eq!(account.risk_state, RiskState::Healthy);
    assert_eq!(account.uncertain_inputs, 0);
    assert_eq!(account.collateral_value, Value::from_raw(20 * PRICE_SCALE));

    // An empty patch removes the account entirely.
    let outcome = processor
        .apply_account_patch(&patch(0, 3, Vec::new(), Vec::new(), Vec::new()))
        .expect("removal");
    assert_eq!(outcome.events.len(), 0);
    let account = processor.account(0).expect("account");
    assert_eq!(account.collateral_value, Value::zero());
    assert_eq!(account.risk_state, RiskState::Healthy);
}

fn first_balance() -> HyperCoreBalance {
    HyperCoreBalance {
        account: 0,
        asset: 1,
        amount: Amount::from_raw(10 * AMOUNT_SCALE),
        included_in_account_value: false,
        position: 1,
        source_version: 1,
    }
}

#[test]
fn patches_cannot_steal_positions_and_sweeps_supersede_patches() {
    let first = generation(
        1,
        Vec::new(),
        vec![
            first_balance(),
            HyperCoreBalance {
                account: 1,
                position: 2,
                ..first_balance()
            },
        ],
        Vec::new(),
        Vec::new(),
    );
    let mut processor = RiskProcessor::new(2, 1, policy(), registry(), &first).expect("processor");

    // Position 2 belongs to account 1.
    let theft = patch(
        0,
        5,
        vec![HyperCoreBalance {
            position: 2,
            source_version: 2,
            ..first_balance()
        }],
        Vec::new(),
        Vec::new(),
    );
    assert!(matches!(
        processor.apply_account_patch(&theft),
        Err(Error::PositionOwnedByOtherAccount {
            position: 2,
            owner: 1
        })
    ));

    processor
        .apply_account_patch(&patch(0, 5, Vec::new(), Vec::new(), Vec::new()))
        .expect("patch");

    // A reconciliation sweep older than the newest patch is stale; a new-enough one
    // resets every per-account cursor.
    assert!(matches!(
        processor.install_portfolio_generation(&generation(
            3,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new()
        )),
        Err(Error::StaleReconciliationSweep {
            received: 3,
            newest_patch: 5
        })
    ));
    processor
        .install_portfolio_generation(&generation(
            5,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ))
        .expect("sweep");
    assert!(matches!(
        processor.apply_account_patch(&patch(0, 4, Vec::new(), Vec::new(), Vec::new())),
        Err(Error::StaleAccountPatch { .. })
    ));
    // Re-sending the sweep's exact per-account content at the sweep's version is idempotent.
    assert_eq!(
        processor
            .apply_account_patch(&patch(0, 5, Vec::new(), Vec::new(), Vec::new()))
            .expect("idempotent patch")
            .status,
        risk_engine::processing::EpochStatus::Duplicate
    );
    processor
        .apply_account_patch(&patch(0, 6, Vec::new(), Vec::new(), Vec::new()))
        .expect("newer patch");
}

#[test]
fn interleaved_patches_and_prices_are_identical_with_one_and_eight_workers() {
    let domains = (0..64)
        .map(|account| domain(account, 1, MarginMode::Cross, 100, 20))
        .collect::<Vec<_>>();
    let positions = (0..64)
        .map(|account| perp(account, 1, 1, if account % 2 == 0 { 1 } else { -1 }, 100))
        .collect::<Vec<_>>();
    let generation = generation(1, Vec::new(), Vec::new(), domains, positions);
    let mut one = RiskProcessor::new(64, 1, policy(), registry(), &generation).expect("one");
    let mut eight = RiskProcessor::new(64, 8, policy(), registry(), &generation).expect("eight");

    for (step, (price, sequence)) in [(110, 1_u64), (80, 2), (105, 3), (70, 4)]
        .into_iter()
        .enumerate()
    {
        let update = update(3, PriceSourceKind::Hyperliquid, price, sequence);
        assert_eq!(
            one.apply_price_update(update).expect("one").events,
            eight.apply_price_update(update).expect("eight").events
        );
        let account = u32::try_from(step * 13 % 64).expect("account fits");
        let mut refreshed = domain(account, 1, MarginMode::Cross, 60, 25);
        refreshed.source_version = 2;
        refreshed
            .snapshot
            .as_mut()
            .expect("snapshot")
            .source_sequence = 2;
        let mut position = perp(account, 1, 1, 2, price);
        position.source_version = 2;
        let account_patch = patch(
            account,
            2 + step as u64,
            Vec::new(),
            vec![refreshed],
            vec![position],
        );
        assert_eq!(
            one.apply_account_patch(&account_patch).expect("one patch"),
            eight
                .apply_account_patch(&account_patch)
                .expect("eight patch")
        );
    }
    assert_eq!(snapshots(&one), snapshots(&eight));
    for account in 0..64 {
        assert_eq!(
            one.venue_margin(account, VenueKind::HyperliquidPerps)
                .expect("one venue"),
            eight
                .venue_margin(account, VenueKind::HyperliquidPerps)
                .expect("eight venue"),
        );
    }
}

#[test]
fn rebuild_after_patches_keeps_domain_routing_intact() {
    // Three accounts share one shard so dense slot numbering and the patched slab's
    // free-list numbering diverge as soon as account 0 gains a second domain.
    let domains = (0..3)
        .map(|account| {
            domain(
                account,
                1,
                MarginMode::Cross,
                100 + i128::from(account) * 20,
                10,
            )
        })
        .collect::<Vec<_>>();
    let positions = (0..3).map(|account| perp(account, 1, 1, 1, 100)).collect();
    let generation = generation(1, Vec::new(), Vec::new(), domains, positions);
    let mut processor =
        RiskProcessor::new(3, 1, policy(), registry(), &generation).expect("processor");
    processor
        .apply_price_update(update(3, PriceSourceKind::Hyperliquid, 110, 1))
        .expect("price");

    let mut split_one = domain(0, 1, MarginMode::Cross, 40, 5);
    split_one.source_version = 2;
    split_one
        .snapshot
        .as_mut()
        .expect("snapshot")
        .source_sequence = 2;
    let mut split_two = domain(0, 2, MarginMode::Hip3, 70, 6);
    split_two.source_version = 2;
    split_two
        .snapshot
        .as_mut()
        .expect("snapshot")
        .source_sequence = 2;
    let mut moved = perp(0, 2, 1, 1, 110);
    moved.source_version = 2;
    processor
        .apply_account_patch(&patch(
            0,
            2,
            Vec::new(),
            vec![split_one, split_two],
            vec![moved],
        ))
        .expect("patch");

    let before_domains: Vec<_> = [(0, 1), (0, 2), (1, 1), (2, 1)]
        .into_iter()
        .map(|(account, index)| {
            processor
                .margin_domain(account, index)
                .expect("query")
                .expect("domain")
        })
        .collect();
    let before_accounts: Vec<_> = snapshots(&processor).iter().map(comparable).collect();

    let events = processor.rebuild().expect("rebuild");
    assert!(events.is_empty());

    let after_domains: Vec<_> = [(0, 1), (0, 2), (1, 1), (2, 1)]
        .into_iter()
        .map(|(account, index)| {
            processor
                .margin_domain(account, index)
                .expect("query")
                .expect("domain")
        })
        .collect();
    assert_eq!(after_domains, before_domains);
    let after_accounts: Vec<_> = snapshots(&processor).iter().map(comparable).collect();
    assert_eq!(after_accounts, before_accounts);

    // The rebuilt stores and renumbered lanes must agree: the next price update routes
    // every delta to the right domain (this panicked a worker before the renumber fix).
    processor
        .apply_price_update(update(3, PriceSourceKind::Hyperliquid, 95, 2))
        .expect("post-rebuild price");
    assert_eq!(
        processor
            .margin_domain(0, 2)
            .expect("query")
            .expect("domain")
            .account_value,
        // 70 anchored at 110: 70 + 1 x (95 - 110) = 55.
        Value::from_raw(55 * PRICE_SCALE)
    );
}

#[test]
fn inconsistent_hyperliquid_snapshot_is_rejected() {
    let mut inconsistent = domain(0, 1, MarginMode::Cross, 100, 10);
    inconsistent
        .snapshot
        .as_mut()
        .expect("snapshot")
        .maintenance = Value::from_raw(-1);
    let result = RiskProcessor::new(
        1,
        1,
        policy(),
        registry(),
        &generation(1, Vec::new(), Vec::new(), vec![inconsistent], Vec::new()),
    );

    assert!(matches!(
        result,
        Err(Error::InconsistentMarginSnapshot { .. })
    ));
}

#[test]
fn venue_health_excludes_other_collateral_and_preserves_pool_isolation() {
    let generation = generation(
        1,
        Vec::new(),
        vec![HyperCoreBalance {
            account: 0,
            asset: 1,
            amount: Amount::from_raw(10_000 * AMOUNT_SCALE),
            included_in_account_value: false,
            position: 100,
            source_version: 1,
        }],
        vec![
            domain(0, 7, MarginMode::Isolated, -10, 20),
            domain(0, 2, MarginMode::Cross, 1_000, 100),
            domain(0, 9, MarginMode::Hip3, 10, 10),
            domain(1, 2, MarginMode::Cross, 5, 100),
        ],
        Vec::new(),
    );
    for workers in [1, 2] {
        let mut processor =
            RiskProcessor::new(2, workers, policy(), registry(), &generation).expect("processor");
        // Unpriced spot makes the whole account incomplete, without contaminating venue health.
        assert_eq!(
            processor.account(0).expect("account").risk_state,
            RiskState::DataIncomplete
        );
        let venue = processor
            .venue_margin(0, VenueKind::HyperliquidPerps)
            .expect("venue")
            .expect("perp pools");
        assert_eq!(venue.account_value, Value::from_raw(1_000 * PRICE_SCALE));
        assert_eq!(venue.maintenance, Value::from_raw(130 * PRICE_SCALE));
        assert_eq!(venue.health, Factor::from_raw(-500_000));
        assert_eq!(venue.margin_ratio, Factor::from_raw(i128::MAX));
        assert_eq!(venue.minimum_margin_health, Factor::from_raw(-500_000));
        assert_eq!(venue.risk_state, RiskState::Liquidatable);
        assert_eq!(venue.uncertain_inputs, 0);
        assert_eq!(
            processor
                .margin_domains(0)
                .expect("pools")
                .map(|pool| pool.domain)
                .collect::<Vec<_>>(),
            vec![2, 7, 9]
        );
        processor
            .apply_price_update(update(1, PriceSourceKind::Hyperliquid, 1, 1))
            .expect("spot price");
        assert_eq!(
            processor.account(0).expect("account").risk_state,
            RiskState::Healthy
        );
        assert_eq!(
            processor
                .venue_margin(0, VenueKind::HyperliquidPerps)
                .expect("venue"),
            Some(venue)
        );
        assert_eq!(
            processor
                .venue_margin(0, VenueKind::HyperCore)
                .expect("spot venue"),
            None
        );
        assert!(matches!(
            processor.venue_margin(2, VenueKind::HyperliquidPerps),
            Err(Error::AccountOutOfRange { account: 2 })
        ));
    }
}

#[test]
fn venue_health_tracks_missing_inputs_zero_maintenance_and_pool_removal() {
    let mut missing = domain(0, 3, MarginMode::Cross, 0, 0);
    missing.snapshot = None;
    let mut processor = RiskProcessor::new(
        1,
        1,
        policy(),
        registry(),
        &generation(
            1,
            Vec::new(),
            Vec::new(),
            vec![missing, domain(0, 1, MarginMode::Isolated, -1, 1)],
            Vec::new(),
        ),
    )
    .expect("processor");
    let venue = processor
        .venue_margin(0, VenueKind::HyperliquidPerps)
        .expect("venue")
        .expect("missing pool still exists");
    assert_eq!(venue.risk_state, RiskState::DataIncomplete);
    assert_eq!(venue.uncertain_inputs, 1);
    for (version, equity, state, health) in [
        (2, -1, RiskState::Liquidatable, i128::MIN),
        (3, 0, RiskState::Healthy, i128::MAX),
        (4, 10, RiskState::Healthy, i128::MAX),
    ] {
        processor
            .apply_account_patch(&patch(
                0,
                version,
                Vec::new(),
                vec![domain(0, 9, MarginMode::Cross, equity, 0)],
                Vec::new(),
            ))
            .expect("replace pool");
        let venue = processor
            .venue_margin(0, VenueKind::HyperliquidPerps)
            .expect("venue")
            .expect("pool");
        assert_eq!(venue.risk_state, state);
        assert_eq!(venue.health.raw(), health);
        assert_eq!(venue.uncertain_inputs, 0);
    }
    processor
        .apply_account_patch(&patch(0, 5, Vec::new(), Vec::new(), Vec::new()))
        .expect("remove pools");
    assert_eq!(
        processor
            .venue_margin(0, VenueKind::HyperliquidPerps)
            .expect("venue"),
        None
    );
    assert_eq!(processor.margin_domains(0).expect("pools").count(), 0);
}

#[test]
fn native_venue_ratios_and_gross_leverage_preserve_cross_and_isolated_pools() {
    let mut cross = domain(0, 1, MarginMode::Cross, 200, 0);
    cross.snapshot.as_mut().expect("snapshot").maintenance = Value::from_raw(1_250_000_000);
    let mut isolated = domain(0, 2, MarginMode::Isolated, 100, 0);
    isolated.snapshot.as_mut().expect("snapshot").maintenance = Value::from_raw(1_250_000_000);
    let input = generation(
        1,
        Vec::new(),
        Vec::new(),
        vec![cross, isolated],
        vec![perp(0, 1, 1, 10, 100), perp(0, 2, 1, -10, 100)],
    );
    let mut processor = RiskProcessor::new(1, 1, policy(), registry(), &input).expect("processor");
    assert_eq!(
        processor
            .venue_margin(0, VenueKind::HyperliquidPerps)
            .expect("query")
            .expect("venue")
            .risk_state,
        RiskState::DataIncomplete
    );
    processor
        .apply_price_update(update(3, PriceSourceKind::Hyperliquid, 100, 1))
        .expect("mark");
    let venue = processor
        .venue_margin(0, VenueKind::HyperliquidPerps)
        .expect("query")
        .expect("venue");
    assert_eq!(venue.account_value, Value::from_raw(300 * PRICE_SCALE));
    assert_eq!(venue.gross_notional, Value::from_raw(2_000 * PRICE_SCALE));
    assert_eq!(venue.leverage, Factor::from_raw(6_666_667));
    assert_eq!(venue.margin_ratio, Factor::from_raw(125_000));
    assert_eq!(venue.health, Factor::from_raw(8 * FACTOR_SCALE));
    // The hedge preserves total equity, but the isolated short runs out of its own margin.
    processor
        .apply_price_update(update(3, PriceSourceKind::Hyperliquid, 110, 2))
        .expect("mark");
    let venue = processor
        .venue_margin(0, VenueKind::HyperliquidPerps)
        .expect("query")
        .expect("venue");
    assert_eq!(venue.account_value, Value::from_raw(300 * PRICE_SCALE));
    assert_eq!(venue.gross_notional, Value::from_raw(2_200 * PRICE_SCALE));
    assert_eq!(venue.leverage, Factor::from_raw(7_333_334));
    assert_eq!(venue.maintenance, Value::from_raw(2_750_000_000));
    assert_eq!(venue.margin_ratio, Factor::from_raw(i128::MAX));
    assert_eq!(venue.risk_state, RiskState::Liquidatable);
    processor
        .apply_account_patch(&patch(
            0,
            2,
            Vec::new(),
            vec![domain(0, 1, MarginMode::Cross, 300, 0)],
            Vec::new(),
        ))
        .expect("close positions");
    let venue = processor
        .venue_margin(0, VenueKind::HyperliquidPerps)
        .expect("query")
        .expect("venue");
    assert_eq!(venue.gross_notional, Value::zero());
    assert_eq!(venue.leverage, Factor::zero());
}

#[test]
fn venue_liquidation_boundary_is_native_and_unified_is_explicitly_unsupported() {
    // A caller's Tackt liquidation threshold must not change Hyperliquid's boundary.
    let custom =
        RiskPolicy::new(Factor::from_raw(2_000_000), Factor::from_raw(1_500_000)).expect("policy");
    let input = generation(
        1,
        Vec::new(),
        Vec::new(),
        vec![domain(0, 1, MarginMode::Cross, 100, 100)],
        Vec::new(),
    );
    let mut processor = RiskProcessor::new(1, 1, custom, registry(), &input).expect("processor");
    let venue = processor
        .venue_margin(0, VenueKind::HyperliquidPerps)
        .expect("query")
        .expect("venue");
    assert_eq!(venue.margin_ratio, Factor::from_raw(FACTOR_SCALE));
    assert_eq!(venue.risk_state, RiskState::Watch); // equality is the boundary, not below it
    let mut below = domain(0, 1, MarginMode::Cross, 100, 100);
    below.snapshot.as_mut().expect("snapshot").account_value =
        Value::from_raw(100 * PRICE_SCALE - 1);
    processor
        .apply_account_patch(&patch(0, 2, Vec::new(), vec![below], Vec::new()))
        .expect("patch");
    assert_eq!(
        processor
            .venue_margin(0, VenueKind::HyperliquidPerps)
            .expect("query")
            .expect("venue")
            .risk_state,
        RiskState::Liquidatable
    );
    let mut unified = domain(0, 1, MarginMode::Cross, 100, 10);
    unified.account_mode = AccountMode::Unified;
    processor
        .apply_account_patch(&patch(0, 3, Vec::new(), vec![unified], Vec::new()))
        .expect("patch");
    assert_eq!(
        processor.venue_margin(0, VenueKind::HyperliquidPerps),
        Err(Error::UnsupportedVenueAccountMode { account: 0 })
    );
}

#[test]
fn native_tier_crossings_match_fresh_install_rebuild_and_patch() {
    // BTC: first $150M at 1/80, remainder at 1/40; 2,000 BTC anchored at $100k.
    let input = generation(
        1,
        Vec::new(),
        Vec::new(),
        vec![domain(0, 1, MarginMode::Cross, 300_000_000, 3_125_000)],
        vec![perp(0, 1, 1, 2_000, 100_000)],
    );
    for workers in [1, 8] {
        let mut incremental =
            RiskProcessor::new(8, workers, policy(), registry(), &input).expect("processor");
        for (index, (mark, maintenance)) in [
            (80_000, 2_125_000),
            (75_000, 1_875_000),
            (50_000, 1_250_000),
            (100_000, 3_125_000),
        ]
        .into_iter()
        .enumerate()
        {
            let mark = update(3, PriceSourceKind::Hyperliquid, mark, index as u64 + 1);
            incremental.apply_price_update(mark).expect("mark");
            let actual = incremental
                .venue_margin(0, VenueKind::HyperliquidPerps)
                .expect("query")
                .expect("venue");
            assert_eq!(
                actual.maintenance,
                Value::from_raw(maintenance * PRICE_SCALE)
            );
            let mut fresh =
                RiskProcessor::new(8, workers, policy(), registry(), &input).expect("fresh");
            fresh.apply_price_update(mark).expect("fresh mark");
            assert_eq!(
                Some(actual.clone()),
                fresh
                    .venue_margin(0, VenueKind::HyperliquidPerps)
                    .expect("fresh query")
            );
            incremental.rebuild().expect("rebuild");
            assert_eq!(
                Some(actual.clone()),
                incremental
                    .venue_margin(0, VenueKind::HyperliquidPerps)
                    .expect("rebuild query")
            );
            incremental
                .apply_account_patch(&patch(
                    0,
                    index as u64 + 2,
                    Vec::new(),
                    input.margin_domains.clone(),
                    input.perp_positions.clone(),
                ))
                .expect("patch");
            assert_eq!(
                Some(actual),
                incremental
                    .venue_margin(0, VenueKind::HyperliquidPerps)
                    .expect("patch query")
            );
        }
    }
}

#[test]
fn fractional_mark_paths_do_not_accumulate_rounding_drift() {
    let mut position = perp(0, 1, 1, 1, 100);
    position.signed_size = SignedAmount::from_raw(1_234_567_891);
    let input = generation(
        1,
        Vec::new(),
        Vec::new(),
        vec![domain(0, 1, MarginMode::Cross, 100, 10)],
        vec![position],
    );
    let mut incremental =
        RiskProcessor::new(1, 1, policy(), registry(), &input).expect("processor");
    for (index, raw) in [
        100 * PRICE_SCALE + 1,
        100 * PRICE_SCALE + 3,
        100 * PRICE_SCALE - 1,
        100 * PRICE_SCALE,
    ]
    .into_iter()
    .enumerate()
    {
        let mark = PriceUpdate {
            price: Price::from_raw(raw),
            ..update(3, PriceSourceKind::Hyperliquid, 100, index as u64 + 1)
        };
        incremental.apply_price_update(mark).expect("mark");
        let mut fresh = RiskProcessor::new(1, 1, policy(), registry(), &input).expect("fresh");
        fresh.apply_price_update(mark).expect("fresh mark");
        assert_eq!(
            incremental
                .venue_margin(0, VenueKind::HyperliquidPerps)
                .expect("query"),
            fresh
                .venue_margin(0, VenueKind::HyperliquidPerps)
                .expect("fresh query")
        );
    }
}

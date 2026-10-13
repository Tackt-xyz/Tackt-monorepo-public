//! Canonical binary digests for portfolio content.
//!
//! Records are encoded with fixed-width little-endian fields in a canonical order, so the
//! digest identifies semantic content: two inputs with the same records in any order share a
//! digest, and any field difference — including in records that compile to no hot-path lanes —
//! produces a different one. The generation digest is the combination of per-account digests,
//! which also serve per-account patch conflict detection.

use sha2::{Digest, Sha256};

use super::{
    AccountIndex, AccountMode, EthereumBalance, Finality, HyperCoreBalance, MarginDomain,
    MarginMode, PerpPosition, PortfolioGeneration, StateVersion,
};
use tackt_primitives::accounts::AccountSubject;
use uuid::Uuid;

const ETHEREUM_SECTION: u8 = 1;
const HYPERCORE_SECTION: u8 = 2;
const DOMAIN_SECTION: u8 = 3;
const PERP_SECTION: u8 = 4;

/// Reusable encoding buffers, so digesting many accounts stays allocation-free.
#[derive(Debug, Default)]
pub(crate) struct DigestScratch {
    bytes: Vec<u8>,
    ranges: Vec<(usize, usize)>,
}

/// Digest of one account's records across all four record kinds.
pub(crate) fn account_digest<'record>(
    account: AccountIndex,
    scratch: &mut DigestScratch,
    ethereum_balances: impl Iterator<Item = &'record EthereumBalance>,
    hypercore_balances: impl Iterator<Item = &'record HyperCoreBalance>,
    margin_domains: impl Iterator<Item = &'record MarginDomain>,
    perp_positions: impl Iterator<Item = &'record PerpPosition>,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(account.to_le_bytes());
    record_sections(
        &mut [&mut hasher],
        scratch,
        ethereum_balances,
        hypercore_balances,
        margin_domains,
        perp_positions,
    );
    hasher.finalize().into()
}

/// Account-prefixed and index-independent digests produced from one canonical encoding pass.
pub(crate) struct AccountDigests {
    pub account: [u8; 32],
    pub content: [u8; 32],
}

/// A generation digest together with the per-account digests it combines.
#[derive(Debug)]
pub(crate) struct GenerationDigests {
    /// Combined whole-generation digest.
    pub digest: [u8; 32],
    /// Digest per account present in the generation, ascending by account.
    pub accounts: Vec<(AccountIndex, [u8; 32])>,
}

/// Digest of one [`super::AccountPortfolio`]'s records.
pub(crate) fn account_portfolio_digests(patch: &super::AccountPortfolio) -> AccountDigests {
    let mut account = Sha256::new();
    account.update(patch.account.to_le_bytes());
    let mut content = Sha256::new();
    record_sections(
        &mut [&mut account, &mut content],
        &mut DigestScratch::default(),
        patch.ethereum_balances.iter(),
        patch.hypercore_balances.iter(),
        patch.margin_domains.iter(),
        patch.perp_positions.iter(),
    );
    AccountDigests {
        account: account.finalize().into(),
        content: content.finalize().into(),
    }
}

/// Named evaluation fields prevent same-typed version arguments from being transposed.
#[derive(Clone, Copy)]
pub(crate) struct EvaluationDigestInput {
    pub action_id: Uuid,
    pub subject: AccountSubject,
    pub account_version: StateVersion,
    pub input_version: StateVersion,
    pub portfolio_digest: [u8; 32],
}

/// Canonical digest of every caller-controlled evaluation input.
pub(crate) fn evaluation_input_digest(input: EvaluationDigestInput) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"tackt-risk-evaluation-input");
    hasher.update(input.action_id.as_bytes());
    hasher.update(input.subject.user_id().as_bytes());
    hasher.update(input.account_version.to_le_bytes());
    hasher.update(input.input_version.to_le_bytes());
    hasher.update(input.portfolio_digest);
    hasher.finalize().into()
}

/// Digest of an account with no records: the committed content of any never-seen account.
pub(crate) fn empty_account_digest(account: AccountIndex) -> [u8; 32] {
    account_digest(
        account,
        &mut DigestScratch::default(),
        std::iter::empty(),
        std::iter::empty(),
        std::iter::empty(),
        std::iter::empty(),
    )
}

/// Digest of a whole generation: per-account digests combined in ascending account order.
#[cfg(test)]
pub(crate) fn generation_digest(generation: &PortfolioGeneration) -> [u8; 32] {
    generation_digests(generation).digest
}

/// Whole-generation digest plus the per-account digests that compose it.
pub(crate) fn generation_digests(generation: &PortfolioGeneration) -> GenerationDigests {
    let ethereum = by_account(&generation.ethereum_balances, |balance| balance.account);
    let hypercore = by_account(&generation.hypercore_balances, |balance| balance.account);
    let domains = by_account(&generation.margin_domains, |domain| domain.account);
    let perps = by_account(&generation.perp_positions, |position| position.account);

    let mut accounts: Vec<AccountIndex> = Vec::with_capacity(
        generation.account_subjects.len()
            + ethereum.len()
            + hypercore.len()
            + domains.len()
            + perps.len(),
    );
    accounts.extend(generation.account_subjects.keys().copied());
    accounts.extend(ethereum.iter().map(|(account, _)| *account));
    accounts.extend(hypercore.iter().map(|(account, _)| *account));
    accounts.extend(domains.iter().map(|(account, _)| *account));
    accounts.extend(perps.iter().map(|(account, _)| *account));
    accounts.sort_unstable();
    accounts.dedup();

    let mut hasher = Sha256::new();
    let mut scratch = DigestScratch::default();
    let mut cursors = [0_usize; 4];
    let mut account_digests = Vec::with_capacity(accounts.len());
    for account in accounts {
        let subject = generation
            .account_subjects
            .get(&account)
            .expect("portfolio validation requires a subject for every account");
        let digest = account_digest(
            account,
            &mut scratch,
            take(&ethereum, &mut cursors[0], account),
            take(&hypercore, &mut cursors[1], account),
            take(&domains, &mut cursors[2], account),
            take(&perps, &mut cursors[3], account),
        );
        hasher.update(account.to_le_bytes());
        hasher.update(subject.user_id().as_bytes());
        hasher.update(digest);
        account_digests.push((account, digest));
    }
    GenerationDigests {
        digest: hasher.finalize().into(),
        accounts: account_digests,
    }
}

fn by_account<T>(records: &[T], account: impl Fn(&T) -> AccountIndex) -> Vec<(AccountIndex, &T)> {
    let mut grouped: Vec<(AccountIndex, &T)> = records
        .iter()
        .map(|record| (account(record), record))
        .collect();
    grouped.sort_by_key(|(account, _)| *account);
    grouped
}

fn take<'group, T>(
    grouped: &'group [(AccountIndex, &'group T)],
    cursor: &mut usize,
    account: AccountIndex,
) -> impl Iterator<Item = &'group T> + use<'group, T> {
    let start = *cursor;
    while *cursor < grouped.len() && grouped[*cursor].0 == account {
        *cursor += 1;
    }
    grouped[start..*cursor].iter().map(|(_, record)| *record)
}

fn section<'record, T: 'record>(
    hashers: &mut [&mut Sha256],
    scratch: &mut DigestScratch,
    tag: u8,
    records: impl Iterator<Item = &'record T>,
    encode: impl Fn(&mut Vec<u8>, &T),
) {
    scratch.bytes.clear();
    scratch.ranges.clear();
    for record in records {
        let start = scratch.bytes.len();
        encode(&mut scratch.bytes, record);
        scratch.ranges.push((start, scratch.bytes.len()));
    }
    let bytes = &scratch.bytes;
    scratch
        .ranges
        .sort_unstable_by(|left, right| bytes[left.0..left.1].cmp(&bytes[right.0..right.1]));

    for hasher in hashers {
        hasher.update([tag]);
        hasher.update((scratch.ranges.len() as u64).to_le_bytes());
        for (start, end) in &scratch.ranges {
            hasher.update(&scratch.bytes[*start..*end]);
        }
    }
}

fn record_sections<'record>(
    hashers: &mut [&mut Sha256],
    scratch: &mut DigestScratch,
    ethereum_balances: impl Iterator<Item = &'record EthereumBalance>,
    hypercore_balances: impl Iterator<Item = &'record HyperCoreBalance>,
    margin_domains: impl Iterator<Item = &'record MarginDomain>,
    perp_positions: impl Iterator<Item = &'record PerpPosition>,
) {
    section(
        hashers,
        scratch,
        ETHEREUM_SECTION,
        ethereum_balances,
        encode_ethereum,
    );
    section(
        hashers,
        scratch,
        HYPERCORE_SECTION,
        hypercore_balances,
        encode_hypercore,
    );
    section(
        hashers,
        scratch,
        DOMAIN_SECTION,
        margin_domains,
        encode_domain,
    );
    section(hashers, scratch, PERP_SECTION, perp_positions, encode_perp);
}

fn encode_ethereum(buffer: &mut Vec<u8>, balance: &EthereumBalance) {
    buffer.extend_from_slice(&balance.asset.to_le_bytes());
    buffer.extend_from_slice(&balance.amount.raw().to_le_bytes());
    buffer.push(finality_tag(balance.finality));
    buffer.extend_from_slice(&balance.position.to_le_bytes());
    buffer.extend_from_slice(&balance.source_version.to_le_bytes());
}

fn encode_hypercore(buffer: &mut Vec<u8>, balance: &HyperCoreBalance) {
    buffer.extend_from_slice(&balance.asset.to_le_bytes());
    buffer.extend_from_slice(&balance.amount.raw().to_le_bytes());
    buffer.push(u8::from(balance.included_in_account_value));
    buffer.extend_from_slice(&balance.position.to_le_bytes());
    buffer.extend_from_slice(&balance.source_version.to_le_bytes());
}

fn encode_domain(buffer: &mut Vec<u8>, domain: &MarginDomain) {
    buffer.extend_from_slice(&domain.domain.to_le_bytes());
    buffer.push(account_mode_tag(domain.account_mode));
    buffer.push(margin_mode_tag(domain.margin_mode));
    match domain.snapshot {
        Some(snapshot) => {
            buffer.push(1);
            buffer.extend_from_slice(&snapshot.account_value.raw().to_le_bytes());
            buffer.extend_from_slice(&snapshot.maintenance.raw().to_le_bytes());
            buffer.extend_from_slice(&snapshot.source_sequence.to_le_bytes());
        }
        None => buffer.push(0),
    }
    buffer.extend_from_slice(&domain.source_version.to_le_bytes());
}

fn encode_perp(buffer: &mut Vec<u8>, position: &PerpPosition) {
    buffer.extend_from_slice(&position.domain.to_le_bytes());
    buffer.extend_from_slice(&position.market.to_le_bytes());
    buffer.extend_from_slice(&position.signed_size.raw().to_le_bytes());
    match position.anchor_price {
        Some(price) => {
            buffer.push(1);
            buffer.extend_from_slice(&price.raw().to_le_bytes());
        }
        None => buffer.push(0),
    }
    buffer.extend_from_slice(&position.position.to_le_bytes());
    buffer.extend_from_slice(&position.source_version.to_le_bytes());
}

const fn finality_tag(finality: Finality) -> u8 {
    match finality {
        Finality::Finalized => 0,
        Finality::InFlight => 1,
        Finality::Reverted => 2,
    }
}

const fn account_mode_tag(mode: AccountMode) -> u8 {
    match mode {
        AccountMode::Standard => 0,
        AccountMode::Unified => 1,
    }
}

const fn margin_mode_tag(mode: MarginMode) -> u8 {
    match mode {
        MarginMode::Cross => 0,
        MarginMode::Isolated => 1,
        MarginMode::Hip3 => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Amount, HyperliquidSnapshot, Price, SignedAmount, Value};
    use super::*;
    use std::collections::BTreeMap;
    use tackt_primitives::accounts::AccountSubject;
    use uuid::Uuid;

    fn balance(account: AccountIndex, position: u32) -> EthereumBalance {
        EthereumBalance {
            account,
            asset: 2,
            amount: Amount::from_raw(position.into()),
            finality: Finality::Finalized,
            position,
            source_version: 1,
        }
    }

    #[test]
    fn digest_is_order_insensitive_and_field_sensitive() {
        let ordered = PortfolioGeneration::new(
            1,
            vec![balance(0, 1), balance(1, 2)],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_account_subjects(BTreeMap::from([
            (0, AccountSubject::new(Uuid::from_u128(1)).expect("subject")),
            (1, AccountSubject::new(Uuid::from_u128(2)).expect("subject")),
        ]));
        let mut reordered = ordered.clone();
        reordered.ethereum_balances.reverse();
        assert_eq!(generation_digest(&ordered), generation_digest(&reordered));

        let mut changed = ordered.clone();
        changed.ethereum_balances[0].finality = Finality::InFlight;
        assert_ne!(generation_digest(&ordered), generation_digest(&changed));
    }

    #[test]
    fn lane_invisible_records_change_the_digest() {
        let empty = PortfolioGeneration::empty(1);
        let mut in_flight = PortfolioGeneration::empty(1);
        in_flight
            .account_subjects
            .insert(0, AccountSubject::new(Uuid::from_u128(1)).expect("subject"));
        in_flight.ethereum_balances.push(EthereumBalance {
            finality: Finality::InFlight,
            ..balance(0, 1)
        });
        assert_ne!(generation_digest(&empty), generation_digest(&in_flight));
    }

    #[test]
    fn record_kinds_do_not_collide() {
        let mut with_domain = PortfolioGeneration::empty(1);
        with_domain
            .account_subjects
            .insert(0, AccountSubject::new(Uuid::from_u128(1)).expect("subject"));
        with_domain.margin_domains.push(MarginDomain {
            account: 0,
            domain: 1,
            account_mode: AccountMode::Standard,
            margin_mode: MarginMode::Cross,
            snapshot: Some(HyperliquidSnapshot {
                account_value: Value::from_raw(1),
                maintenance: Value::zero(),
                source_sequence: 1,
            }),
            source_version: 1,
        });
        let mut with_perp = PortfolioGeneration::empty(1);
        with_perp
            .account_subjects
            .insert(0, AccountSubject::new(Uuid::from_u128(1)).expect("subject"));
        with_perp.perp_positions.push(PerpPosition {
            account: 0,
            domain: 1,
            market: 1,
            signed_size: SignedAmount::from_raw(1),
            anchor_price: Some(Price::from_raw(1)),
            position: 1,
            source_version: 1,
        });
        assert_ne!(
            generation_digest(&with_domain),
            generation_digest(&with_perp)
        );
    }
}

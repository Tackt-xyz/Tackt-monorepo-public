use std::collections::HashSet;
use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::error::{Error, Result};

use super::{
    AccountIndex, AccountMode, AccountPortfolio, Amount, AssetIndex, AssetMetadata, AssetRegistry,
    Finality, HyperliquidSnapshot, MAX_AMOUNT_RAW, MAX_PRICE_RAW, MAX_VALUE_RAW, MarginDomainIndex,
    MarginMode, PerpMarketIndex, PortfolioGeneration, PositionIndex, Price, PriceIndex,
    SignedAmount, StateVersion, VenueKind,
    arithmetic::weighted_amount,
    digest::{account_portfolio_digests, empty_account_digest, generation_digests},
};

/// Validated portfolio generation compiled into price-indexed, sharded hot-path lanes.
#[derive(Debug, Clone)]
pub struct PortfolioIndex {
    generation: StateVersion,
    // Sole content authority for same-version install comparison; covers every field of
    // every record, including those that compile to no lanes (in-flight balances,
    // included spot).
    generation_digest: [u8; 32],
    shard_count: usize,
    by_price: BTreeMap<PriceIndex, Vec<PriceShardLanes>>,
    domains: Vec<Vec<IndexedDomain>>,
    // Bookkeeping for account patches; derived from content and excluded from equality.
    // Dense by account so fresh-account patches update in O(1); `None` means the account
    // has no committed records (its digest is the empty-account digest).
    account_digests: Vec<Option<[u8; 32]>>,
    positions_by_account: HashMap<AccountIndex, Vec<PositionIndex>>,
    position_owner: HashMap<PositionIndex, AccountIndex>,
}

/// Content equality is the digest alone (plus generation and shard count): lane
/// compilation is a deterministic function of digested content, while lane tie order for
/// equal sort keys depends on input record order, so comparing lanes would report
/// conflicts for reordered-but-identical content.
impl PartialEq for PortfolioIndex {
    fn eq(&self, other: &Self) -> bool {
        self.generation == other.generation
            && self.generation_digest == other.generation_digest
            && self.shard_count == other.shard_count
    }
}

impl Eq for PortfolioIndex {}

#[derive(Debug, Clone, Default)]
pub(crate) struct PriceShardLanes {
    pub balances: BalanceLane,
    pub perps: PerpLane,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct BalanceLane {
    pub accounts: Vec<AccountIndex>,
    pub weighted_amounts: Vec<i128>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct PerpLane {
    pub accounts: Vec<AccountIndex>,
    pub domain_offsets: Vec<usize>,
    pub signed_sizes: Vec<SignedAmount>,
    pub anchor_prices: Vec<Option<Price>>,
    pub markets: Vec<PerpMarketIndex>,
}

#[derive(Debug, Clone)]
pub(crate) struct IndexedDomain {
    pub account: AccountIndex,
    pub external_index: MarginDomainIndex,
    pub account_mode: AccountMode,
    pub margin_mode: MarginMode,
    pub snapshot: Option<HyperliquidSnapshot>,
}

impl PortfolioIndex {
    /// Validates and compiles an immutable portfolio generation into hot-path lanes.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid accounts, assets, markets, domains, versions, or values.
    #[allow(clippy::too_many_lines)]
    pub fn build(
        generation: &PortfolioGeneration,
        account_count: usize,
        shard_count: usize,
        registry: &AssetRegistry,
    ) -> Result<Self> {
        if shard_count == 0 {
            return Err(Error::InvalidShardCount { shard_count });
        }
        if generation.version == 0 {
            return Err(Error::InvalidPortfolioGenerationVersion);
        }
        let mut subjects = HashSet::with_capacity(generation.account_subjects.len());
        for (&account, &subject) in &generation.account_subjects {
            validate_account(account, account_count)?;
            if !subjects.insert(subject) {
                return Err(Error::DuplicateAccountSubject { subject });
            }
        }
        let require_subject = |account: AccountIndex| {
            generation
                .account_subjects
                .contains_key(&account)
                .then_some(())
                .ok_or(Error::MissingAccountSubject { account })
        };
        for account in generation
            .ethereum_balances
            .iter()
            .map(|record| record.account)
            .chain(
                generation
                    .hypercore_balances
                    .iter()
                    .map(|record| record.account),
            )
            .chain(
                generation
                    .margin_domains
                    .iter()
                    .map(|record| record.account),
            )
            .chain(
                generation
                    .perp_positions
                    .iter()
                    .map(|record| record.account),
            )
        {
            require_subject(account)?;
        }
        let digests = generation_digests(generation);
        let mut positions_by_account: HashMap<AccountIndex, Vec<PositionIndex>> = HashMap::new();

        let mut positions = BTreeSet::new();
        let mut domain_keys = BTreeSet::new();
        let mut domains = vec![Vec::new(); shard_count];
        for domain in &generation.margin_domains {
            validate_account(domain.account, account_count)?;
            validate_source_version(domain.account, domain.source_version)?;
            validate_domain_snapshot(domain.account, domain.domain, domain.snapshot)?;
            let key = (domain.account, domain.domain);
            let shard = shard_for(domain.account, shard_count);
            if !domain_keys.insert(key) {
                return Err(Error::DuplicateMarginDomain {
                    account: domain.account,
                    domain: domain.domain,
                });
            }
            domains[shard].push(IndexedDomain {
                account: domain.account,
                external_index: domain.domain,
                account_mode: domain.account_mode,
                margin_mode: domain.margin_mode,
                snapshot: domain.snapshot,
            });
        }
        let mut domain_lookup = BTreeMap::new();
        for shard_domains in &mut domains {
            shard_domains.sort_unstable_by_key(|domain| (domain.account, domain.external_index));
            for (offset, domain) in shard_domains.iter().enumerate() {
                domain_lookup.insert((domain.account, domain.external_index), offset);
            }
        }

        let mut balance_records = Vec::new();
        for balance in &generation.ethereum_balances {
            let asset = validate_balance(
                registry,
                account_count,
                &mut positions,
                balance.account,
                balance.asset,
                balance.amount,
                balance.position,
                balance.source_version,
                VenueKind::Ethereum,
            )?;
            positions_by_account
                .entry(balance.account)
                .or_default()
                .push(balance.position);
            if !asset.collateral_enabled {
                return Err(Error::CollateralAssetDisabled {
                    asset: balance.asset,
                });
            }
            if balance.finality == Finality::Finalized {
                balance_records.push((
                    asset.price_index,
                    balance.account,
                    weighted_amount(balance.amount, asset.collateral_factor)?,
                ));
            }
        }

        for balance in &generation.hypercore_balances {
            let asset = validate_balance(
                registry,
                account_count,
                &mut positions,
                balance.account,
                balance.asset,
                balance.amount,
                balance.position,
                balance.source_version,
                VenueKind::HyperCore,
            )?;
            positions_by_account
                .entry(balance.account)
                .or_default()
                .push(balance.position);
            if balance.included_in_account_value {
                require_margin_domain(&domain_keys, balance.account, balance.asset)?;
            } else {
                if !asset.collateral_enabled {
                    return Err(Error::CollateralAssetDisabled {
                        asset: balance.asset,
                    });
                }
                balance_records.push((
                    asset.price_index,
                    balance.account,
                    weighted_amount(balance.amount, asset.collateral_factor)?,
                ));
            }
        }

        let mut perp_records = Vec::with_capacity(generation.perp_positions.len());
        for position in &generation.perp_positions {
            validate_account(position.account, account_count)?;
            validate_source_version(position.account, position.source_version)?;
            validate_position(position.position, &mut positions)?;
            positions_by_account
                .entry(position.account)
                .or_default()
                .push(position.position);
            let price_index = validate_perp_position(
                registry,
                position.account,
                position.market,
                position.signed_size,
                position.anchor_price,
            )?;
            let domain_offset = domain_lookup
                .get(&(position.account, position.domain))
                .copied()
                .ok_or(Error::MissingMarginDomain {
                    account: position.account,
                    domain: position.domain,
                })?;
            perp_records.push((
                price_index,
                position.account,
                position.domain,
                domain_offset,
                position.signed_size,
                position.anchor_price,
                position.market,
            ));
        }

        let mut by_price = BTreeMap::<PriceIndex, Vec<PriceShardLanes>>::new();
        balance_records.sort_unstable_by_key(|(price, account, _)| {
            (*price, shard_for(*account, shard_count), *account)
        });
        for (price, account, weighted) in balance_records {
            let shard = shard_for(account, shard_count);
            let lanes = by_price
                .entry(price)
                .or_insert_with(|| vec![PriceShardLanes::default(); shard_count]);
            lanes[shard].balances.accounts.push(account);
            lanes[shard].balances.weighted_amounts.push(weighted);
        }

        // Sort by the external domain index, not the slot: record order must be a pure
        // function of content so patched insertions land identically to a full rebuild.
        perp_records.sort_unstable_by_key(|(price, account, external, ..)| {
            (
                *price,
                shard_for(*account, shard_count),
                *account,
                *external,
            )
        });
        for (price, account, _, slot, size, anchor, market) in perp_records {
            let shard = shard_for(account, shard_count);
            let lanes = by_price
                .entry(price)
                .or_insert_with(|| vec![PriceShardLanes::default(); shard_count]);
            let lane = &mut lanes[shard].perps;
            lane.accounts.push(account);
            lane.domain_offsets.push(slot);
            lane.signed_sizes.push(size);
            lane.anchor_prices.push(anchor);
            lane.markets.push(market);
        }

        let position_owner = positions_by_account
            .iter()
            .flat_map(|(account, positions)| positions.iter().map(|position| (*position, *account)))
            .collect();
        let mut account_digests = vec![None; account_count];
        for (account, digest) in digests.accounts {
            account_digests[account as usize] = Some(digest);
        }
        Ok(Self {
            generation: generation.version,
            generation_digest: digests.digest,
            shard_count,
            by_price,
            domains,
            account_digests,
            positions_by_account,
            position_owner,
        })
    }

    #[must_use]
    pub const fn generation(&self) -> StateVersion {
        self.generation
    }

    #[must_use]
    pub const fn shard_count(&self) -> usize {
        self.shard_count
    }

    #[must_use]
    pub fn balance_count(&self) -> usize {
        self.by_price
            .values()
            .flatten()
            .map(|lanes| lanes.balances.accounts.len())
            .sum()
    }

    #[must_use]
    pub fn perp_position_count(&self) -> usize {
        self.by_price
            .values()
            .flatten()
            .map(|lanes| lanes.perps.accounts.len())
            .sum()
    }

    #[must_use]
    pub fn margin_domain_count(&self) -> usize {
        self.domains.iter().map(Vec::len).sum()
    }

    pub(crate) fn for_price(&self, price: PriceIndex) -> Option<&[PriceShardLanes]> {
        self.by_price.get(&price).map(Vec::as_slice)
    }

    pub(crate) fn prices(&self) -> impl Iterator<Item = (PriceIndex, &[PriceShardLanes])> {
        self.by_price
            .iter()
            .map(|(price, shards)| (*price, shards.as_slice()))
    }

    pub(crate) fn domains(&self) -> &[Vec<IndexedDomain>] {
        &self.domains
    }

    pub(crate) fn max_records_per_shard(&self) -> Vec<usize> {
        let mut maximums = vec![0; self.shard_count];
        for shards in self.by_price.values() {
            for (maximum, lanes) in maximums.iter_mut().zip(shards) {
                *maximum =
                    (*maximum).max(lanes.balances.accounts.len() + lanes.perps.accounts.len());
            }
        }
        maximums
    }

    /// Returns the committed content digest of one account.
    pub(crate) fn account_digest(&self, account: AccountIndex) -> [u8; 32] {
        self.account_digests
            .get(account as usize)
            .copied()
            .flatten()
            .unwrap_or_else(|| empty_account_digest(account))
    }

    /// Rejects patch positions already owned by another account.
    ///
    /// # Errors
    ///
    /// Returns an error naming the first stolen position.
    pub(crate) fn validate_position_owners(
        &self,
        account: AccountIndex,
        positions: &[PositionIndex],
    ) -> Result<()> {
        for position in positions {
            if let Some(owner) = self.position_owner.get(position)
                && *owner != account
            {
                return Err(Error::PositionOwnedByOtherAccount {
                    position: *position,
                    owner: *owner,
                });
            }
        }
        Ok(())
    }

    /// Splices one account's records out of every lane. Infallible.
    pub(crate) fn remove_account_lanes(&mut self, account: AccountIndex) {
        let shard = shard_for(account, self.shard_count);
        for lanes in self.by_price.values_mut() {
            let balances = &mut lanes[shard].balances;
            let start = balances.accounts.partition_point(|&other| other < account);
            let end = balances.accounts.partition_point(|&other| other <= account);
            if start < end {
                balances.accounts.drain(start..end);
                balances.weighted_amounts.drain(start..end);
            }
            let perps = &mut lanes[shard].perps;
            let start = perps.accounts.partition_point(|&other| other < account);
            let end = perps.accounts.partition_point(|&other| other <= account);
            if start < end {
                perps.accounts.drain(start..end);
                perps.domain_offsets.drain(start..end);
                perps.signed_sizes.drain(start..end);
                perps.anchor_prices.drain(start..end);
                perps.markets.drain(start..end);
            }
        }
    }

    /// Splices one compiled account's records into the lanes, mapping each perp record's
    /// external domain index to its slab slot. Infallible; records must be price-grouped and
    /// the slot map sorted by external index, as `compile_account` produces.
    pub(crate) fn insert_account_lanes(
        &mut self,
        compiled: &CompiledAccount,
        domain_slots: &[(MarginDomainIndex, u32)],
    ) {
        let account = compiled.account;
        let shard = shard_for(account, self.shard_count);
        let shard_count = self.shard_count;

        let mut cursor = 0;
        while cursor < compiled.balance_records.len() {
            let price = compiled.balance_records[cursor].0;
            let start = cursor;
            while cursor < compiled.balance_records.len()
                && compiled.balance_records[cursor].0 == price
            {
                cursor += 1;
            }
            let lanes = self
                .by_price
                .entry(price)
                .or_insert_with(|| vec![PriceShardLanes::default(); shard_count]);
            let balances = &mut lanes[shard].balances;
            let at = balances.accounts.partition_point(|&other| other < account);
            balances
                .accounts
                .splice(at..at, std::iter::repeat_n(account, cursor - start));
            balances.weighted_amounts.splice(
                at..at,
                compiled.balance_records[start..cursor]
                    .iter()
                    .map(|(_, weighted)| *weighted),
            );
        }

        let mut cursor = 0;
        while cursor < compiled.perp_records.len() {
            let price = compiled.perp_records[cursor].price;
            let start = cursor;
            while cursor < compiled.perp_records.len()
                && compiled.perp_records[cursor].price == price
            {
                cursor += 1;
            }
            let lanes = self
                .by_price
                .entry(price)
                .or_insert_with(|| vec![PriceShardLanes::default(); shard_count]);
            let perps = &mut lanes[shard].perps;
            let at = perps.accounts.partition_point(|&other| other < account);
            let records = &compiled.perp_records[start..cursor];
            perps
                .accounts
                .splice(at..at, std::iter::repeat_n(account, records.len()));
            perps.domain_offsets.splice(
                at..at,
                records.iter().map(|record| {
                    let found = domain_slots
                        .binary_search_by_key(&record.domain, |(external, _)| *external)
                        .expect("patch perps reference patch domains");
                    domain_slots[found].1 as usize
                }),
            );
            perps
                .signed_sizes
                .splice(at..at, records.iter().map(|record| record.signed_size));
            perps
                .anchor_prices
                .splice(at..at, records.iter().map(|record| record.anchor_price));
            perps
                .markets
                .splice(at..at, records.iter().map(|record| record.market));
        }
    }

    /// Rewrites every perp lane's domain slots to the dense numbering a store rebuild
    /// derives from the retained domain lists, resolving each record's current slot to its
    /// external index through `external_of(shard, slot)`.
    ///
    /// Patches allocate slab slots from a free list, so patched lanes diverge from the
    /// dense numbering; rebuilding stores without renumbering would route perp deltas to
    /// other accounts' domains.
    pub(crate) fn renumber_domain_offsets(
        &mut self,
        external_of: impl Fn(usize, usize) -> MarginDomainIndex,
    ) {
        let lookups: Vec<BTreeMap<(AccountIndex, MarginDomainIndex), usize>> = self
            .domains
            .iter()
            .map(|list| {
                list.iter()
                    .enumerate()
                    .map(|(offset, domain)| ((domain.account, domain.external_index), offset))
                    .collect()
            })
            .collect();
        for lanes in self.by_price.values_mut() {
            for (shard, lane) in lanes.iter_mut().enumerate() {
                for (account, slot) in lane
                    .perps
                    .accounts
                    .iter()
                    .zip(lane.perps.domain_offsets.iter_mut())
                {
                    let external = external_of(shard, *slot);
                    *slot = lookups[shard][&(*account, external)];
                }
            }
        }
    }

    /// Replaces one account's domain memberships in the retained build inputs. Infallible.
    pub(crate) fn replace_account_domains(
        &mut self,
        account: AccountIndex,
        domains: &[IndexedDomain],
    ) {
        let shard = shard_for(account, self.shard_count);
        let list = &mut self.domains[shard];
        let start = list.partition_point(|domain| domain.account < account);
        let end = list.partition_point(|domain| domain.account <= account);
        list.splice(start..end, domains.iter().cloned());
    }

    /// Replaces one account's position ownership records. Infallible.
    pub(crate) fn commit_positions(
        &mut self,
        account: AccountIndex,
        positions: Vec<PositionIndex>,
        digest: [u8; 32],
    ) {
        if let Some(previous) = self.positions_by_account.remove(&account) {
            for position in previous {
                self.position_owner.remove(&position);
            }
        }
        for position in &positions {
            self.position_owner.insert(*position, account);
        }
        if positions.is_empty() {
            self.positions_by_account.remove(&account);
        } else {
            self.positions_by_account.insert(account, positions);
        }
        self.account_digests[account as usize] = Some(digest);
    }
}

/// One account's records validated and grouped for lane insertion.
#[derive(Debug)]
pub(crate) struct CompiledAccount {
    pub account: AccountIndex,
    pub digest: [u8; 32],
    pub content_digest: [u8; 32],
    /// `(price, weighted amount)` sorted by price.
    pub balance_records: Vec<(PriceIndex, i128)>,
    /// Sorted by `(price, external domain index)`.
    pub perp_records: Vec<CompiledPerp>,
    /// Sorted by external domain index.
    pub domains: Vec<IndexedDomain>,
    pub positions: Vec<PositionIndex>,
}

#[derive(Debug)]
pub(crate) struct CompiledPerp {
    pub price: PriceIndex,
    pub domain: MarginDomainIndex,
    pub signed_size: SignedAmount,
    pub anchor_price: Option<Price>,
    pub market: PerpMarketIndex,
}

/// Validates one account patch and compiles it into splice-ready records, mirroring the
/// per-record rules of [`PortfolioIndex::build`] exactly.
///
/// # Errors
///
/// Returns the same errors full generation compilation would for equivalent records.
#[allow(clippy::too_many_lines)]
pub(crate) fn compile_account(
    registry: &AssetRegistry,
    account_count: usize,
    patch: &AccountPortfolio,
) -> Result<CompiledAccount> {
    let account = patch.account;
    validate_account(account, account_count)?;
    validate_source_version(account, patch.version)?;

    let mut positions = BTreeSet::new();
    let mut domain_keys = BTreeSet::new();
    let mut domains = Vec::with_capacity(patch.margin_domains.len());
    for domain in &patch.margin_domains {
        if domain.account != account {
            return Err(Error::PatchAccountMismatch { account });
        }
        validate_source_version(account, domain.source_version)?;
        validate_domain_snapshot(account, domain.domain, domain.snapshot)?;
        if !domain_keys.insert((account, domain.domain)) {
            return Err(Error::DuplicateMarginDomain {
                account,
                domain: domain.domain,
            });
        }
        domains.push(IndexedDomain {
            account,
            external_index: domain.domain,
            account_mode: domain.account_mode,
            margin_mode: domain.margin_mode,
            snapshot: domain.snapshot,
        });
    }
    domains.sort_unstable_by_key(|domain| domain.external_index);

    let mut balance_records = Vec::new();
    for balance in &patch.ethereum_balances {
        if balance.account != account {
            return Err(Error::PatchAccountMismatch { account });
        }
        let asset = validate_balance(
            registry,
            account_count,
            &mut positions,
            account,
            balance.asset,
            balance.amount,
            balance.position,
            balance.source_version,
            VenueKind::Ethereum,
        )?;
        if !asset.collateral_enabled {
            return Err(Error::CollateralAssetDisabled {
                asset: balance.asset,
            });
        }
        if balance.finality == Finality::Finalized {
            balance_records.push((
                asset.price_index,
                weighted_amount(balance.amount, asset.collateral_factor)?,
            ));
        }
    }
    for balance in &patch.hypercore_balances {
        if balance.account != account {
            return Err(Error::PatchAccountMismatch { account });
        }
        let asset = validate_balance(
            registry,
            account_count,
            &mut positions,
            account,
            balance.asset,
            balance.amount,
            balance.position,
            balance.source_version,
            VenueKind::HyperCore,
        )?;
        if balance.included_in_account_value {
            require_margin_domain(&domain_keys, account, balance.asset)?;
        } else {
            if !asset.collateral_enabled {
                return Err(Error::CollateralAssetDisabled {
                    asset: balance.asset,
                });
            }
            balance_records.push((
                asset.price_index,
                weighted_amount(balance.amount, asset.collateral_factor)?,
            ));
        }
    }
    balance_records.sort_by_key(|(price, _)| *price);

    let mut perp_records = Vec::with_capacity(patch.perp_positions.len());
    for position in &patch.perp_positions {
        if position.account != account {
            return Err(Error::PatchAccountMismatch { account });
        }
        validate_source_version(account, position.source_version)?;
        validate_position(position.position, &mut positions)?;
        let price_index = validate_perp_position(
            registry,
            account,
            position.market,
            position.signed_size,
            position.anchor_price,
        )?;
        if !domain_keys.contains(&(account, position.domain)) {
            return Err(Error::MissingMarginDomain {
                account,
                domain: position.domain,
            });
        }
        perp_records.push(CompiledPerp {
            price: price_index,
            domain: position.domain,
            signed_size: position.signed_size,
            anchor_price: position.anchor_price,
            market: position.market,
        });
    }
    perp_records.sort_by_key(|record| (record.price, record.domain));

    let digests = account_portfolio_digests(patch);
    Ok(CompiledAccount {
        account,
        digest: digests.account,
        content_digest: digests.content,
        balance_records,
        perp_records,
        domains,
        positions: positions.into_iter().collect(),
    })
}

const fn shard_for(account: AccountIndex, shard_count: usize) -> usize {
    account as usize % shard_count
}

fn validate_account(account: AccountIndex, account_count: usize) -> Result<()> {
    if account as usize >= account_count {
        return Err(Error::AccountOutOfRange { account });
    }
    Ok(())
}

fn validate_source_version(account: AccountIndex, version: StateVersion) -> Result<()> {
    if version == 0 {
        return Err(Error::InvalidSourceVersion { account });
    }
    Ok(())
}

fn validate_position(
    position: PositionIndex,
    positions: &mut BTreeSet<PositionIndex>,
) -> Result<()> {
    if !positions.insert(position) {
        return Err(Error::DuplicatePosition { position });
    }
    Ok(())
}

/// Rejects snapshots with a zero source sequence or values outside the engine bounds.
///
/// Shared by full generation compilation and per-account patch compilation so both apply
/// identical snapshot rules.
fn validate_domain_snapshot(
    account: AccountIndex,
    domain: MarginDomainIndex,
    snapshot: Option<HyperliquidSnapshot>,
) -> Result<()> {
    if let Some(snapshot) = snapshot
        && (snapshot.source_sequence == 0
            || snapshot.maintenance.raw() < 0
            || snapshot.maintenance.raw() > MAX_VALUE_RAW
            || snapshot.account_value.raw().unsigned_abs() > MAX_VALUE_RAW as u128)
    {
        return Err(Error::InconsistentMarginSnapshot { account, domain });
    }
    Ok(())
}

/// Validates one perp position's size, anchor, and market, returning its registry price lane.
///
/// Shared by full generation compilation and per-account patch compilation so both apply
/// identical position rules. Keep the market index so valuation can select the exact
/// maintenance tier at each mark price instead of freezing an average anchor rate.
fn validate_perp_position(
    registry: &AssetRegistry,
    account: AccountIndex,
    market_index: PerpMarketIndex,
    signed_size: SignedAmount,
    anchor_price: Option<Price>,
) -> Result<PriceIndex> {
    if signed_size.raw().unsigned_abs() > MAX_AMOUNT_RAW as u128 {
        return Err(Error::InvalidAmount {
            account,
            asset: market_index,
        });
    }
    if anchor_price.is_some_and(|price| price.raw() <= 0 || price.raw() > MAX_PRICE_RAW) {
        return Err(Error::InvalidPrice {
            price_index: market_index,
        });
    }
    let market = registry
        .perp_market(market_index)
        .ok_or(Error::UnknownPerpMarket {
            market: market_index,
        })?;
    Ok(market.price_index)
}

fn validate_amount(account: AccountIndex, asset: AssetIndex, amount: i128) -> Result<()> {
    if !(0..=MAX_AMOUNT_RAW).contains(&amount) {
        return Err(Error::InvalidAmount { account, asset });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_balance<'registry>(
    registry: &'registry AssetRegistry,
    account_count: usize,
    positions: &mut BTreeSet<PositionIndex>,
    account: AccountIndex,
    asset_index: AssetIndex,
    amount: Amount,
    position: PositionIndex,
    source_version: StateVersion,
    expected: VenueKind,
) -> Result<&'registry AssetMetadata> {
    validate_account(account, account_count)?;
    validate_source_version(account, source_version)?;
    validate_position(position, positions)?;
    validate_amount(account, asset_index, amount.raw())?;
    let asset = registry
        .asset(asset_index)
        .ok_or(Error::UnknownAsset { asset: asset_index })?;
    let venue = registry
        .venue(asset.venue)
        .ok_or_else(|| Error::InvalidAssetRegistry {
            reason: "asset references a missing venue".to_owned(),
        })?;
    if venue.kind != expected {
        return Err(Error::AssetVenueMismatch {
            asset: asset_index,
            expected,
        });
    }
    Ok(asset)
}

fn require_margin_domain(
    domain_keys: &BTreeSet<(AccountIndex, MarginDomainIndex)>,
    account: AccountIndex,
    asset: AssetIndex,
) -> Result<()> {
    if domain_keys
        .range((account, MarginDomainIndex::MIN)..=(account, MarginDomainIndex::MAX))
        .next()
        .is_none()
    {
        return Err(Error::IncludedBalanceWithoutMarginDomain { account, asset });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use tackt_primitives::accounts::AccountSubject;
    use uuid::Uuid;

    fn subject(account: AccountIndex) -> AccountSubject {
        AccountSubject::new(Uuid::from_u128(u128::from(account) + 1)).expect("subject")
    }
    use crate::processing::{
        AMOUNT_SCALE, AccountMode, Amount, EthereumBalance, HyperCoreBalance, HyperliquidSnapshot,
        MarginDomain, PRICE_SCALE, PerpPosition, SignedAmount, Value,
    };

    fn registry() -> AssetRegistry {
        AssetRegistry::bundled().expect("bundled registry")
    }

    #[test]
    fn compiles_balances_and_perps_into_separate_lanes() {
        let generation = PortfolioGeneration::new(
            1,
            vec![EthereumBalance {
                account: 0,
                asset: 2,
                amount: Amount::from_raw(AMOUNT_SCALE),
                finality: Finality::Finalized,
                position: 1,
                source_version: 1,
            }],
            vec![HyperCoreBalance {
                account: 0,
                asset: 1,
                amount: Amount::from_raw(AMOUNT_SCALE),
                included_in_account_value: true,
                position: 2,
                source_version: 1,
            }],
            vec![MarginDomain {
                account: 0,
                domain: 1,
                account_mode: AccountMode::Unified,
                margin_mode: MarginMode::Cross,
                snapshot: Some(HyperliquidSnapshot {
                    account_value: Value::from_raw(100 * PRICE_SCALE),
                    maintenance: Value::from_raw(10 * PRICE_SCALE),
                    source_sequence: 1,
                }),
                source_version: 1,
            }],
            vec![PerpPosition {
                account: 0,
                domain: 1,
                market: 1,
                signed_size: SignedAmount::from_raw(AMOUNT_SCALE),
                anchor_price: Some(Price::from_raw(50_000 * PRICE_SCALE)),
                position: 3,
                source_version: 1,
            }],
        )
        .with_account_subjects(BTreeMap::from([(0, subject(0))]));
        let index = PortfolioIndex::build(&generation, 1, 1, &registry()).expect("valid portfolio");

        assert_eq!(index.balance_count(), 1);
        assert_eq!(index.perp_position_count(), 1);
        assert_eq!(index.margin_domain_count(), 1);
        assert!(index.for_price(2).is_some());
        assert!(index.for_price(3).is_some());
    }

    #[test]
    fn rejects_zero_shard_count_instead_of_panicking() {
        let generation = PortfolioGeneration::new(
            1,
            Vec::new(),
            Vec::new(),
            vec![MarginDomain {
                account: 0,
                domain: 1,
                account_mode: AccountMode::Standard,
                margin_mode: MarginMode::Cross,
                snapshot: None,
                source_version: 1,
            }],
            Vec::new(),
        )
        .with_account_subjects(BTreeMap::from([(0, subject(0))]));
        let result = PortfolioIndex::build(&generation, 1, 0, &registry());

        assert!(matches!(
            result,
            Err(Error::InvalidShardCount { shard_count: 0 })
        ));
    }

    #[test]
    fn accepts_every_registered_perp_without_a_collateral_whitelist_check() {
        for market in [1, 2] {
            let generation = PortfolioGeneration::new(
                1,
                Vec::new(),
                Vec::new(),
                vec![MarginDomain {
                    account: 0,
                    domain: 1,
                    account_mode: AccountMode::Standard,
                    margin_mode: MarginMode::Hip3,
                    snapshot: None,
                    source_version: 1,
                }],
                vec![PerpPosition {
                    account: 0,
                    domain: 1,
                    market,
                    signed_size: SignedAmount::from_raw(AMOUNT_SCALE),
                    anchor_price: None,
                    position: market,
                    source_version: 1,
                }],
            )
            .with_account_subjects(BTreeMap::from([(0, subject(0))]));
            PortfolioIndex::build(&generation, 1, 1, &registry()).expect("registered perp");
        }
    }
}

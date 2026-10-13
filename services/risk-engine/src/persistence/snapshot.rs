//! Canonical binary snapshot encoding with a SHA-256 trailer.
//!
//! All fields are fixed-width little-endian in a fixed order; map sections iterate in key
//! order, so identical content always encodes to identical bytes. The file is the encoded
//! body followed by the SHA-256 digest of that body; decoding verifies the trailer before
//! parsing and rejects any surplus bytes, so a torn, truncated, or bit-flipped file is
//! discarded whole.

use std::collections::BTreeMap;
use std::io::Write;

use sha2::{Digest, Sha256};
use tackt_primitives::accounts::AccountSubject;
use uuid::Uuid;

use crate::processing::{
    AccountIndex, AccountMode, AccountPortfolio, Amount, EthereumBalance, Finality,
    HyperCoreBalance, HyperliquidSnapshot, MarginDomain, MarginMode, PerpPosition, Price,
    PriceIndex, PriceSourceKind, PriceUpdate, ProcessingEpoch, RiskState, SignedAmount,
    StateVersion, Value,
};

use super::{LedgerEntry, PersistenceError, Result};

const MAGIC: &[u8; 8] = b"RISKSNAP";
const TRAILER_LEN: usize = 32;

/// One complete persisted image: engine inputs plus the egress ledger, nothing derived.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Engine epoch at the last accepted event; informational, epochs restart on recovery.
    pub engine_epoch: ProcessingEpoch,
    /// Acknowledged position per input stream.
    pub cursors: BTreeMap<String, u64>,
    /// Max `StateVersion` across accepted generations and patches.
    pub version_high_water_mark: StateVersion,
    /// Version of the last installed generation; accounts stamped past it are patches.
    pub generation_version: StateVersion,
    /// Permanent dense-index to stable-subject bindings.
    pub account_subjects: BTreeMap<AccountIndex, AccountSubject>,
    /// Last accepted update per price index.
    pub prices: BTreeMap<PriceIndex, PriceUpdate>,
    /// Latest complete portfolio per account.
    pub accounts: BTreeMap<AccountIndex, AccountPortfolio>,
    /// Accounts removed by an applied empty patch, at the removing patch's version.
    pub removed: BTreeMap<AccountIndex, StateVersion>,
    /// Last published risk state per account.
    pub ledger: BTreeMap<AccountIndex, LedgerEntry>,
}

/// Borrowed view of one snapshot's content, encodable without copying the underlying maps.
///
/// The live fold and ledger encode through this view straight to the staged snapshot file,
/// so writing a snapshot never clones account state or builds the full image in memory.
/// [`Snapshot`] remains the owned decode-side type; both encode byte-identically because
/// [`Snapshot::encode`] delegates here.
#[derive(Debug, Clone, Copy)]
pub struct SnapshotView<'fold> {
    /// Engine epoch at the last accepted event; informational, epochs restart on recovery.
    pub engine_epoch: ProcessingEpoch,
    /// Acknowledged position per input stream.
    pub cursors: &'fold BTreeMap<String, u64>,
    /// Max `StateVersion` across accepted generations and patches.
    pub version_high_water_mark: StateVersion,
    /// Version of the last installed generation; accounts stamped past it are patches.
    pub generation_version: StateVersion,
    /// Permanent dense-index to stable-subject bindings.
    pub account_subjects: &'fold BTreeMap<AccountIndex, AccountSubject>,
    /// Last accepted update per price index.
    pub prices: &'fold BTreeMap<PriceIndex, PriceUpdate>,
    /// Latest complete portfolio per account.
    pub accounts: &'fold BTreeMap<AccountIndex, AccountPortfolio>,
    /// Accounts removed by an applied empty patch, at the removing patch's version.
    pub removed: &'fold BTreeMap<AccountIndex, StateVersion>,
    /// Last published risk state per account.
    pub ledger: &'fold BTreeMap<AccountIndex, LedgerEntry>,
}

impl SnapshotView<'_> {
    /// Streams the canonical encoding, trailer included, into `writer`.
    ///
    /// The SHA-256 trailer is computed on the fly, so encoding needs no intermediate
    /// buffer beyond the writer's own.
    ///
    /// # Errors
    ///
    /// Returns an error when a section exceeds the format's `u32` count capacity or the
    /// writer fails.
    pub fn encode_to<W: Write>(&self, writer: W) -> Result<()> {
        let mut writer = HashingWriter::new(writer);
        writer.bytes(MAGIC)?;
        put_u64(&mut writer, self.engine_epoch)?;

        put_count(&mut writer, self.cursors.len())?;
        for (stream, sequence) in self.cursors {
            put_str(&mut writer, stream)?;
            put_u64(&mut writer, *sequence)?;
        }

        put_u64(&mut writer, self.version_high_water_mark)?;
        put_u64(&mut writer, self.generation_version)?;

        put_count(&mut writer, self.account_subjects.len())?;
        for (account, subject) in self.account_subjects {
            put_u32(&mut writer, *account)?;
            writer.bytes(subject.user_id().as_bytes())?;
        }

        put_count(&mut writer, self.prices.len())?;
        for update in self.prices.values() {
            put_price_update(&mut writer, update)?;
        }

        put_count(&mut writer, self.accounts.len())?;
        for portfolio in self.accounts.values() {
            put_portfolio(&mut writer, portfolio)?;
        }

        put_count(&mut writer, self.removed.len())?;
        for (account, version) in self.removed {
            put_u32(&mut writer, *account)?;
            put_u64(&mut writer, *version)?;
        }

        put_count(&mut writer, self.ledger.len())?;
        for (account, entry) in self.ledger {
            put_u32(&mut writer, *account)?;
            writer.bytes(&[risk_state_code(entry.risk_state)])?;
            put_u64(&mut writer, entry.state_version)?;
        }

        writer.finish()
    }
}

impl Snapshot {
    /// Borrows this snapshot as an encodable view.
    #[must_use]
    pub fn view(&self) -> SnapshotView<'_> {
        SnapshotView {
            engine_epoch: self.engine_epoch,
            cursors: &self.cursors,
            version_high_water_mark: self.version_high_water_mark,
            generation_version: self.generation_version,
            account_subjects: &self.account_subjects,
            prices: &self.prices,
            accounts: &self.accounts,
            removed: &self.removed,
            ledger: &self.ledger,
        }
    }

    /// Encodes the snapshot into its canonical bytes, trailer included.
    ///
    /// # Errors
    ///
    /// Returns an error when a section exceeds the format's `u32` count capacity.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        self.view().encode_to(&mut bytes)?;
        Ok(bytes)
    }

    /// Decodes and verifies canonical snapshot bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when the trailer digest, framing, or any field fails verification.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let body_len = bytes
            .len()
            .checked_sub(TRAILER_LEN)
            .ok_or_else(|| decode_error("file is shorter than the digest trailer"))?;
        let (body, trailer) = bytes.split_at(body_len);
        let digest: [u8; TRAILER_LEN] = Sha256::digest(body).into();
        if digest != trailer {
            return Err(decode_error("trailer digest mismatch"));
        }

        let mut reader = Reader::new(body);
        if reader.take(MAGIC.len())? != MAGIC {
            return Err(decode_error("bad magic"));
        }
        let engine_epoch = reader.u64()?;

        let mut cursors = BTreeMap::new();
        for _ in 0..reader.u32()? {
            let stream = read_str(&mut reader)?;
            let sequence = reader.u64()?;
            insert_ascending(&mut cursors, stream, sequence, "stream cursor")?;
        }

        let version_high_water_mark = reader.u64()?;
        let generation_version = reader.u64()?;

        let mut account_subjects = BTreeMap::new();
        for _ in 0..reader.u32()? {
            let account = reader.u32()?;
            let mut user_id_bytes = [0_u8; 16];
            user_id_bytes.copy_from_slice(reader.take(16)?);
            let user_id = Uuid::from_bytes(user_id_bytes);
            let subject = AccountSubject::new(user_id)
                .map_err(|_| decode_error("invalid stable account subject"))?;
            insert_ascending(&mut account_subjects, account, subject, "account subject")?;
        }

        let mut prices = BTreeMap::new();
        for _ in 0..reader.u32()? {
            let update = read_price_update(&mut reader)?;
            insert_ascending(&mut prices, update.price_index, update, "price index")?;
        }

        let mut accounts = BTreeMap::new();
        for _ in 0..reader.u32()? {
            let portfolio = read_portfolio(&mut reader)?;
            insert_ascending(&mut accounts, portfolio.account, portfolio, "account")?;
        }

        let mut removed = BTreeMap::new();
        for _ in 0..reader.u32()? {
            let account = reader.u32()?;
            let version = reader.u64()?;
            insert_ascending(&mut removed, account, version, "removed account")?;
        }

        let mut ledger = BTreeMap::new();
        for _ in 0..reader.u32()? {
            let account = reader.u32()?;
            let risk_state = risk_state_from(reader.u8()?)?;
            let state_version = reader.u64()?;
            insert_ascending(
                &mut ledger,
                account,
                LedgerEntry {
                    risk_state,
                    state_version,
                },
                "ledger account",
            )?;
        }

        reader.finish()?;
        Ok(Self {
            engine_epoch,
            cursors,
            version_high_water_mark,
            generation_version,
            account_subjects,
            prices,
            accounts,
            removed,
            ledger,
        })
    }
}

fn decode_error(reason: &str) -> PersistenceError {
    PersistenceError::Decode {
        reason: reason.to_owned(),
    }
}

/// Inserts one decoded map entry, rejecting out-of-order or duplicate keys.
///
/// Canonical encoding writes map sections in strictly ascending key order; enforcing the
/// same order on decode means a digest-valid but non-canonical file (which no writer of
/// this format produces) is rejected instead of silently re-encoding to different bytes.
fn insert_ascending<K: Ord, V>(
    map: &mut BTreeMap<K, V>,
    key: K,
    value: V,
    section: &str,
) -> Result<()> {
    if map.last_key_value().is_some_and(|(last, _)| *last >= key) {
        return Err(decode_error(&format!("{section} out of order")));
    }
    map.insert(key, value);
    Ok(())
}

struct Reader<'bytes> {
    bytes: &'bytes [u8],
    position: usize,
}

impl<'bytes> Reader<'bytes> {
    const fn new(bytes: &'bytes [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'bytes [u8]> {
        let end = self
            .position
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| decode_error("unexpected end of snapshot"))?;
        let slice = &self.bytes[self.position..end];
        self.position = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        let bytes: [u8; 4] = self.take(4)?.try_into().expect("slice length is fixed");
        Ok(u32::from_le_bytes(bytes))
    }

    fn u64(&mut self) -> Result<u64> {
        let bytes: [u8; 8] = self.take(8)?.try_into().expect("slice length is fixed");
        Ok(u64::from_le_bytes(bytes))
    }

    fn i128(&mut self) -> Result<i128> {
        let bytes: [u8; 16] = self.take(16)?.try_into().expect("slice length is fixed");
        Ok(i128::from_le_bytes(bytes))
    }

    fn bool(&mut self) -> Result<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(decode_error("invalid boolean")),
        }
    }

    fn finish(&self) -> Result<()> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(decode_error("surplus bytes after the encoded body"))
        }
    }
}

/// Tees every written body byte into a SHA-256 hasher, then appends the digest trailer.
struct HashingWriter<W: Write> {
    inner: W,
    hasher: Sha256,
}

impl<W: Write> HashingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
        }
    }

    fn bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.hasher.update(bytes);
        self.inner
            .write_all(bytes)
            .map_err(|error| PersistenceError::Encode {
                reason: format!("write failed: {error}"),
            })
    }

    /// Writes the digest trailer of everything written so far and flushes.
    fn finish(mut self) -> Result<()> {
        let digest: [u8; TRAILER_LEN] = self.hasher.finalize().into();
        self.inner
            .write_all(&digest)
            .and_then(|()| self.inner.flush())
            .map_err(|error| PersistenceError::Encode {
                reason: format!("write failed: {error}"),
            })
    }
}

fn put_u32<W: Write>(writer: &mut HashingWriter<W>, value: u32) -> Result<()> {
    writer.bytes(&value.to_le_bytes())
}

fn put_u64<W: Write>(writer: &mut HashingWriter<W>, value: u64) -> Result<()> {
    writer.bytes(&value.to_le_bytes())
}

fn put_i128<W: Write>(writer: &mut HashingWriter<W>, value: i128) -> Result<()> {
    writer.bytes(&value.to_le_bytes())
}

fn put_count<W: Write>(writer: &mut HashingWriter<W>, count: usize) -> Result<()> {
    let count = u32::try_from(count).map_err(|_| PersistenceError::Encode {
        reason: "section exceeds u32 count capacity".to_owned(),
    })?;
    put_u32(writer, count)
}

fn put_str<W: Write>(writer: &mut HashingWriter<W>, value: &str) -> Result<()> {
    put_count(writer, value.len())?;
    writer.bytes(value.as_bytes())
}

fn read_str(reader: &mut Reader<'_>) -> Result<String> {
    let length = reader.u32()?;
    let raw = reader.take(length as usize)?;
    String::from_utf8(raw.to_vec()).map_err(|_| decode_error("invalid utf-8 stream name"))
}

fn put_price_update<W: Write>(writer: &mut HashingWriter<W>, update: &PriceUpdate) -> Result<()> {
    put_u32(writer, update.price_index)?;
    writer.bytes(&[source_code(update.source)])?;
    put_u64(writer, update.source_sequence)?;
    put_i128(writer, update.price.raw())
}

fn read_price_update(reader: &mut Reader<'_>) -> Result<PriceUpdate> {
    Ok(PriceUpdate {
        price_index: reader.u32()?,
        source: source_from(reader.u8()?)?,
        source_sequence: reader.u64()?,
        price: Price::from_raw(reader.i128()?),
    })
}

fn put_portfolio<W: Write>(
    writer: &mut HashingWriter<W>,
    portfolio: &AccountPortfolio,
) -> Result<()> {
    put_u32(writer, portfolio.account)?;
    writer.bytes(portfolio.account_subject.user_id().as_bytes())?;
    put_u64(writer, portfolio.version)?;

    put_count(writer, portfolio.ethereum_balances.len())?;
    for balance in &portfolio.ethereum_balances {
        put_u32(writer, balance.account)?;
        put_u32(writer, balance.asset)?;
        put_i128(writer, balance.amount.raw())?;
        writer.bytes(&[finality_code(balance.finality)])?;
        put_u32(writer, balance.position)?;
        put_u64(writer, balance.source_version)?;
    }

    put_count(writer, portfolio.hypercore_balances.len())?;
    for balance in &portfolio.hypercore_balances {
        put_u32(writer, balance.account)?;
        put_u32(writer, balance.asset)?;
        put_i128(writer, balance.amount.raw())?;
        writer.bytes(&[u8::from(balance.included_in_account_value)])?;
        put_u32(writer, balance.position)?;
        put_u64(writer, balance.source_version)?;
    }

    put_count(writer, portfolio.margin_domains.len())?;
    for domain in &portfolio.margin_domains {
        put_u32(writer, domain.account)?;
        put_u32(writer, domain.domain)?;
        writer.bytes(&[
            account_mode_code(domain.account_mode),
            margin_mode_code(domain.margin_mode),
        ])?;
        match domain.snapshot {
            Some(snapshot) => {
                writer.bytes(&[1])?;
                put_i128(writer, snapshot.account_value.raw())?;
                put_i128(writer, snapshot.maintenance.raw())?;
                put_u64(writer, snapshot.source_sequence)?;
            }
            None => writer.bytes(&[0])?,
        }
        put_u64(writer, domain.source_version)?;
    }

    put_count(writer, portfolio.perp_positions.len())?;
    for position in &portfolio.perp_positions {
        put_u32(writer, position.account)?;
        put_u32(writer, position.domain)?;
        put_u32(writer, position.market)?;
        put_i128(writer, position.signed_size.raw())?;
        match position.anchor_price {
            Some(anchor) => {
                writer.bytes(&[1])?;
                put_i128(writer, anchor.raw())?;
            }
            None => writer.bytes(&[0])?,
        }
        put_u32(writer, position.position)?;
        put_u64(writer, position.source_version)?;
    }
    Ok(())
}

fn read_portfolio(reader: &mut Reader<'_>) -> Result<AccountPortfolio> {
    let account = reader.u32()?;
    let account_subject = AccountSubject::new(Uuid::from_bytes(
        reader
            .take(16)?
            .try_into()
            .expect("UUID byte length is fixed"),
    ))
    .map_err(|_| decode_error("invalid portfolio account subject"))?;
    let version = reader.u64()?;
    // Every record in a canonical portfolio carries the portfolio's own account; a
    // mismatch would make fold bookkeeping disagree with engine attribution.
    let owned = |record: AccountIndex| {
        if record == account {
            Ok(record)
        } else {
            Err(decode_error(
                "record account differs from portfolio account",
            ))
        }
    };

    let mut ethereum_balances = Vec::new();
    for _ in 0..reader.u32()? {
        ethereum_balances.push(EthereumBalance {
            account: owned(reader.u32()?)?,
            asset: reader.u32()?,
            amount: Amount::from_raw(reader.i128()?),
            finality: finality_from(reader.u8()?)?,
            position: reader.u32()?,
            source_version: reader.u64()?,
        });
    }

    let mut hypercore_balances = Vec::new();
    for _ in 0..reader.u32()? {
        hypercore_balances.push(HyperCoreBalance {
            account: owned(reader.u32()?)?,
            asset: reader.u32()?,
            amount: Amount::from_raw(reader.i128()?),
            included_in_account_value: reader.bool()?,
            position: reader.u32()?,
            source_version: reader.u64()?,
        });
    }

    let mut margin_domains = Vec::new();
    for _ in 0..reader.u32()? {
        let account = owned(reader.u32()?)?;
        let domain = reader.u32()?;
        let account_mode = account_mode_from(reader.u8()?)?;
        let margin_mode = margin_mode_from(reader.u8()?)?;
        let snapshot = if reader.bool()? {
            Some(HyperliquidSnapshot {
                account_value: Value::from_raw(reader.i128()?),
                maintenance: Value::from_raw(reader.i128()?),
                source_sequence: reader.u64()?,
            })
        } else {
            None
        };
        margin_domains.push(MarginDomain {
            account,
            domain,
            account_mode,
            margin_mode,
            snapshot,
            source_version: reader.u64()?,
        });
    }

    let mut perp_positions = Vec::new();
    for _ in 0..reader.u32()? {
        let account = owned(reader.u32()?)?;
        let domain = reader.u32()?;
        let market = reader.u32()?;
        let signed_size = SignedAmount::from_raw(reader.i128()?);
        let anchor_price = if reader.bool()? {
            Some(Price::from_raw(reader.i128()?))
        } else {
            None
        };
        perp_positions.push(PerpPosition {
            account,
            domain,
            market,
            signed_size,
            anchor_price,
            position: reader.u32()?,
            source_version: reader.u64()?,
        });
    }

    Ok(AccountPortfolio {
        account,
        account_subject,
        version,
        ethereum_balances,
        hypercore_balances,
        margin_domains,
        perp_positions,
    })
}

const fn source_code(source: PriceSourceKind) -> u8 {
    match source {
        PriceSourceKind::Ethereum => 0,
        PriceSourceKind::Hyperliquid => 1,
    }
}

fn source_from(code: u8) -> Result<PriceSourceKind> {
    match code {
        0 => Ok(PriceSourceKind::Ethereum),
        1 => Ok(PriceSourceKind::Hyperliquid),
        _ => Err(decode_error("invalid price source")),
    }
}

const fn finality_code(finality: Finality) -> u8 {
    match finality {
        Finality::Finalized => 0,
        Finality::InFlight => 1,
        Finality::Reverted => 2,
    }
}

fn finality_from(code: u8) -> Result<Finality> {
    match code {
        0 => Ok(Finality::Finalized),
        1 => Ok(Finality::InFlight),
        2 => Ok(Finality::Reverted),
        _ => Err(decode_error("invalid finality")),
    }
}

const fn account_mode_code(mode: AccountMode) -> u8 {
    match mode {
        AccountMode::Standard => 0,
        AccountMode::Unified => 1,
    }
}

fn account_mode_from(code: u8) -> Result<AccountMode> {
    match code {
        0 => Ok(AccountMode::Standard),
        1 => Ok(AccountMode::Unified),
        _ => Err(decode_error("invalid account mode")),
    }
}

const fn margin_mode_code(mode: MarginMode) -> u8 {
    match mode {
        MarginMode::Cross => 0,
        MarginMode::Isolated => 1,
        MarginMode::Hip3 => 2,
    }
}

fn margin_mode_from(code: u8) -> Result<MarginMode> {
    match code {
        0 => Ok(MarginMode::Cross),
        1 => Ok(MarginMode::Isolated),
        2 => Ok(MarginMode::Hip3),
        _ => Err(decode_error("invalid margin mode")),
    }
}

const fn risk_state_code(state: RiskState) -> u8 {
    match state {
        RiskState::Healthy => 0,
        RiskState::Watch => 1,
        RiskState::Liquidatable => 2,
        RiskState::DataIncomplete => 3,
    }
}

fn risk_state_from(code: u8) -> Result<RiskState> {
    match code {
        0 => Ok(RiskState::Healthy),
        1 => Ok(RiskState::Watch),
        2 => Ok(RiskState::Liquidatable),
        3 => Ok(RiskState::DataIncomplete),
        _ => Err(decode_error("invalid risk state")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Snapshot {
        let account_subject =
            AccountSubject::new(Uuid::from_u128(5)).expect("non-zero account subject");
        let portfolio = AccountPortfolio {
            account: 4,
            account_subject,
            version: 9,
            ethereum_balances: vec![EthereumBalance {
                account: 4,
                asset: 2,
                amount: Amount::from_raw(1_000_000_000),
                finality: Finality::Finalized,
                position: 1,
                source_version: 5,
            }],
            hypercore_balances: vec![HyperCoreBalance {
                account: 4,
                asset: 1,
                amount: Amount::from_raw(2_000_000_000),
                included_in_account_value: true,
                position: 2,
                source_version: 5,
            }],
            margin_domains: vec![
                MarginDomain {
                    account: 4,
                    domain: 1,
                    account_mode: AccountMode::Unified,
                    margin_mode: MarginMode::Cross,
                    snapshot: Some(HyperliquidSnapshot {
                        account_value: Value::from_raw(-3),
                        maintenance: Value::from_raw(4),
                        source_sequence: 6,
                    }),
                    source_version: 5,
                },
                MarginDomain {
                    account: 4,
                    domain: 2,
                    account_mode: AccountMode::Standard,
                    margin_mode: MarginMode::Hip3,
                    snapshot: None,
                    source_version: 5,
                },
            ],
            perp_positions: vec![
                PerpPosition {
                    account: 4,
                    domain: 1,
                    market: 1,
                    signed_size: SignedAmount::from_raw(-7),
                    anchor_price: Some(Price::from_raw(8)),
                    position: 3,
                    source_version: 5,
                },
                PerpPosition {
                    account: 4,
                    domain: 2,
                    market: 2,
                    signed_size: SignedAmount::from_raw(7),
                    anchor_price: None,
                    position: 4,
                    source_version: 5,
                },
            ],
        };
        Snapshot {
            engine_epoch: 42,
            cursors: BTreeMap::from([("prices".to_owned(), 10), ("patches".to_owned(), 20)]),
            version_high_water_mark: 9,
            generation_version: 5,
            account_subjects: BTreeMap::from([(4, account_subject)]),
            prices: BTreeMap::from([(
                3,
                PriceUpdate {
                    price_index: 3,
                    source: PriceSourceKind::Hyperliquid,
                    source_sequence: 11,
                    price: Price::from_raw(12),
                },
            )]),
            accounts: BTreeMap::from([(4, portfolio)]),
            removed: BTreeMap::from([(6, 7)]),
            ledger: BTreeMap::from([(
                4,
                LedgerEntry {
                    risk_state: RiskState::Watch,
                    state_version: 9,
                },
            )]),
        }
    }

    #[test]
    fn round_trip_preserves_every_field() {
        let snapshot = sample();
        let bytes = snapshot.encode().expect("encode");
        let decoded = Snapshot::decode(&bytes).expect("decode");
        assert_eq!(decoded, snapshot);
    }

    #[test]
    fn identical_content_encodes_identically() {
        let first = sample().encode().expect("encode");
        let second = sample().encode().expect("encode");
        assert_eq!(first, second);
    }

    #[test]
    fn any_flipped_byte_is_rejected() {
        let bytes = sample().encode().expect("encode");
        for position in [0, 8, bytes.len() / 2, bytes.len() - 1] {
            let mut corrupt = bytes.clone();
            corrupt[position] ^= 0x40;
            assert!(
                Snapshot::decode(&corrupt).is_err(),
                "flip at {position} was accepted"
            );
        }
    }

    #[test]
    fn truncation_and_surplus_are_rejected() {
        let bytes = sample().encode().expect("encode");
        assert!(Snapshot::decode(&bytes[..bytes.len() - 1]).is_err());
        assert!(Snapshot::decode(&bytes[..10]).is_err());
        assert!(Snapshot::decode(&[]).is_err());
        let mut surplus = bytes;
        surplus.push(0);
        assert!(Snapshot::decode(&surplus).is_err());
    }

    #[test]
    fn every_enum_code_round_trips() {
        for source in [PriceSourceKind::Ethereum, PriceSourceKind::Hyperliquid] {
            assert_eq!(source_from(source_code(source)).expect("source"), source);
        }
        for finality in [Finality::Finalized, Finality::InFlight, Finality::Reverted] {
            assert_eq!(
                finality_from(finality_code(finality)).expect("finality"),
                finality
            );
        }
        for mode in [AccountMode::Standard, AccountMode::Unified] {
            assert_eq!(
                account_mode_from(account_mode_code(mode)).expect("mode"),
                mode
            );
        }
        for mode in [MarginMode::Cross, MarginMode::Isolated, MarginMode::Hip3] {
            assert_eq!(
                margin_mode_from(margin_mode_code(mode)).expect("mode"),
                mode
            );
        }
        for state in [
            RiskState::Healthy,
            RiskState::Watch,
            RiskState::Liquidatable,
            RiskState::DataIncomplete,
        ] {
            assert_eq!(
                risk_state_from(risk_state_code(state)).expect("state"),
                state
            );
        }
    }

    #[test]
    fn out_of_order_map_sections_are_rejected() {
        let mut snapshot = sample();
        snapshot.ledger.insert(
            2,
            LedgerEntry {
                risk_state: RiskState::Healthy,
                state_version: 1,
            },
        );
        let bytes = snapshot.encode().expect("encode");

        // The ledger is the final body section: swap its two 13-byte entries in place and
        // restamp the trailer, producing a digest-valid but non-canonical file.
        let body_len = bytes.len() - TRAILER_LEN;
        let mut swapped = bytes[..body_len].to_vec();
        let second = body_len - 13;
        let first = second - 13;
        swapped[first..body_len].rotate_left(13);
        let digest: [u8; TRAILER_LEN] = Sha256::digest(&swapped).into();
        swapped.extend_from_slice(&digest);

        let error = Snapshot::decode(&swapped).expect_err("non-canonical order accepted");
        assert!(error.to_string().contains("ledger account out of order"));
    }

    #[test]
    fn record_account_mismatch_is_rejected() {
        let mut snapshot = sample();
        snapshot
            .accounts
            .get_mut(&4)
            .expect("sample account")
            .ethereum_balances[0]
            .account = 5;
        let bytes = snapshot.encode().expect("encode");

        let error = Snapshot::decode(&bytes).expect_err("mismatched record accepted");
        assert!(
            error
                .to_string()
                .contains("record account differs from portfolio account")
        );
    }
}

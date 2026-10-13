use anyhow::{Context, Result, bail};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashMap};
use tackt_primitives::{EventKind, Observation, Provenance, Source};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HyperliquidStream {
    Trades,
    Events,
}

impl HyperliquidStream {
    pub const fn observation_stream(self) -> tackt_nats::HyperliquidObservationStream {
        match self {
            Self::Trades => tackt_nats::HyperliquidObservationStream::Trades,
            Self::Events => tackt_nats::HyperliquidObservationStream::Events,
        }
    }
    pub const fn name(self) -> &'static str {
        match self {
            Self::Trades => "trades",
            Self::Events => "events",
        }
    }

    pub const fn method(self) -> &'static str {
        match self {
            Self::Trades => "StreamDataBytes/TRADES",
            Self::Events => "StreamDataBytes/EVENTS",
        }
    }
}

#[derive(Debug)]
pub struct NormalizedBlock {
    pub block_number: u64,
    pub observations: Vec<Observation>,
    pub dirty_accounts: Vec<String>,
}

pub fn normalize_block(
    stream: HyperliquidStream,
    grpc_block_number: u64,
    payload: &[u8],
    accounts: &HashMap<String, Uuid>,
    endpoint: &str,
) -> Result<NormalizedBlock> {
    let raw: Value = serde_json::from_slice(payload).context("decode Hyperliquid stream JSON")?;
    normalize_parsed_block(stream, grpc_block_number, &raw, accounts, endpoint)
}

/// RPC backfill already owns parsed JSON; normalize it without a serialization
/// and parsing round trip. Live gRPC payloads use the byte entry point above.
pub(super) fn normalize_parsed_block(
    stream: HyperliquidStream,
    grpc_block_number: u64,
    raw: &Value,
    accounts: &HashMap<String, Uuid>,
    endpoint: &str,
) -> Result<NormalizedBlock> {
    let block_number = raw["block_number"]
        .as_u64()
        .context("Hyperliquid stream block missing block_number")?;
    if block_number != grpc_block_number {
        bail!(
            "Hyperliquid {} protobuf block {grpc_block_number} disagrees with JSON block {block_number}",
            stream.name()
        )
    }
    let events = raw["events"]
        .as_array()
        .context("Hyperliquid stream block missing events")?;
    let mut observations = Vec::new();
    let mut dirty = BTreeSet::new();
    // These streams are global and unfiltered: no record from an unregistered
    // account can fail this block.
    for event in events {
        normalize_record(
            stream,
            event,
            block_number,
            accounts,
            endpoint,
            &mut observations,
            &mut dirty,
        );
    }
    disambiguate_collisions(&mut observations);
    Ok(NormalizedBlock {
        block_number,
        observations,
        dirty_accounts: dirty.into_iter().collect(),
    })
}

/// The only degrade-to-dirty path: identity is read leniently, so an unmatched
/// record never reaches a strict parse, and a matched record that fails to parse
/// is quarantined instead of failing the block. Bailing would wedge the stream —
/// the supervisor reconnects, replays the same block, and fails again forever —
/// so portfolio reconciliation recovers the affected accounts instead.
fn normalize_record(
    stream: HyperliquidStream,
    event: &Value,
    block: u64,
    accounts: &HashMap<String, Uuid>,
    endpoint: &str,
    observations: &mut Vec<Observation>,
    dirty: &mut BTreeSet<String>,
) {
    let mut affected = touched_accounts(stream, event, accounts);
    if affected.is_empty() {
        return;
    }
    let mut parsed = Vec::new();
    let outcome = match stream {
        HyperliquidStream::Trades => {
            normalize_trade(event, block, accounts, endpoint, &mut parsed, dirty)
        }
        HyperliquidStream::Events => {
            normalize_event(event, block, accounts, endpoint, &mut parsed, dirty)
        }
    };
    match outcome {
        Ok(()) => observations.append(&mut parsed),
        Err(error) => {
            metrics::counter!(
                "indexer_hyperliquid_records_quarantined_total",
                "stream" => stream.name()
            )
            .increment(1);
            tracing::warn!(
                stream = stream.name(),
                block,
                kind = record_kind(stream, event),
                accounts = affected.len(),
                %error,
                "unparsable Hyperliquid record; deferring to reconciliation"
            );
            dirty.append(&mut affected);
        }
    }
}

/// Registered accounts this raw record could be about, using lenient reads only.
fn touched_accounts(
    stream: HyperliquidStream,
    event: &Value,
    accounts: &HashMap<String, Uuid>,
) -> BTreeSet<String> {
    if stream == HyperliquidStream::Trades
        && let Some(address) = event.get(0).and_then(Value::as_str)
    {
        // Every exchange fill takes this path; only an unexpected shape pays for
        // the deep scan below.
        let address = address.to_lowercase();
        return accounts
            .contains_key(&address)
            .then_some(address)
            .into_iter()
            .collect();
    }
    let mut found = BTreeSet::new();
    collect_registered_addresses(event, accounts, &mut found);
    found
}

fn record_kind(stream: HyperliquidStream, event: &Value) -> &str {
    match stream {
        HyperliquidStream::Trades => "trade",
        HyperliquidStream::Events => event["inner"]
            .as_object()
            .filter(|inner| inner.len() == 1)
            .and_then(|inner| inner.keys().next())
            .map_or("event", String::as_str),
    }
}

/// Lenient identity read for one item inside a record.
fn registered_account(
    value: Option<&Value>,
    accounts: &HashMap<String, Uuid>,
) -> Option<(String, Uuid)> {
    let account = value?.as_str()?.to_lowercase();
    let user = accounts.get(&account).copied()?;
    Some((account, user))
}

/// Legacy REST parity requires first occurrences to keep their bare IDs; only
/// genuine within-block collisions (self-trade legs, ledger events sharing a tx
/// hash) get an ordinal suffix, in block order, instead of being dropped by the
/// event-id upsert.
fn disambiguate_collisions(observations: &mut [Observation]) {
    let mut seen: HashMap<String, u32> = HashMap::new();
    for observation in observations {
        let occurrence = seen.entry(observation.event_id.clone()).or_default();
        *occurrence += 1;
        if *occurrence > 1 {
            let source_id = format!("{}:{occurrence}", observation.provenance.source_id);
            observation.event_id = Observation::stable_id(
                &Source::Hyperliquid,
                &observation.kind,
                &[
                    observation
                        .account
                        .as_ref()
                        .map_or("", tackt_primitives::accounts::SourceAccount::external_id),
                    &source_id,
                ],
            );
            observation.provenance.source_id = source_id;
        }
    }
}

fn normalize_trade(
    event: &Value,
    block: u64,
    accounts: &HashMap<String, Uuid>,
    endpoint: &str,
    observations: &mut Vec<Observation>,
    dirty: &mut BTreeSet<String>,
) -> Result<()> {
    let Some((account, user)) = registered_account(event.get(0), accounts) else {
        return Ok(());
    };
    let tuple = event
        .as_array()
        .filter(|tuple| tuple.len() == 2)
        .context("Hyperliquid trade event must be [user, fill]")?;
    let fill = &tuple[1];
    let tid = identifier(&fill["tid"]).context("Hyperliquid trade missing tid")?;
    let coin = required_string(fill, "coin", "Hyperliquid trade")?;
    let size = validate_decimal(
        required_string(fill, "sz", "Hyperliquid trade")?,
        "Hyperliquid trade sz",
    )?;
    let timestamp = millis(
        fill["time"]
            .as_i64()
            .context("Hyperliquid trade missing time")?,
    )?;
    observations.push(observation(
        user,
        &account,
        EventKind::HyperliquidFill,
        coin,
        Some(size),
        fill.clone(),
        endpoint,
        HyperliquidStream::Trades.method(),
        &tid,
        timestamp,
        block,
    )?);
    let fee = validate_decimal(
        required_string(fill, "fee", "Hyperliquid trade")?,
        "Hyperliquid trade fee",
    )?;
    if !decimal_is_zero(fee) {
        let fee_token = required_string(fill, "feeToken", "Hyperliquid trade")?;
        observations.push(observation(
            user,
            &account,
            EventKind::HyperliquidFee,
            fee_token,
            Some(fee),
            fill.clone(),
            endpoint,
            HyperliquidStream::Trades.method(),
            &format!("{tid}:fee"),
            timestamp,
            block,
        )?);
    }
    dirty.insert(account);
    Ok(())
}

fn normalize_event(
    event: &Value,
    block: u64,
    accounts: &HashMap<String, Uuid>,
    endpoint: &str,
    observations: &mut Vec<Observation>,
    dirty: &mut BTreeSet<String>,
) -> Result<()> {
    let timestamp = parse_event_time(
        event["time"]
            .as_str()
            .context("Hyperliquid event missing time")?,
    )?;
    let inner = event["inner"]
        .as_object()
        .context("Hyperliquid event missing inner object")?;
    if inner.len() != 1 {
        bail!("Hyperliquid event inner object must contain one variant")
    }
    let (kind, value) = inner.iter().next().expect("one inner variant");
    match kind.as_str() {
        "Funding" => normalize_funding(
            value,
            event,
            timestamp,
            block,
            accounts,
            endpoint,
            observations,
            dirty,
        ),
        "LedgerUpdate" => normalize_ledger(
            value,
            event,
            timestamp,
            block,
            accounts,
            endpoint,
            observations,
            dirty,
        ),
        "CDeposit" | "CWithdrawal" | "Delegation" => normalize_single_user_event(
            kind,
            value,
            event,
            timestamp,
            block,
            accounts,
            endpoint,
            observations,
            dirty,
        ),
        "ValidatorRewards" => normalize_validator_rewards(
            value,
            event,
            timestamp,
            block,
            accounts,
            endpoint,
            observations,
            dirty,
        ),
        "GossipPriorityAuctionRestart" => Ok(()),
        other => bail!("unsupported Hyperliquid event variant {other}"),
    }
}

#[allow(clippy::too_many_arguments)]
fn normalize_funding(
    funding: &Value,
    raw: &Value,
    timestamp: DateTime<Utc>,
    block: u64,
    accounts: &HashMap<String, Uuid>,
    endpoint: &str,
    observations: &mut Vec<Observation>,
    dirty: &mut BTreeSet<String>,
) -> Result<()> {
    let deltas = funding["deltas"]
        .as_array()
        .context("Hyperliquid Funding event missing deltas")?;
    for delta in deltas {
        let Some((account, user)) = registered_account(delta.get("user"), accounts) else {
            continue;
        };
        let coin = required_string(delta, "coin", "Hyperliquid funding delta")?;
        let amount = validate_decimal(
            required_string(delta, "funding_amount", "Hyperliquid funding delta")?,
            "Hyperliquid funding amount",
        )?;
        let source_id = format!("{}:{coin}", timestamp.timestamp_millis());
        let fact = event_envelope(raw, json!({"Funding":{"deltas":[delta]}}));
        observations.push(observation(
            user,
            &account,
            EventKind::HyperliquidFunding,
            coin,
            Some(amount),
            fact,
            endpoint,
            HyperliquidStream::Events.method(),
            &source_id,
            timestamp,
            block,
        )?);
        dirty.insert(account);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn normalize_ledger(
    ledger: &Value,
    raw: &Value,
    timestamp: DateTime<Utc>,
    block: u64,
    accounts: &HashMap<String, Uuid>,
    endpoint: &str,
    observations: &mut Vec<Observation>,
    dirty: &mut BTreeSet<String>,
) -> Result<()> {
    let users = ledger["users"]
        .as_array()
        .context("Hyperliquid LedgerUpdate missing users")?;
    let relevant: Vec<_> = users
        .iter()
        .filter_map(Value::as_str)
        .map(str::to_lowercase)
        .filter_map(|account| accounts.get(&account).copied().map(|user| (account, user)))
        .collect();
    if relevant.is_empty() {
        return Ok(());
    }
    let delta = &ledger["delta"];
    let delta_type = required_string(delta, "type", "Hyperliquid ledger delta")?;
    if delta_type == "ledgerLiquidation" {
        // Executions are authoritative on TRADES; this record only triggers
        // reconciliation.
        dirty.extend(relevant.into_iter().map(|(account, _)| account));
        return Ok(());
    }
    let (asset, quantity) =
        if let Some(token) = delta["token"].as_str().filter(|token| !token.is_empty()) {
            (
                token,
                delta["amount"].as_str().with_context(|| {
                    format!("Hyperliquid ledger delta {delta_type} missing amount")
                })?,
            )
        } else if let Some(usdc) = delta["usdc"].as_str() {
            ("USDC", usdc)
        } else if let Some(net_withdrawn) = delta["netWithdrawnUsd"].as_str() {
            ("USDC", net_withdrawn)
        } else {
            bail!("Hyperliquid ledger delta {delta_type} missing asset and quantity")
        };
    let quantity = validate_decimal(quantity, "Hyperliquid ledger quantity")?;
    let event_kind = if delta_type.to_lowercase().contains("fee") {
        EventKind::HyperliquidFee
    } else {
        EventKind::HyperliquidTransfer
    };
    let source_id = event_source_id(raw, timestamp, delta);
    for (account, user) in relevant {
        let fact = event_envelope(
            raw,
            json!({"LedgerUpdate":{"users":[account],"delta":delta}}),
        );
        observations.push(observation(
            user,
            &account,
            event_kind.clone(),
            asset,
            Some(quantity),
            fact,
            endpoint,
            HyperliquidStream::Events.method(),
            &source_id,
            timestamp,
            block,
        )?);
        dirty.insert(account);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn normalize_single_user_event(
    variant: &str,
    value: &Value,
    raw: &Value,
    timestamp: DateTime<Utc>,
    block: u64,
    accounts: &HashMap<String, Uuid>,
    endpoint: &str,
    observations: &mut Vec<Observation>,
    dirty: &mut BTreeSet<String>,
) -> Result<()> {
    let account = required_string(value, "user", "Hyperliquid event")?.to_lowercase();
    let Some(user) = accounts.get(&account).copied() else {
        return Ok(());
    };
    let amount = validate_decimal(
        required_string(value, "amount", "Hyperliquid event")?,
        "Hyperliquid event amount",
    )?;
    let source_id = event_source_id(raw, timestamp, value);
    observations.push(observation(
        user,
        &account,
        EventKind::HyperliquidTransfer,
        "HYPE",
        Some(amount),
        raw.clone(),
        endpoint,
        HyperliquidStream::Events.method(),
        &format!("{variant}:{source_id}"),
        timestamp,
        block,
    )?);
    dirty.insert(account);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn normalize_validator_rewards(
    value: &Value,
    raw: &Value,
    timestamp: DateTime<Utc>,
    block: u64,
    accounts: &HashMap<String, Uuid>,
    endpoint: &str,
    observations: &mut Vec<Observation>,
    dirty: &mut BTreeSet<String>,
) -> Result<()> {
    let rewards = value["validator_to_reward"]
        .as_array()
        .context("Hyperliquid ValidatorRewards missing validator_to_reward")?;
    for reward in rewards {
        let Some((account, user)) = registered_account(reward.get(0), accounts) else {
            continue;
        };
        let tuple = reward
            .as_array()
            .filter(|tuple| tuple.len() == 2)
            .context("Hyperliquid validator reward must be [validator, amount]")?;
        let amount = validate_decimal(
            tuple[1]
                .as_str()
                .context("Hyperliquid validator reward missing amount")?,
            "Hyperliquid validator reward amount",
        )?;
        let source_id = format!(
            "ValidatorRewards:{}:{}",
            timestamp.timestamp_millis(),
            Observation::content_digest(reward)
        );
        let fact = event_envelope(
            raw,
            json!({"ValidatorRewards":{
                "validator_to_reward":[reward]
            }}),
        );
        observations.push(observation(
            user,
            &account,
            EventKind::HyperliquidTransfer,
            "HYPE",
            Some(amount),
            fact,
            endpoint,
            HyperliquidStream::Events.method(),
            &source_id,
            timestamp,
            block,
        )?);
        dirty.insert(account);
    }
    Ok(())
}

fn event_envelope(raw: &Value, inner: Value) -> Value {
    json!({
        "time": raw["time"],
        "hash": raw["hash"],
        "inner": inner,
    })
}

fn event_source_id(raw: &Value, timestamp: DateTime<Utc>, fallback: &Value) -> String {
    match raw["hash"].as_str() {
        Some(hash)
            if !hash.is_empty() && hash != "0x0" && !hash.chars().skip(2).all(|c| c == '0') =>
        {
            hash.to_owned()
        }
        _ => format!(
            "{}:{}",
            timestamp.timestamp_millis(),
            Observation::content_digest(fallback)
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn observation(
    user: Uuid,
    account: &str,
    kind: EventKind,
    asset: &str,
    quantity: Option<&str>,
    raw: Value,
    endpoint: &str,
    method: &str,
    source_id: &str,
    source_timestamp: DateTime<Utc>,
    block_number: u64,
) -> Result<Observation> {
    let subject = tackt_primitives::accounts::AccountSubject::new(user)?;
    let account =
        tackt_primitives::accounts::SourceAccount::new(subject, Source::Hyperliquid, account)?;
    let observation = Observation {
        event_id: Observation::stable_id(
            &Source::Hyperliquid,
            &kind,
            &[account.external_id(), source_id],
        ),
        source: Source::Hyperliquid,
        kind,
        account: Some(account),
        asset: None,
        external_asset: Some(asset.to_owned()),
        quantity: quantity.map(str::to_owned),
        source_timestamp,
        observed_at: Utc::now(),
        block_number: Some(block_number),
        block_hash: None,
        confirmations: None,
        canonical: true,
        finalized: true,
        raw,
        provenance: Provenance {
            endpoint: endpoint.to_owned(),
            method: method.to_owned(),
            source_id: source_id.to_owned(),
        },
    };
    Ok(observation)
}

fn required_string<'a>(value: &'a Value, field: &str, context: &str) -> Result<&'a str> {
    value[field]
        .as_str()
        .filter(|value| !value.is_empty())
        .with_context(|| format!("{context} missing {field}"))
}

fn identifier(value: &Value) -> Option<String> {
    value
        .as_str()
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .or_else(|| value.as_u64().map(|value| value.to_string()))
}

fn parse_event_time(value: &str) -> Result<DateTime<Utc>> {
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        return Ok(timestamp.with_timezone(&Utc));
    }
    NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
        .map(|timestamp| timestamp.and_utc())
        .with_context(|| format!("invalid Hyperliquid event time {value}"))
}

fn millis(value: i64) -> Result<DateTime<Utc>> {
    Utc.timestamp_millis_opt(value)
        .single()
        .with_context(|| format!("unrepresentable Hyperliquid timestamp {value}"))
}

pub(crate) fn validate_decimal<'a>(value: &'a str, context: &str) -> Result<&'a str> {
    if value.is_empty() || value.len() > 128 || value.trim() != value {
        bail!("{context} is not a bounded decimal")
    }
    let unsigned = value.strip_prefix(['+', '-']).unwrap_or(value);
    let mut decimal_points = 0;
    let mut digits = 0;
    for character in unsigned.chars() {
        match character {
            '0'..='9' => digits += 1,
            '.' => decimal_points += 1,
            _ => bail!("{context} is not a decimal"),
        }
    }
    if digits == 0 || decimal_points > 1 {
        bail!("{context} is not a decimal")
    }
    Ok(value)
}

fn decimal_is_zero(value: &str) -> bool {
    value
        .trim_start_matches(['+', '-'])
        .chars()
        .all(|character| character == '0' || character == '.')
}

fn collect_registered_addresses(
    value: &Value,
    accounts: &HashMap<String, Uuid>,
    found: &mut BTreeSet<String>,
) {
    match value {
        Value::String(value) => {
            let value = value.to_lowercase();
            if accounts.contains_key(&value) {
                found.insert(value);
            }
        }
        Value::Array(values) => values
            .iter()
            .for_each(|value| collect_registered_addresses(value, accounts, found)),
        Value::Object(values) => {
            // Some variants key account data by address.
            for (key, value) in values {
                let key = key.to_lowercase();
                if accounts.contains_key(&key) {
                    found.insert(key);
                }
                collect_registered_addresses(value, accounts, found);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const REGISTERED: &str = "0x1111111111111111111111111111111111111111";
    const STRANGER: &str = "0x2222222222222222222222222222222222222222";

    fn registered() -> (HashMap<String, Uuid>, Uuid) {
        let user = Uuid::new_v4();
        (HashMap::from([(REGISTERED.into(), user)]), user)
    }

    #[test]
    fn validates_exact_decimal_syntax() {
        for valid in ["0", "-0.25", "+12", ".5", "5."] {
            assert_eq!(validate_decimal(valid, "test").unwrap(), valid);
        }
        for invalid in ["", ".", "-.", "1e3", "NaN", " 1", "1.2.3"] {
            assert!(validate_decimal(invalid, "test").is_err(), "{invalid}");
        }
        assert!(validate_decimal(&"1".repeat(129), "test").is_err());
    }

    #[test]
    fn normalizes_registered_trade_with_legacy_ids() {
        let (accounts, user) = registered();
        let payload = serde_json::to_vec(&json!({
            "block_number": 42,
            "events": [["0x1111111111111111111111111111111111111111", {
                "coin":"BTC", "sz":"0.2", "time":1764867600518_i64,
                "tid":1081368440085115_u64, "fee":"0.10", "feeToken":"USDC"
            }], ["0x2222222222222222222222222222222222222222", {
                "coin":"BTC", "sz":"0.2", "time":1764867600518_i64,
                "tid":1081368440085115_u64, "fee":"0", "feeToken":"USDC"
            }]]
        }))
        .unwrap();
        let normalized = normalize_block(
            HyperliquidStream::Trades,
            42,
            &payload,
            &accounts,
            "https://example:10000",
        )
        .unwrap();
        assert_eq!(normalized.observations.len(), 2);
        assert_eq!(
            normalized.observations[0]
                .account
                .as_ref()
                .map(|account| account.subject().user_id()),
            Some(user)
        );
        assert_eq!(
            normalized.observations[0].event_id,
            Observation::stable_id(
                &Source::Hyperliquid,
                &EventKind::HyperliquidFill,
                &[
                    "0x1111111111111111111111111111111111111111",
                    "1081368440085115"
                ]
            )
        );
        assert_eq!(
            normalized.observations[1].provenance.source_id,
            "1081368440085115:fee"
        );
        assert_eq!(normalized.dirty_accounts.len(), 1);
    }

    #[test]
    fn normalizes_funding_and_all_registered_ledger_users() {
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let accounts = HashMap::from([
            ("0x1111111111111111111111111111111111111111".into(), first),
            ("0x2222222222222222222222222222222222222222".into(), second),
        ]);
        let payload = serde_json::to_vec(&json!({
            "block_number": 43,
            "events": [{
                "time":"2026-04-30T19:00:00.030459967",
                "hash":"0x0000000000000000000000000000000000000000000000000000000000000000",
                "inner":{"Funding":{"deltas":[{
                    "user":"0x1111111111111111111111111111111111111111",
                    "coin":"HYPE", "funding_amount":"0.092948"
                }]}}
            }, {
                "time":"2025-11-05T10:00:12.061687082Z",
                "hash":"0xcfa4de23c8340733d11e042ee18eda020291000963372605736d89768737e11e",
                "inner":{"LedgerUpdate":{
                    "users":[
                        "0x1111111111111111111111111111111111111111",
                        "0x2222222222222222222222222222222222222222"
                    ],
                    "delta":{"type":"spotTransfer","token":"HYPE","amount":"5.0","usdc":"999"}
                }}
            }]
        }))
        .unwrap();
        let normalized = normalize_block(
            HyperliquidStream::Events,
            43,
            &payload,
            &accounts,
            "https://example:10000",
        )
        .unwrap();
        assert_eq!(normalized.observations.len(), 3);
        assert_eq!(
            normalized.observations[0].quantity.as_deref(),
            Some("0.092948")
        );
        assert_eq!(
            normalized.observations[0].provenance.source_id,
            "1777575600030:HYPE"
        );
        assert_eq!(
            normalized.observations[0].event_id,
            Observation::stable_id(
                &Source::Hyperliquid,
                &EventKind::HyperliquidFunding,
                &[
                    "0x1111111111111111111111111111111111111111",
                    "1777575600030:HYPE"
                ],
            )
        );
        assert_eq!(normalized.dirty_accounts.len(), 2);
        assert_eq!(
            normalized.observations[1].external_asset.as_deref(),
            Some("HYPE")
        );
        assert!(normalized.observations[1].asset.is_none());
        assert_eq!(normalized.observations[1].quantity.as_deref(), Some("5.0"));
        assert_eq!(
            normalized.observations[1].provenance.source_id,
            "0xcfa4de23c8340733d11e042ee18eda020291000963372605736d89768737e11e"
        );
        assert_eq!(
            normalized.observations[2].provenance.source_id,
            normalized.observations[1].provenance.source_id
        );
        assert_eq!(
            normalized.observations[1].event_id,
            Observation::stable_id(
                &Source::Hyperliquid,
                &EventKind::HyperliquidTransfer,
                &[
                    "0x1111111111111111111111111111111111111111",
                    "0xcfa4de23c8340733d11e042ee18eda020291000963372605736d89768737e11e",
                ],
            )
        );
    }

    #[test]
    fn rejects_mismatched_block_and_reconciles_unknown_registered_variant() {
        let (accounts, _) = registered();
        let mismatch = serde_json::to_vec(&json!({"block_number":41,"events":[]})).unwrap();
        assert!(
            normalize_block(
                HyperliquidStream::Trades,
                42,
                &mismatch,
                &accounts,
                "fixture"
            )
            .is_err()
        );
        let unknown = serde_json::to_vec(&json!({
            "block_number":42,
            "events":[{
                "time":"2026-04-30T19:00:00Z", "hash":"0x0",
                "inner":{"FutureEconomicEvent":{
                    "0x1111111111111111111111111111111111111111":{"amount":"1"}
                }}
            }]
        }))
        .unwrap();
        let normalized = normalize_block(
            HyperliquidStream::Events,
            42,
            &unknown,
            &accounts,
            "fixture",
        )
        .unwrap();
        assert!(normalized.observations.is_empty());
        assert_eq!(
            normalized.dirty_accounts,
            vec!["0x1111111111111111111111111111111111111111".to_owned()]
        );
    }

    fn malformed_events(address: &str) -> Vec<Value> {
        vec![
            json!({"hash":"0xaa","inner":{"CDeposit":{"user":address,"amount":"1"}}}),
            json!({"time":"2026-04-30T19:00:00Z","hash":"0xbb","inner":{
                "CDeposit":{"user":address,"amount":"1"},
                "Delegation":{"user":address,"amount":"1"}
            }}),
            json!({"time":"2026-04-30T19:00:00Z","hash":"0xcc","inner":{"LedgerUpdate":{
                "users":[address], "delta":{"token":"HYPE","amount":"5.0"}
            }}}),
            json!({"time":"2026-04-30T19:00:00Z","hash":"0xdd","inner":{"CDeposit":{
                "validator":address,"amount":"1"
            }}}),
        ]
    }

    #[test]
    fn skips_malformed_records_belonging_to_nobody_registered() {
        let (accounts, _) = registered();
        let payload = serde_json::to_vec(&json!({
            "block_number": 44,
            "events": malformed_events(STRANGER)
        }))
        .unwrap();
        let normalized = normalize_block(
            HyperliquidStream::Events,
            44,
            &payload,
            &accounts,
            "fixture",
        )
        .unwrap();
        assert!(normalized.observations.is_empty());
        assert!(normalized.dirty_accounts.is_empty());
    }

    #[test]
    fn quarantines_malformed_registered_records_without_losing_the_block() {
        let (accounts, _) = registered();
        let mut events = malformed_events(REGISTERED);
        events.push(json!({
            "time":"2026-04-30T19:00:00.030459967",
            "hash":"0xee",
            "inner":{"Funding":{"deltas":[
                {"user":STRANGER,"coin":"HYPE"},
                {"user":REGISTERED,"coin":"HYPE","funding_amount":"0.092948"}
            ]}}
        }));
        let payload = serde_json::to_vec(&json!({
            "block_number": 44,
            "events": events
        }))
        .unwrap();
        let normalized = normalize_block(
            HyperliquidStream::Events,
            44,
            &payload,
            &accounts,
            "fixture",
        )
        .unwrap();
        assert_eq!(normalized.observations.len(), 1);
        assert_eq!(
            normalized.observations[0].quantity.as_deref(),
            Some("0.092948")
        );
        assert_eq!(normalized.dirty_accounts, vec![REGISTERED.to_owned()]);
    }

    #[test]
    fn quarantines_malformed_registered_trades_without_losing_the_block() {
        let (accounts, _) = registered();
        let fill = json!({
            "coin":"BTC", "sz":"0.2", "time":1764867600518_i64,
            "tid":1081368440085115_u64, "fee":"0", "feeToken":"USDC"
        });
        let payload = serde_json::to_vec(&json!({
            "block_number": 45,
            "events": [
                "not-a-tuple",
                [STRANGER, {"coin":"BTC"}, "extra"],
                [REGISTERED, {"coin":"BTC","sz":"0.2","time":1764867600518_i64}],
                [REGISTERED, fill, "extra"],
                [REGISTERED, fill]
            ]
        }))
        .unwrap();
        let normalized = normalize_block(
            HyperliquidStream::Trades,
            45,
            &payload,
            &accounts,
            "fixture",
        )
        .unwrap();
        assert_eq!(normalized.observations.len(), 1);
        assert_eq!(
            normalized.observations[0].provenance.source_id,
            "1081368440085115"
        );
        assert_eq!(normalized.dirty_accounts, vec![REGISTERED.to_owned()]);
    }

    #[test]
    fn self_trade_legs_get_distinct_event_ids_with_legacy_first_leg() {
        let (accounts, _) = registered();
        let leg = |side: &str| {
            json!(["0x1111111111111111111111111111111111111111", {
                "coin":"BTC", "sz":"0.2", "time":1764867600518_i64, "side":side,
                "tid":1081368440085115_u64, "fee":"0", "feeToken":"USDC"
            }])
        };
        let payload = serde_json::to_vec(&json!({
            "block_number": 42,
            "events": [leg("B"), leg("A")]
        }))
        .unwrap();
        let normalized = normalize_block(
            HyperliquidStream::Trades,
            42,
            &payload,
            &accounts,
            "fixture",
        )
        .unwrap();
        assert_eq!(normalized.observations.len(), 2);
        assert_ne!(
            normalized.observations[0].event_id,
            normalized.observations[1].event_id
        );
        assert_eq!(
            normalized.observations[0].provenance.source_id,
            "1081368440085115"
        );
        assert_eq!(
            normalized.observations[1].provenance.source_id,
            "1081368440085115:2"
        );
    }
}

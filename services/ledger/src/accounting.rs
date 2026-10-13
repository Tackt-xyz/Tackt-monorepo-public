//! Ledger's accounting authority: commands, validation, postings, balances,
//! reservations and replay rules. PostgreSQL only persists our decisions.

use crate::state::DepositPhase;
use crate::state::{ReservationChange, ReservationKind, ReservationOperation};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use tackt_postgres::{
    LedgerAccount, LedgerAccountDefinition, LedgerJournalEntry, LedgerReservation, LedgerWrite,
    PgRepository,
};
use tackt_primitives::ledger::*;
use thiserror::Error;
use uuid::Uuid;

const MAX_EXPORT_TRANSACTIONS: i64 = 10_000;

#[derive(Debug, Error)]
pub enum LedgerError {
    #[error("quantity must be positive and fit the Ledger's u128 arithmetic")]
    InvalidQuantity,
    #[error("idempotency key reused with different command")]
    IdempotencyConflict,
    #[error("insufficient funds")]
    InsufficientFunds,
    #[error("controlled adjustment requires a non-empty approval reference")]
    MissingApproval,
    #[error("ledger conflict: {0}")]
    Conflict(String),
    #[error("ledger integrity failure: {0}")]
    Integrity(String),
    #[error("ledger storage failure: {0}")]
    Storage(#[from] anyhow::Error),
}

#[async_trait]
pub trait LedgerRepository: Send + Sync + 'static {
    async fn execute(&self, command: LedgerCommand) -> Result<CommandResult, LedgerError>;
}

#[derive(Clone)]
struct Leg {
    code: String,
    user_id: Option<Uuid>,
    bucket: String,
    asset: LedgerAsset,
    side: PostingSide,
    quantity: u128,
    negative: bool,
}
type CommitTemplate = (Vec<Leg>, Option<ReservationChange>, Option<Uuid>);

/// Direct observation accounting: no Core command or authorization envelope is constructed.
pub(crate) async fn post_deposit(
    write: &mut LedgerWrite,
    deposit: &tackt_postgres::Deposit,
    phase: DepositPhase,
    revision: u64,
) -> Result<(), LedgerError> {
    let d = deposit;
    let legs = match phase {
        DepositPhase::Pending => pair(
            d.user_id,
            BalanceBucket::Pending,
            d.asset,
            PostingSide::Debit,
            d.quantity,
            "external_pending",
        ),
        DepositPhase::Finalized => move_between(
            d.user_id,
            d.asset,
            d.quantity,
            BalanceBucket::Pending,
            BalanceBucket::Available,
        ),
        DepositPhase::Rejected => pair(
            d.user_id,
            if d.status == "finalized" {
                BalanceBucket::Available
            } else {
                BalanceBucket::Pending
            },
            d.asset,
            PostingSide::Credit,
            d.quantity,
            "external_pending",
        ),
    };
    let phase = phase.as_ref();
    let reference = format!("{}:{phase}:{revision}", d.event_id);
    let postings = Value::Array(legs.iter().map(Leg::json).collect());
    commit(
        write,
        LedgerCommandSource::Evm,
        d.chain_id,
        phase,
        &reference,
        Some(&reference),
        d.effective_at,
        json!({"actor":"indexer","reason":phase,"observation_id":d.event_id}),
        postings,
        &legs,
        None,
        None,
    )
    .await?;
    Ok(())
}

fn move_between(
    user: Uuid,
    asset: LedgerAsset,
    quantity: u128,
    from: BalanceBucket,
    to: BalanceBucket,
) -> Vec<Leg> {
    vec![
        Leg::user(user, from, asset, PostingSide::Credit, quantity),
        Leg::user(user, to, asset, PostingSide::Debit, quantity),
    ]
}

#[derive(Clone)]
pub struct Ledger {
    pub(crate) repository: PgRepository,
}
impl Ledger {
    pub fn new(repository: PgRepository) -> Self {
        Self { repository }
    }
}
impl Leg {
    fn user(
        id: Uuid,
        bucket: BalanceBucket,
        asset: LedgerAsset,
        side: PostingSide,
        q: u128,
    ) -> Self {
        Self {
            code: format!("user:{id}:{}:{}", bucket.as_ref(), asset.code()),
            user_id: Some(id),
            bucket: bucket.as_ref().into(),
            asset,
            side,
            quantity: q,
            negative: matches!(bucket, BalanceBucket::PrincipalLiability),
        }
    }
    fn system(bucket: &str, asset: LedgerAsset, side: PostingSide, q: u128) -> Self {
        Self {
            code: format!("system:{bucket}:{}", asset.code()),
            user_id: None,
            bucket: bucket.into(),
            asset,
            side,
            quantity: q,
            negative: true,
        }
    }
    fn json(&self) -> Value {
        json!({"account_code":self.code,"user_id":self.user_id,"bucket":self.bucket,"asset":self.asset.code(),"side":self.side.as_ref(),"quantity":self.quantity.to_string(),"allow_negative":self.negative})
    }
    fn opposite(mut self) -> Self {
        let x = &mut self;
        x.side = match x.side {
            PostingSide::Debit => PostingSide::Credit,
            PostingSide::Credit => PostingSide::Debit,
        };
        self
    }
}
fn amount(q: u128) -> Result<u128, LedgerError> {
    if q == 0 {
        Err(LedgerError::InvalidQuantity)
    } else {
        Ok(q)
    }
}
/// Commands retain their accounting checks regardless of ingress.
fn validate_command(command: &LedgerCommand) -> Result<(), LedgerError> {
    let h = command.header();
    if h.chain_id == 0
        || !tackt_primitives::ledger::valid_idempotency_key(&h.idempotency_key)
        || h.audit.actor.trim().is_empty()
        || h.audit.reason.trim().is_empty()
    {
        return Err(LedgerError::Conflict(
            "positive chain id, valid idempotency key (1-256 bytes, no control characters), and audit actor/reason are required".into(),
        ));
    }
    let Some(user_id) = command.core_authorized_user() else {
        return Ok(());
    };
    let Some(authorization) = &h.audit.authorization else {
        return Err(LedgerError::Conflict(
            "this command requires Core authorization evidence".into(),
        ));
    };
    // `ledger.accounts.user_id` and `ledger.reservations.id` have no foreign keys, so a nil
    // identifier books real postings against a placeholder rather than failing.
    if user_id.is_nil() {
        return Err(LedgerError::Conflict(
            "this command requires a non-nil user".into(),
        ));
    }
    if command.core_authorized_reservation() == Some(Uuid::nil()) {
        return Err(LedgerError::Conflict(
            "this command requires a non-nil reservation".into(),
        ));
    }
    // One authorization books one mutation: binding the retry key to the authorization is what
    // stops the same approved evidence being replayed under a second key and booked twice. The
    // HTTP surface binds its `Idempotency-Key` to the same field, so both ingresses agree.
    if authorization.core_authorization_id() != h.idempotency_key {
        return Err(LedgerError::Conflict(
            "idempotency key must equal core_authorization_id".into(),
        ));
    }
    if let Some(exit) = command.custody_exit() {
        if authorization.core_action_id().parse::<Uuid>() != Ok(exit.reservation_id) {
            return Err(LedgerError::Conflict(
                "custody-exit reservation id must equal core_action_id".into(),
            ));
        }
        if exit.destination == CustodyExitDestination::HyperliquidMargin
            && exit.asset.is_some_and(|asset| asset != LedgerAsset::Usdc)
        {
            return Err(LedgerError::Conflict(
                "Hyperliquid custody exits require USDC".into(),
            ));
        }
    }
    // Settlement derives its keys from observation identities and reaches `execute` in process;
    // a Core-authorized command never legitimately claims that namespace, and one that did would
    // squat the row a deposit's settlement step is about to write.
    if tackt_primitives::is_reserved_observation_reference(&h.idempotency_key)
        || h.external_reference
            .as_deref()
            .is_some_and(tackt_primitives::is_reserved_observation_reference)
    {
        return Err(LedgerError::Conflict(
            "reserved observation namespace".into(),
        ));
    }
    Ok(())
}
fn pair(
    user: Uuid,
    bucket: BalanceBucket,
    asset: LedgerAsset,
    side: PostingSide,
    q: u128,
    system: &str,
) -> Vec<Leg> {
    vec![
        Leg::user(user, bucket, asset, side, q),
        Leg::system(
            system,
            asset,
            match side {
                PostingSide::Debit => PostingSide::Credit,
                PostingSide::Credit => PostingSide::Debit,
            },
            q,
        ),
    ]
}

fn credit_draw_legs(user: Uuid, destination: BalanceBucket, quantity: u128) -> Vec<Leg> {
    vec![
        Leg::user(
            user,
            destination,
            LedgerAsset::Usdc,
            PostingSide::Debit,
            quantity,
        ),
        Leg::system(
            "provider_funding",
            LedgerAsset::Usdc,
            PostingSide::Credit,
            quantity,
        ),
        Leg::system(
            "provider_receivable",
            LedgerAsset::Usdc,
            PostingSide::Debit,
            quantity,
        ),
        Leg::user(
            user,
            BalanceBucket::PrincipalLiability,
            LedgerAsset::Usdc,
            PostingSide::Credit,
            quantity,
        ),
    ]
}

#[async_trait]
impl LedgerRepository for Ledger {
    async fn execute(&self, command: LedgerCommand) -> Result<CommandResult, LedgerError> {
        validate_command(&command)?;
        let mut write = self.repository.begin_ledger_write().await?;
        let h = command.header();
        let (legs, reservation, reverses): CommitTemplate = match &command {
            LedgerCommand::DrawCredit {
                user_id,
                quantity,
                destination,
                ..
            } => {
                let q = amount(*quantity)?;
                let b = match destination {
                    CreditDestination::EthereumAvailable => BalanceBucket::Available,
                    CreditDestination::HyperliquidMargin => BalanceBucket::HyperliquidMargin,
                };
                (credit_draw_legs(*user_id, b, q), None, None)
            }
            LedgerCommand::ReserveCustodyExit {
                user_id,
                reservation_id,
                asset,
                quantity,
                destination,
                ..
            } => {
                let q = amount(*quantity)?;
                (
                    move_between(
                        *user_id,
                        *asset,
                        q,
                        BalanceBucket::Available,
                        BalanceBucket::Reserved,
                    ),
                    Some(ReservationChange {
                        operation: ReservationOperation::Create,
                        id: *reservation_id,
                        user_id: *user_id,
                        kind: ReservationKind::CustodyExit,
                        asset: *asset,
                        quantity: q,
                        destination: Some(*destination),
                    }),
                    None,
                )
            }
            LedgerCommand::DrawCreditToCustodyExit {
                user_id,
                reservation_id,
                quantity,
                destination,
                ..
            } => {
                let q = amount(*quantity)?;
                (
                    credit_draw_legs(*user_id, BalanceBucket::Reserved, q),
                    Some(ReservationChange {
                        operation: ReservationOperation::Create,
                        id: *reservation_id,
                        user_id: *user_id,
                        kind: ReservationKind::CustodyExit,
                        asset: LedgerAsset::Usdc,
                        quantity: q,
                        destination: Some(*destination),
                    }),
                    None,
                )
            }
            LedgerCommand::ConsumeCustodyExit {
                user_id,
                reservation_id,
                asset,
                quantity,
                destination,
                ..
            } => custody_exit_move(
                *user_id,
                *reservation_id,
                *asset,
                *quantity,
                *destination,
                CustodyExitTransition::Consume,
            )?,
            LedgerCommand::ReleaseCustodyExit {
                user_id,
                reservation_id,
                asset,
                quantity,
                destination,
                ..
            } => custody_exit_move(
                *user_id,
                *reservation_id,
                *asset,
                *quantity,
                *destination,
                CustodyExitTransition::Release,
            )?,
            LedgerCommand::ReserveCreditForOrder {
                user_id,
                reservation_id,
                quantity,
                ..
            } => {
                let q = amount(*quantity)?;
                (
                    move_between(
                        *user_id,
                        LedgerAsset::Usdc,
                        q,
                        BalanceBucket::Available,
                        BalanceBucket::OrderReserved,
                    ),
                    Some(ReservationChange {
                        operation: ReservationOperation::Create,
                        id: *reservation_id,
                        user_id: *user_id,
                        kind: ReservationKind::Order,
                        asset: LedgerAsset::Usdc,
                        quantity: q,
                        destination: None,
                    }),
                    None,
                )
            }
            LedgerCommand::ConsumeOrderReservation {
                user_id,
                reservation_id,
                quantity,
                ..
            } => reservation_move(
                *user_id,
                *reservation_id,
                LedgerAsset::Usdc,
                *quantity,
                BalanceBucket::OrderReserved,
                BalanceBucket::HyperliquidMargin,
                ReservationKind::Order,
                ReservationOperation::Consume,
            )?,
            LedgerCommand::ReleaseOrderReservation {
                user_id,
                reservation_id,
                quantity,
                ..
            } => reservation_move(
                *user_id,
                *reservation_id,
                LedgerAsset::Usdc,
                *quantity,
                BalanceBucket::OrderReserved,
                BalanceBucket::Available,
                ReservationKind::Order,
                ReservationOperation::Release,
            )?,
            LedgerCommand::RecordHyperliquidTransfer {
                user_id,
                quantity,
                direction,
                ..
            } => {
                let q = amount(*quantity)?;
                let (from, to) = match direction {
                    TransferDirection::ToHyperliquid => {
                        (BalanceBucket::Available, BalanceBucket::HyperliquidMargin)
                    }
                    TransferDirection::FromHyperliquid => {
                        (BalanceBucket::HyperliquidMargin, BalanceBucket::Available)
                    }
                };
                (
                    move_between(*user_id, LedgerAsset::Usdc, q, from, to),
                    None,
                    None,
                )
            }
            LedgerCommand::RecordRealizedPnl {
                user_id,
                quantity,
                direction,
                ..
            }
            | LedgerCommand::RecordFundingPayment {
                user_id,
                quantity,
                direction,
                ..
            } => {
                let q = amount(*quantity)?;
                let side = if *direction == GainLoss::Gain {
                    PostingSide::Debit
                } else {
                    PostingSide::Credit
                };
                (
                    pair(
                        *user_id,
                        BalanceBucket::HyperliquidMargin,
                        LedgerAsset::Usdc,
                        side,
                        q,
                        if matches!(command, LedgerCommand::RecordRealizedPnl { .. }) {
                            "realized_pnl"
                        } else {
                            "funding"
                        },
                    ),
                    None,
                    None,
                )
            }
            LedgerCommand::RecordVenueFee {
                user_id, quantity, ..
            } => (
                pair(
                    *user_id,
                    BalanceBucket::HyperliquidMargin,
                    LedgerAsset::Usdc,
                    PostingSide::Credit,
                    amount(*quantity)?,
                    "venue_fees",
                ),
                None,
                None,
            ),
            LedgerCommand::ReserveWithdrawal {
                user_id,
                reservation_id,
                asset,
                quantity,
                ..
            } => {
                let q = amount(*quantity)?;
                (
                    move_between(
                        *user_id,
                        *asset,
                        q,
                        BalanceBucket::Available,
                        BalanceBucket::WithdrawalReserved,
                    ),
                    Some(ReservationChange {
                        operation: ReservationOperation::Create,
                        id: *reservation_id,
                        user_id: *user_id,
                        kind: ReservationKind::Withdrawal,
                        asset: *asset,
                        quantity: q,
                        destination: None,
                    }),
                    None,
                )
            }
            LedgerCommand::SettleWithdrawal {
                user_id,
                reservation_id,
                asset,
                quantity,
                ..
            } => {
                let q = amount(*quantity)?;
                (
                    pair(
                        *user_id,
                        BalanceBucket::WithdrawalReserved,
                        *asset,
                        PostingSide::Credit,
                        q,
                        "custody_outflow",
                    ),
                    Some(ReservationChange {
                        operation: ReservationOperation::Settle,
                        id: *reservation_id,
                        user_id: *user_id,
                        kind: ReservationKind::Withdrawal,
                        asset: *asset,
                        quantity: q,
                        destination: None,
                    }),
                    None,
                )
            }
            LedgerCommand::ReleaseWithdrawal {
                user_id,
                reservation_id,
                asset,
                quantity,
                ..
            } => reservation_move(
                *user_id,
                *reservation_id,
                *asset,
                *quantity,
                BalanceBucket::WithdrawalReserved,
                BalanceBucket::Available,
                ReservationKind::Withdrawal,
                ReservationOperation::Release,
            )?,
            LedgerCommand::RecordPrincipalRepayment {
                user_id, quantity, ..
            } => {
                let q = amount(*quantity)?;
                (
                    vec![
                        Leg::user(
                            *user_id,
                            BalanceBucket::Available,
                            LedgerAsset::Usdc,
                            PostingSide::Credit,
                            q,
                        ),
                        Leg::user(
                            *user_id,
                            BalanceBucket::PrincipalLiability,
                            LedgerAsset::Usdc,
                            PostingSide::Debit,
                            q,
                        ),
                        Leg::system("provider_funding", LedgerAsset::Usdc, PostingSide::Debit, q),
                        Leg::system(
                            "provider_receivable",
                            LedgerAsset::Usdc,
                            PostingSide::Credit,
                            q,
                        ),
                    ],
                    None,
                    None,
                )
            }
            LedgerCommand::RecordLiquidationProceeds {
                user_id, quantity, ..
            } => (
                pair(
                    *user_id,
                    BalanceBucket::LiquidationProceeds,
                    LedgerAsset::Usdc,
                    PostingSide::Debit,
                    amount(*quantity)?,
                    "liquidation",
                ),
                None,
                None,
            ),
            LedgerCommand::RecordCustomerSurplus {
                user_id, quantity, ..
            } => {
                let q = amount(*quantity)?;
                (
                    move_between(
                        *user_id,
                        LedgerAsset::Usdc,
                        q,
                        BalanceBucket::LiquidationProceeds,
                        BalanceBucket::CustomerSurplus,
                    ),
                    None,
                    None,
                )
            }
            LedgerCommand::RecordCreditProviderShortfall {
                user_id, quantity, ..
            } => {
                let q = amount(*quantity)?;
                (
                    vec![
                        Leg::user(
                            *user_id,
                            BalanceBucket::PrincipalLiability,
                            LedgerAsset::Usdc,
                            PostingSide::Debit,
                            q,
                        ),
                        Leg::user(
                            *user_id,
                            BalanceBucket::ProviderShortfall,
                            LedgerAsset::Usdc,
                            PostingSide::Debit,
                            q,
                        ),
                        Leg::system(
                            "provider_receivable",
                            LedgerAsset::Usdc,
                            PostingSide::Credit,
                            q,
                        ),
                        Leg::system(
                            "provider_loss_equity",
                            LedgerAsset::Usdc,
                            PostingSide::Credit,
                            q,
                        ),
                    ],
                    None,
                    None,
                )
            }
            LedgerCommand::ControlledAdjustment {
                user_id,
                asset,
                bucket,
                quantity,
                direction,
                approval_reference,
                ..
            } => {
                if approval_reference.trim().is_empty() {
                    return Err(LedgerError::MissingApproval);
                };
                let q = amount(*quantity)?;
                let side = if *direction == GainLoss::Gain {
                    PostingSide::Debit
                } else {
                    PostingSide::Credit
                };
                (
                    pair(*user_id, *bucket, *asset, side, q, "controlled_adjustment"),
                    None,
                    None,
                )
            }
            LedgerCommand::ReverseTransaction { transaction_id, .. } => {
                let postings = write.postings(*transaction_id).await?;
                if postings.is_empty() {
                    return Err(LedgerError::Conflict(
                        "transaction to reverse does not exist".into(),
                    ));
                }
                let xs = postings
                    .into_iter()
                    .map(|posting| {
                        Leg {
                            code: posting.account.code,
                            user_id: posting.account.user_id,
                            bucket: posting.account.bucket,
                            asset: posting.account.asset,
                            side: posting.side,
                            quantity: posting.quantity,
                            negative: posting.account.allow_negative,
                        }
                        .opposite()
                    })
                    .collect();
                (xs, None, Some(*transaction_id))
            }
        };
        let postings = Value::Array(legs.iter().map(Leg::json).collect());
        let mut audit =
            serde_json::to_value(&h.audit).map_err(|e| LedgerError::Integrity(e.to_string()))?;
        if let LedgerCommand::ControlledAdjustment {
            approval_reference, ..
        } = &command
        {
            audit["approval_reference"] = Value::String(approval_reference.clone());
        }
        let result = commit(
            &mut write,
            command.source(),
            h.chain_id,
            command.kind(),
            &h.idempotency_key,
            h.external_reference.as_deref(),
            h.effective_at,
            audit,
            postings,
            &legs,
            reservation,
            reverses,
        )
        .await?;
        write.commit().await?;
        Ok(result)
    }
}

impl Ledger {
    pub async fn health(&self) -> Result<(), LedgerError> {
        self.repository
            .ledger_health()
            .await
            .map_err(LedgerError::from)
    }
    pub async fn balances(
        &self,
        user_id: Uuid,
        as_of: Option<DateTime<Utc>>,
        time_basis: BalanceTimeBasis,
    ) -> Result<Vec<LedgerBalance>, LedgerError> {
        Ok(self
            .repository
            .ledger_balances(user_id, as_of, time_basis)
            .await?)
    }

    pub async fn export(&self) -> Result<LedgerExport, LedgerError> {
        let export = self
            .repository
            .ledger_export(MAX_EXPORT_TRANSACTIONS as u32 + 1)
            .await?;
        if export.transactions.len() > MAX_EXPORT_TRANSACTIONS as usize {
            return Err(LedgerError::Conflict(format!(
                "export exceeds the {MAX_EXPORT_TRANSACTIONS}-transaction safety limit"
            )));
        }
        Ok(export)
    }
}

#[allow(clippy::too_many_arguments)]
async fn commit(
    write: &mut LedgerWrite,
    source: LedgerCommandSource,
    chain_id: u64,
    kind: &str,
    idempotency_key: &str,
    external_reference: Option<&str>,
    effective_at: DateTime<Utc>,
    audit: Value,
    postings: Value,
    legs: &[Leg],
    reservation: Option<ReservationChange>,
    reverses: Option<Uuid>,
) -> Result<CommandResult, LedgerError> {
    validate_balanced(legs)?;
    let reservation_json = reservation
        .as_ref()
        .map(serde_json::to_value)
        .transpose()
        .map_err(|error| LedgerError::Integrity(error.to_string()))?;
    let command_hash = hash_command(
        source,
        chain_id,
        kind,
        external_reference,
        effective_at,
        &audit,
        &postings,
        reservation_json.as_ref(),
        reverses,
    )?;
    if let Some((result, previous_hash)) = write.existing_command(idempotency_key).await? {
        if previous_hash != command_hash {
            return Err(LedgerError::IdempotencyConflict);
        }
        return Ok(result);
    }
    if let Some(reference) = external_reference
        && write
            .external_reference_exists(source, chain_id, reference)
            .await?
    {
        return Err(LedgerError::Conflict(
            "external reference already journaled".into(),
        ));
    }
    if let Some(original) = reverses {
        let original_kind = write.transaction_kind(original).await?;
        if original_kind
            .as_deref()
            .is_some_and(|kind| CUSTODY_EXIT_COMMAND_KINDS.contains(&kind))
        {
            return Err(LedgerError::Conflict(
                "custody-exit transactions cannot be reversed".into(),
            ));
        }
        if original_kind.is_none() || write.reversal_exists(original).await? {
            return Err(LedgerError::Conflict(
                "invalid or duplicate reversal".into(),
            ));
        }
    }
    let result = write
        .append_transaction(LedgerJournalEntry {
            source,
            chain_id,
            kind,
            hash: &command_hash,
            idempotency_key,
            external_reference,
            effective_at,
            audit: &audit,
            reverses,
        })
        .await?;
    // Validation and persistence share one serialized transaction. A failure
    // below rolls back the journal, projections and reservations together.
    let mut ordered: Vec<_> = legs.iter().collect();
    ordered.sort_by(|a, b| a.code.cmp(&b.code));
    let codes: Vec<_> = ordered.iter().map(|leg| leg.code.as_str()).collect();
    let mut accounts = write.accounts(&codes).await?;
    let mut rows = Vec::with_capacity(legs.len());
    for leg in ordered {
        let definition = LedgerAccountDefinition {
            code: leg.code.clone(),
            user_id: leg.user_id,
            bucket: leg.bucket.clone(),
            asset: leg.asset,
            allow_negative: leg.negative,
        };
        let account = accounts
            .entry(leg.code.clone())
            .or_insert_with(|| LedgerAccount {
                id: Uuid::new_v4(),
                definition: definition.clone(),
                debit: 0,
                credit: 0,
            });
        if account.definition != definition {
            return Err(LedgerError::Conflict("account identity mismatch".into()));
        }
        let total = match leg.side {
            PostingSide::Debit => &mut account.debit,
            PostingSide::Credit => &mut account.credit,
        };
        *total = total
            .checked_add(leg.quantity)
            .ok_or(LedgerError::InvalidQuantity)?;
        if leg.user_id.is_some() {
            let allowed = if leg.bucket == "principal_liability" {
                account.debit <= account.credit
            } else {
                account.debit >= account.credit
            };
            if !allowed {
                return Err(LedgerError::InsufficientFunds);
            }
        }
        rows.push(tackt_postgres::LedgerPostingWrite {
            account_id: account.id,
            asset: leg.asset,
            side: leg.side,
            quantity: leg.quantity,
        });
    }
    write
        .store_accounts(
            &accounts.into_values().collect::<Vec<_>>(),
            result.recorded_at,
        )
        .await?;
    write.append_postings(result.transaction_id, &rows).await?;
    apply_reservation(
        write,
        reservation.as_ref(),
        result.transaction_id,
        result.recorded_at,
    )
    .await?;
    Ok(result)
}

fn validate_balanced(legs: &[Leg]) -> Result<(), LedgerError> {
    let mut assets: HashMap<LedgerAsset, (Vec<u128>, Vec<u128>)> = HashMap::new();
    let mut identities = std::collections::HashSet::new();
    for leg in legs {
        amount(leg.quantity)?;
        if !identities.insert((&leg.code, leg.side.as_ref())) {
            return Err(LedgerError::Conflict(
                "duplicate posting for account and side".into(),
            ));
        }
        let (debits, credits) = assets.entry(leg.asset).or_default();
        match leg.side {
            PostingSide::Debit => debits.push(leg.quantity),
            PostingSide::Credit => credits.push(leg.quantity),
        }
    }
    // Cancel opposite quantities instead of summing them: a balanced pair of
    // full-u128 postings must not overflow an intermediate per-asset total.
    for (mut debits, mut credits) in assets.into_values() {
        while let (Some(debit), Some(credit)) = (debits.last_mut(), credits.last_mut()) {
            let matched = (*debit).min(*credit);
            *debit -= matched;
            *credit -= matched;
            if *debit == 0 {
                debits.pop();
            }
            if *credit == 0 {
                credits.pop();
            }
        }
        if !debits.is_empty() || !credits.is_empty() {
            return Err(LedgerError::Conflict(
                "transaction is not balanced independently per asset".into(),
            ));
        }
    }
    if legs.len() < 2 {
        return Err(LedgerError::Conflict(
            "transaction is not balanced independently per asset".into(),
        ));
    }
    Ok(())
}

async fn apply_reservation(
    write: &mut LedgerWrite,
    reservation: Option<&ReservationChange>,
    transaction_id: Uuid,
    recorded_at: DateTime<Utc>,
) -> Result<(), LedgerError> {
    let Some(reservation) = reservation else {
        return Ok(());
    };
    let ReservationChange {
        id,
        user_id,
        asset,
        quantity,
        kind,
        operation,
        destination,
    } = *reservation;
    let kind = kind.as_ref();
    let destination = destination.map(CustodyExitDestination::code);
    let existing = write.reservation(id).await?;
    let value = if matches!(operation, ReservationOperation::Create) {
        if existing.is_some() {
            return Err(LedgerError::Conflict("reservation already exists".into()));
        }
        LedgerReservation {
            id,
            user_id,
            kind: kind.into(),
            destination: destination.map(str::to_owned),
            asset,
            original_quantity: quantity,
            remaining_quantity: quantity,
            status: "open".into(),
            created_transaction_id: transaction_id,
        }
    } else {
        let message = if kind == "custody_exit" {
            "invalid custody-exit reservation transition"
        } else {
            "invalid or insufficient reservation"
        };
        let mut value = existing.ok_or_else(|| LedgerError::Conflict(message.into()))?;
        if value.user_id != user_id
            || value.asset != asset
            || value.kind != kind
            || value.status != "open"
            || value.remaining_quantity < quantity
            || (kind == "custody_exit"
                && (value.destination.as_deref() != destination
                    || value.remaining_quantity != quantity))
        {
            return Err(LedgerError::Conflict(message.into()));
        }
        value.remaining_quantity -= quantity;
        if value.remaining_quantity == 0 {
            value.status = operation.terminal_status().into();
        }
        value
    };
    write.store_reservation(&value, recorded_at).await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn hash_command(
    source: LedgerCommandSource,
    chain_id: u64,
    kind: &str,
    external_reference: Option<&str>,
    effective_at: DateTime<Utc>,
    audit: &Value,
    postings: &Value,
    reservation: Option<&Value>,
    reverses: Option<Uuid>,
) -> Result<String, LedgerError> {
    let canonical = json!({
        "audit": audit,
        "chain_id": chain_id,
        "effective_at": effective_at,
        "external_reference": external_reference,
        "kind": kind,
        "postings": postings,
        "reservation": reservation,
        "reverses": reverses,
        "source": source.code(),
    });
    let bytes = serde_json::to_vec(&canonical)
        .map_err(|error| LedgerError::Integrity(format!("cannot hash command: {error}")))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

#[derive(Clone, Copy)]
enum CustodyExitTransition {
    Consume,
    Release,
}

fn custody_exit_move(
    user: Uuid,
    id: Uuid,
    asset: LedgerAsset,
    quantity: u128,
    destination: CustodyExitDestination,
    transition: CustodyExitTransition,
) -> Result<CommitTemplate, LedgerError> {
    let q = amount(quantity)?;
    let legs = match transition {
        CustodyExitTransition::Release => move_between(
            user,
            asset,
            q,
            BalanceBucket::Reserved,
            BalanceBucket::Available,
        ),
        CustodyExitTransition::Consume => match destination {
            CustodyExitDestination::CustodyOutflow => pair(
                user,
                BalanceBucket::Reserved,
                asset,
                PostingSide::Credit,
                q,
                "custody_outflow",
            ),
            CustodyExitDestination::HyperliquidMargin => move_between(
                user,
                asset,
                q,
                BalanceBucket::Reserved,
                BalanceBucket::HyperliquidMargin,
            ),
        },
    };
    Ok((
        legs,
        Some(ReservationChange {
            operation: match transition {
                CustodyExitTransition::Consume => ReservationOperation::Consume,
                CustodyExitTransition::Release => ReservationOperation::Release,
            },
            id,
            user_id: user,
            kind: ReservationKind::CustodyExit,
            asset,
            quantity: q,
            destination: Some(destination),
        }),
        None,
    ))
}

#[allow(clippy::too_many_arguments)]
fn reservation_move(
    user: Uuid,
    id: Uuid,
    asset: LedgerAsset,
    quantity: u128,
    from: BalanceBucket,
    to: BalanceBucket,
    kind: ReservationKind,
    op: ReservationOperation,
) -> Result<CommitTemplate, LedgerError> {
    let q = amount(quantity)?;
    Ok((
        move_between(user, asset, q, from, to),
        Some(ReservationChange {
            operation: op,
            id,
            user_id: user,
            kind,
            asset,
            quantity: q,
            destination: None,
        }),
        None,
    ))
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn balancing_is_per_asset_and_handles_full_width_quantities() {
        let legs = credit_draw_legs(Uuid::from_u128(1), BalanceBucket::Available, u128::MAX);
        validate_balanced(&legs).unwrap();

        let mut wrong_asset = legs.clone();
        wrong_asset[0].asset = LedgerAsset::Eth;
        assert!(validate_balanced(&wrong_asset).is_err());

        let mut wrong_quantity = legs.clone();
        wrong_quantity[0].quantity -= 1;
        assert!(validate_balanced(&wrong_quantity).is_err());

        let mut zero_quantity = legs.clone();
        zero_quantity[0].quantity = 0;
        assert!(matches!(
            validate_balanced(&zero_quantity),
            Err(LedgerError::InvalidQuantity)
        ));

        let mut duplicate = legs.clone();
        duplicate.extend(legs);
        assert!(matches!(
            validate_balanced(&duplicate),
            Err(LedgerError::Conflict(message)) if message.contains("duplicate posting")
        ));
        assert!(validate_balanced(&[]).is_err());
    }

    #[test]
    fn command_hash_encoding_is_stable() {
        // Historical journal identities remain stable even when a command is retired.
        let effective_at = "2026-08-04T12:00:00Z".parse().unwrap();
        let hash = hash_command(
            LedgerCommandSource::Evm,
            1,
            "record_pending_deposit",
            Some("0xabc:0"),
            effective_at,
            &json!({"actor":"indexer","reason":"finalized deposit"}),
            &json!([{"account_code":"user:fixture:pending:ETH","asset":"ETH","quantity":"42","side":"debit"}]),
            None,
            None,
        )
        .unwrap();
        assert_eq!(
            hash,
            "50d4e53abb3400c73753180fff618ca384843e6241bef2a53089448a5da7d346"
        );
    }

    fn header(authorization: Option<CoreAuthorization>) -> CommandHeader {
        CommandHeader {
            chain_id: 1,
            idempotency_key: "authorization-1".into(),
            external_reference: None,
            effective_at: "2026-08-04T12:00:00Z".parse().unwrap(),
            audit: AuditMetadata {
                actor: "core-api".into(),
                reason: "approved credit draw".into(),
                risk_snapshot_id: None,
                authorization,
                attributes: Default::default(),
            },
        }
    }

    fn evidence() -> CoreAuthorization {
        CoreAuthorization::new(
            "action-1".into(),
            "authorization-1".into(),
            "recommendation-1".into(),
        )
        .unwrap()
    }

    #[test]
    fn core_authorized_commands_are_refused_without_evidence_or_a_real_user() {
        let draw = |header, user_id| LedgerCommand::DrawCredit {
            header,
            user_id,
            quantity: 10,
            destination: CreditDestination::EthereumAvailable,
        };
        let user = Uuid::from_u128(1);
        validate_command(&draw(header(Some(evidence())), user)).unwrap();

        // Each precondition names itself, so a test cannot pass on the wrong rejection.
        let rebound = {
            let mut h = header(Some(evidence()));
            h.idempotency_key = "a-second-key-for-one-authorization".into();
            h
        };
        let reserved = {
            let mut h = header(Some(evidence()));
            h.external_reference = Some(format!(
                "{}deadbeef:pending",
                tackt_primitives::OBSERVATION_ID_PREFIX
            ));
            h
        };
        for (refused, expected) in [
            (
                draw(header(None), user),
                "requires Core authorization evidence",
            ),
            (draw(header(Some(evidence())), Uuid::nil()), "non-nil user"),
            (draw(rebound, user), "must equal core_authorization_id"),
            (draw(reserved, user), "reserved observation namespace"),
        ] {
            match validate_command(&refused) {
                Err(LedgerError::Conflict(message)) => assert!(
                    message.contains(expected),
                    "expected {expected:?}, got {message:?}"
                ),
                other => panic!("expected {expected:?}, got {other:?}"),
            }
        }

        // A reservation command additionally refuses a nil reservation.
        let nil_reservation = LedgerCommand::ReserveCreditForOrder {
            header: header(Some(evidence())),
            user_id: user,
            reservation_id: Uuid::nil(),
            quantity: 10,
        };
        assert!(matches!(
            validate_command(&nil_reservation),
            Err(LedgerError::Conflict(message)) if message.contains("non-nil reservation")
        ));

        // Commands Core does not authorize keep their existing preconditions.
        validate_command(&LedgerCommand::RecordFundingPayment {
            header: header(None),
            user_id: user,
            quantity: 10,
            direction: GainLoss::Loss,
        })
        .unwrap();
    }

    #[test]
    fn custody_exit_commands_bind_the_core_action_and_destination_asset() {
        let action_id = Uuid::from_u128(42);
        let custody_evidence = || {
            CoreAuthorization::new(
                action_id.to_string(),
                "authorization-1".into(),
                "recommendation-1".into(),
            )
            .unwrap()
        };
        validate_command(&LedgerCommand::ReserveCustodyExit {
            header: header(Some(custody_evidence())),
            user_id: Uuid::from_u128(1),
            reservation_id: action_id,
            asset: LedgerAsset::Usdc,
            quantity: 10,
            destination: CustodyExitDestination::HyperliquidMargin,
        })
        .unwrap();

        let wrong_action = LedgerCommand::ReserveCustodyExit {
            header: header(Some(custody_evidence())),
            user_id: Uuid::from_u128(1),
            reservation_id: Uuid::from_u128(43),
            asset: LedgerAsset::Usdc,
            quantity: 10,
            destination: CustodyExitDestination::CustodyOutflow,
        };
        assert!(matches!(
            validate_command(&wrong_action),
            Err(LedgerError::Conflict(message)) if message.contains("must equal core_action_id")
        ));

        let non_usdc_margin = LedgerCommand::ReserveCustodyExit {
            header: header(Some(custody_evidence())),
            user_id: Uuid::from_u128(1),
            reservation_id: action_id,
            asset: LedgerAsset::Eth,
            quantity: 10,
            destination: CustodyExitDestination::HyperliquidMargin,
        };
        assert!(matches!(
            validate_command(&non_usdc_margin),
            Err(LedgerError::Conflict(message)) if message.contains("require USDC")
        ));
    }
}

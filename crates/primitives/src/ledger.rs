use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use strum::{AsRefStr, EnumString};
use uuid::Uuid;

use crate::assets::{self, AssetError, AssetInfo};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "UPPERCASE")]
pub enum LedgerAsset {
    Eth,
    /// Bitcoin, held on Ethereum as the WBTC ERC-20 — the registry identity is that contract.
    Btc,
    /// Solana, held on Ethereum as wrapped SOL (Wormhole) — the registry identity is that
    /// contract.
    Sol,
    Usdc,
    /// Native ETH on Base (eip155:8453). Deliberately a distinct accounting asset from mainnet
    /// [`Self::Eth`]: the ledger does not treat chains as fungible, so a Base deposit never
    /// backs a mainnet withdrawal without an explicit bridging action.
    #[serde(rename = "ETH_BASE")]
    EthBase,
    /// Circle-native USDC on Base — distinct from mainnet [`Self::Usdc`] for the same reason.
    #[serde(rename = "USDC_BASE")]
    UsdcBase,
    /// Native SOL on Solana; distinct from the existing Ethereum wrapped-SOL identity.
    #[serde(rename = "SOL_SOLANA")]
    SolSolana,
    /// Circle-native USDC on Solana, with its own accounting balance.
    #[serde(rename = "USDC_SOLANA")]
    UsdcSolana,
    /// Wrapped SOL on Solana.
    Wsol,
    /// Jito liquid staked SOL.
    Jitosol,
    /// Marinade liquid staked SOL.
    Msol,
    /// Jupiter liquid staked SOL.
    Jupsol,
    /// Wrapped Ether on Ethereum.
    Weth,
    /// Lido liquid staked Ether; balances rebase.
    Steth,
    /// Wrapped Lido liquid staked Ether.
    Wsteth,
    /// Coinbase liquid staked Ether.
    Cbeth,
    /// Coinbase wrapped Bitcoin.
    Cbbtc,
    /// Threshold Bitcoin.
    Tbtc,
    /// Lombard yield-bearing Bitcoin.
    Lbtc,
    /// Solv Bitcoin reserve token.
    Solvbtc,
}

impl LedgerAsset {
    /// Every accounting asset. The asset registry is refused at load unless it defines exactly
    /// one entry for each of them.
    pub const ALL: [Self; 20] = [
        Self::Eth,
        Self::Btc,
        Self::Sol,
        Self::Usdc,
        Self::EthBase,
        Self::UsdcBase,
        Self::SolSolana,
        Self::UsdcSolana,
        Self::Wsol,
        Self::Jitosol,
        Self::Msol,
        Self::Jupsol,
        Self::Weth,
        Self::Steth,
        Self::Wsteth,
        Self::Cbeth,
        Self::Cbbtc,
        Self::Tbtc,
        Self::Lbtc,
        Self::Solvbtc,
    ];

    /// The code as stored in the journal and sent on the wire. Intrinsic to the enum rather than
    /// read from the registry: the registry loader compares its symbols against these, and a
    /// registry that could rename an asset could rename what a stored balance means.
    pub const fn code(self) -> &'static str {
        match self {
            Self::Eth => "ETH",
            Self::Btc => "BTC",
            Self::Sol => "SOL",
            Self::Usdc => "USDC",
            Self::EthBase => "ETH_BASE",
            Self::UsdcBase => "USDC_BASE",
            Self::SolSolana => "SOL_SOLANA",
            Self::UsdcSolana => "USDC_SOLANA",
            Self::Wsol => "WSOL",
            Self::Jitosol => "JITOSOL",
            Self::Msol => "MSOL",
            Self::Jupsol => "JUPSOL",
            Self::Weth => "WETH",
            Self::Steth => "STETH",
            Self::Wsteth => "WSTETH",
            Self::Cbeth => "CBETH",
            Self::Cbbtc => "CBBTC",
            Self::Tbtc => "TBTC",
            Self::Lbtc => "LBTC",
            Self::Solvbtc => "SOLVBTC",
        }
    }

    /// Parses an accounting code. `None` for anything else, including a case variant.
    #[must_use]
    pub fn from_symbol(symbol: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|asset| asset.code() == symbol)
    }

    /// Parses an accounting code, failing closed on anything the registry does not define.
    ///
    /// # Errors
    ///
    /// Returns [`AssetError::UnsupportedAsset`] for any other code.
    pub fn from_code(code: &str) -> Result<Self, AssetError> {
        assets::by_symbol(code).map(|entry| entry.asset)
    }

    /// This asset's registry entry: chain, canonical identity, token address, and decimals.
    #[must_use]
    pub fn info(self) -> &'static AssetInfo {
        assets::info(self)
    }

    /// On-chain decimal places, from the locked asset registry. Quantities are always carried in
    /// raw atomic units; this only describes how to present them.
    #[must_use]
    pub fn decimals(self) -> u8 {
        self.info().decimals
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuditMetadata {
    pub actor: String,
    pub reason: String,
    #[serde(default)]
    pub risk_snapshot_id: Option<String>,
    /// Core's evidence for the commands [`LedgerCommand::requires_core_authorization`] answers
    /// true for, and absent on every other command.
    ///
    /// Optional for commands that do not require Core authorization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization: Option<CoreAuthorization>,
    #[serde(default)]
    pub attributes: BTreeMap<String, String>,
}

/// One rule for idempotency keys, enforced at ledger execution and reused by
/// the NATS publish path (keys travel as `Nats-Msg-Id` headers, which reject
/// control characters; the length cap is policy, not a protocol limit).
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 256;

/// The same rule for the identifiers inside [`CoreAuthorization`]. `core_authorization_id` must
/// equal the idempotency key, so the two bounds cannot be allowed to drift apart.
pub const MAX_AUTHORIZATION_ID_BYTES: usize = MAX_IDEMPOTENCY_KEY_BYTES;

/// Shared bounds for internal bearer credentials used between Core and Ledger.
pub const MIN_INTERNAL_TOKEN_BYTES: usize = 32;
pub const MAX_INTERNAL_TOKEN_BYTES: usize = 512;

/// Stored command kinds that participate in the custody-exit workflow.
///
/// Persistence uses this list when applying rules to the workflow as a whole. [`LedgerCommand::kind`]
/// stays the authority for what each variant persists — this list restates those names rather than
/// defining them, because `command_kind` is written into an immutable journal and must never move
/// with the order of a list. `custody_exit_command_kinds_name_their_variants` pins the two together.
///
/// Adding a custody-exit command means extending this list, [`LedgerCommand::custody_exit`], and the
/// migration source-kind constraint together.
pub const CUSTODY_EXIT_COMMAND_KINDS: [&str; 4] = [
    "reserve_custody_exit",
    "draw_credit_to_custody_exit",
    "consume_custody_exit",
    "release_custody_exit",
];

/// Bounded, non-blank, single-line text. Control characters are rejected by byte, matching what
/// `Nats-Msg-Id` headers reject; callers that also render the value to a human should use
/// [`valid_display_text`].
fn valid_bounded_text(value: &str, max_bytes: usize) -> bool {
    !value.trim().is_empty()
        && value.len() <= max_bytes
        && !value.bytes().any(|b| b.is_ascii_control())
}

/// [`valid_bounded_text`] plus the Unicode control characters — C1 among them — that survive the
/// byte check but still corrupt journal and log rendering.
pub fn valid_display_text(value: &str, max_bytes: usize) -> bool {
    valid_bounded_text(value, max_bytes) && !value.chars().any(char::is_control)
}

pub fn valid_idempotency_key(key: &str) -> bool {
    valid_bounded_text(key, MAX_IDEMPOTENCY_KEY_BYTES)
}

/// Core's evidence that a financial command was authorized: the Core action it belongs to, the
/// authorization that approved it, and the synchronous risk evaluation it was weighed against.
///
/// The fields are private, so [`CoreAuthorization::new`] is the only way to build one and
/// deserialization is routed through the same checks. Carrying this type therefore means
/// carrying complete, well-formed evidence, whichever ingress delivered the command.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(try_from = "CoreAuthorizationWire")]
pub struct CoreAuthorization {
    core_action_id: String,
    core_authorization_id: String,
    risk_evaluation_id: String,
}

impl CoreAuthorization {
    /// Builds the evidence, refusing anything that cannot identify a Core decision.
    ///
    /// # Errors
    ///
    /// Returns [`CoreAuthorizationError`] for a blank, oversized, or control-character
    /// identifier.
    pub fn new(
        core_action_id: String,
        core_authorization_id: String,
        risk_evaluation_id: String,
    ) -> Result<Self, CoreAuthorizationError> {
        for (field, value) in [
            ("core_action_id", &core_action_id),
            ("core_authorization_id", &core_authorization_id),
            ("risk_evaluation_id", &risk_evaluation_id),
        ] {
            if !valid_display_text(value, MAX_AUTHORIZATION_ID_BYTES) {
                return Err(CoreAuthorizationError::InvalidIdentifier { field });
            }
        }
        Ok(Self {
            core_action_id,
            core_authorization_id,
            risk_evaluation_id,
        })
    }

    #[must_use]
    pub fn core_action_id(&self) -> &str {
        &self.core_action_id
    }

    #[must_use]
    pub fn core_authorization_id(&self) -> &str {
        &self.core_authorization_id
    }

    #[must_use]
    pub fn risk_evaluation_id(&self) -> &str {
        &self.risk_evaluation_id
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CoreAuthorizationWire {
    core_action_id: String,
    core_authorization_id: String,
    risk_evaluation_id: String,
}

impl TryFrom<CoreAuthorizationWire> for CoreAuthorization {
    type Error = CoreAuthorizationError;

    fn try_from(wire: CoreAuthorizationWire) -> Result<Self, Self::Error> {
        Self::new(
            wire.core_action_id,
            wire.core_authorization_id,
            wire.risk_evaluation_id,
        )
    }
}

/// Core authorization evidence that cannot identify the decision it claims to record.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum CoreAuthorizationError {
    #[error("core authorization {field} is blank, oversized, or contains control characters")]
    InvalidIdentifier { field: &'static str },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommandHeader {
    pub chain_id: u64,
    pub idempotency_key: String,
    #[serde(default)]
    pub external_reference: Option<String>,
    pub effective_at: DateTime<Utc>,
    pub audit: AuditMetadata,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, EnumString)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum LedgerCommandSource {
    Evm,
    Hyperliquid,
    Internal,
}

impl LedgerCommandSource {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Evm => "evm",
            Self::Hyperliquid => "hyperliquid",
            Self::Internal => "internal",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LedgerCommand {
    DrawCredit {
        header: CommandHeader,
        user_id: Uuid,
        #[serde(with = "atomic_quantity")]
        quantity: u128,
        destination: CreditDestination,
    },
    ReserveCustodyExit {
        header: CommandHeader,
        user_id: Uuid,
        reservation_id: Uuid,
        asset: LedgerAsset,
        #[serde(with = "atomic_quantity")]
        quantity: u128,
        destination: CustodyExitDestination,
    },
    DrawCreditToCustodyExit {
        header: CommandHeader,
        user_id: Uuid,
        reservation_id: Uuid,
        #[serde(with = "atomic_quantity")]
        quantity: u128,
        destination: CustodyExitDestination,
    },
    ConsumeCustodyExit {
        header: CommandHeader,
        user_id: Uuid,
        reservation_id: Uuid,
        asset: LedgerAsset,
        #[serde(with = "atomic_quantity")]
        quantity: u128,
        destination: CustodyExitDestination,
    },
    ReleaseCustodyExit {
        header: CommandHeader,
        user_id: Uuid,
        reservation_id: Uuid,
        asset: LedgerAsset,
        #[serde(with = "atomic_quantity")]
        quantity: u128,
        destination: CustodyExitDestination,
    },
    ReserveCreditForOrder {
        header: CommandHeader,
        user_id: Uuid,
        reservation_id: Uuid,
        #[serde(with = "atomic_quantity")]
        quantity: u128,
    },
    ConsumeOrderReservation {
        header: CommandHeader,
        user_id: Uuid,
        reservation_id: Uuid,
        #[serde(with = "atomic_quantity")]
        quantity: u128,
    },
    ReleaseOrderReservation {
        header: CommandHeader,
        user_id: Uuid,
        reservation_id: Uuid,
        #[serde(with = "atomic_quantity")]
        quantity: u128,
    },
    RecordHyperliquidTransfer {
        header: CommandHeader,
        user_id: Uuid,
        #[serde(with = "atomic_quantity")]
        quantity: u128,
        direction: TransferDirection,
    },
    RecordRealizedPnl {
        header: CommandHeader,
        user_id: Uuid,
        #[serde(with = "atomic_quantity")]
        quantity: u128,
        direction: GainLoss,
    },
    RecordFundingPayment {
        header: CommandHeader,
        user_id: Uuid,
        #[serde(with = "atomic_quantity")]
        quantity: u128,
        direction: GainLoss,
    },
    RecordVenueFee {
        header: CommandHeader,
        user_id: Uuid,
        #[serde(with = "atomic_quantity")]
        quantity: u128,
    },
    ReserveWithdrawal {
        header: CommandHeader,
        user_id: Uuid,
        reservation_id: Uuid,
        asset: LedgerAsset,
        #[serde(with = "atomic_quantity")]
        quantity: u128,
    },
    SettleWithdrawal {
        header: CommandHeader,
        user_id: Uuid,
        reservation_id: Uuid,
        asset: LedgerAsset,
        #[serde(with = "atomic_quantity")]
        quantity: u128,
    },
    ReleaseWithdrawal {
        header: CommandHeader,
        user_id: Uuid,
        reservation_id: Uuid,
        asset: LedgerAsset,
        #[serde(with = "atomic_quantity")]
        quantity: u128,
    },
    RecordPrincipalRepayment {
        header: CommandHeader,
        user_id: Uuid,
        #[serde(with = "atomic_quantity")]
        quantity: u128,
    },
    RecordLiquidationProceeds {
        header: CommandHeader,
        user_id: Uuid,
        #[serde(with = "atomic_quantity")]
        quantity: u128,
    },
    RecordCustomerSurplus {
        header: CommandHeader,
        user_id: Uuid,
        #[serde(with = "atomic_quantity")]
        quantity: u128,
    },
    RecordCreditProviderShortfall {
        header: CommandHeader,
        user_id: Uuid,
        #[serde(with = "atomic_quantity")]
        quantity: u128,
    },
    ReverseTransaction {
        header: CommandHeader,
        transaction_id: Uuid,
    },
    ControlledAdjustment {
        header: CommandHeader,
        user_id: Uuid,
        asset: LedgerAsset,
        bucket: BalanceBucket,
        #[serde(with = "atomic_quantity")]
        quantity: u128,
        direction: GainLoss,
        approval_reference: String,
    },
}

impl LedgerCommand {
    pub fn header(&self) -> &CommandHeader {
        match self {
            Self::DrawCredit { header, .. }
            | Self::ReserveCustodyExit { header, .. }
            | Self::DrawCreditToCustodyExit { header, .. }
            | Self::ConsumeCustodyExit { header, .. }
            | Self::ReleaseCustodyExit { header, .. }
            | Self::ReserveCreditForOrder { header, .. }
            | Self::ConsumeOrderReservation { header, .. }
            | Self::ReleaseOrderReservation { header, .. }
            | Self::RecordHyperliquidTransfer { header, .. }
            | Self::RecordRealizedPnl { header, .. }
            | Self::RecordFundingPayment { header, .. }
            | Self::RecordVenueFee { header, .. }
            | Self::ReserveWithdrawal { header, .. }
            | Self::SettleWithdrawal { header, .. }
            | Self::ReleaseWithdrawal { header, .. }
            | Self::RecordPrincipalRepayment { header, .. }
            | Self::RecordLiquidationProceeds { header, .. }
            | Self::RecordCustomerSurplus { header, .. }
            | Self::RecordCreditProviderShortfall { header, .. }
            | Self::ReverseTransaction { header, .. }
            | Self::ControlledAdjustment { header, .. } => header,
        }
    }
    pub fn source(&self) -> LedgerCommandSource {
        match self {
            Self::ReserveWithdrawal { .. }
            | Self::SettleWithdrawal { .. }
            | Self::ReleaseWithdrawal { .. } => LedgerCommandSource::Evm,
            Self::ReserveCreditForOrder { .. }
            | Self::ConsumeOrderReservation { .. }
            | Self::ReleaseOrderReservation { .. }
            | Self::RecordHyperliquidTransfer { .. }
            | Self::RecordRealizedPnl { .. }
            | Self::RecordFundingPayment { .. }
            | Self::RecordVenueFee { .. } => LedgerCommandSource::Hyperliquid,
            Self::DrawCredit { destination, .. } => destination.source(),
            Self::ReserveCustodyExit { destination, .. }
            | Self::DrawCreditToCustodyExit { destination, .. }
            | Self::ConsumeCustodyExit { destination, .. }
            | Self::ReleaseCustodyExit { destination, .. } => destination.source(),
            Self::RecordPrincipalRepayment { .. }
            | Self::RecordLiquidationProceeds { .. }
            | Self::RecordCustomerSurplus { .. }
            | Self::RecordCreditProviderShortfall { .. }
            | Self::ReverseTransaction { .. }
            | Self::ControlledAdjustment { .. } => LedgerCommandSource::Internal,
        }
    }
    /// The user a Core-authorized command books against, or `None` when Core does not authorize
    /// this command. One exhaustive match so the set of authorized commands and the user each
    /// one touches cannot drift apart.
    #[must_use]
    pub fn core_authorized_user(&self) -> Option<Uuid> {
        match self {
            Self::DrawCredit { user_id, .. }
            | Self::ReserveCustodyExit { user_id, .. }
            | Self::DrawCreditToCustodyExit { user_id, .. }
            | Self::ConsumeCustodyExit { user_id, .. }
            | Self::ReleaseCustodyExit { user_id, .. }
            | Self::ReserveCreditForOrder { user_id, .. }
            | Self::ReserveWithdrawal { user_id, .. } => Some(*user_id),
            _ => None,
        }
    }

    /// The reservation a Core-authorized command addresses, when it has one.
    #[must_use]
    pub fn core_authorized_reservation(&self) -> Option<Uuid> {
        match self {
            Self::ReserveCreditForOrder { reservation_id, .. }
            | Self::ReserveCustodyExit { reservation_id, .. }
            | Self::DrawCreditToCustodyExit { reservation_id, .. }
            | Self::ConsumeCustodyExit { reservation_id, .. }
            | Self::ReleaseCustodyExit { reservation_id, .. }
            | Self::ReserveWithdrawal { reservation_id, .. } => Some(*reservation_id),
            _ => None,
        }
    }

    /// Custody-exit facts needed by infrastructure validation, independent of the variant.
    #[must_use]
    pub fn custody_exit(&self) -> Option<CustodyExit> {
        match self {
            Self::ReserveCustodyExit {
                reservation_id,
                asset,
                destination,
                ..
            }
            | Self::ConsumeCustodyExit {
                reservation_id,
                asset,
                destination,
                ..
            }
            | Self::ReleaseCustodyExit {
                reservation_id,
                asset,
                destination,
                ..
            } => Some(CustodyExit {
                reservation_id: *reservation_id,
                asset: Some(*asset),
                destination: *destination,
            }),
            Self::DrawCreditToCustodyExit {
                reservation_id,
                destination,
                ..
            } => Some(CustodyExit {
                reservation_id: *reservation_id,
                asset: None,
                destination: *destination,
            }),
            _ => None,
        }
    }

    /// Whether Core must have authorized this command, which the ledger refuses to execute
    /// without [`AuditMetadata::authorization`]. A property of the command itself, so every
    /// ingress inherits it rather than each transport deciding for itself.
    pub fn requires_core_authorization(&self) -> bool {
        self.core_authorized_user().is_some()
    }
    pub fn kind(&self) -> &'static str {
        match self {
            Self::DrawCredit { .. } => "draw_credit",
            Self::ReserveCustodyExit { .. } => "reserve_custody_exit",
            Self::DrawCreditToCustodyExit { .. } => "draw_credit_to_custody_exit",
            Self::ConsumeCustodyExit { .. } => "consume_custody_exit",
            Self::ReleaseCustodyExit { .. } => "release_custody_exit",
            Self::ReserveCreditForOrder { .. } => "reserve_credit_for_order",
            Self::ConsumeOrderReservation { .. } => "consume_order_reservation",
            Self::ReleaseOrderReservation { .. } => "release_order_reservation",
            Self::RecordHyperliquidTransfer { .. } => "record_hyperliquid_transfer",
            Self::RecordRealizedPnl { .. } => "record_realized_pnl",
            Self::RecordFundingPayment { .. } => "record_funding_payment",
            Self::RecordVenueFee { .. } => "record_venue_fee",
            Self::ReserveWithdrawal { .. } => "reserve_withdrawal",
            Self::SettleWithdrawal { .. } => "settle_withdrawal",
            Self::ReleaseWithdrawal { .. } => "release_withdrawal",
            Self::RecordPrincipalRepayment { .. } => "record_principal_repayment",
            Self::RecordLiquidationProceeds { .. } => "record_liquidation_proceeds",
            Self::RecordCustomerSurplus { .. } => "record_customer_surplus",
            Self::RecordCreditProviderShortfall { .. } => "record_credit_provider_shortfall",
            Self::ReverseTransaction { .. } => "reverse_transaction",
            Self::ControlledAdjustment { .. } => "controlled_adjustment",
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CreditDestination {
    EthereumAvailable,
    HyperliquidMargin,
}

impl CreditDestination {
    pub const fn source(self) -> LedgerCommandSource {
        match self {
            Self::EthereumAvailable => LedgerCommandSource::Evm,
            Self::HyperliquidMargin => LedgerCommandSource::Hyperliquid,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CustodyExit {
    pub reservation_id: Uuid,
    pub asset: Option<LedgerAsset>,
    pub destination: CustodyExitDestination,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CustodyExitDestination {
    CustodyOutflow,
    HyperliquidMargin,
}

impl CustodyExitDestination {
    pub const fn code(self) -> &'static str {
        match self {
            Self::CustodyOutflow => "custody_outflow",
            Self::HyperliquidMargin => "hyperliquid_margin",
        }
    }

    pub const fn source(self) -> LedgerCommandSource {
        match self {
            Self::CustodyOutflow => LedgerCommandSource::Evm,
            Self::HyperliquidMargin => LedgerCommandSource::Hyperliquid,
        }
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TransferDirection {
    ToHyperliquid,
    FromHyperliquid,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GainLoss {
    Gain,
    Loss,
}
#[derive(
    Clone,
    Copy,
    Debug,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    AsRefStr,
    EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum BalanceBucket {
    Pending,
    Available,
    Reserved,
    OrderReserved,
    WithdrawalReserved,
    HyperliquidMargin,
    PrincipalLiability,
    LiquidationProceeds,
    CustomerSurplus,
    ProviderShortfall,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommandResult {
    pub transaction_id: Uuid,
    pub duplicate: bool,
    pub recorded_at: DateTime<Utc>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LedgerBalance {
    pub user_id: Uuid,
    pub asset: LedgerAsset,
    pub bucket: BalanceBucket,
    #[serde(with = "atomic_signed_quantity")]
    pub quantity: i128,
    pub as_of: DateTime<Utc>,
    pub time_basis: BalanceTimeBasis,
}

/// Current accounting totals read together, without netting debits against credits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UserLedgerBalanceSnapshot {
    /// Database statement time, not an external-ingestion watermark.
    pub as_of: DateTime<Utc>,
    /// Stored totals for each asset and bucket belonging to the queried user.
    pub balances: Vec<LedgerBalanceTotals>,
}

/// Cumulative posting totals for one user's asset and bucket, in atomic units.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LedgerBalanceTotals {
    /// Asset whose atomic units denominate both posting totals.
    pub asset: LedgerAsset,
    /// Accounting bucket to which both posting totals belong.
    pub bucket: BalanceBucket,
    /// Cumulative debit postings in atomic units, before subtracting credits.
    pub debit_quantity: u128,
    /// Cumulative credit postings in atomic units, before subtracting debits.
    pub credit_quantity: u128,
    /// Last committed update to these stored totals.
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BalanceTimeBasis {
    #[default]
    Recorded,
    Effective,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LedgerExport {
    pub exported_at: DateTime<Utc>,
    pub transactions: Vec<ExportTransaction>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ExportTransaction {
    pub id: Uuid,
    pub source: LedgerCommandSource,
    pub chain_id: u64,
    pub command_kind: String,
    pub idempotency_key: String,
    pub external_reference: Option<String>,
    pub recorded_at: DateTime<Utc>,
    pub effective_at: DateTime<Utc>,
    pub reverses_transaction_id: Option<Uuid>,
    pub audit: Value,
    pub postings: Vec<ExportPosting>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExportPosting {
    pub account_code: String,
    pub asset: LedgerAsset,
    pub side: PostingSide,
    #[serde(with = "atomic_quantity")]
    pub quantity: u128,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, AsRefStr, EnumString)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum PostingSide {
    Debit,
    Credit,
}

mod atomic_quantity {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub fn serialize<S: Serializer>(value: &u128, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u128, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Value {
            Text(String),
            Integer(u64),
        }
        match Value::deserialize(deserializer)? {
            Value::Text(value) => value.parse().map_err(D::Error::custom),
            Value::Integer(value) => Ok(u128::from(value)),
        }
    }
}

mod atomic_signed_quantity {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub fn serialize<S: Serializer>(value: &i128, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i128, D::Error> {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn ledger_enum_strings_match_existing_wire_values() {
        fn check<T>(cases: &[(T, &str)])
        where
            T: AsRef<str> + std::str::FromStr + Serialize + std::fmt::Debug + PartialEq,
            T::Err: std::fmt::Debug,
        {
            for (value, text) in cases {
                assert_eq!(value.as_ref(), *text);
                assert_eq!(&text.parse::<T>().unwrap(), value);
                assert_eq!(serde_json::to_value(value).unwrap(), json!(text));
                assert!(text.to_uppercase().parse::<T>().is_err());
                assert!(format!(" {text}").parse::<T>().is_err());
            }
            assert!("unknown".parse::<T>().is_err());
        }
        check(&[
            (BalanceBucket::Pending, "pending"),
            (BalanceBucket::Available, "available"),
            (BalanceBucket::Reserved, "reserved"),
            (BalanceBucket::OrderReserved, "order_reserved"),
            (BalanceBucket::WithdrawalReserved, "withdrawal_reserved"),
            (BalanceBucket::HyperliquidMargin, "hyperliquid_margin"),
            (BalanceBucket::PrincipalLiability, "principal_liability"),
            (BalanceBucket::LiquidationProceeds, "liquidation_proceeds"),
            (BalanceBucket::CustomerSurplus, "customer_surplus"),
            (BalanceBucket::ProviderShortfall, "provider_shortfall"),
        ]);
        check(&[
            (PostingSide::Debit, "debit"),
            (PostingSide::Credit, "credit"),
        ]);
        for source in [
            LedgerCommandSource::Evm,
            LedgerCommandSource::Hyperliquid,
            LedgerCommandSource::Internal,
        ] {
            assert_eq!(
                source.code().parse::<LedgerCommandSource>().unwrap(),
                source
            );
            assert_eq!(serde_json::to_value(source).unwrap(), json!(source.code()));
            assert!(
                source
                    .code()
                    .to_uppercase()
                    .parse::<LedgerCommandSource>()
                    .is_err()
            );
        }
        assert!("unknown".parse::<LedgerCommandSource>().is_err());
    }

    fn command(kind: &str, chain_id: Option<u64>, fields: Value) -> Value {
        let mut header = json!({
            "idempotency_key":"test-command",
            "effective_at":"2026-08-04T12:00:00Z",
            "audit":{"actor":"test","reason":"template classification"}
        });
        if let Some(chain_id) = chain_id {
            header["chain_id"] = json!(chain_id);
        }
        let mut value = json!({"type":kind,"header":header});
        value
            .as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        value
    }

    /// [`LedgerCommand::kind`] names the string persisted into the immutable
    /// `ledger.transactions.command_kind`; [`CUSTODY_EXIT_COMMAND_KINDS`] is the list persistence
    /// applies workflow-wide rules to, such as refusing reversal. The two are written separately,
    /// so pin the mapping: a name that drifted would relabel committed history, and a custody-exit
    /// command absent from the list would silently escape those rules.
    #[test]
    fn custody_exit_command_kinds_name_their_variants() {
        let fields = json!({
            "user_id":"11111111-1111-4111-8111-111111111111",
            "reservation_id":"22222222-2222-4222-8222-222222222222",
            "asset":"USDC",
            "quantity":"1",
            "destination":"custody_outflow"
        });
        for kind in CUSTODY_EXIT_COMMAND_KINDS {
            let parsed: LedgerCommand =
                serde_json::from_value(command(kind, Some(1), fields.clone()))
                    .expect("every listed kind names a custody-exit command");
            assert_eq!(
                parsed.kind(),
                kind,
                "{kind} does not round-trip through kind()"
            );
            assert!(
                parsed.custody_exit().is_some(),
                "{kind} is listed as a custody exit but custody_exit() does not classify it"
            );
        }
    }

    #[test]
    fn incoming_commands_require_chain_id_and_have_disjoint_sources() {
        let user = "11111111-1111-4111-8111-111111111111";
        let evm: LedgerCommand = serde_json::from_value(command(
            "settle_withdrawal",
            Some(1),
            json!({"user_id":user,"reservation_id":user,"asset":"ETH","quantity":"1"}),
        ))
        .unwrap();
        assert_eq!(evm.source(), LedgerCommandSource::Evm);

        let hyperliquid: LedgerCommand = serde_json::from_value(command(
            "record_funding_payment",
            Some(42161),
            json!({"user_id":user,"quantity":"1","direction":"loss"}),
        ))
        .unwrap();
        assert_eq!(hyperliquid.source(), LedgerCommandSource::Hyperliquid);
        assert_eq!(hyperliquid.header().chain_id, 42161);

        let missing_chain = command(
            "settle_withdrawal",
            None,
            json!({"user_id":user,"reservation_id":user,"asset":"ETH","quantity":"1"}),
        );
        assert!(serde_json::from_value::<LedgerCommand>(missing_chain).is_err());
    }

    #[test]
    fn deposit_lifecycle_commands_are_not_accepted() {
        for kind in [
            "record_pending_deposit",
            "finalize_deposit",
            "reject_pending_deposit",
        ] {
            let wire = command(
                kind,
                Some(1),
                json!({
                    "user_id":"11111111-1111-4111-8111-111111111111",
                    "asset":"ETH", "quantity":"1"
                }),
            );
            let error = serde_json::from_value::<LedgerCommand>(wire).unwrap_err();
            assert!(error.to_string().contains("unknown variant"), "{error}");
        }
    }

    #[test]
    fn core_authorization_refuses_evidence_that_identifies_nothing() {
        let evidence = |action: &str| {
            CoreAuthorization::new(
                action.to_owned(),
                "authorization-1".to_owned(),
                "recommendation-1".to_owned(),
            )
        };
        assert_eq!(evidence("action-1").unwrap().core_action_id(), "action-1");
        assert_eq!(
            evidence("  "),
            Err(CoreAuthorizationError::InvalidIdentifier {
                field: "core_action_id"
            })
        );
        assert_eq!(
            evidence("action\n1"),
            Err(CoreAuthorizationError::InvalidIdentifier {
                field: "core_action_id"
            })
        );
        assert_eq!(
            evidence(&"a".repeat(MAX_AUTHORIZATION_ID_BYTES + 1)),
            Err(CoreAuthorizationError::InvalidIdentifier {
                field: "core_action_id"
            })
        );
    }

    #[test]
    fn authorization_is_optional_on_the_wire_and_required_by_command_kind() {
        let user = "11111111-1111-4111-8111-111111111111";
        let settlement: LedgerCommand = serde_json::from_value(command(
            "settle_withdrawal",
            Some(1),
            json!({"user_id":user,"reservation_id":user,"asset":"ETH","quantity":"1"}),
        ))
        .unwrap();
        assert!(!settlement.requires_core_authorization());
        assert!(settlement.header().audit.authorization.is_none());
        // Audit JSON is unchanged for commands without evidence, so an in-flight idempotency
        // key still hashes to the value it was committed under.
        assert_eq!(
            serde_json::to_value(&settlement.header().audit).unwrap(),
            json!({
                "actor":"test",
                "reason":"template classification",
                "risk_snapshot_id":null,
                "attributes":{}
            })
        );

        let mut authorized = command(
            "draw_credit",
            Some(1),
            json!({"user_id":user,"quantity":"1","destination":"ethereum_available"}),
        );
        authorized["header"]["audit"]["authorization"] = json!({
            "core_action_id":"action-1",
            "core_authorization_id":"authorization-1",
            "risk_evaluation_id":"evaluation-1",
        });
        let draw: LedgerCommand = serde_json::from_value(authorized.clone()).unwrap();
        assert!(draw.requires_core_authorization());
        let authorization = draw.header().audit.authorization.as_ref().unwrap();
        assert_eq!(authorization.core_action_id(), "action-1");
        assert_eq!(authorization.risk_evaluation_id(), "evaluation-1");

        authorized["header"]["audit"]["authorization"]["risk_evaluation_id"] = json!("");
        assert!(serde_json::from_value::<LedgerCommand>(authorized).is_err());
    }
}

use serde::Serialize;
use strum::AsRefStr;
use tackt_primitives::ledger::{CustodyExitDestination, LedgerAsset};
use uuid::Uuid;

#[derive(Clone, Copy, PartialEq, AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum DepositPhase {
    Pending,
    Finalized,
    Rejected,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReservationOperation {
    Create,
    Consume,
    Settle,
    Release,
}

impl ReservationOperation {
    pub fn terminal_status(self) -> &'static str {
        match self {
            Self::Consume | Self::Settle => "consumed",
            Self::Release => "released",
            Self::Create => "open",
        }
    }
}

#[derive(Clone, Copy, Serialize, PartialEq, AsRefStr)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub(crate) enum ReservationKind {
    Order,
    Withdrawal,
    CustodyExit,
}

#[derive(Clone, Copy, Serialize)]
pub(crate) struct ReservationChange {
    pub operation: ReservationOperation,
    pub id: Uuid,
    pub user_id: Uuid,
    pub kind: ReservationKind,
    pub asset: LedgerAsset,
    #[serde(serialize_with = "serialize_quantity")]
    pub quantity: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination: Option<CustodyExitDestination>,
}

fn serialize_quantity<S: serde::Serializer>(
    quantity: &u128,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&quantity.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_codes_keep_existing_journal_and_reservation_values() {
        assert_eq!(DepositPhase::Pending.as_ref(), "pending");
        assert_eq!(DepositPhase::Finalized.as_ref(), "finalized");
        assert_eq!(DepositPhase::Rejected.as_ref(), "rejected");
        for (kind, code) in [
            (ReservationKind::Order, "order"),
            (ReservationKind::Withdrawal, "withdrawal"),
            (ReservationKind::CustodyExit, "custody_exit"),
        ] {
            assert_eq!(kind.as_ref(), code);
            assert_eq!(serde_json::to_value(kind).unwrap(), serde_json::json!(code));
        }
    }
}

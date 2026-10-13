//! Wire bindings for the risk streams. Payload types live in this crate (the
//! dependency direction forbids `tackt-nats` from referencing them); the
//! envelope, subjects, and stream specs come from `tackt-nats`.

use risk_engine::ingest::IngestEvent;
use risk_engine::processing::{
    AccountPortfolio, EvaluationInput, PortfolioGeneration, PriceUpdate, ProjectedAccount,
    StateVersion,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tackt_primitives::accounts::AccountSubject;
use uuid::Uuid;

/// Strict wire request for one synchronous evaluation.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RiskEvaluationRequest {
    pub action_id: Uuid,
    pub account: AccountSubject,
    pub account_version: StateVersion,
    pub input_version: StateVersion,
    pub projected_account: ProjectedAccount,
}

impl From<RiskEvaluationRequest> for EvaluationInput {
    fn from(request: RiskEvaluationRequest) -> Self {
        Self {
            action_id: request.action_id,
            account: request.account,
            account_version: request.account_version,
            input_version: request.input_version,
            projected_account: request.projected_account,
        }
    }
}

/// Envelope wrapping one risk payload.
pub type Envelope<T> = tackt_nats::Envelope<T>;

/// Which ingress stream a message arrived on; decides its payload type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamKind {
    /// `PriceUpdate` payloads.
    Prices,
    /// `AccountPortfolio` patch payloads.
    Patches,
    /// `PortfolioGeneration` reconciliation sweeps.
    Sweeps,
}

/// Re-materialize an ingest event from a retained payload: `ApplyLoop::apply`
/// consumes its event even on rejection and `IngestEvent` is not `Clone`.
pub fn decode(kind: StreamKind, payload: &[u8]) -> anyhow::Result<IngestEvent> {
    fn payload_of<T: DeserializeOwned>(bytes: &[u8]) -> anyhow::Result<T> {
        Ok(serde_json::from_slice::<Envelope<T>>(bytes)?.payload)
    }
    Ok(match kind {
        StreamKind::Prices => IngestEvent::Price(payload_of::<PriceUpdate>(payload)?),
        StreamKind::Patches => IngestEvent::AccountPatch(payload_of::<AccountPortfolio>(payload)?),
        StreamKind::Sweeps => IngestEvent::Portfolio(payload_of::<PortfolioGeneration>(payload)?),
    })
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use risk_engine::processing::HyperliquidSnapshot;
    use tackt_nats::prices::{
        MarketSymbol, PriceAuthority, PriceEvidence, PriceEvidenceEnvelope, PriceQuote,
        PriceSubject,
    };
    use tackt_primitives::Provenance;

    use super::{StreamKind, decode};

    #[test]
    fn durable_nested_payloads_accept_additive_fields() {
        let snapshot = serde_json::json!({
            "account_value": 1,
            "maintenance": 1,
            "source_sequence": 1,
            "future_field": true
        });
        serde_json::from_value::<HyperliquidSnapshot>(snapshot).expect("additive snapshot field");
    }

    #[test]
    fn price_evidence_never_decodes_as_an_executable_price_update() {
        // The two price contracts are deliberately disjoint: evidence describes what a source
        // said, `PriceUpdate` is what this engine may act on. Even a misrouted publish must fail
        // to decode rather than enter the engine as an executable price.
        let evidence = PriceEvidence::new(
            PriceAuthority::Hyperliquid,
            PriceSubject::Market(MarketSymbol::new("BTC").expect("symbol")),
            PriceQuote::new(6_500_000_000_000, 8).expect("quote"),
            2181,
            Utc.timestamp_opt(1_800_000_000, 0).single().expect("time"),
            Utc.timestamp_opt(1_800_000_001, 0).single().expect("time"),
            Provenance {
                endpoint: "https://api.hyperliquid.xyz/info".to_string(),
                method: "allMids".to_string(),
                source_id: "mids:2181".to_string(),
            },
            serde_json::json!({ "mid": "65000.0" }),
        )
        .expect("evidence");
        let payload = serde_json::to_vec(&PriceEvidenceEnvelope::new(evidence)).expect("serialize");

        for kind in [StreamKind::Prices, StreamKind::Patches, StreamKind::Sweeps] {
            assert!(decode(kind, &payload).is_err(), "{kind:?} decoded evidence");
        }
    }
}

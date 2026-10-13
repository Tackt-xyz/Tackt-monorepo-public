//! Durable advisory Risk Engine recommendations.
//!
//! This module owns the complete transport contract. The payload cannot execute anything and is
//! intentionally disjoint from Ledger, signing, funding, and trading command types.

use tackt_primitives::risk::RiskRecommendation;

use crate::{Consumer, ConsumerSpec, Envelope, NatsClient, StreamSpec};

/// Typed envelope for one advisory recommendation.
pub type RiskRecommendationEnvelope = Envelope<RiskRecommendation>;

impl NatsClient {
    /// Publishes a recommendation durably using its occurrence ID for broker dedupe.
    pub async fn publish_risk_recommendation(
        &self,
        stream: &StreamSpec,
        recommendation: &RiskRecommendation,
    ) -> anyhow::Result<()> {
        let envelope = RiskRecommendationEnvelope::new(recommendation.clone());
        // Discarded deliberately: republishing one recommendation identity is
        // the engine repeating advice it already gave, and a deduplicated
        // publish carries that same meaning downstream.
        self.publish_durable(
            stream.subject(),
            Some(recommendation.recommendation_id()),
            &envelope,
        )
        .await?;
        Ok(())
    }

    /// Creates a durable consumer of advisory recommendations.
    pub async fn risk_recommendations(
        &self,
        stream: &StreamSpec,
        durable_name: &str,
    ) -> anyhow::Result<Consumer<RiskRecommendationEnvelope>> {
        self.consumer(stream, ConsumerSpec::durable(durable_name))
            .await
    }
}

#[cfg(test)]
mod tests {
    use tackt_primitives::{
        accounts::AccountSubject,
        risk::{RecommendationReason, RiskClassification, RiskMeasurements, RiskRecommendation},
    };
    use uuid::Uuid;

    use super::*;

    #[test]
    fn recommendation_wire_shape_is_stable_and_advisory() {
        let recommendation = RiskRecommendation::new(
            AccountSubject::new(Uuid::from_u128(1)).expect("subject"),
            RiskClassification::Healthy,
            RiskClassification::Watch,
            RecommendationReason::HealthBelowWarning,
            RiskMeasurements::new(100, 0, 50, 1_100_000, 1_100_000).expect("measurements"),
            3,
            4,
        )
        .expect("recommendation");
        let envelope = RiskRecommendationEnvelope::new(recommendation.clone());
        let wire = serde_json::to_value(&envelope).expect("serialize");
        assert_eq!(wire["payload"]["account"], Uuid::from_u128(1).to_string());
        assert_eq!(wire["payload"]["classification"], "watch");
        assert!(wire["payload"].get("command").is_none());
        let decoded: RiskRecommendationEnvelope =
            serde_json::from_value(wire).expect("deserialize");
        assert_eq!(decoded.payload, recommendation);
    }
}

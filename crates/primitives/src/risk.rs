//! Shared advisory risk recommendation contract.
//!
//! These values describe deterministic evidence. They are deliberately not executable commands,
//! authorization decisions, or signing capabilities.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::accounts::AccountSubject;

/// Account-level risk classification. Its text form matches the serde wire name.
#[derive(
    Clone,
    Copy,
    Debug,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    Hash,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum RiskClassification {
    Healthy,
    Watch,
    Liquidatable,
    DataIncomplete,
}

/// Evidence explaining a recommendation.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RecommendationReason {
    HealthBelowWarning,
    HealthBelowLiquidation,
    MissingInput,
    Recovered,
}

/// Why a point-in-time financial evaluation received its classification.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationReason {
    AboveWarningThreshold,
    HealthBelowWarning,
    HealthBelowLiquidation,
    MissingInput,
}

/// Versioned engine inputs that make an evaluation reproducible.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EvaluationVersions {
    /// Committed account version on which the projection is based.
    pub account_version: u64,
    /// Caller-owned projected-input version.
    pub input_version: u64,
    /// Last committed engine processing epoch observed by the evaluation.
    pub processing_epoch: u64,
    /// Last committed Ethereum price sequence, if one exists.
    pub ethereum_price_sequence: Option<u64>,
    /// Last committed Hyperliquid price sequence, if one exists.
    pub hyperliquid_price_sequence: Option<u64>,
}

/// Deterministic, read-only evidence returned for one proposed financial action.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(try_from = "RiskEvaluationWire")]
pub struct RiskEvaluation {
    evaluation_id: String,
    action_id: Uuid,
    account: AccountSubject,
    input_digest: String,
    classification: RiskClassification,
    reason: EvaluationReason,
    measurements: RiskMeasurements,
    uncertain_inputs: u32,
    versions: EvaluationVersions,
}

impl RiskEvaluation {
    /// Constructs and deterministically identifies evaluation evidence.
    ///
    /// # Errors
    ///
    /// Rejects nil action IDs, malformed digests, zero runtime revisions,
    /// invalid price sequences, and inconsistent classification/reason pairs.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        action_id: Uuid,
        account: AccountSubject,
        input_digest: [u8; 32],
        classification: RiskClassification,
        reason: EvaluationReason,
        measurements: RiskMeasurements,
        uncertain_inputs: u32,
        versions: EvaluationVersions,
    ) -> Result<Self, RecommendationError> {
        if action_id.is_nil() {
            return Err(RecommendationError::NilActionId);
        }
        validate_evaluation_versions(&versions)?;
        if !evaluation_reason_matches(classification, reason) {
            return Err(RecommendationError::EvaluationReasonMismatch {
                classification,
                reason,
            });
        }
        if (uncertain_inputs > 0) != (classification == RiskClassification::DataIncomplete) {
            return Err(RecommendationError::EvaluationUncertaintyMismatch {
                classification,
                uncertain_inputs,
            });
        }
        let input_digest = hex::encode(input_digest);
        let evaluation_id = evaluation_id(
            action_id,
            account,
            &input_digest,
            classification,
            reason,
            measurements,
            uncertain_inputs,
            &versions,
        );
        Ok(Self {
            evaluation_id,
            action_id,
            account,
            input_digest,
            classification,
            reason,
            measurements,
            uncertain_inputs,
            versions,
        })
    }

    #[must_use]
    pub fn evaluation_id(&self) -> &str {
        &self.evaluation_id
    }

    #[must_use]
    pub const fn action_id(&self) -> Uuid {
        self.action_id
    }

    #[must_use]
    pub const fn account(&self) -> AccountSubject {
        self.account
    }

    #[must_use]
    pub fn input_digest(&self) -> &str {
        &self.input_digest
    }

    #[must_use]
    pub const fn classification(&self) -> RiskClassification {
        self.classification
    }

    #[must_use]
    pub const fn reason(&self) -> EvaluationReason {
        self.reason
    }

    #[must_use]
    pub const fn measurements(&self) -> RiskMeasurements {
        self.measurements
    }

    #[must_use]
    pub const fn uncertain_inputs(&self) -> u32 {
        self.uncertain_inputs
    }

    #[must_use]
    pub const fn versions(&self) -> &EvaluationVersions {
        &self.versions
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RiskEvaluationWire {
    evaluation_id: String,
    action_id: Uuid,
    account: AccountSubject,
    input_digest: String,
    classification: RiskClassification,
    reason: EvaluationReason,
    measurements: RiskMeasurements,
    uncertain_inputs: u32,
    versions: EvaluationVersions,
}

impl TryFrom<RiskEvaluationWire> for RiskEvaluation {
    type Error = RecommendationError;

    fn try_from(wire: RiskEvaluationWire) -> Result<Self, Self::Error> {
        let digest = decode_digest("input_digest", &wire.input_digest)?;
        let evaluation = Self::new(
            wire.action_id,
            wire.account,
            digest,
            wire.classification,
            wire.reason,
            wire.measurements,
            wire.uncertain_inputs,
            wire.versions,
        )?;
        if evaluation.evaluation_id != wire.evaluation_id {
            return Err(RecommendationError::EvaluationIdMismatch);
        }
        Ok(evaluation)
    }
}

/// Measured fixed-point values supporting a recommendation.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(try_from = "RiskMeasurementsWire")]
pub struct RiskMeasurements {
    collateral_value: i128,
    debt_value: i128,
    maintenance_value: i128,
    health: i128,
    minimum_margin_health: i128,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RiskMeasurementsWire {
    collateral_value: i128,
    debt_value: i128,
    maintenance_value: i128,
    health: i128,
    minimum_margin_health: i128,
}

impl TryFrom<RiskMeasurementsWire> for RiskMeasurements {
    type Error = RecommendationError;

    fn try_from(wire: RiskMeasurementsWire) -> Result<Self, Self::Error> {
        Self::new(
            wire.collateral_value,
            wire.debt_value,
            wire.maintenance_value,
            wire.health,
            wire.minimum_margin_health,
        )
    }
}

impl RiskMeasurements {
    /// Creates validated measurements. Collateral, debt, and maintenance are non-negative.
    ///
    /// # Errors
    ///
    /// Returns [`RecommendationError::NegativeValue`] for a negative unsigned aggregate.
    pub fn new(
        collateral_value: i128,
        debt_value: i128,
        maintenance_value: i128,
        health: i128,
        minimum_margin_health: i128,
    ) -> Result<Self, RecommendationError> {
        for (field, value) in [
            ("collateral_value", collateral_value),
            ("debt_value", debt_value),
            ("maintenance_value", maintenance_value),
        ] {
            if value < 0 {
                return Err(RecommendationError::NegativeValue { field });
            }
        }
        Ok(Self {
            collateral_value,
            debt_value,
            maintenance_value,
            health,
            minimum_margin_health,
        })
    }

    #[must_use]
    pub const fn collateral_value(self) -> i128 {
        self.collateral_value
    }

    #[must_use]
    pub const fn debt_value(self) -> i128 {
        self.debt_value
    }

    #[must_use]
    pub const fn maintenance_value(self) -> i128 {
        self.maintenance_value
    }

    #[must_use]
    pub const fn health(self) -> i128 {
        self.health
    }

    #[must_use]
    pub const fn minimum_margin_health(self) -> i128 {
        self.minimum_margin_health
    }
}

/// Deterministic, non-executable recommendation derived by the Risk Engine.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(try_from = "RiskRecommendationWire")]
pub struct RiskRecommendation {
    recommendation_id: String,
    account: AccountSubject,
    previous_classification: RiskClassification,
    classification: RiskClassification,
    reason: RecommendationReason,
    measurements: RiskMeasurements,
    portfolio_version: u64,
    processing_epoch: u64,
}

impl RiskRecommendation {
    /// Builds and deterministically identifies one advisory recommendation.
    ///
    /// # Errors
    ///
    /// Fails on zero portfolio revisions or an inconsistent
    /// classification/reason pair.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        account: AccountSubject,
        previous_classification: RiskClassification,
        classification: RiskClassification,
        reason: RecommendationReason,
        measurements: RiskMeasurements,
        portfolio_version: u64,
        processing_epoch: u64,
    ) -> Result<Self, RecommendationError> {
        if portfolio_version == 0 {
            return Err(RecommendationError::ZeroVersion {
                field: "portfolio_version",
            });
        }
        if !reason_matches(classification, reason) {
            return Err(RecommendationError::ReasonMismatch {
                classification,
                reason,
            });
        }
        let recommendation_id = recommendation_id(
            account,
            previous_classification,
            classification,
            reason,
            measurements,
            portfolio_version,
            processing_epoch,
        );
        Ok(Self {
            recommendation_id,
            account,
            previous_classification,
            classification,
            reason,
            measurements,
            portfolio_version,
            processing_epoch,
        })
    }

    #[must_use]
    pub fn recommendation_id(&self) -> &str {
        &self.recommendation_id
    }

    #[must_use]
    pub const fn account(&self) -> AccountSubject {
        self.account
    }

    #[must_use]
    pub const fn previous_classification(&self) -> RiskClassification {
        self.previous_classification
    }

    #[must_use]
    pub const fn classification(&self) -> RiskClassification {
        self.classification
    }

    #[must_use]
    pub const fn reason(&self) -> RecommendationReason {
        self.reason
    }

    #[must_use]
    pub const fn measurements(&self) -> RiskMeasurements {
        self.measurements
    }

    #[must_use]
    pub const fn portfolio_version(&self) -> u64 {
        self.portfolio_version
    }

    /// Monotonic committed engine epoch that distinguishes repeated,
    /// byte-identical transitions within the broker duplicate window.
    #[must_use]
    pub const fn processing_epoch(&self) -> u64 {
        self.processing_epoch
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RiskRecommendationWire {
    recommendation_id: String,
    account: AccountSubject,
    previous_classification: RiskClassification,
    classification: RiskClassification,
    reason: RecommendationReason,
    measurements: RiskMeasurements,
    portfolio_version: u64,
    processing_epoch: u64,
}

impl TryFrom<RiskRecommendationWire> for RiskRecommendation {
    type Error = RecommendationError;

    fn try_from(wire: RiskRecommendationWire) -> Result<Self, Self::Error> {
        let recommendation = Self::new(
            wire.account,
            wire.previous_classification,
            wire.classification,
            wire.reason,
            wire.measurements,
            wire.portfolio_version,
            wire.processing_epoch,
        )?;
        if recommendation.recommendation_id != wire.recommendation_id {
            return Err(RecommendationError::RecommendationIdMismatch);
        }
        Ok(recommendation)
    }
}

/// Invalid or forged advisory recommendation.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum RecommendationError {
    #[error("{field} must not be negative")]
    NegativeValue { field: &'static str },
    #[error("{field} must be greater than zero")]
    ZeroVersion { field: &'static str },
    #[error("recommendation reason {reason:?} is inconsistent with {classification:?}")]
    ReasonMismatch {
        classification: RiskClassification,
        reason: RecommendationReason,
    },
    #[error("recommendation_id is inconsistent with the recommendation fields")]
    RecommendationIdMismatch,
    #[error("action_id must not be nil")]
    NilActionId,
    #[error("{field} must be a 32-byte lowercase hexadecimal digest")]
    InvalidDigest { field: &'static str },
    #[error("evaluation reason {reason:?} is inconsistent with {classification:?}")]
    EvaluationReasonMismatch {
        classification: RiskClassification,
        reason: EvaluationReason,
    },
    #[error("uncertain input count {uncertain_inputs} is inconsistent with {classification:?}")]
    EvaluationUncertaintyMismatch {
        classification: RiskClassification,
        uncertain_inputs: u32,
    },
    #[error("evaluation_id is inconsistent with the evaluation fields")]
    EvaluationIdMismatch,
}

fn validate_evaluation_versions(versions: &EvaluationVersions) -> Result<(), RecommendationError> {
    for (field, value) in [
        ("account_version", versions.account_version),
        ("input_version", versions.input_version),
    ] {
        if value == 0 {
            return Err(RecommendationError::ZeroVersion { field });
        }
    }
    for (field, value) in [
        ("ethereum_price_sequence", versions.ethereum_price_sequence),
        (
            "hyperliquid_price_sequence",
            versions.hyperliquid_price_sequence,
        ),
    ] {
        if value == Some(0) {
            return Err(RecommendationError::ZeroVersion { field });
        }
    }
    Ok(())
}

fn decode_digest(field: &'static str, encoded: &str) -> Result<[u8; 32], RecommendationError> {
    crate::decode_sha256_hex(encoded).ok_or(RecommendationError::InvalidDigest { field })
}

const fn evaluation_reason_matches(
    classification: RiskClassification,
    reason: EvaluationReason,
) -> bool {
    matches!(
        (classification, reason),
        (
            RiskClassification::Healthy,
            EvaluationReason::AboveWarningThreshold
        ) | (
            RiskClassification::Watch,
            EvaluationReason::HealthBelowWarning
        ) | (
            RiskClassification::Liquidatable,
            EvaluationReason::HealthBelowLiquidation
        ) | (
            RiskClassification::DataIncomplete,
            EvaluationReason::MissingInput
        )
    )
}

#[allow(clippy::too_many_arguments)]
fn evaluation_id(
    action_id: Uuid,
    account: AccountSubject,
    input_digest: &str,
    classification: RiskClassification,
    reason: EvaluationReason,
    measurements: RiskMeasurements,
    uncertain_inputs: u32,
    versions: &EvaluationVersions,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"tackt-risk-evaluation");
    digest.update(action_id.as_bytes());
    digest.update(account.user_id().as_bytes());
    digest.update(input_digest.as_bytes());
    digest.update([classification_code(classification)]);
    digest.update([evaluation_reason_code(reason)]);
    for value in [
        measurements.collateral_value,
        measurements.debt_value,
        measurements.maintenance_value,
        measurements.health,
        measurements.minimum_margin_health,
    ] {
        digest.update(value.to_be_bytes());
    }
    digest.update(uncertain_inputs.to_be_bytes());
    digest.update(versions.account_version.to_be_bytes());
    digest.update(versions.input_version.to_be_bytes());
    digest.update(versions.processing_epoch.to_be_bytes());
    encode_optional_version(&mut digest, versions.ethereum_price_sequence);
    encode_optional_version(&mut digest, versions.hyperliquid_price_sequence);
    format!("riskeval_{}", hex::encode(digest.finalize()))
}

fn encode_optional_version(digest: &mut Sha256, value: Option<u64>) {
    digest.update([u8::from(value.is_some())]);
    digest.update(value.unwrap_or_default().to_be_bytes());
}

const fn evaluation_reason_code(reason: EvaluationReason) -> u8 {
    match reason {
        EvaluationReason::AboveWarningThreshold => 0,
        EvaluationReason::HealthBelowWarning => 1,
        EvaluationReason::HealthBelowLiquidation => 2,
        EvaluationReason::MissingInput => 3,
    }
}

const fn reason_matches(classification: RiskClassification, reason: RecommendationReason) -> bool {
    matches!(
        (classification, reason),
        (RiskClassification::Healthy, RecommendationReason::Recovered)
            | (
                RiskClassification::Watch,
                RecommendationReason::HealthBelowWarning
            )
            | (
                RiskClassification::Liquidatable,
                RecommendationReason::HealthBelowLiquidation
            )
            | (
                RiskClassification::DataIncomplete,
                RecommendationReason::MissingInput
            )
    )
}

#[allow(clippy::too_many_arguments)]
fn recommendation_id(
    account: AccountSubject,
    previous_classification: RiskClassification,
    classification: RiskClassification,
    reason: RecommendationReason,
    measurements: RiskMeasurements,
    portfolio_version: u64,
    processing_epoch: u64,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"tackt-risk-recommendation");
    digest.update(account.user_id().as_bytes());
    digest.update([classification_code(previous_classification)]);
    digest.update([classification_code(classification)]);
    digest.update([reason_code(reason)]);
    for value in [
        measurements.collateral_value,
        measurements.debt_value,
        measurements.maintenance_value,
        measurements.health,
        measurements.minimum_margin_health,
    ] {
        digest.update(value.to_be_bytes());
    }
    digest.update(portfolio_version.to_be_bytes());
    digest.update(processing_epoch.to_be_bytes());
    format!("riskrec_{}", hex::encode(digest.finalize()))
}

const fn classification_code(value: RiskClassification) -> u8 {
    match value {
        RiskClassification::Healthy => 0,
        RiskClassification::Watch => 1,
        RiskClassification::Liquidatable => 2,
        RiskClassification::DataIncomplete => 3,
    }
}

const fn reason_code(value: RecommendationReason) -> u8 {
    match value {
        RecommendationReason::HealthBelowWarning => 0,
        RecommendationReason::HealthBelowLiquidation => 1,
        RecommendationReason::MissingInput => 2,
        RecommendationReason::Recovered => 3,
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn recommendation() -> RiskRecommendation {
        RiskRecommendation::new(
            AccountSubject::new(Uuid::from_u128(1)).expect("subject"),
            RiskClassification::Healthy,
            RiskClassification::Watch,
            RecommendationReason::HealthBelowWarning,
            RiskMeasurements::new(100, 10, 50, 1_100_000, 1_050_000).expect("measurements"),
            3,
            4,
        )
        .expect("recommendation")
    }

    fn evaluation() -> RiskEvaluation {
        RiskEvaluation::new(
            Uuid::from_u128(2),
            AccountSubject::new(Uuid::from_u128(1)).expect("subject"),
            [7; 32],
            RiskClassification::Watch,
            EvaluationReason::HealthBelowWarning,
            RiskMeasurements::new(100, 10, 50, 1_100_000, 1_050_000).expect("measurements"),
            0,
            EvaluationVersions {
                account_version: 4,
                input_version: 5,
                processing_epoch: 6,
                ethereum_price_sequence: Some(7),
                hyperliquid_price_sequence: Some(8),
            },
        )
        .expect("evaluation")
    }

    #[test]
    fn recommendation_identity_is_deterministic_and_self_consistent() {
        let first = recommendation();
        let second = recommendation();
        assert_eq!(first.recommendation_id(), second.recommendation_id());
        let repeated = RiskRecommendation::new(
            first.account(),
            first.previous_classification(),
            first.classification(),
            first.reason(),
            first.measurements(),
            first.portfolio_version(),
            first.processing_epoch() + 1,
        )
        .expect("repeated transition");
        assert_ne!(first.recommendation_id(), repeated.recommendation_id());
        let mut wire = serde_json::to_value(first).expect("serialize");
        wire["portfolio_version"] = serde_json::json!(5);
        assert!(serde_json::from_value::<RiskRecommendation>(wire).is_err());
    }

    #[test]
    fn recommendation_is_not_a_ledger_command_shape() {
        let wire = serde_json::to_value(recommendation()).expect("serialize");
        assert!(serde_json::from_value::<crate::ledger::LedgerCommand>(wire).is_err());
    }

    #[test]
    fn measurements_round_trip_as_numeric_values() {
        let measurements: RiskMeasurements = serde_json::from_str(
            r#"{"collateral_value":100,"debt_value":10,"maintenance_value":5,"health":900,"minimum_margin_health":-42}"#,
        )
        .expect("deserialize numeric measurements");
        assert_eq!(measurements.health(), 900);
        assert_eq!(measurements.minimum_margin_health(), -42);

        let wire = serde_json::to_value(measurements).expect("serialize measurements");
        assert_eq!(wire["health"], 900);
        assert!(wire["health"].is_number());
    }

    #[test]
    fn evaluation_identity_is_deterministic_and_rejects_tampering() {
        let first = evaluation();
        assert_eq!(first, evaluation());
        let mut wire = serde_json::to_value(first).expect("serialize");
        wire["versions"]["input_version"] = serde_json::json!(3);
        assert!(serde_json::from_value::<RiskEvaluation>(wire).is_err());
    }

    #[test]
    fn evaluation_contract_fails_closed() {
        let base = evaluation();
        assert!(
            RiskEvaluation::new(
                base.action_id(),
                base.account(),
                [7; 32],
                RiskClassification::Healthy,
                EvaluationReason::MissingInput,
                base.measurements(),
                0,
                base.versions().clone(),
            )
            .is_err()
        );
        assert!(
            RiskEvaluation::new(
                base.action_id(),
                base.account(),
                [7; 32],
                RiskClassification::Watch,
                EvaluationReason::HealthBelowWarning,
                base.measurements(),
                1,
                base.versions().clone(),
            )
            .is_err()
        );
        assert!(
            RiskEvaluation::new(
                base.action_id(),
                base.account(),
                [7; 32],
                RiskClassification::DataIncomplete,
                EvaluationReason::MissingInput,
                base.measurements(),
                0,
                base.versions().clone(),
            )
            .is_err()
        );
        let mut wire = serde_json::to_value(base).expect("serialize");
        wire["input_digest"] = serde_json::json!("AA".repeat(32));
        assert!(serde_json::from_value::<RiskEvaluation>(wire).is_err());
    }

    #[test]
    fn versions_and_reason_pairs_fail_closed() {
        assert!(
            RiskRecommendation::new(
                AccountSubject::new(Uuid::from_u128(1)).expect("subject"),
                RiskClassification::Healthy,
                RiskClassification::Liquidatable,
                RecommendationReason::Recovered,
                RiskMeasurements::new(1, 0, 1, 1, 1).expect("measurements"),
                1,
                1,
            )
            .is_err()
        );
        assert!(RiskMeasurements::new(-1, 0, 0, 0, 0).is_err());
    }
}

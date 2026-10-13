//! User-lifecycle events (JetStream, at-least-once).
//!
//! Published by core-api after each committed mutation. The stream is an
//! acceleration channel, not a source of truth: a publish lost between the
//! database commit and NATS is acceptable because consumers reconcile from
//! the users table (the indexer periodically compares its complete registry).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tackt_primitives::{CoreUser, Source, UserStatus};
use uuid::Uuid;

use crate::NatsClient;
use crate::jetstream::{Consumer, ConsumerSpec};
use crate::subjects;

/// Envelope for one user-lifecycle event.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UserEvent {
    /// Producer-generated id; doubles as the `Nats-Msg-Id` dedupe key.
    pub event_id: Uuid,
    pub occurred_at: DateTime<Utc>,
    #[serde(flatten)]
    pub kind: UserEventKind,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum UserEventKind {
    Created {
        user: CoreUser,
    },
    StatusChanged {
        user_id: Uuid,
        status: UserStatus,
    },
    AccountAssociated {
        user_id: Uuid,
        source: Source,
        account: String,
    },
}

impl UserEventKind {
    fn subject(&self) -> &'static str {
        match self {
            Self::Created { .. } => subjects::USERS_CREATED,
            Self::StatusChanged { .. } => subjects::USERS_STATUS_CHANGED,
            Self::AccountAssociated { .. } => subjects::USERS_ACCOUNT_ASSOCIATED,
        }
    }
}

impl NatsClient {
    /// Wrap `kind` in a fresh envelope and publish it with a server ack.
    pub async fn publish_user_event(&self, kind: UserEventKind) -> anyhow::Result<()> {
        self.publish_user_event_with_request_id(kind, None).await
    }

    /// Preserve the originating HTTP request ID as transport metadata without
    /// changing the durable event schema.
    pub async fn publish_user_event_with_request_id(
        &self,
        kind: UserEventKind,
        request_id: Option<&str>,
    ) -> anyhow::Result<()> {
        let event = UserEvent {
            event_id: Uuid::new_v4(),
            occurred_at: Utc::now(),
            kind,
        };
        let dedupe_id = event.event_id.to_string();
        // Discarded deliberately: a lifecycle event is a refresh trigger, so a
        // broker-deduplicated republish and a stored one mean the same thing to
        // every consumer.
        self.publish_durable_traced(event.kind.subject(), Some(&dedupe_id), &event, request_id)
            .await?;
        Ok(())
    }

    /// Per-instance signal consumer: new events only, ephemeral. Lifecycle
    /// events are refresh triggers, not state transfer — payloads may arrive
    /// out of commit order, and every instance must see them (e.g. each
    /// indexer replica refreshes its own websocket subscriptions), so history
    /// replay and shared durables are both wrong here. Consumers reconcile
    /// actual state from the users table.
    pub async fn user_events(&self) -> anyhow::Result<Consumer<UserEvent>> {
        self.consumer(&subjects::users_stream(), ConsumerSpec::ephemeral())
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_event_wire_shape_is_stable() {
        let event = UserEvent {
            event_id: Uuid::nil(),
            occurred_at: DateTime::<Utc>::MIN_UTC,
            kind: UserEventKind::StatusChanged {
                user_id: Uuid::nil(),
                status: UserStatus::Active,
            },
        };
        let value = serde_json::to_value(&event).expect("serialize");
        assert_eq!(value["type"], "status_changed");
        assert_eq!(value["status"], "active");
        let round_trip: UserEvent = serde_json::from_value(value).expect("deserialize");
        assert_eq!(round_trip, event);
    }

    #[test]
    fn subjects_route_by_variant() {
        let created = UserEventKind::Created {
            user: serde_json::from_value(serde_json::json!({
                "id": Uuid::nil(),
                "external_id": "x",
                "status": "pending",
                "accounts": {},
                "metadata": {},
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
            }))
            .expect("core user fixture"),
        };
        assert_eq!(created.subject(), subjects::USERS_CREATED);
        let changed = UserEventKind::StatusChanged {
            user_id: Uuid::nil(),
            status: UserStatus::Active,
        };
        assert_eq!(changed.subject(), subjects::USERS_STATUS_CHANGED);
        let associated = UserEventKind::AccountAssociated {
            user_id: Uuid::nil(),
            source: Source::Ethereum,
            account: "0xabc".to_string(),
        };
        assert_eq!(associated.subject(), subjects::USERS_ACCOUNT_ASSOCIATED);
    }
}

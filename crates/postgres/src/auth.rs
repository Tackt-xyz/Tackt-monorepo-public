use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use crate::{PgRepository, UserRepositoryError};

pub struct AuthSession {
    pub id: Uuid,
    pub user_id: Uuid,
    pub expires_at: DateTime<Utc>,
}

pub struct LoginIdentity {
    pub issuer: String,
    pub subject: String,
}

impl PgRepository {
    /// Claim a verified challenge once across all replicas. Expired challenges
    /// cannot be reclaimed after cleanup, even if a caller's clock is behind.
    pub async fn consume_auth_challenge(
        &self,
        id: Uuid,
        expires_at: DateTime<Utc>,
    ) -> Result<bool, UserRepositoryError> {
        sqlx::query("DELETE FROM core_auth_used_challenges WHERE expires_at <= now()")
            .execute(&self.pool)
            .await?;
        let claimed = sqlx::query_scalar::<_, Uuid>(
            "INSERT INTO core_auth_used_challenges(id,expires_at) SELECT $1,$2 WHERE $2>now() ON CONFLICT(id) DO NOTHING RETURNING id",
        )
        .bind(id)
        .bind(expires_at)
        .fetch_optional(&self.pool)
        .await?;
        Ok(claimed.is_some())
    }

    pub async fn core_user_identities(
        &self,
        user_id: Uuid,
        after: Option<&LoginIdentity>,
        limit: u32,
    ) -> Result<Vec<LoginIdentity>, UserRepositoryError> {
        let rows = sqlx::query(
            "SELECT issuer,subject FROM user_identities WHERE user_id=$1 AND ($2::text IS NULL OR (issuer,subject)>($2,$3)) ORDER BY issuer,subject LIMIT $4",
        )
        .bind(user_id)
        .bind(after.map(|identity| &identity.issuer))
        .bind(after.map(|identity| &identity.subject))
        .bind(i64::from(limit.clamp(1, 101)))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| LoginIdentity {
                issuer: row.get("issuer"),
                subject: row.get("subject"),
            })
            .collect())
    }

    pub async fn create_auth_session(
        &self,
        session: &AuthSession,
        refresh_hash: &[u8; 32],
    ) -> Result<(), UserRepositoryError> {
        sqlx::query("DELETE FROM core_auth_sessions WHERE expires_at <= now()")
            .execute(&self.pool)
            .await?;
        sqlx::query("INSERT INTO core_auth_sessions(id,user_id,refresh_hash,expires_at) VALUES($1,$2,$3,$4)")
            .bind(session.id).bind(session.user_id).bind(refresh_hash.as_slice()).bind(session.expires_at)
            .execute(&self.pool).await?;
        Ok(())
    }

    pub async fn auth_session_active(
        &self,
        session_id: Uuid,
        user_id: Uuid,
    ) -> Result<bool, UserRepositoryError> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM core_auth_sessions WHERE id=$1 AND user_id=$2 AND expires_at>now())")
            .bind(session_id).bind(user_id).fetch_one(&self.pool).await?)
    }

    /// A single compare-and-swap makes a refresh token usable exactly once,
    /// including concurrent refresh requests on different Core replicas.
    pub async fn rotate_auth_session(
        &self,
        session_id: Uuid,
        old_hash: &[u8; 32],
        new_hash: &[u8; 32],
    ) -> Result<Option<AuthSession>, UserRepositoryError> {
        let row = sqlx::query("UPDATE core_auth_sessions SET refresh_hash=$3 WHERE id=$1 AND refresh_hash=$2 AND expires_at>now() RETURNING id,user_id,expires_at")
            .bind(session_id).bind(old_hash.as_slice()).bind(new_hash.as_slice())
            .fetch_optional(&self.pool).await?;
        Ok(row.map(|row| AuthSession {
            id: row.get("id"),
            user_id: row.get("user_id"),
            expires_at: row.get("expires_at"),
        }))
    }

    pub async fn revoke_auth_session(
        &self,
        session_id: Uuid,
        user_id: Uuid,
    ) -> Result<(), UserRepositoryError> {
        sqlx::query("DELETE FROM core_auth_sessions WHERE id=$1 AND user_id=$2")
            .bind(session_id)
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

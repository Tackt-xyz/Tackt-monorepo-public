use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use std::collections::{BTreeMap, HashMap};
use tackt_primitives::{CoreUser, CreateCoreUser, Source, UserStatus};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    PgRepository, UserSigningKeys, UserWalletAddresses, signing_keys::insert_signing_keys,
};

#[derive(Debug, Error)]
pub enum UserRepositoryError {
    #[error("user not found")]
    NotFound,
    #[error("external id already belongs to a different request")]
    IdempotencyConflict,
    #[error("account is already associated with another user")]
    AssociationConflict,
    #[error("cannot transition user from {} to {}", .0.as_str(), .1.as_str())]
    InvalidTransition(UserStatus, UserStatus),
    #[error("database error")]
    Database(#[from] sqlx::Error),
    #[error("invalid persisted user state: {0}")]
    InvalidState(String),
    #[error("invalid external identity")]
    InvalidIdentity,
    #[error("external identity is already linked to another user")]
    IdentityConflict,
}

impl UserRepositoryError {
    /// Infrastructure failures callers may safely expose as a retryable 503.
    pub fn is_unavailable(&self) -> bool {
        match self {
            Self::Database(
                sqlx::Error::Io(_)
                | sqlx::Error::Tls(_)
                | sqlx::Error::PoolTimedOut
                | sqlx::Error::PoolClosed,
            ) => true,
            Self::Database(sqlx::Error::Database(error)) => error.code().is_some_and(|code| {
                code.starts_with("08")
                    || code.starts_with("53")
                    || matches!(code.as_ref(), "57P01" | "57P02" | "57P03")
            }),
            _ => false,
        }
    }
}

impl PgRepository {
    pub async fn create_core_user(
        &self,
        input: CreateCoreUser,
        keys: &UserSigningKeys,
    ) -> Result<(CoreUser, bool), UserRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let inserted = sqlx::query("INSERT INTO users(external_id,status,metadata,enabled) VALUES($1,'pending',$2,true) ON CONFLICT(external_id) DO NOTHING RETURNING id,external_id,status::text,beta_access,metadata,created_at,updated_at")
            .bind(&input.external_id).bind(&input.metadata).fetch_optional(&mut *transaction).await?;
        if let Some(row) = inserted {
            insert_signing_keys(&mut transaction, row.get("id"), keys).await?;
            transaction.commit().await?;
            let user = hydrate_user(&self.pool, row).await?;
            return Ok((user, true));
        }
        transaction.rollback().await?;
        let row = sqlx::query("SELECT id,external_id,status::text,beta_access,metadata,created_at,updated_at FROM users WHERE external_id=$1")
            .bind(&input.external_id).fetch_one(&self.pool).await?;
        let user = hydrate_user(&self.pool, row).await?;
        if user.metadata != input.metadata {
            return Err(UserRepositoryError::IdempotencyConflict);
        }
        Ok((user, false))
    }

    pub async fn core_user(&self, id: Uuid) -> Result<CoreUser, UserRepositoryError> {
        let row = sqlx::query("SELECT id,external_id,status::text,beta_access,metadata,created_at,updated_at FROM users WHERE id=$1")
            .bind(id).fetch_optional(&self.pool).await?.ok_or(UserRepositoryError::NotFound)?;
        hydrate_user(&self.pool, row).await
    }

    /// Loads the user and public wallet addresses together, then hydrates source accounts.
    /// Like `core_user`, this performs two queries and never loads private keys.
    pub async fn core_user_with_wallet_addresses(
        &self,
        id: Uuid,
    ) -> Result<(CoreUser, Option<UserWalletAddresses>), UserRepositoryError> {
        let row = sqlx::query("SELECT u.id,u.external_id,u.status::text,u.beta_access,u.metadata,u.created_at,u.updated_at,k.ethereum_address,k.solana_address FROM users u LEFT JOIN user_signing_keys k ON k.user_id=u.id WHERE u.id=$1")
            .bind(id).fetch_optional(&self.pool).await?.ok_or(UserRepositoryError::NotFound)?;
        let ethereum: Option<String> = row.try_get("ethereum_address")?;
        let addresses = ethereum.map(|ethereum| UserWalletAddresses {
            ethereum,
            solana: row.get("solana_address"),
        });
        let user = hydrate_user(&self.pool, row).await?;
        Ok((user, addresses))
    }

    /// Resolves a verified external identity to the Core user it belongs to.
    /// The issuer and subject are compared exactly as required by OIDC; email
    /// addresses and other mutable claims never participate in identity.
    pub async fn core_user_by_identity(
        &self,
        issuer: &str,
        subject: &str,
    ) -> Result<CoreUser, UserRepositoryError> {
        validate_identity(issuer, subject)?;
        let row = sqlx::query("SELECT u.id,u.external_id,u.status::text,u.beta_access,u.metadata,u.created_at,u.updated_at FROM user_identities i JOIN users u ON u.id=i.user_id WHERE i.issuer=$1 AND i.subject=$2")
            .bind(issuer)
            .bind(subject)
            .fetch_optional(&self.pool)
            .await?
            .ok_or(UserRepositoryError::NotFound)?;
        hydrate_user(&self.pool, row).await
    }

    /// Resolves an external identity and, when explicitly enabled, provisions
    /// one pending Core user with custody keys on first login. The candidate user,
    /// keys and identity commit together; concurrent losers roll back their
    /// candidate and return the already committed winner.
    pub async fn bootstrap_core_user_identity(
        &self,
        issuer: &str,
        subject: &str,
        allow_signup: bool,
        keys: &UserSigningKeys,
    ) -> Result<(CoreUser, bool), UserRepositoryError> {
        validate_identity(issuer, subject)?;
        match self.core_user_by_identity(issuer, subject).await {
            Ok(user) => return Ok((user, false)),
            Err(UserRepositoryError::NotFound) => {}
            Err(error) => return Err(error),
        }
        if !allow_signup {
            return Err(UserRepositoryError::NotFound);
        }
        let mut transaction = self.pool.begin().await?;
        let id = Uuid::new_v4();
        let external_id = format!("auth:{id}");
        let row = sqlx::query("INSERT INTO users(id,external_id,status,metadata,enabled) VALUES($1,$2,'pending','{}'::jsonb,true) RETURNING id,external_id,status::text,beta_access,metadata,created_at,updated_at")
            .bind(id)
            .bind(external_id)
            .fetch_one(&mut *transaction)
            .await?;
        insert_signing_keys(&mut transaction, id, keys).await?;
        let claimed = sqlx::query_scalar::<_, Uuid>(
            "INSERT INTO user_identities(user_id,issuer,subject) VALUES($1,$2,$3) ON CONFLICT(issuer,subject) DO NOTHING RETURNING user_id",
        )
        .bind(id)
        .bind(issuer)
        .bind(subject)
        .fetch_optional(&mut *transaction)
        .await?;
        if claimed.is_none() {
            transaction.rollback().await?;
            let user = self.core_user_by_identity(issuer, subject).await?;
            return Ok((user, false));
        }
        transaction.commit().await?;
        let user = hydrate_user(&self.pool, row).await?;
        Ok((user, true))
    }

    /// Explicitly links an external identity to an existing Core user.
    /// Repeating the same link is idempotent, while attempting to move an
    /// identity between users fails closed. Multiple distinct identities may
    /// intentionally belong to one user so future broker login methods can be
    /// linked without changing the Core user ID.
    pub async fn link_core_user_identity(
        &self,
        user_id: Uuid,
        issuer: &str,
        subject: &str,
    ) -> Result<bool, UserRepositoryError> {
        validate_identity(issuer, subject)?;
        let target = sqlx::query_scalar::<_, Uuid>("SELECT id FROM users WHERE id=$1")
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await?;
        if target.is_none() {
            return Err(UserRepositoryError::NotFound);
        }
        let inserted = sqlx::query_scalar::<_, Uuid>(
            "INSERT INTO user_identities(user_id,issuer,subject) VALUES($1,$2,$3) ON CONFLICT(issuer,subject) DO NOTHING RETURNING user_id",
        )
        .bind(user_id)
        .bind(issuer)
        .bind(subject)
        .fetch_optional(&self.pool)
        .await?;
        if inserted.is_some() {
            return Ok(true);
        }
        let linked_user = sqlx::query_scalar::<_, Uuid>(
            "SELECT user_id FROM user_identities WHERE issuer=$1 AND subject=$2",
        )
        .bind(issuer)
        .bind(subject)
        .fetch_one(&self.pool)
        .await?;
        if linked_user != user_id {
            return Err(UserRepositoryError::IdentityConflict);
        }
        Ok(false)
    }

    /// Keyset-paginated listing ordered by `(created_at, id)`; `after` is the
    /// position of the last row of the previous page.
    pub async fn list_core_users(
        &self,
        limit: u32,
        after: Option<(DateTime<Utc>, Uuid)>,
    ) -> Result<Vec<CoreUser>, UserRepositoryError> {
        let (after_created_at, after_id) = match after {
            Some((created_at, id)) => (Some(created_at), Some(id)),
            None => (None, None),
        };
        let rows = sqlx::query("SELECT id,external_id,status::text,beta_access,metadata,created_at,updated_at FROM users WHERE $2::timestamptz IS NULL OR (created_at,id) > ($2,$3) ORDER BY created_at,id LIMIT $1")
            .bind(limit.min(200) as i64).bind(after_created_at).bind(after_id).fetch_all(&self.pool).await?;
        let ids: Vec<Uuid> = rows.iter().map(|row| row.get("id")).collect();
        let account_rows = sqlx::query(
            "SELECT user_id,source::text,account FROM user_accounts WHERE user_id=ANY($1)",
        )
        .bind(&ids)
        .fetch_all(&self.pool)
        .await?;
        let mut accounts: HashMap<Uuid, BTreeMap<Source, String>> = HashMap::new();
        for row in account_rows {
            if let Some(source) = parse_account_source(&row.get::<String, _>("source")) {
                accounts
                    .entry(row.get("user_id"))
                    .or_default()
                    .insert(source, row.get("account"));
            }
        }
        rows.into_iter()
            .map(|row| {
                let id: Uuid = row.get("id");
                user_from_row(row, accounts.remove(&id).unwrap_or_default())
            })
            .collect()
    }

    /// Returns the user plus whether the status actually changed; a no-op
    /// request (already at the target status) reports `false` so callers can
    /// suppress side effects such as lifecycle event publication.
    pub async fn update_user_status(
        &self,
        id: Uuid,
        status: UserStatus,
    ) -> Result<(CoreUser, bool), UserRepositoryError> {
        let before = load_user(&self.pool, id).await?;
        if before.status == status {
            return Ok((before, false));
        }
        if !before.status.can_transition_to(status) {
            return Err(UserRepositoryError::InvalidTransition(
                before.status,
                status,
            ));
        }
        sqlx::query("UPDATE users SET status=$2::user_lifecycle_status,enabled=$2 NOT IN ('restricted','closed'),updated_at=now() WHERE id=$1")
            .bind(id).bind(status.as_str()).execute(&self.pool).await?;
        let after = load_user(&self.pool, id).await?;
        Ok((after, true))
    }

    /// Records beta enrollment after Core has verified the invite code.
    /// Repeated grants preserve the original modification timestamp.
    pub async fn grant_beta_access(&self, id: Uuid) -> Result<(), UserRepositoryError> {
        let result = sqlx::query("UPDATE users SET beta_access=true,updated_at=CASE WHEN beta_access THEN updated_at ELSE now() END WHERE id=$1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            return Err(UserRepositoryError::NotFound);
        }
        Ok(())
    }

    pub async fn replace_user_account(
        &self,
        id: Uuid,
        source: Source,
        account: String,
    ) -> Result<CoreUser, UserRepositoryError> {
        load_user(&self.pool, id).await?;
        let result = sqlx::query("INSERT INTO user_accounts(user_id,source,account) VALUES($1,$2::indexer_source,$3) ON CONFLICT(user_id,source) WHERE source IN ('ethereum','hyperliquid') DO UPDATE SET account=excluded.account")
            .bind(id).bind(source.as_str()).bind(&account).execute(&self.pool).await;
        if let Err(error) = result {
            if error
                .as_database_error()
                .is_some_and(|e| e.is_unique_violation())
            {
                return Err(UserRepositoryError::AssociationConflict);
            }
            return Err(error.into());
        }
        sqlx::query("UPDATE users SET updated_at=now() WHERE id=$1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        load_user(&self.pool, id).await
    }
}

fn validate_identity(issuer: &str, subject: &str) -> Result<(), UserRepositoryError> {
    let valid = !issuer.is_empty()
        && issuer.len() <= 2048
        && !subject.is_empty()
        && subject.len() <= 255
        && !issuer.chars().any(char::is_control)
        && !subject.chars().any(char::is_control);
    if valid {
        Ok(())
    } else {
        Err(UserRepositoryError::InvalidIdentity)
    }
}

async fn load_user(pool: &PgPool, id: Uuid) -> Result<CoreUser, UserRepositoryError> {
    let row = sqlx::query(
        "SELECT id,external_id,status::text,beta_access,metadata,created_at,updated_at FROM users WHERE id=$1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?
    .ok_or(UserRepositoryError::NotFound)?;
    hydrate_user(pool, row).await
}

async fn hydrate_user(
    pool: &PgPool,
    row: sqlx::postgres::PgRow,
) -> Result<CoreUser, UserRepositoryError> {
    let id: Uuid = row.get("id");
    let account_rows =
        sqlx::query("SELECT source::text,account FROM user_accounts WHERE user_id=$1")
            .bind(id)
            .fetch_all(pool)
            .await?;
    let mut accounts = BTreeMap::new();
    for account in account_rows {
        if let Some(source) = parse_account_source(&account.get::<String, _>("source")) {
            accounts.insert(source, account.get("account"));
        }
    }
    user_from_row(row, accounts)
}

fn user_from_row(
    row: sqlx::postgres::PgRow,
    accounts: BTreeMap<Source, String>,
) -> Result<CoreUser, UserRepositoryError> {
    Ok(CoreUser {
        id: row.get("id"),
        external_id: row.get("external_id"),
        status: parse_status(row.get("status"))?,
        beta_access: row.get("beta_access"),
        accounts,
        metadata: row.get("metadata"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

/// Rows whose source is not yet modelled in [`Source`] are skipped rather than
/// failing the read, so the database enum can gain values ahead of a deploy.
/// The skip is logged: silently dropping accounts is how a stale binary would
/// otherwise go unnoticed.
fn parse_account_source(value: &str) -> Option<Source> {
    match value {
        "ethereum" => Some(Source::Ethereum),
        "hyperliquid" => Some(Source::Hyperliquid),
        _ => {
            tracing::warn!(
                source = value,
                "skipping account with a source this binary does not model; deploy lag suspected"
            );
            None
        }
    }
}

pub(super) fn parse_status(value: String) -> Result<UserStatus, UserRepositoryError> {
    match value.as_str() {
        "pending" => Ok(UserStatus::Pending),
        "active" => Ok(UserStatus::Active),
        "restricted" => Ok(UserStatus::Restricted),
        "closed" => Ok(UserStatus::Closed),
        _ => Err(UserRepositoryError::InvalidState(value)),
    }
}

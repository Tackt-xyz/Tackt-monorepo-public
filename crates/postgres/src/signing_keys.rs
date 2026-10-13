use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::{PgRepository, UserRepositoryError};

/// Demo-only, unencrypted custody keys. Deliberately neither Debug nor serializable.
/// Generation and signing belong to Core; PostgreSQL only stores the key bytes.
pub struct UserSigningKeys {
    /// Absent only for keys written before address indexing was introduced.
    pub addresses: Option<UserWalletAddresses>,
    /// A big-endian secp256k1 private scalar.
    pub ethereum_private_key: [u8; 32],
    /// The Ed25519 seed, not Solana's 64-byte seed + public-key export format.
    pub solana_private_key: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UserWalletAddresses {
    pub ethereum: String,
    pub solana: String,
}

pub enum WalletAddress<'a> {
    Ethereum(&'a str),
    Solana(&'a str),
}

impl PgRepository {
    pub async fn user_wallet_addresses(
        &self,
        user_id: Uuid,
    ) -> Result<Option<UserWalletAddresses>, UserRepositoryError> {
        let row = sqlx::query("SELECT ethereum_address,solana_address FROM user_signing_keys WHERE user_id=$1 AND ethereum_address IS NOT NULL")
            .bind(user_id).fetch_optional(&self.pool).await?;
        Ok(row.map(|row| UserWalletAddresses {
            ethereum: row.get("ethereum_address"),
            solana: row.get("solana_address"),
        }))
    }

    pub async fn user_id_by_wallet_address(
        &self,
        address: WalletAddress<'_>,
    ) -> Result<Option<Uuid>, UserRepositoryError> {
        let (query, value) = match address {
            WalletAddress::Ethereum(value) => (
                "SELECT user_id FROM user_signing_keys WHERE ethereum_address=$1",
                value,
            ),
            WalletAddress::Solana(value) => (
                "SELECT user_id FROM user_signing_keys WHERE solana_address=$1",
                value,
            ),
        };
        Ok(sqlx::query_scalar(query)
            .bind(value)
            .fetch_optional(&self.pool)
            .await?)
    }

    /// A bounded batch for Core's restart-safe address derivation backfill.
    pub async fn signing_keys_without_addresses(
        &self,
    ) -> Result<Vec<(Uuid, UserSigningKeys)>, UserRepositoryError> {
        let rows = sqlx::query("SELECT user_id,ethereum_private_key,solana_private_key FROM user_signing_keys WHERE ethereum_address IS NULL ORDER BY user_id LIMIT 100")
            .fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|row| {
                Ok((
                    row.get("user_id"),
                    UserSigningKeys {
                        addresses: None,
                        ethereum_private_key: key_bytes(row.try_get("ethereum_private_key")?)?,
                        solana_private_key: key_bytes(row.try_get("solana_private_key")?)?,
                    },
                ))
            })
            .collect()
    }

    /// Fills a previously unindexed key pair without changing an existing address or key.
    pub async fn set_missing_wallet_addresses(
        &self,
        user_id: Uuid,
        addresses: &UserWalletAddresses,
    ) -> Result<(), UserRepositoryError> {
        sqlx::query("UPDATE user_signing_keys SET ethereum_address=$2,solana_address=$3 WHERE user_id=$1 AND ethereum_address IS NULL")
            .bind(user_id).bind(&addresses.ethereum).bind(&addresses.solana).execute(&self.pool).await?;
        Ok(())
    }

    /// Loads custody material separately from profiles, sessions and lifecycle events.
    /// Users created before the key migration have no keys until explicitly provisioned.
    pub async fn user_signing_keys(
        &self,
        user_id: Uuid,
    ) -> Result<Option<UserSigningKeys>, UserRepositoryError> {
        let row = sqlx::query(
            "SELECT ethereum_private_key,solana_private_key,ethereum_address,solana_address FROM user_signing_keys WHERE user_id=$1",
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            let ethereum: Option<String> = row.try_get("ethereum_address")?;
            Ok(UserSigningKeys {
                addresses: ethereum.map(|ethereum| UserWalletAddresses {
                    ethereum,
                    solana: row.get("solana_address"),
                }),
                ethereum_private_key: key_bytes(row.try_get("ethereum_private_key")?)?,
                solana_private_key: key_bytes(row.try_get("solana_private_key")?)?,
            })
        })
        .transpose()
    }
}

fn key_bytes(bytes: Vec<u8>) -> Result<[u8; 32], UserRepositoryError> {
    bytes
        .try_into()
        .map_err(|_| UserRepositoryError::InvalidState("invalid signing key length".into()))
}

pub(crate) async fn insert_signing_keys(
    connection: &mut PgConnection,
    user_id: Uuid,
    keys: &UserSigningKeys,
) -> Result<(), UserRepositoryError> {
    sqlx::query(
        "INSERT INTO user_signing_keys(user_id,ethereum_private_key,solana_private_key,ethereum_address,solana_address) VALUES($1,$2,$3,$4,$5)",
    )
    .bind(user_id)
    .bind(keys.ethereum_private_key.as_slice())
    .bind(keys.solana_private_key.as_slice())
    .bind(keys.addresses.as_ref().map(|a| a.ethereum.as_str()))
    .bind(keys.addresses.as_ref().map(|a| a.solana.as_str()))
    .execute(connection)
    .await?;
    Ok(())
}

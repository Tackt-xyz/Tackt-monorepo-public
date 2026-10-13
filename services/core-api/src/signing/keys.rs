use k256::elliptic_curve::sec1::ToEncodedPoint;
use k256::{
    SecretKey,
    elliptic_curve::rand_core::{OsRng, RngCore},
};
use sha3::{Digest, Keccak256};
use tackt_postgres::{PgRepository, UserRepositoryError, UserSigningKeys, UserWalletAddresses};

/// Generates independent Ethereum and Solana custody keys for demo user provisioning.
pub fn generate_user_signing_keys() -> UserSigningKeys {
    let ethereum = SecretKey::random(&mut OsRng);
    let mut solana_seed = [0u8; 32];
    OsRng.fill_bytes(&mut solana_seed);
    let mut keys = UserSigningKeys {
        addresses: None,
        ethereum_private_key: ethereum.to_bytes().into(),
        solana_private_key: solana_seed,
    };
    keys.addresses = Some(addresses_for_keys(&keys).expect("generated secp256k1 key is valid"));
    keys
}

pub fn addresses_for_keys(
    keys: &UserSigningKeys,
) -> Result<UserWalletAddresses, UserRepositoryError> {
    let ethereum = SecretKey::from_slice(&keys.ethereum_private_key)
        .map_err(|_| UserRepositoryError::InvalidState("invalid Ethereum signing key".into()))?;
    let public_key = ethereum.public_key().to_encoded_point(false);
    let hash = Keccak256::digest(&public_key.as_bytes()[1..]);
    let solana = ed25519_dalek::SigningKey::from_bytes(&keys.solana_private_key);
    Ok(UserWalletAddresses {
        ethereum: format!("0x{}", hex::encode(&hash[12..])),
        solana: bs58::encode(solana.verifying_key().as_bytes()).into_string(),
    })
}

pub async fn backfill_wallet_addresses(repo: &PgRepository) -> Result<(), UserRepositoryError> {
    loop {
        let pending = repo.signing_keys_without_addresses().await?;
        if pending.is_empty() {
            return Ok(());
        }
        for (user_id, keys) in pending {
            repo.set_missing_wallet_addresses(user_id, &addresses_for_keys(&keys)?)
                .await?;
        }
    }
}

pub(super) async fn load_user_keys(
    repo: &PgRepository,
    user_id: uuid::Uuid,
) -> Result<UserSigningKeys, super::SigningError> {
    repo.user_signing_keys(user_id)
        .await?
        .ok_or(super::SigningError::MissingKeys)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, Verifier};

    #[test]
    fn generated_keys_sign_and_verify_for_both_chains() {
        let keys = generate_user_signing_keys();
        let message = b"demo signing round trip";
        let ethereum = k256::ecdsa::SigningKey::from_slice(&keys.ethereum_private_key).unwrap();
        let digest = sha3::Keccak256::new_with_prefix(message);
        use sha3::Digest;
        let (signature, recovery_id) = ethereum.sign_digest_recoverable(digest.clone()).unwrap();
        let recovered =
            k256::ecdsa::VerifyingKey::recover_from_digest(digest, &signature, recovery_id)
                .unwrap();
        assert_eq!(&recovered, ethereum.verifying_key());
        let solana = ed25519_dalek::SigningKey::from_bytes(&keys.solana_private_key);
        solana
            .verifying_key()
            .verify(message, &solana.sign(message))
            .unwrap();

        let other = generate_user_signing_keys();
        assert!(keys.ethereum_private_key != other.ethereum_private_key);
        assert!(keys.solana_private_key != other.solana_private_key);
        assert!(keys.ethereum_private_key != keys.solana_private_key);
    }
}

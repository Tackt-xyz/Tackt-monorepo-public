//! Solana legacy and v0 transaction signing.

use super::SigningError;
use base64::{Engine, engine::general_purpose::STANDARD};
use ed25519_dalek::Signer;
use solana_transaction::versioned::VersionedTransaction;
use tackt_postgres::{PgRepository, UserSigningKeys};
use uuid::Uuid;

#[derive(Debug)]
pub struct SignedTransaction {
    pub address: String,
    pub signed_transaction: String,
    pub signature: String,
}

/// Signs with this user's stored key and returns the signed payload without broadcasting.
pub async fn sign(
    repo: &PgRepository,
    user_id: Uuid,
    transaction: &str,
) -> Result<SignedTransaction, SigningError> {
    let keys = super::keys::load_user_keys(repo, user_id).await?;
    sign_with_keys(&keys, transaction)
}

fn sign_with_keys(
    keys: &UserSigningKeys,
    encoded: &str,
) -> Result<SignedTransaction, SigningError> {
    if encoded.len() > 1644 {
        return Err(SigningError::InvalidTransaction(
            "Solana transaction exceeds 1232 bytes",
        ));
    }
    let bytes = STANDARD
        .decode(encoded)
        .map_err(|_| SigningError::InvalidTransaction("transaction must be base64"))?;
    if bytes.len() > 1232 {
        return Err(SigningError::InvalidTransaction(
            "Solana transaction exceeds 1232 bytes",
        ));
    }
    let mut tx: VersionedTransaction = wincode::deserialize_exact(&bytes)
        .map_err(|_| SigningError::InvalidTransaction("invalid Solana wire transaction"))?;
    if !matches!(
        tx.message,
        solana_message::VersionedMessage::Legacy(_) | solana_message::VersionedMessage::V0(_)
    ) {
        return Err(SigningError::InvalidTransaction(
            "only legacy and v0 Solana transactions are supported",
        ));
    }
    tx.sanitize().map_err(|_| {
        SigningError::InvalidTransaction("invalid Solana transaction structure or signature slots")
    })?;
    let key = ed25519_dalek::SigningKey::from_bytes(&keys.solana_private_key);
    let required = usize::from(tx.message.header().num_required_signatures);
    let slot = tx.message.static_account_keys()[..required]
        .iter()
        .position(|address| address.as_ref() == key.verifying_key().as_bytes())
        .ok_or(SigningError::InvalidTransaction(
            "user wallet is not a required transaction signer",
        ))?;
    let message = wincode::serialize(&tx.message).map_err(|_| SigningError::SigningFailed)?;
    let signature = key.sign(&message).to_bytes();
    tx.signatures[slot] = signature.into();
    Ok(SignedTransaction {
        address: bs58::encode(key.verifying_key().as_bytes()).into_string(),
        signed_transaction: STANDARD
            .encode(wincode::serialize(&tx).map_err(|_| SigningError::SigningFailed)?),
        signature: bs58::encode(signature).into_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signing::generate_user_signing_keys;
    use ed25519_dalek::Verifier;
    use solana_message::{Message, MessageHeader, VersionedMessage, v0};

    fn solana(
        keys: &UserSigningKeys,
        other: &ed25519_dalek::SigningKey,
        versioned: bool,
    ) -> VersionedTransaction {
        let key = ed25519_dalek::SigningKey::from_bytes(&keys.solana_private_key);
        let header = MessageHeader {
            num_required_signatures: 2,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 0,
        };
        let account_keys = vec![
            other.verifying_key().to_bytes().into(),
            key.verifying_key().to_bytes().into(),
        ];
        let message = if versioned {
            VersionedMessage::V0(v0::Message {
                header,
                account_keys,
                ..Default::default()
            })
        } else {
            VersionedMessage::Legacy(Message {
                header,
                account_keys,
                ..Default::default()
            })
        };
        let other_signature = other
            .sign(&wincode::serialize(&message).unwrap())
            .to_bytes();
        VersionedTransaction {
            message,
            signatures: vec![other_signature.into(), [0; 64].into()],
        }
    }

    #[test]
    fn solana_legacy_and_v0_sign_the_correct_slot_and_preserve_other_signatures() {
        let keys = generate_user_signing_keys();
        let other =
            ed25519_dalek::SigningKey::from_bytes(&generate_user_signing_keys().solana_private_key);
        for versioned in [false, true] {
            let tx = solana(&keys, &other, versioned);
            let encoded = STANDARD.encode(wincode::serialize(&tx).unwrap());
            let SignedTransaction {
                address,
                signed_transaction,
                signature,
            } = sign_with_keys(&keys, &encoded).unwrap();
            let signed: VersionedTransaction =
                wincode::deserialize_exact(&STANDARD.decode(signed_transaction).unwrap()).unwrap();
            assert_eq!(signed.message, tx.message);
            assert_eq!(signed.signatures[0], tx.signatures[0]);
            let public =
                ed25519_dalek::SigningKey::from_bytes(&keys.solana_private_key).verifying_key();
            public
                .verify(
                    &wincode::serialize(&signed.message).unwrap(),
                    &ed25519_dalek::Signature::from_slice(signed.signatures[1].as_ref()).unwrap(),
                )
                .unwrap();
            assert_eq!(
                signature,
                bs58::encode(signed.signatures[1].as_ref()).into_string()
            );
            assert_eq!(address, bs58::encode(public.as_bytes()).into_string());
            assert!(matches!(
                sign_with_keys(&generate_user_signing_keys(), &encoded),
                Err(SigningError::InvalidTransaction(_))
            ));
        }
    }

    #[test]
    fn solana_rejects_malformed_payloads_and_signature_slots() {
        let keys = generate_user_signing_keys();
        let other =
            ed25519_dalek::SigningKey::from_bytes(&generate_user_signing_keys().solana_private_key);
        let mut tx = solana(&keys, &other, false);
        let mut trailing = wincode::serialize(&tx).unwrap();
        trailing.push(0);
        tx.signatures.pop();
        for encoded in [
            "invalid!".into(),
            STANDARD.encode([0u8; 1233]),
            STANDARD.encode(trailing),
            STANDARD.encode(wincode::serialize(&tx).unwrap()),
        ] {
            assert!(matches!(
                sign_with_keys(&keys, &encoded),
                Err(SigningError::InvalidTransaction(_))
            ));
        }
    }
}

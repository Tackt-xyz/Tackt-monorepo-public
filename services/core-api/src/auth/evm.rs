use chrono::{DateTime, SecondsFormat};
use k256::ecdsa::{RecoveryId, Signature, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Keccak256};
use uuid::Uuid;

use super::{
    AuthError, VerifiedIdentity,
    tokens::{LinkContext, Tokens},
};

// A wallet is an authentication identity, independent of RPC clusters and
// dedicated source-account provisioning. OIDC issuers cannot use this namespace.
pub(super) const IDENTITY_ISSUER: &str = "urn:tackt:auth:evm";
pub(super) const CHALLENGE_SECONDS: i64 = 5 * 60;

pub(super) struct EvmAuth {
    uri: String,
    domain: String,
}

impl EvmAuth {
    pub fn new(uri: &str) -> anyhow::Result<Self> {
        let uri = reqwest::Url::parse(uri)?;
        super::validate_url(&uri)?;
        anyhow::ensure!(
            uri.query().is_none(),
            "EVM login URI must not contain a query"
        );
        let host = uri
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("EVM login URI needs a host"))?;
        let domain = match uri.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_owned(),
        };
        Ok(Self {
            uri: uri.to_string(),
            domain,
        })
    }

    pub(super) fn start(
        &self,
        tokens: &Tokens,
        address: String,
        link: Option<LinkContext>,
    ) -> Result<ChallengeResponse, AuthError> {
        let address = wallet_address(&address).map_err(|_| AuthError::InvalidField("address"))?;
        let address = checksum_address(&address);
        let (challenge_token, challenge) = tokens.evm_challenge(address, self.uri.clone(), link)?;
        let sign_in_input = self.sign_in_input(&challenge)?;
        Ok(ChallengeResponse {
            message: sign_in_input.message(),
            sign_in_input,
            challenge_token,
        })
    }

    fn sign_in_input(&self, challenge: &Challenge) -> Result<SignInInput, AuthError> {
        if challenge.uri != self.uri {
            return Err(AuthError::Unauthorized);
        }
        let issued_at =
            DateTime::from_timestamp(challenge.iat, 0).ok_or(AuthError::Unauthorized)?;
        let expiration =
            DateTime::from_timestamp(challenge.exp, 0).ok_or(AuthError::Unauthorized)?;
        let statement = match &challenge.link {
            Some(link) => format!("Link this wallet to Tackt user {}.", link.user_id),
            None => "Sign in to Tackt.".into(),
        };
        Ok(SignInInput {
            domain: self.domain.clone(),
            address: challenge.address.clone(),
            statement,
            uri: self.uri.clone(),
            version: "1".into(),
            // Login currently targets Ethereum mainnet and verifies EOA signatures locally.
            chain_id: 1,
            nonce: challenge.nonce.simple().to_string(),
            issued_at: issued_at.to_rfc3339_opts(SecondsFormat::Secs, true),
            expiration_time: expiration.to_rfc3339_opts(SecondsFormat::Secs, true),
        })
    }

    pub(super) fn verify_signature(
        &self,
        challenge: &Challenge,
        signature: &str,
    ) -> Result<VerifiedIdentity, AuthError> {
        let message = self.sign_in_input(challenge)?.message();
        if recover_wallet(&message, signature)? != wallet_address(&challenge.address)? {
            return Err(AuthError::Unauthorized);
        }
        Ok(VerifiedIdentity {
            issuer: IDENTITY_ISSUER.into(),
            subject: challenge.address.to_ascii_lowercase(),
        })
    }
}

// EIP-191 personal_sign hashes the UTF-8 byte length, not the character count.
fn personal_sign_digest(message: &str) -> Keccak256 {
    Keccak256::new()
        .chain_update(format!("\x19Ethereum Signed Message:\n{}", message.len()))
        .chain_update(message.as_bytes())
}

fn recover_wallet(message: &str, signature: &str) -> Result<[u8; 20], AuthError> {
    if signature.len() != 132 || !signature.starts_with("0x") {
        return Err(AuthError::Unauthorized);
    }
    let mut bytes = [0_u8; 65];
    hex::decode_to_slice(&signature[2..], &mut bytes).map_err(|_| AuthError::Unauthorized)?;
    let signature = Signature::from_slice(&bytes[..64]).map_err(|_| AuthError::Unauthorized)?;
    let recovery = match bytes[64] {
        0 | 1 => bytes[64],
        27 | 28 => bytes[64] - 27,
        _ => return Err(AuthError::Unauthorized),
    };
    let recovery = RecoveryId::from_byte(recovery).ok_or(AuthError::Unauthorized)?;
    let key =
        VerifyingKey::recover_from_digest(personal_sign_digest(message), &signature, recovery)
            .map_err(|_| AuthError::Unauthorized)?;
    let point = key.to_encoded_point(false);
    let hash = Keccak256::digest(&point.as_bytes()[1..]);
    let mut address = [0_u8; 20];
    address.copy_from_slice(&hash[12..]);
    Ok(address)
}

// ERC-55 hashes the lowercase address's hexadecimal text to choose letter casing.
fn checksum_address(address: &[u8; 20]) -> String {
    let lower = hex::encode(address);
    let hash = hex::encode(Keccak256::digest(lower.as_bytes()));
    let checksum: String = lower
        .bytes()
        .zip(hash.bytes())
        .map(|(letter, nibble)| {
            if nibble >= b'8' {
                letter.to_ascii_uppercase() as char
            } else {
                letter as char
            }
        })
        .collect();
    format!("0x{checksum}")
}

fn wallet_address(address: &str) -> Result<[u8; 20], AuthError> {
    if address.len() != 42 || !address.starts_with("0x") {
        return Err(AuthError::Unauthorized);
    }
    let mut bytes = [0_u8; 20];
    hex::decode_to_slice(&address[2..], &mut bytes).map_err(|_| AuthError::Unauthorized)?;
    let checksum = checksum_address(&bytes);
    let has_lower = address[2..].bytes().any(|b| b.is_ascii_lowercase());
    let has_upper = address[2..].bytes().any(|b| b.is_ascii_uppercase());
    if has_lower && has_upper && address != checksum {
        return Err(AuthError::Unauthorized);
    }
    Ok(bytes)
}

#[derive(Serialize, Deserialize)]
pub(super) struct Challenge {
    pub nonce: Uuid,
    pub address: String,
    pub uri: String,
    pub link: Option<LinkContext>,
    pub iss: String,
    pub aud: String,
    pub iat: i64,
    pub exp: i64,
}

#[derive(Serialize, utoipa::ToSchema)]
#[schema(as = EvmSignInInput)]
#[serde(rename_all = "camelCase")]
pub(super) struct SignInInput {
    domain: String,
    address: String,
    statement: String,
    uri: String,
    version: String,
    chain_id: u64,
    nonce: String,
    issued_at: String,
    expiration_time: String,
}

impl SignInInput {
    fn message(&self) -> String {
        format!(
            "{} wants you to sign in with your Ethereum account:\n{}\n\n{}\n\nURI: {}\nVersion: {}\nChain ID: {}\nNonce: {}\nIssued At: {}\nExpiration Time: {}",
            self.domain,
            self.address,
            self.statement,
            self.uri,
            self.version,
            self.chain_id,
            self.nonce,
            self.issued_at,
            self.expiration_time
        )
    }
}

#[derive(Serialize, utoipa::ToSchema)]
#[schema(as = EvmChallengeResponse)]
#[serde(rename_all = "camelCase")]
pub(super) struct ChallengeResponse {
    message: String,
    sign_in_input: SignInInput,
    challenge_token: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    use k256::{ecdsa::SigningKey, elliptic_curve::rand_core::OsRng};

    fn tokens() -> Tokens {
        Tokens::new(
            "https://api.example".into(),
            uuid::Uuid::new_v4().to_string().as_bytes(),
        )
    }

    fn address(wallet: &SigningKey) -> String {
        let point = wallet.verifying_key().to_encoded_point(false);
        format!(
            "0x{}",
            hex::encode(&Keccak256::digest(&point.as_bytes()[1..])[12..])
        )
    }

    fn sign(wallet: &SigningKey, message: &str) -> String {
        let (signature, recovery) = wallet
            .sign_digest_recoverable(personal_sign_digest(message))
            .unwrap();
        format!(
            "0x{}{:02x}",
            hex::encode(signature.to_bytes()),
            recovery.to_byte() + 27
        )
    }

    #[test]
    fn wallet_signs_exact_domain_bound_siwe_message() {
        let wallet = SigningKey::random(&mut OsRng);
        let address = address(&wallet);
        let evm = EvmAuth::new("https://app.example").unwrap();
        let tokens = tokens();
        let response = evm.start(&tokens, address.clone(), None).unwrap();
        let challenge = tokens
            .verify_evm_challenge(&response.challenge_token)
            .unwrap();
        assert_eq!(challenge.exp - challenge.iat, CHALLENGE_SECONDS);
        assert!(response.message.starts_with(&format!("app.example wants you to sign in with your Ethereum account:\n{}\n\nSign in to Tackt.\n\nURI: https://app.example/\nVersion: 1\nChain ID: 1\nNonce: ", checksum_address(&wallet_address(&address).unwrap()))));
        assert_eq!(response.message, response.sign_in_input.message());
        let signature = sign(&wallet, &response.message);
        let identity = evm.verify_signature(&challenge, &signature).unwrap();
        assert_eq!(identity.issuer, IDENTITY_ISSUER);
        assert_eq!(identity.subject, address);
        let mut bytes = hex::decode(&signature[2..]).unwrap();
        bytes[64] -= 27;
        assert!(
            evm.verify_signature(&challenge, &format!("0x{}", hex::encode(bytes)))
                .is_ok()
        );
        assert!(
            EvmAuth::new("https://other.example")
                .unwrap()
                .verify_signature(&challenge, &signature)
                .is_err()
        );
    }

    #[test]
    fn rejects_wrong_wallet_message_nonce_and_signature_encoding() {
        let wallet = SigningKey::random(&mut OsRng);
        let other = SigningKey::random(&mut OsRng);
        let evm = EvmAuth::new("https://app.example").unwrap();
        let tokens = tokens();
        let response = evm.start(&tokens, address(&wallet), None).unwrap();
        let challenge = tokens
            .verify_evm_challenge(&response.challenge_token)
            .unwrap();
        let signature = sign(&wallet, &response.message);
        for invalid in [
            sign(&other, &response.message),
            sign(&wallet, &format!("{}\n", response.message)),
            sign(
                &wallet,
                &response.message.replace("Chain ID: 1", "Chain ID: 10"),
            ),
            "not hex".into(),
            format!("0x{}", "00".repeat(65)),
            signature[..130].to_owned(),
            format!("{}25", &signature[..130]),
            format!("{}02", &signature[..130]),
            "A".repeat(133),
        ] {
            assert!(evm.verify_signature(&challenge, &invalid).is_err());
        }
        let second = evm.start(&tokens, challenge.address.clone(), None).unwrap();
        let second = tokens
            .verify_evm_challenge(&second.challenge_token)
            .unwrap();
        assert_ne!(challenge.nonce, second.nonce);
        assert!(evm.verify_signature(&second, &signature).is_err());
    }

    #[test]
    fn canonicalizes_addresses_using_eip55_vectors_and_rejects_bad_checksums() {
        for expected in [
            "0x52908400098527886E0F7030069857D2E4169EE7",
            "0xde709f2102306220921060314715629080e2fb77",
            "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed",
        ] {
            assert_eq!(
                checksum_address(&wallet_address(expected).unwrap()),
                expected
            );
            assert_eq!(
                checksum_address(&wallet_address(&expected.to_ascii_lowercase()).unwrap()),
                expected
            );
            assert_eq!(
                checksum_address(
                    &wallet_address(&format!("0x{}", expected[2..].to_ascii_uppercase())).unwrap()
                ),
                expected
            );
        }
        let evm = EvmAuth::new("https://app.example").unwrap();
        for invalid in [
            "",
            "0x1234",
            "0x5aaeb6053F3E94C9b9A09f33669435E7Ef1BeAed",
            "0xgggggggggggggggggggggggggggggggggggggggggggg",
        ] {
            assert!(matches!(
                evm.start(&tokens(), invalid.into(), None),
                Err(AuthError::InvalidField("address"))
            ));
        }
    }

    #[test]
    fn linking_message_names_target_user() {
        let wallet = SigningKey::random(&mut OsRng);
        let user_id = Uuid::new_v4();
        let evm = EvmAuth::new("http://localhost:3000").unwrap();
        let tokens = tokens();
        let response = evm
            .start(
                &tokens,
                address(&wallet),
                Some(LinkContext {
                    user_id,
                    session_id: Uuid::new_v4(),
                }),
            )
            .unwrap();
        assert!(response.message.starts_with("localhost:3000 wants you"));
        assert_eq!(
            response.sign_in_input.statement,
            format!("Link this wallet to Tackt user {user_id}.")
        );
        let challenge = tokens
            .verify_evm_challenge(&response.challenge_token)
            .unwrap();
        assert!(
            evm.verify_signature(&challenge, &sign(&wallet, &response.message))
                .is_ok()
        );
    }
}

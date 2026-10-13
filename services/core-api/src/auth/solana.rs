use base64::{Engine as _, engine::general_purpose::STANDARD};
use chrono::{DateTime, SecondsFormat};
use ed25519_dalek::{Signature, VerifyingKey};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    AuthError, VerifiedIdentity,
    tokens::{LinkContext, Tokens},
};

// A wallet is an authentication identity, independent of RPC clusters and
// dedicated source-account provisioning. OIDC issuers cannot use this namespace.
pub(super) const IDENTITY_ISSUER: &str = "urn:tackt:auth:solana";
pub(super) const CHALLENGE_SECONDS: i64 = 5 * 60;

pub(super) struct SolanaAuth {
    uri: String,
    domain: String,
}

impl SolanaAuth {
    pub fn new(uri: &str) -> anyhow::Result<Self> {
        let uri = reqwest::Url::parse(uri)?;
        super::validate_url(&uri)?;
        anyhow::ensure!(
            uri.query().is_none(),
            "Solana login URI must not contain a query"
        );
        let host = uri
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("Solana login URI needs a host"))?;
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
        wallet_key(&address).map_err(|_| AuthError::InvalidField("address"))?;
        let (challenge_token, challenge) =
            tokens.solana_challenge(address, self.uri.clone(), link)?;
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
        if signature.len() > 88 {
            return Err(AuthError::Unauthorized);
        }
        let bytes = STANDARD
            .decode(signature)
            .map_err(|_| AuthError::Unauthorized)?;
        let signature = Signature::from_slice(&bytes).map_err(|_| AuthError::Unauthorized)?;
        let key = wallet_key(&challenge.address)?;
        let message = self.sign_in_input(challenge)?.message();
        key.verify_strict(message.as_bytes(), &signature)
            .map_err(|_| AuthError::Unauthorized)?;
        Ok(VerifiedIdentity {
            issuer: IDENTITY_ISSUER.into(),
            subject: challenge.address.clone(),
        })
    }
}

fn wallet_key(address: &str) -> Result<VerifyingKey, AuthError> {
    if !(32..=44).contains(&address.len()) {
        return Err(AuthError::Unauthorized);
    }
    let mut bytes = [0_u8; 32];
    let size = bs58::decode(address)
        .onto(&mut bytes)
        .map_err(|_| AuthError::Unauthorized)?;
    if size != 32 || bs58::encode(bytes).into_string() != address {
        return Err(AuthError::Unauthorized);
    }
    let key = VerifyingKey::from_bytes(&bytes).map_err(|_| AuthError::Unauthorized)?;
    if key.is_weak() {
        return Err(AuthError::Unauthorized);
    }
    Ok(key)
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
#[schema(as = SolanaSignInInput)]
#[serde(rename_all = "camelCase")]
pub(super) struct SignInInput {
    domain: String,
    address: String,
    statement: String,
    uri: String,
    version: String,
    nonce: String,
    issued_at: String,
    expiration_time: String,
}

impl SignInInput {
    fn message(&self) -> String {
        format!(
            "{} wants you to sign in with your Solana account:\n{}\n\n{}\n\nURI: {}\nVersion: {}\nNonce: {}\nIssued At: {}\nExpiration Time: {}",
            self.domain,
            self.address,
            self.statement,
            self.uri,
            self.version,
            self.nonce,
            self.issued_at,
            self.expiration_time
        )
    }
}

#[derive(Serialize, utoipa::ToSchema)]
#[schema(as = SolanaChallengeResponse)]
#[serde(rename_all = "camelCase")]
pub(super) struct ChallengeResponse {
    message: String,
    sign_in_input: SignInInput,
    challenge_token: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};
    use p256::elliptic_curve::rand_core::{OsRng, RngCore};

    fn wallet() -> SigningKey {
        let mut seed = [0; 32];
        OsRng.fill_bytes(&mut seed);
        SigningKey::from_bytes(&seed)
    }

    fn tokens() -> Tokens {
        Tokens::new(
            "https://api.example".into(),
            uuid::Uuid::new_v4().to_string().as_bytes(),
        )
    }

    #[test]
    fn wallet_signs_the_exact_domain_bound_siws_message() {
        let wallet = wallet();
        let address = bs58::encode(wallet.verifying_key().as_bytes()).into_string();
        let solana = SolanaAuth::new("https://app.example").unwrap();
        let tokens = tokens();
        let response = solana.start(&tokens, address.clone(), None).unwrap();
        let challenge = tokens
            .verify_solana_challenge(&response.challenge_token)
            .unwrap();
        assert_eq!(challenge.exp - challenge.iat, CHALLENGE_SECONDS);
        assert!(response.message.starts_with(&format!("app.example wants you to sign in with your Solana account:\n{address}\n\nSign in to Tackt.\n\nURI: https://app.example/\nVersion: 1\nNonce: ")));
        assert_eq!(response.message, response.sign_in_input.message());
        let signature = STANDARD.encode(wallet.sign(response.message.as_bytes()).to_bytes());
        let identity = solana.verify_signature(&challenge, &signature).unwrap();
        assert_eq!(identity.issuer, IDENTITY_ISSUER);
        assert_eq!(identity.subject, address);
        assert!(
            SolanaAuth::new("https://other.example")
                .unwrap()
                .verify_signature(&challenge, &signature)
                .is_err()
        );
    }

    #[test]
    fn rejects_wrong_wallet_modified_message_and_bad_signature_encoding() {
        let signer = wallet();
        let other = wallet();
        let address = bs58::encode(signer.verifying_key().as_bytes()).into_string();
        let solana = SolanaAuth::new("https://app.example").unwrap();
        let tokens = tokens();
        let response = solana.start(&tokens, address, None).unwrap();
        let challenge = tokens
            .verify_solana_challenge(&response.challenge_token)
            .unwrap();
        let wrong_wallet = STANDARD.encode(other.sign(response.message.as_bytes()).to_bytes());
        let changed_message = STANDARD.encode(
            signer
                .sign(format!("{}\n", response.message).as_bytes())
                .to_bytes(),
        );
        for signature in [
            wrong_wallet,
            changed_message,
            "not base64!".into(),
            STANDARD.encode([0; 63]),
            "A".repeat(89),
        ] {
            assert!(solana.verify_signature(&challenge, &signature).is_err());
        }
        // A valid signature cannot be transferred to a second challenge.
        let signature = STANDARD.encode(signer.sign(response.message.as_bytes()).to_bytes());
        let second = solana
            .start(&tokens, challenge.address.clone(), None)
            .unwrap();
        let second = tokens
            .verify_solana_challenge(&second.challenge_token)
            .unwrap();
        assert_ne!(challenge.nonce, second.nonce);
        assert!(solana.verify_signature(&second, &signature).is_err());
    }

    #[test]
    fn rejects_malformed_addresses_and_weak_keys() {
        let mut identity = [0; 32];
        identity[0] = 1;
        for address in [
            String::new(),
            "0".repeat(44),
            "1".repeat(45),
            bs58::encode([2; 31]).into_string(),
            bs58::encode(identity).into_string(),
            "1".repeat(32),
        ] {
            assert!(
                wallet_key(&address).is_err(),
                "accepted invalid wallet {address}"
            );
        }
    }

    #[test]
    fn linking_message_names_the_target_user() {
        let wallet = wallet();
        let user_id = Uuid::new_v4();
        let solana = SolanaAuth::new("http://localhost:3000").unwrap();
        let response = solana
            .start(
                &tokens(),
                bs58::encode(wallet.verifying_key().as_bytes()).into_string(),
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
    }
}

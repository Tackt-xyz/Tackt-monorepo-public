use chrono::Utc;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use uuid::Uuid;

use super::AuthError;

pub const ACCESS_SECONDS: i64 = 15 * 60;
pub const ID_TOKEN_SECONDS: i64 = 10 * 60;
pub const SESSION_DAYS: i64 = 30;

pub struct Tokens {
    issuer: String,
    encoding: EncodingKey,
    decoding: DecodingKey,
}

#[derive(Serialize, Deserialize)]
pub struct AccessClaims {
    pub sub: Uuid,
    pub sid: Uuid,
    pub iss: String,
    pub aud: String,
    pub iat: i64,
    pub exp: i64,
}

// A short-lived envelope bound to the provider and optional linking session.
// Its nonce is consumed in shared storage only after the ID token is verified.
#[derive(Serialize, Deserialize)]
pub struct IdTokenChallenge {
    pub provider: String,
    pub nonce: Uuid,
    pub link: Option<LinkContext>,
    pub iss: String,
    pub aud: String,
    pub iat: i64,
    pub exp: i64,
}

#[derive(Serialize, Deserialize)]
pub struct LinkContext {
    pub user_id: Uuid,
    pub session_id: Uuid,
}

impl Tokens {
    pub fn new(issuer: String, secret: &[u8]) -> Self {
        Self {
            issuer,
            encoding: EncodingKey::from_secret(secret),
            decoding: DecodingKey::from_secret(secret),
        }
    }

    pub fn access(
        &self,
        user: Uuid,
        session: Uuid,
        session_expiry: i64,
    ) -> Result<String, AuthError> {
        let now = Utc::now().timestamp();
        let exp = (now + ACCESS_SECONDS).min(session_expiry);
        if exp <= now {
            return Err(AuthError::Unauthorized);
        }
        self.sign(
            "at+jwt",
            &AccessClaims {
                sub: user,
                sid: session,
                iss: self.issuer.clone(),
                aud: "tackt-user-api".into(),
                iat: now,
                exp,
            },
        )
    }

    pub fn id_token_challenge(&self, mut challenge: IdTokenChallenge) -> Result<String, AuthError> {
        challenge.iss = self.issuer.clone();
        challenge.aud = "tackt-id-token".into();
        challenge.iat = Utc::now().timestamp();
        challenge.exp = challenge.iat + ID_TOKEN_SECONDS;
        self.sign("tackt-id-token+jwt", &challenge)
    }

    pub fn verify_access(&self, token: &str) -> Result<AccessClaims, AuthError> {
        let claims: AccessClaims = self.verify(token, "at+jwt", "tackt-user-api")?;
        validate_times(claims.iat, claims.exp, ACCESS_SECONDS)?;
        Ok(claims)
    }

    pub fn verify_id_token_challenge(&self, token: &str) -> Result<IdTokenChallenge, AuthError> {
        let claims: IdTokenChallenge =
            self.verify(token, "tackt-id-token+jwt", "tackt-id-token")?;
        validate_times(claims.iat, claims.exp, ID_TOKEN_SECONDS)?;
        Ok(claims)
    }

    pub fn solana_challenge(
        &self,
        address: String,
        uri: String,
        link: Option<LinkContext>,
    ) -> Result<(String, super::solana::Challenge), AuthError> {
        let iat = Utc::now().timestamp();
        let challenge = super::solana::Challenge {
            nonce: Uuid::new_v4(),
            address,
            uri,
            link,
            iss: self.issuer.clone(),
            aud: "tackt-solana".into(),
            iat,
            exp: iat + super::solana::CHALLENGE_SECONDS,
        };
        let token = self.sign("tackt-solana+jwt", &challenge)?;
        Ok((token, challenge))
    }

    pub fn verify_solana_challenge(
        &self,
        token: &str,
    ) -> Result<super::solana::Challenge, AuthError> {
        let challenge: super::solana::Challenge =
            self.verify(token, "tackt-solana+jwt", "tackt-solana")?;
        validate_times(
            challenge.iat,
            challenge.exp,
            super::solana::CHALLENGE_SECONDS,
        )?;
        Ok(challenge)
    }

    pub fn evm_challenge(
        &self,
        address: String,
        uri: String,
        link: Option<LinkContext>,
    ) -> Result<(String, super::evm::Challenge), AuthError> {
        let iat = Utc::now().timestamp();
        let challenge = super::evm::Challenge {
            nonce: Uuid::new_v4(),
            address,
            uri,
            link,
            iss: self.issuer.clone(),
            aud: "tackt-evm".into(),
            iat,
            exp: iat + super::evm::CHALLENGE_SECONDS,
        };
        let token = self.sign("tackt-evm+jwt", &challenge)?;
        Ok((token, challenge))
    }

    pub fn verify_evm_challenge(&self, token: &str) -> Result<super::evm::Challenge, AuthError> {
        let challenge: super::evm::Challenge = self.verify(token, "tackt-evm+jwt", "tackt-evm")?;
        validate_times(challenge.iat, challenge.exp, super::evm::CHALLENGE_SECONDS)?;
        Ok(challenge)
    }

    fn sign(&self, typ: &str, claims: &impl Serialize) -> Result<String, AuthError> {
        let mut header = Header::new(Algorithm::HS256);
        header.typ = Some(typ.into());
        encode(&header, claims, &self.encoding).map_err(|_| AuthError::Internal)
    }

    fn verify<T: DeserializeOwned>(
        &self,
        token: &str,
        typ: &str,
        audience: &str,
    ) -> Result<T, AuthError> {
        if token.len() > 16 * 1024 {
            return Err(AuthError::Unauthorized);
        }
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_issuer(&[&self.issuer]);
        validation.set_audience(&[audience]);
        validation.set_required_spec_claims(&["iss", "aud", "iat", "exp"]);
        validation.leeway = 0;
        let decoded =
            decode::<T>(token, &self.decoding, &validation).map_err(|_| AuthError::Unauthorized)?;
        if decoded.header.typ.as_deref() != Some(typ) {
            return Err(AuthError::Unauthorized);
        }
        Ok(decoded.claims)
    }
}

fn validate_times(iat: i64, exp: i64, max_lifetime: i64) -> Result<(), AuthError> {
    let now = Utc::now().timestamp();
    if iat > now + 30 || exp <= now || exp <= iat || exp.saturating_sub(iat) > max_lifetime {
        return Err(AuthError::Unauthorized);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_wrong_key_issuer_audience_type_and_expiry() {
        let secret = uuid::Uuid::new_v4().to_string();
        let key = secret.as_bytes();
        let tokens = Tokens::new("https://api.example".into(), key);
        let user = Uuid::new_v4();
        let session = Uuid::new_v4();
        let token = tokens
            .access(user, session, Utc::now().timestamp() + 3600)
            .unwrap();
        assert_eq!(tokens.verify_access(&token).unwrap().sub, user);
        assert!(
            Tokens::new(
                "https://api.example".into(),
                uuid::Uuid::new_v4().to_string().as_bytes()
            )
            .verify_access(&token)
            .is_err()
        );
        assert!(
            Tokens::new("https://other.example".into(), key)
                .verify_access(&token)
                .is_err()
        );
        assert!(tokens.verify_id_token_challenge(&token).is_err());
        let mut claims = tokens.verify_access(&token).unwrap();
        claims.aud = "other".into();
        assert!(
            tokens
                .verify_access(&tokens.sign("at+jwt", &claims).unwrap())
                .is_err()
        );
        claims.aud = "tackt-user-api".into();
        assert!(
            tokens
                .verify_access(&tokens.sign("JWT", &claims).unwrap())
                .is_err()
        );
        claims.exp = Utc::now().timestamp() - 1;
        assert!(
            tokens
                .verify_access(&tokens.sign("at+jwt", &claims).unwrap())
                .is_err()
        );
    }
    #[test]
    fn id_token_challenges_are_signed_expiring_and_separate_from_access_tokens() {
        let secret = uuid::Uuid::new_v4().to_string();
        let key = secret.as_bytes();
        let tokens = Tokens::new("https://api.example".into(), key);
        let challenge = IdTokenChallenge {
            provider: "google".into(),
            nonce: Uuid::new_v4(),
            link: Some(LinkContext {
                user_id: Uuid::new_v4(),
                session_id: Uuid::new_v4(),
            }),
            iss: String::new(),
            aud: String::new(),
            iat: 0,
            exp: 0,
        };
        let signed = tokens.id_token_challenge(challenge).unwrap();
        assert!(tokens.verify_access(&signed).is_err());
        let mut claims = tokens.verify_id_token_challenge(&signed).unwrap();
        assert_eq!(claims.provider, "google");
        assert!(claims.link.is_some());
        let mut parts: Vec<_> = signed.split('.').map(str::to_owned).collect();
        use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
        claims.provider = "attacker".into();
        parts[1] = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap());
        assert!(tokens.verify_id_token_challenge(&parts.join(".")).is_err());
        claims.iat = Utc::now().timestamp() - ID_TOKEN_SECONDS - 10;
        claims.exp = Utc::now().timestamp() - 10;
        assert!(
            tokens
                .verify_id_token_challenge(&tokens.sign("tackt-id-token+jwt", &claims).unwrap())
                .is_err()
        );
        assert!(
            tokens
                .access(Uuid::new_v4(), Uuid::new_v4(), Utc::now().timestamp())
                .is_err()
        );
    }
    #[test]
    fn solana_challenges_reject_tampering_expiry_and_token_substitution() {
        let secret = uuid::Uuid::new_v4().to_string();
        let tokens = Tokens::new("https://api.example".into(), secret.as_bytes());
        let (token, mut challenge) = tokens
            .solana_challenge("address".into(), "https://app.example/".into(), None)
            .unwrap();
        assert!(tokens.verify_solana_challenge(&token).is_ok());
        assert!(tokens.verify_access(&token).is_err());
        assert!(tokens.verify_id_token_challenge(&token).is_err());
        let access = tokens
            .access(
                Uuid::new_v4(),
                Uuid::new_v4(),
                Utc::now().timestamp() + 3600,
            )
            .unwrap();
        assert!(tokens.verify_solana_challenge(&access).is_err());
        assert!(
            Tokens::new("https://other.example".into(), secret.as_bytes())
                .verify_solana_challenge(&token)
                .is_err()
        );
        use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
        let mut parts: Vec<_> = token.split('.').map(str::to_owned).collect();
        challenge.address = "attacker-wallet".into();
        parts[1] = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&challenge).unwrap());
        assert!(tokens.verify_solana_challenge(&parts.join(".")).is_err());
        challenge.iat = Utc::now().timestamp() - 301;
        challenge.exp = Utc::now().timestamp() - 1;
        assert!(
            tokens
                .verify_solana_challenge(&tokens.sign("tackt-solana+jwt", &challenge).unwrap())
                .is_err()
        );
    }
    #[test]
    fn evm_challenges_reject_tampering_expiry_and_token_substitution() {
        let secret = uuid::Uuid::new_v4().to_string();
        let tokens = Tokens::new("https://api.example".into(), secret.as_bytes());
        let (token, mut challenge) = tokens
            .evm_challenge("address".into(), "https://app.example/".into(), None)
            .unwrap();
        assert!(tokens.verify_evm_challenge(&token).is_ok());
        assert!(tokens.verify_solana_challenge(&token).is_err());
        let (solana_token, _) = tokens
            .solana_challenge("address".into(), "https://app.example/".into(), None)
            .unwrap();
        assert!(tokens.verify_evm_challenge(&solana_token).is_err());
        assert!(tokens.verify_access(&token).is_err());
        assert!(tokens.verify_id_token_challenge(&token).is_err());
        let access = tokens
            .access(
                Uuid::new_v4(),
                Uuid::new_v4(),
                Utc::now().timestamp() + 3600,
            )
            .unwrap();
        assert!(tokens.verify_evm_challenge(&access).is_err());
        assert!(
            Tokens::new("https://other.example".into(), secret.as_bytes())
                .verify_evm_challenge(&token)
                .is_err()
        );
        use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
        let mut parts: Vec<_> = token.split('.').map(str::to_owned).collect();
        challenge.address = "attacker-wallet".into();
        parts[1] = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&challenge).unwrap());
        assert!(tokens.verify_evm_challenge(&parts.join(".")).is_err());
        challenge.iat = Utc::now().timestamp() - 301;
        challenge.exp = Utc::now().timestamp() - 1;
        assert!(
            tokens
                .verify_evm_challenge(&tokens.sign("tackt-evm+jwt", &challenge).unwrap())
                .is_err()
        );
    }
}

use crate::auth::UserAuthConfig;
use crate::beta_access::BetaInviteCode;
use crate::orchestration::FinancialHttpConfig;
use anyhow::{Context, Result};
use std::{env, net::SocketAddr, time::Duration};

/// Deliberately not `Debug`: `operator_token` and `database_url` are secrets
/// and must never reach logs through derived formatting.
#[derive(Clone)]
pub struct Config {
    pub database_url: String,
    pub listen_addr: SocketAddr,
    pub operator_token: String,
    pub database_connections: u32,
    /// Explicit opt-in for serving the bearer-token API on a non-loopback
    /// listener without TLS termination in front of it.
    pub allow_cleartext: bool,
    /// Optional: without it lifecycle events are not published and consumers
    /// rely on database polling.
    pub nats_url: Option<String>,
    /// OIDC and wallet login verification and Core-issued user JWTs, configured together.
    pub user_auth: UserAuthConfig,
    /// First-login provisioning is an explicit deployment decision. When
    /// false, only identities already linked to Core users may authenticate.
    pub user_signup_enabled: bool,
    /// A shared invite code; absent configuration disables beta redemption.
    pub beta_invite_code: Option<BetaInviteCode>,
    /// Optional as one all-or-nothing capability; partial or unsafe configuration fails startup.
    pub financial_services: Option<FinancialHttpConfig>,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let operator_token = required("CORE_API_OPERATOR_TOKEN")?;
        if operator_token.len() < 24 {
            anyhow::bail!("CORE_API_OPERATOR_TOKEN must be at least 24 characters");
        }
        let user_auth = user_auth_config()?;
        let user_signup_enabled = parse_bool("CORE_API_USER_SIGNUP_ENABLED", false)?;
        let beta_invite_code = match env::var("CORE_API_BETA_INVITE_CODE") {
            Ok(code) => Some(BetaInviteCode::new(&code)?),
            Err(env::VarError::NotPresent) => None,
            Err(env::VarError::NotUnicode(_)) => {
                anyhow::bail!("CORE_API_BETA_INVITE_CODE must be valid Unicode")
            }
        };
        Ok(Self {
            database_url: required("DATABASE_URL")?,
            listen_addr: env::var("CORE_API_LISTEN_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:8080".into())
                .parse()
                .context("invalid CORE_API_LISTEN_ADDR")?,
            operator_token,
            database_connections: {
                let connections: u32 = env::var("CORE_API_DATABASE_CONNECTIONS")
                    .unwrap_or_else(|_| "10".into())
                    .parse()
                    .context("invalid CORE_API_DATABASE_CONNECTIONS")?;
                if connections == 0 {
                    anyhow::bail!("CORE_API_DATABASE_CONNECTIONS must be at least 1");
                }
                connections
            },
            allow_cleartext: match env::var("CORE_API_ALLOW_CLEARTEXT").as_deref() {
                Ok("true") | Ok("1") => true,
                Ok("false") | Ok("0") | Err(_) => false,
                Ok(other) => {
                    anyhow::bail!("invalid CORE_API_ALLOW_CLEARTEXT {other:?}; use true or false")
                }
            },
            nats_url: env::var("NATS_URL").ok(),
            user_auth,
            user_signup_enabled,
            beta_invite_code,
            financial_services: financial_services_config()?,
        })
    }
}

fn financial_services_config() -> Result<Option<FinancialHttpConfig>> {
    let risk_url = env::var("CORE_API_RISK_URL").ok();
    let risk_token = env::var("CORE_API_RISK_TOKEN").ok();
    let ledger_url = env::var("CORE_API_LEDGER_URL").ok();
    let ledger_token = env::var("CORE_API_LEDGER_TOKEN").ok();
    match (risk_url, risk_token, ledger_url, ledger_token) {
        (None, None, None, None) => Ok(None),
        (Some(risk_url), Some(risk_token), Some(ledger_url), Some(ledger_token)) => {
            let timeout_ms = env::var("CORE_API_FINANCIAL_HTTP_TIMEOUT_MS")
                .unwrap_or_else(|_| "5000".into())
                .parse()
                .context("invalid CORE_API_FINANCIAL_HTTP_TIMEOUT_MS")?;
            if !(1..=30_000).contains(&timeout_ms) {
                anyhow::bail!("CORE_API_FINANCIAL_HTTP_TIMEOUT_MS must be within 1..=30000");
            }
            FinancialHttpConfig::new(
                &risk_url,
                risk_token,
                &ledger_url,
                ledger_token,
                Duration::from_millis(timeout_ms),
            )
            .map(Some)
            .map_err(anyhow::Error::from)
        }
        _ => anyhow::bail!(
            "CORE_API_RISK_URL, CORE_API_RISK_TOKEN, CORE_API_LEDGER_URL, and CORE_API_LEDGER_TOKEN must be configured together"
        ),
    }
}

fn user_auth_config() -> Result<UserAuthConfig> {
    for legacy in [
        "CORE_API_USER_AUTH_ISSUER",
        "CORE_API_USER_AUTH_AUDIENCE",
        "CORE_API_USER_AUTH_JWKS_URL",
    ] {
        if env::var_os(legacy).is_some() {
            anyhow::bail!(
                "{legacy} is no longer supported; configure CORE_API_AUTH_ISSUER, CORE_API_AUTH_JWT_SECRET, CORE_API_AUTH_FRONTEND_URI and any OIDC providers"
            );
        }
    }
    for legacy in ["CORE_API_AUTH_SOLANA_URI", "CORE_API_AUTH_EVM_URI"] {
        if env::var_os(legacy).is_some() {
            anyhow::bail!(
                "{legacy} is no longer supported; set CORE_API_AUTH_FRONTEND_URI for both wallet methods"
            );
        }
    }
    let config = UserAuthConfig {
        issuer: required("CORE_API_AUTH_ISSUER")?,
        frontend_uri: required("CORE_API_AUTH_FRONTEND_URI")?,
        jwt_secret: hex::decode(required("CORE_API_AUTH_JWT_SECRET")?)
            .context("CORE_API_AUTH_JWT_SECRET must be hex-encoded random bytes")?,
        providers: serde_json::from_str(
            &env::var("CORE_API_AUTH_PROVIDERS").unwrap_or_else(|_| "[]".into()),
        )
        .map_err(|_| {
            anyhow::anyhow!(
                "CORE_API_AUTH_PROVIDERS must be a JSON array of provider configurations"
            )
        })?,
    };
    config.validate()?;
    Ok(config)
}

fn parse_bool(key: &str, default: bool) -> Result<bool> {
    match env::var(key).as_deref() {
        Ok("true") | Ok("1") => Ok(true),
        Ok("false") | Ok("0") => Ok(false),
        Err(_) => Ok(default),
        Ok(other) => anyhow::bail!("invalid {key} {other:?}; use true or false"),
    }
}

fn required(key: &str) -> Result<String> {
    env::var(key).with_context(|| format!("{key} is required"))
}

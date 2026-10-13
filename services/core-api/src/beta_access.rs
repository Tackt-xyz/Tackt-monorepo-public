use crate::openapi::routes;
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use utoipa::ToSchema;

use crate::{
    api::ApiState,
    auth::{AuthError, AuthenticatedUser, require_open_user},
};

const MAX_INVITE_BYTES: usize = 128;

/// Stores only the configured code's digest. Deliberately has no Debug implementation.
#[derive(Clone)]
pub struct BetaInviteCode([u8; 32]);

impl BetaInviteCode {
    pub fn new(code: &str) -> anyhow::Result<Self> {
        anyhow::ensure!(
            valid_code_shape(code),
            "CORE_API_BETA_INVITE_CODE must contain 1–128 bytes without surrounding whitespace or control characters"
        );
        Ok(Self(Sha256::digest(code.as_bytes()).into()))
    }

    fn matches(&self, code: &str) -> bool {
        let digest: [u8; 32] = Sha256::digest(code.as_bytes()).into();
        bool::from(self.0.ct_eq(&digest))
    }
}

fn valid_code_shape(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= MAX_INVITE_BYTES
        && code.trim() == code
        && !code.chars().any(char::is_control)
}

pub(crate) fn router() -> (Router<ApiState>, utoipa::openapi::OpenApi) {
    let (router, spec) = routes!(redeem => __path_redeem);
    (router.layer(DefaultBodyLimit::max(1024)), spec)
}

#[derive(Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RedeemInvite {
    /// Shared invite code; maximum 128 UTF-8 bytes, no surrounding whitespace or control characters.
    #[schema(min_length = 1, max_length = 128)]
    invite_code: String,
}

/// Redeem CORE_API_BETA_INVITE_CODE. Closed users are rejected before invite validation. For open users, invalid codes return 403 and an unconfigured code returns 503. Repeated valid redemption succeeds.
#[utoipa::path(post, path = "/api/v1/users/me/beta-access", request_body = RedeemInvite,
    responses((status = 200, body = BetaAccess)),
    security(("AccessCookie" = []), ("UserBearer" = [], "UserId" = [])), tag = "Users")]
async fn redeem(
    user: AuthenticatedUser,
    State(state): State<ApiState>,
    Json(input): Json<RedeemInvite>,
) -> Result<Json<BetaAccess>, AuthError> {
    let user = state.repo.core_user(user.user_id).await?;
    require_open_user(&user)?;
    if !valid_code_shape(&input.invite_code) {
        return Err(AuthError::InvalidField("inviteCode"));
    }
    let code = state
        .beta_invite_code
        .as_ref()
        .ok_or(AuthError::BetaAccessUnavailable)?;
    if !code.matches(&input.invite_code) {
        return Err(AuthError::InvalidInviteCode);
    }
    state.repo.grant_beta_access(user.id).await?;
    tracing::info!(user_id = %user.id, success = true, "beta access granted");
    Ok(Json(BetaAccess { beta_access: true }))
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct BetaAccess {
    beta_access: bool,
}

use crate::openapi::{UserStatusSchema, routes};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header::CACHE_CONTROL},
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tackt_postgres::{AuthSession, LoginIdentity};
use tackt_primitives::{CoreUser, UserStatus};
use tower_http::set_header::SetResponseHeaderLayer;
use tracing::Instrument;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use super::{
    AuthError, AuthenticatedUser, UserAuth, VerifiedIdentity, authenticate, cookies, evm,
    require_open_user, solana,
    tokens::{ACCESS_SECONDS, LinkContext, SESSION_DAYS},
};
use crate::api::ApiState;

pub fn router() -> (Router<ApiState>, utoipa::openapi::OpenApi) {
    let (router, spec) = routes!(
        providers => __path_providers,
        login => __path_login,
        link => __path_link,
        create_session => __path_create_session,
        methods => __path_methods,
        create_identity => __path_create_identity,
        refresh => __path_refresh,
        logout => __path_logout,
        current_user => __path_current_user,
    );
    (
        router
            .layer(DefaultBodyLimit::max(32 * 1024))
            .layer(SetResponseHeaderLayer::overriding(
                CACHE_CONTROL,
                HeaderValue::from_static("no-store"),
            )),
        spec,
    )
}

#[derive(Deserialize, Default, IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PageQuery {
    #[param(minimum = 1, maximum = 100, default = 20)]
    limit: Option<u32>,
    #[param(max_length = 8192)]
    cursor: Option<String>,
}

impl PageQuery {
    fn limit(&self) -> Result<u32, AuthError> {
        let limit = self.limit.unwrap_or(20);
        if !(1..=100).contains(&limit) {
            return Err(AuthError::InvalidField("limit"));
        }
        Ok(limit)
    }

    fn after<T: serde::de::DeserializeOwned>(&self) -> Result<Option<T>, AuthError> {
        self.cursor
            .as_deref()
            .map(|cursor| {
                if cursor.len() > 8192 {
                    return Err(AuthError::InvalidField("cursor"));
                }
                let bytes = URL_SAFE_NO_PAD
                    .decode(cursor)
                    .map_err(|_| AuthError::InvalidField("cursor"))?;
                serde_json::from_slice(&bytes).map_err(|_| AuthError::InvalidField("cursor"))
            })
            .transpose()
    }
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct Page<T> {
    items: Vec<T>,
    #[schema(required = true)]
    next_cursor: Option<String>,
    has_more: bool,
}

fn cursor(value: &impl Serialize) -> Result<String, AuthError> {
    Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(value).map_err(|_| AuthError::Internal)?))
}

/// List configured OIDC providers and the built-in solana and evm providers.
#[utoipa::path(get, path = "/api/v1/auth/providers", params(PageQuery), responses((status = 200, body = Page<String>)), tag = "Authentication")]
async fn providers(
    State(state): State<ApiState>,
    Query(query): Query<PageQuery>,
) -> Result<Json<Page<String>>, AuthError> {
    let auth = &state.user_auth;
    let limit = query.limit()? as usize;
    let after: Option<String> = query.after()?;
    let mut names: Vec<_> = auth.0.providers.keys().cloned().collect();
    names.extend(["solana".into(), "evm".into()]);
    names.sort();
    names.retain(|name| after.as_ref().is_none_or(|after| name > after));
    let has_more = names.len() > limit;
    names.truncate(limit);
    let next_cursor = if has_more {
        names.last().map(cursor).transpose()?
    } else {
        None
    };
    Ok(Json(Page {
        items: names,
        next_cursor,
        has_more,
    }))
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct LoginStarted {
    client_id: String,
    nonce: Uuid,
    challenge_token: String,
    /// Challenge expiry in Unix milliseconds.
    expires_at: i64,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
struct EmptyRequest {}

#[derive(Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WalletChallengeRequest {
    address: String,
}

#[derive(Deserialize, ToSchema)]
#[serde(untagged)]
enum ChallengeRequest {
    Wallet(WalletChallengeRequest),
    Oidc(EmptyRequest),
}

/// Start login: send `{}` for OIDC or `{address}` for a wallet. Complete the returned challenge through POST /api/v1/auth/sessions.
#[utoipa::path(post, path = "/api/v1/auth/providers/{provider}/challenges", params(("provider" = String, Path, description = "solana, evm, or a configured OIDC provider")), request_body = ChallengeRequest, responses((status = 201, body = LoginChallenge)), tag = "Authentication")]
async fn login(
    State(state): State<ApiState>,
    Path(provider): Path<String>,
    Json(input): Json<ChallengeRequest>,
) -> Result<Response, AuthError> {
    start(&state, &provider, input, None).await
}

/// Start an identity-linking challenge bound to the current user and session. Uses the same provider-specific input as a login challenge.
#[utoipa::path(post, path = "/api/v1/users/me/identities/{provider}/challenges", params(("provider" = String, Path, description = "solana, evm, or a configured OIDC provider")), request_body = ChallengeRequest, responses((status = 201, body = LoginChallenge)), security(("AccessCookie" = []), ("UserBearer" = [], "UserId" = [])), tag = "Identities")]
async fn link(
    user: AuthenticatedUser,
    State(state): State<ApiState>,
    Path(provider): Path<String>,
    Json(input): Json<ChallengeRequest>,
) -> Result<Response, AuthError> {
    load_open_user(&state, user.user_id).await?;
    start(&state, &provider, input, Some(user)).await
}

async fn start(
    state: &ApiState,
    provider: &str,
    input: ChallengeRequest,
    user: Option<AuthenticatedUser>,
) -> Result<Response, AuthError> {
    let auth = &state.user_auth;
    let link = user.map(|user| LinkContext {
        session_id: user.session_id,
        user_id: user.user_id,
    });
    let response = match (provider, input) {
        ("solana", ChallengeRequest::Wallet(input)) => {
            Json(auth.0.solana.start(&auth.0.tokens, input.address, link)?).into_response()
        }
        ("evm", ChallengeRequest::Wallet(input)) => {
            Json(auth.0.evm.start(&auth.0.tokens, input.address, link)?).into_response()
        }
        (name, ChallengeRequest::Oidc(_)) if !matches!(name, "solana" | "evm") => {
            let provider = auth.provider(name)?;
            let challenge = super::tokens::IdTokenChallenge {
                provider: name.into(),
                nonce: Uuid::new_v4(),
                link,
                iss: String::new(),
                aud: String::new(),
                iat: 0,
                exp: 0,
            };
            let nonce = challenge.nonce;
            let challenge_token = auth.0.tokens.id_token_challenge(challenge)?;
            let expires_at = auth
                .0
                .tokens
                .verify_id_token_challenge(&challenge_token)?
                .exp
                * 1000;
            Json(LoginStarted {
                client_id: provider.config.client_id.clone(),
                nonce,
                challenge_token,
                expires_at,
            })
            .into_response()
        }
        _ => return Err(AuthError::InvalidRequest),
    };
    Ok((StatusCode::CREATED, response).into_response())
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct IdentityResource {
    user_id: Uuid,
    issuer: String,
    subject: String,
}

#[derive(Deserialize, ToSchema)]
#[serde(
    tag = "method",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub(super) enum Proof {
    #[serde(rename_all = "camelCase")]
    Oidc {
        id_token: String,
        challenge_token: String,
    },
    #[serde(rename_all = "camelCase")]
    Evm {
        challenge_token: String,
        signature: String,
    },
    #[serde(rename_all = "camelCase")]
    Solana {
        challenge_token: String,
        signature: String,
    },
}

/// Verify an OIDC ID token or wallet signature and create a session. New users are created only when signup is enabled; credentials are returned in cookies.
#[utoipa::path(post, path = "/api/v1/auth/sessions", request_body = Proof, responses((status = 201, body = SessionResource)), tag = "Authentication")]
async fn create_session(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(proof): Json<Proof>,
) -> Result<Response, AuthError> {
    complete_proof(state, headers, proof, false).await
}

/// Complete a linking challenge using the same authenticated user and session that started it. Returns 201 for a new identity or 200 for an existing link.
#[utoipa::path(post, path = "/api/v1/users/me/identities", request_body = Proof, responses((status = 200, body = IdentityResource), (status = 201, body = IdentityResource)), security(("AccessCookie" = []), ("UserBearer" = [], "UserId" = [])), tag = "Identities")]
async fn create_identity(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(proof): Json<Proof>,
) -> Result<Response, AuthError> {
    complete_proof(state, headers, proof, true).await
}

async fn complete_proof(
    state: ApiState,
    headers: HeaderMap,
    proof: Proof,
    linking: bool,
) -> Result<Response, AuthError> {
    let method = match &proof {
        Proof::Oidc { .. } => "oidc",
        Proof::Solana { .. } => "solana",
        Proof::Evm { .. } => "evm",
    };
    let span = tracing::info_span!("authentication", method, user_id = tracing::field::Empty);
    async {
        let result = verify_proof(&state, &headers, proof, linking).await;
        if result.is_err() {
            tracing::warn!(success = false, "authentication proof rejected");
        }
        let (identity, user) = result?;
        finish_identity(&state, identity, user).await
    }
    .instrument(span)
    .await
}

async fn verify_proof(
    state: &ApiState,
    headers: &HeaderMap,
    proof: Proof,
    linking: bool,
) -> Result<(VerifiedIdentity, Option<AuthenticatedUser>), AuthError> {
    let auth = &state.user_auth;
    match proof {
        Proof::Oidc {
            id_token,
            challenge_token,
        } => {
            let challenge = auth.0.tokens.verify_id_token_challenge(&challenge_token)?;
            let user = authenticate_link(headers, state, challenge.link.as_ref(), linking).await?;
            let identity = auth
                .provider(&challenge.provider)?
                .verify(&id_token, &challenge)
                .await?;
            consume_challenge(state, challenge.nonce, challenge.exp).await?;
            Ok((identity, user))
        }
        Proof::Solana {
            challenge_token,
            signature,
        } => {
            let solana = &auth.0.solana;
            let challenge = auth.0.tokens.verify_solana_challenge(&challenge_token)?;
            let user = authenticate_link(headers, state, challenge.link.as_ref(), linking).await?;
            let identity = solana.verify_signature(&challenge, &signature)?;
            consume_challenge(state, challenge.nonce, challenge.exp).await?;
            Ok((identity, user))
        }
        Proof::Evm {
            challenge_token,
            signature,
        } => {
            let evm = &auth.0.evm;
            let challenge = auth.0.tokens.verify_evm_challenge(&challenge_token)?;
            let user = authenticate_link(headers, state, challenge.link.as_ref(), linking).await?;
            let identity = evm.verify_signature(&challenge, &signature)?;
            consume_challenge(state, challenge.nonce, challenge.exp).await?;
            Ok((identity, user))
        }
    }
}

async fn consume_challenge(state: &ApiState, nonce: Uuid, exp: i64) -> Result<(), AuthError> {
    let expires_at = DateTime::from_timestamp(exp, 0).ok_or(AuthError::Unauthorized)?;
    // Only valid proofs consume nonces, before any identity/session write.
    if !state.repo.consume_auth_challenge(nonce, expires_at).await? {
        return Err(AuthError::ChallengeUsed);
    }
    Ok(())
}

async fn authenticate_link(
    headers: &HeaderMap,
    state: &ApiState,
    link: Option<&LinkContext>,
    linking: bool,
) -> Result<Option<AuthenticatedUser>, AuthError> {
    if link.is_some() != linking {
        return Err(AuthError::InvalidFlow);
    }
    let Some(link) = link else {
        return Ok(None);
    };
    let user = authenticate(headers, &state.user_auth)?;
    if user.user_id != link.user_id || user.session_id != link.session_id {
        return Err(AuthError::Unauthorized);
    }
    load_open_user(state, user.user_id).await?;
    Ok(Some(user))
}

async fn finish_identity(
    state: &ApiState,
    identity: VerifiedIdentity,
    linking_user: Option<AuthenticatedUser>,
) -> Result<Response, AuthError> {
    let auth = &state.user_auth;
    if let Some(user) = linking_user {
        let linked = state
            .repo
            .link_core_user_identity(user.user_id, &identity.issuer, &identity.subject)
            .await?;
        tracing::Span::current().record("user_id", tracing::field::display(user.user_id));
        tracing::info!(user_id = %user.user_id, resource_type = "login_identity", success = true, "login identity linked");
        return Ok((
            if linked {
                StatusCode::CREATED
            } else {
                StatusCode::OK
            },
            Json(IdentityResource {
                user_id: user.user_id,
                issuer: identity.issuer,
                subject: identity.subject,
            }),
        )
            .into_response());
    }
    let (user, created) = state
        .repo
        .bootstrap_core_user_identity(
            &identity.issuer,
            &identity.subject,
            state.user_signup_enabled,
            &crate::signing::generate_user_signing_keys(),
        )
        .await?;
    require_open_user(&user)?;
    if created {
        crate::api::publish_event(
            state,
            tackt_nats::UserEventKind::Created { user: user.clone() },
        )
        .await;
    }
    let session = AuthSession {
        id: Uuid::new_v4(),
        user_id: user.id,
        expires_at: Utc::now() + chrono::Duration::days(SESSION_DAYS),
    };
    let refresh_token = new_refresh(session.id);
    let response = session_response(auth, &user, &session, &refresh_token)?;
    state
        .repo
        .create_auth_session(&session, &refresh_hash(&refresh_token))
        .await?;
    tracing::Span::current().record("user_id", tracing::field::display(user.id));
    tracing::info!(user_id = %user.id, resource_id = %session.id, resource_type = "session", success = true, "session created");
    Ok(response)
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct SessionResource {
    id: Uuid,
    user: UserProfile,
    /// Access JWT expiry in Unix milliseconds.
    expires_at: i64,
    /// Fixed session expiry in Unix milliseconds.
    refresh_expires_at: i64,
}

fn session_response(
    auth: &UserAuth,
    user: &CoreUser,
    session: &AuthSession,
    refresh_token: &str,
) -> Result<Response, AuthError> {
    let access_token = auth
        .0
        .tokens
        .access(user.id, session.id, session.expires_at.timestamp())?;
    let access_seconds = ACCESS_SECONDS
        .min((session.expires_at - Utc::now()).num_seconds())
        .max(0);
    let mut response = (
        StatusCode::CREATED,
        Json(SessionResource {
            id: session.id,
            user: UserProfile::from(user),
            expires_at: (Utc::now().timestamp() + access_seconds) * 1000,
            refresh_expires_at: session.expires_at.timestamp_millis(),
        }),
    )
        .into_response();
    cookies::set(
        response.headers_mut(),
        &access_token,
        refresh_token,
        access_seconds,
        (session.expires_at - Utc::now()).num_seconds(),
    );
    Ok(response)
}

#[derive(Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RefreshRequest {
    refresh_token: Option<String>,
}

/// Rotate a refresh credential without extending the session deadline. Send `{}` with the refresh cookie, or `{refreshToken}` without it; supplying both is rejected.
#[utoipa::path(post, path = "/api/v1/auth/session-tokens", request_body = RefreshRequest, responses((status = 201, body = SessionResource)), security(("RefreshCookie" = []), ()), tag = "Authentication")]
async fn refresh(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(input): Json<RefreshRequest>,
) -> Result<Response, AuthError> {
    let cookie = cookies::get(&headers, cookies::REFRESH_COOKIE)?;
    if cookie.is_some() && input.refresh_token.is_some() {
        return Err(AuthError::InvalidRequest);
    }
    let refresh_token = cookie
        .or(input.refresh_token)
        .ok_or(AuthError::SessionExpired)?;
    if refresh_token.len() > 128 {
        return Err(AuthError::Unauthorized);
    }
    let (id, _) = refresh_token
        .split_once('.')
        .ok_or(AuthError::Unauthorized)?;
    let id = Uuid::parse_str(id).map_err(|_| AuthError::Unauthorized)?;
    let next = new_refresh(id);
    let session = state
        .repo
        .rotate_auth_session(id, &refresh_hash(&refresh_token), &refresh_hash(&next))
        .await?
        .ok_or(AuthError::SessionExpired)?;
    let user = state.repo.core_user(session.user_id).await?;
    if let Err(error) = require_open_user(&user) {
        state
            .repo
            .revoke_auth_session(session.id, session.user_id)
            .await?;
        return Err(error);
    }
    tracing::Span::current().record("user_id", tracing::field::display(user.id));
    tracing::info!(user_id = %user.id, resource_id = %session.id, success = true, "session credentials rotated");
    session_response(&state.user_auth, &user, &session, &next)
}

/// Revoke the current refresh session and clear both cookies. Requires a valid access JWT; issued access JWTs remain valid until expiry.
#[utoipa::path(delete, path = "/api/v1/users/me/sessions/current", responses((status = 200, body = RevokedSession)), security(("AccessCookie" = []), ("UserBearer" = [], "UserId" = [])), tag = "Authentication")]
async fn logout(
    user: AuthenticatedUser,
    State(state): State<ApiState>,
) -> Result<Response, AuthError> {
    state
        .repo
        .revoke_auth_session(user.session_id, user.user_id)
        .await?;
    tracing::info!(user_id = %user.user_id, resource_id = %user.session_id, success = true, "session revoked");
    let mut response = Json(RevokedSession {
        id: user.session_id,
        revoked: true,
    })
    .into_response();
    cookies::clear(response.headers_mut());
    Ok(response)
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct UserProfile {
    id: Uuid,
    #[schema(value_type = UserStatusSchema)]
    status: UserStatus,
    beta_access: bool,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    /// Unix milliseconds.
    #[schema(value_type = i64)]
    created_at: DateTime<Utc>,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    /// Unix milliseconds.
    #[schema(value_type = i64)]
    updated_at: DateTime<Utc>,
}

impl From<&CoreUser> for UserProfile {
    fn from(user: &CoreUser) -> Self {
        Self {
            id: user.id,
            status: user.status,
            beta_access: user.beta_access,
            created_at: user.created_at,
            updated_at: user.updated_at,
        }
    }
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct CurrentUserProfile {
    #[serde(flatten)]
    user: UserProfile,
    /// Provisioned Solana wallet public key in base58; null when no wallet is provisioned.
    #[schema(required = true)]
    solana_address: Option<String>,
    /// Provisioned EVM wallet address in lowercase hex; null when no wallet is provisioned.
    #[schema(required = true)]
    evm_address: Option<String>,
}

/// Return the authenticated user profile and provisioned wallet addresses. Closed users are rejected.
#[utoipa::path(get, path = "/api/v1/users/me", responses((status = 200, body = CurrentUserProfile)), security(("AccessCookie" = []), ("UserBearer" = [], "UserId" = [])), tag = "Users")]
async fn current_user(
    user: AuthenticatedUser,
    State(state): State<ApiState>,
) -> Result<Json<CurrentUserProfile>, AuthError> {
    let (user, addresses) = state
        .repo
        .core_user_with_wallet_addresses(user.user_id)
        .await?;
    require_open_user(&user)?;
    let (solana_address, evm_address) = match addresses {
        Some(addresses) => (Some(addresses.solana), Some(addresses.ethereum)),
        None => (None, None),
    };
    Ok(Json(CurrentUserProfile {
        user: UserProfile::from(&user),
        solana_address,
        evm_address,
    }))
}

async fn load_open_user(state: &ApiState, user_id: Uuid) -> Result<CoreUser, AuthError> {
    let user = state.repo.core_user(user_id).await?;
    require_open_user(&user)?;
    Ok(user)
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct LoginMethod {
    #[schema(required = true)]
    provider: Option<String>,
    issuer: String,
    subject: String,
}

/// List the authenticated user’s linked identities. Closed users are rejected.
#[utoipa::path(get, path = "/api/v1/users/me/identities", params(PageQuery), responses((status = 200, body = Page<LoginMethod>)), security(("AccessCookie" = []), ("UserBearer" = [], "UserId" = [])), tag = "Identities")]
async fn methods(
    user: AuthenticatedUser,
    State(state): State<ApiState>,
    Query(query): Query<PageQuery>,
) -> Result<Json<Page<LoginMethod>>, AuthError> {
    load_open_user(&state, user.user_id).await?;
    let auth = &state.user_auth;
    let limit = query.limit()?;
    let after: Option<(String, String)> = query.after()?;
    let after = after.map(|(issuer, subject)| LoginIdentity { issuer, subject });
    let mut identities = state
        .repo
        .core_user_identities(user.user_id, after.as_ref(), limit + 1)
        .await?;
    let has_more = identities.len() > limit as usize;
    identities.truncate(limit as usize);
    let next_cursor = if has_more {
        identities
            .last()
            .map(|identity| cursor(&(&identity.issuer, &identity.subject)))
            .transpose()?
    } else {
        None
    };
    Ok(Json(Page {
        next_cursor,
        has_more,
        items: identities
            .into_iter()
            .map(|identity| LoginMethod {
                provider: if identity.issuer == solana::IDENTITY_ISSUER {
                    Some("solana".into())
                } else if identity.issuer == evm::IDENTITY_ISSUER {
                    Some("evm".into())
                } else {
                    auth.0
                        .providers
                        .iter()
                        .find(|(_, p)| p.config.issuer == identity.issuer)
                        .map(|(name, _)| name.clone())
                },
                issuer: identity.issuer,
                subject: identity.subject,
            })
            .collect(),
    }))
}

fn new_refresh(session: Uuid) -> String {
    {
        use k256::elliptic_curve::rand_core::{OsRng, RngCore};
        let mut secret = [0_u8; 32];
        OsRng.fill_bytes(&mut secret);
        format!("{session}.{}", URL_SAFE_NO_PAD.encode(secret))
    }
}
fn refresh_hash(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

use super::OperatorIdentity;
use crate::api::ApiError;
use axum::extract::Extension;
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct LinkIdentityRequest {
    issuer: String,
    subject: String,
}

/// The identity mapping as a plain resource; `201` versus `200` tells the
/// operator whether this call created the link.
#[derive(Serialize, ToSchema)]
pub(crate) struct LinkIdentityResponse {
    user_id: Uuid,
    issuer: String,
    subject: String,
}

/// Link a provider issuer/subject to a user using operator authority. Returns 201 for a new link or 200 for an existing link.
#[utoipa::path(post, path = "/v1/users/{id}/identities", params(("id" = Uuid, Path)), request_body = LinkIdentityRequest, responses((status = 200, body = LinkIdentityResponse), (status = 201, body = LinkIdentityResponse)), security(("OperatorBearer" = [])), tag = "Operators")]
pub(crate) async fn link_identity(
    State(state): State<ApiState>,
    Extension(operator): Extension<OperatorIdentity>,
    Path(user_id): Path<Uuid>,
    Json(input): Json<LinkIdentityRequest>,
) -> Result<(StatusCode, Json<LinkIdentityResponse>), ApiError> {
    let created = state
        .repo
        .link_core_user_identity(user_id, &input.issuer, &input.subject)
        .await?;
    tracing::info!(
        actor_id = %operator.actor_id,
        %user_id,
        identity_issuer = %input.issuer,
        identity_subject = %input.subject,
        created,
        "operator linked external identity"
    );
    Ok((
        if created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(LinkIdentityResponse {
            user_id,
            issuer: input.issuer,
            subject: input.subject,
        }),
    ))
}

#[derive(Serialize, ToSchema)]
struct RevokedSession {
    id: Uuid,
    revoked: bool,
}

// The runtime challenge response is chosen by provider.
#[allow(dead_code)]
#[derive(Serialize, ToSchema)]
#[serde(untagged)]
enum LoginChallenge {
    Oidc(LoginStarted),
    Solana(solana::ChallengeResponse),
    Evm(evm::ChallengeResponse),
}

use std::sync::Arc;

/// Validity of a minted capability token, matching the one-hour access
/// tokens of the SQL `generate_access_token` mint.
const CAPABILITY_TOKEN_VALIDITY_SECONDS: u64 = 3600;

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
#[serde(tag = "grant_type")]
pub enum TokenRequest {
    #[serde(rename = "refresh_token")]
    RefreshToken {
        refresh_token_id: models::Id,
        secret: String,
    },
    /// Mint an access token whose authority is limited to a selected set of
    /// capabilities. The caller authenticates with a normal full-authority
    /// bearer token; the minted token identifies the same user and carries
    /// `capability_mask` as a claim, which authorization intersects with the
    /// user's live grants at use time.
    #[serde(rename = "capability_token")]
    CapabilityToken {
        /// Capability-bundle names to mask the minted token's authority to.
        /// Names are opaque here: unrecognized names are carried through and
        /// are inert at use, and an empty list is valid — it mints an
        /// identity-only token.
        capability_mask: Vec<String>,
    },
}

#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct TokenResponse {
    pub access_token: String,
    // `generate_access_token` omits this for multi-use tokens (no rotation),
    // so it must default to `None` when absent from the SQL JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<RefreshTokenResponse>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct RefreshTokenResponse {
    pub id: models::Id,
    pub secret: String,
}

pub async fn handle_post_token(
    axum::extract::State(app): axum::extract::State<Arc<crate::App>>,
    crate::Authority { envelope, .. }: crate::Authority,
    axum::Json(req): axum::Json<TokenRequest>,
) -> Result<axum::Json<TokenResponse>, crate::ApiError> {
    match req {
        // The refresh_token grant is unauthenticated: the credential is the
        // (id, secret) pair in the body, and `envelope` goes unused. A request
        // which nonetheless presents an Authorization bearer has it verified
        // by extraction above, so a broken bearer is rejected regardless of
        // grant.
        TokenRequest::RefreshToken {
            refresh_token_id,
            secret,
        } => {
            let response = generate_access_token(&app.pg_pool, refresh_token_id, &secret).await?;
            Ok(axum::Json(response))
        }
        TokenRequest::CapabilityToken { capability_mask } => {
            let response = mint_capability_token(
                &envelope,
                capability_mask,
                &app.control_plane_jwt_encode_key,
            )
            .await?;
            Ok(axum::Json(response))
        }
    }
}

/// Mint a capability-masked access token for the authenticated caller.
///
/// The `capability_token` grant requires an unmasked, human caller:
/// - A missing or invalid bearer is `401 Unauthorized`.
/// - A masked bearer is `403 unmasked_token_required`: the mint is brokered
///   by a holder of the user's full-authority token, and a reduced token
///   must not be able to widen or re-mint itself by calling this exchange.
/// - A service-account bearer is `403 service_account_forbidden`: masked
///   tokens are a human-delegation mechanism, and service accounts hold
///   admin-issued API keys instead. The lookup cost lives here at the mint
///   only — enforcement needs no such check, because a mask only ever
///   narrows authority.
async fn mint_capability_token(
    envelope: &crate::Envelope,
    capability_mask: Vec<String>,
    encoding_key: &tokens::jwt::EncodingKey,
) -> Result<TokenResponse, crate::ApiError> {
    let claims = envelope.claims()?;

    if claims.capability_mask.is_some() {
        return Err(crate::ApiError::Forbidden(
            crate::Forbidden::unmasked_token_required(),
        ));
    }
    if super::graphql::is_service_account(&envelope.pg_pool, claims.sub).await? {
        return Err(crate::ApiError::Forbidden(
            crate::Forbidden::service_account_forbidden(),
        ));
    }

    let access_token = sign_capability_token(claims, capability_mask, encoding_key)?;
    Ok(TokenResponse {
        access_token,
        refresh_token: None,
    })
}

/// Sign a one-hour access token which copies the caller's verified identity
/// claims and carries `capability_mask` verbatim.
///
/// The identity claims (`sub`, `role`, `aud`, and `email` when the caller's
/// token has it) are a pure copy-through, so the minted token is a fully
/// functional access token everywhere its bearer's identity is what
/// matters — including Supabase/PostgREST, which reads `sub` and `role` and
/// ignores claims it doesn't know. Aside from `email` and `capability_mask`,
/// this is the claim set of the SQL `generate_access_token` mint, whose
/// shape is pinned by the pgTAP tests in
/// `supabase/tests/scoped_ci_tokens.test.sql`; the two mint paths must not
/// drift.
///
/// The mask is deliberately not validated: enforcement recognizes the
/// bundle names it knows and ignores the rest, which is what makes unknown
/// names inert and mixed-version fleets safe. See
/// `models::authorizations::ControlClaims::capability_mask`.
fn sign_capability_token(
    caller: &models::authorizations::ControlClaims,
    capability_mask: Vec<String>,
    encoding_key: &tokens::jwt::EncodingKey,
) -> tonic::Result<String> {
    let iat = tokens::now().timestamp() as u64;

    let claims = models::authorizations::ControlClaims {
        aud: caller.aud.clone(),
        iat,
        exp: iat + CAPABILITY_TOKEN_VALIDITY_SECONDS,
        sub: caller.sub,
        role: caller.role.clone(),
        email: caller.email.clone(),
        capability_mask: Some(capability_mask),
    };

    tokens::jwt::sign(&claims, encoding_key)
}

// Exchange a refresh token for an access token by calling the SQL
// `generate_access_token` function and returning its parsed response.
//
// Shared by the `POST /api/v1/auth/token` endpoint (above) and the
// bearer-credential authentication path
// (`crate::server::exchange_refresh_token`), so the credential-error
// sanitization below lives in exactly one place rather than being duplicated —
// and kept in sync — across both.
//
// The SQL delegation is transitional: existing clients (flowctl via
// flow-client) still authenticate against the PostgREST
// `/rpc/generate_access_token` surface, so the function must keep working
// unchanged. The plan is to migrate those callers onto this endpoint and then
// retire the SQL function, folding refresh-token minting into an
// application-layer path. New clients should target this endpoint rather than
// PostgREST.
pub(crate) async fn generate_access_token(
    pg_pool: &sqlx::PgPool,
    refresh_token_id: models::Id,
    secret: &str,
) -> tonic::Result<TokenResponse> {
    let response = sqlx::query!(
        "select generate_access_token($1, $2) as token",
        refresh_token_id as models::Id,
        secret,
    )
    .fetch_one(pg_pool)
    .await
    .map_err(|err| {
        // `generate_access_token` signals an unusable credential (unknown id,
        // bad secret, or expired/revoked token) by `raise`-ing, which surfaces
        // as SQLSTATE P0001. Those are the only legitimate 401s, and we collapse
        // them into a single generic message so the response neither reveals
        // which check failed nor leaks the raw DB error. Any other error is an
        // internal fault: log the detail and return 500.
        if err.as_database_error().and_then(|e| e.code()).as_deref() == Some("P0001") {
            tonic::Status::unauthenticated("invalid, expired, or unknown credential")
        } else {
            tracing::error!(?err, "failed to exchange refresh token");
            tonic::Status::internal("failed to exchange refresh token")
        }
    })?;

    serde_json::from_value(response.token.unwrap_or_default()).map_err(|err| {
        tracing::error!(
            ?err,
            "generate_access_token returned an unparseable response"
        );
        tonic::Status::internal("invalid token response")
    })
}

#[cfg(test)]
mod test {
    use crate::test_server;

    const ALICE: uuid::Uuid = uuid::Uuid::from_bytes([0x11; 16]);

    async fn post_token(
        server: &test_server::TestServer,
        body: &serde_json::Value,
        bearer: Option<&str>,
    ) -> (reqwest::StatusCode, String) {
        let response = server
            .rest_client()
            .post("/api/v1/auth/token", body, bearer)
            .send()
            .await
            .unwrap();
        (response.status(), response.text().await.unwrap())
    }

    fn capability_request(mask: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "grant_type": "capability_token",
            "capability_mask": mask,
        })
    }

    /// Decode a minted token's claims without signature verification. The
    /// signature itself is proven by presenting the token back to the server
    /// as a bearer, which routes it through real Envelope verification.
    fn claims_of(body: &str) -> (models::authorizations::ControlClaims, String) {
        let body: serde_json::Value = serde_json::from_str(body).unwrap();
        let access_token = body["access_token"].as_str().unwrap().to_string();

        // The capability_token grant mints no refresh credential, so the
        // response carries exactly the access_token.
        assert_eq!(
            body.as_object().unwrap().keys().collect::<Vec<_>>(),
            vec!["access_token"],
        );

        let unverified = tokens::jwt::parse_unverified::<models::authorizations::ControlClaims>(
            access_token.as_bytes(),
        )
        .unwrap();
        (unverified.claims().clone(), access_token)
    }

    /// Covers the capability_token grant end-to-end: the copy-through claim
    /// set of a successful mint, verbatim mask stamping, the identity-only
    /// empty mask, and every refusal — missing bearer, masked caller,
    /// service-account caller — plus the bearer handling of the endpoint as
    /// a whole (an invalid bearer is rejected regardless of grant) and the
    /// typed extractor's rejection of an unknown grant.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_capability_token_mint(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;
        let alice_token = server.make_access_token(ALICE, Some("alice@example.com"));

        // === A full-authority caller mints a masked token ===
        // The mask is stamped verbatim: unrecognized names and duplicates
        // carry through, because enforcement (not the mint) decides what a
        // name enables.
        let (status, body) = post_token(
            &server,
            &capability_request(serde_json::json!([
                "CatalogRead",
                "Viewer",
                "NotARealBundle",
                "CatalogRead"
            ])),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, reqwest::StatusCode::OK, "mint failed: {body}");
        let (claims, minted_token) = claims_of(&body);

        // Identity claims are a pure copy-through of the caller's, and the
        // fresh validity window matches the SQL mint's one hour.
        assert_eq!(claims.sub, ALICE);
        assert_eq!(claims.email.as_deref(), Some("alice@example.com"));
        assert_eq!(claims.role, "authenticated");
        assert_eq!(claims.aud, "authenticated");
        assert_eq!(claims.exp, claims.iat + 3600);
        let now = tokens::now().timestamp() as u64;
        assert!(now - claims.iat < 60, "iat {} is fresh", claims.iat);
        assert_eq!(
            claims.capability_mask,
            Some(vec![
                "CatalogRead".to_string(),
                "Viewer".to_string(),
                "NotARealBundle".to_string(),
                "CatalogRead".to_string(),
            ]),
        );

        // === A masked caller cannot mint (and so cannot widen itself) ===
        // Presenting the minted token as the bearer also proves its
        // signature: the refusal below requires Envelope verification of the
        // token to have succeeded.
        let (status, body) = post_token(
            &server,
            &capability_request(serde_json::json!(["Admin"])),
            Some(&minted_token),
        )
        .await;
        insta::assert_snapshot!(
            format!("{status}: {body}"),
            @r###"403 Forbidden: {"error":"unmasked_token_required","message":"this operation requires a full-authority token, but the bearer token carries a capability mask","missing_capabilities":[]}"###
        );

        // === An empty mask mints a valid identity-only token ===
        let (status, body) = post_token(
            &server,
            &capability_request(serde_json::json!([])),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, reqwest::StatusCode::OK, "empty-mask mint: {body}");
        let (claims, identity_token) = claims_of(&body);
        assert_eq!(claims.capability_mask, Some(vec![]));

        // "Masked" is the claim's presence, never its value: the
        // identity-only token is refused as a mint caller like any other
        // masked bearer.
        let (status, _body) = post_token(
            &server,
            &capability_request(serde_json::json!([])),
            Some(&identity_token),
        )
        .await;
        assert_eq!(status, reqwest::StatusCode::FORBIDDEN);

        // === A caller without an email claim mints a token without one ===
        let no_email_token = server.make_access_token(ALICE, None);
        let (status, body) = post_token(
            &server,
            &capability_request(serde_json::json!(["CatalogRead"])),
            Some(&no_email_token),
        )
        .await;
        assert_eq!(status, reqwest::StatusCode::OK, "email-less mint: {body}");
        let (claims, _token) = claims_of(&body);
        assert_eq!(claims.email, None);

        // === The grant requires an authenticated caller ===
        let (status, body) = post_token(
            &server,
            &capability_request(serde_json::json!(["CatalogRead"])),
            None,
        )
        .await;
        insta::assert_snapshot!(
            format!("{status}: {body}"),
            @"401 Unauthorized: This is an authenticated API but the request is missing a required Authorization: Bearer token"
        );

        // === A service-account caller is refused ===
        let svc_user = uuid::Uuid::from_bytes([0x77; 16]);
        sqlx::query("INSERT INTO auth.users (id, email) VALUES ($1, 'svc@example.com')")
            .bind(svc_user)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO internal.service_accounts (user_id, catalog_name, created_by) \
             VALUES ($1, 'aliceCo/service-account', $2)",
        )
        .bind(svc_user)
        .bind(ALICE)
        .execute(&pool)
        .await
        .unwrap();

        let svc_token = server.make_access_token(svc_user, Some("svc@example.com"));
        let (status, body) = post_token(
            &server,
            &capability_request(serde_json::json!(["CatalogRead"])),
            Some(&svc_token),
        )
        .await;
        insta::assert_snapshot!(
            format!("{status}: {body}"),
            @r###"403 Forbidden: {"error":"service_account_forbidden","message":"this operation is restricted to human users, but the bearer token belongs to a service account","missing_capabilities":[]}"###
        );

        // === An invalid bearer is rejected regardless of grant ===
        // The endpoint verifies any Authorization header it is given, so a
        // broken bearer fails even the otherwise-unauthenticated
        // refresh_token grant. (Bearer-less refresh_token requests are
        // covered by the refresh-token management test.)
        let (status, _body) = post_token(
            &server,
            &serde_json::json!({
                "grant_type": "refresh_token",
                "refresh_token_id": "00:00:00:00:00:00:00:00",
                "secret": "irrelevant",
            }),
            Some("not-a-valid.jwt.token"),
        )
        .await;
        assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);

        // === An unknown grant is rejected by the typed extractor ===
        let (status, _body) = post_token(
            &server,
            &serde_json::json!({ "grant_type": "not_a_grant" }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    }
}

use axum_extra::{
    TypedHeader,
    headers::{Authorization, authorization::Bearer},
};
use itertools::Itertools;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
#[serde(tag = "grant_type", deny_unknown_fields)]
pub enum TokenRequest {
    #[serde(rename = "capability_token")]
    CapabilityToken { capability_mask: Vec<String> },
    #[serde(rename = "refresh_token")]
    RefreshToken {
        refresh_token_id: models::Id,
        secret: String,
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
    authorization_header: Result<
        TypedHeader<Authorization<Bearer>>,
        axum_extra::typed_header::TypedHeaderRejection,
    >,
    axum::Json(req): axum::Json<TokenRequest>,
) -> Result<axum::Json<TokenResponse>, crate::ApiError> {
    match req {
        TokenRequest::RefreshToken {
            refresh_token_id,
            secret,
        } => {
            let response = generate_access_token(&app.pg_pool, refresh_token_id, &secret).await?;
            Ok(axum::Json(response))
        }
        TokenRequest::CapabilityToken { capability_mask } => {
            let response =
                mint_capability_token(authorization_header, capability_mask, &app).await?;
            Ok(axum::Json(response))
        }
    }
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

/// Upper bound on a minted capability token's lifetime. The actual lifetime
/// is the lesser of this and the minting bearer's remaining lifetime.
pub const CAPABILITY_TOKEN_DURATION: std::time::Duration = std::time::Duration::from_secs(3600);

/// Mint a capability-masked access token for the authenticated caller.
///
/// This creates a capability masked token, that has a mask limiting what the
/// minted token is allowed to do.
async fn mint_capability_token(
    bearer_token_header: Result<
        TypedHeader<Authorization<Bearer>>,
        axum_extra::typed_header::TypedHeaderRejection,
    >,
    capability_mask: Vec<String>,
    app: &Arc<crate::App>,
) -> Result<TokenResponse, crate::ApiError> {
    let maybe_claims = match bearer_token_header {
        Ok(bearer_header) => {
            crate::envelope::parse_authorization_header(bearer_header, app).await?
        }
        Err(err) => {
            match err.reason() {
                axum_extra::typed_header::TypedHeaderRejectionReason::Missing => {
                    crate::MaybeControlClaims::with_unauthenticated()
                }
                axum_extra::typed_header::TypedHeaderRejectionReason::Error(error) => {
                    return Err(crate::ApiError::Status(tonic::Status::invalid_argument(
                        error.to_string(),
                    )));
                }
                &_ => {
                    // NOTE(BB): This exists because the reason is marked non-exhaustive.
                    return Err(crate::ApiError::Status(tonic::Status::internal(
                        "An unknown authentication error occurred, unable to read header",
                    )));
                }
            }
        }
    };
    let claims = maybe_claims.result()?;
    if claims.role != "authenticated" {
        return Err(crate::ApiError::Status(tonic::Status::permission_denied(
            "Unable to mint a capability masked token without the role of authenticated",
        )));
    }
    if claims.capability_mask.is_some() {
        return Err(crate::ApiError::Status(tonic::Status::permission_denied(
            "Unable to mint a new token from a token with a capability mask",
        )));
    }
    if crate::server::public::graphql::service_accounts::is_not_service_account(
        &app.pg_pool,
        claims.sub,
    )
    .await?
    {
        return Err(crate::ApiError::Status(tonic::Status::permission_denied(
            "Service account tokens cannot be used to mint capability masked tokens",
        )));
    }

    let invalid_capabilities = capability_mask
        .iter()
        .filter(|mask| {
            models::authz::CapabilityBundle::deserialize(serde::de::value::StrDeserializer::<
                serde::de::value::Error,
            >::new(mask))
            .is_err()
        })
        .collect::<Vec<&String>>();
    if !invalid_capabilities.is_empty() {
        return Err(crate::ApiError::Status(tonic::Status::invalid_argument(
            format!(
                "Invalid capability requested: {}",
                invalid_capabilities.iter().join(", ")
            ),
        )));
    }

    let iat = tokens::now().timestamp() as u64;
    // A minted token must never outlive the bearer that minted it. Otherwise
    // re-minting would extend a leaked or expiring credential indefinitely,
    // and revoking a session would leave live masked tokens behind.
    let exp = (iat + CAPABILITY_TOKEN_DURATION.as_secs()).min(claims.exp);

    let claims = models::authorizations::ControlClaims {
        aud: claims.aud.clone(),
        iat,
        exp,
        sub: claims.sub,
        role: claims.role.clone(),
        email: claims.email.clone(),
        capability_mask: Some(capability_mask),
    };

    let access_token = tokens::jwt::sign(&claims, &app.control_plane_jwt_encode_key)?;
    tracing::info!(
        %claims.sub,
        capability_mask = ?claims.capability_mask,
        exp = claims.exp,
        "minted capability-masked token"
    );

    Ok(TokenResponse {
        access_token,
        refresh_token: None,
    })
}

#[cfg(test)]
mod test {
    use crate::test_server;

    /// Covers the `capability_token` grant of `POST /api/v1/auth/token`: the
    /// request-shape rejections (unknown bundle names, no bearer, a
    /// service-account caller) and, for accepted masks, that the minted JWT
    /// verifies under the server's keys and carries the caller's identity
    /// claims plus the requested mask verbatim.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_mint_capability_token(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;

        let alice = uuid::Uuid::from_bytes([0x11; 16]);
        // The bearer outlives CAPABILITY_TOKEN_DURATION so that the minted
        // lifetime below is set by the constant, not by the bearer's expiry.
        let alice_token = server.make_access_token_with(
            alice,
            Some("alice@example.test"),
            None,
            chrono::Duration::hours(2),
        );

        // === Unknown and wrong-case bundle names are rejected ===
        // Every offender is listed, and a valid name alongside them does not
        // rescue the request.
        let rejected = server
            .rest_client()
            .post(
                "/api/v1/auth/token",
                &serde_json::json!({
                    "grant_type": "capability_token",
                    "capability_mask": ["bogus", "Admin", "viewer"],
                }),
                Some(&alice_token),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(rejected.status(), reqwest::StatusCode::BAD_REQUEST);
        insta::assert_snapshot!(
            rejected.text().await.unwrap(),
            @"Invalid capability requested: bogus, Admin"
        );

        // === A valid mask mints a token carrying it verbatim ===
        // Order is preserved and nothing is deduplicated or normalized: the
        // claim is exactly what was requested.
        let minted = server
            .rest_client()
            .post(
                "/api/v1/auth/token",
                &serde_json::json!({
                    "grant_type": "capability_token",
                    "capability_mask": ["viewer", "admin"],
                }),
                Some(&alice_token),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(minted.status(), reqwest::StatusCode::OK);
        let body: serde_json::Value = minted.json().await.unwrap();
        assert!(
            body.get("refresh_token").is_none(),
            "a capability token must not come with a refresh token: {body}"
        );

        let claims = server.verify_access_token(body["access_token"].as_str().unwrap());
        assert_eq!(
            claims.exp - claims.iat,
            super::CAPABILITY_TOKEN_DURATION.as_secs(),
            "the minted token expires after CAPABILITY_TOKEN_DURATION"
        );
        insta::assert_json_snapshot!(claims, {
            ".iat" => "[iat]",
            ".exp" => "[exp]",
        }, @r#"
        {
          "aud": "authenticated",
          "iat": "[iat]",
          "exp": "[exp]",
          "sub": "11111111-1111-1111-1111-111111111111",
          "role": "authenticated",
          "email": "alice@example.test",
          "capability_mask": [
            "viewer",
            "admin"
          ]
        }
        "#);

        // === An empty mask mints an identity-only token ===
        // The claim must be present-but-empty, not absent: `None` would be an
        // unmasked token with the caller's full authority.
        let minted = server
            .rest_client()
            .post(
                "/api/v1/auth/token",
                &serde_json::json!({
                    "grant_type": "capability_token",
                    "capability_mask": [],
                }),
                Some(&alice_token),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(minted.status(), reqwest::StatusCode::OK);
        let body: serde_json::Value = minted.json().await.unwrap();

        let claims = server.verify_access_token(body["access_token"].as_str().unwrap());
        assert_eq!(
            claims.exp - claims.iat,
            super::CAPABILITY_TOKEN_DURATION.as_secs()
        );
        insta::assert_json_snapshot!(claims, {
            ".iat" => "[iat]",
            ".exp" => "[exp]",
        }, @r#"
        {
          "aud": "authenticated",
          "iat": "[iat]",
          "exp": "[exp]",
          "sub": "11111111-1111-1111-1111-111111111111",
          "role": "authenticated",
          "email": "alice@example.test",
          "capability_mask": []
        }
        "#);

        // === The minted lifetime is capped by the bearer's remaining lifetime ===
        // A masked token must never outlive the credential that minted it:
        // otherwise a leaked bearer's expiry could be extended indefinitely by
        // re-minting, and an expiring session would leave live tokens behind.
        let short_lived_token = server.make_access_token_with(
            alice,
            Some("alice@example.test"),
            None,
            chrono::Duration::minutes(10),
        );
        let bearer_exp = server.verify_access_token(&short_lived_token).exp;

        let minted = server
            .rest_client()
            .post(
                "/api/v1/auth/token",
                &serde_json::json!({
                    "grant_type": "capability_token",
                    "capability_mask": ["viewer"],
                }),
                Some(&short_lived_token),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(minted.status(), reqwest::StatusCode::OK);
        let body: serde_json::Value = minted.json().await.unwrap();

        let claims = server.verify_access_token(body["access_token"].as_str().unwrap());
        assert_eq!(
            claims.exp, bearer_exp,
            "a minted token expires no later than the bearer that minted it"
        );
        assert!(
            claims.exp - claims.iat < super::CAPABILITY_TOKEN_DURATION.as_secs(),
            "the bearer's expiry, not CAPABILITY_TOKEN_DURATION, bounds this token"
        );

        // === The grant requires an authenticated caller ===
        // The route itself accepts anonymous requests (the refresh_token grant
        // needs no bearer), so the 401 comes from this grant's use of the
        // claims rather than from the extractor.
        let anonymous = server
            .rest_client()
            .post(
                "/api/v1/auth/token",
                &serde_json::json!({
                    "grant_type": "capability_token",
                    "capability_mask": ["viewer"],
                }),
                None,
            )
            .send()
            .await
            .unwrap();
        assert_eq!(anonymous.status(), reqwest::StatusCode::UNAUTHORIZED);
        insta::assert_snapshot!(
            anonymous.text().await.unwrap(),
            @"This is an authenticated API but the request is missing a required Authorization: Bearer token"
        );

        // === Only the `authenticated` Postgres role may mint ===
        // The `role` claim is what PostgREST assumes with SET ROLE. Scoped CI
        // and dekaf bearers carry other roles, and a masked token copies the
        // bearer's role verbatim, so refusing here keeps those roles from
        // being laundered into a fresh, longer-lived credential.
        let dekaf_claims = {
            let now = tokens::now();
            models::authorizations::ControlClaims {
                iat: now.timestamp() as u64,
                exp: (now + chrono::Duration::hours(1)).timestamp() as u64,
                sub: alice,
                role: "dekaf".to_string(),
                aud: "authenticated".to_string(),
                email: None,
                capability_mask: None,
            }
        };
        let dekaf_token = server.sign_claims(&dekaf_claims);

        let refused = server
            .rest_client()
            .post(
                "/api/v1/auth/token",
                &serde_json::json!({
                    "grant_type": "capability_token",
                    "capability_mask": ["viewer"],
                }),
                Some(&dekaf_token),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(refused.status(), reqwest::StatusCode::FORBIDDEN);
        insta::assert_snapshot!(
            refused.text().await.unwrap(),
            @"Unable to mint a capability masked token without the role of authenticated"
        );

        // === Service accounts cannot mint ===
        // Create an account as alice, then fabricate the access token its API
        // key would exchange into (equivalent to a bearer exchange, without
        // the pgjwt signing path the sqlx::test DB lacks).
        let created: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        createServiceAccount(catalogName: "aliceCo/mint-bot", grants: []) {
                            catalogName
                        }
                    }"#
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            created["errors"].is_null(),
            "creating the service account should succeed: {created}"
        );
        let sa_user_id: uuid::Uuid = sqlx::query_scalar!(
            r#"SELECT user_id FROM internal.service_accounts WHERE catalog_name = 'aliceCo/mint-bot'::catalog_name"#
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let sa_token = server.make_access_token(sa_user_id, None);

        let refused = server
            .rest_client()
            .post(
                "/api/v1/auth/token",
                &serde_json::json!({
                    "grant_type": "capability_token",
                    "capability_mask": ["viewer"],
                }),
                Some(&sa_token),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(refused.status(), reqwest::StatusCode::FORBIDDEN);
        insta::assert_snapshot!(
            refused.text().await.unwrap(),
            @"Service account tokens cannot be used to mint capability masked tokens"
        );
    }

    /// A token minted by the endpoint is honored by GraphQL authorization:
    /// alice is admin of `aliceCo/`, and `updateAlertConfig` on that prefix
    /// requires admin, so the outcome depends solely on the mask. A minted
    /// masked token also cannot mint again, so a holder cannot widen its mask.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_minted_token_enforces_mask(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;

        let alice = uuid::Uuid::from_bytes([0x11; 16]);
        let alice_token = server.make_access_token(alice, Some("alice@example.test"));

        // The admin mask runs first and creates the config row; the two
        // denials that follow prove the mask attenuates a user who genuinely
        // holds admin.
        for (name, mask) in [
            ("admin", serde_json::json!(["admin"])),
            ("viewer", serde_json::json!(["viewer"])),
            ("empty", serde_json::json!([])),
        ] {
            let minted = server
                .rest_client()
                .post(
                    "/api/v1/auth/token",
                    &serde_json::json!({
                        "grant_type": "capability_token",
                        "capability_mask": mask,
                    }),
                    Some(&alice_token),
                )
                .send()
                .await
                .unwrap();
            assert_eq!(
                minted.status(),
                reqwest::StatusCode::OK,
                "minting the {name} mask should succeed"
            );
            let body: serde_json::Value = minted.json().await.unwrap();
            let masked_token = body["access_token"].as_str().unwrap();

            let response: serde_json::Value = server
                .graphql(
                    &serde_json::json!({
                        "query": r#"
                        mutation {
                            updateAlertConfig(
                                catalogPrefixOrName: "aliceCo/"
                                config: {}
                            ) {
                                catalogPrefixOrName
                                created
                            }
                        }"#
                    }),
                    Some(masked_token),
                )
                .await;
            insta::assert_json_snapshot!(format!("enforces_mask_{name}"), response);
        }

        // === A masked token cannot mint again ===
        let minted = server
            .rest_client()
            .post(
                "/api/v1/auth/token",
                &serde_json::json!({
                    "grant_type": "capability_token",
                    "capability_mask": ["admin"],
                }),
                Some(&alice_token),
            )
            .send()
            .await
            .unwrap();
        let body: serde_json::Value = minted.json().await.unwrap();
        let masked_token = body["access_token"].as_str().unwrap();

        let widened = server
            .rest_client()
            .post(
                "/api/v1/auth/token",
                &serde_json::json!({
                    "grant_type": "capability_token",
                    "capability_mask": ["admin", "viewer"],
                }),
                Some(masked_token),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(widened.status(), reqwest::StatusCode::FORBIDDEN);
        insta::assert_snapshot!(
            widened.text().await.unwrap(),
            @"Unable to mint a new token from a token with a capability mask"
        );
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../../fixtures",
            scripts("sso_tenant", "data_planes", "bob_co", "bob_co2")
        )
    )]
    async fn test_unheld_mask_bundles_are_inert(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;

        let bob = uuid::Uuid::from_bytes([0x22; 16]);
        let carol = uuid::Uuid::from_bytes([0x33; 16]);

        // Bob, a genuine admin of `bobCo2/`, seeds a config row so that a
        // Viewer read below has something to return.
        let bob_token = server.make_access_token(bob, Some("bob@example.test"));
        let seeded: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        updateAlertConfig(catalogPrefixOrName: "bobCo2/", config: {}) {
                            catalogPrefixOrName
                        }
                    }"#
                }),
                Some(&bob_token),
            )
            .await;
        assert!(
            seeded["errors"].is_null(),
            "bob should be able to seed the config: {seeded}"
        );

        // === Minting accepts a bundle the caller does not hold ===
        let carol_token = server.make_access_token(carol, Some("carol@example.test"));
        let minted = server
            .rest_client()
            .post(
                "/api/v1/auth/token",
                &serde_json::json!({
                    "grant_type": "capability_token",
                    "capability_mask": ["admin"],
                }),
                Some(&carol_token),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(
            minted.status(),
            reqwest::StatusCode::OK,
            "a mask wider than the caller's grants is still minted"
        );
        let body: serde_json::Value = minted.json().await.unwrap();
        let masked_token = body["access_token"].as_str().unwrap();

        // === The unheld admin bit confers nothing ===
        let denied: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        updateAlertConfig(catalogPrefixOrName: "bobCo2/", config: {}) {
                            catalogPrefixOrName
                        }
                    }"#
                }),
                Some(masked_token),
            )
            .await;
        assert!(denied["data"].is_null(), "mutation must not run: {denied}");
        assert_eq!(
            denied["errors"][0]["message"],
            "PermissionDenied: carol@example.test is not authorized to access prefix or name 'bobCo2/' with required capability admin",
            "the mask cannot grant admin carol lacks: {denied}"
        );

        // === The Viewer bits carol does hold keep working ===
        // `admin` includes Viewer, and carol's grant is Viewer, so the
        // intersection is exactly her real authority.
        let read: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        alertConfigs(filter: { catalogPrefixOrName: { startsWith: "bobCo2/" } }) {
                            edges { node { catalogPrefixOrName } }
                        }
                    }"#
                }),
                Some(masked_token),
            )
            .await;
        assert!(read["errors"].is_null(), "the read must succeed: {read}");
        assert_eq!(
            read["data"]["alertConfigs"]["edges"],
            serde_json::json!([{ "node": { "catalogPrefixOrName": "bobCo2/" } }]),
            "carol still reads what Viewer allows: {read}"
        );
    }
}

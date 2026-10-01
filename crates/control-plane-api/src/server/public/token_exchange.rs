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
    // Only the capability_token grant authenticates via the Authorization
    // header. The refresh_token grant carries its credential in the body and
    // must neither act on nor fail because of a header, so the Envelope is
    // extracted from these (cloned) parts only once the body selects that grant.
    mut parts: axum::http::request::Parts,
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
            use axum::extract::FromRequestParts;
            let env = crate::Envelope::from_request_parts(&mut parts, &app).await?;
            let response = mint_capability_token(env.claims()?, capability_mask, &app).await?;
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

/// Lifetime of a token minted with a `capability_mask`. It is deliberately not
/// capped at the bearer's expiry: a masked token cannot mint again, and an
/// unmasked bearer can already obtain longer-lived credentials through
/// `createRefreshToken`, so a cap would bound nothing.
const CAPABILITY_TOKEN_DURATION: std::time::Duration = std::time::Duration::from_secs(3600);

/// Mint a capability-masked access token for the authenticated caller.
///
/// Note that the requested bundles aren't checked against the caller's grants: a mask
/// can only narrow what the user's grants allow, enforced when the token is used.
async fn mint_capability_token(
    claims: &crate::ControlClaims,
    capability_mask: Vec<String>,
    app: &Arc<crate::App>,
) -> Result<TokenResponse, crate::ApiError> {
    // The requested mask replaces the bearer's, so a masked bearer could
    // otherwise widen its own mask.
    if claims.capability_mask.is_some() {
        return Err(crate::ApiError::Status(tonic::Status::permission_denied(
            "Unable to mint a new token from a token with a capability mask",
        )));
    }
    // Only user sessions mint. Machine bearers (dekaf, scoped CI roles) also
    // carry `aud: authenticated`, but they don't act for a user with grants.
    if claims.role != "authenticated" {
        return Err(crate::ApiError::Status(tonic::Status::permission_denied(
            "Capability tokens can only be minted from a user session",
        )));
    }

    if crate::server::is_service_account(&app.pg_pool, claims.sub).await? {
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
    let exp = iat + CAPABILITY_TOKEN_DURATION.as_secs();

    let claims = models::authorizations::ControlClaims {
        aud: claims.aud.clone(),
        iat,
        exp,
        sub: claims.sub,
        // PostgREST uses this claim for `SET ROLE`. No Postgres role has this
        // name, so PostgREST refuses the token instead of serving it with the
        // user's unmasked RLS grants. Never create a role with this name.
        role: "postgrest_cant_use_this_token".to_string(),
        email: claims.email.clone(),
        capability_mask: Some(capability_mask),
        // A prefix-scoped bearer mints a token that keeps its scope: the mask
        // only ever narrows, so the result must not widen the bearer either.
        prefix_scope: claims.prefix_scope.clone(),
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
    /// rejections (unknown bundle names, unknown request fields, no bearer, a
    /// non-user `role`, a service-account caller) and, for accepted masks, that
    /// the minted JWT verifies under the server's keys, carries the caller's
    /// identity claims and `prefix_scope` plus the requested mask verbatim,
    /// and has a fixed one-hour lifetime that is independent of the bearer's.
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
        let alice_token = server.make_access_token(alice, Some("alice@example.test"));

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
          "role": "postgrest_cant_use_this_token",
          "email": "alice@example.test",
          "capability_mask": [
            "viewer",
            "admin"
          ]
        }
        "#);

        // === Unknown request fields are rejected ===
        // A field this server doesn't know (say, a future `prefixes`) must fail
        // the request rather than mint a token that silently ignores it.
        let rejected = server
            .rest_client()
            .post(
                "/api/v1/auth/token",
                &serde_json::json!({
                    "grant_type": "capability_token",
                    "capability_mask": ["viewer"],
                    "prefixes": ["aliceCo/"],
                }),
                Some(&alice_token),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(rejected.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
        insta::assert_snapshot!(
            rejected.text().await.unwrap(),
            @"Failed to deserialize the JSON body into the target type: unknown field `prefixes`, expected `capability_mask`"
        );

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
        insta::assert_json_snapshot!(claims, {
            ".iat" => "[iat]",
            ".exp" => "[exp]",
        }, @r#"
        {
          "aud": "authenticated",
          "iat": "[iat]",
          "exp": "[exp]",
          "sub": "11111111-1111-1111-1111-111111111111",
          "role": "postgrest_cant_use_this_token",
          "email": "alice@example.test",
          "capability_mask": []
        }
        "#);

        // === A prefix-scoped bearer's scope carries into the minted token ===
        // Dropping it would widen the bearer. The scope is copied as-is; the
        // trailing-slash normalization happens when the claims become a Subject.
        let scoped_bearer = server.make_restricted_access_token(
            alice,
            Some("alice@example.test"),
            None,
            Some("aliceCo/".to_string()),
        );
        let minted = server
            .rest_client()
            .post(
                "/api/v1/auth/token",
                &serde_json::json!({
                    "grant_type": "capability_token",
                    "capability_mask": ["viewer"],
                }),
                Some(&scoped_bearer),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(minted.status(), reqwest::StatusCode::OK);
        let body: serde_json::Value = minted.json().await.unwrap();
        let claims = server.verify_access_token(body["access_token"].as_str().unwrap());
        assert_eq!(
            claims.prefix_scope.as_deref(),
            Some("aliceCo/"),
            "the bearer's prefix_scope must survive the mint"
        );
        assert_eq!(claims.capability_mask, Some(vec!["viewer".to_string()]));

        // === The lifetime is fixed, not capped by the bearer's ===
        // A bearer with ten minutes left still mints a full-hour token. A cap
        // would bound nothing, since an unmasked bearer can already obtain
        // long-lived credentials via createRefreshToken, and a predictable
        // lifetime is simpler for clients to reason about.
        let life_extending_token = {
            let now = tokens::now();
            server.sign_claims(&models::authorizations::ControlClaims {
                iat: now.timestamp() as u64,
                exp: (now + chrono::Duration::minutes(10)).timestamp() as u64,
                sub: alice,
                role: "authenticated".to_string(),
                aud: "authenticated".to_string(),
                email: Some("alice@example.test".to_string()),
                capability_mask: None,
                prefix_scope: None,
            })
        };
        let bearer_exp = server.verify_access_token(&life_extending_token).exp;

        let minted = server
            .rest_client()
            .post(
                "/api/v1/auth/token",
                &serde_json::json!({
                    "grant_type": "capability_token",
                    "capability_mask": ["viewer"],
                }),
                Some(&life_extending_token),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(minted.status(), reqwest::StatusCode::OK);
        let body: serde_json::Value = minted.json().await.unwrap();

        let claims = server.verify_access_token(body["access_token"].as_str().unwrap());
        assert_eq!(
            claims.exp - claims.iat,
            super::CAPABILITY_TOKEN_DURATION.as_secs(),
            "the lifetime is the constant even when the bearer expires sooner"
        );
        assert!(
            claims.exp > bearer_exp,
            "the minted token outlives the bearer that minted it"
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
        // Scoped CI and dekaf bearers carry other `role` claims. They pass the
        // Envelope's `aud` check, but they are machine credentials rather than
        // a user session with grants to narrow, so the grant refuses them.
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
                prefix_scope: None,
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
            @"Capability tokens can only be minted from a user session"
        );

        // === Service accounts cannot mint ===
        // Create an account as alice, then fabricate the access token its API
        // key would exchange into. This is equivalent to a bearer exchange
        // without needing the `jwt_sign_polyfill` fixture.
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

    /// The `refresh_token` grant authenticates with the credential in its body
    /// and must ignore the `Authorization` header entirely: the header is
    /// neither acted on nor allowed to fail the request. The sharpest case is
    /// a client that sends its single-use credential in both places. If the
    /// header were exchanged first, that exchange would rotate the secret and
    /// discard the replacement, the body exchange would then fail, and the
    /// client would be locked out. Instead the credential rotates exactly
    /// once and the response carries the replacement.
    ///
    /// Runs the real SQL `generate_access_token` under the `jwt_sign_polyfill`
    /// fixture, which supplies the signing secret and HS256 `sign()` that the
    /// sqlx::test database otherwise lacks.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../../fixtures",
            scripts("data_planes", "alice", "jwt_sign_polyfill")
        )
    )]
    async fn test_refresh_grant_ignores_authorization_header(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;

        let alice = uuid::Uuid::from_bytes([0x11; 16]);

        // `createRefreshToken` only issues multi-use tokens, which don't
        // rotate, so insert the single-use row directly.
        let secret = "single-use-secret";
        let id: models::Id = sqlx::query_scalar(
            "INSERT INTO refresh_tokens (id, user_id, multi_use, valid_for, hash, detail)
             VALUES (internal.id_generator(), $1, false, interval '30 days', crypt($2, gen_salt('bf')), 'single-use')
             RETURNING id",
        )
        .bind(alice)
        .bind(secret)
        .fetch_one(&pool)
        .await
        .unwrap();

        let uses = || async {
            sqlx::query_scalar::<_, i32>("SELECT uses FROM refresh_tokens WHERE id = $1")
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap()
        };
        let refresh_body = |secret: &str| {
            serde_json::json!({
                "grant_type": "refresh_token",
                "refresh_token_id": id,
                "secret": secret,
            })
        };
        let bearer_credential = |secret: &str| {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD
                .encode(serde_json::json!({ "id": id, "secret": secret }).to_string())
        };

        // === The same single-use credential in header and body rotates once ===
        let exchanged = server
            .rest_client()
            .post(
                "/api/v1/auth/token",
                &refresh_body(secret),
                Some(&bearer_credential(secret)),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(exchanged.status(), reqwest::StatusCode::OK);
        let body: super::TokenResponse = exchanged.json().await.unwrap();
        assert_eq!(uses().await, 1, "the credential was exchanged exactly once");

        let rotated = body
            .refresh_token
            .expect("a single-use exchange returns the replacement credential");
        assert_eq!(rotated.id, id);
        assert_ne!(rotated.secret, secret, "the secret was rotated");

        // The SQL-minted access token is a full-authority user token that
        // verifies under the server's keys.
        let claims = server.verify_access_token(&body.access_token);
        insta::assert_json_snapshot!(claims, {
            ".iat" => "[iat]",
            ".exp" => "[exp]",
        }, @r#"
        {
          "aud": "authenticated",
          "iat": "[iat]",
          "exp": "[exp]",
          "sub": "11111111-1111-1111-1111-111111111111",
          "role": "authenticated"
        }
        "#);

        // The pre-rotation secret is dead, and the replacement works. This is
        // what a header exchange would have broken: the client would hold only
        // the dead secret.
        let stale = server
            .rest_client()
            .post("/api/v1/auth/token", &refresh_body(secret), None)
            .send()
            .await
            .unwrap();
        assert_eq!(stale.status(), reqwest::StatusCode::UNAUTHORIZED);
        assert_eq!(uses().await, 1, "a failed exchange does not count as a use");

        let mut secret = rotated.secret;
        let renewed = server
            .rest_client()
            .post("/api/v1/auth/token", &refresh_body(&secret), None)
            .send()
            .await
            .unwrap();
        assert_eq!(renewed.status(), reqwest::StatusCode::OK);
        let body: super::TokenResponse = renewed.json().await.unwrap();
        secret = body.refresh_token.unwrap().secret;
        assert_eq!(uses().await, 2);

        // === Malformed and expired headers are ignored on the refresh grant ===
        // Each of these would be rejected by the Envelope extractor, so a
        // successful exchange proves the header was never examined.
        let expired_bearer = {
            let now = tokens::now();
            server.sign_claims(&models::authorizations::ControlClaims {
                iat: (now - chrono::Duration::hours(2)).timestamp() as u64,
                exp: (now - chrono::Duration::hours(1)).timestamp() as u64,
                sub: alice,
                role: "authenticated".to_string(),
                aud: "authenticated".to_string(),
                email: None,
                capability_mask: None,
                prefix_scope: None,
            })
        };
        for (name, header) in [
            ("a non-bearer scheme", "Basic Zm9vOmJhcg==".to_string()),
            ("an unparseable bearer", "Bearer not.a.jwt".to_string()),
            ("an expired bearer", format!("Bearer {expired_bearer}")),
        ] {
            let exchanged = server
                .rest_client()
                .post("/api/v1/auth/token", &refresh_body(&secret), None)
                .header("Authorization", &header)
                .send()
                .await
                .unwrap();
            let status = exchanged.status();
            let body: serde_json::Value = exchanged.json().await.unwrap();
            assert_eq!(
                status,
                reqwest::StatusCode::OK,
                "{name} in the header must not affect the refresh grant: {body}"
            );
            secret = body["refresh_token"]["secret"]
                .as_str()
                .unwrap()
                .to_string();
        }
        assert_eq!(uses().await, 5);

        // === Contrast: the capability_token grant does read the header ===
        // The same expired bearer is rejected there, so the refresh grant's
        // indifference above is a property of the grant, not of the route.
        let refused = server
            .rest_client()
            .post(
                "/api/v1/auth/token",
                &serde_json::json!({
                    "grant_type": "capability_token",
                    "capability_mask": ["viewer"],
                }),
                Some(&expired_bearer),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(refused.status(), reqwest::StatusCode::UNAUTHORIZED);
    }
}

//! The compatibility half of the suite: unmasked callers keep the full
//! refresh-token loop — create, exchange, and full authority from the
//! exchanged token — and an identity-only minted token still operates as
//! its user. The masked *refusals* guarding this loop (`createRefreshToken`
//! and the `/admin` surfaces) are pinned in those surfaces' own modules and
//! deliberately not repeated here; see the module doc.
//!
//! The loop's exchange runs the real SQL `generate_access_token` under the
//! `jwt_sign_polyfill` fixture, which supplies the HS256 `sign()` and
//! secret the sqlx::test database otherwise lacks.

use super::{DANA, authorize_collection, mint, post_token};
use crate::test_server;

fn create_refresh_token_query() -> serde_json::Value {
    serde_json::json!({
        "query": r#"mutation { createRefreshToken(validFor: "P30D", detail: "suite") { id secret } }"#,
    })
}

/// The full-authority credential loop, end to end: an unmasked caller
/// creates a refresh token, exchanges it at `POST /api/v1/auth/token`, and
/// the exchanged access token — minted by the SQL path, carrying no
/// `capability_mask` — exercises the caller's full authority. This is the
/// deliberate design fact the `createRefreshToken` guard exists to protect:
/// refresh tokens are full-authority credentials, so only unmasked bearers
/// may create them, and revocation (which never widens) stays open to any
/// bearer — including an identity-only empty-mask token.
#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../fixtures",
        scripts("data_planes", "masked_suite", "jwt_sign_polyfill")
    )
)]
async fn test_credential_loop(pool: sqlx::PgPool) {
    let _guard = test_server::init();
    let server = test_server::TestServer::start(
        pool.clone(),
        test_server::snapshot(pool.clone(), false).await,
    )
    .await;
    let dana = server.make_access_token(DANA, Some("dana@example.test"));

    // === An unmasked bearer runs the full loop ===
    // (That a masked bearer cannot create the credential is pinned by
    // `graphql/refresh_tokens`' own guard test.)
    let created: serde_json::Value = server
        .graphql(&create_refresh_token_query(), Some(&dana))
        .await;
    assert!(created["errors"].is_null(), "{created}");
    let id = created["data"]["createRefreshToken"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let secret = created["data"]["createRefreshToken"]["secret"]
        .as_str()
        .unwrap()
        .to_string();

    let (status, body) = post_token(
        &server,
        &serde_json::json!({
            "grant_type": "refresh_token",
            "refresh_token_id": id,
            "secret": secret,
        }),
        None,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK, "exchange: {body}");
    let body: serde_json::Value = serde_json::from_str(&body).unwrap();
    let exchanged = body["access_token"].as_str().unwrap().to_string();

    // The exchanged token is unmasked and exercises dana's full authority:
    // Write under her direct admin grant, through real Envelope
    // verification of the SQL-signed token.
    let claims = tokens::jwt::parse_unverified::<models::authorizations::ControlClaims>(
        exchanged.as_bytes(),
    )
    .unwrap();
    assert_eq!(claims.claims().capability_mask, None);
    assert_eq!(claims.claims().sub, DANA);

    let outcome =
        authorize_collection(&server, &exchanged, "danaCo/data/collection", "write").await;
    assert!(outcome.starts_with("OK"), "{outcome}");

    // The bearer-credential form — the dot-less base64 (id, secret) pair
    // exchanged inside Envelope extraction — is the same full authority.
    use base64::Engine;
    let bearer_credential = base64::engine::general_purpose::STANDARD
        .encode(serde_json::json!({ "id": id, "secret": secret }).to_string());
    let outcome = authorize_collection(
        &server,
        &bearer_credential,
        "danaCo/data/collection",
        "write",
    )
    .await;
    assert!(outcome.starts_with("OK"), "{outcome}");

    // === Revocation stays open to a masked bearer, and never widens ===
    // An identity-only token revokes the credential: its identity operates
    // (the half of "empty mask is identity-only" the walk tests can't pin),
    // and afterward the credential is dead — the loop is closed.
    let identity_only = mint(&server, &dana, &[]).await;
    let revoked: serde_json::Value = server
        .graphql(
            &serde_json::json!({
                "query": r#"mutation($id: Id!) { revokeRefreshToken(id: $id) }"#,
                "variables": { "id": id },
            }),
            Some(&identity_only),
        )
        .await;
    assert!(revoked["errors"].is_null(), "{revoked}");
    assert_eq!(revoked["data"]["revokeRefreshToken"], true);

    let (status, _body) = post_token(
        &server,
        &serde_json::json!({
            "grant_type": "refresh_token",
            "refresh_token_id": id,
            "secret": secret,
        }),
        None,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
}

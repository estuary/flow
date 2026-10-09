use super::*;

const CLIENT: &str = "test-client";
const REDIRECT: &str = "https://client.example/callback?existing=yes";
const RESOURCE: &str = "https://api.example";
const VERIFIER: &str = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";

async fn start(pool: sqlx::PgPool) -> (crate::test_server::TestServer, reqwest::Client, String) {
    let config: Config = serde_json::from_value(serde_json::json!({
        "issuer": RESOURCE, "dashboard": "https://dashboard.example/oauth/consent",
        "clients": { CLIENT: {
            "client_id": CLIENT, "client_name": "Example client", "redirect_uris": [REDIRECT],
            "token_endpoint_auth_method": "none", "grant_types": ["authorization_code"],
            "response_types": ["code"]
        }}
    }))
    .unwrap();
    config.validate().unwrap();
    let snapshot = crate::test_server::snapshot(pool.clone(), false).await;
    let server = crate::test_server::TestServer::start_with_oauth_config(
        pool,
        snapshot,
        None,
        None,
        models::AlertConfig::default(),
        Some(config),
    )
    .await;
    let bearer = server.make_access_token(
        uuid::Uuid::from_bytes([0x11; 16]),
        Some("alice@example.test"),
    );
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    (server, client, bearer)
}

fn authorization_url(server: &crate::test_server::TestServer) -> url::Url {
    let mut url = server.base_url().join("oauth/authorize").unwrap();
    url.query_pairs_mut().extend_pairs([
        ("response_type", "code"),
        ("client_id", CLIENT),
        ("redirect_uri", REDIRECT),
        (
            "code_challenge",
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
        ),
        ("code_challenge_method", "S256"),
        ("state", "state & + ?"),
        ("resource", RESOURCE),
        ("scope", SCOPE),
    ]);
    url
}

async fn begin(
    server: &crate::test_server::TestServer,
    client: &reqwest::Client,
    bearer: &str,
) -> uuid::Uuid {
    let response = client.get(authorization_url(server)).send().await.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::SEE_OTHER);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let dashboard = url::Url::parse(response.headers()["location"].to_str().unwrap()).unwrap();
    assert_eq!(
        dashboard.origin().ascii_serialization(),
        "https://dashboard.example"
    );
    let id = dashboard
        .query_pairs()
        .find(|(k, _)| k == "request_id")
        .unwrap()
        .1
        .parse()
        .unwrap();
    let response = client
        .get(
            server
                .base_url()
                .join(&format!("oauth/consent/{id}"))
                .unwrap(),
        )
        .bearer_auth(bearer)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let details: serde_json::Value = response.json().await.unwrap();
    assert_eq!(details["client_id"], CLIENT);
    assert_eq!(details["redirect_uri"], REDIRECT);
    assert_eq!(details["scope"], SCOPE);
    assert_eq!(details["resource"], RESOURCE);
    id
}

async fn decision(
    server: &crate::test_server::TestServer,
    client: &reqwest::Client,
    bearer: &str,
    id: uuid::Uuid,
    approve: bool,
) -> reqwest::Response {
    client
        .post(
            server
                .base_url()
                .join(&format!("oauth/consent/{id}"))
                .unwrap(),
        )
        .bearer_auth(bearer)
        .json(&serde_json::json!({ "approve": approve }))
        .send()
        .await
        .unwrap()
}

async fn approved_code(response: reqwest::Response) -> String {
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    let callback = url::Url::parse(body["redirect_uri"].as_str().unwrap()).unwrap();
    let params: std::collections::BTreeMap<_, _> = callback.query_pairs().into_owned().collect();
    assert_eq!(params["existing"], "yes");
    assert_eq!(params["state"], "state & + ?");
    assert_eq!(params["iss"], RESOURCE);
    params["code"].clone()
}

async fn post_token(
    server: &crate::test_server::TestServer,
    client: &reqwest::Client,
    fields: &[(&str, &str)],
) -> reqwest::Response {
    client
        .post(server.base_url().join("oauth/token").unwrap())
        .form(fields)
        .send()
        .await
        .unwrap()
}

fn code_fields(code: &str) -> Vec<(&str, &str)> {
    vec![
        ("grant_type", "authorization_code"),
        ("client_id", CLIENT),
        ("code", code),
        ("code_verifier", VERIFIER),
        ("redirect_uri", REDIRECT),
        ("resource", RESOURCE),
    ]
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../fixtures",
        scripts("data_planes", "alice", "jwt_sign_polyfill")
    )
)]
async fn complete_authorization_bearer_renewal_and_revocation(pool: sqlx::PgPool) {
    let (server, client, bearer) = start(pool.clone()).await;
    let metadata: serde_json::Value = client
        .get(
            server
                .base_url()
                .join(".well-known/oauth-authorization-server")
                .unwrap(),
        )
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    insta::assert_json_snapshot!(metadata, @r#"
    {
      "authorization_endpoint": "https://api.example/oauth/authorize",
      "authorization_response_iss_parameter_supported": true,
      "client_id_metadata_document_supported": true,
      "code_challenge_methods_supported": [
        "S256"
      ],
      "grant_types_supported": [
        "authorization_code"
      ],
      "issuer": "https://api.example",
      "response_modes_supported": [
        "query"
      ],
      "response_types_supported": [
        "code"
      ],
      "scopes_supported": [
        "estuary"
      ],
      "token_endpoint": "https://api.example/oauth/token",
      "token_endpoint_auth_methods_supported": [
        "none"
      ]
    }
    "#);
    let id = begin(&server, &client, &bearer).await;
    // Two approvals must not issue independent codes or credentials.
    let (a, b) = tokio::join!(
        decision(&server, &client, &bearer, id, true),
        decision(&server, &client, &bearer, id, true)
    );
    let (accepted, rejected) = if a.status().is_success() {
        (a, b)
    } else {
        (b, a)
    };
    assert_eq!(rejected.status(), reqwest::StatusCode::BAD_REQUEST);
    let code = approved_code(accepted).await;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM refresh_tokens")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        count, 0,
        "consent must not mint an intermediate refresh token"
    );
    let hash: Vec<u8> =
        sqlx::query_scalar("SELECT code_hash FROM internal.oauth_requests WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(hash, sha2::Sha256::digest(code.as_bytes()).to_vec());

    // Every binding failure leaves the code redeemable.
    for (field, bad) in [
        ("client_id", "other-client"),
        ("redirect_uri", "https://evil.example/"),
        (
            "code_verifier",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ),
        ("resource", "https://other.example"),
        ("scope", "read-only"),
    ] {
        let mut fields = code_fields(&code);
        fields.retain(|(k, _)| *k != field);
        fields.push((field, bad));
        let response = post_token(&server, &client, &fields).await;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "{field}"
        );
    }
    let fields = code_fields(&code);
    let (a, b) = tokio::join!(
        post_token(&server, &client, &fields),
        post_token(&server, &client, &fields)
    );
    let (accepted, rejected) = if a.status().is_success() {
        (a, b)
    } else {
        (b, a)
    };
    assert_eq!(rejected.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(accepted.headers()["cache-control"], "no-store");
    let token: serde_json::Value = accepted.json().await.unwrap();
    let consumed: bool = sqlx::query_scalar(
        "SELECT decision IS TRUE AND code_hash IS NULL AND code_expires_at IS NULL
         FROM internal.oauth_requests WHERE id = $1",
    )
    .bind(id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        consumed,
        "redemption preserves consent while consuming the code"
    );
    assert_eq!(
        decision(&server, &client, &bearer, id, true).await.status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    assert_eq!(token["token_type"], "bearer");
    assert!(
        token.get("expires_in").is_none(),
        "sliding expiry has no fixed lifetime"
    );
    assert!(
        token.get("refresh_token").is_none(),
        "only one client credential is issued"
    );
    assert_eq!(token["scope"], SCOPE);
    let access = token["access_token"].as_str().unwrap();
    let credential: serde_json::Value = serde_json::from_slice(
        &base64::engine::general_purpose::STANDARD
            .decode(access)
            .unwrap(),
    )
    .unwrap();
    let token_id = credential["id"].as_str().unwrap();
    let (initial_hash, initial_uses, multi_use): (String, i32, bool) = sqlx::query_as(
        "SELECT hash, uses, multi_use FROM refresh_tokens WHERE id = $1::text::flowid",
    )
    .bind(token_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        initial_uses, 0,
        "redemption must not exchange an intermediate credential"
    );
    assert!(multi_use);

    // Use the OAuth access_token directly against the existing bearer API.
    // Backdate its last use near expiry to prove the inactivity window renews.
    sqlx::query("UPDATE refresh_tokens SET updated_at = now() - interval '29 days' WHERE id = $1::text::flowid")
        .bind(token_id).execute(&pool).await.unwrap();
    let query = serde_json::json!({
        "query": "query { refreshTokens { edges { node { id uses multiUse } } } }"
    });
    let list: serde_json::Value = server.graphql(&query, Some(access)).await;
    assert!(list["errors"].is_null(), "{list}");
    let edges = list["data"]["refreshTokens"]["edges"].as_array().unwrap();
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0]["node"]["uses"], 1);
    assert_eq!(edges[0]["node"]["multiUse"], true);
    let renewed: bool = sqlx::query_scalar(
        "SELECT updated_at > now() - interval '1 minute' FROM refresh_tokens WHERE id = $1::text::flowid"
    ).bind(token_id).fetch_one(&pool).await.unwrap();
    assert!(renewed);

    // The same credential remains usable, including concurrent requests.
    let (a, b): (serde_json::Value, serde_json::Value) = tokio::join!(
        server.graphql(&query, Some(access)),
        server.graphql(&query, Some(access))
    );
    assert!(a["errors"].is_null(), "{a}");
    assert!(b["errors"].is_null(), "{b}");
    let (hash, uses): (String, i32) =
        sqlx::query_as("SELECT hash, uses FROM refresh_tokens WHERE id = $1::text::flowid")
            .bind(token_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(hash, initial_hash, "multi-use credentials do not rotate");
    assert_eq!(uses, 3);

    let unsupported = post_token(
        &server,
        &client,
        &[
            ("grant_type", "refresh_token"),
            ("client_id", CLIENT),
            ("refresh_token", access),
        ],
    )
    .await;
    assert_eq!(unsupported.status(), reqwest::StatusCode::BAD_REQUEST);
    let unsupported: serde_json::Value = unsupported.json().await.unwrap();
    assert_eq!(unsupported["error"], "unsupported_grant_type");

    // This is an ordinary Estuary credential, including legacy token exchange.
    let legacy = client.post(server.base_url().join("api/v1/auth/token").unwrap()).json(&serde_json::json!({
        "grant_type": "refresh_token", "refresh_token_id": credential["id"], "secret": credential["secret"]
    })).send().await.unwrap();
    assert_eq!(legacy.status(), reqwest::StatusCode::OK);
    let legacy: serde_json::Value = legacy.json().await.unwrap();
    assert!(legacy["access_token"].is_string());
    assert!(legacy.get("refresh_token").is_none());

    let revoked: serde_json::Value = server.graphql(&serde_json::json!({
        "query": "mutation($id: Id!) { revokeRefreshToken(id: $id) }", "variables": { "id": credential["id"] }
    }), Some(&bearer)).await;
    assert!(revoked["errors"].is_null(), "{revoked}");
    let rejected = client
        .post(server.base_url().join("api/graphql").unwrap())
        .bearer_auth(access)
        .json(&query)
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), reqwest::StatusCode::UNAUTHORIZED);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(
        path = "../../fixtures",
        scripts("data_planes", "alice", "jwt_sign_polyfill")
    )
)]
async fn expiry_denial_and_consent_identity(pool: sqlx::PgPool) {
    let (server, client, bearer) = start(pool.clone()).await;
    let id = begin(&server, &client, &bearer).await;
    let url = server
        .base_url()
        .join(&format!("oauth/consent/{id}"))
        .unwrap();
    assert!(
        !client
            .get(url.clone())
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    for restricted in [
        server.make_restricted_access_token(
            uuid::Uuid::from_bytes([0x11; 16]),
            None,
            Some(vec!["admin".into()]),
            None,
        ),
        server.make_access_token(uuid::Uuid::from_bytes([0x22; 16]), None),
    ] {
        assert!(
            !client
                .get(url.clone())
                .bearer_auth(&restricted)
                .send()
                .await
                .unwrap()
                .status()
                .is_success()
        );
        assert!(
            !decision(&server, &client, &restricted, id, true)
                .await
                .status()
                .is_success()
        );
    }
    let denied = decision(&server, &client, &bearer, id, false).await;
    assert_eq!(denied.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = denied.json().await.unwrap();
    let redirect = url::Url::parse(body["redirect_uri"].as_str().unwrap()).unwrap();
    assert!(
        redirect
            .query_pairs()
            .any(|(k, v)| k == "error" && v == "access_denied")
    );
    assert!(
        !decision(&server, &client, &bearer, id, true)
            .await
            .status()
            .is_success()
    );

    let expired = begin(&server, &client, &bearer).await;
    sqlx::query(
        "UPDATE internal.oauth_requests SET expires_at = now() - interval '1 second' WHERE id = $1",
    )
    .bind(expired)
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        !decision(&server, &client, &bearer, expired, true)
            .await
            .status()
            .is_success()
    );

    let id = begin(&server, &client, &bearer).await;
    let code = approved_code(decision(&server, &client, &bearer, id, true).await).await;
    sqlx::query("UPDATE internal.oauth_requests SET code_expires_at = now() - interval '1 second' WHERE id = $1")
        .bind(id).execute(&pool).await.unwrap();
    assert_eq!(
        post_token(&server, &client, &code_fields(&code))
            .await
            .status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    sqlx::query("SELECT internal.cleanup_oauth_requests()")
        .execute(&pool)
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM internal.oauth_requests WHERE code_hash IS NOT NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 0);
    let cleaned: bool = sqlx::query_scalar(
        "SELECT decision IS TRUE AND code_hash IS NULL AND code_expires_at IS NULL
         FROM internal.oauth_requests WHERE id = $1",
    )
    .bind(id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(cleaned, "code cleanup preserves consent");
    assert_eq!(
        decision(&server, &client, &bearer, id, true).await.status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM internal.oauth_requests WHERE id = $1")
            .bind(expired)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 0);

    let id = begin(&server, &client, &bearer).await;
    let code = approved_code(decision(&server, &client, &bearer, id, true).await).await;
    let token: serde_json::Value = post_token(&server, &client, &code_fields(&code))
        .await
        .json()
        .await
        .unwrap();
    sqlx::query("UPDATE refresh_tokens SET updated_at = now() - interval '31 days'")
        .execute(&pool)
        .await
        .unwrap();
    let rejected = client
        .post(server.base_url().join("api/graphql").unwrap())
        .bearer_auth(token["access_token"].as_str().unwrap())
        .json(&serde_json::json!({ "query": "query { refreshTokens { edges { node { id } } } }" }))
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), reqwest::StatusCode::UNAUTHORIZED);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(path = "../../fixtures", scripts("data_planes", "alice"))
)]
async fn validation_and_atomic_mint_failure(pool: sqlx::PgPool) {
    let (server, client, bearer) = start(pool.clone()).await;
    for (field, bad) in [
        ("redirect_uri", "https://evil.example/"),
        ("code_challenge_method", "plain"),
        ("code_challenge", "not-a-challenge"),
        ("resource", "https://other.example"),
        ("scope", "readonly"),
        ("response_type", "token"),
        ("client_id", "https://127.0.0.1/metadata.json"),
    ] {
        let original = authorization_url(&server);
        let fields: Vec<_> = original
            .query_pairs()
            .filter(|(k, _)| k != field)
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        let mut url = original.clone();
        url.set_query(None);
        url.query_pairs_mut()
            .extend_pairs(fields)
            .append_pair(field, bad);
        let response = client.get(url).send().await.unwrap();
        assert_eq!(
            response.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "{field}"
        );
        assert!(!response.headers().contains_key("location"));
    }
    let id = begin(&server, &client, &bearer).await;
    let code = approved_code(decision(&server, &client, &bearer, id, true).await).await;
    // Force credential creation to fail after code consumption. Both operations
    // must roll back, leaving the code available for a later successful retry.
    sqlx::query(
        "ALTER TABLE refresh_tokens ADD CONSTRAINT reject_test_token CHECK (detail IS NULL)",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        post_token(&server, &client, &code_fields(&code))
            .await
            .status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM internal.oauth_requests WHERE id = $1 AND code_hash IS NOT NULL",
    )
    .bind(id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 1);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM refresh_tokens")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    sqlx::query("ALTER TABLE refresh_tokens DROP CONSTRAINT reject_test_token")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        post_token(&server, &client, &code_fields(&code))
            .await
            .status(),
        reqwest::StatusCode::OK
    );
}

#[test]
fn pkce_rfc7636() {
    assert_eq!(
        challenge(Some("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"))
            .ok()
            .as_deref(),
        Some("E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM")
    );
    for verifier in [
        "short".to_string(),
        "a".repeat(129),
        "+".repeat(43),
        "é".repeat(43),
    ] {
        assert!(challenge(Some(&verifier)).is_err());
    }
}

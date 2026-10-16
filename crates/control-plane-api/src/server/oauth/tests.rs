use super::*;

const RESOURCE: &str = "https://mcp.example.test/mcp";
const ISSUER: &str = "https://api.example.test";
const CALLBACK: &str = "http://127.0.0.1:54321/callback";
const VERIFIER: &str = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";

fn settings() -> Config {
    Config::new(
        ISSUER.into(),
        "https://dashboard.example.test/mcp-auth".into(),
        vec![RESOURCE.into()],
        vec![clients::Client {
            client_id: "test-client".into(),
            client_name: "Test client".into(),
            redirect_uris: vec!["http://127.0.0.1/callback".into()],
            token_endpoint_auth_method: "none".into(),
        }],
    )
    .unwrap()
}
fn http() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}
fn param(uri: &str, name: &str) -> String {
    url::Url::parse(uri)
        .unwrap()
        .query_pairs()
        .find(|(k, _)| k == name)
        .unwrap()
        .1
        .into_owned()
}
async fn start(server: &crate::test_server::TestServer, scope: &str) -> String {
    let response = http()
        .get(server.base_url().join("oauth/authorize").unwrap())
        .query(&[
            ("client_id", "test-client"),
            ("redirect_uri", CALLBACK),
            ("response_type", "code"),
            ("code_challenge_method", "S256"),
            (
                "code_challenge",
                "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
            ),
            ("resource", RESOURCE),
            ("scope", scope),
            ("state", "original-state"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 302);
    param(response.headers()["location"].to_str().unwrap(), "request")
}
async fn approve(server: &crate::test_server::TestServer, bearer: &str) -> String {
    let request = start(server, "viewer").await;
    let response = http()
        .post(server.base_url().join("oauth/consent").unwrap())
        .bearer_auth(bearer)
        .json(&serde_json::json!({"request":request,"approve":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let value: serde_json::Value = response.json().await.unwrap();
    let uri = value["redirect_uri"].as_str().unwrap();
    assert_eq!(param(uri, "state"), "original-state");
    assert_eq!(param(uri, "iss"), ISSUER);
    param(uri, "code")
}
async fn exchange(
    server: &crate::test_server::TestServer,
    code: &str,
    verifier: &str,
    client: &str,
) -> reqwest::Response {
    http()
        .post(server.base_url().join("oauth/token").unwrap())
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", code),
            ("code_verifier", verifier),
            ("client_id", client),
            ("redirect_uri", CALLBACK),
        ])
        .send()
        .await
        .unwrap()
}
async fn refresh(server: &crate::test_server::TestServer, token: &str) -> reqwest::Response {
    http()
        .post(server.base_url().join("oauth/token").unwrap())
        .form(&[
            ("grant_type", "refresh_token"),
            ("refresh_token", token),
            ("client_id", "test-client"),
        ])
        .send()
        .await
        .unwrap()
}

#[test]
fn pkce_and_config() {
    assert!(pkce(
        VERIFIER,
        "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
    ));
    assert!(!pkce(
        "short",
        "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
    ));
    assert!(!pkce(
        &"!".repeat(43),
        &base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hash(&"!".repeat(43)))
    ));
    assert!(
        Config::new(
            "http://evil.test".into(),
            "https://dashboard.example.test/consent".into(),
            vec![RESOURCE.into()],
            vec![]
        )
        .is_err()
    );
    assert!(
        Config::new(
            ISSUER.into(),
            "https://dashboard.example.test/consent".into(),
            vec![],
            vec![]
        )
        .is_err()
    );
    assert!(
        Config::new(
            "https://api.example.test/path".into(),
            "https://dashboard.example.test/consent".into(),
            vec![RESOURCE.into()],
            vec![]
        )
        .is_err()
    );
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(path = "../../fixtures", scripts("data_planes", "alice"))
)]
async fn oauth_flow_and_refresh_reuse(pool: sqlx::PgPool) {
    let server = crate::test_server::TestServer::start_with_oauth(pool.clone(), settings()).await;
    let bearer = server.make_access_token(
        uuid::Uuid::from_bytes([0x11; 16]),
        Some("alice@example.test"),
    );
    let metadata: serde_json::Value = http()
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
    assert_eq!(metadata["issuer"], ISSUER);
    assert_eq!(
        metadata["code_challenge_methods_supported"],
        serde_json::json!(["S256"])
    );
    let code = approve(&server, &bearer).await;
    assert_eq!(
        exchange(&server, &code, VERIFIER, "wrong-client")
            .await
            .status(),
        400
    );
    assert_eq!(
        exchange(&server, &code, &"x".repeat(43), "test-client")
            .await
            .status(),
        400
    );
    let response = exchange(&server, &code, VERIFIER, "test-client").await;
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let body: serde_json::Value = response.json().await.unwrap();
    let claims = server.verify_access_token(body["access_token"].as_str().unwrap());
    assert_eq!(claims.aud, RESOURCE);
    assert_eq!(claims.capability_mask, Some(vec!["viewer".into()]));
    assert_eq!(claims.sub, uuid::Uuid::from_bytes([0x11; 16]));
    assert_eq!(claims.exp - claims.iat, 3600);
    // Resource tokens work at the control plane but cannot approve a new grant.
    let response = http()
        .post(server.base_url().join("api/graphql").unwrap())
        .bearer_auth(body["access_token"].as_str().unwrap())
        .json(&serde_json::json!({"query":"query { refreshTokens { edges { node { id } } } }"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let request = start(&server, "viewer").await;
    assert_eq!(
        http()
            .post(server.base_url().join("oauth/consent").unwrap())
            .bearer_auth(body["access_token"].as_str().unwrap())
            .json(&serde_json::json!({"request":request,"approve":true}))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    let response = http()
        .post(server.base_url().join("api/v1/auth/token").unwrap())
        .bearer_auth(body["access_token"].as_str().unwrap())
        .json(&serde_json::json!({"grant_type":"capability_token","capability_mask":["admin"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 403);
    for (field, value) in [
        ("iss", "https://other.example.test"),
        ("aud", "https://other.example.test/mcp"),
    ] {
        let mut claims = tokens::jwt::parse_unverified::<serde_json::Value>(
            body["access_token"].as_str().unwrap().as_bytes(),
        )
        .unwrap()
        .claims()
        .clone();
        claims[field] = value.into();
        let bearer = tokens::jwt::sign(claims, &server.encoding_key).unwrap();
        let rejected = http()
            .post(server.base_url().join("api/graphql").unwrap())
            .bearer_auth(bearer)
            .json(&serde_json::json!({"query":"query { refreshTokens { edges { node { id } } } }"}))
            .send()
            .await
            .unwrap();
        assert_eq!(rejected.status(), 401);
    }
    let first = body["refresh_token"].as_str().unwrap();
    let response = refresh(&server, first).await;
    assert_eq!(response.status(), 200);
    let rotated: serde_json::Value = response.json().await.unwrap();
    assert_ne!(body["refresh_token"], rotated["refresh_token"]);
    assert_eq!(refresh(&server, first).await.status(), 400);
    assert_eq!(
        refresh(&server, rotated["refresh_token"].as_str().unwrap())
            .await
            .status(),
        400
    );
    let revoked: bool =
        sqlx::query_scalar("SELECT bool_and(revoked_at IS NOT NULL) FROM internal.oauth_grants")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(revoked);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(path = "../../fixtures", scripts("data_planes", "alice"))
)]
async fn consent_expiry_and_revocation(pool: sqlx::PgPool) {
    let server = crate::test_server::TestServer::start_with_oauth(pool.clone(), settings()).await;
    let user = uuid::Uuid::from_bytes([0x11; 16]);
    let bearer = server.make_access_token(user, None);
    let request = start(&server, "admin").await;
    let context = server
        .base_url()
        .join(&format!("oauth/consent-context?request={request}"))
        .unwrap();
    assert_eq!(
        http().get(context.clone()).send().await.unwrap().status(),
        401
    );
    let context: serde_json::Value = http()
        .get(context)
        .bearer_auth(&bearer)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    insta::assert_json_snapshot!(context, @r#"
    {
      "client_host": null,
      "client_id": "test-client",
      "client_name": "Test client",
      "resource": "https://mcp.example.test/mcp",
      "scope": "admin"
    }
    "#);
    let scoped = server.make_restricted_access_token(user, None, None, Some("aliceCo/".into()));
    assert_eq!(
        http()
            .post(server.base_url().join("oauth/consent").unwrap())
            .bearer_auth(scoped)
            .json(&serde_json::json!({"request":request,"approve":true}))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    let denied: serde_json::Value = http()
        .post(server.base_url().join("oauth/consent").unwrap())
        .bearer_auth(&bearer)
        .json(&serde_json::json!({"request":request,"approve":false}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        param(denied["redirect_uri"].as_str().unwrap(), "error"),
        "access_denied"
    );
    assert_eq!(
        http()
            .post(server.base_url().join("oauth/consent").unwrap())
            .bearer_auth(&bearer)
            .json(&serde_json::json!({"request":request,"approve":true}))
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    let expired = start(&server, "viewer").await;
    sqlx::query("UPDATE internal.oauth_requests SET expires_at=now()-interval '1 second'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        http()
            .post(server.base_url().join("oauth/consent").unwrap())
            .bearer_auth(&bearer)
            .json(&serde_json::json!({"request":expired,"approve":true}))
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    let code = approve(&server, &bearer).await;
    let body: serde_json::Value = exchange(&server, &code, VERIFIER, "test-client")
        .await
        .json()
        .await
        .unwrap();
    let listed: serde_json::Value = http()
        .get(server.base_url().join("oauth/grants").unwrap())
        .bearer_auth(&bearer)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed["grants"].as_array().unwrap().len(), 1);
    let id = listed["grants"][0]["id"].as_str().unwrap();
    assert_eq!(
        http()
            .delete(
                server
                    .base_url()
                    .join(&format!("oauth/grants/{id}"))
                    .unwrap()
            )
            .bearer_auth(&bearer)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        refresh(&server, body["refresh_token"].as_str().unwrap())
            .await
            .status(),
        400
    );
    // RFC 7009 does not reveal whether a refresh token was known.
    assert_eq!(
        http()
            .post(server.base_url().join("oauth/revoke").unwrap())
            .form(&[("token", "unknown"), ("client_id", "test-client")])
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let code = approve(&server, &bearer).await;
    sqlx::query("UPDATE internal.oauth_codes SET expires_at=now()-interval '1 second'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        exchange(&server, &code, VERIFIER, "test-client")
            .await
            .status(),
        400
    );
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(path = "../../fixtures", scripts("data_planes", "alice"))
)]
async fn concurrent_redemption(pool: sqlx::PgPool) {
    let server = crate::test_server::TestServer::start_with_oauth(pool, settings()).await;
    let bearer = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);
    let code = approve(&server, &bearer).await;
    let (a, b) = tokio::join!(
        exchange(&server, &code, VERIFIER, "test-client"),
        exchange(&server, &code, VERIFIER, "test-client")
    );
    let mut statuses = vec![a.status().as_u16(), b.status().as_u16()];
    statuses.sort();
    assert_eq!(statuses, vec![200, 400]);
    let success = if a.status() == 200 { a } else { b };
    let success: serde_json::Value = success.json().await.unwrap();
    assert_eq!(
        refresh(&server, success["refresh_token"].as_str().unwrap())
            .await
            .status(),
        400
    );
    let code = approve(&server, &bearer).await;
    let body: serde_json::Value = exchange(&server, &code, VERIFIER, "test-client")
        .await
        .json()
        .await
        .unwrap();
    let token = body["refresh_token"].as_str().unwrap();
    let (a, b) = tokio::join!(refresh(&server, token), refresh(&server, token));
    let mut statuses = vec![a.status().as_u16(), b.status().as_u16()];
    statuses.sort();
    assert_eq!(statuses, vec![200, 400]);
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(path = "../../fixtures", scripts("data_planes", "alice"))
)]
async fn authorization_boundaries(pool: sqlx::PgPool) {
    let server = crate::test_server::TestServer::start_with_oauth(pool.clone(), settings()).await;
    let base = vec![
        ("client_id", "test-client"),
        ("redirect_uri", CALLBACK),
        ("response_type", "code"),
        ("code_challenge_method", "S256"),
        (
            "code_challenge",
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
        ),
        ("resource", RESOURCE),
        ("scope", "viewer"),
    ];
    for (key, value, expected) in [
        ("redirect_uri", "https://evil.example.test/steal", None),
        ("code_challenge_method", "plain", Some("invalid_request")),
        (
            "resource",
            "https://other.example.test/mcp",
            Some("invalid_target"),
        ),
        ("scope", "viewer admin", Some("invalid_scope")),
        ("response_type", "token", Some("unsupported_response_type")),
    ] {
        let mut query = base.clone();
        query.iter_mut().find(|(k, _)| *k == key).unwrap().1 = value;
        let response = http()
            .get(server.base_url().join("oauth/authorize").unwrap())
            .query(&query)
            .send()
            .await
            .unwrap();
        if let Some(expected) = expected {
            assert_eq!(response.status(), 302);
            assert_eq!(
                param(response.headers()["location"].to_str().unwrap(), "error"),
                expected
            );
        } else {
            assert_eq!(response.status(), 400);
            assert!(response.headers().get("location").is_none());
        }
    }
    let bearer = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);
    let code = approve(&server, &bearer).await;
    for (key, value) in [
        ("redirect_uri", "http://127.0.0.1:54321/other"),
        ("resource", "https://other.example.test/mcp"),
    ] {
        let mut form = vec![
            ("grant_type", "authorization_code"),
            ("client_id", "test-client"),
            ("code", code.as_str()),
            ("code_verifier", VERIFIER),
            ("redirect_uri", CALLBACK),
            ("resource", RESOURCE),
        ];
        form.iter_mut().find(|(k, _)| *k == key).unwrap().1 = value;
        assert_eq!(
            http()
                .post(server.base_url().join("oauth/token").unwrap())
                .form(&form)
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
    }
    let body: serde_json::Value = exchange(&server, &code, VERIFIER, "test-client")
        .await
        .json()
        .await
        .unwrap();
    let token = body["refresh_token"].as_str().unwrap();
    // Another user cannot list or revoke this connection.
    sqlx::query("INSERT INTO auth.users (id,email) VALUES ($1,'bob@example.test')")
        .bind(uuid::Uuid::from_bytes([0x22; 16]))
        .execute(&pool)
        .await
        .unwrap();
    let other = server.make_access_token(uuid::Uuid::from_bytes([0x22; 16]), None);
    let id: uuid::Uuid = sqlx::query_scalar("SELECT id FROM internal.oauth_grants LIMIT 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    http()
        .delete(
            server
                .base_url()
                .join(&format!("oauth/grants/{id}"))
                .unwrap(),
        )
        .bearer_auth(&other)
        .send()
        .await
        .unwrap();
    let list: serde_json::Value = http()
        .get(server.base_url().join("oauth/grants").unwrap())
        .bearer_auth(&other)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list["grants"], serde_json::json!([]));
    assert_eq!(
        http()
            .post(server.base_url().join("oauth/revoke").unwrap())
            .form(&[("token", token), ("client_id", "test-client")])
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(refresh(&server, token).await.status(), 400);
    let metadata = http()
        .get(
            server
                .base_url()
                .join(".well-known/oauth-authorization-server")
                .unwrap(),
        )
        .header("Origin", "https://client.example.test")
        .send()
        .await
        .unwrap();
    assert_eq!(metadata.headers()["access-control-allow-origin"], "*");
}

#[tokio::test]
async fn cimd_rejects_unsafe_destinations() {
    for client in [
        "http://example.test/client.json",
        "https://user@example.test/client.json",
        "https://example.test/",
        "https://example.test/client.json?x=1",
        "https://127.0.0.1/client.json",
    ] {
        assert!(clients::resolve(client, &[]).await.is_err(), "{client}");
    }
}

pub mod common;

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures("data_planes", "alice")
)]
async fn test_graphql_prefixes(pool: sqlx::PgPool) {
    let _guard = common::init();
    let server = common::TestServer::start(
        pool.clone(),
        // Use an immediate Snapshot. Prefixes doesn't use Envelope::authorization_outcome
        // and won't trigger an authorization retry.
        common::snapshot(pool, false).await,
    )
    .await;

    let token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);

    let response: serde_json::Value = server
        .graphql(
            &serde_json::json!({
                "query": r#"
                query {
                    prefixes(by: { minCapability: read }) {
                        edges {
                            node {
                                prefix
                                userCapability
                            }
                        }
                    }
                }
            "#
            }),
            Some(&token),
        )
        .await;

    insta::assert_json_snapshot!(response,
      @r#"
    {
      "data": {
        "prefixes": {
          "edges": [
            {
              "node": {
                "prefix": "aliceCo/",
                "userCapability": "admin"
              }
            },
            {
              "node": {
                "prefix": "aliceCo/data/",
                "userCapability": "write"
              }
            },
            {
              "node": {
                "prefix": "ops/dp/public/",
                "userCapability": "read"
              }
            }
          ]
        }
      }
    }
    "#);

    // Again, but omit the authorization token with this request.
    let response: serde_json::Value = server
        .graphql(
            &serde_json::json!({
                "query": r#"
                query {
                    prefixes(by: { minCapability: read }) {
                        edges {
                            node {
                                prefix
                            }
                        }
                    }
                }
            "#
            }),
            None,
        )
        .await;

    insta::assert_json_snapshot!(response,
      @r#"
    {
      "data": null,
      "errors": [
        {
          "locations": [
            {
              "column": 21,
              "line": 3
            }
          ],
          "message": "status: 'The request does not have valid authentication credentials', self: \"This is an authenticated API but the request is missing a required Authorization: Bearer token\"",
          "path": [
            "prefixes"
          ]
        }
      ]
    }
    "#);
}

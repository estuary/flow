pub mod common;

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures("data_planes", "alice")
)]
async fn test_graphql_live_specs_by_prefix(pool: sqlx::PgPool) {
    let _guard = common::init();
    let server = common::TestServer::start(pool.clone(), common::snapshot(pool, true).await).await;

    let token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);

    let response: serde_json::Value = server
        .graphql(
            &serde_json::json!({
                "query": r#"
                query {
                    liveSpecs(by: { prefix: "aliceCo/" }) {
                        edges {
                            node {
                                catalogName
                                userCapability
                                liveSpec {
                                    catalogType
                                    dataPlaneId
                                }
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
        "liveSpecs": {
          "edges": [
            {
              "node": {
                "catalogName": "aliceCo/data/foo",
                "liveSpec": {
                  "catalogType": "collection",
                  "dataPlaneId": "111111fffe111111"
                },
                "userCapability": "admin"
              }
            },
            {
              "node": {
                "catalogName": "aliceCo/in/capture-foo",
                "liveSpec": {
                  "catalogType": "capture",
                  "dataPlaneId": "111111fffe111111"
                },
                "userCapability": "admin"
              }
            },
            {
              "node": {
                "catalogName": "aliceCo/out/materialize-bar",
                "liveSpec": {
                  "catalogType": "materialization",
                  "dataPlaneId": "111111fffe111111"
                },
                "userCapability": "admin"
              }
            }
          ]
        }
      }
    }
    "#);
}

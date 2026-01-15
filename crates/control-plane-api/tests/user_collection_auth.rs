use flow_client_next as flow_client;

pub mod common;

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures("data_planes", "alice")
)]
async fn test_user_collection_auth_success(pool: sqlx::PgPool) {
    let _guard = common::init();

    let server =
        common::TestServer::start(pool.clone(), common::snapshot(pool.clone(), true).await).await;
    let user_tokens = server.make_fixed_user_tokens(uuid::Uuid::from_bytes([0x11; 16]), None);
    tokio::time::pause();

    let source = flow_client::workflows::UserCollectionAuth {
        client: server.rest_client(),
        user_tokens: user_tokens.clone(),
        collection: models::Collection::new("aliceCo/data/foo"),
        capability: models::Capability::Write,
    };
    let refresh = tokens::watch(source).ready_owned().await;

    insta::assert_json_snapshot!(
        refresh.token().result().unwrap(),
        {".brokerToken" => "<redacted>"},
        @r###"
        {
          "brokerAddress": "broker.dp.one",
          "brokerToken": "<redacted>",
          "journalNamePrefix": "aliceCo/data/foo/gen1234/",
          "retryMillis": 0
        }
        "###,
    );
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures("data_planes", "alice")
)]
async fn test_user_collection_auth_failure(pool: sqlx::PgPool) {
    let _guard = common::init();

    let server =
        common::TestServer::start(pool.clone(), common::snapshot(pool.clone(), true).await).await;
    let user_tokens = server.make_fixed_user_tokens(
        uuid::Uuid::from_bytes([0x11; 16]),
        Some("alice@example.com"),
    );
    tokio::time::pause();

    let source = flow_client::workflows::UserCollectionAuth {
        client: server.rest_client(),
        user_tokens: user_tokens.clone(),
        collection: models::Collection::new("Some/Other/Collection"),
        capability: models::Capability::Write,
    };
    let refresh = tokens::watch(source).ready_owned().await;

    insta::assert_debug_snapshot!(
        refresh.token().result().unwrap_err(),
        @r#"
    Status {
        code: PermissionDenied,
        message: "alice@example.com is not authorized to Some/Other/Collection for Write",
        source: None,
    }
    "#,
    );
}

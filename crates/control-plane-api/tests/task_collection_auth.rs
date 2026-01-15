use flow_client_next as flow_client;

pub mod common;

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures("data_planes", "alice")
)]
async fn test_task_collection_auth_success(pool: sqlx::PgPool) {
    let _guard = common::init();

    let server =
        common::TestServer::start(pool.clone(), common::snapshot(pool.clone(), true).await).await;
    tokio::time::pause();

    // Create signed source with data-plane HMAC key.
    // The capture task aliceCo/in/capture-foo has write access to aliceCo/data/
    // via role_grant: aliceCo/in/ -> aliceCo/data/ (write)
    let signed_source = flow_client::workflows::task_collection_auth::new_signed_source(
        "aliceCo/data/foo/gen1234/pivot=00".to_string(), // journal name
        "capture/aliceCo/in/capture-foo/gen5678/00000000-00000000".to_string(), // shard id
        proto_gazette::capability::APPEND,
        "dp.one".to_string(), // data_plane_fqdn from fixture
        tokens::jwt::EncodingKey::from_secret(b"secret"), // HMAC key (c2VjcmV0 decoded)
    );

    let source = flow_client::workflows::TaskCollectionAuth {
        client: server.rest_client(),
        signed_source,
    };
    let refresh = tokens::watch(source).ready_owned().await;

    insta::assert_json_snapshot!(
        refresh.token().result().unwrap(),
        {".token" => "<redacted>"},
        @r###"
        {
          "token": "<redacted>",
          "brokerAddress": "broker.dp.one",
          "retryMillis": 0
        }
        "###,
    );
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures("data_planes", "alice")
)]
async fn test_task_collection_auth_failure(pool: sqlx::PgPool) {
    let _guard = common::init();

    let server =
        common::TestServer::start(pool.clone(), common::snapshot(pool.clone(), true).await).await;
    tokio::time::pause();

    // The capture task aliceCo/in/capture-foo does NOT have access to some/other/collection
    let signed_source = flow_client::workflows::task_collection_auth::new_signed_source(
        "some/other/collection/gen9999/pivot=00".to_string(), // unauthorized journal
        "capture/aliceCo/in/capture-foo/gen5678/00000000-00000000".to_string(), // shard id
        proto_gazette::capability::APPEND,
        "dp.one".to_string(),
        tokens::jwt::EncodingKey::from_secret(b"secret"),
    );

    let source = flow_client::workflows::TaskCollectionAuth {
        client: server.rest_client(),
        signed_source,
    };
    let refresh = tokens::watch(source).ready_owned().await;

    insta::assert_debug_snapshot!(
        refresh.token().result().unwrap_err(),
        @r#"
    Status {
        code: PermissionDenied,
        message: "task shard capture/aliceCo/in/capture-foo/gen5678/00000000-00000000 is not authorized to some/other/collection/gen9999/pivot=00 for Write",
        source: None,
    }
    "#,
    );
}

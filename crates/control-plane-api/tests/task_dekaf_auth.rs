use flow_client_next as flow_client;

pub mod common;

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures("data_planes", "alice")
)]
async fn test_task_dekaf_auth_success(pool: sqlx::PgPool) {
    let _guard = common::init();

    let server =
        common::TestServer::start(pool.clone(), common::snapshot(pool.clone(), true).await).await;
    tokio::time::pause();

    // Create signed source for the materialization task
    let signed_source = flow_client::workflows::task_dekaf_auth::new_signed_source(
        "aliceCo/out/materialize-bar".to_string(), // task name
        "dp.one".to_string(),                      // data_plane_fqdn from fixture
        tokens::jwt::EncodingKey::from_secret(b"secret"), // HMAC key (c2VjcmV0 decoded)
    );

    let source = flow_client::workflows::TaskDekafAuth {
        client: server.rest_client(),
        signed_source,
    };
    let refresh = tokens::watch(source).ready_owned().await;

    insta::assert_json_snapshot!(
        refresh.token().result().unwrap(),
        {".token" => "<redacted>"},
        @r#"
    {
      "token": "<redacted>",
      "opsLogsJournal": "ops/tasks/public/one/logs/gen1234/kind=materialization/name=aliceCo%2Fout%2Fmaterialize-bar/pivot=00",
      "opsStatsJournal": "ops/tasks/public/one/stats/gen1234/kind=materialization/name=aliceCo%2Fout%2Fmaterialize-bar/pivot=00",
      "taskSpec": {
        "$serde_json::private::RawValue": "{\"shardTemplate\":{\"id\":\"materialization/aliceCo/out/materialize-bar/gen9012\"}}"
      },
      "retryMillis": 0
    }
    "#,
    );
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures("data_planes", "alice")
)]
async fn test_task_dekaf_auth_failure(pool: sqlx::PgPool) {
    let _guard = common::init();

    let server =
        common::TestServer::start(pool.clone(), common::snapshot(pool.clone(), true).await).await;
    tokio::time::pause();

    // Non-existent task
    let signed_source = flow_client::workflows::task_dekaf_auth::new_signed_source(
        "some/nonexistent/task".to_string(),
        "dp.one".to_string(),
        jsonwebtoken::EncodingKey::from_secret(b"secret"),
    );

    let source = flow_client::workflows::TaskDekafAuth {
        client: server.rest_client(),
        signed_source,
    };
    let refresh = tokens::watch(source).ready_owned().await;

    insta::assert_debug_snapshot!(
        refresh.token().result().unwrap_err(),
        @r#"
    Status {
        code: NotFound,
        message: "task some/nonexistent/task not found",
        source: None,
    }
    "#,
    );
}

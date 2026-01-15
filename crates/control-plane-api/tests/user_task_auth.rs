use flow_client_next as flow_client;

pub mod common;

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures("data_planes", "alice")
)]
async fn test_user_task_auth_success(pool: sqlx::PgPool) {
    let _guard = common::init();

    let server =
        common::TestServer::start(pool.clone(), common::snapshot(pool.clone(), true).await).await;
    let user_tokens = server.make_fixed_user_tokens(uuid::Uuid::from_bytes([0x11; 16]), None);
    tokio::time::pause();

    let source = flow_client::workflows::UserTaskAuth {
        client: server.rest_client(),
        user_tokens: user_tokens.clone(),
        task: models::Name::new("aliceCo/in/capture-foo"),
        capability: models::Capability::Write,
    };
    let refresh = tokens::watch(source).ready_owned().await;

    insta::assert_json_snapshot!(
        refresh.token().result().unwrap(),
        {".brokerToken" => "<redacted>", ".reactorToken" => "<redacted>"},
        @r###"
        {
          "brokerAddress": "broker.dp.one",
          "brokerToken": "<redacted>",
          "opsLogsJournal": "ops/tasks/public/one/logs/gen1234/kind=capture/name=aliceCo%2Fin%2Fcapture-foo/pivot=00",
          "opsStatsJournal": "ops/tasks/public/one/stats/gen1234/kind=capture/name=aliceCo%2Fin%2Fcapture-foo/pivot=00",
          "reactorAddress": "reactor.dp.one",
          "reactorToken": "<redacted>",
          "retryMillis": 0,
          "shardIdPrefix": "capture/aliceCo/in/capture-foo/gen5678/"
        }
        "###,
    );
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures("data_planes", "alice")
)]
async fn test_user_task_auth_failure(pool: sqlx::PgPool) {
    let _guard = common::init();

    let server =
        common::TestServer::start(pool.clone(), common::snapshot(pool.clone(), true).await).await;
    let user_tokens = server.make_fixed_user_tokens(
        uuid::Uuid::from_bytes([0x11; 16]),
        Some("alice@example.com"),
    );
    tokio::time::pause();

    let source = flow_client::workflows::UserTaskAuth {
        client: server.rest_client(),
        user_tokens: user_tokens.clone(),
        task: models::Name::new("Some/Other/Task"),
        capability: models::Capability::Write,
    };
    let refresh = tokens::watch(source).ready_owned().await;

    insta::assert_debug_snapshot!(
        refresh.token().result().unwrap_err(),
        @r#"
    Status {
        code: PermissionDenied,
        message: "alice@example.com is not authorized to Some/Other/Task for Write",
        source: None,
    }
    "#,
    );
}

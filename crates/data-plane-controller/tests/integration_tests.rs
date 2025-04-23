use anyhow::Context;
use mockall_double::double;
use tokio;

use data_plane_controller::{run, Args};

fn test_args() -> Args {
    data_plane_controller::Args {
        database_url: "postgres://postgres:postgres@127.0.0.1:5432/postgres".parse().unwrap(),
        database_ca: None,
        concurrency: 1,
        dequeue_interval: std::time::Duration::from_secs(1),
        heartbeat_timeout: std::time::Duration::from_secs(60),
        git_repo: "git@github.com:estuary/est-dry-dock.git".to_string(),
        secrets_provider: "gcpkms://projects/estuary-control/locations/us-central1/keyRings/pulumi/cryptoKeys/state-secrets".to_string(),
        state_backend: "gs://estuary-pulumi".parse().unwrap(),
        dry_run: false,
    }
}

async fn test_pool(database_url: &url::Url) -> sqlx::PgPool {
    let pg_options = database_url
        .as_str()
        .parse::<sqlx::postgres::PgConnectOptions>()
        .unwrap()
        .application_name("data-plane-controller-integration-test")
        .ssl_mode(sqlx::postgres::PgSslMode::Prefer);

    sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_secs(30))
        .connect_with(pg_options)
        .await
        .unwrap()
}

async fn test_data_plane(pool: &sqlx::PgPool) -> anyhow::Result<models::Id> {
    let base_name = "test-dataplane";
    let prefix = "aliceCo/";

    let data_plane_name = format!("ops/dp/{base_name}");
    let data_plane_fqdn = format!(
        "{:x}.dp.estuary-data.com",
        xxhash_rust::xxh3::xxh3_64(base_name.as_bytes())
    );
    let pulumi_stack = format!("private-{}-{base_name}", prefix.trim_end_matches("/"));
    let ops_l1_inferred_name = format!("ops/rollups/L1/{base_name}/inferred-schemas");
    let ops_l1_stats_name = format!("ops/rollups/L1/{base_name}/catalog-stats");
    let ops_l1_events_name = format!("ops/rollups/L1/{base_name}/events");
    let ops_l2_inferred_transform = format!("from.{data_plane_fqdn}");
    let ops_l2_stats_transform = format!("from.{data_plane_fqdn}");
    let ops_l2_events_transform = format!("from.{data_plane_fqdn}");
    let ops_logs_name = format!("ops/tasks/{base_name}/logs");
    let ops_stats_name = format!("ops/tasks/{base_name}/stats");

    let (broker_address, reactor_address, hmac_keys) = (
        format!("https://gazette.{data_plane_fqdn}"),
        format!("https://reactor.{data_plane_fqdn}"),
        Vec::new(),
    );

    let insert = sqlx::query!(
        r#"
        insert into data_planes (
            data_plane_name,
            data_plane_fqdn,
            ops_logs_name,
            ops_stats_name,
            ops_l1_inferred_name,
            ops_l1_stats_name,
            ops_l1_events_name,
            ops_l2_inferred_transform,
            ops_l2_stats_transform,
            ops_l2_events_transform,
            broker_address,
            reactor_address,
            hmac_keys,
            enable_l2,
            pulumi_stack
        ) values (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
        )
        on conflict (data_plane_name) do update
            set ops_logs_name = $3,
            ops_stats_name = $4,
            ops_l1_inferred_name = $5,
            ops_l1_stats_name = $6,
            ops_l1_events_name = $7,
            ops_l2_inferred_transform = $8,
            ops_l2_stats_transform = $9,
            ops_l2_events_transform = $10,
            broker_address = $11,
            reactor_address = $12,
            hmac_keys = $13,
            enable_l2 = $14,
            pulumi_stack = $15
        returning id AS "id: models::Id", controller_task_id AS "task_id: models::Id"
        ;
        "#,
        &data_plane_name as &String,
        data_plane_fqdn,
        &ops_logs_name as &String,
        &ops_stats_name as &String,
        &ops_l1_inferred_name as &String,
        &ops_l1_stats_name as &String,
        &ops_l1_events_name as &String,
        &ops_l2_inferred_transform,
        &ops_l2_stats_transform,
        &ops_l2_events_transform,
        broker_address,
        reactor_address,
        hmac_keys.as_slice(),
        !hmac_keys.is_empty(), // Enable L2 if HMAC keys are defined at creation.
        pulumi_stack,
    )
    .fetch_one(pool)
    .await?;

    sqlx::query!(
        r#"SELECT internal.create_task($1, 1::smallint, '00:00:00:00:00:00:00:00'::macaddr8)"#,
        insert.task_id.unwrap() as models::Id,
    )
    .fetch_one(pool)
    .await
    .context("failed to fetch controller task id")?;

    return Ok(insert.id);
}

async fn test_send_command(
    pool: &sqlx::PgPool,
    data_plane_id: models::Id,
    command: &str,
) -> anyhow::Result<()> {
    let row = sqlx::query!(
        r#"
        SELECT
            controller_task_id AS "task_id: models::Id"
        FROM data_planes
        WHERE id = $1
        "#,
        data_plane_id as models::Id,
    )
    .fetch_one(pool)
    .await
    .context("failed to fetch controller task id")?;

    sqlx::query!(
        r#"SELECT internal.send_to_task($1, '00:00:00:00:00:00:00:00'::flowid, $2::json)"#,
        row.task_id.unwrap() as models::Id,
        serde_json::Value::String(command.to_string()),
    )
    .fetch_one(pool)
    .await
    .context("failed to send task")?;

    return Ok(());
}

#[tokio::test]
async fn basic_run() {
    let args = test_args();
    let pool = test_pool(&args.database_url).await;
    let data_plane_id = test_data_plane(&pool).await.unwrap();
    test_send_command(&pool, data_plane_id, "enable")
        .await
        .unwrap();
    test_send_command(&pool, data_plane_id, "converge")
        .await
        .unwrap();

    eprintln!("{:?}", data_plane_id);

    let result = run(args).await;

    assert!(result.is_ok());
}

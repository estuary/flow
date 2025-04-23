use anyhow::Context;
use serde_json::json;
use sqlx::types::uuid::Uuid;
use std::fs;
use std::path::Path;
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

async fn test_data_plane(pool: &sqlx::PgPool) -> anyhow::Result<(models::Id, models::Id, Uuid)> {
    let base_name = "local-test-dataplane";
    let prefix = "aliceCo/";

    let data_plane_name = format!("ops/dp/{base_name}");
    let data_plane_fqdn = format!(
        "{:x}.dp.estuary-data.com",
        xxhash_rust::xxh3::xxh3_64(base_name.as_bytes())
    );
    let deploy_branch = "main";
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

    let existing_data_plane = sqlx::query!(
        r#"
        SELECT
            controller_task_id AS "task_id: models::Id"
        FROM data_planes
        WHERE data_plane_name = $1
        "#,
        &data_plane_name as &String,
    )
    .fetch_optional(pool)
    .await
    .context("failed to fetch controller task id")?;

    if let Some(row) = existing_data_plane {
        sqlx::query!(
            r#"
            delete from internal.tasks WHERE task_id=$1
            "#,
            row.task_id.unwrap() as models::Id,
        )
        .execute(pool)
        .await?;

        sqlx::query!(
            r#"
            delete from data_planes WHERE data_plane_name=$1
            "#,
            &data_plane_name as &String,
        )
        .execute(pool)
        .await?;
    }

    let tests_dir = Path::new(file!())
        .parent()
        .unwrap()
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap();
    let config: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(format!("{tests_dir}/config.json")).unwrap())
            .unwrap();

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
            pulumi_stack,
            deploy_branch,
            config
        ) values (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
        )
        returning id AS "id: models::Id", controller_task_id AS "task_id: models::Id", logs_token
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
        deploy_branch,
        config,
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

    test_send_command(&pool, insert.task_id.unwrap(), json!({"start": insert.id}))
        .await
        .unwrap();

    return Ok((insert.id, insert.task_id.unwrap(), insert.logs_token));
}

async fn test_send_command(
    pool: &sqlx::PgPool,
    task_id: models::Id,
    command: serde_json::Value,
) -> anyhow::Result<()> {
    sqlx::query!(
        r#"SELECT internal.send_to_task($1, '00:00:00:00:00:00:00:00'::flowid, $2::json)"#,
        task_id as models::Id,
        command,
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
    let (data_plane_id, task_id, logs_token) = test_data_plane(&pool).await.unwrap();
    test_send_command(&pool, task_id, json!("enable"))
        .await
        .unwrap();
    test_send_command(&pool, task_id, json!("converge"))
        .await
        .unwrap();

    eprintln!("logs: {}", logs_token);

    let result = run(args).await;

    assert!(result.is_ok());
}

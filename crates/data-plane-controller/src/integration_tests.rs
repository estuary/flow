use std::sync::{Arc, Mutex};

#[double]
use super::pulumi::Pulumi;
use super::repo::Checkout;
#[double]
use super::repo::Repo;
use super::stack;

use anyhow::Context;
use mockall_double::double;
use serde_json::json;
use sqlx::types::uuid::Uuid;
use std::fs;
use std::path::Path;
use tokio;

use crate::{run, Args};

const PULUMI_STACK: &str = "local-test-pulumi-stack";
const SECRETS_PROVIDER: &str = "gcpkms://projects/estuary-control/locations/us-central1/keyRings/pulumi/cryptoKeys/state-secrets";

fn test_args() -> Args {
    Args {
        database_url: "postgres://postgres:postgres@127.0.0.1:5432/postgres"
            .parse()
            .unwrap(),
        database_ca: None,
        concurrency: 1,
        dequeue_interval: std::time::Duration::from_secs(1),
        heartbeat_timeout: std::time::Duration::from_secs(60),
        git_repo: "git@github.com:estuary/est-dry-dock.git".to_string(),
        secrets_provider: SECRETS_PROVIDER.to_string(),
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

async fn test_data_plane(
    pool: &sqlx::PgPool,
) -> anyhow::Result<(models::Id, models::Id, Uuid, serde_json::Value)> {
    let base_name = "local-test-dataplane";
    let prefix = "aliceCo/";

    let data_plane_name = format!("ops/dp/{base_name}");
    let data_plane_fqdn = format!(
        "{:x}.dp.estuary-data.com",
        xxhash_rust::xxh3::xxh3_64(base_name.as_bytes())
    );
    let deploy_branch = "main";
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

    let tests_dir_path = Path::new(file!()).parent().unwrap();
    let tests_dir = tests_dir_path.file_name().and_then(|s| s.to_str()).unwrap();
    let config: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(format!("{tests_dir}/config_fixture.json")).unwrap(),
    )
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
        PULUMI_STACK,
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

    return Ok((
        insert.id,
        insert.task_id.unwrap(),
        insert.logs_token,
        config,
    ));
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

    let (data_plane_id, task_id, logs_token, config) = test_data_plane(&pool).await.unwrap();

    let ctx_repo = Repo::new_context();
    let path = Arc::new(Mutex::new("".to_string()));
    let repo_path = path.clone();
    ctx_repo.expect().returning(move |_| {
        let mut mock_repo = Repo::default();
        let repo_path = repo_path.clone();
        mock_repo.expect_checkout().returning(move |_, _, _| {
            let dir = tempfile::TempDir::with_prefix(format!("dpc_checkout_"))
                .context("failed to create temp directory")?;
            let checkout = Checkout::test_instance(dir);
            let mut p = repo_path.lock().unwrap();
            *p = checkout.path().to_str().unwrap().to_string();

            anyhow::Ok(checkout)
        });

        mock_repo
    });

    let stack = stack::PulumiStack {
        config: stack::PulumiStackConfig {
            model: serde_json::from_value(config).unwrap(),
        },
        secrets_provider: SECRETS_PROVIDER.to_string(),
        encrypted_key: "test_key".to_string(),
    };

    let ctx_pulumi = Pulumi::new_context();
    ctx_pulumi.expect().return_once(move || {
        let mut mock_pulumi = Pulumi::default();
        let stack_copy = stack.clone();

        mock_pulumi
            .expect_set_encryption()
            .returning(move |_, _, _, _, _, _, _, _| {
                let p = path.lock().unwrap();
                std::fs::write(
                    format!("{p}/Pulumi.{PULUMI_STACK}.yaml"),
                    serde_json::to_string(&stack_copy).unwrap(),
                )
                .unwrap();

                anyhow::Ok(())
            });

        mock_pulumi
            .expect_refresh()
            .returning(move |_, _, _, _, _, _, _, _, _| anyhow::Ok(()));

        mock_pulumi
            .expect_up()
            .returning(move |_, _, _, _, _, _, _, _| anyhow::Ok(()));

        mock_pulumi.expect_last_run().returning(move |_, _, _, _| {
            anyhow::Ok(stack::PulumiStackHistory {
                resource_changes: stack::PulumiStackResourceChanges {
                    same: 1,
                    update: 0,
                    delete: 0,
                    create: 0,
                },
            })
        });

        mock_pulumi
    });

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

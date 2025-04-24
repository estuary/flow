use futures::future::FutureExt;
use std::sync::{Arc, Mutex};

#[double]
use super::ansible::Ansible;
#[double]
use super::pulumi::Pulumi;
use super::repo::Checkout;
#[double]
use super::repo::Repo;
use super::stack;

use anyhow::Context;
use mockall_double::double;
use serde_json::json;

use crate::{run_internal, Args};

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
    base_name: &str,
    config: &serde_json::Value,
    private_links: &Vec<serde_json::Value>,
) -> anyhow::Result<DataPlaneRef> {
    let data_plane_name = format!("ops/dp/{base_name}");
    let data_plane_fqdn = format!(
        "{:x}.dp.estuary-data.com",
        xxhash_rust::xxh3::xxh3_64(base_name.as_bytes())
    );
    let deploy_branch = "main";
    let pulumi_stack = format!("pulumi-{base_name}");
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
            config,
            private_links
        ) values (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
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
        private_links,
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

    return Ok(DataPlaneRef {
        id: insert.id,
        task_id: insert.task_id.unwrap(),
        pool: pool.clone(),
    });
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

struct TestCase {
    name: &'static str,
    config: serde_json::Value,
    private_links: Vec<serde_json::Value>,
    pulumi_up_1_history: stack::PulumiStackHistory,
    pulumi_up_2_history: stack::PulumiStackHistory,
    pulumi_up_1_output: stack::PulumiExports,
}

struct DataPlaneRef {
    id: models::Id,
    task_id: models::Id,
    pool: sqlx::PgPool,
}

impl DataPlaneRef {
    async fn cleanup(&self) -> anyhow::Result<()> {
        sqlx::query!(
            r#"
            delete from internal.tasks WHERE task_id=$1
            "#,
            self.task_id as models::Id,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query!(
            r#"
            delete from data_planes WHERE id=$1
            "#,
            self.id as models::Id,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

async fn dpc_test(
    pool: &sqlx::PgPool,
    shutdown_send: futures::channel::oneshot::Sender<()>,
    case: TestCase,
) -> (
    super::repo::__mock_MockRepo::__new::Context,
    super::pulumi::__mock_MockPulumi::__new::Context,
    super::ansible::__mock_MockAnsible::__new::Context,
    DataPlaneRef,
) {
    let config = case.config;
    let private_links = case.private_links;

    let data_plane_ref = test_data_plane(&pool, case.name, &config, &private_links)
        .await
        .unwrap();

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
    let pulumi_up_1_history = case.pulumi_up_1_history;
    let pulumi_up_2_history = case.pulumi_up_2_history;
    let pulumi_up_1_output = case.pulumi_up_1_output;
    let pulumi_stack = format!("pulumi-{0}", case.name);
    ctx_pulumi.expect().return_once(move || {
        let mut mock_pulumi = Pulumi::default();
        let stack_copy = stack.clone();

        mock_pulumi
            .expect_set_encryption()
            .returning(move |_, _, _, _, _, _, _, _| {
                let p = path.lock().unwrap();
                std::fs::write(
                    format!("{p}/Pulumi.{pulumi_stack}.yaml"),
                    serde_json::to_string(&stack_copy).unwrap(),
                )
                .unwrap();

                anyhow::Ok(())
            });

        // Pulumi Refresh
        mock_pulumi
            .expect_refresh()
            .times(..2)
            .return_once(move |_, _, _, _, _, _, _, _, _| anyhow::Ok(()));

        // Pulumi Up 1
        mock_pulumi
            .expect_up()
            .times(..2)
            .returning(move |_, _, _, _, _, _, _, _| anyhow::Ok(()));

        mock_pulumi
            .expect_last_run()
            .times(..2)
            .return_once(move |_, _, _, _| anyhow::Ok(pulumi_up_1_history.clone()));

        // Pulumi Up 2
        mock_pulumi
            .expect_up()
            .times(..2)
            .return_once(move |_, _, _, _, _, _, _, _| anyhow::Ok(()));

        mock_pulumi
            .expect_last_run()
            .times(..2)
            .return_once(move |_, _, _, _| {
                // shutdown task after pulumi up 2 is done
                shutdown_send.send(()).unwrap();
                anyhow::Ok(pulumi_up_2_history.clone())
            });

        // Ansible and Pulumi Output
        mock_pulumi
            .expect_output()
            .times(..2)
            .return_once(move |_, _, _, _, _| anyhow::Ok(pulumi_up_1_output.clone()));

        mock_pulumi
    });

    let ctx_ansible = Ansible::new_context();
    ctx_ansible.expect().return_once(move || {
        let mut mock_ansible = Ansible::default();
        mock_ansible
            .expect_install()
            .times(..2)
            .return_once(move |_, _, _, _| anyhow::Ok(()));
        mock_ansible
            .expect_run_playbook()
            .times(..2)
            .return_once(move |_, _, _, _, _| anyhow::Ok(()));

        mock_ansible
    });

    test_send_command(&pool, data_plane_ref.task_id, json!("enable"))
        .await
        .unwrap();
    test_send_command(&pool, data_plane_ref.task_id, json!("converge"))
        .await
        .unwrap();

    // These contexts have a significance and must be kept around until the end of the test
    // On Drop their effect on new constructions is reversed
    (ctx_repo, ctx_pulumi, ctx_ansible, data_plane_ref)
}

async fn get_state(pool: &sqlx::PgPool, task_id: &models::Id) -> anyhow::Result<stack::State> {
    let row = sqlx::query!(
        r#"
        SELECT
            inner_state AS "state: sqlx::types::Json<stack::State>"
        FROM internal.tasks
        WHERE task_id = $1
        "#,
        task_id as &models::Id,
    )
    .fetch_one(pool)
    .await
    .context("failed to fetch controller task id")?;

    Ok(row.state.unwrap().0)
}

#[tokio::test]
#[serial_test::serial]
async fn basic_run() {
    let args = test_args();
    let pool = test_pool(&args.database_url).await;
    let (shutdown_send, shutdown_recv) = futures::channel::oneshot::channel::<()>();

    let config: serde_json::Value =
        serde_json::from_slice(&include_bytes!("config_fixture.json").to_vec()).unwrap();

    let pulumi_up_1_output: stack::PulumiExports =
        serde_json::from_slice(&include_bytes!("dry_run_fixture.json").to_vec()).unwrap();
    let pulumi_up_1_history = stack::PulumiStackHistory {
        resource_changes: stack::PulumiStackResourceChanges {
            same: 1,
            update: 0,
            delete: 0,
            create: 0,
        },
    };
    let pulumi_up_2_history = pulumi_up_1_history.clone();

    let (_ctx_repo, _ctx_pulumi, _ctx_ansible, data_plane_ref) = dpc_test(
        &pool,
        shutdown_send,
        TestCase {
            name: "basic-run",
            config: config.clone(),
            private_links: Vec::new(),
            pulumi_up_1_history,
            pulumi_up_2_history,
            pulumi_up_1_output,
        },
    )
    .await;

    let result = run_internal(args, shutdown_recv.map(|_| ())).await;

    assert!(result.is_ok());

    let state = get_state(&pool, &data_plane_ref.task_id).await.unwrap();

    data_plane_ref.cleanup().await.unwrap();

    assert_eq!(state.status, stack::Status::Idle);
    assert_eq!(state.stack.encrypted_key, "test_key".to_string());
    assert_eq!(
        state.stack.config.model.name,
        Some("ops/dp/basic-run".to_string())
    );
    assert_eq!(
        state.stack.config.model.fqdn,
        Some("d817384827fb59f3.dp.estuary-data.com".to_string())
    );

    assert_eq!(
        state.stack.config.model.deployments,
        serde_json::from_value::<Vec<stack::Deployment>>(config["deployments"].clone()).unwrap()
    );
}

#[tokio::test]
#[serial_test::serial]
async fn private_links_duplicate() {
    let args = test_args();
    let pool = test_pool(&args.database_url).await;
    let (shutdown_send, shutdown_recv) = futures::channel::oneshot::channel::<()>();

    let config: serde_json::Value =
        serde_json::from_slice(&include_bytes!("config_fixture.json").to_vec()).unwrap();

    let pulumi_up_1_output: stack::PulumiExports =
        serde_json::from_slice(&include_bytes!("dry_run_fixture.json").to_vec()).unwrap();
    let pulumi_up_1_history = stack::PulumiStackHistory {
        resource_changes: stack::PulumiStackResourceChanges {
            same: 1,
            update: 0,
            delete: 0,
            create: 0,
        },
    };
    let pulumi_up_2_history = pulumi_up_1_history.clone();

    let (_ctx_repo, _ctx_pulumi, _ctx_ansible, data_plane_ref) = dpc_test(
        &pool,
        shutdown_send,
        TestCase {
            name: "private_links_duplicate",
            config: config.clone(),
            private_links: vec![json!({
                "location": "centralus",
                "service_name": "/subscriptions/59436316-c7f2-4163-86d0-fdf5d1fe5367/resourceGroups/D236RGKDPEA01/providers/Microsoft.Network/privateLinkServices/d236plsestryea01"
            })],
            pulumi_up_1_history,
            pulumi_up_2_history,
            pulumi_up_1_output,
        },
    )
    .await;

    let result = run_internal(args, shutdown_recv.map(|_| ())).await;

    assert_eq!(
        result.err().unwrap().to_string(),
        "cannot set both config.private_links and private_links, prefer the latter."
    );

    data_plane_ref.cleanup().await.unwrap();
}

use anyhow::Context;
use std::collections::BTreeMap;
use std::sync::OnceLock;
use std::time::Duration;

/// Prefix for test namespaces. Requires storage mappings and user grants
/// provisioned by `mise run local:test-tenant`.
const TEST_NAMESPACE_PREFIX: &str = "test/dekaf";

pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("e2e=debug".parse().unwrap())
                .add_directive("dekaf=info".parse().unwrap()),
        )
        .with_test_writer()
        .try_init();
}

/// Create a flowctl command configured for local stack.
/// Requires FLOW_ACCESS_TOKEN environment variable to be set.
fn flowctl_command() -> anyhow::Result<async_process::Command> {
    // Try to find flowctl in cargo-target/debug first (where `cargo build` puts it),
    // falling back to locate_bin (which checks alongside the test binary and PATH).
    let cargo_target = std::env::var("CARGO_TARGET_DIR")
        .unwrap_or_else(|_| format!("{}/cargo-target", std::env::var("HOME").unwrap()));
    let debug_flowctl = std::path::PathBuf::from(&cargo_target).join("debug/flowctl");

    let flowctl = if debug_flowctl.exists() {
        debug_flowctl
    } else {
        locate_bin::locate("flowctl")?
    };

    let home = std::env::var("HOME").unwrap();
    let ca_cert =
        std::env::var("SSL_CERT_FILE").unwrap_or_else(|_| format!("{}/flow-local/ca.crt", home));
    let access_token = std::env::var("FLOW_ACCESS_TOKEN")
        .context("FLOW_ACCESS_TOKEN environment variable must be set for e2e tests")?;

    let mut cmd = async_process::Command::new(flowctl);
    cmd.env("FLOW_ACCESS_TOKEN", access_token);
    cmd.env("SSL_CERT_FILE", ca_cert);
    cmd.arg("--profile").arg("local");
    Ok(cmd)
}

/// Test environment for Dekaf E2E tests.
pub struct DekafTestEnv {
    /// Unique namespace for this test (e.g., "test/dekaf/my_test/a1b2")
    pub namespace: String,
    /// The rewritten catalog for modification in disable/enable/reset operations
    catalog: models::Catalog,
}

impl DekafTestEnv {
    /// Setup test environment from a fixture.
    ///
    /// The fixture is a catalog YAML. Names are automatically
    /// rewritten to include a unique test namespace.
    pub async fn setup(test_name: &str, fixture_yaml: &str) -> anyhow::Result<Self> {
        let suffix = format!("{:04x}", rand::random::<u16>());
        let namespace = format!("{TEST_NAMESPACE_PREFIX}/{test_name}/{suffix}");

        tracing::info!(%namespace, "Setting up test environment");

        let catalog = rewrite_fixture(&namespace, fixture_yaml)?;

        let temp_file = tempfile::Builder::new().suffix(".json").tempfile()?;
        std::fs::write(temp_file.path(), serde_json::to_string_pretty(&catalog)?)?;

        let output = async_process::output(flowctl_command()?.args([
            "catalog",
            "publish",
            "--auto-approve",
            "--init-data-plane",
            "ops/dp/public/local-cluster",
            "--source",
            temp_file.path().to_str().unwrap(),
        ]))
        .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            tracing::error!(%stderr, "flowctl publish failed");
            anyhow::bail!("flowctl publish failed: {}", stderr);
        }

        tracing::info!("Publish succeeded");

        let env = Self { namespace, catalog };

        // Wait for capture shard to be ready (Dekaf materializations don't have shards)
        if let Some(capture) = env.capture_name() {
            env.wait_for_primary(capture, Duration::from_secs(60))
                .await?;
        }

        Ok(env)
    }

    /// Returns the name of the first materialization in the catalog.
    pub fn materialization_name(&self) -> Option<&str> {
        self.catalog
            .materializations
            .keys()
            .next()
            .map(|k| k.as_ref())
    }

    /// Returns the name of the first capture in the catalog.
    pub fn capture_name(&self) -> Option<&str> {
        self.catalog.captures.keys().next().map(|k| k.as_ref())
    }

    /// Returns an iterator over collection names in the catalog.
    pub fn collection_names(&self) -> impl Iterator<Item = &str> {
        self.catalog.collections.keys().map(|k| k.as_ref())
    }

    /// Wait for a task's shard to become primary.
    /// Only needed for captures/derivations, not materializations.
    pub async fn wait_for_primary(
        &self,
        task_name: &str,
        duration: Duration,
    ) -> anyhow::Result<()> {
        tracing::info!(%task_name, "Waiting for shard to become primary");
        let deadline = std::time::Instant::now() + duration;

        loop {
            let output = async_process::output(flowctl_command()?.args([
                "raw",
                "list-shards",
                "--task",
                task_name,
                "-ojson",
            ]))
            .await?;

            if output.status.success() {
                let shard: proto_gazette::consumer::list_response::Shard =
                    serde_json::from_slice(&output.stdout)?;

                let status_codes: Vec<_> = shard.status.iter().map(|s| s.code()).collect();
                tracing::debug!(?status_codes, "Shard status");

                if shard
                    .status
                    .iter()
                    .any(|s| s.code() == proto_gazette::consumer::replica_status::Code::Primary)
                {
                    tracing::info!(%task_name, "Shard is primary");
                    return Ok(());
                }

                if let Some(failed) = shard
                    .status
                    .iter()
                    .find(|s| s.code() == proto_gazette::consumer::replica_status::Code::Failed)
                {
                    // Extract and format error messages cleanly
                    let errors: Vec<&str> = failed.errors.iter().map(|s| s.as_str()).collect();
                    for (i, error) in errors.iter().enumerate() {
                        tracing::error!(%task_name, error_num = i + 1, error = %error, "Shard error");
                    }
                    anyhow::bail!("shard {task_name} failed:\n{}", errors.join("\n"));
                }
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                tracing::debug!(%stderr, "list-shards failed (shard may not exist yet)");
            }

            if std::time::Instant::now() > deadline {
                tracing::error!(%task_name, "Timeout waiting for shard");
                anyhow::bail!("timeout waiting for {task_name} to become primary");
            }

            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    /// Inject documents into a collection via HTTP ingest.
    pub async fn inject_documents(
        &self,
        path: &str,
        docs: impl IntoIterator<Item = serde_json::Value>,
    ) -> anyhow::Result<()> {
        let capture = self.capture_name().context("no capture in fixture")?;

        // Get shard endpoint from flowctl
        let output = async_process::output(flowctl_command()?.args([
            "raw",
            "list-shards",
            "--task",
            capture,
            "-ojson",
        ]))
        .await?;

        let shard: proto_gazette::consumer::list_response::Shard =
            serde_json::from_slice(&output.stdout)?;

        let route = shard.route.context("missing route")?;
        let spec = shard.spec.context("missing spec")?;
        let labels = spec.labels.context("missing labels")?;

        let endpoint = route.endpoints.first().context("no endpoints")?;
        let hostname = labels
            .labels
            .iter()
            .find(|l| l.name == "estuary.dev/hostname")
            .map(|l| &l.value)
            .context("no hostname label")?;
        let port = labels
            .labels
            .iter()
            .find(|l| l.name == "estuary.dev/expose-port")
            .map(|l| &l.value)
            .context("no port label")?;

        let base = endpoint.replace("https://", "");
        let url = format!("https://{hostname}-{port}.{base}/{path}");

        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()?;

        let docs: Vec<_> = docs.into_iter().collect();
        tracing::info!(count = docs.len(), %path, "Injecting documents");

        for doc in docs {
            let resp = client
                .post(&url)
                .header("Content-Type", "application/json")
                .json(&doc)
                .send()
                .await?;

            if !resp.status().is_success() {
                anyhow::bail!(
                    "inject failed: {} {}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                );
            }
        }

        Ok(())
    }

    /// Get Kafka connection info for the default dataplane (local-cluster).
    pub async fn connection_info(&self) -> anyhow::Result<ConnectionInfo> {
        let username = self.materialization_name().unwrap_or_default();
        let collections = self.collection_names().map(String::from).collect();
        connection_info_for_dataplane("local-cluster", username, collections).await
    }

    /// Get the Dekaf auth token from the fixture's materialization config.
    ///
    /// Extracts the token from `materializations.<name>.endpoint.dekaf.config.token`.
    pub fn dekaf_token(&self) -> anyhow::Result<String> {
        let mat_def = self
            .catalog
            .materializations
            .values()
            .next()
            .context("no materialization in fixture")?;

        let dekaf_config = match &mat_def.endpoint {
            models::MaterializationEndpoint::Dekaf(cfg) => cfg,
            _ => anyhow::bail!("materialization endpoint is not Dekaf"),
        };

        let config: serde_json::Value = serde_json::from_str(dekaf_config.config.get())?;
        config["token"]
            .as_str()
            .map(String::from)
            .context("dekaf config missing token field")
    }

    /// Create a Rust Kafka consumer connected to Dekaf on the default dataplane.
    ///
    /// Uses the token from the fixture's materialization config.
    pub async fn kafka_consumer(&self) -> anyhow::Result<super::kafka::KafkaConsumer> {
        let info = self.connection_info().await?;
        let token = self.dekaf_token()?;
        Ok(super::kafka::KafkaConsumer::new(
            &info.broker,
            &info.registry,
            &info.username,
            &token,
        ))
    }

    /// Create a Rust Kafka consumer connected to a specific dataplane's Dekaf instance.
    pub async fn kafka_consumer_for_dataplane(
        &self,
        dataplane: &str,
    ) -> anyhow::Result<super::kafka::KafkaConsumer> {
        let username = self.materialization_name().unwrap_or_default();
        let collections = self.collection_names().map(String::from).collect();
        let info = connection_info_for_dataplane(dataplane, username, collections).await?;
        let token = self.dekaf_token()?;
        Ok(super::kafka::KafkaConsumer::new(
            &info.broker,
            &info.registry,
            &info.username,
            &token,
        ))
    }

    pub async fn kafka_consumer_with_group_id(
        &self,
        dataplane: &str,
        group_id: &str,
    ) -> anyhow::Result<super::kafka::KafkaConsumer> {
        let username = self.materialization_name().unwrap_or_default();
        let collections = self.collection_names().map(String::from).collect();
        let info = connection_info_for_dataplane(dataplane, username, collections).await?;
        let token = self.dekaf_token()?;
        Ok(super::kafka::KafkaConsumer::with_group_id(
            &info.broker,
            &info.registry,
            &info.username,
            &token,
            group_id,
        ))
    }

    async fn publish_catalog(&self, catalog: &models::Catalog) -> anyhow::Result<()> {
        let temp_file = tempfile::Builder::new().suffix(".json").tempfile()?;
        std::fs::write(temp_file.path(), serde_json::to_string_pretty(catalog)?)?;

        let output = async_process::output(flowctl_command()?.args([
            "catalog",
            "publish",
            "--auto-approve",
            "--source",
            temp_file.path().to_str().unwrap(),
        ]))
        .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            tracing::error!(%stderr, "flowctl publish failed");
            anyhow::bail!("flowctl publish failed: {}", stderr);
        }

        Ok(())
    }

    /// Disable the capture task by publishing with `shards.disable = true`.
    pub async fn disable_capture(&self) -> anyhow::Result<()> {
        let (capture_name, capture_def) = self
            .catalog
            .captures
            .iter()
            .next()
            .context("no capture in fixture")?;

        tracing::info!(%capture_name, "Disabling capture");

        let mut capture_def = capture_def.clone();
        capture_def.shards.disable = true;

        let catalog = models::Catalog {
            captures: [(capture_name.clone(), capture_def)].into(),
            ..Default::default()
        };

        self.publish_catalog(&catalog).await?;
        tracing::info!(%capture_name, "Capture disabled");
        Ok(())
    }

    /// Re-enable the capture task by publishing with `shards.disable = false`.
    ///
    /// After enabling, use `wait_for_primary()` to wait for the capture to be ready.
    pub async fn enable_capture(&self) -> anyhow::Result<()> {
        let (capture_name, capture_def) = self
            .catalog
            .captures
            .iter()
            .next()
            .context("no capture in fixture")?;

        tracing::info!(%capture_name, "Enabling capture");

        let mut capture_def = capture_def.clone();
        capture_def.shards.disable = false;

        let catalog = models::Catalog {
            captures: [(capture_name.clone(), capture_def)].into(),
            ..Default::default()
        };

        self.publish_catalog(&catalog).await?;
        tracing::info!(%capture_name, "Capture enabled");
        Ok(())
    }

    /// Reset all collections by publishing with `reset: true`.
    ///
    /// This increments the backfill counter for the collection, which Dekaf maps
    /// to a new leader epoch.
    ///
    /// For proper reset behavior, you should:
    /// 1. Disable the capture first (`disable_capture()`)
    /// 2. Reset the collection(s)
    /// 3. Re-enable the capture (`enable_capture()`)
    /// 4. Wait for the capture to be primary (`wait_for_primary()`)
    /// 5. Wait for Dekaf to pick up the new epoch (`wait_for_epoch_change()`)
    pub async fn reset_collections(&self) -> anyhow::Result<()> {
        let collections: BTreeMap<models::Collection, models::CollectionDef> = self
            .catalog
            .collections
            .iter()
            .map(|(k, v)| {
                let mut coll_def = v.clone();
                coll_def.reset = true;
                (k.clone(), coll_def)
            })
            .collect();

        let names: Vec<&str> = collections.keys().map(|k| k.as_ref()).collect();
        tracing::info!(?names, "Resetting collections");

        let catalog = models::Catalog {
            collections,
            ..Default::default()
        };

        self.publish_catalog(&catalog).await
    }

    /// Cleanup test specs synchronously.
    fn cleanup_sync(&self) {
        let cmd = match flowctl_command() {
            Ok(mut cmd) => {
                cmd.args([
                    "catalog",
                    "delete",
                    "--prefix",
                    &self.namespace,
                    "--dangerous-auto-approve",
                ]);
                cmd
            }
            Err(e) => {
                tracing::warn!(error = %e, namespace = %self.namespace, "Failed to create cleanup command");
                return;
            }
        };

        let result = std::process::Command::new(cmd.get_program())
            .args(cmd.get_args())
            .envs(cmd.get_envs().filter_map(|(k, v)| v.map(|v| (k, v))))
            .output();

        match result {
            Ok(output) if output.status.success() => {}
            Ok(output) => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                tracing::warn!(namespace = %self.namespace, %stderr, "Test cleanup failed");
            }
            Err(e) => {
                tracing::warn!(namespace = %self.namespace, error = %e, "Test cleanup command failed");
            }
        }
    }
}

impl Drop for DekafTestEnv {
    fn drop(&mut self) {
        self.cleanup_sync();
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub broker: String,
    pub registry: String,
    pub username: String,
    pub collections: Vec<String>,
}

const LOCAL_DB_URL: &str = "postgresql://postgres:postgres@localhost:5432/postgres";

static DB_POOL: OnceLock<sqlx::PgPool> = OnceLock::new();

pub async fn db_pool() -> anyhow::Result<&'static sqlx::PgPool> {
    if let Some(pool) = DB_POOL.get() {
        return Ok(pool);
    }

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(LOCAL_DB_URL)
        .await
        .context("failed to connect to local postgres")?;

    Ok(DB_POOL.get_or_init(|| pool))
}

/// Get connection info for a specific dataplane by querying the database.
///
/// Returns the Dekaf broker address and schema registry address for the given dataplane.
pub async fn connection_info_for_dataplane(
    dataplane_name: &str,
    username: &str,
    collections: Vec<String>,
) -> anyhow::Result<ConnectionInfo> {
    let pool = db_pool().await?;

    let full_name = format!("ops/dp/public/{dataplane_name}");
    let row: Option<(Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT dekaf_address, dekaf_registry_address FROM data_planes WHERE data_plane_name = $1",
    )
    .bind(&full_name)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to query dataplane {full_name}"))?;

    let (dekaf_address, dekaf_registry_address) =
        row.with_context(|| format!("dataplane {full_name} not found"))?;

    let dekaf_address = dekaf_address
        .with_context(|| format!("dataplane {full_name} has no dekaf_address configured"))?;

    let dekaf_registry_address = dekaf_registry_address.with_context(|| {
        format!("dataplane {full_name} has no dekaf_registry_address configured")
    })?;

    // Parse the Kafka address URL (format: tcp://host:port or tls://host:port)
    let kafka_url = url::Url::parse(&dekaf_address)
        .with_context(|| format!("invalid dekaf_address: {dekaf_address}"))?;

    let host = kafka_url
        .host_str()
        .with_context(|| format!("dekaf_address missing host: {dekaf_address}"))?;
    let kafka_port = kafka_url
        .port()
        .with_context(|| format!("dekaf_address missing port: {dekaf_address}"))?;

    Ok(ConnectionInfo {
        broker: format!("{host}:{kafka_port}"),
        registry: dekaf_registry_address,
        username: username.to_string(),
        collections,
    })
}

/// Trigger a cross-dataplane migration.
///
/// Inserts a row into `data_plane_migrations` to initiate the migration.
/// Returns the migration ID.
pub async fn trigger_migration(
    catalog_name_or_prefix: &str,
    src_dataplane: &str,
    tgt_dataplane: &str,
) -> anyhow::Result<models::Id> {
    let pool = db_pool().await?;

    let src_name = format!("ops/dp/public/{src_dataplane}");
    let tgt_name = format!("ops/dp/public/{tgt_dataplane}");

    tracing::info!(
        %catalog_name_or_prefix,
        %src_name,
        %tgt_name,
        "Triggering migration"
    );

    let migration_id: (models::Id,) = sqlx::query_as(
        r#"
        INSERT INTO data_plane_migrations (
            catalog_name_or_prefix,
            src_plane_id,
            tgt_plane_id,
            cordon_at
        )
        SELECT $1, src.id, tgt.id, NOW()
        FROM data_planes src, data_planes tgt
        WHERE src.data_plane_name = $2
          AND tgt.data_plane_name = $3
        RETURNING id
        "#,
    )
    .bind(catalog_name_or_prefix)
    .bind(&src_name)
    .bind(&tgt_name)
    .fetch_one(pool)
    .await?;

    tracing::info!(migration_id = %migration_id.0, "Migration triggered");
    Ok(migration_id.0)
}

/// Wait for a migration to complete (active = false).
pub async fn wait_for_migration_complete(
    migration_id: models::Id,
    timeout: Duration,
) -> anyhow::Result<()> {
    let pool = db_pool().await?;
    let deadline = std::time::Instant::now() + timeout;

    tracing::info!(%migration_id, "Waiting for migration to complete");

    loop {
        let row: (bool,) = sqlx::query_as("SELECT active FROM data_plane_migrations WHERE id = $1")
            .bind(migration_id)
            .fetch_one(pool)
            .await
            .context("failed to query migration status")?;

        if !row.0 {
            tracing::info!(%migration_id, "Migration completed");
            return Ok(());
        }

        if std::time::Instant::now() > deadline {
            anyhow::bail!("migration {migration_id} did not complete within timeout");
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Wait until Dekaf on the source dataplane returns a redirect to the target dataplane.
///
/// This polls the source Dekaf's metadata endpoint until the broker address in the response
/// matches the target dataplane's Dekaf address, indicating Dekaf knows about the migration.
pub async fn wait_for_dekaf_redirect(
    src_dataplane: &str,
    tgt_dataplane: &str,
    username: &str,
    password: &str,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + timeout;

    let src_info = connection_info_for_dataplane(src_dataplane, username, vec![]).await?;
    let tgt_info = connection_info_for_dataplane(tgt_dataplane, username, vec![]).await?;

    tracing::info!(
        src_broker = %src_info.broker,
        tgt_broker = %tgt_info.broker,
        "Waiting for Dekaf redirect"
    );

    let mut client: Option<super::raw_kafka::TestKafkaClient> = None;

    loop {
        if client.is_none() {
            client =
                super::raw_kafka::TestKafkaClient::connect(&src_info.broker, username, password)
                    .await
                    .ok();
        }

        if let Some(c) = client.as_mut() {
            match c.metadata(&[]).await {
                Ok(metadata) => {
                    for broker in &metadata.brokers {
                        let broker_addr = format!("{}:{}", broker.host.as_str(), broker.port);
                        tracing::debug!(%broker_addr, "Got broker from metadata");

                        if broker_addr == tgt_info.broker {
                            return Ok(());
                        }
                    }
                }
                Err(_) => client = None,
            }
        }

        if std::time::Instant::now() > deadline {
            anyhow::bail!(
                "timeout waiting for Dekaf redirect from {} to {}",
                src_dataplane,
                tgt_dataplane
            );
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Rewrite fixture names to include test namespace.
fn rewrite_fixture(namespace: &str, yaml: &str) -> anyhow::Result<models::Catalog> {
    let yaml_value: serde_yaml::Value = serde_yaml::from_str(yaml)?;
    let original: models::Catalog = serde_json::from_value(serde_json::to_value(&yaml_value)?)?;

    let prefix = |name: &str| format!("{namespace}/{name}");

    // Rewrite collection names
    let collections = original
        .collections
        .into_iter()
        .map(|(name, def)| (models::Collection::new(prefix(name.as_ref())), def))
        .collect();

    // Rewrite capture names and binding targets
    let captures = original
        .captures
        .into_iter()
        .map(|(name, mut def)| {
            for binding in &mut def.bindings {
                binding.target = models::Collection::new(prefix(binding.target.as_ref()));
            }
            (models::Capture::new(prefix(name.as_ref())), def)
        })
        .collect();

    // Rewrite materialization names and binding sources
    let materializations = original
        .materializations
        .into_iter()
        .map(|(name, mut def)| {
            for binding in &mut def.bindings {
                let old_collection = binding.source.collection();
                binding
                    .source
                    .set_collection(models::Collection::new(prefix(old_collection.as_ref())));
            }
            (models::Materialization::new(prefix(name.as_ref())), def)
        })
        .collect();

    Ok(models::Catalog {
        collections,
        captures,
        materializations,
        ..Default::default()
    })
}

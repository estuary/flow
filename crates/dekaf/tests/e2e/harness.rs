//! Test environment for Dekaf E2E tests.
//!
//! Publishes a Flow YAML catalog fixture, waits for captures to be ready,
//! and provides helpers for Kafka client creation.

use anyhow::Context;
use std::time::Duration;

/// Initialize tracing for tests. Call this at the start of each test.
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
///
/// Publishes a Flow YAML catalog fixture, waits for captures to be ready,
/// and provides helpers for Kafka client creation.
pub struct DekafTestEnv {
    /// Unique namespace for this test (e.g., "test/dekaf/my_test/a1b2")
    pub namespace: String,
    /// The rewritten materialization name (username for SASL auth)
    pub materialization: String,
    /// The rewritten capture name (if any)
    pub capture: Option<String>,
    /// Collection names mapped from fixture names
    pub collections: Vec<String>,
    /// The rewritten catalog as serde_yaml::Value for modification in disable/enable/reset operations
    catalog_yaml: serde_yaml::Value,
}

impl DekafTestEnv {
    /// Setup test environment from a Flow YAML fixture.
    ///
    /// The fixture is a standard Flow catalog YAML. Names are automatically
    /// rewritten to include a unique test namespace.
    ///
    /// Example fixture:
    /// ```yaml
    /// collections:
    ///   test_collection:
    ///     schema:
    ///       type: object
    ///       properties:
    ///         id: { type: string }
    ///       required: [id]
    ///     key: [/id]
    ///
    /// captures:
    ///   test_capture:
    ///     endpoint:
    ///       connector:
    ///         image: ghcr.io/estuary/source-http-ingest:dev
    ///         config:
    ///           paths: ["/data"]
    ///     bindings:
    ///       - resource: { path: "/data", stream: "/data" }
    ///         target: test_collection
    ///
    /// materializations:
    ///   test_dekaf:
    ///     endpoint:
    ///       dekaf:
    ///         variant: testing
    ///         config:
    ///           token: "test-token"
    ///           strict_topic_names: false
    ///     bindings:
    ///       - source: test_collection
    ///         resource: { topic_name: test_topic }
    /// ```
    pub async fn setup(test_name: &str, fixture_yaml: &str) -> anyhow::Result<Self> {
        let suffix = format!("{:04x}", rand::random::<u16>());
        // Use test/ prefix which is provisioned by `mise run local:test-tenant`
        // with storage mappings and user grants for the local system user
        let namespace = format!("test/dekaf/{test_name}/{suffix}");

        tracing::info!(%namespace, "Setting up test environment");

        // Parse and rewrite the fixture
        let (rewritten, collections) = rewrite_fixture(&namespace, fixture_yaml)?;

        // Write to temp file and publish
        let temp_file = tempfile::NamedTempFile::new()?;
        std::fs::write(temp_file.path(), &rewritten)?;

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

        // Parse the rewritten fixture as serde_yaml::Value for later modification
        let catalog_yaml: serde_yaml::Value = serde_yaml::from_str(&rewritten)?;

        let materialization = catalog_yaml["materializations"]
            .as_mapping()
            .and_then(|m| m.keys().next())
            .and_then(|k| k.as_str())
            .map(String::from)
            .context("no materialization in fixture")?;

        let capture = catalog_yaml["captures"]
            .as_mapping()
            .and_then(|m| m.keys().next())
            .and_then(|k| k.as_str())
            .map(String::from);

        let env = Self {
            namespace,
            materialization,
            capture,
            collections,
            catalog_yaml,
        };

        // Wait for capture shard to be ready (materializations are shardless)
        if let Some(ref capture) = env.capture {
            env.wait_for_primary(capture).await?;
        }

        Ok(env)
    }

    /// Wait for a task's shard to become primary.
    /// Only needed for captures/derivations, not materializations.
    pub async fn wait_for_primary(&self, task_name: &str) -> anyhow::Result<()> {
        tracing::info!(%task_name, "Waiting for shard to become primary");
        let deadline = std::time::Instant::now() + Duration::from_secs(60);

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

    /// Wait for at least one fragment to be persisted for the collection.
    /// This is needed for timestamp-based offset queries which rely on persisted
    /// fragment mod_times.
    pub async fn wait_for_fragments(
        &self,
        collection: &str,
        min_fragments: usize,
        timeout: Duration,
    ) -> anyhow::Result<FragmentInfo> {
        tracing::info!(%collection, min_fragments, "Waiting for fragments to be persisted");
        let deadline = std::time::Instant::now() + timeout;

        loop {
            let output = async_process::output(flowctl_command()?.args([
                "collections",
                "list-fragments",
                "--collection",
                collection,
                "-o",
                "json",
            ]))
            .await?;

            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                // Parse JSON output - each line is a JSON object
                let fragments: Vec<FragmentResponse> = stdout
                    .lines()
                    .filter(|line| !line.trim().is_empty())
                    .map(|line| {
                        serde_json::from_str(line)
                            .with_context(|| format!("failed to parse fragment JSON: {line}"))
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;

                tracing::debug!(
                    fragment_count = fragments.len(),
                    ?fragments,
                    "Found fragments"
                );

                if fragments.len() >= min_fragments {
                    let parsed_fragments: Vec<Fragment> = fragments
                        .iter()
                        .filter_map(|f| {
                            f.spec.as_ref().map(|s| Fragment {
                                begin: s.begin,
                                end: s.end,
                                mod_time: s.mod_time,
                            })
                        })
                        .collect();

                    let persisted_count =
                        parsed_fragments.iter().filter(|f| f.is_persisted()).count();

                    tracing::info!(
                        %collection,
                        total = parsed_fragments.len(),
                        persisted = persisted_count,
                        "Fragments found"
                    );

                    return Ok(FragmentInfo {
                        fragments: parsed_fragments,
                    });
                }
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                tracing::debug!(%stderr, "list-fragments failed (may not have fragments yet)");
            }

            if std::time::Instant::now() > deadline {
                tracing::error!(%collection, "Timeout waiting for fragments");
                anyhow::bail!(
                    "timeout waiting for {} fragments for {collection}",
                    min_fragments
                );
            }

            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }

    /// Inject documents into a collection via HTTP ingest.
    pub async fn inject_documents(
        &self,
        path: &str,
        docs: impl IntoIterator<Item = serde_json::Value>,
    ) -> anyhow::Result<()> {
        let capture = self.capture.as_ref().context("no capture in fixture")?;

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

    /// Get Kafka connection info for external clients.
    /// Returns (broker, registry_url, username, collections).
    pub fn connection_info(&self) -> ConnectionInfo {
        ConnectionInfo {
            broker: std::env::var("DEKAF_BROKER").unwrap_or("localhost:9092".into()),
            registry: std::env::var("DEKAF_REGISTRY").unwrap_or("http://localhost:9093".into()),
            username: self.materialization.clone(),
            collections: self.collections.clone(),
        }
    }

    /// Get the Dekaf auth token from the fixture's materialization config.
    ///
    /// Extracts the token from `materializations.<name>.endpoint.dekaf.config.token`.
    /// This provides a single source of truth for the test token.
    pub fn dekaf_token(&self) -> anyhow::Result<&str> {
        self.catalog_yaml["materializations"]
            .as_mapping()
            .and_then(|m| m.values().next())
            .and_then(|v| v["endpoint"]["dekaf"]["config"]["token"].as_str())
            .context("fixture materialization missing endpoint.dekaf.config.token")
    }

    /// Create a Rust Kafka consumer connected to Dekaf.
    ///
    /// Uses the token from the fixture's materialization config.
    pub fn kafka_consumer(&self) -> anyhow::Result<super::kafka::KafkaConsumer> {
        let info = self.connection_info();
        let token = self.dekaf_token()?;
        Ok(super::kafka::KafkaConsumer::new(
            &info.broker,
            &info.registry,
            &info.username,
            token,
        ))
    }

    /// Publish a serde_yaml::Value catalog to the control plane.
    async fn publish_yaml(&self, yaml_value: &serde_yaml::Value) -> anyhow::Result<()> {
        let yaml = serde_yaml::to_string(yaml_value)?;
        let temp_file = tempfile::NamedTempFile::new()?;
        std::fs::write(temp_file.path(), &yaml)?;

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
    ///
    /// This stops the capture from writing to collections.
    pub async fn disable_capture(&self) -> anyhow::Result<()> {
        let capture_name = self.capture.as_ref().context("no capture in fixture")?;
        tracing::info!(%capture_name, "Disabling capture");

        // Clone the capture def from the stored catalog and add shards.disable = true
        let mut catalog = serde_yaml::Mapping::new();
        let mut captures = serde_yaml::Mapping::new();

        let original_capture = self.catalog_yaml["captures"][capture_name.as_str()].clone();
        let mut modified_capture = original_capture
            .as_mapping()
            .context("capture is not a mapping")?
            .clone();

        // Add or update the shards section
        let mut shards = modified_capture
            .get(&serde_yaml::Value::String("shards".into()))
            .and_then(|v| v.as_mapping())
            .cloned()
            .unwrap_or_default();
        shards.insert(
            serde_yaml::Value::String("disable".into()),
            serde_yaml::Value::Bool(true),
        );
        modified_capture.insert(
            serde_yaml::Value::String("shards".into()),
            serde_yaml::Value::Mapping(shards),
        );

        captures.insert(
            serde_yaml::Value::String(capture_name.clone()),
            serde_yaml::Value::Mapping(modified_capture),
        );
        catalog.insert(
            serde_yaml::Value::String("captures".into()),
            serde_yaml::Value::Mapping(captures),
        );

        self.publish_yaml(&serde_yaml::Value::Mapping(catalog))
            .await?;
        tracing::info!(%capture_name, "Capture disabled");
        Ok(())
    }

    /// Re-enable the capture task by publishing with `shards.disable = false`.
    ///
    /// After enabling, use `wait_for_primary()` to wait for the capture to be ready.
    pub async fn enable_capture(&self) -> anyhow::Result<()> {
        let capture_name = self.capture.as_ref().context("no capture in fixture")?;
        tracing::info!(%capture_name, "Enabling capture");

        // Clone the capture def from the stored catalog and set shards.disable = false
        let mut catalog = serde_yaml::Mapping::new();
        let mut captures = serde_yaml::Mapping::new();

        let original_capture = self.catalog_yaml["captures"][capture_name.as_str()].clone();
        let mut modified_capture = original_capture
            .as_mapping()
            .context("capture is not a mapping")?
            .clone();

        // Add or update the shards section
        let mut shards = modified_capture
            .get(&serde_yaml::Value::String("shards".into()))
            .and_then(|v| v.as_mapping())
            .cloned()
            .unwrap_or_default();
        shards.insert(
            serde_yaml::Value::String("disable".into()),
            serde_yaml::Value::Bool(false),
        );
        modified_capture.insert(
            serde_yaml::Value::String("shards".into()),
            serde_yaml::Value::Mapping(shards),
        );

        captures.insert(
            serde_yaml::Value::String(capture_name.clone()),
            serde_yaml::Value::Mapping(modified_capture),
        );
        catalog.insert(
            serde_yaml::Value::String("captures".into()),
            serde_yaml::Value::Mapping(captures),
        );

        self.publish_yaml(&serde_yaml::Value::Mapping(catalog))
            .await?;
        tracing::info!(%capture_name, "Capture enabled");
        Ok(())
    }

    /// Reset collections by publishing with `reset: true`.
    ///
    /// This increments the backfill counter for the collection, which Dekaf maps
    /// to a new leader epoch.
    ///
    /// **IMPORTANT**: For proper reset behavior, you should:
    /// 1. Disable the capture first (`disable_capture()`)
    /// 2. Reset the collection(s)
    /// 3. Re-enable the capture (`enable_capture()`)
    /// 4. Wait for the capture to be primary (`wait_for_primary()`)
    /// 5. Wait for Dekaf to pick up the new epoch (`wait_for_epoch_change()`)
    ///
    /// If `collection_name` is None, resets all collections in `self.collections`.
    pub async fn reset_collection(&self, collection_name: Option<&str>) -> anyhow::Result<()> {
        let collections_to_reset: Vec<&str> = match collection_name {
            Some(name) => vec![name],
            None => self.collections.iter().map(|s| s.as_str()).collect(),
        };

        tracing::info!(?collections_to_reset, "Resetting collections");

        // Build a catalog with reset: true for each collection
        let mut catalog = serde_yaml::Mapping::new();
        let mut collections = serde_yaml::Mapping::new();

        for coll_name in &collections_to_reset {
            let original_collection = self.catalog_yaml["collections"][*coll_name].clone();
            let mut modified_collection = original_collection
                .as_mapping()
                .context(format!("collection {coll_name} is not a mapping"))?
                .clone();

            // Add reset: true
            modified_collection.insert(
                serde_yaml::Value::String("reset".into()),
                serde_yaml::Value::Bool(true),
            );

            collections.insert(
                serde_yaml::Value::String(coll_name.to_string()),
                serde_yaml::Value::Mapping(modified_collection),
            );
        }

        catalog.insert(
            serde_yaml::Value::String("collections".into()),
            serde_yaml::Value::Mapping(collections),
        );

        self.publish_yaml(&serde_yaml::Value::Mapping(catalog))
            .await?;
        tracing::info!(?collections_to_reset, "Collections reset published");
        Ok(())
    }

    /// Cleanup test resources.
    fn cleanup_sync(&self) {
        // Build the command - if this fails, we can't clean up but shouldn't panic in Drop
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

        // Run cleanup synchronously using std::process::Command
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

/// Connection info for external Kafka clients.
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub broker: String,
    pub registry: String,
    pub username: String,
    pub collections: Vec<String>,
}

/// Information about fragments for a collection.
#[derive(Debug, Clone)]
pub struct FragmentInfo {
    /// All fragments found.
    pub fragments: Vec<Fragment>,
}

/// A single fragment's information.
#[derive(Debug, Clone)]
pub struct Fragment {
    /// Begin offset (inclusive). None for first fragment.
    pub begin: Option<i64>,
    /// End offset (exclusive).
    pub end: Option<i64>,
    /// Modification time in Unix seconds. None or 0 means unpersisted/open fragment.
    pub mod_time: Option<i64>,
}

impl Fragment {
    /// Returns true if this fragment has been persisted to storage.
    /// Unpersisted (open) fragments have mod_time of 0 or None.
    pub fn is_persisted(&self) -> bool {
        self.mod_time.map_or(false, |t| t > 0)
    }
}

impl FragmentInfo {
    /// Number of fragments.
    pub fn count(&self) -> usize {
        self.fragments.len()
    }

    /// Number of persisted fragments (those with mod_time > 0).
    pub fn persisted_count(&self) -> usize {
        self.fragments.iter().filter(|f| f.is_persisted()).count()
    }

    /// Returns the first persisted fragment, if any.
    pub fn first_persisted(&self) -> Option<&Fragment> {
        self.fragments.iter().find(|f| f.is_persisted())
    }
}

/// JSON response from `flowctl collections list-fragments -o json`.
/// Mirrors the Gazette FragmentsResponse.Fragment proto.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct FragmentResponse {
    spec: Option<FragmentSpec>,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct FragmentSpec {
    #[serde(default, deserialize_with = "deserialize_option_i64_or_string")]
    begin: Option<i64>,
    #[serde(default, deserialize_with = "deserialize_option_i64_or_string")]
    end: Option<i64>,
    #[serde(default, deserialize_with = "deserialize_option_i64_or_string")]
    mod_time: Option<i64>,
}

/// Deserialize an optional i64 that may be represented as a string (common in protobuf JSON).
fn deserialize_option_i64_or_string<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrInt {
        String(String),
        Int(i64),
    }

    Option::<StringOrInt>::deserialize(deserializer)?
        .map(|v| match v {
            StringOrInt::String(s) => s.parse().map_err(serde::de::Error::custom),
            StringOrInt::Int(i) => Ok(i),
        })
        .transpose()
}

/// Rewrite fixture names to include test namespace.
fn rewrite_fixture(namespace: &str, yaml: &str) -> anyhow::Result<(String, Vec<String>)> {
    let mut doc: serde_yaml::Value = serde_yaml::from_str(yaml)?;
    let mut collections = Vec::new();

    // Rewrite collection names
    if let Some(colls) = doc.get_mut("collections").and_then(|v| v.as_mapping_mut()) {
        let keys: Vec<_> = colls.keys().cloned().collect();
        for key in keys {
            if let Some(value) = colls.remove(&key) {
                let old_name = key.as_str().unwrap_or("");
                let new_name = format!("{namespace}/{old_name}");
                collections.push(new_name.clone());
                colls.insert(serde_yaml::Value::String(new_name), value);
            }
        }
    }

    // Rewrite capture names and targets
    if let Some(captures) = doc.get_mut("captures").and_then(|v| v.as_mapping_mut()) {
        let keys: Vec<_> = captures.keys().cloned().collect();
        for key in keys {
            if let Some(mut value) = captures.remove(&key) {
                // Rewrite binding targets
                if let Some(bindings) = value.get_mut("bindings").and_then(|v| v.as_sequence_mut())
                {
                    for binding in bindings {
                        if let Some(target) = binding.get_mut("target") {
                            if let Some(t) = target.as_str() {
                                *target = serde_yaml::Value::String(format!("{namespace}/{t}"));
                            }
                        }
                    }
                }

                let new_key = format!("{namespace}/{}", key.as_str().unwrap_or(""));
                captures.insert(serde_yaml::Value::String(new_key), value);
            }
        }
    }

    // Rewrite materialization names and sources
    if let Some(mats) = doc
        .get_mut("materializations")
        .and_then(|v| v.as_mapping_mut())
    {
        let keys: Vec<_> = mats.keys().cloned().collect();
        for key in keys {
            if let Some(mut value) = mats.remove(&key) {
                // Rewrite binding sources
                if let Some(bindings) = value.get_mut("bindings").and_then(|v| v.as_sequence_mut())
                {
                    for binding in bindings {
                        if let Some(source) = binding.get_mut("source") {
                            if let Some(s) = source.as_str() {
                                *source = serde_yaml::Value::String(format!("{namespace}/{s}"));
                            }
                        }
                    }
                }

                let new_key = format!("{namespace}/{}", key.as_str().unwrap_or(""));
                mats.insert(serde_yaml::Value::String(new_key), value);
            }
        }
    }

    Ok((serde_yaml::to_string(&doc)?, collections))
}

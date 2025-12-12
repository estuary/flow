//! Test environment for Dekaf E2E tests.
//!
//! Publishes a Flow YAML catalog fixture, waits for captures to be ready,
//! and provides helpers for Kafka client creation.

#![allow(dead_code)] // Test utilities may not all be used yet

use anyhow::Context;
use std::time::Duration;

pub mod kafka;

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

/// Default access token for the local stack's system user (support@estuary.dev).
/// This JWT is signed against the local supabase secret and expires in 2055.
/// Can be overridden via FLOW_ACCESS_TOKEN environment variable.
const DEFAULT_LOCAL_ACCESS_TOKEN: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwOi8vMTI3LjAuMC4xOjU0MzEvYXV0aC92MSIsInN1YiI6ImZmZmZmZmZmLWZmZmYtZmZmZi1mZmZmLWZmZmZmZmZmZmZmZiIsImF1ZCI6ImF1dGhlbnRpY2F0ZWQiLCJleHAiOjI3MDAwMDAwMDAsImlhdCI6MTcwMDAwMDAwMCwiZW1haWwiOiJzdXBwb3J0QGVzdHVhcnkuZGV2Iiwicm9sZSI6ImF1dGhlbnRpY2F0ZWQiLCJpc19hbm9ueW1vdXMiOmZhbHNlfQ.Nb-N4s_YnObBHGivSTe_8FEniVUUpehzrRkF5JgNWWU";

/// Create a flowctl command configured for local stack.
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
    let ca_cert = std::env::var("SSL_CERT_FILE")
        .unwrap_or_else(|_| format!("{}/flow-local/ca.crt", home));
    let access_token = std::env::var("FLOW_ACCESS_TOKEN")
        .unwrap_or_else(|_| DEFAULT_LOCAL_ACCESS_TOKEN.to_string());

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

        tracing::info!(path = ?temp_file.path(), "Publishing fixture via flowctl");

        let output = async_process::output(
            flowctl_command()?.args([
                "catalog",
                "publish",
                "--auto-approve",
                "--init-data-plane",
                "ops/dp/public/local-cluster",
                "--source",
                temp_file.path().to_str().unwrap(),
            ]),
        )
        .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            tracing::error!(%stderr, "flowctl publish failed");
            anyhow::bail!("flowctl publish failed: {}", stderr);
        }

        tracing::info!("Publish succeeded");

        // Extract rewritten names from the parsed fixture
        let parsed: serde_yaml::Value = serde_yaml::from_str(&rewritten)?;
        let materialization = parsed["materializations"]
            .as_mapping()
            .and_then(|m| m.keys().next())
            .and_then(|k| k.as_str())
            .map(String::from)
            .context("no materialization in fixture")?;

        let capture = parsed["captures"]
            .as_mapping()
            .and_then(|m| m.keys().next())
            .and_then(|k| k.as_str())
            .map(String::from);

        let env = Self {
            namespace,
            materialization,
            capture,
            collections,
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
            let output = async_process::output(
                flowctl_command()?.args(["raw", "list-shards", "--task", task_name, "-ojson"]),
            )
            .await?;

            if output.status.success() {
                let shard: proto_gazette::consumer::list_response::Shard =
                    serde_json::from_slice(&output.stdout)?;

                let status_codes: Vec<_> = shard.status.iter().map(|s| s.code()).collect();
                tracing::debug!(?status_codes, "Shard status");

                if shard.status.iter().any(|s| {
                    s.code() == proto_gazette::consumer::replica_status::Code::Primary
                }) {
                    tracing::info!(%task_name, "Shard is primary");
                    return Ok(());
                }

                if shard.status.iter().any(|s| {
                    s.code() == proto_gazette::consumer::replica_status::Code::Failed
                }) {
                    tracing::error!(?shard.status, "Shard failed");
                    anyhow::bail!("shard failed: {:?}", shard.status);
                }
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                tracing::debug!(%stderr, "list-shards failed (shard may not exist yet)");
            }

            if std::time::Instant::now() > deadline {
                tracing::error!(%task_name, "Timeout waiting for shard");
                anyhow::bail!("timeout waiting for {task_name} to become primary");
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
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
        let output = async_process::output(
            flowctl_command()?.args(["raw", "list-shards", "--task", capture, "-ojson"]),
        )
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

    /// Create a Rust Kafka consumer connected to Dekaf.
    pub fn kafka_consumer(&self, token: &str) -> kafka::KafkaConsumer {
        let info = self.connection_info();
        kafka::KafkaConsumer::new(&info.broker, &info.registry, &info.username, token)
    }

    /// Create a Kafka consumer builder for custom configuration.
    pub fn kafka_consumer_builder(&self, token: &str) -> kafka::KafkaConsumerBuilder {
        let info = self.connection_info();
        kafka::KafkaConsumerBuilder::new(&info.broker, &info.registry, &info.username, token)
    }

    /// Cleanup test resources explicitly.
    /// Normally not needed due to timestamped namespaces, but useful for
    /// interactive development or long-running test sessions.
    pub async fn cleanup(&self) -> anyhow::Result<()> {
        let _ = async_process::output(
            flowctl_command()?.args([
                "catalog",
                "delete",
                "--prefix",
                &self.namespace,
                "--captures=true",
                "--collections=true",
                "--materializations=true",
                "--dangerous-auto-approve",
            ]),
        )
        .await;

        Ok(())
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
    if let Some(mats) = doc.get_mut("materializations").and_then(|v| v.as_mapping_mut()) {
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

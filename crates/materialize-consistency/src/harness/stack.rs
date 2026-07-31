//! Everything the harness needs from the local stack, all of it through
//! `flowctl`.
//!
//! Driving the stack through the CLI rather than linking the control-plane and
//! data-plane clients is deliberate: publishing, deleting, listing shards,
//! reading a collection and splitting a task are all already single `flowctl`
//! subcommands whose output is JSON, and the auth plumbing behind them — user
//! tokens exchanged for data-plane authorizations — lives inside `flowctl` and is
//! not exported. Re-deriving it here would be a second copy of something with no
//! test of its own.

use crate::invariants::Event;
use anyhow::Context;

/// Identity of the stack this checkout owns, as `mise` makes it ambient.
pub struct Stack {
    pub name: String,
    pub cluster: String,
    pub tenant: String,
    pub stack_dir: std::path::PathBuf,
    pub target_dir: std::path::PathBuf,
    flowctl: std::path::PathBuf,
    auth_token: String,
    ca_cert: String,
}

impl Stack {
    /// Resolve the stack from the environment `mise` provides.
    ///
    /// Stack identity is per-checkout and dynamic — ports, names and paths all
    /// derive from which worktree you are in — so there is nothing to default to
    /// and every missing variable is a hard error naming the task to run.
    pub fn from_env() -> anyhow::Result<Self> {
        let var = |name: &str| -> anyhow::Result<String> {
            std::env::var(name).with_context(|| {
                format!("{name} must be set — run this suite via `mise run ci:consistency`")
            })
        };

        let name = var("FLOW_STACK_NAME")?;
        let cluster = var("FLOW_CLUSTER")?;
        let stack_dir = std::path::PathBuf::from(var("FLOW_STACK_DIR")?);
        let target_dir = std::path::PathBuf::from(var("CARGO_TARGET_DIR")?);

        // Provisioned by `mise run local:test-tenant`, whose env file also
        // carries the matching FLOW_AUTH_TOKEN.
        let tenant = std::env::var("FLOW_TEST_TENANT").unwrap_or_else(|_| "test".to_string());
        let auth_token = var("FLOW_AUTH_TOKEN")?;

        let home = var("HOME")?;
        let ca_cert =
            std::env::var("SSL_CERT_FILE").unwrap_or_else(|_| format!("{home}/flow-local/ca.crt"));

        let flowctl = target_dir.join("debug/flowctl");
        anyhow::ensure!(
            flowctl.exists(),
            "flowctl is not built at {flowctl:?} — run `cargo build -p flowctl`",
        );

        Ok(Self {
            name,
            cluster,
            tenant,
            stack_dir,
            target_dir,
            flowctl,
            auth_token,
            ca_cert,
        })
    }

    /// A binary this crate built, by absolute path.
    ///
    /// Absolute because the reactor spawns `local:` connectors from `$HOME`, not
    /// from the checkout.
    pub fn binary(&self, name: &str) -> anyhow::Result<std::path::PathBuf> {
        let path = self.target_dir.join("debug").join(name);
        anyhow::ensure!(
            path.exists(),
            "{name} is not built at {path:?} — run `cargo build -p materialize-consistency`",
        );
        Ok(path)
    }

    fn command(&self) -> async_process::Command {
        let mut cmd = async_process::Command::new(&self.flowctl);
        cmd.env("FLOW_AUTH_TOKEN", &self.auth_token);
        cmd.env("SSL_CERT_FILE", &self.ca_cert);
        // Passed explicitly rather than relying on the ambient FLOWCTL_PROFILE,
        // so the suite does not depend on env inheritance.
        cmd.arg("--profile").arg(&self.name);
        cmd
    }

    async fn run(&self, args: &[&str]) -> anyhow::Result<String> {
        let output = async_process::output(self.command().args(args))
            .await
            .with_context(|| format!("running flowctl {args:?}"))?;

        if !output.status.success() {
            anyhow::bail!(
                "flowctl {args:?} failed:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    /// Publish a catalog, retrying a build that fails for reasons outside the
    /// catalog.
    ///
    /// A publication builds against the whole control plane, so it can fail from
    /// contention with unrelated work on a busy stack — several scenarios publish at
    /// once, and the stack's own ops-catalog maintenance publishes alongside them. A
    /// catalog that is genuinely wrong fails the same way every time, so retrying a
    /// bounded number of times distinguishes the two without hiding either: the last
    /// error is reported in full.
    pub async fn publish(&self, catalog: &models::Catalog) -> anyhow::Result<()> {
        let file = tempfile::Builder::new().suffix(".json").tempfile()?;
        std::fs::write(file.path(), serde_json::to_string_pretty(catalog)?)?;

        let source = file.path().to_string_lossy().to_string();
        let init_data_plane = format!("ops/dp/public/{}", self.cluster);

        const ATTEMPTS: usize = 4;
        let mut last = None;

        for attempt in 1..=ATTEMPTS {
            match self
                .run(&[
                    "catalog",
                    "publish",
                    "--auto-approve",
                    "--init-data-plane",
                    &init_data_plane,
                    "--source",
                    &source,
                ])
                .await
            {
                Ok(_) => return Ok(()),
                Err(err) => {
                    tracing::warn!(attempt, %err, "publication failed; retrying");
                    last = Some(err);
                    tokio::time::sleep(std::time::Duration::from_secs(5 * attempt as u64)).await;
                }
            }
        }

        Err(last.expect("at least one attempt was made"))
            .context("publishing the scenario's catalog")
    }

    pub async fn delete_prefix(&self, prefix: &str) -> anyhow::Result<()> {
        self.run(&[
            "catalog",
            "delete",
            "--prefix",
            prefix,
            "--dangerous-auto-approve",
        ])
        .await
        .with_context(|| format!("deleting {prefix}"))?;
        Ok(())
    }

    /// Every shard of a task, as the data plane reports it.
    pub async fn shards(
        &self,
        task: &str,
    ) -> anyhow::Result<Vec<proto_gazette::consumer::list_response::Shard>> {
        let stdout = self
            .run(&["raw", "list-shards", "--task", task, "-o", "json"])
            .await?;

        // One JSON value per shard, concatenated.
        let mut shards = Vec::new();
        let mut stream = serde_json::Deserializer::from_str(&stdout).into_iter();
        while let Some(shard) = stream.next() {
            shards.push(shard.context("parsing a shard listing")?);
        }
        Ok(shards)
    }

    /// Whether every shard of `task` currently reports a primary.
    ///
    /// A single listing with no waiting. `await_primary` would block for its whole
    /// timeout, and a caller polling in a loop — like `drain` — would spend its
    /// patience inside this check rather than on giving the task time to finish.
    pub async fn all_primary(&self, task: &str) -> anyhow::Result<bool> {
        use proto_gazette::consumer::replica_status::Code;

        let shards = self.shards(task).await?;

        Ok(!shards.is_empty()
            && shards
                .iter()
                .all(|s| s.status.iter().any(|s| s.code() == Code::Primary)))
    }

    /// Await every shard of `task` reporting a primary.
    ///
    /// A `Failed` status is *not* fatal here, and that is the whole subtlety: after
    /// an injected crash the connector exited non-zero, so the shard reports Failed
    /// until the runtime's backoff restarts it. Treating that as an error would
    /// abort every crash scenario before it could check anything. So a failure is
    /// remembered and reported only if the deadline passes with the shard still
    /// down — which is itself a finding, and one that arrives with the shard's own
    /// error text.
    pub async fn await_primary(
        &self,
        task: &str,
        timeout: std::time::Duration,
    ) -> anyhow::Result<()> {
        use proto_gazette::consumer::replica_status::Code;

        let deadline = std::time::Instant::now() + timeout;
        let mut last_failure = String::new();

        loop {
            match self.shards(task).await {
                Ok(shards) if !shards.is_empty() => {
                    let mut all_primary = true;

                    for shard in &shards {
                        if let Some(failed) = shard.status.iter().find(|s| s.code() == Code::Failed)
                        {
                            last_failure = failed.errors.join("\n");
                            tracing::debug!(%task, failure = %last_failure, "shard is down; awaiting restart");
                        }
                        all_primary &= shard.status.iter().any(|s| s.code() == Code::Primary);
                    }
                    if all_primary {
                        return Ok(());
                    }
                }
                // The shard may not exist yet, which is not yet a failure.
                Ok(_) => {}
                Err(err) => tracing::debug!(%err, %task, "listing shards"),
            }

            if std::time::Instant::now() >= deadline {
                anyhow::bail!(
                    "timed out waiting for {task} to have a primary on every shard{}",
                    if last_failure.is_empty() {
                        String::new()
                    } else {
                        format!("; its last reported failure was:\n{last_failure}")
                    },
                );
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    }

    /// Read every committed document of a collection.
    ///
    /// This is the harness's independent expectation. The connector under test had
    /// no hand in it, so comparing the destination against it detects loss, which
    /// a self-consistency check over the destination alone cannot: a
    /// tail-truncated materialization is internally consistent.
    pub async fn read_collection(&self, collection: &str) -> anyhow::Result<Vec<Event>> {
        let stdout = self
            .run(&[
                "collections",
                "read",
                "--collection",
                collection,
                "-o",
                "json",
            ])
            .await
            .with_context(|| format!("reading collection {collection}"))?;

        let mut events = Vec::new();
        for line in stdout.lines().filter(|l| !l.trim().is_empty()) {
            events.push(
                serde_json::from_str(line)
                    .with_context(|| format!("parsing a document of {collection}: {line}"))?,
            );
        }
        Ok(events)
    }

    /// Read a collection repeatedly until its contents stop growing, and return
    /// them.
    ///
    /// This is how the harness knows a collection is *final*, and it observes that
    /// directly rather than inferring it from the capture's lifecycle: disabling a
    /// capture does not promptly remove its shards, and a task that still has shards
    /// may or may not still be producing. What matters is only that no new document
    /// is arriving — and an expectation read one document early would report the
    /// materialization as having duplicated something it merely delivered on time.
    pub async fn read_collection_when_final(
        &self,
        collection: &str,
        timeout: std::time::Duration,
    ) -> anyhow::Result<Vec<Event>> {
        let deadline = std::time::Instant::now() + timeout;
        let mut previous = usize::MAX;

        loop {
            let documents = self.read_collection(collection).await?;

            if documents.len() == previous {
                return Ok(documents);
            }
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "collection {collection} was still growing after {timeout:?} ({} documents)",
                documents.len(),
            );
            previous = documents.len();
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        }
    }

    /// Unassign a task's shards so the allocator can schedule them again.
    ///
    /// This is how a crash scenario recovers, and it is not a workaround: a shard
    /// whose processing loop fails is marked FAILED and *stays* failed — the
    /// allocator will not reschedule it on its own. In production something
    /// eventually re-activates the task, which unassigns as a side effect; the suite
    /// does the unassigning directly, so recovery is immediate and does not perturb
    /// the specification of the task under test.
    ///
    /// Unconditionally, not `--only-failed`: gazette declines to unassign a shard
    /// whose *primary* has failed under that filter, which is exactly the case here —
    /// it reports zero shards unassigned and the task stays down. Unassigning a
    /// healthy shard costs a brief reassignment, and the harness only calls this
    /// while it is already waiting for a task that is not making progress.
    pub async fn unassign_shards(&self, task: &str) -> anyhow::Result<()> {
        self.run(&["raw", "unassign-shards", "--task", task, "--all"])
            .await
            .with_context(|| format!("unassigning shards of {task}"))?;
        Ok(())
    }

    /// Split every shard of a task in two, on shuffled key.
    ///
    /// The split workflow lives at the shared consumer layer and runs before the
    /// v1/v2 dispatch, so it applies to V2 tasks. It also manufactures a zombie by
    /// design: the source shard's primary is fenced off its recovery log during
    /// the children's recovery and then unassigned, so these scenarios exercise
    /// fencing for free alongside the shim-driven zombie.
    pub async fn split_shards(&self, task: &str) -> anyhow::Result<()> {
        self.run(&["raw", "split-shards", "--task", task])
            .await
            .with_context(|| format!("splitting shards of {task}"))?;
        Ok(())
    }

    /// Join every pair of a task's shards, halving their number.
    ///
    /// The inverse of `split_shards`, and the other half of verifying that scaling
    /// is safe: a survivor absorbs its partner's key range and the partner is
    /// deleted, so any key the departing shard still owed work for has to be picked
    /// up by the one that remains.
    pub async fn join_shards(&self, task: &str) -> anyhow::Result<()> {
        self.run(&["raw", "join-shards", "--task", task])
            .await
            .with_context(|| format!("joining shards of {task}"))?;
        Ok(())
    }

    /// Read a materialized resource back through the connector binary.
    ///
    /// Reading through the connector rather than reaching into the destination is
    /// what lets one harness serve every connector: retrieving all rows of a
    /// resource is already a required method of the shared materializer interface,
    /// and `materialize-boilerplate` exposes it as this same subcommand.
    pub async fn read_destination(
        &self,
        connector: &std::path::Path,
        config: &serde_json::Value,
        table: &str,
        delta: bool,
    ) -> anyhow::Result<Vec<Event>> {
        let mut cmd = async_process::Command::new(connector);
        cmd.arg("read")
            .arg("--config")
            .arg(config.to_string())
            .arg("--table")
            .arg(table);
        if delta {
            cmd.arg("--delta");
        }

        let output = async_process::output(&mut cmd)
            .await
            .with_context(|| format!("reading destination resource {table}"))?;

        anyhow::ensure!(
            output.status.success(),
            "reading destination resource {table} failed:\n{}",
            String::from_utf8_lossy(&output.stderr),
        );

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut events = Vec::new();
        for line in stdout.lines().filter(|l| !l.trim().is_empty()) {
            events.push(
                serde_json::from_str(line)
                    .with_context(|| format!("parsing a row of {table}: {line}"))?,
            );
        }
        Ok(events)
    }
}

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

/// Marker in the error chain of a publication that would not land.
///
/// Present so that a caller can distinguish a stack that refused to publish from a task
/// that published fine and then failed. The defective half of a scenario treats a failed
/// *run* as the defect being caught; a failed *publish* is the environment, and counting
/// it as a catch would let a flaky control plane silently vacate a defect pairing.
#[derive(Debug)]
pub struct PublishFailed;

impl std::fmt::Display for PublishFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the stack would not publish the catalog")
    }
}

impl std::error::Error for PublishFailed {}

/// Which reader a destination is read through.
///
/// `Copy`, because a drain reads three resources per poll and passing it by value each time
/// reads better than borrowing a borrow.
#[derive(Clone, Copy)]
pub enum ReadVia<'a> {
    /// The reference connector's own `read` subcommand. It lives in this repository and is run
    /// by nothing but this suite, so a subcommand there costs nothing.
    Reference {
        connector: &'a std::path::Path,
        config: &'a serde_json::Value,
    },
    /// `tests/materialize/testctl` from the connectors repository, which calls the same
    /// `Materializer.SnapshotTestResource` and `DeleteResource` the connectors' own integration
    /// tests call. A production connector therefore grows no subcommand for this suite.
    Testctl(&'a super::subject::External),
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

    /// Every flowctl invocation is bounded, because none of the scenario deadlines
    /// bound *this*: they guard the wait loops, and a subprocess that never returns
    /// sits under all of them. `destination-ahead-of-checkpoint` hit nextest's 960s
    /// ceiling having logged only its publish line — a `catalog publish` had blocked,
    /// and with four publish retries a single hang multiplies.
    ///
    /// Generous, because a publish on a loaded stack legitimately takes tens of
    /// seconds; the point is to convert "hangs forever" into a named failure that the
    /// publish retry can act on.
    const INVOCATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(150);

    /// For invocations whose duration scales with how much data a run produced,
    /// rather than with how busy the control plane is.
    ///
    /// Reading a collection scans the whole thing, so its cost grows with every
    /// document the workload wrote — and the scenarios that run longest write the
    /// most. One blanket bound failed `join-after-split` on exactly that: a
    /// `collections read` of the collection it had spent 270 seconds filling took
    /// longer than a publish ever would, and 150s cut it off.
    const READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

    /// Attempts at a read before giving up.
    ///
    /// A read does not merely run slowly under contention, it can hang outright, and for
    /// long enough that no bound distinguishes a hang from a large read. Whatever stalls it
    /// — a broker reassignment mid-read is the likeliest — clears, so a fresh attempt
    /// succeeds where waiting longer does not.
    const READ_ATTEMPTS: usize = 3;

    async fn run(&self, args: &[&str]) -> anyhow::Result<String> {
        self.run_bounded(args, Self::INVOCATION_TIMEOUT).await
    }

    async fn run_bounded(
        &self,
        args: &[&str],
        timeout: std::time::Duration,
    ) -> anyhow::Result<String> {
        let output =
            tokio::time::timeout(timeout, async_process::output(self.command().args(args)))
                .await
                .map_err(|_| {
                    anyhow::anyhow!(
                        "flowctl {args:?} did not return within {}s",
                        timeout.as_secs(),
                    )
                })?
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
            .context(PublishFailed)
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
    /// A single listing with no waiting, used only as `drain`'s health signal — to tell
    /// a task that has genuinely gone quiet from one that has fallen over.
    ///
    /// Nothing *waits* on shard status any more. A blocking version existed and was
    /// removed: it failed both after a perturbation, by catching the crash the scenario
    /// had just injected, and before one, because a task starts processing the moment it
    /// is activated and could crash while the harness was still checking a sibling.
    /// Progress — `await_commits` over the shim's trace — is the gate instead.
    pub async fn all_primary(&self, task: &str) -> anyhow::Result<bool> {
        use proto_gazette::consumer::replica_status::Code;

        let shards = self.shards(task).await?;

        Ok(!shards.is_empty()
            && shards
                .iter()
                .all(|s| s.status.iter().any(|s| s.code() == Code::Primary)))
    }

    /// Read every committed document of a collection.
    ///
    /// This is the harness's independent expectation. The connector under test had
    /// no hand in it, so comparing the destination against it detects loss, which
    /// a self-consistency check over the destination alone cannot: a
    /// tail-truncated materialization is internally consistent.
    pub async fn read_collection(&self, collection: &str) -> anyhow::Result<Vec<Event>> {
        let args = [
            "collections",
            "read",
            "--collection",
            collection,
            "-o",
            "json",
        ];

        let mut stdout = String::new();
        for attempt in 1..=Self::READ_ATTEMPTS {
            match self.run_bounded(&args, Self::READ_TIMEOUT).await {
                Ok(out) => {
                    stdout = out;
                    break;
                }
                Err(err) if attempt < Self::READ_ATTEMPTS => {
                    tracing::warn!(%err, %collection, attempt, "the read stalled; retrying");
                }
                Err(err) => {
                    return Err(err).with_context(|| format!("reading collection {collection}"));
                }
            }
        }

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
        // Bounded by attempts as well as by the clock, because the two limits fail for
        // different reasons and only one of them means the capture is still writing.
        //
        // A read of this collection can take a minute under contention, so a wall-clock
        // deadline alone can expire having compared only two or three samples — and it then
        // reports "still growing", which reads as a capture that would not stop when it is
        // really a runner that did not get to look twice in a row.
        const ATTEMPTS: usize = 8;

        let deadline = std::time::Instant::now() + timeout;
        let mut previous = usize::MAX;

        for attempt in 1..=ATTEMPTS {
            let documents = self.read_collection(collection).await?;

            if documents.len() == previous {
                return Ok(documents);
            }
            anyhow::ensure!(
                attempt < ATTEMPTS && std::time::Instant::now() < deadline,
                "collection {collection} was still growing after {attempt} reads over \
                 {timeout:?} ({} documents, previously {previous})",
                documents.len(),
            );
            previous = documents.len();
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        }
        unreachable!("the loop returns or the ensure fails on the last attempt")
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
    /// Every shard, not only the failed ones. Gazette does remove a FAILED assignment
    /// under `--failed`; what that filter skips is a shard whose primary is merely
    /// *wedged* and has not been marked FAILED — which is precisely the state the stall
    /// detection in `recover` fires on, so filtering would report zero shards unassigned
    /// and leave the task down. Unassigning a healthy shard costs a brief reassignment,
    /// and this is only called while already waiting on a task making no progress.
    pub async fn unassign_shards(&self, task: &str) -> anyhow::Result<()> {
        self.shard_tool(&["unassign", task])
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
        self.shard_tool(&["join", task])
            .await
            .with_context(|| format!("joining shards of {task}"))?;
        Ok(())
    }

    /// Run the suite's shard tooling, which drives `gazctl` rather than `flowctl`.
    ///
    /// Unassigning a shard and joining a task's shards are local test affordances, not
    /// things an operator or connector author needs from the CLI, so they are scripts in
    /// this crate instead of `flowctl` subcommands. `gazctl` already implements both; the
    /// only missing piece was pointing it at a Flow data plane, which
    /// `flowctl raw gazctl-env` supplies.
    async fn shard_tool(&self, args: &[&str]) -> anyhow::Result<String> {
        // Resolved from the crate's own source directory, not from the target directory:
        // a build may put artefacts far outside the checkout, but the scripts ship beside
        // the code that calls them.
        let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("scripts")
            .join("shard-tools.sh");

        let mut cmd = async_process::Command::new(&script);
        cmd.args(args);
        cmd.env("FLOWCTL", &self.flowctl);
        cmd.env("FLOW_AUTH_TOKEN", &self.auth_token);
        cmd.env("SSL_CERT_FILE", &self.ca_cert);
        cmd.env("FLOWCTL_PROFILE", &self.name);

        let output =
            tokio::time::timeout(Self::INVOCATION_TIMEOUT, async_process::output(&mut cmd))
                .await
                .map_err(|_| {
                    anyhow::anyhow!(
                        "{script:?} {args:?} did not return within {}s",
                        Self::INVOCATION_TIMEOUT.as_secs(),
                    )
                })?
                .with_context(|| format!("running {script:?} {args:?}"))?;

        anyhow::ensure!(
            output.status.success(),
            "{script:?} {args:?} failed:\n{}",
            String::from_utf8_lossy(&output.stderr),
        );
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    /// Every row of one materialized resource, as newline-delimited JSON objects keyed by
    /// column name.
    ///
    /// Read through connector code rather than by reaching into the destination: the harness
    /// has no client for an arbitrary endpoint and should not grow one.
    pub async fn read_destination(
        &self,
        via: ReadVia<'_>,
        resource: &serde_json::Value,
    ) -> anyhow::Result<Vec<Event>> {
        let stdout = self.read_rows(via, resource, "snapshot").await?;

        // Rows of columns rather than the documents that produced them, which is what a
        // materialized resource actually holds — a standard binding need not carry a root
        // document at all.
        let mut events = Vec::new();
        for line in stdout.lines() {
            if line.trim().is_empty() {
                continue;
            }
            events.push(
                Event::from_row(line)
                    .with_context(|| format!("parsing a row of {resource}: {line}"))?,
            );
        }
        Ok(events)
    }

    /// Remove one materialized resource, through `testctl`.
    ///
    /// Only for a real subject: the reference connector's destination is a file inside the run
    /// directory, deleted with it.
    pub async fn drop_resource(
        &self,
        external: &super::subject::External,
        resource: &serde_json::Value,
    ) -> anyhow::Result<()> {
        self.read_rows(ReadVia::Testctl(external), resource, "drop")
            .await
            .map(|_| ())
    }

    /// Run whichever reader the subject needs, returning its stdout.
    ///
    /// Configurations go in temporary files rather than on the command line, because an
    /// endpoint config carries credentials and an argument vector is readable by anyone who can
    /// list processes — which is also why `testctl` takes paths.
    async fn read_rows(
        &self,
        via: ReadVia<'_>,
        resource: &serde_json::Value,
        mode: &str,
    ) -> anyhow::Result<String> {
        let dir = tempfile::tempdir().context("creating a directory for the read's configs")?;
        let config_path = dir.path().join("config.json");
        let resource_path = dir.path().join("resource.json");

        let config = match via {
            ReadVia::Reference { config, .. } => config,
            ReadVia::Testctl(external) => &external.config,
        };
        std::fs::write(&config_path, config.to_string()).context("writing the read's config")?;
        std::fs::write(&resource_path, resource.to_string())
            .context("writing the read's resource")?;

        let mut cmd = match via {
            ReadVia::Reference { connector, .. } => {
                let mut cmd = async_process::Command::new(connector);
                cmd.arg("read")
                    .arg("--config")
                    .arg(&config_path)
                    .arg("--resource")
                    .arg(&resource_path);
                cmd
            }
            ReadVia::Testctl(external) => {
                let mut cmd = async_process::Command::new(&external.tool);
                cmd.arg("-connector")
                    .arg(&external.name)
                    .arg("-config")
                    .arg(&config_path)
                    .arg("-resource")
                    .arg(&resource_path)
                    .arg("-mode")
                    .arg(mode);
                cmd
            }
        };

        // Bounded because this spawns a process that talks to the endpoint: one that hangs
        // would otherwise sit under every deadline the scenario has.
        let output = tokio::time::timeout(Self::READ_TIMEOUT, async_process::output(&mut cmd))
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "reading {resource} did not finish within {}s",
                    Self::READ_TIMEOUT.as_secs(),
                )
            })?
            .with_context(|| format!("reading {resource}"))?;

        anyhow::ensure!(
            output.status.success(),
            "reading {resource} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim(),
        );
        String::from_utf8(output.stdout).context("the reader produced invalid UTF-8")
    }
}

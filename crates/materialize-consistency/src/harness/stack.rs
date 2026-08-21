//! Everything the harness needs from the local stack, through other people's binaries rather
//! than linked clients.
//!
//! `flowctl` for most of it: publishing, deleting, listing shards, reading a collection and
//! splitting a task are all already single subcommands whose output is JSON, and the auth
//! plumbing behind them — user tokens exchanged for data-plane authorizations — lives inside
//! `flowctl` and is not exported. Re-deriving it here would be a second copy of something with
//! no test of its own.
//!
//! Two things are not `flowctl`. Unassigning a shard and joining a task's shards go through
//! `gazctl`, via the scripts under `scripts/`, because neither is a `flowctl` subcommand. And
//! reading a destination back goes through the connector's own code — its `read` subcommand for
//! the reference connector, `testctl` for a real subject — see [`ReadVia`].

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

/// Run a command to completion under a deadline, returning its stdout.
///
/// `what` is used to name the invocation in returned Errs.
async fn bounded_output(
    cmd: &mut async_process::Command,
    timeout: std::time::Duration,
    what: &str,
) -> anyhow::Result<String> {
    let output = tokio::time::timeout(timeout, async_process::output(cmd))
        .await
        .map_err(|_| anyhow::anyhow!("{what} did not return within {}s", timeout.as_secs()))?
        .with_context(|| format!("running {what}"))?;

    anyhow::ensure!(
        output.status.success(),
        "{what} failed:\n{}",
        String::from_utf8_lossy(&output.stderr).trim(),
    );
    String::from_utf8(output.stdout).with_context(|| format!("{what} produced invalid UTF-8"))
}

/// Which reader a destination is read through.
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
    /// sits under all of them.
    ///
    /// Generous, because a publish on a loaded stack legitimately takes tens of
    /// seconds; the point is to convert "hangs forever" into a named failure.
    const INVOCATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(150);

    /// For invocations whose duration scales with how much data a run produced.
    /// Scenarios that run for long, also end up reading a lot.
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
        bounded_output(
            self.command().args(args),
            timeout,
            &format!("flowctl {args:?}"),
        )
        .await
    }

    /// Publish a catalog, retrying a build that fails for reasons outside the
    /// catalog.
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
            .context(super::Environment::PublishFailed)
            .context("publishing the scenario's catalog")
    }

    pub async fn delete_prefix(&self, prefix: &str) -> anyhow::Result<()> {
        let prefix = format!("{}/", prefix.trim_end_matches('/'));
        self.run(&[
            "catalog",
            "delete",
            "--prefix",
            &prefix,
            "--dangerous-auto-approve",
        ])
        .await
        .with_context(|| format!("deleting {prefix}"))?;
        Ok(())
    }

    /// Every shard of a task, as the data plane reports it.
    async fn shards(
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
    pub async fn all_primary(&self, task: &str) -> anyhow::Result<bool> {
        use proto_gazette::consumer::replica_status::Code;

        let shards = self.shards(task).await?;

        Ok(!shards.is_empty()
            && shards
                .iter()
                .all(|s| s.status.iter().any(|s| s.code() == Code::Primary)))
    }

    /// Whether `task` has stopped writing: every shard disabled in the data plane, and none
    /// of them still primary.
    pub async fn is_stopped(&self, task: &str) -> anyhow::Result<bool> {
        use proto_gazette::consumer::replica_status::Code;

        let shards = self.shards(task).await?;

        // A disabled task keeps its shard specs, so an empty listing here is not the expected
        // steady state.
        Ok(!shards.is_empty()
            && shards.iter().all(|shard| {
                shard.spec.as_ref().is_some_and(|spec| spec.disable)
                    && !shard.status.iter().any(|s| s.code() == Code::Primary)
            }))
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
    pub async fn read_collection_when_final(
        &self,
        collection: &str,
        timeout: std::time::Duration,
    ) -> anyhow::Result<Vec<Event>> {
        // The caller waits for the capture's shards to be gone first, after which nothing can
        // append to this collection; this loop guards against an inconsistent read — a
        // `collections read` that returns mid-append, or against a broker still catching up.
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
    /// This is how a crash scenario recovers: a shard whose processing loop fails is
    /// marked FAILED and the allocator will not reschedule it on its own.
    ///
    /// Every shard, not only the failed ones: `--failed` skips a shard whose primary is
    /// merely wedged without being marked FAILED, which is exactly the state the stall
    /// detection in `recover` fires on. Unassigning a healthy shard costs only a brief
    /// reassignment.
    pub async fn unassign_shards(&self, task: &str) -> anyhow::Result<()> {
        self.shard_tool(&["unassign", task])
            .await
            .with_context(|| format!("unassigning shards of {task}"))?;
        Ok(())
    }

    /// Split every shard of a task in two, on shuffled key.
    pub async fn split_shards(&self, task: &str) -> anyhow::Result<()> {
        self.run(&["raw", "split-shards", "--task", task])
            .await
            .with_context(|| format!("splitting shards of {task}"))?;
        Ok(())
    }

    /// Join every pair of a task's shards, halving their number: a survivor absorbs its
    /// partner's key range and the partner is deleted.
    pub async fn join_shards(&self, task: &str) -> anyhow::Result<()> {
        self.shard_tool(&["join", task])
            .await
            .with_context(|| format!("joining shards of {task}"))?;
        Ok(())
    }

    /// Run the suite's shard tooling, which drives `gazctl` rather than `flowctl`. Why these
    /// are scripts rather than new `flowctl` subcommands is recorded in
    /// `docs/materialize/consistency-testing.md`.
    async fn shard_tool(&self, args: &[&str]) -> anyhow::Result<String> {
        let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("scripts")
            .join("shard-tools.sh");

        let mut cmd = async_process::Command::new(&script);
        cmd.args(args);
        cmd.env("FLOWCTL", &self.flowctl);
        cmd.env("FLOW_AUTH_TOKEN", &self.auth_token);
        cmd.env("SSL_CERT_FILE", &self.ca_cert);
        cmd.env("FLOWCTL_PROFILE", &self.name);

        bounded_output(
            &mut cmd,
            Self::INVOCATION_TIMEOUT,
            &format!("{script:?} {args:?}"),
        )
        .await
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
        bounded_output(&mut cmd, Self::READ_TIMEOUT, &format!("reading {resource}")).await
    }
}

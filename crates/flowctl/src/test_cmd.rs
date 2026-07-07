//! `flowctl test` — run a catalog's tests locally on the runtime-next stack via
//! the `runtime-harness` crate. No Gazette broker, etcd, Go consumer, or
//! `flowctl-go` binary is involved; derivations run as resident runtime-next
//! sessions (derive-sqlite in-process, image derivations as containers), and
//! tests execute against an in-memory collection store.
//!
//! This is a local command (there is no control-plane round-trip). It is
//! unrelated to `flowctl catalog test`, which is a remote dry-run publish.

use anyhow::Context;
use std::io::IsTerminal;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Test {
    /// Path or URL to a Flow specification file.
    #[clap(long)]
    source: String,
    /// Docker network to run connector images (for image derivations).
    #[clap(long, default_value = "bridge")]
    network: String,
    /// When set, failed verifications write their actual documents as
    /// `{snapshot}/{test}/verify-{step}.json`, for review or as a baseline.
    #[clap(long)]
    snapshot: Option<std::path::PathBuf>,
    /// Number of shards to activate for image derivations, to exercise
    /// multi-shard key routing. derive-sqlite always runs single-shard.
    #[clap(long, default_value = "3")]
    splits: u32,
    /// Emit connector and runtime logs as JSON to stderr (rather than through
    /// the tracing subscriber).
    #[clap(long, action)]
    log_json: bool,
}

impl Test {
    pub async fn run(&self, _ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        let log_handler: fn(&::ops::Log) = if self.log_json {
            ::ops::stderr_log_handler
        } else {
            ::ops::tracing_log_handler
        };

        // Build to `Validations` (including built tests) fully locally — no
        // control-plane round-trip, matching `flowctl-go test`. Derivation
        // connectors are validated; captures / materializations are not, as
        // tests never run them.
        let source_url = build::arg_source_to_url(&self.source, false)?;
        let built = build::for_catalog_test(&source_url, &self.network, log_handler)
            .await
            .into_result()
            .map_err(|errors| {
                for tables::Error { scope, error } in errors.iter() {
                    tracing::error!(%scope, ?error);
                }
                anyhow::anyhow!("catalog build failed with {} error(s)", errors.len())
            })
            .context("building catalog for testing")?;

        let options = runtime_harness::Options {
            network: self.network.clone(),
            splits: self.splits,
            snapshot_dir: self.snapshot.clone(),
            log_handler: std::sync::Arc::new(log_handler),
            // `flowctl test` runs every connector locally; only the agent's
            // publication-test path offloads to remote data planes.
            remote_connectors: None,
        };

        let results = runtime_harness::run_tests(&built.built, options).await?;
        render(&results);

        if results.all_passed() {
            Ok(())
        } else {
            anyhow::bail!(
                "{} of {} tests failed",
                results.failed(),
                results.outcomes.len()
            )
        }
    }
}

/// Print a V1-style summary: one line per test, then a tally.
fn render(results: &runtime_harness::TestResults) {
    let color = std::io::stdout().is_terminal();

    println!("Running {} tests...", results.outcomes.len());
    for outcome in &results.outcomes {
        let (path, ptr) = scope_to_path_and_ptr(&outcome.scope);
        match &outcome.error {
            None => println!("✔️  {} :: {}", path, green(&outcome.name, color)),
            Some(err) => {
                println!(
                    "❌ {} failure at step {} :",
                    yellow(&path, color),
                    red(&ptr, color)
                );
                println!("{err}");
            }
        }
    }
    println!(
        "\nRan {} tests, {} passed, {} failed",
        results.outcomes.len(),
        results.passed(),
        results.failed()
    );
}

/// Split a step scope (`file:///path/to/spec#/json/pointer`) into a display path
/// and its JSON-pointer fragment.
fn scope_to_path_and_ptr(scope: &str) -> (String, String) {
    match scope.split_once('#') {
        Some((path, ptr)) => (
            path.strip_prefix("file://").unwrap_or(path).to_string(),
            ptr.to_string(),
        ),
        None => (scope.to_string(), String::new()),
    }
}

fn green(s: &str, color: bool) -> String {
    paint(s, "32", color)
}
fn red(s: &str, color: bool) -> String {
    paint(s, "31", color)
}
fn yellow(s: &str, color: bool) -> String {
    paint(s, "33", color)
}
fn paint(s: &str, code: &str, color: bool) -> String {
    if color {
        format!("\x1b[{code}m{s}\x1b[0m")
    } else {
        s.to_string()
    }
}

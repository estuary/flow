//! `run_tests`: the harness entry point, ported from `go/flowctl-go/cmd-api-test.go`.
//!
//! It loads the built `TestSpec`s, starts a resident [`DerivationRunner`] for
//! every enabled derivation, and runs each test case (sorted by step scope)
//! through the ported [`run_test_case`](crate::action::run_test_case) loop —
//! resetting connector state between cases via the runtime-next Reset flow.
//! Ingest / Verify steps are executed by [`crate::steps`]; the scheduler cascade
//! is driven by [`LiveDriver`].
//!
//! Shard counts match V1's publication-test path: derive-sqlite (and any
//! remote-authoritative connector, whose checkpoint lives in its endpoint) runs
//! single-shard, while image derivations run with `Options::splits` shards
//! (default three) to exercise multi-shard key routing.

use crate::action::{Driver, run_test_case};
use crate::clock::Clock;
use crate::graph::{Graph, PendingStat, TestTime};
use crate::runner::DerivationRunner;
use crate::steps;
use crate::store::CollectionStore;
use anyhow::Context;
use proto_flow::flow::{CollectionSpec, TestSpec, collection_spec::derivation::ConnectorType};
use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

/// Options controlling a catalog-test run.
pub struct Options {
    /// Docker network for image connectors (empty for the default).
    pub network: String,
    /// Shards to activate for image (non-remote-authoritative) derivations, to
    /// exercise multi-shard key routing. V1 uses three.
    pub splits: u32,
    /// When set, failed verifications write their actual documents as
    /// `{dir}/{test}/verify-{step}.json` (parity with `--snapshot`).
    pub snapshot_dir: Option<std::path::PathBuf>,
    /// Sink for connector / runtime ops logs (the agent path feeds `logs_tx`).
    pub log_handler: crate::logger::LogHandler,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            network: String::new(),
            splits: 3,
            snapshot_dir: None,
            log_handler: std::sync::Arc::new(ops::tracing_log_handler),
        }
    }
}

/// The outcome of one test case.
pub struct TestOutcome {
    /// Test name.
    pub name: String,
    /// Scope of the reached step (the failing step on failure, else the first
    /// step) — a source URL with a JSON-pointer fragment, for path rendering.
    pub scope: String,
    /// Rendered failure (verify diff or execution error); `None` on success.
    pub error: Option<String>,
}

impl TestOutcome {
    pub fn passed(&self) -> bool {
        self.error.is_none()
    }
}

/// Results of a full catalog-test run, in execution (scope) order.
pub struct TestResults {
    pub outcomes: Vec<TestOutcome>,
}

impl TestResults {
    pub fn passed(&self) -> usize {
        self.outcomes.iter().filter(|o| o.passed()).count()
    }
    pub fn failed(&self) -> usize {
        self.outcomes.iter().filter(|o| !o.passed()).count()
    }
    pub fn all_passed(&self) -> bool {
        self.outcomes.iter().all(TestOutcome::passed)
    }
}

/// Run all catalog tests in `built` and return their per-case outcomes.
///
/// The caller is responsible for installing a process-level rustls
/// [`CryptoProvider`](https://docs.rs/rustls) before calling — the runtime-next
/// loopback stack dials over rustls (`flowctl` and the agent install one at
/// startup).
pub async fn run_tests(
    built: &tables::Validations,
    options: Options,
) -> anyhow::Result<TestResults> {
    let collections: BTreeMap<String, CollectionSpec> = built
        .built_collections
        .iter()
        .filter_map(|bc| bc.spec.as_ref().map(|s| (s.name.clone(), s.clone())))
        .collect();

    // Run test cases ordered by their first step's scope, which implicitly
    // orders on resource file then test name (matching V1).
    let mut tests: Vec<TestSpec> = built
        .built_tests
        .iter()
        .filter_map(|bt| bt.spec.clone())
        .collect();
    tests.sort_by(|a, b| step_scope(a).cmp(step_scope(b)));

    let store = Arc::new(Mutex::new(CollectionStore::new()));
    let clock = Arc::new(AtomicU64::new(1));
    let mut graph =
        Graph::from_built_collections(&collections.values().cloned().collect::<Vec<_>>());

    let mut runners = start_runners(&collections, &store, &clock, &options).await?;

    let mut outcomes = Vec::with_capacity(tests.len());
    for test in &tests {
        // Run the case; the driver borrows `runners` for its duration.
        let (result, failure, last_scope) = {
            let mut driver = LiveDriver {
                runners: &mut runners,
                store: store.clone(),
                clock: clock.clone(),
                collections: &collections,
                failure: None,
                last_scope: step_scope(test).to_string(),
            };
            let result = run_test_case(&mut graph, &mut driver, test).await;
            (result, driver.failure.take(), driver.last_scope)
        };

        let outcome = match result {
            Ok(_) => TestOutcome {
                name: test.name.clone(),
                scope: step_scope(test).to_string(),
                error: None,
            },
            Err(err) => {
                if let (Some(dir), Some(failure)) = (&options.snapshot_dir, &failure) {
                    write_snapshot(dir, &test.name, failure)
                        .context("writing verification snapshot")?;
                }
                TestOutcome {
                    name: test.name.clone(),
                    scope: last_scope,
                    error: Some(format!("{err:#}")),
                }
            }
        };
        outcomes.push(outcome);

        // Reset connector state between cases (V1 resets after every case).
        for runner in runners.values_mut() {
            runner
                .reset()
                .await
                .with_context(|| format!("resetting state after test {}", test.name))?;
        }
    }

    // Gracefully stop every resident session.
    for (_, runner) in runners {
        runner
            .shutdown()
            .await
            .context("shutting down derivation session")?;
    }

    Ok(TestResults { outcomes })
}

/// Start a resident [`DerivationRunner`] for every enabled derivation, choosing
/// its shard count by connector authority.
async fn start_runners(
    collections: &BTreeMap<String, CollectionSpec>,
    store: &Arc<Mutex<CollectionStore>>,
    clock: &Arc<AtomicU64>,
    options: &Options,
) -> anyhow::Result<BTreeMap<String, DerivationRunner>> {
    let registry = service_kit::Registry::new();
    let mut runners = BTreeMap::new();

    for (name, spec) in collections {
        let Some(derivation) = &spec.derivation else {
            continue;
        };
        if derivation
            .shard_template
            .as_ref()
            .map(|s| s.disable)
            .unwrap_or(false)
        {
            continue; // Disabled tasks don't run (matching the graph).
        }

        // Remote-authoritative connectors (derive-sqlite) report connector
        // checkpoint state at Opened and must be single-shard; image derivations
        // run with `splits` shards to exercise multi-shard key routing.
        let single_shard = derivation.connector_type == ConnectorType::Sqlite as i32;
        let n_shards = if single_shard {
            1
        } else {
            options.splits.max(1)
        };

        let runner = DerivationRunner::start(
            spec,
            n_shards,
            options.network.clone(),
            registry.clone(),
            store.clone(),
            clock.clone(),
            options.log_handler.clone(),
        )
        .await
        .with_context(|| format!("starting derivation session for {name}"))?;
        runners.insert(name.clone(), runner);
    }

    Ok(runners)
}

/// The first step's scope of a test (empty for a test with no steps).
fn step_scope(test: &TestSpec) -> &str {
    test.steps
        .first()
        .map(|s| s.step_scope.as_str())
        .unwrap_or("")
}

/// A captured verification failure, for reporting and snapshotting.
struct VerifyFailure {
    test_step: usize,
    actuals: Vec<serde_json::Value>,
}

/// Write a failed verification's actual documents to `{dir}/{test}/verify-{step}.json`
/// (parity with V1's `--snapshot`).
fn write_snapshot(
    dir: &std::path::Path,
    test_name: &str,
    failure: &VerifyFailure,
) -> anyhow::Result<()> {
    let test_dir = dir.join(test_name);
    std::fs::create_dir_all(&test_dir)
        .with_context(|| format!("creating snapshot directory {test_dir:?}"))?;
    let path = test_dir.join(format!("verify-{}.json", failure.test_step));
    let json = serde_json::to_string_pretty(&failure.actuals)
        .expect("serializing actual documents cannot fail");
    std::fs::write(&path, json).with_context(|| format!("writing snapshot {path:?}"))?;
    tracing::warn!(?path, "wrote verification snapshot");
    Ok(())
}

/// Drives the scheduler's Stat / Ingest / Verify / Advance against the resident
/// runners and the collection store.
struct LiveDriver<'a> {
    runners: &'a mut BTreeMap<String, DerivationRunner>,
    store: Arc<Mutex<CollectionStore>>,
    clock: Arc<AtomicU64>,
    collections: &'a BTreeMap<String, CollectionSpec>,
    /// Set when a verify fails, for snapshotting.
    failure: Option<VerifyFailure>,
    /// Scope of the most-recently-executed step, for failure reporting.
    last_scope: String,
}

impl Driver for LiveDriver<'_> {
    async fn stat(&mut self, stat: &PendingStat) -> anyhow::Result<(Clock, Clock)> {
        let runner = self
            .runners
            .get_mut(&stat.task_name)
            .with_context(|| format!("no resident session for task {}", stat.task_name))?;
        runner.stat(stat).await
    }

    async fn ingest(&mut self, test: &TestSpec, test_step: usize) -> anyhow::Result<Clock> {
        let step = &test.steps[test_step];
        self.last_scope = step.step_scope.clone();
        let collection = self
            .collections
            .get(&step.collection)
            .with_context(|| format!("unknown collection {}", step.collection))?;
        steps::ingest(&self.store, &self.clock, collection, &step.docs_json_vec)
    }

    async fn verify(
        &mut self,
        test: &TestSpec,
        test_step: usize,
        from: &Clock,
        to: &Clock,
    ) -> anyhow::Result<()> {
        let step = &test.steps[test_step];
        self.last_scope = step.step_scope.clone();
        let collection = self
            .collections
            .get(&step.collection)
            .with_context(|| format!("unknown collection {}", step.collection))?;

        let outcome = steps::verify(&self.store, collection, step, from, to)?;
        if outcome.failures.is_empty() {
            return Ok(());
        }

        let rendered = crate::diff::render_failures(&outcome.failures);
        self.failure = Some(VerifyFailure {
            test_step,
            actuals: outcome.actuals,
        });
        anyhow::bail!(rendered);
    }

    async fn advance(&mut self, _delta: TestTime) -> anyhow::Result<()> {
        // Read delays are realized by the scheduler withholding a delayed stat's
        // documents until synthetic time reaches its `ReadyAt`; the runner needs
        // no wall clock, so there is nothing to advance here.
        Ok(())
    }
}

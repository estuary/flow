//! `run_tests`: the crate's entry point.
//!
//! It loads the built `TestSpec`s, starts a resident [`DerivationSession`] for
//! every enabled derivation, and runs each test case (sorted by step scope)
//! through the [`crate::scheduler::run_test_case`] loop, with Ingest / Verify steps
//! executed by [`crate::steps`]. Connector state is reset after every case
//! including the last, so no case can observe another's, and a failing case is
//! recorded rather than aborting the run. A failure can kill its session
//! outright — which a Reset cannot revive — in which case the remaining cases
//! are reported as not-run rather than discarding the outcomes already in hand.
//!
//! Remote-authoritative derivations (derive-sqlite, whose checkpoint lives in
//! its endpoint) run single-shard, while image derivations run with
//! `Options::splits` shards to exercise multi-shard key routing.

use crate::clock::Clock;
use crate::graph::{Graph, PendingRead, TestTime};
use crate::scheduler::{Driver, run_test_case};
use crate::session::DerivationSession;
use crate::steps;
use crate::store::CollectionStore;
use anyhow::Context;
use proto_flow::flow::{CollectionSpec, TestSpec, collection_spec::derivation::ConnectorType};
use std::collections::BTreeMap;
use std::sync::atomic::AtomicI32;
use std::sync::{Arc, Mutex};

/// A clonable ops-log sink. The user-visible logs of a run — connector stderr
/// and runtime events — flow through it.
pub type LogHandler = Arc<dyn Fn(&ops::Log) + Send + Sync>;

/// Options controlling a catalog-test run.
pub struct Options {
    /// Docker network for image connectors (empty for the default).
    pub network: String,
    /// Shards to activate for image (non-remote-authoritative) derivations.
    pub splits: u32,
    /// Sink for connector / runtime ops logs (the agent path feeds `logs_tx`).
    /// Each task's own `shards: {logLevel}` decides what reaches it; see
    /// [`logger_factory`].
    pub log_handler: LogHandler,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            network: String::new(),
            splits: 3,
            log_handler: Arc::new(ops::tracing_log_handler),
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
    /// The case's disposition.
    pub status: TestStatus,
}

/// The disposition of one test case.
#[derive(Debug)]
pub enum TestStatus {
    Passed,
    /// The case failed, with its rendered verify diff or execution error.
    Failed {
        error: String,
    },
    /// The case never ran: an earlier case's failure left a session that could
    /// not be Reset, ending the run. `reason` names that earlier case.
    NotRun {
        reason: String,
    },
}

impl TestOutcome {
    pub fn passed(&self) -> bool {
        matches!(self.status, TestStatus::Passed)
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
        self.outcomes
            .iter()
            .filter(|o| matches!(o.status, TestStatus::Failed { .. }))
            .count()
    }
    pub fn not_run(&self) -> usize {
        self.outcomes
            .iter()
            .filter(|o| matches!(o.status, TestStatus::NotRun { .. }))
            .count()
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
    // orders on resource file then test name: a stable, source-order execution
    // that makes a run's output diffable from one invocation to the next.
    let mut tests: Vec<TestSpec> = built
        .built_tests
        .iter()
        .filter_map(|bt| bt.spec.clone())
        .collect();
    tests.sort_by(|a, b| step_scope(a).cmp(step_scope(b)));

    let store = Arc::new(Mutex::new(CollectionStore::new()));
    let mut graph =
        Graph::from_built_collections(&collections.values().cloned().collect::<Vec<_>>())?;

    let mut sessions = start_sessions(&collections, &store, &options).await?;

    // From here on every exit must pass through `shutdown_all`: a resident
    // session's shard tasks hold RocksDB open inside the run's tempdir, and
    // dropping sessions without stopping them lets the tempdir vanish underneath
    // a live RocksDB, which aborts the process.
    let result = run_cases(&mut graph, &mut sessions, &collections, &store, &tests).await;
    let shutdown = shutdown_all(sessions).await;

    let outcomes = result?;
    if let Err(shutdown_err) = shutdown {
        if outcomes
            .iter()
            .any(|o| matches!(o.status, TestStatus::Failed { .. }))
        {
            // A session dead at shutdown is a consequence of a failure already
            // recorded in `outcomes`, which must not be discarded for this
            // teardown symptom (its cause repeats what the outcome carries).
            tracing::warn!(err = ?shutdown_err, "session shutdown error after failed test cases");
        } else {
            return Err(shutdown_err);
        }
    }

    Ok(TestResults { outcomes })
}

/// Run each test case in order, resetting connector state after every one.
async fn run_cases(
    graph: &mut Graph,
    sessions: &mut BTreeMap<String, DerivationSession>,
    collections: &BTreeMap<String, CollectionSpec>,
    store: &Arc<Mutex<CollectionStore>>,
    tests: &[TestSpec],
) -> anyhow::Result<Vec<TestOutcome>> {
    let mut outcomes = Vec::with_capacity(tests.len());

    for (index, test) in tests.iter().enumerate() {
        let (result, last_scope) = {
            let mut driver = LiveDriver {
                sessions,
                store: store.clone(),
                collections,
                last_scope: step_scope(test).to_string(),
            };
            let result = run_test_case(graph, &mut driver, test).await;
            (result, driver.last_scope)
        };
        let case_failed = result.is_err();

        outcomes.push(match result {
            Ok(_) => TestOutcome {
                name: test.name.clone(),
                scope: step_scope(test).to_string(),
                status: TestStatus::Passed,
            },
            Err(err) => TestOutcome {
                name: test.name.clone(),
                scope: last_scope,
                status: TestStatus::Failed {
                    error: format!("{err:#}"),
                },
            },
        });

        // Reset connector state after every case, including the last.
        let mut reset_result = Ok(());
        for session in sessions.values_mut() {
            reset_result = session
                .reset()
                .await
                .with_context(|| format!("resetting state after test {}", test.name));
            if reset_result.is_err() {
                break;
            }
        }

        match reset_result {
            Ok(()) => {}
            Err(reset_err) if case_failed => {
                // A session dead at Reset is a consequence of the failure just
                // recorded, and that outcome — not this teardown symptom — is
                // what the caller must see. Stop here and report what didn't run.
                tracing::warn!(
                    err = ?reset_err,
                    test = %test.name,
                    "sessions cannot be reset after a failed test case; stopping the run",
                );
                let reason = format!("a session failed during test {}", test.name);
                outcomes.extend(tests[index + 1..].iter().map(|not_run| TestOutcome {
                    name: not_run.name.clone(),
                    scope: step_scope(not_run).to_string(),
                    status: TestStatus::NotRun {
                        reason: reason.clone(),
                    },
                }));
                break;
            }
            // The case passed and Reset still failed: a genuine runtime fault
            // with no user-attributable cause to fold it into.
            Err(reset_err) => return Err(reset_err),
        }
    }

    Ok(outcomes)
}

/// Gracefully stop every resident session, reporting the first failure but
/// always attempting all of them.
async fn shutdown_all(sessions: BTreeMap<String, DerivationSession>) -> anyhow::Result<()> {
    let mut first_err = None;

    for (name, session) in sessions {
        match session.shutdown().await {
            Ok(()) => {}
            Err(err) if first_err.is_none() => {
                first_err = Some(err.context(format!("shutting down session for {name}")));
            }
            Err(err) => tracing::warn!(?err, %name, "secondary session shutdown error"),
        }
    }

    match first_err {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

/// Start a resident [`DerivationSession`] for every enabled derivation, choosing
/// its shard count by connector authority.
async fn start_sessions(
    collections: &BTreeMap<String, CollectionSpec>,
    store: &Arc<Mutex<CollectionStore>>,
    options: &Options,
) -> anyhow::Result<BTreeMap<String, DerivationSession>> {
    let registry = service_kit::Registry::new();
    let mut sessions = BTreeMap::new();

    for (name, spec) in collections {
        let Some(derivation) = &spec.derivation else {
            continue;
        };
        let shard_template = derivation
            .shard_template
            .as_ref()
            .with_context(|| format!("derivation {name} has no shard template"))?;

        if shard_template.disable {
            continue; // Disabled tasks don't run (matching the graph).
        }
        let logger_factory = logger_factory(&options.log_handler, shard_template)
            .with_context(|| format!("building logger for derivation {name}"))?;

        // Remote-authoritative connectors (derive-sqlite) report checkpoint
        // state at Opened, and must be single-shard.
        let single_shard = derivation.connector_type == ConnectorType::Sqlite as i32;
        let n_shards = if single_shard {
            1
        } else {
            options.splits.max(1)
        };

        let started = DerivationSession::start(
            spec,
            n_shards,
            options.network.clone(),
            registry.clone(),
            store.clone(),
            logger_factory,
        )
        .await;

        match started {
            Ok(session) => {
                sessions.insert(name.clone(), session);
            }
            Err(err) => {
                // Stop the sessions already running before returning; dropping
                // them instead aborts the process (see `run_tests`).
                _ = shutdown_all(sessions).await;
                return Err(err).with_context(|| format!("starting derivation session for {name}"));
            }
        }
    }

    Ok(sessions)
}

/// One derivation's logger seam: `log_handler`, gated by the task's own
/// `shards: {logLevel}` as carried by its shard template's
/// `estuary.dev/log-level` label.
fn logger_factory(
    log_handler: &LogHandler,
    shard_template: &proto_gazette::consumer::ShardSpec,
) -> anyhow::Result<impl runtime_next::LoggerFactory> {
    let labels = shard_template
        .labels
        .as_ref()
        .context("shard template has no label set")?;

    let level = labels::expect_one(labels, labels::LOG_LEVEL)?;
    let level = ops::LogLevel::from_str_name(level)
        .with_context(|| format!("invalid {} label {level:?}", labels::LOG_LEVEL))?;

    // `Arc<dyn Fn>` isn't itself an `Fn`, so wrap it in a closure which is (and
    // is Clone + Send + Sync, as the factory requires).
    let log_handler = log_handler.clone();

    Ok(runtime_next::FnLoggerFactory::new(
        move |log: &ops::Log| (log_handler)(log),
        // Nothing moves this level once set: `start_sessions` passes no
        // `set_log_level` to `runtime_next::shard::Service`.
        Arc::new(AtomicI32::new(level as i32)),
    ))
}

/// The first step's scope of a test (empty for a test with no steps).
fn step_scope(test: &TestSpec) -> &str {
    test.steps
        .first()
        .map(|s| s.step_scope.as_str())
        .unwrap_or("")
}

/// Drives the scheduler's Read / Ingest / Verify / Advance against the resident
/// sessions and the collection store.
struct LiveDriver<'a> {
    sessions: &'a mut BTreeMap<String, DerivationSession>,
    store: Arc<Mutex<CollectionStore>>,
    collections: &'a BTreeMap<String, CollectionSpec>,
    /// Scope of the most-recently-executed step, for failure reporting.
    last_scope: String,
}

impl Driver for LiveDriver<'_> {
    fn begin_transaction(&mut self) {
        self.store.lock().unwrap().begin_transaction();
    }

    async fn read(&mut self, read: &PendingRead) -> anyhow::Result<(Clock, Clock)> {
        let session = self
            .sessions
            .get_mut(&read.derivation)
            .with_context(|| format!("no resident session for derivation {}", read.derivation))?;
        session.read(read).await
    }

    async fn ingest(&mut self, test: &TestSpec, test_step: usize) -> anyhow::Result<Clock> {
        let step = &test.steps[test_step];
        self.last_scope = step.step_scope.clone();
        let collection = self
            .collections
            .get(&step.collection)
            .with_context(|| format!("unknown collection {}", step.collection))?;
        steps::ingest(&self.store, collection, &step.docs_json_vec)
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

        let failures = steps::verify(&self.store, collection, step, from, to)?;
        if failures.is_empty() {
            return Ok(());
        }
        anyhow::bail!(crate::diff::render_failures(&failures));
    }

    async fn advance(&mut self, _delta: TestTime) -> anyhow::Result<()> {
        // Synthetic time lives entirely in the graph, which withholds a delayed
        // read until its `ready_at`. The session has no clock to advance.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::logger_factory;
    use runtime_next::{Logger, LoggerFactory};
    use std::sync::{Arc, Mutex};

    /// Levels which reach the sink for a task labeled `logLevel`.
    fn forwarded(log_level: &str) -> Vec<ops::LogLevel> {
        let shard_template = proto_gazette::consumer::ShardSpec {
            labels: Some(labels::build_set([(labels::LOG_LEVEL, log_level)])),
            ..Default::default()
        };

        let seen: Arc<Mutex<Vec<ops::LogLevel>>> = Default::default();
        let logger = {
            let seen = seen.clone();
            let log_handler: super::LogHandler = Arc::new(move |log: &ops::Log| {
                seen.lock().unwrap().push(log.level());
            });
            logger_factory(&log_handler, &shard_template)
                .unwrap()
                .open("acmeCo/a-task")
        };

        for level in [
            ops::LogLevel::Error,
            ops::LogLevel::Warn,
            ops::LogLevel::Info,
            ops::LogLevel::Debug,
            ops::LogLevel::Trace,
        ] {
            logger.log(&ops::Log {
                level: level as i32,
                message: "hello".to_string(),
                ..Default::default()
            });
        }

        let seen = seen.lock().unwrap();
        seen.clone()
    }

    #[test]
    fn task_log_level_gates_the_sink() {
        // "info" is what a build labels a task which declares no `shards`.
        assert_eq!(
            forwarded("info"),
            vec![
                ops::LogLevel::Error,
                ops::LogLevel::Warn,
                ops::LogLevel::Info,
            ]
        );

        // Raising the task is how a user gets its debug output into
        // `flowctl raw test`'s tracing stream or a publication's job logs.
        assert_eq!(
            forwarded("debug"),
            vec![
                ops::LogLevel::Error,
                ops::LogLevel::Warn,
                ops::LogLevel::Info,
                ops::LogLevel::Debug,
            ]
        );
    }
}

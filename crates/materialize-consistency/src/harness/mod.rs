//! The scenario runner: publish, perturb, quiesce, verify, clean up.

pub mod catalog;
pub mod stack;

use crate::invariants::{self, Expectation, Invariant, Violation};
use crate::protocol::{Event, RunDir, TraceEvent, Trigger};
use crate::scenarios::{Scenario, Subject};
use anyhow::Context;
use std::io::BufRead;

/// An invariant a connector is not held to, and why.
///
/// The compliance model is default-strict: every connector is held to every
/// invariant, and anything weaker is an explicit entry here. The rejected
/// alternative was having each connector declare its class and running only that
/// class's invariants — under which the cheapest way to make a failing test pass
/// is to downgrade the claim, and a connector that silently regresses to a weaker
/// class gets reclassified and passes. This way the pressure runs the other way,
/// and the set of exemptions reads as a map of where the fleet is actually weak.
#[derive(serde::Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Exemption {
    pub invariant: Invariant,
    /// Why this is a reviewed property of the destination rather than a defect.
    /// Required, and not defaulted: an exemption without a rationale is a defect
    /// with better paperwork.
    pub justification: String,
    /// What the exemption covers. A connector's class is not always a per-connector
    /// constant — a delta-updates binding is push-only even inside a
    /// post-commit-apply connector, and enabling scale-out changes the contract — so
    /// an exemption that could not be narrowed would overstate the weakness.
    #[serde(default)]
    pub scope: Scope,
}

#[derive(serde::Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum Scope {
    /// Every binding, under every configuration.
    #[default]
    Connector,
    /// Only bindings materializing combined delta updates.
    DeltaBindings,
    /// Only when these feature flags are set.
    FeatureFlags(Vec<String>),
}

impl Exemption {
    /// Load a connector's exemptions.
    ///
    /// They live beside the connector rather than in the harness, so that a
    /// weakened guarantee shows up in review of the connector that weakened it. The
    /// file is JSON rather than YAML only because the harness has no YAML
    /// dependency; it holds a bare array of exemptions.
    pub fn load(path: &std::path::Path) -> anyhow::Result<Vec<Self>> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading exemptions {path:?}"))?;

        let exemptions: Vec<Self> = serde_json::from_str(&raw)
            .with_context(|| format!("parsing exemptions {path:?}"))?;

        for exemption in &exemptions {
            anyhow::ensure!(
                exemption.justification.trim().len() >= 40,
                "the exemption for {} in {path:?} has no real justification. An exemption \
                 records a reviewed property of the destination, so say which property and \
                 why it is inherent rather than a defect.",
                exemption.invariant,
            );
        }
        Ok(exemptions)
    }
}

/// What a run produced.
pub struct Outcome {
    pub scenario: &'static str,
    /// Violations that were not exempted.
    pub violations: Vec<Violation>,
    /// Violations an exemption suppressed, kept so a run can report what it chose
    /// not to hold the connector to.
    pub exempted: Vec<Violation>,
    /// Faults the shim reports having injected. A scenario whose fault never
    /// fired proved nothing, so the runner refuses to pass one.
    pub faults_fired: usize,
    pub documents: usize,
    /// Where the shim's trace and the destination were left. Retained on failure
    /// and removed on success — a caller that *expected* the failure (the defective
    /// half of every scenario) removes it itself.
    pub run_dir: std::path::PathBuf,
}

impl Outcome {
    pub fn passed(&self) -> bool {
        self.violations.is_empty()
    }

    /// A one-line verdict naming the invariant, the scenario, and the fault, so a
    /// failure says what broke rather than merely that something did.
    pub fn summary(&self) -> String {
        if self.violations.is_empty() {
            return format!(
                "{}: upheld every invariant over {} documents ({} faults injected)",
                self.scenario, self.documents, self.faults_fired
            );
        }
        let mut lines = vec![format!(
            "{}: {} violation(s) over {} documents ({} faults injected)",
            self.scenario,
            self.violations.len(),
            self.documents,
            self.faults_fired
        )];
        // Bounded: a lost account produces a violation per account, and the first
        // few say everything the rest would.
        for violation in self.violations.iter().take(10) {
            lines.push(format!("  {violation}"));
        }
        if self.violations.len() > 10 {
            lines.push(format!("  … and {} more", self.violations.len() - 10));
        }
        lines.join("\n")
    }
}

/// A suffix distinguishing this run's catalog names and destination from every
/// other run's, including concurrent ones on the same stack.
///
/// Process id and nanoseconds rather than a random number: two runs in one test
/// binary differ in the clock, two binaries differ in the pid, and a catalog name
/// admits only a limited alphabet anyway.
fn run_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or_default();

    format!("{:x}{:x}", std::process::id(), nanos)
}

/// Run one scenario against one subject.
pub async fn run(scenario: &Scenario, subject: &Subject) -> anyhow::Result<Outcome> {
    let stack = stack::Stack::from_env()?;
    let run_id = run_id();

    let names = catalog::Names::new(&stack.tenant, scenario.name, &run_id);
    let run_dir = stack
        .stack_dir
        .join("consistency")
        .join(format!("{}-{run_id}", scenario.name));
    std::fs::create_dir_all(&run_dir)
        .with_context(|| format!("creating run directory {run_dir:?}"))?;

    let shim = stack.binary("consistency-shim")?;
    let capture = capture_command()?;

    // The destination lives in the run directory, so it is deleted with the rest
    // of the run's debris and two concurrent runs cannot see each other's rows.
    let mut config = subject.config.clone();
    config["path"] = serde_json::json!(run_dir.join("destination.sqlite").to_string_lossy());

    let subject_config = catalog::Subject {
        connector: subject.connector.clone(),
        config,
    };
    let workload = catalog::Workload::default();

    let plan = catalog::Plan {
        names: &names,
        subject: &subject_config,
        shim: &shim,
        capture: &capture,
        run_dir: &run_dir,
        faults: &scenario.faults,
        workload: &workload,
    };

    tracing::info!(scenario = scenario.name, %run_id, verifies = scenario.verifies, "publishing");

    let result = execute(&stack, scenario, &names, &run_dir, &plan).await;

    // Clean up whether or not the scenario passed, so repeated runs do not
    // accumulate debris. The run directory is left behind on failure: its trace is
    // the only record of what the shim actually did.
    if let Err(err) = stack.delete_prefix(&names.prefix).await {
        tracing::error!(%err, prefix = %names.prefix, "failed to delete the run's tasks");
    }

    let outcome = result?;

    if outcome.passed() {
        let _ = std::fs::remove_dir_all(&run_dir);
    } else {
        tracing::warn!(?run_dir, "left the run directory in place for inspection");
    }

    Ok(outcome)
}

async fn execute(
    stack: &stack::Stack,
    scenario: &Scenario,
    names: &catalog::Names,
    run_dir: &std::path::Path,
    plan: &catalog::Plan<'_>,
) -> anyhow::Result<Outcome> {
    stack.publish(&catalog::build(plan)?).await?;

    let deadline = std::time::Duration::from_secs(180);
    stack.await_primary(&names.source_merged, deadline).await?;
    stack.await_primary(&names.source_log, deadline).await?;
    stack.await_primary(&names.sink, deadline).await?;

    let trace = RunDir::new(run_dir);

    // Let the task establish a rhythm before perturbing it. Without this a fault
    // keyed on the third StartCommit could fire while the first binding is still
    // being applied, and the scenario would be testing startup rather than what it
    // claims to.
    await_commits(&trace, scenario.warmup_commits, deadline).await?;

    if scenario.split_shards {
        tracing::info!(task = %names.sink, "splitting shards");
        stack.split_shards(&names.sink).await?;
        // Both children must come up before the run can continue; a split that
        // wedges is itself a finding.
        stack.await_primary(&names.sink, deadline).await?;
    }

    // The fault must actually have fired, or the scenario is vacuous.
    let faults_fired = await_faults(&trace, scenario.faults.len(), deadline).await?;

    // Recover the shard, then require it to keep committing.
    //
    // Both halves matter. A crashed connector leaves its shard FAILED and the
    // allocator will not reschedule it, so without the unassign the run would wait
    // out its deadline — and a destination nothing wrote to is trivially
    // consistent, which is a vacuous pass. Requiring further commits afterwards is
    // what proves the connector *recovered* rather than merely stopped.
    // Unconditional, including for the split scenarios that inject no fault: a
    // split can fail a shard too, and unassigning a healthy one is a no-op, so
    // there is nothing to gain by predicting which perturbations need it.
    let after = count_commits(&trace)? + scenario.settle_commits;
    recover(stack, &names.sink, &trace, after, deadline).await?;
    await_commits(&trace, after, deadline).await?;
    stack.await_primary(&names.sink, deadline).await?;

    // Stop the workload and let the materialization drain. Only this run's own
    // captures are touched; scenarios never disable or restart anything
    // stack-wide, which is what makes a shared stack safe for concurrent runs.
    tracing::info!("disabling the workload to reach quiescence");
    stack.publish(&catalog::quiesce(plan)?).await?;

    let connector = std::path::PathBuf::from(
        plan.subject
            .connector
            .first()
            .context("the subject names no connector binary")?,
    );

    // Read each collection until it stops growing. A capture keeps producing for a
    // while after the publication that disables it, and an expectation read one
    // document early would report the materialization as having duplicated
    // something it merely delivered on time.
    let merged_expected = Expectation::from_documents(
        stack
            .read_collection_when_final(&names.merged, deadline)
            .await?,
    );
    let log_expected = Expectation::from_documents(
        stack.read_collection_when_final(&names.log, deadline).await?,
    );

    let destination = drain(
        stack,
        &connector,
        &plan.subject.config,
        (&merged_expected, &log_expected),
        deadline,
    )
    .await?;

    let bindings = invariants::Bindings {
        merged_expected,
        log_expected,
        standard: destination.standard,
        merged_delta: destination.merged_delta,
        log: destination.log,
    };
    let documents = bindings.log_expected.documents() + bindings.merged_expected.documents();

    let (violations, exempted) = partition_exempt(invariants::check(&bindings), &scenario.exempt);

    Ok(Outcome {
        scenario: scenario.name,
        violations,
        exempted,
        faults_fired,
        documents,
        run_dir: run_dir.to_path_buf(),
    })
}

/// Split violations into those the subject is held to and those it is exempt
/// from.
fn partition_exempt(
    violations: Vec<Violation>,
    exemptions: &[Exemption],
) -> (Vec<Violation>, Vec<Violation>) {
    violations
        .into_iter()
        .partition(|v| !exemptions.iter().any(|e| e.invariant == v.invariant))
}

/// The soak capture, reused unmodified as this suite's workload generator.
fn capture_command() -> anyhow::Result<std::path::PathBuf> {
    // The launcher is self-locating but must be named absolutely, since the
    // reactor spawns `local:` connectors from `$HOME`.
    let root = repo_root()?;
    let path = root.join("tests/soak/capture/source-soak");

    anyhow::ensure!(
        path.exists(),
        "the workload capture is missing at {path:?}",
    );
    // It runs out of the repository's poetry venv, which the mise task installs.
    anyhow::ensure!(
        root.join(".venv/bin/python").exists(),
        "the workload capture needs the repository's poetry venv — run `poetry install --no-root`",
    );
    Ok(path)
}

fn repo_root() -> anyhow::Result<std::path::PathBuf> {
    // CARGO_MANIFEST_DIR is this crate; the repository root is two levels up.
    let manifest = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.to_path_buf())
        .context("resolving the repository root")
}

/// Read the shim's trace.
///
/// Missing is not an error: until the first connector process starts there is
/// nothing to read, and the callers are all polling loops.
fn read_trace(run: &RunDir) -> anyhow::Result<Vec<TraceEvent>> {
    let file = match std::fs::File::open(run.trace()) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err).context("opening the trace"),
    };

    let mut events = Vec::new();
    for line in std::io::BufReader::new(file).lines() {
        let line = line.context("reading the trace")?;
        if line.trim().is_empty() {
            continue;
        }
        // A concurrent append can leave a partial final line; it will be complete
        // on the next poll.
        if let Ok(event) = serde_json::from_str(&line) {
            events.push(event);
        }
    }
    Ok(events)
}

fn count_commits(run: &RunDir) -> anyhow::Result<u64> {
    let events = read_trace(run)?;
    Ok(events
        .iter()
        .filter(|e| matches!(e.event, Event::Phase { trigger: Trigger::StartedCommit, .. }))
        .count() as u64)
}

/// Nudge a failed shard back into service until it is committing again.
///
/// Polled rather than done once: the unassign has to land *after* the shard has
/// reached FAILED, and a fault fires a moment before that. Retrying is simpler and
/// more robust than trying to observe that transition.
async fn recover(
    stack: &stack::Stack,
    task: &str,
    run: &RunDir,
    target: u64,
    timeout: std::time::Duration,
) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + timeout;

    loop {
        if count_commits(run)? >= target {
            return Ok(());
        }
        if let Err(err) = stack.unassign_shards(task).await {
            tracing::debug!(%err, %task, "unassign did not apply");
        }

        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "{task} never resumed committing after its fault{}",
            trace_failures(run),
        );
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

async fn await_commits(
    run: &RunDir,
    target: u64,
    timeout: std::time::Duration,
) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + timeout;

    loop {
        let commits = count_commits(run)?;
        if commits >= target {
            return Ok(());
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {target} committed transactions; saw {commits}{}",
            trace_failures(run),
        );
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

async fn await_faults(
    run: &RunDir,
    expected: usize,
    timeout: std::time::Duration,
) -> anyhow::Result<usize> {
    if expected == 0 {
        return Ok(0); // The baseline scenario injects nothing.
    }
    let deadline = std::time::Instant::now() + timeout;

    loop {
        let fired = read_trace(run)?
            .iter()
            .filter(|e| matches!(e.event, Event::Fault { .. }))
            .count();

        if fired >= expected {
            return Ok(fired);
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {expected} fault(s) to fire; {fired} did{}",
            trace_failures(run),
        );
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

/// Whatever the shim said went wrong, appended to a timeout message so the
/// failure reports the shim's own reason rather than just "nothing happened".
fn trace_failures(run: &RunDir) -> String {
    let failures: Vec<String> = read_trace(run)
        .unwrap_or_default()
        .iter()
        .filter_map(|e| match &e.event {
            Event::Failed { error } => Some(format!("\n  shim (pid {}): {error}", e.pid)),
            _ => None,
        })
        .collect();
    failures.join("")
}

struct Contents {
    standard: Vec<invariants::Event>,
    merged_delta: Vec<invariants::Event>,
    log: Vec<invariants::Event>,
}

/// Poll the destination until it holds everything the collections do, or stops
/// changing.
///
/// Quiescence is required, not merely convenient: the document-counter class
/// appends during `Store`, so a mid-flight read would report a violation where none
/// exists.
///
/// Progress is measured per binding. For the append-only binding that is its row
/// count. For the merged binding it is the sum of `seq + 1` over its rows: sequences
/// are contiguous from zero, so that counts the documents reduced into it — and it
/// counts *progress* rather than correctness, since `seq` is last-write-wins and a
/// duplicate does not inflate it.
async fn drain(
    stack: &stack::Stack,
    connector: &std::path::Path,
    config: &serde_json::Value,
    (merged_expected, log_expected): (&Expectation, &Expectation),
    timeout: std::time::Duration,
) -> anyhow::Result<Contents> {
    let deadline = std::time::Instant::now() + timeout;
    let mut unchanged_for = 0;
    let mut previous = usize::MAX;

    loop {
        let contents = Contents {
            standard: stack
                .read_destination(connector, config, catalog::TABLE_STANDARD, false)
                .await?,
            merged_delta: stack
                .read_destination(connector, config, catalog::TABLE_MERGED_DELTA, true)
                .await?,
            log: stack
                .read_destination(connector, config, catalog::TABLE_LOG, true)
                .await?,
        };

        let merged_delivered: usize = contents
            .standard
            .iter()
            .map(|row| (row.seq + 1).max(0) as usize)
            .sum();

        if contents.log.len() >= log_expected.documents()
            && merged_delivered >= merged_expected.documents()
        {
            return Ok(contents);
        }

        // A short-fall is *not* an error, and giving up on it is how the run
        // reports one: it is exactly the data loss the invariants exist to name,
        // and a violation listing the missing documents says far more than a
        // timeout would. So stop once the destination has gone quiet, or once
        // patience runs out.
        let total = contents.log.len() + merged_delivered;
        unchanged_for = if total == previous { unchanged_for + 1 } else { 0 };
        previous = total;

        if unchanged_for >= 3 || std::time::Instant::now() >= deadline {
            tracing::warn!(
                log = format!("{}/{}", contents.log.len(), log_expected.documents()),
                merged = format!("{merged_delivered}/{}", merged_expected.documents()),
                "the destination stopped short of the collections",
            );
            return Ok(contents);
        }

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn write(contents: &str) -> tempfile::NamedTempFile {
        let file = tempfile::Builder::new().suffix(".json").tempfile().unwrap();
        std::fs::write(file.path(), contents).unwrap();
        file
    }

    #[test]
    fn an_exemption_file_round_trips() {
        let file = write(
            r#"[
              {
                "invariant": "monotonicity",
                "scope": "deltaBindings",
                "justification": "Rows become visible before the Flow transaction commits, because this connector appends to the destination's channel during Store. Recovery skips them rather than re-sending."
              }
            ]"#,
        );

        let exemptions = Exemption::load(file.path()).unwrap();
        assert_eq!(exemptions.len(), 1);
        assert_eq!(exemptions[0].invariant, Invariant::Monotonicity);
        assert_eq!(exemptions[0].scope, Scope::DeltaBindings);
    }

    /// The point of the compliance model is that a weaker guarantee costs an
    /// explanation. A one-word justification would make the exemption list useless
    /// as the map of the fleet's weaknesses it is supposed to be.
    #[test]
    fn an_exemption_without_a_real_justification_is_refused() {
        let file = write(r#"[{"invariant": "no-duplicates", "justification": "wontfix"}]"#);

        let err = Exemption::load(file.path()).unwrap_err().to_string();
        assert!(err.contains("no real justification"), "{err}");
    }

    #[test]
    fn an_unknown_invariant_is_refused_rather_than_ignored() {
        let file = write(
            r#"[{"invariant": "eventual-consistency", "justification": "a long enough string to pass the length check on justifications"}]"#,
        );
        assert!(Exemption::load(file.path()).is_err());
    }

    /// Exemptions filter by invariant, and only by invariant: a connector exempt
    /// from duplicate-freedom is still held to everything else.
    #[test]
    fn exemptions_suppress_only_what_they_name() {
        let violations = vec![
            Violation {
                invariant: Invariant::NoDuplicates,
                detail: "duplicated".to_string(),
            },
            Violation {
                invariant: Invariant::NoLoss,
                detail: "lost".to_string(),
            },
        ];
        let exemptions = vec![Exemption {
            invariant: Invariant::NoDuplicates,
            justification: "at-least-once by construction".to_string(),
            scope: Scope::Connector,
        }];

        let (held, exempt) = partition_exempt(violations, &exemptions);
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].invariant, Invariant::NoLoss);
        assert_eq!(exempt.len(), 1);
    }
}

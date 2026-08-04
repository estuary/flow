//! The scenario runner: publish, perturb, quiesce, verify, clean up.

pub mod catalog;
pub mod stack;
pub mod subject;

use crate::invariants::{self, Expectation, Invariant, Violation};
use crate::protocol::{Event, RunDir, TraceEvent, Trigger};
use crate::scenarios::{Scenario, Subject};
use anyhow::Context;
use std::collections::BTreeMap;
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
#[derive(Clone, Debug)]
pub struct Exemption {
    pub invariant: Invariant,
    /// Why this is a reviewed property of the destination rather than a defect.
    /// Required, and not defaulted: an exemption without a rationale is a defect
    /// with better paperwork.
    pub justification: String,
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
    /// Append rows the destination recognised as already applied.
    ///
    /// Reported on success as well as failure, because it is the difference between a
    /// scenario that survived re-delivery and one that never saw any. A split scenario
    /// passing with zero of these has demonstrated nothing about idempotency, however
    /// green it looks — the same vacuity the paired-defect rule exists to prevent.
    pub suppressed_rows: i64,
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
                "{}: upheld every invariant over {} documents \
                 ({} faults injected, {} re-delivered rows absorbed)",
                self.scenario, self.documents, self.faults_fired, self.suppressed_rows,
            );
        }
        let mut lines = vec![format!(
            "{}: {} violation(s) over {} documents \
             ({} faults injected, {} re-delivered rows absorbed)",
            self.scenario,
            self.violations.len(),
            self.documents,
            self.faults_fired,
            self.suppressed_rows,
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
///
/// `external` carries a real connector's discovered resource shape; `None` means the
/// reference connector, whose shape the harness knows.
pub async fn run(
    scenario: &Scenario,
    subject: &Subject,
    external: Option<&subject::External>,
) -> anyhow::Result<Outcome> {
    let stack = stack::Stack::from_env()?;
    let run_id = run_id();

    let names = catalog::Names::new(&stack.tenant, scenario.name, &run_id, external.is_some());
    let run_dir = stack
        .stack_dir
        .join("consistency")
        .join(format!("{}-{run_id}", scenario.name));
    std::fs::create_dir_all(&run_dir)
        .with_context(|| format!("creating run directory {run_dir:?}"))?;

    let shim = stack.binary("consistency-shim")?;
    let capture = capture_command()?;

    // The reference connector's destination lives in the run directory, so it is deleted
    // with the rest of the run's debris and two concurrent runs cannot see each other's
    // rows. A real connector's config is its own and must not be edited: `path` means
    // nothing to it, and connectors parse their configs strictly.
    let mut config = subject.config.clone();
    if external.is_none() {
        config["path"] = serde_json::json!(run_dir.join("destination.sqlite").to_string_lossy());
    }

    let subject_config = Subject {
        connector: subject.connector.clone(),
        config,
    };
    let workload = match external {
        Some(_) => catalog::Workload::remote(),
        None => catalog::Workload::default(),
    };

    let plan = catalog::Plan {
        names: &names,
        subject: &subject_config,
        shim: &shim,
        capture: &capture,
        run_dir: &run_dir,
        faults: &scenario.faults,
        workload: &workload,
        standard_binding: scenario.standard_binding,
        resource_shape: external.map(|e| &e.shape),
    };

    tracing::info!(scenario = scenario.name, %run_id, verifies = scenario.verifies, "publishing");

    let destination = run_dir.join("destination.sqlite");
    let result = tokio::select! {
        result = execute(&stack, scenario, &names, &run_dir, &plan, external) => result,
        err = watch_destination_size(&destination) => Err(err),
    };

    // Clean up whether or not the scenario passed, so repeated runs do not
    // accumulate debris. The run directory is left behind on failure: its trace is
    // the only record of what the shim actually did.
    if let Err(err) = stack.delete_prefix(&names.prefix).await {
        tracing::error!(%err, prefix = %names.prefix, "failed to delete the run's tasks");
    }

    // A real subject's tables outlive its specifications too, and unlike the ops journals
    // the harness *can* reach these — through the connector's own `drop-resource`, since it
    // has no client for an arbitrary endpoint and should not grow one.
    //
    // Best-effort and deliberately not fatal: a warehouse refusing a `DROP` says nothing
    // about the connector's consistency, and letting it fail a scenario that passed would
    // trade a real signal for a housekeeping one. It is also unconditional on the outcome,
    // unlike the run directory kept below for inspection — a failing scenario's evidence is
    // already in its violation report, so the table itself is not needed afterwards.
    //
    // Table names carry the run id, so every run would otherwise leave three per scenario in
    // someone's warehouse, holding data nobody reads again.
    if let Some(external) = external {
        for (table, delta) in [
            (catalog::TABLE_STANDARD, false),
            (catalog::TABLE_MERGED_DELTA, true),
            (catalog::TABLE_LOG, true),
        ] {
            if table == catalog::TABLE_STANDARD && !scenario.standard_binding {
                continue; // Never materialized, so never created.
            }
            let resource = catalog::resource_config(Some(&external.shape), &names, table, delta);
            if let Err(err) = stack
                .drop_resource(&external.connector, &external.config, &resource)
                .await
            {
                tracing::warn!(%err, table = %names.table(table), "could not drop the run's table");
            }
        }
    }

    // The run's ops log and stats partitions outlive its specifications, and nothing
    // here can remove them: they live under `ops/`, and a task-scoped authorization does
    // not reach that far. `mise run ci:consistency` purges them against etcd before each
    // suite run, which is the only level with the access. See that task for why the leak
    // matters — it saturated the broker allocator once and cost a whole run.

    let outcome = result?;

    if outcome.passed() {
        let _ = std::fs::remove_dir_all(&run_dir);
    } else {
        tracing::warn!(?run_dir, "left the run directory in place for inspection");
    }

    Ok(outcome)
}

/// Fail a run whose destination is growing without bound, before it fills the disk.
///
/// A subject carrying an append-side defect writes a row per replayed `Acknowledge`, and
/// the runtime will replay for as long as the connector keeps refusing to make progress.
/// Left alone that is unbounded: one such run reached 40 GiB and wedged every later run
/// on the machine, because a scenario killed by the test runner's timeout never reaches
/// any cleanup this crate could write. So the bound is enforced while the file grows
/// rather than after the run ends.
///
/// Never resolves while the destination is a sane size, so it composes as the losing arm
/// of a `select!`.
async fn watch_destination_size(destination: &std::path::Path) -> anyhow::Error {
    /// 4 GiB — roughly ten times the largest destination a passing scenario has produced,
    /// which is enough headroom that tripping this means "runaway", not "a big run".
    const LIMIT: u64 = 4 * 1024 * 1024 * 1024;

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        let Ok(meta) = std::fs::metadata(destination) else {
            continue; // Not yet created, or already cleaned up.
        };
        if meta.len() > LIMIT {
            // Keep the traces, which say what the shim did, and drop only the bulk.
            let _ = std::fs::remove_file(destination);
            return anyhow::anyhow!(
                "the destination grew past {} GiB, so the run was abandoned before it \
                 could fill the disk; the subject is appending without ever making \
                 progress, which is the defect this run was checking for",
                LIMIT >> 30,
            );
        }
    }
}

async fn execute(
    stack: &stack::Stack,
    scenario: &Scenario,
    names: &catalog::Names,
    run_dir: &std::path::Path,
    plan: &catalog::Plan<'_>,
    external: Option<&subject::External>,
) -> anyhow::Result<Outcome> {
    let published = std::time::Instant::now();
    stack.publish(&catalog::build(plan)?).await?;
    tracing::info!(elapsed = ?published.elapsed(), "published");

    // Scaled to the subject, because every gate here counts *commits*: a remote
    // destination takes tens of seconds per transaction, so a budget sized for a local
    // one expires while the connector is working correctly.
    // The external figure is measured rather than guessed: against a remote warehouse the
    // scenarios that pass take 200-600s, and every scenario that failed at 900s failed by
    // running out of budget mid-phase — one having committed 2 of 3 transactions, another
    // never reaching a fault keyed on the second post-split commit. A perturbed scenario
    // needs several times the budget of an unperturbed one, because a split doubles the
    // shards committing to the same warehouse and recovery republishes the task.
    let deadline = match external {
        Some(_) => std::time::Duration::from_secs(1800),
        None => std::time::Duration::from_secs(180),
    };

    // Longer than the general deadline, because settling a collection means *reading*
    // it repeatedly and a read of a large one can take a minute under contention.
    let finality_timeout = match external {
        Some(_) => std::time::Duration::from_secs(2400),
        None => std::time::Duration::from_secs(600),
    };

    let trace = RunDir::new(run_dir);

    // Nothing in a run waits on shard *status*, at any point. Progress is the gate
    // instead: `await_commits` and `recover` both watch the shim's trace, and a task
    // that is committing is by definition being served, while a shard reported primary
    // may still be doing nothing.
    //
    // A status check is unsafe in either position. After a perturbation it catches the
    // crash the scenario just injected and fails before recovery is attempted. And before
    // one it is no safer, because a task begins processing the moment it is activated: a
    // fault keyed on an early protocol event can fire in the materialization while the
    // harness is still checking a capture, leaving the sink's own check to find a FAILED
    // shard with the recovery machinery still far below it.

    // Let the task establish a rhythm before perturbing it. Without this a fault
    // keyed on the third StartCommit could fire while the first binding is still
    // being applied, and the scenario would be testing startup rather than what it
    // claims to.
    // Split activation from cadence: the first traced message means the sink's connector
    // is up and being spoken to, so everything before it is the runtime scheduling a shard
    // and starting a process, and everything after is transactions.
    let activating = std::time::Instant::now();
    await_first_message(&trace, deadline).await?;
    tracing::info!(elapsed = ?activating.elapsed(), "sink connector started");

    // The gap between the connector being spoken to and its first *non-empty* transaction
    // is the sink sitting idle while its captures activate and produce. Measured
    // separately because it, not the commit cadence, is what varies between runs.
    let feeding = std::time::Instant::now();
    await_first_documents(&trace, deadline).await?;
    tracing::info!(elapsed = ?feeding.elapsed(), "workload feeding the sink");

    let warmed = std::time::Instant::now();
    await_commits(&trace, scenario.warmup_commits, deadline).await?;
    tracing::info!(elapsed = ?warmed.elapsed(), commits = scenario.warmup_commits, "warmed up");

    // A scenario that scales out *because of* the fault has to see it land first,
    // or the two race and the run is no longer testing the sequence it describes.
    // The crash leaves the task down, so the split lands while nothing is writing
    // and the work is finished by the larger set of shards that replaces it.
    if scenario.split_after_fault {
        await_faults(&trace, scenario.faults.len(), deadline).await?;
    }

    if scenario.split_shards {
        tracing::info!(task = %names.sink, "splitting shards");
        stack.split_shards(&names.sink).await?;
        // Both children must come up before the run can continue; a split that
        // wedges is itself a finding.
        recover(
            stack,
            plan,
            &names.sink,
            &trace,
            count_commits(&trace)? + 1,
            deadline,
        )
        .await?;
    }

    if scenario.join_shards {
        // Every child must commit *for itself* before the join, not merely two commits
        // between them. The survivor keeps its recovery log through the join, and if it
        // has not yet written a checkpoint of its own, that log still holds the
        // parent's — whose clock predates the log's close, which recovery refuses. That
        // is what wedged this scenario in a 33-restart loop.
        await_commits_each_shard(&trace, 2, 2, deadline).await?;

        tracing::info!(task = %names.sink, "joining shards");
        stack.join_shards(&names.sink).await?;
        recover(
            stack,
            plan,
            &names.sink,
            &trace,
            count_commits(&trace)? + 1,
            deadline,
        )
        .await?;
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
    let settled = std::time::Instant::now();
    let after = count_commits(&trace)? + scenario.settle_commits;
    recover(stack, plan, &names.sink, &trace, after, deadline).await?;
    await_commits(&trace, after, deadline).await?;
    tracing::info!(elapsed = ?settled.elapsed(), commits = scenario.settle_commits, "settled");

    // Stop the workload and let the materialization drain. Only this run's own
    // captures are touched; scenarios never disable or restart anything
    // stack-wide, which is what makes a shared stack safe for concurrent runs.
    tracing::info!("disabling the workload to reach quiescence");
    let quiesced = std::time::Instant::now();
    stack.publish(&catalog::disable_captures(plan)?).await?;
    tracing::info!(elapsed = ?quiesced.elapsed(), "quiesced");

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
    // Concurrently: the two collections are independent, and each read loops until its
    // own contents stop growing, so serialising them doubled the wait for nothing.
    let read = std::time::Instant::now();
    let (merged_expected, log_expected) = tokio::try_join!(
        stack.read_collection_when_final(&names.merged, finality_timeout),
        stack.read_collection_when_final(&names.log, finality_timeout),
    )?;
    let merged_expected = Expectation::from_documents(merged_expected);
    let log_expected = Expectation::from_documents(log_expected);
    tracing::info!(elapsed = ?read.elapsed(), "read the collections");

    let drained = std::time::Instant::now();
    let destination = drain(
        stack,
        &connector,
        &plan.subject.config,
        names,
        (&merged_expected, &log_expected),
        plan.standard_binding,
        external.map(|e| &e.shape),
        deadline,
    )
    .await?;

    tracing::info!(elapsed = ?drained.elapsed(), "drained the destination");

    let bindings = invariants::Bindings {
        merged_expected,
        log_expected,
        standard: destination.standard,
        merged_delta: destination.merged_delta,
        log: destination.log,
    };
    let documents = bindings.log_expected.documents() + bindings.merged_expected.documents();

    // Monotonicity is exempted for a subject read as a table, because the order rows come
    // back in is not guaranteed to be the order they were stored in — not merely unobserved
    // but unguaranteeable, since a distributed destination is free to return rows from any
    // partition or file in any order. There is no ordering to recover: a commit timestamp
    // ties every row of a transaction, and inventing a total order out of that would
    // manufacture violations rather than find them.
    //
    // The reference connector is the exception the check was written against — its tables
    // carry an autoincrementing `ord`, so a read replays the sequence of appends — which is
    // why the assumption survived until a real connector was pointed at.
    //
    // The set-based checks — no-loss, no-duplicates, conservation and oracle agreement —
    // carry the exactly-once claim, and none of them depends on arrival order.
    let mut exempt = scenario.exempt.clone();
    if external.is_some() {
        exempt.push(Exemption {
            invariant: Invariant::Monotonicity,
            justification: "This subject's destination is read as a table, and the order rows are \
                 returned in is not guaranteed to be the order they were stored in — a \
                 distributed destination may return rows from any partition or file in any \
                 order. Delivery order is therefore not recoverable, and the set-based \
                 invariants carry the exactly-once claim."
                .to_string(),
        });
    }

    let (violations, exempted) = partition_exempt(invariants::check(&bindings), &exempt);

    // A failure writes down everything it judged, next to the trace.
    //
    // Diagnosing one of these from the violation list alone means guessing which side
    // is wrong — the destination, or the expectation read from the collection. Both
    // are cheap to record and impossible to reconstruct afterwards, because the run's
    // tasks are deleted on the way out.
    if !violations.is_empty() {
        if let Err(err) = dump_evidence(run_dir, &bindings) {
            tracing::error!(%err, "failed to write the failure's evidence");
        }
    }

    // Read before the run directory is cleaned up, and on every path: a passing run is
    // exactly the one where this number decides whether anything was proved.
    let suppressed_rows = suppressed_rows(run_dir)
        .map(|rows| rows.iter().map(|(_, n)| n).sum())
        .unwrap_or(0);

    Ok(Outcome {
        scenario: scenario.name,
        violations,
        exempted,
        suppressed_rows,
        faults_fired,
        documents,
        run_dir: run_dir.to_path_buf(),
    })
}

/// Append rows the destination recognised as already applied, per table.
///
/// Read straight from the destination rather than through the connector, because it is
/// the connector's own bookkeeping rather than a binding's contents. A non-zero count
/// says a document was handed to the connector twice — which the delivered rows cannot
/// say, since suppressing the second copy is precisely what makes them look correct.
fn suppressed_rows(run_dir: &std::path::Path) -> anyhow::Result<Vec<(String, i64)>> {
    let conn = rusqlite::Connection::open_with_flags(
        run_dir.join("destination.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let mut stmt = conn.prepare("SELECT tbl, rows FROM _flow_suppressed ORDER BY tbl")?;
    let rows = stmt
        .query_map((), |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn dump_evidence(run_dir: &std::path::Path, b: &invariants::Bindings) -> anyhow::Result<()> {
    let expectation = |e: &Expectation| -> Vec<serde_json::Value> {
        e.accounts
            .iter()
            .map(|(id, a)| {
                serde_json::json!({
                    "id": id,
                    "documents": a.seqs.len(),
                    "maxSeq": a.max_seq,
                    "totalDelta": a.total_delta,
                    "finalOracleBalance": a.final_oracle.balance,
                    "seqs": a.seqs.iter().collect::<Vec<_>>(),
                })
            })
            .collect()
    };

    let evidence = serde_json::json!({
        "expected": {
            "merged": expectation(&b.merged_expected),
            "log": expectation(&b.log_expected),
            "duplicatedSourceDocuments": {
                "merged": b.merged_expected.duplicated_documents,
                "log": b.log_expected.duplicated_documents,
            },
        },
        "suppressedAppendRows": suppressed_rows(run_dir)
            .unwrap_or_else(|err| vec![(format!("unreadable: {err:#}"), -1)]),
        "delivered": {
            "standard": &b.standard,
            "mergedDelta": &b.merged_delta,
            "log": &b.log,
        },
    });

    let path = run_dir.join("evidence.json");
    std::fs::write(&path, serde_json::to_vec_pretty(&evidence)?)
        .with_context(|| format!("writing {path:?}"))?;

    tracing::warn!(
        ?path,
        "wrote the failure's expectation and destination contents"
    );
    Ok(())
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

    anyhow::ensure!(path.exists(), "the workload capture is missing at {path:?}",);
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

/// Commits made by each shard a split produced, keyed by its range.
///
/// `count_commits` sums over the whole task, which is the wrong gate before a join:
/// two commits can both come from one child while the other has committed none. The
/// survivor of a join is then widened while its recovery log still holds the *parent's*
/// connector checkpoint, and recovery refuses it — `connector_checkpoint has clock ...
/// which doesn't match Recover's committed_close or hinted_close`, 26 times in a
/// restart loop, because the checkpoint predates the log's close.
///
/// Restarts are folded together: a shard that crashed and came back is one shard, and
/// the commits of both its processes count towards its total.
fn commits_per_split_shard(run: &RunDir) -> anyhow::Result<BTreeMap<(u32, u32), u64>> {
    let events = read_trace(run)?;

    let mut range_of: BTreeMap<u32, (u32, u32)> = BTreeMap::new();
    for event in &events {
        if let Event::Opened {
            key_begin, key_end, ..
        } = event.event
        {
            range_of.insert(event.pid, (key_begin, key_end));
        }
    }

    let mut commits: BTreeMap<(u32, u32), u64> = BTreeMap::new();
    for event in &events {
        let Event::Phase {
            trigger: Trigger::StartedCommit,
            ..
        } = event.event
        else {
            continue;
        };
        let Some(&range) = range_of.get(&event.pid) else {
            continue; // A commit before this process's Open cannot happen.
        };
        // The unsplit parent is not a shard the join will touch.
        if range == (0, u32::MAX) {
            continue;
        }
        *commits.entry(range).or_default() += 1;
    }
    Ok(commits)
}

/// Wait until every shard a split produced has committed `each` transactions of its
/// own. See [`commits_per_split_shard`] for why a task-wide count will not do.
async fn await_commits_each_shard(
    run: &RunDir,
    shards: usize,
    each: u64,
    timeout: std::time::Duration,
) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + timeout;

    loop {
        let commits = commits_per_split_shard(run)?;
        let ready = commits.len() >= shards && commits.values().all(|&n| n >= each);

        if ready {
            tracing::info!(?commits, "every split shard has committed for itself");
            return Ok(());
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {shards} shards to commit {each} transactions each; \
             saw {commits:?}{}",
            trace_failures(run),
        );
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

fn count_commits(run: &RunDir) -> anyhow::Result<u64> {
    let events = read_trace(run)?;
    Ok(events
        .iter()
        .filter(|e| {
            matches!(
                e.event,
                Event::Phase {
                    trigger: Trigger::StartedCommit,
                    ..
                }
            )
        })
        .count() as u64)
}

/// Take the task down and bring it back, for a fault that failed the whole task
/// rather than one shard.
///
/// Unassigning failed shards and waiting for the allocator to reschedule them is right
/// for a single-shard crash, and not sufficient for a split task: a crash in *either*
/// shard fails both, since the survivor reports `expected leader message ... unexpected
/// EOF`, and unassigning restores such a task only about two runs in three. Disabling the
/// materialization tears its shards down; republishing the enabled catalog builds them
/// again from the recovery log. A restart rather than a reschedule, and what an operator
/// would do.
///
/// Deliberately does *not* wait for a primary. The caller's recovery loop is the resilient
/// step — it unassigns until the task is committing again — so waiting here would only add
/// a way to fail before that loop gets its turn, on the surviving shard's `expected leader
/// message ... unexpected EOF`.
async fn restart_task(
    stack: &stack::Stack,
    plan: &catalog::Plan<'_>,
    task: &str,
) -> anyhow::Result<()> {
    tracing::info!(%task, "restarting the task after its fault");

    stack.publish(&catalog::sink_disabled(plan)?).await?;
    stack.publish(&catalog::build(plan)?).await?;

    Ok(())
}

/// Nudge the task back into service until it is committing again, escalating if it will
/// not come back.
///
/// Two remedies, in order of cost. Unassigning a FAILED shard is enough for most faults:
/// the allocator will not reschedule a shard it has given up on, and unassigning clears
/// that. But some do not come back that way — a crash in either shard of a split task
/// fails the whole task, and even a single-shard crash sometimes sat FAILED for the full
/// deadline — so after a third of the budget this republishes the task, disabled then
/// enabled, which tears the shards down and rebuilds them from the recovery log.
///
/// The escalation lives here rather than behind a per-scenario flag because it is not a
/// property of the scenario: any crash can land a shard somewhere unassigning will not
/// lift it from, and a scenario author cannot predict which. Cheap remedy first means a
/// task that recovers on its own never pays for the expensive one.
async fn recover(
    stack: &stack::Stack,
    plan: &catalog::Plan<'_>,
    task: &str,
    run: &RunDir,
    target: u64,
    timeout: std::time::Duration,
) -> anyhow::Result<()> {
    let started = std::time::Instant::now();
    let deadline = started + timeout;
    let escalate_after = timeout / 3;
    let mut restarted = false;

    // How long without a commit before the task counts as stuck, and how often to look.
    //
    // Proportional to the subject's own transaction pace, not a constant. Unassigning is a
    // *remedy* — it takes the shard from its reactor so a failed one is rescheduled — and
    // applying it to a task that is merely mid-transaction interrupts work that was going
    // to succeed. Three transaction-lengths is long enough that a connector committing on
    // schedule is never touched, and short enough to rescue one that has genuinely died.
    //
    // A fixed threshold cannot do both: 20s is ample for a subject committing in
    // milliseconds and less than one commit for a subject committing to a warehouse, which
    // is why `materialize-databricks` was being yanked every 20s while working correctly.
    let pace = plan.workload.max_txn;
    let stalled_after = pace * 3;
    let poll = std::cmp::max(std::time::Duration::from_secs(5), pace / 3);

    let mut last_commits = count_commits(run)?;
    let mut stalled_since = std::time::Instant::now();

    loop {
        let commits = count_commits(run)?;
        if commits >= target {
            return Ok(());
        }

        if commits != last_commits {
            last_commits = commits;
            stalled_since = std::time::Instant::now();
        } else if stalled_since.elapsed() > stalled_after {
            stalled_since = std::time::Instant::now();
            tracing::info!(
                %task,
                stalled_secs = stalled_after.as_secs(),
                "no commit within three transaction lengths; unassigning",
            );
            if let Err(err) = stack.unassign_shards(task).await {
                tracing::debug!(%err, %task, "unassign did not apply");
            }
        }
        if !restarted && started.elapsed() > escalate_after {
            restarted = true;
            tracing::warn!(
                %task,
                elapsed_secs = started.elapsed().as_secs(),
                "unassigning has not restored the task; republishing it",
            );
            if let Err(err) = restart_task(stack, plan, task).await {
                tracing::warn!(%err, %task, "the republish did not apply");
            }
        }

        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "{task} never resumed committing after its fault{}",
            trace_failures(run),
        );
        tokio::time::sleep(poll).await;
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

/// Wait until the shim has traced anything at all, which is the sink's connector being
/// spoken to for the first time.
async fn await_first_message(run: &RunDir, timeout: std::time::Duration) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + timeout;

    loop {
        if !read_trace(run)?.is_empty() {
            return Ok(());
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "timed out waiting for the sink's connector to be started",
        );
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

/// Wait until a transaction has carried at least one document, which means the captures
/// are producing and the sink is being fed.
async fn await_first_documents(run: &RunDir, timeout: std::time::Duration) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + timeout;

    loop {
        let fed = read_trace(run)?.iter().any(|e| match &e.event {
            Event::Stored { per_binding } => per_binding.iter().any(|n| *n != 0),
            _ => false,
        });
        if fed {
            return Ok(());
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "timed out waiting for the workload to feed the sink{}",
            trace_failures(run),
        );
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
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
    standard: Option<Vec<invariants::Event>>,
    merged_delta: Vec<invariants::Event>,
    log: Vec<invariants::Event>,
}

/// The highest sequence delivered for an account, from the standard binding when the
/// task has one and the merged delta binding otherwise.
///
/// `seq` is last-write-wins in the merged collection, so the highest delivered value is
/// a sound measure of progress that no duplicate can inflate.
fn delivered_max_seq(contents: &Contents, id: i64) -> Option<i64> {
    if let Some(rows) = &contents.standard {
        return rows.iter().find(|r| r.id == id).map(|r| r.seq);
    }
    contents
        .merged_delta
        .iter()
        .filter(|r| r.id == id)
        .map(|r| r.seq)
        .max()
}

async fn drain(
    stack: &stack::Stack,
    connector: &std::path::Path,
    config: &serde_json::Value,
    names: &catalog::Names,
    (merged_expected, log_expected): (&Expectation, &Expectation),
    standard_binding: bool,
    shape: Option<&subject::ResourceShape>,
    timeout: std::time::Duration,
) -> anyhow::Result<Contents> {
    // Named exactly as the catalog named them when it built the bindings, so a read asks
    // for the resource the connector was actually given.
    let resource = |table: &str, delta: bool| catalog::resource_config(shape, names, table, delta);

    let deadline = std::time::Instant::now() + timeout;
    let mut unchanged_for = 0;
    let mut stuck_for = 0;
    let mut previous = usize::MAX;

    /// Consecutive quiet polls before a destination counts as settled.
    ///
    /// Ten, at three seconds each, because a plateau is weak evidence and a short one
    /// is worthless: transactions land every one to two seconds, so five polls was
    /// fifteen seconds — a gap a task takes just by restarting after a fault, or by
    /// being starved on a loaded stack. A scenario was once failed by exactly that,
    /// reporting 229 still-undelivered documents as invariant violations.
    const QUIET_POLLS: usize = 10;

    /// Consecutive polls of an *unhealthy* destination going nowhere before the wait ends.
    ///
    /// Twice `QUIET_POLLS`, because "unhealthy" covers both a shard restarting — which
    /// resolves in a poll or two — and a task that can never run again, which is how
    /// several defects surface. Without a bound of its own the second case can only end at
    /// the deadline, and that dominated the suite: `split-during-store`'s defective half
    /// spent 150 of its 180 seconds waiting for a task whose two shards were fencing each
    /// other off and were never going to progress.
    const STUCK_POLLS: usize = 20;

    loop {
        let standard = match standard_binding {
            true => Some(
                stack
                    .read_destination(connector, config, &resource(catalog::TABLE_STANDARD, false))
                    .await?,
            ),
            false => None,
        };

        let contents = Contents {
            standard,
            merged_delta: stack
                .read_destination(
                    connector,
                    config,
                    &resource(catalog::TABLE_MERGED_DELTA, true),
                )
                .await?,
            log: stack
                .read_destination(connector, config, &resource(catalog::TABLE_LOG, true))
                .await?,
        };

        // Documents reduced into the merged bindings, per the highest sequence reached
        // for each account. Taken from the standard binding when there is one, and
        // otherwise from the delta binding's latest row per account — the same measure
        // of *progress*, since `seq` is last-write-wins and a duplicate cannot inflate
        // it.
        let merged_delivered: usize = match &contents.standard {
            Some(rows) => rows.iter().map(|r| (r.seq + 1).max(0) as usize).sum(),
            None => {
                let mut highest: std::collections::BTreeMap<i64, i64> = Default::default();
                for row in &contents.merged_delta {
                    let seen = highest.entry(row.id).or_insert(row.seq);
                    *seen = (*seen).max(row.seq);
                }
                highest.values().map(|s| (s + 1).max(0) as usize).sum()
            }
        };

        // The two collections need different completion measures, because they are
        // keyed differently.
        //
        // `log` is keyed [/id, /seq], so every document is its own key and arrives as
        // its own row: a row count is exact.
        //
        // `merged` is keyed [/id] and reduced, so the runtime delivers one document per
        // key per *transaction* — several source documents for one account combine into
        // one delivered row. Its row count is therefore always below the collection's
        // document count, and a row-count gate there could never be met at all.
        // Completion is per-account instead — every
        // account must have reached its highest expected `seq`, which is exact because
        // `seq` is last-write-wins and no duplicate can push it past the collection's.
        let merged_complete = merged_expected.accounts.iter().all(|(id, account)| {
            delivered_max_seq(&contents, *id).is_some_and(|seq| seq >= account.max_seq)
        });

        if contents.log.len() >= log_expected.documents() && merged_complete {
            return Ok(contents);
        }

        // A short-fall is *not* an error, and giving up on it is how the run reports
        // one: it is exactly the data loss the invariants exist to name, and a
        // violation listing the missing documents says far more than a timeout would.
        // So stop once the destination has gone quiet, or once patience runs out.
        //
        // "Quiet" means unchanged *and* the task healthy. Without the health gate a
        // shard that is mid-restart looks identical to one that has finished its
        // work, and every membership-change scenario becomes flaky in the direction
        // of falsely reporting loss.
        let total = contents.log.len() + merged_delivered;
        let healthy = stack.all_primary(&names.sink).await.unwrap_or(false);

        unchanged_for = if total == previous && healthy {
            unchanged_for + 1
        } else {
            0
        };
        stuck_for = if total == previous && !healthy {
            stuck_for + 1
        } else {
            0
        };
        previous = total;

        let quiet = unchanged_for >= QUIET_POLLS;
        let stuck = stuck_for >= STUCK_POLLS;
        let expired = std::time::Instant::now() >= deadline;

        if quiet || stuck || expired {
            // Which of the three ended the wait matters when reading a failure. "quiet"
            // means a healthy task stopped writing while still short, which is a finding.
            // "stuck unhealthy" means it cannot run at all — the shape several defects
            // take, and not a shortfall to reason about. "deadline" means the runner ran
            // out of patience, which is neither.
            // Reported as the *delta* row count, the same figure the completion gate
            // uses. The seq-derived `merged_delivered` belongs in the plateau check and
            // nowhere else: it read "1020/997" — complete — for a destination whose
            // delta binding held 768 of 997, which is precisely the shortfall being
            // reported. A warning that hides what it is warning about is worse than
            // none.
            let behind: Vec<String> = merged_expected
                .accounts
                .iter()
                .filter_map(|(id, account)| {
                    let seq = delivered_max_seq(&contents, *id);
                    match seq {
                        Some(seq) if seq >= account.max_seq => None,
                        Some(seq) => Some(format!("{id}@{seq}<{}", account.max_seq)),
                        None => Some(format!("{id}@absent<{}", account.max_seq)),
                    }
                })
                .collect();

            let short = format!(
                "log {}/{}, merged accounts behind {} of {}{}",
                contents.log.len(),
                log_expected.documents(),
                behind.len(),
                merged_expected.accounts.len(),
                match behind.is_empty() {
                    true => String::new(),
                    // Bounded: a stalled task leaves every account behind, and a few
                    // name the shape as well as forty would.
                    false => format!(
                        " [{}]",
                        behind.iter().take(6).cloned().collect::<Vec<_>>().join(" ")
                    ),
                },
            );

            // An `Err`, not a short destination handed to the checkers. Whether those
            // documents were lost by the connector or merely not waited for is exactly
            // what cannot be told apart here, and attributing it to the connector
            // produces confident nonsense: 109 oracle-agreement violations over a log
            // binding that was perfect at 1020 of 1020. The defective half of a scenario
            // counts an `Err` as caught, so a defect that genuinely loses data is still
            // reported as caught rather than passing.
            anyhow::bail!(
                "the destination stopped short of the collections ({short}); \
                 reason={}, task healthy={healthy}",
                match (quiet, stuck) {
                    (true, _) => "went quiet",
                    (_, true) => "stuck unhealthy",
                    _ => "deadline",
                },
            );
        }

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}

#[cfg(test)]
mod test {
    use super::*;

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
        }];

        let (held, exempt) = partition_exempt(violations, &exemptions);
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].invariant, Invariant::NoLoss);
        assert_eq!(exempt.len(), 1);
    }
}

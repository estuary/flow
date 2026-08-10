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
    /// Most violations this exemption may absorb before the run fails anyway.
    ///
    /// An exemption is a statement about a *cause* — "one replayed transaction re-delivers what
    /// it already stored" — and every such cause implies a volume. Without a ceiling the
    /// exemption also absorbs a subject that re-delivered the entire workload, which is a
    /// different failure wearing the same justification.
    ///
    /// `None` where the cause has no principled bound to write down. That is not laziness: the
    /// reordering seen around membership changes is observed but unexplained, so any number here
    /// would be invented, and an invented ceiling produces intermittent failures that teach the
    /// reader to raise it. Better to leave it off and say so.
    ///
    /// A ceiling is also worthless where the *checker* bounds the count for it. `OracleAgreement`
    /// reports at most three violations per account in `check_standard` and two in
    /// `check_merged_delta`, over a forty-account workload — so any ceiling above about two hundred
    /// can never bind, and one below it is measuring the account count rather than the subject.
    /// Only the per-document counts, `NoDuplicates` above all, carry volume information at all.
    pub max_suppressed: Option<usize>,
    /// An invariant that must *also* have been violated for this exemption to apply.
    ///
    /// This is what makes an exemption say what its justification says. "A duplicated document
    /// leaves the reduced balance disagreeing with its own oracle" licenses an oracle disagreement
    /// *caused by duplication* — and as a bare exemption it licensed one from any cause, including
    /// a replay path that corrupted merged values while emitting no extra rows at all. Such a
    /// subject broke oracle agreement and conservation, both exempt, while the per-document counts
    /// stayed clean, and it passed the whole suite.
    ///
    /// Naming `NoDuplicates` here ties the licence to its stated cause: no duplicate row anywhere
    /// in the run means nothing was duplicated, so a divergence is unexplained and held. It is
    /// evaluated over the raw violations, before exemption, because a duplicate that this
    /// scenario exempts is still a duplicate that happened.
    pub conditional_on: Option<Invariant>,
}

/// Marker in the error chain of a run that failed for a reason that says nothing about the subject.
///
/// The defective half of a scenario treats a failed run as the defect being caught, which is
/// right for a defect that wedges the task — `ignore-key-range` leaves two shards fencing each
/// other and neither can commit — and wrong for everything else that can go wrong on the way. A
/// gate that timed out during warmup, a split that never landed, a capture slow to deactivate, a
/// collection read that could not settle: none is evidence about the subject, and counting one
/// silently vacates the pairing the scenario exists to provide.
///
/// So every such cause is *named* here rather than left as an untyped `anyhow` string, and the
/// pairing guard asks for evidence rather than accepting the absence of a clean result as evidence.
/// Four of these variants were untyped until a review pointed out that each one lands *after* the
/// fault fires — so a stack degrading between the two halves scored as a catch, which is the exact
/// regression the typing exists to prevent.
#[derive(Debug)]
pub enum Environment {
    /// The control plane would not publish the scenario's catalog.
    PublishFailed,
    /// A gate before the run's perturbation timed out.
    ///
    /// The line is drawn at the perturbation, and for four scenarios that is *not* the fault: a
    /// membership change is a perturbation in its own right, and `split-during-store` and
    /// `join-after-split` inject no fault at all. Their line sits at the split — before it, a
    /// failure is setup; after it, a failure may be the defect, since `ignore-key-range` leaves
    /// children fencing each other off and that is what stops a task committing.
    ///
    /// Hence the asymmetry in what is annotated: the call that *issues* the split carries this,
    /// because a perturbation that never happened is setup failing, while the gates waiting on its
    /// consequences do not. The join does not carry it either, for the same reason those gates do
    /// not — it comes after the split, which is already `join-after-split`'s perturbation, so a
    /// join that cannot be issued may be the defect's doing rather than the environment's.
    BeforePerturbation,
    /// A capture was still running well after being published as disabled.
    WorkloadWouldNotStop,
    /// A collection would not settle, so no expectation could be read from it.
    CollectionUnread,
    /// The collection held a repeated `(id, seq)`, so the comparison the run would have made is
    /// not sound. The fault is in what the harness was given, not in what the subject did.
    UnsoundWorkload,
    /// The destination was still short of the collection when the runner ran out of patience —
    /// as distinct from having *stopped* short, which is a finding and is not this.
    DrainDeadline,
}

impl std::fmt::Display for Environment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::PublishFailed => "the stack would not publish the catalog",
            Self::BeforePerturbation => "the run failed before its perturbation was applied",
            Self::WorkloadWouldNotStop => "the workload would not stop",
            Self::CollectionUnread => "a collection would not settle to be read",
            Self::UnsoundWorkload => "the workload was unsound for this run",
            Self::DrainDeadline => "the runner ran out of patience waiting for the destination",
        })
    }
}

impl std::error::Error for Environment {}

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
                self.scenario, self.documents, self.faults_fired,
            );
        }
        let mut lines = vec![format!(
            "{}: {} violation(s) over {} documents ({} faults injected)",
            self.scenario,
            self.violations.len(),
            self.documents,
            self.faults_fired,
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
    let mut subject_config = subject.clone();
    if external.is_none() {
        subject_config.config["path"] =
            serde_json::json!(run_dir.join("destination.sqlite").to_string_lossy());
    }
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
        // A scenario's own class decides this for the reference connector. For a real subject the
        // *subject's* class decides it: a counted channel is delta-only by definition, so handing
        // one a merge binding would test a shape it never claimed to support.
        standard_binding: scenario.standard_binding()
            && external.map_or(true, |e| {
                e.class != crate::reference::Class::DocumentCounter
            }),
        resource_shape: external.map(|e| &e.shape),
    };

    tracing::info!(scenario = scenario.name, %run_id, verifies = scenario.verifies, "publishing");

    // The size guard watches the reference connector's destination, which is a file in the
    // run directory. A real subject's destination is remote and unbounded by anything the
    // harness can see, so there the watcher would poll a path that never exists.
    let destination = run_dir.join("destination.sqlite");
    let result = match external {
        None => tokio::select! {
            result = execute(&stack, scenario, &plan, external) => result,
            err = watch_destination_size(&destination) => Err(err),
        },
        Some(_) => execute(&stack, scenario, &plan, external).await,
    };

    // Clean up whether or not the scenario passed, so repeated runs do not
    // accumulate debris. The run directory is left behind on failure: its trace is
    // the only record of what the shim actually did.
    if let Err(err) = stack.delete_prefix(&names.prefix).await {
        tracing::error!(%err, prefix = %names.prefix, "failed to delete the run's tasks");
    }

    // A real subject's tables outlive its specifications too, and unlike the ops journals
    // the harness *can* reach these — through `testctl`, which calls the same
    // `Materializer.DeleteResource` the connectors' own integration tests call. The harness
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
            if table == catalog::TABLE_STANDARD && !plan.standard_binding {
                continue; // Never materialized, so never created.
            }
            let resource = catalog::resource_config(Some(&external.shape), &names, table, delta);
            if let Err(err) = stack.drop_resource(external, &resource).await {
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
    plan: &catalog::Plan<'_>,
    external: Option<&subject::External>,
) -> anyhow::Result<Outcome> {
    // Taken from the plan rather than passed alongside it: the plan already carries both, and
    // two ways to reach the same value is two values to keep in step.
    let (names, run_dir) = (plan.names, plan.run_dir);

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
    await_first_message(&trace, deadline)
        .await
        .context(Environment::BeforePerturbation)?;
    tracing::info!(elapsed = ?activating.elapsed(), "sink connector started");

    // The gap between the connector being spoken to and its first *non-empty* transaction
    // is the sink sitting idle while its captures activate and produce. Measured
    // separately because it, not the commit cadence, is what varies between runs.
    let feeding = std::time::Instant::now();
    await_first_documents(&trace, deadline)
        .await
        .context(Environment::BeforePerturbation)?;
    tracing::info!(elapsed = ?feeding.elapsed(), "workload feeding the sink");

    let warmed = std::time::Instant::now();
    await_commits(&trace, scenario.warmup_commits, deadline)
        .await
        .context(Environment::BeforePerturbation)?;
    tracing::info!(elapsed = ?warmed.elapsed(), commits = scenario.warmup_commits, "warmed up");

    // A scenario that scales out *because of* the fault has to see it land first,
    // or the two race and the run is no longer testing the sequence it describes.
    // The crash leaves the task down, so the split lands while nothing is writing
    // and the work is finished by the larger set of shards that replaces it.
    if scenario.split_after_fault {
        await_faults(&trace, scenario.faults.len(), deadline)
            .await
            .context(Environment::BeforePerturbation)?;
    }

    if scenario.split_shards {
        tracing::info!(task = %names.sink, "splitting shards");
        // Marked, unlike the gates below it: this is the split being *issued*, and a failure here
        // means the perturbation never happened at all.
        stack
            .split_shards(&names.sink)
            .await
            .context(Environment::BeforePerturbation)?;
        // Both children must come up before the run can continue; a split that
        // wedges is itself a finding — which is why this carries no marker. See [`Environment`].
        recover(
            stack,
            plan,
            &names.sink,
            &trace,
            count_commits(&read_trace(&trace)?) + 1,
            deadline,
        )
        .await?;
    }

    if scenario.join_shards {
        // Every child must commit *for itself* before the join, not merely two commits between
        // them; `commits_per_split_shard` is where that requirement is explained.
        //
        // Unmarked, and that is the correction rather than an oversight: this gate runs *after*
        // the split, and the only scenario that joins injects no fault, so the split is its
        // perturbation. It pairs `ignore-key-range`, whose signature is children fencing each
        // other off — exactly the state that stops them committing for themselves. Marked, the
        // defective half would report a caught defect as "the run failed before its fault fired",
        // i.e. as the environment's doing. See [`Environment`].
        await_commits_each_shard(&trace, 2, 2, deadline).await?;

        tracing::info!(task = %names.sink, "joining shards");
        stack.join_shards(&names.sink).await?;
        recover(
            stack,
            plan,
            &names.sink,
            &trace,
            count_commits(&read_trace(&trace)?) + 1,
            deadline,
        )
        .await?;
    }

    // The fault must actually have fired, or the scenario is vacuous.
    let faults_fired = await_faults(&trace, scenario.faults.len(), deadline)
        .await
        .context(Environment::BeforePerturbation)?;

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
    let after = count_commits(&read_trace(&trace)?) + scenario.settle_commits;
    // `recover` returns only once the task has reached `after`, so it is the gate — an
    // `await_commits(after)` here was a guaranteed no-op, and reading like a second check made
    // the first look weaker than it is.
    recover(stack, plan, &names.sink, &trace, after, deadline).await?;
    tracing::info!(elapsed = ?settled.elapsed(), commits = scenario.settle_commits, "settled");

    // Stop the workload and let the materialization drain. Only this run's own
    // captures are touched; scenarios never disable or restart anything
    // stack-wide, which is what makes a shared stack safe for concurrent runs.
    tracing::info!("disabling the workload to reach quiescence");
    let quiesced = std::time::Instant::now();
    stack.publish(&catalog::disable_captures(plan)?).await?;

    // Then wait for the captures to have actually *stopped*, not merely been asked to.
    //
    // The publication returns once the spec is stored, and activation carries it to the data
    // plane afterwards, so the capture is still writing at that moment.
    // `read_collection_when_final` guarded this with a plateau — two equal reads three seconds
    // apart — and a capture that paused across that window read as finished. The destination then
    // held 74 documents the expectation did not, which looks exactly like a connector delivering
    // them twice: 131 violations against a materialization that was right. Once the capture is
    // stopped nothing can append, so the plateau below is confirming rather than deciding.
    await_stopped(
        stack,
        &[&names.source_merged, &names.source_log],
        finality_timeout,
    )
    .await
    .context(Environment::WorkloadWouldNotStop)?;
    tracing::info!(elapsed = ?quiesced.elapsed(), "quiesced");

    // Panic, not an error: a `Subject` with no argv cannot be constructed by anything here, so
    // this is an impossible state rather than a condition to report up the stack.
    let connector = std::path::PathBuf::from(
        plan.subject
            .connector
            .first()
            .expect("a Subject always names its connector binary"),
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
    )
    .context(Environment::CollectionUnread)?;
    let merged_expected = Expectation::from_documents(merged_expected);
    let log_expected = Expectation::from_documents(log_expected);
    tracing::info!(elapsed = ?read.elapsed(), "read the collections");

    let drained = std::time::Instant::now();
    // The reference connector reads its own destination; a real one is read through
    // `testctl`, which calls the same functions the connectors' integration tests do.
    let via = match external {
        Some(external) => stack::ReadVia::Testctl(external),
        None => stack::ReadVia::Reference {
            connector: &connector,
            config: &plan.subject.config,
        },
    };

    let destination = drain(
        stack,
        via,
        plan,
        (&merged_expected, &log_expected),
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
            // Uncapped: order is not recoverable at all through this read, so there is no
            // volume of disorder that would mean anything.
            max_suppressed: None,
            conditional_on: None,
        });
    }

    // A duplicate in the *collection* is not a connector defect and must not be reported as one:
    // it means the harness's own comparison has stopped being sound, because the expectation folds
    // a repeated `(id, seq)` to one document while a reducing binding would sum it twice. Every
    // count that follows would then be wrong in a direction that looks exactly like a connector
    // over-delivering. This is refused rather than recorded, so a run can never quietly conclude
    // "exactly-once" from numbers it could not have compared.
    let duplicated =
        bindings.merged_expected.duplicated_documents + bindings.log_expected.duplicated_documents;
    if duplicated != 0 {
        let _ = dump_evidence(run_dir, &bindings);
        return Err(anyhow::anyhow!(
            "the collection read surfaced {duplicated} repeated (id, seq) document(s), which the \
             expectation folds to one but a reducing binding would count twice. No invariant can \
             be judged against it; see evidence.json in {run_dir:?}",
        )
        .context(Environment::UnsoundWorkload));
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

    Ok(Outcome {
        scenario: scenario.name,
        violations,
        exempted,
        faults_fired,
        documents,
        run_dir: run_dir.to_path_buf(),
    })
}

/// Write what a failing run compared, so the next reader does not have to guess which side
/// was wrong — the destination, or the expectation read from the collection.
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
    // `DocumentIntegrity` is not exemptable, and this is where that is enforced rather than left
    // to review. A connector may deliver a document twice or in a surprising order and still be
    // doing its declared job; none has a reason to *alter* one in transit. The check used to be
    // filed under `OracleAgreement`, where `at-least-once-never-loses`'s duplication exemption
    // silenced it — which is how a corruption check came to be switched off by a scenario about
    // duplication.
    assert!(
        !exemptions
            .iter()
            .any(|e| e.invariant == Invariant::DocumentIntegrity),
        "a scenario declares an exemption for {}, which nothing may exempt",
        Invariant::DocumentIntegrity,
    );

    // An exemption whose stated cause did not occur does not apply. See
    // [`Exemption::conditional_on`]: evaluated over the raw violations, so a duplicate this
    // scenario also exempts still counts as having happened.
    let exemptions: Vec<&Exemption> = exemptions
        .iter()
        .filter(|e| match e.conditional_on {
            None => true,
            Some(cause) => violations.iter().any(|v| v.invariant == cause),
        })
        .collect();

    // Ceilings are per *invariant*, not per exemption, because a run can carry more than one
    // exemption for the same invariant — a scenario's own, plus the blanket monotonicity exemption
    // a remotely-read destination gets — and the broadest claim has to govern. So an unbounded
    // exemption removes the ceiling: it says order is not recoverable through this read at all,
    // after which no volume of disorder means anything, and failing the run on the narrower
    // exemption's ceiling would hold a subject to a claim nobody made about it.
    let mut ceilings: BTreeMap<Invariant, Option<usize>> = BTreeMap::new();
    for exemption in &exemptions {
        ceilings
            .entry(exemption.invariant)
            .and_modify(|ceiling| {
                *ceiling = match (*ceiling, exemption.max_suppressed) {
                    (Some(a), Some(b)) => Some(a.max(b)),
                    _ => None,
                }
            })
            .or_insert(exemption.max_suppressed);
    }

    // An exemption that has absorbed more than it claimed stops absorbing anything: the whole
    // invariant reverts to being held, so the run fails with every one of those violations in its
    // report rather than with a count. Dropping only the excess would be arbitrary — the
    // violations are a set and nothing distinguishes the ones "within budget" — and would hide the
    // shape of what happened behind whichever ones survived the cut.
    let overrun: Vec<(Invariant, usize, usize)> = ceilings
        .iter()
        .filter_map(|(invariant, ceiling)| {
            let max = (*ceiling)?;
            let count = violations
                .iter()
                .filter(|v| v.invariant == *invariant)
                .count();
            (count > max).then_some((*invariant, count, max))
        })
        .collect();

    let (mut held, exempted) = violations.into_iter().partition::<Vec<_>, _>(|v| {
        !ceilings.contains_key(&v.invariant)
            || overrun
                .iter()
                .any(|(invariant, _, _)| *invariant == v.invariant)
    });

    // Named as its own violation so the report says *why* an exempted invariant is being held,
    // which the reverted violations alone would not explain.
    for (invariant, count, max) in overrun {
        held.push(Violation {
            invariant,
            detail: format!(
                "this scenario exempts {invariant} for at most {max} violation(s) and the run \
                 produced {count}, which is more than its justification accounts for: {}",
                exemptions
                    .iter()
                    .filter(|e| e.invariant == invariant)
                    .map(|e| e.justification.as_str())
                    .collect::<Vec<_>>()
                    .join(" / "),
            ),
        });
    }

    (held, exempted)
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

    // Only the *final* line may be partial, and only because a concurrent append can leave it
    // that way — it will be complete on the next poll. An unparseable line anywhere else is a
    // torn write from two shims appending at once, and swallowing it silently is how a lost
    // event turns into a gate that times out for no visible reason. So it is a warning naming
    // the line, and the read continues: a partial trace still answers most gates.
    let mut events = Vec::new();
    let lines: Vec<String> = std::io::BufReader::new(file)
        .lines()
        .collect::<std::io::Result<_>>()
        .context("reading the trace")?;
    let last = lines.len().saturating_sub(1);

    for (n, line) in lines.iter().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str(line) {
            Ok(event) => events.push(event),
            Err(_) if n == last => break, // Being appended to right now.
            Err(err) => tracing::warn!(
                line = %n + 1,
                %err,
                "a trace line could not be parsed; a gate may now time out without cause"
            ),
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
/// which doesn't match committed_close (...) or hinted_close (...)`, 26 times in a
/// restart loop, because the checkpoint predates the log's close.
///
/// Restarts are folded together: a shard that crashed and came back is one shard, and
/// the commits of both its processes count towards its total.
fn commits_per_split_shard(events: &[TraceEvent]) -> BTreeMap<(u32, u32), u64> {
    let mut range_of: BTreeMap<u32, (u32, u32)> = BTreeMap::new();
    for event in events {
        if let Event::Opened {
            key_begin, key_end, ..
        } = event.event
        {
            range_of.insert(event.pid, (key_begin, key_end));
        }
    }

    let mut commits: BTreeMap<(u32, u32), u64> = BTreeMap::new();
    for event in events {
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
    commits
}

/// Wait until every shard a split produced has committed `each` transactions of its
/// own. See [`commits_per_split_shard`] for why a task-wide count will not do.
async fn await_commits_each_shard(
    run: &RunDir,
    shards: usize,
    each: u64,
    timeout: std::time::Duration,
) -> anyhow::Result<()> {
    poll_trace(run, timeout, std::time::Duration::from_secs(2), |trace| {
        let commits = commits_per_split_shard(trace);

        match commits.len() >= shards && commits.values().all(|&n| n >= each) {
            true => {
                tracing::info!(?commits, "every split shard has committed for itself");
                Ok(())
            }
            false => Err(format!(
                "timed out waiting for {shards} shards to commit {each} transactions each; \
                 saw {commits:?}"
            )),
        }
    })
    .await
}

fn count_commits(events: &[TraceEvent]) -> u64 {
    events
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
        .count() as u64
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
/// that. After a third of the budget it escalates to [`restart_task`], which is where the
/// reasoning for that heavier remedy lives.
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

    let mut last_commits = count_commits(&read_trace(run)?);
    let mut stalled_since = std::time::Instant::now();

    loop {
        let commits = count_commits(&read_trace(run)?);
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

/// Poll the shim's trace until it satisfies `decide`, or the deadline passes.
///
/// Every gate below is this shape — read the trace, decide from it, sleep — and the only
/// thing that genuinely differs is what a timeout *means*, which is why the message is the
/// caller's. Any failure the shim recorded is appended to it: a gate that timed out because
/// the connector died should say so rather than report only the count it wanted.
async fn poll_trace<T>(
    run: &RunDir,
    timeout: std::time::Duration,
    interval: std::time::Duration,
    mut decide: impl FnMut(&[TraceEvent]) -> Result<T, String>,
) -> anyhow::Result<T> {
    let deadline = std::time::Instant::now() + timeout;

    loop {
        let trace = read_trace(run)?;
        let unmet = match decide(&trace) {
            Ok(value) => return Ok(value),
            Err(unmet) => unmet,
        };
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "{unmet}{}",
            trace_failures(run),
        );
        tokio::time::sleep(interval).await;
    }
}

async fn await_commits(
    run: &RunDir,
    target: u64,
    timeout: std::time::Duration,
) -> anyhow::Result<()> {
    poll_trace(run, timeout, std::time::Duration::from_secs(2), |trace| {
        let commits = count_commits(trace);
        match commits >= target {
            true => Ok(()),
            false => Err(format!(
                "timed out waiting for {target} committed transactions; saw {commits}"
            )),
        }
    })
    .await
}

/// Wait until every named task has stopped writing; see [`stack::Stack::is_stopped`].
async fn await_stopped(
    stack: &stack::Stack,
    tasks: &[&str],
    timeout: std::time::Duration,
) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + timeout;

    loop {
        let mut running = Vec::new();
        for task in tasks {
            if !stack.is_stopped(task).await? {
                running.push(*task);
            }
        }
        if running.is_empty() {
            return Ok(());
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "these tasks were still running {timeout:?} after being disabled: {}",
            running.join(", "),
        );
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

/// Wait until the shim has traced anything at all, which is the sink's connector being
/// spoken to for the first time.
async fn await_first_message(run: &RunDir, timeout: std::time::Duration) -> anyhow::Result<()> {
    poll_trace(
        run,
        timeout,
        std::time::Duration::from_millis(250),
        |trace| match trace.is_empty() {
            false => Ok(()),
            true => Err("timed out waiting for the sink's connector to be started".to_string()),
        },
    )
    .await
}

/// Wait until a transaction has carried at least one document, which means the captures
/// are producing and the sink is being fed.
async fn await_first_documents(run: &RunDir, timeout: std::time::Duration) -> anyhow::Result<()> {
    poll_trace(
        run,
        timeout,
        std::time::Duration::from_millis(250),
        |trace| {
            let fed = trace.iter().any(|e| match &e.event {
                Event::Stored { per_binding } => per_binding.iter().any(|n| *n != 0),
                _ => false,
            });
            match fed {
                true => Ok(()),
                false => Err("timed out waiting for the workload to feed the sink".to_string()),
            }
        },
    )
    .await
}

async fn await_faults(
    run: &RunDir,
    expected: usize,
    timeout: std::time::Duration,
) -> anyhow::Result<usize> {
    if expected == 0 {
        return Ok(0); // The baseline scenario injects nothing.
    }
    poll_trace(run, timeout, std::time::Duration::from_secs(2), |trace| {
        let fired = trace
            .iter()
            .filter(|e| matches!(e.event, Event::Fault { .. }))
            .count();
        match fired >= expected {
            true => Ok(fired),
            false => Err(format!(
                "timed out waiting for {expected} fault(s) to fire; {fired} did"
            )),
        }
    })
    .await
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
    via: stack::ReadVia<'_>,
    plan: &catalog::Plan<'_>,
    (merged_expected, log_expected): (&Expectation, &Expectation),
    timeout: std::time::Duration,
) -> anyhow::Result<Contents> {
    // The plan, rather than the three fields of it this needs: it is the same plan the catalog
    // was built from, which is the point — a read asks for the resource the connector was
    // actually given, named exactly as the catalog named it.
    let (names, standard_binding) = (plan.names, plan.standard_binding);
    let resource = |table: &str, delta: bool| {
        catalog::resource_config(plan.resource_shape, names, table, delta)
    };

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
                    .read_destination(via, &resource(catalog::TABLE_STANDARD, false))
                    .await?,
            ),
            false => None,
        };

        let contents = Contents {
            standard,
            merged_delta: stack
                .read_destination(via, &resource(catalog::TABLE_MERGED_DELTA, true))
                .await?,
            log: stack
                .read_destination(via, &resource(catalog::TABLE_LOG, true))
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

        // The delta binding's row count is in here as well as its seq-derived progress, because
        // the two see different things: `merged_delivered` is derived from the highest sequence
        // reached, so rows arriving that do *not* advance any sequence — which is exactly what a
        // duplicate looks like — left the total unchanged and read as settled. Free, and strictly
        // more than the gate saw before.
        let total = contents.log.len() + merged_delivered + contents.merged_delta.len();

        // Complete, and then confirmed unchanged by one further poll before the contents are
        // handed to the checkers.
        //
        // The gate is "at least as many as the collection holds", so it is met the moment the
        // last expected document lands — and returning there hands over a destination that a
        // duplicate still in flight has yet to reach. Every duplication check would then be
        // racing the very thing it looks for, and would win the race exactly when the connector
        // is slow. One quiet poll is not proof that nothing more is coming, but it is the
        // difference between "we looked after it settled" and "we looked at the first moment it
        // could have passed".
        if total == previous && contents.log.len() >= log_expected.documents() && merged_complete {
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
        // A listing that *errors* is not evidence the task is unhealthy, but it has to be
        // treated as such to make progress — so it is logged. Folded silently into `false`, a
        // persistently failing listing reported as "stuck unhealthy", which is a real state
        // with a different cause and sends the reader looking in the wrong place.
        let healthy = match stack.all_primary(&names.sink).await {
            Ok(healthy) => healthy,
            Err(err) => {
                tracing::warn!(%err, task = %names.sink, "could not list shards; assuming unhealthy");
                false
            }
        };

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

        // A complete destination that will not hold still is not a shortfall, and must not be
        // reported as one. It is what a connector writing without end looks like, and the
        // duplication checks are the right place to say so — reaching them needs the contents,
        // so the deadline hands them over rather than erroring.
        if expired && contents.log.len() >= log_expected.documents() && merged_complete {
            tracing::warn!(
                log = contents.log.len(),
                "the destination was complete but never settled; verifying it anyway"
            );
            return Ok(contents);
        }

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
            //
            // Of the three ways to get here only two are findings. "Went quiet" and "stuck
            // unhealthy" are states the subject put the task in; the *deadline* is the runner
            // running out of patience, which says nothing about the subject and must not be
            // scored as a defect caught — so it alone is tagged as the environment.
            let err = anyhow::anyhow!(
                "the destination stopped short of the collections ({short}); \
                 reason={}, task healthy={healthy}",
                match (quiet, stuck) {
                    (true, _) => "went quiet",
                    (_, true) => "stuck unhealthy",
                    _ => "deadline",
                },
            );
            return Err(match quiet || stuck {
                true => err,
                false => err.context(Environment::DrainDeadline),
            });
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
            max_suppressed: None,
            conditional_on: None,
        }];

        let (held, exempt) = partition_exempt(violations, &exemptions);
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].invariant, Invariant::NoLoss);
        assert_eq!(exempt.len(), 1);
    }

    #[test]
    fn an_exemption_over_its_ceiling_holds_the_invariant_after_all() {
        let violations = (0..3)
            .map(|i| Violation {
                invariant: Invariant::NoDuplicates,
                detail: format!("duplicated {i}"),
            })
            .collect();
        let exemptions = vec![Exemption {
            invariant: Invariant::NoDuplicates,
            justification: "one replayed transaction".to_string(),
            max_suppressed: Some(2),
            conditional_on: None,
        }];

        // All three revert, plus the violation naming the overrun.
        let (held, exempt) = partition_exempt(violations, &exemptions);
        assert_eq!(held.len(), 4);
        assert!(exempt.is_empty());
    }

    /// An exemption licensed by duplication does not apply to a run that duplicated nothing.
    ///
    /// This is the hole it closes: a subject whose replay path corrupts merged values without
    /// emitting extra rows breaks oracle agreement and conservation, both of which
    /// `at-least-once-never-loses` exempts, while every per-document count stays clean — so it
    /// passed the whole suite.
    #[test]
    fn an_exemption_does_not_apply_without_its_stated_cause() {
        let oracle = || Violation {
            invariant: Invariant::OracleAgreement,
            detail: "reduced balance disagrees with its oracle".to_string(),
        };
        let exemptions = vec![Exemption {
            invariant: Invariant::OracleAgreement,
            justification: "a duplicated document leaves the balance disagreeing".to_string(),
            max_suppressed: None,
            conditional_on: Some(Invariant::NoDuplicates),
        }];

        // Nothing was duplicated, so the licence does not apply and the subject is held.
        let (held, exempt) = partition_exempt(vec![oracle()], &exemptions);
        assert_eq!(held.len(), 1);
        assert!(exempt.is_empty());

        // A duplicate did occur, so the same divergence is licensed. Counted over the raw
        // violations, so a duplicate the scenario also exempts still counts as having happened.
        let duplicated = Violation {
            invariant: Invariant::NoDuplicates,
            detail: "delivered twice".to_string(),
        };
        let (held, exempt) = partition_exempt(vec![oracle(), duplicated], &exemptions);
        assert_eq!(held.len(), 1, "the duplicate itself is not exempt here");
        assert_eq!(held[0].invariant, Invariant::NoDuplicates);
        assert_eq!(exempt.len(), 1);
    }

    /// The case a real subject hits: its blanket exemption is unbounded, and a scenario's
    /// narrower ceiling for the same invariant must not fail it.
    #[test]
    fn an_unbounded_exemption_lifts_a_narrower_ceiling() {
        let violations = (0..3)
            .map(|i| Violation {
                invariant: Invariant::Monotonicity,
                detail: format!("out of order {i}"),
            })
            .collect();
        let exemptions = vec![
            Exemption {
                invariant: Invariant::Monotonicity,
                justification: "one replayed transaction".to_string(),
                max_suppressed: Some(2),
                conditional_on: None,
            },
            Exemption {
                invariant: Invariant::Monotonicity,
                justification: "this destination is read as an unordered table".to_string(),
                max_suppressed: None,
                conditional_on: None,
            },
        ];

        let (held, exempt) = partition_exempt(violations, &exemptions);
        assert!(held.is_empty());
        assert_eq!(exempt.len(), 3);
    }
}

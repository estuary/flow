//! The scenarios, and the defects they must catch.
//!
//! Two rules govern everything here, and they are the reason the suite can be
//! trusted:
//!
//! 1. **A scenario is keyed on protocol events, never on document identity.**
//!    Which documents land in which transaction varies between runs — transaction
//!    boundaries are shaped by the runtime's duration policy and a rate-paced
//!    capture, not by a document count the spec can set — so an assertion that
//!    depended on it would be a flake. This costs nothing because verification is
//!    invariant-based rather than snapshot-based.
//!
//! 2. **A scenario without a paired defect is not finished.** Every scenario
//!    declares a `defect` it provably catches, and the suite runs it both ways:
//!    clean, where it must pass, and defective, where it must fail. A checker that
//!    later goes blind through refactoring is then itself a test failure, rather
//!    than a green result that means nothing.

use crate::harness::Exemption;
use crate::invariants::Invariant;
use crate::protocol::{Action, FaultRule, ShardTarget, Trigger};
use crate::reference::{Class, Defect};

/// The connector under test: what the shim `exec`s, and the endpoint config it is
/// given.
///
/// The harness overwrites `path` for the reference connector, whose destination belongs
/// to the run; a real connector's config is passed through untouched.
#[derive(Clone)]
pub struct Subject {
    pub connector: Vec<String>,
    pub config: serde_json::Value,
}

pub struct Scenario {
    pub name: &'static str,
    /// The invariant this scenario exists to verify, in one line. Reported on
    /// failure so the result names the property rather than the mechanism.
    pub verifies: &'static str,
    /// Class the reference connector is configured as, and the class whose mechanism
    /// this scenario was written to exercise.
    pub class: Class,
    /// Classes expected to *pass* this scenario, and so the classes it runs against
    /// when the subject is a real connector.
    ///
    /// Wider than [`Scenario::class`] on purpose. A fault a connector must survive is
    /// rarely a property of how it divides durability with the runtime: a crash mid-`Store`
    /// must lose nothing whether the connector fences a remote checkpoint, stages queries
    /// for a post-commit merge, or counts the rows a channel accepted. And exemptions are
    /// permissive, so a scenario written against one class holds another to a weaker or
    /// differently-shaped property rather than an impossible one.
    ///
    /// Defaults to every class claiming exactly-once. [`Class::AtLeastOnce`] is left out
    /// because it duplicates by construction, so exactly-once invariants would fail against
    /// it for a guarantee it never made; the scenarios that do hold it to something opt it
    /// back in. Narrow this further only where one class alone can succeed — and say why.
    pub applies_to: &'static [Class],
    pub faults: Vec<FaultRule>,
    /// The defect this scenario must catch. `None` only for the baseline, whose
    /// job is to fail when the harness itself is miswired — it has no defect to
    /// pair with because it injects nothing.
    pub defect: Option<Defect>,
    /// Split every shard of the task in two, after the warmup.
    pub split_shards: bool,
    /// Split only once the fault has fired, rather than while the task is healthy.
    ///
    /// Turns a race into a sequence: the fault crashes the connector, the task is scaled
    /// out while it is down, and it comes back with more shards than staged the work. A
    /// scenario that splits a *running* task is asking a different question, so this is
    /// opt-in rather than the default.
    pub split_after_fault: bool,
    /// Join the task's shards pairwise, after the split has settled.
    ///
    /// Only meaningful together with `split_shards`: a task starts with one shard,
    /// so there is nothing to join until it has been split.
    pub join_shards: bool,
    /// Committed transactions to observe before perturbing anything.
    pub warmup_commits: u64,
    /// Committed transactions to observe after the fault, proving the task
    /// recovered rather than merely stopped.
    pub settle_commits: u64,
    /// Invariants this scenario does not hold the subject to.
    pub exempt: Vec<Exemption>,
    /// A limitation of the *runtime* — not of the subject — that this scenario is
    /// currently expected to expose, for the classes it exposes it to.
    ///
    /// Set only where the runtime is known to violate a guarantee a correct connector
    /// depends on. For an exposed class the scenario is an *expected failure*: it runs, and
    /// it fails with its violation count, which is the measurement of the gap. It is
    /// deliberately not silenced — a scenario excused from failing is one nobody reads
    /// again — and the marker is removed once the runtime closes the gap, at which point it
    /// becomes an ordinary passing scenario.
    ///
    /// A gap is scoped to classes rather than the whole scenario because exposure to one is
    /// a property of how a class writes, not of the perturbation. The same membership change
    /// landing on the same prepared transaction is unsurvivable for a class that has already
    /// written to the destination and ordinary for a class that has only staged work, so the
    /// scenario runs for both and only the exposed one is excused from passing.
    pub known_limitation: Option<RuntimeGap>,
}

/// A runtime guarantee that is missing, and the classes it leaves exposed.
pub struct RuntimeGap {
    /// Classes for which this scenario is an expected failure. A class absent from this
    /// list must pass the scenario normally.
    pub classes: &'static [Class],
    /// Which guarantee is missing and why the exposed classes cannot work around it.
    pub detail: &'static str,
}

/// Most reordering a membership-change exemption may absorb.
///
/// Uncapped, these would have been the suite's widest blind spot, and on precisely the scenarios
/// where reordering bugs live. Against the reference connector order *is* recoverable — its tables
/// carry an autoincrementing `ord`, so a read replays the sequence of appends — so a defect that
/// shuffled thousands of rows while keeping the set exactly right would be absorbed in full.
///
/// 500 against a measured 9-54 per run: an order-of-magnitude guard, not a tight bound, chosen the
/// same way and for the same reason as the duplication ceiling in `at-least-once-never-loses`. It
/// binds only on the reference connector: a remotely-read subject also carries the blanket
/// monotonicity exemption, which is unbounded because order is not recoverable through a table
/// scan at all, and an unbounded exemption lifts a narrower ceiling for the same invariant.
const REORDERING_CEILING: usize = 500;

/// Why a membership change is not held to delivery order.
///
/// Stated once and shared: four scenarios reconfigure shards and every one of them owes
/// the same explanation, so a copy per scenario would only give the wording room to drift.
///
/// The wording is deliberately about what is *observed*. An earlier version asserted the
/// interleaving — a split child delivering a sequence the departing parent had raced past —
/// and review could not construct it: on these classes' delta paths a parent write past a
/// child's resume point is either fence-refused or a duplicate, and `NoDuplicates` is not
/// exempt, so the run would fail regardless. Yet these exemptions suppress 9-33 violations
/// per run, measured after the two monotonicity checkers were made to agree, so the
/// reordering is real. Rather than keep a mechanism nobody has demonstrated, the
/// justification records the observation and says the cause is unknown.
const MEMBERSHIP_CHANGE_REORDERS: &str = "A membership change does not preserve delivery *order* at the sink, only \
         exactly-once delivery of the set: rows of an id are observed landing out of \
         order across a split while remaining exactly one row per document. The \
         mechanism is NOT understood — the obvious candidate, a parent delivering past \
         a child's resume point, is either fenced off or a duplicate, and duplicates are \
         not exempt. What is established is the observation: 9-33 such violations per \
         run, with the set-based checks passing. Those checks — no-loss, no-duplicates, \
         conservation and oracle agreement — carry the exactly-once claim here and are \
         NOT exempt, which is why an unexplained ordering deviation can be tolerated \
         without weakening what the scenario proves.";

/// Why a class that appends during `Store` is not held to delivery order.
///
/// Same treatment as [`MEMBERSHIP_CHANGE_REORDERS`], and for the same reason: the stated
/// mechanism was more confident than the evidence. Rows of an uncommitted transaction being
/// visible until recovery skips past them explains why *uncommitted* rows appear, not why
/// committed ones arrive out of order — and a re-opened channel appending the same `(id, seq)`
/// again is a duplicate, which is not exempt. The 40-54 violations these suppress per run are
/// the part that is established.
const APPENDS_DURING_STORE_REORDERS: &str = "This class appends during Store, so rows of a transaction that never commits \
         stay visible until recovery skips past them — and separately, delivery order at \
         the sink is observed not advancing monotonically across a recovery or membership \
         change, at 40-54 violations per run. The mechanism for the latter is NOT \
         understood: a re-opened channel re-appending the same row would be a duplicate, \
         and duplicates are not exempt. The set-based checks carry the exactly-once claim \
         and are NOT exempt, so what the scenario proves does not rest on this.";

/// The exactly-once classes a membership change can be *fairly* asked of.
///
/// Excludes the counted channel, which writes during `Store`: when a membership change lands
/// on a transaction whose rows are already in the destination, the children open channels at
/// offset zero and append the replay a second time. That is the runtime gap discussion 2581
/// names, and `split-lands-on-prepared-transaction` is where it is measured.
///
/// The four scenarios excluded here reach that state only by race — a split lands
/// mid-transaction nearly always rather than always, and only once a batch has been appended.
/// Asking a counted channel a question whose answer is a coin flip would report the runtime's
/// gap as the connector's defect on some runs and pass it on others, which is worse than not
/// asking. Two of them — `split-during-commit` and `split-after-commit-before-apply` — split
/// while the task is *down* after a crash, which is deterministic in when the split lands but
/// not in what the counted channel had already appended before dying, so the coin flip is the
/// same one.
///
/// And `split-lands-on-prepared-transaction` is no more deterministic; it simply carries the
/// gap declaration instead. Its own comment records the two attempts to force the overlap that
/// both suppressed it.
///
/// Note this is *not* true of `crash-in-split-leader` and `crash-in-split-non-leader`: they
/// crash after the split has settled, so the replay happens under stable membership, and a
/// counted channel handles it — which is why those two hold it to a clean pass.
const MEMBERSHIP_CHANGE_FAIRLY_ASKED: &[Class] =
    &[Class::RemoteAuthoritative, Class::PostCommitApply];

/// The classes claiming exactly-once, which is the default applicability set.
const EXACTLY_ONCE: &[Class] = &[
    Class::RemoteAuthoritative,
    Class::PostCommitApply,
    Class::DocumentCounter,
];

/// Every class, for the scenarios whose invariants still hold where duplicates are
/// permitted — either because nothing is replayed, or because duplication is exempt.
const EVERY_CLASS: &[Class] = &[
    Class::RemoteAuthoritative,
    Class::PostCommitApply,
    Class::DocumentCounter,
    Class::AtLeastOnce,
];

impl Scenario {
    fn new(name: &'static str, verifies: &'static str, class: Class) -> Self {
        Self {
            name,
            verifies,
            class,
            applies_to: EXACTLY_ONCE,
            faults: Vec::new(),
            defect: None,
            split_shards: false,
            split_after_fault: false,
            join_shards: false,
            warmup_commits: 3,
            settle_commits: 3,
            exempt: Vec::new(),
            known_limitation: None,
        }
    }

    /// Whether the task materializes a standard (merge) binding as well as the two delta ones.
    ///
    /// Derived from the class rather than stored, because it was only ever assigned from the
    /// class — and a stored copy invited a guard asserting the two agreed, which could not fail.
    ///
    /// The document-counter class cannot take one: a counted channel's offset is a count of rows
    /// the destination *accepted*, which says nothing about an upsert. Snowpipe Streaming v2
    /// handles delta-updates bindings only, and folding a merge binding into that model would
    /// emulate something no such connector does.
    pub fn standard_binding(&self) -> bool {
        !matches!(self.class, Class::DocumentCounter)
    }

    /// See [`Scenario::applies_to`]. Widens or narrows which classes the scenario runs
    /// against when the subject is a real connector.
    fn applies_to(mut self, classes: &'static [Class]) -> Self {
        assert!(
            classes.contains(&self.class),
            "{}: a scenario must apply to the class it is written against",
            self.name,
        );
        self.applies_to = classes;
        self
    }

    /// Split every shard after the warmup, settling for `settle_commits` afterwards.
    ///
    /// Paired because they always travel together: a membership change costs the task a
    /// recovery, so a scenario that splits needs more committed transactions afterwards to show
    /// it recovered rather than merely stopped.
    fn splitting(mut self, settle_commits: u64) -> Self {
        self.split_shards = true;
        self.settle_commits = settle_commits;
        self
    }

    /// As [`Scenario::splitting`], but the split is issued only once the fault has fired — so the
    /// task is scaled out while it is *down*, and comes back with more shards than staged its work.
    ///
    /// For a crash, which is the only fault this is used with. It is not a general way to order a
    /// split against a fault: pairing it with a stall closed the window it was meant to open,
    /// because a runtime handed a shard that will hold still finishes the transaction and hands
    /// over at a quiet point. See `split-lands-on-prepared-transaction`.
    fn splitting_after_fault(mut self, settle_commits: u64) -> Self {
        self.split_after_fault = true;
        self.splitting(settle_commits)
    }

    /// As [`Scenario::splitting`], then joins the children back together.
    fn splitting_then_joining(mut self, settle_commits: u64) -> Self {
        self.join_shards = true;
        self.splitting(settle_commits)
    }

    fn fault(mut self, rule: FaultRule) -> Self {
        self.faults.push(rule);
        self
    }

    /// See [`Scenario::known_limitation`]. Takes the same shape as an exemption —
    /// a justification long enough to have said something — because the cost of a
    /// scenario that cannot fail is that someone must be able to audit why.
    fn blocked_on_runtime(mut self, classes: &'static [Class], detail: &'static str) -> Self {
        assert!(
            detail.len() >= 40,
            "state which runtime guarantee is missing, not just that one is",
        );
        assert!(!classes.is_empty(), "name the classes the gap exposes");
        self.known_limitation = Some(RuntimeGap { classes, detail });
        self
    }

    fn catches(mut self, defect: Defect) -> Self {
        self.defect = Some(defect);
        self
    }

    /// A subject built from the reference connector for this scenario's class,
    /// optionally with the paired defect enabled.
    ///
    /// `defective` is what makes the suite prove itself: the same scenario, the
    /// same faults, the same checkers, and an outcome that must flip.
    pub fn subject(&self, connector: &std::path::Path, defective: bool) -> Subject {
        let defects: Vec<Defect> = match (defective, self.defect) {
            (true, Some(defect)) => vec![defect],
            _ => Vec::new(),
        };

        Subject {
            connector: vec![connector.to_string_lossy().to_string()],
            config: serde_json::json!({
                // Replaced by the harness with this run's destination.
                "path": "",
                "class": self.class,
                "defects": defects,
            }),
        }
    }
}

/// Every scenario the suite runs.
pub fn all() -> Vec<Scenario> {
    vec![
        baseline(),
        crash_between_commits(),
        crash_mid_store(),
        crash_at_flush(),
        split_during_store(),
        split_during_commit(),
        split_after_commit_before_apply(),
        split_lands_on_prepared_transaction(),
        join_after_split(),
        zombie_at_start_commit(),
        destination_ahead_of_checkpoint(),
        recovery_reconciles_with_destination(),
        crash_in_split_leader(),
        crash_in_split_non_leader(),
        at_least_once_never_loses(),
    ]
}

/// A no-fault run. Its job is to fail when the harness is miswired: a scenario
/// suite that cannot see a wiring problem would report every other scenario as a
/// pass for the wrong reason.
fn baseline() -> Scenario {
    Scenario::new(
        "baseline",
        "an unperturbed materialization upholds every invariant",
        Class::RemoteAuthoritative,
    )
    // Nothing is perturbed, so nothing is replayed, so not even the at-least-once class
    // has an opportunity to duplicate. Every class must pass this one.
    .applies_to(EVERY_CLASS)
}

/// The window the Snowpipe Streaming v2 work verified by hand in production: the
/// connector's work is durable and the recovery log has committed, but the process
/// dies before the two are reconciled.
///
/// The crash is keyed on the `Acknowledged` *response*, the earliest point at which the
/// connector has finished applying a transaction and the shim can still kill it — not the only
/// one, since a crash anywhere up to the next recovery-log commit replays the same
/// `Acknowledge`, but the earliest, so the least is happening around it. Restarting there
/// replays that `Acknowledge`, and only an idempotent one leaves the destination unchanged.
fn crash_between_commits() -> Scenario {
    Scenario::new(
        "crash-between-commits",
        "a crash after applying a committed transaction replays its Acknowledge \
         without applying it twice",
        Class::PostCommitApply,
    )
    .fault(FaultRule::crash_at(Trigger::Acknowledged, 5))
    .catches(Defect::NonIdempotentAcknowledge)
}

/// A transaction that never reached `StartCommit` never happened. Anything it left
/// in the destination must not be applied a second time by the replay.
///
/// Armed after three commits so the crash lands in a transaction of a task that has
/// established a rhythm, rather than in its first — and, since the warmup is three, never
/// inside the warmup gate, which has no recovery step.
fn crash_mid_store() -> Scenario {
    Scenario::new(
        "crash-mid-store",
        "a crash mid-Store, before StartCommit, leaves nothing behind that \
         double-applies on replay",
        Class::RemoteAuthoritative,
    )
    .fault(FaultRule::crash_at(Trigger::Store, 25).armed_after(3))
    .catches(Defect::CommitDuringStore)
}

/// A crash at the boundary between the load and store phases: loads are done,
/// stores have not started, and the reductions the next transaction computes must
/// be unaffected.
///
/// Paired with document-dropping rather than a commit-timing defect, because at
/// this point in a transaction there is nothing yet staged for a commit-timing
/// defect to mishandle — the property under test is that the interruption costs no
/// data.
fn crash_at_flush() -> Scenario {
    Scenario::new(
        "crash-at-flush",
        "a crash between the load and store phases does not corrupt subsequent \
         reductions or lose documents",
        Class::RemoteAuthoritative,
    )
    .fault(FaultRule::crash_at(Trigger::Flush, 4))
    .catches(Defect::DropDocuments)
}

/// Scale-out during the store phase. A split also manufactures a zombie by design:
/// the runtime fences the source shard's primary off its recovery log during the
/// children's recovery and then unassigns it, so this exercises the runtime's
/// fencing alongside the connector's.
fn split_during_store() -> Scenario {
    Scenario::new(
        "split-during-store",
        "splitting a task's shards mid-transaction preserves exactly-once semantics",
        Class::RemoteAuthoritative,
    )
    .catches(Defect::IgnoreKeyRange)
    .declaring(Invariant::Monotonicity, MEMBERSHIP_CHANGE_REORDERS)
    .at_most(REORDERING_CEILING)
    .applies_to(MEMBERSHIP_CHANGE_FAIRLY_ASKED)
    .splitting(5)
}

/// A transaction dies *before* committing, the task is scaled out while it is down, and the
/// replay is finished by a larger set of shards than staged it.
///
/// Read the fault carefully, because the name misleads. The shim fires a fault *before*
/// forwarding the request that triggered it, so a `Crash` on a request trigger kills the
/// connector before it receives that request — see [`Trigger`]. So "crash at `StartCommit` #4"
/// means the connector never sees `StartCommit` #4: rows of transaction 4 are staged (in whole
/// batches of 64; the remainder was in memory and died with the process), no statements were
/// rendered, and no state patch was published. Nothing in any checkpoint names those rows.
///
/// What that verifies is still worth having, and it is not what the old wording claimed. It is
/// the abandoned-staging hazard: staging whose transaction never committed must never be applied,
/// and the destination cannot distinguish it from staging awaiting application. An earlier version
/// of this connector decided by inspecting the destination and applied abandoned work — landing on
/// exactly this recovery. The replayed transaction must then be delivered exactly once by shards
/// that did not stage it.
///
/// For committed-but-unapplied work crossing a membership change, see
/// [`split_after_commit_before_apply`]. That is a different state and needs its own scenario;
/// this one cannot reach it, because the transaction it interrupts never commits.
///
/// Post-commit-apply needs no fence for either. Its authority is the recovery log, and the
/// crashed session is gone rather than competing — what it needs is for applying staged work to
/// be repeatable, in any order, by whoever inherits it.
///
/// What makes it pass is the rule every real connector of this class follows: stage load keys as
/// `Load` requests arrive, and read the destination only once `Flush` has come. `Flush` is the
/// runtime's signal that the previous transaction was acknowledged by *every* shard — the
/// guarantee a coordinating connector needs and cannot obtain any other way, because one shard
/// applies staged work on behalf of its peers, so a peer reading earlier would reduce onto a base
/// that shard has not finished writing.
fn split_during_commit() -> Scenario {
    Scenario::new(
        "split-during-commit",
        "staging whose transaction never committed is never applied, and the replay is \
         delivered exactly once by the larger set of shards that replaces it",
        Class::PostCommitApply,
    )
    .fault(FaultRule::crash_at(Trigger::StartCommit, 4))
    .catches(Defect::IgnoreKeyRange)
    .declaring(Invariant::Monotonicity, MEMBERSHIP_CHANGE_REORDERS)
    .at_most(REORDERING_CEILING)
    .applies_to(MEMBERSHIP_CHANGE_FAIRLY_ASKED)
    .splitting_after_fault(5)
}

/// Work the log has committed but nobody has applied, crossing a membership change.
///
/// The state `split-during-commit` cannot reach, and until this existed no scenario did — which
/// left the predecessor-inheritance machinery untested: `peers` recovery at `Open`,
/// `merge_peer_patches`, and `apply_pending` over a range that no longer exists.
///
/// The fault is keyed on the `Acknowledge` *request*, and the timing is worth spelling out. The
/// runtime's cycle is `Acknowledge → Flush → Store → StartCommit → Persist`, so an `Acknowledge`
/// opens each transaction and confirms the one before it. Since the shim fires before forwarding,
/// crashing at `Acknowledge` #4 leaves transaction *3* in exactly the state wanted: its statements
/// were rendered at `StartCommit`, its state patch went into the recovery log at `Persist`, and
/// the apply that `Acknowledge` #4 would have performed never happened. The connector's in-memory
/// record of it died with the process, so recovery has only the checkpoint — which is the point.
///
/// Then the split. The pending entry is filed under the *departed parent's* range key, so each
/// child sees it as a peer's rather than its own, and only the primary may run it. Exactly once is
/// the claim, and `ignore-key-range` breaks it in the way that matters here: with every shard
/// claiming the whole keyspace, both children compute themselves primary, both find the entry
/// under their own range key, and both apply it.
fn split_after_commit_before_apply() -> Scenario {
    Scenario::new(
        "split-after-commit-before-apply",
        "staged work the log has committed is applied exactly once by the larger set of \
         shards that replaces the one which staged it",
        Class::PostCommitApply,
    )
    .fault(FaultRule::crash_at(Trigger::Acknowledge, 4))
    .catches(Defect::IgnoreKeyRange)
    .declaring(Invariant::Monotonicity, MEMBERSHIP_CHANGE_REORDERS)
    .at_most(REORDERING_CEILING)
    .applies_to(MEMBERSHIP_CHANGE_FAIRLY_ASKED)
    .splitting_after_fault(5)
}

/// The same window, against a counted channel — and this one cannot survive it.
///
/// A counted channel writes during `Store`, before the transaction commits, so rows of a
/// prepared-but-uncommitted transaction are already in the destination and cannot be taken
/// back. When the split lands inside that window the children open fresh channels at offset
/// zero, replay the same input, and append it a second time.
///
/// Post-commit-apply is not exposed to this: it applies only at `Acknowledge`, after the
/// log has committed, so an uncommitted transaction was never applied. Its own
/// `split-during-commit` is held to a clean result.
fn split_lands_on_prepared_transaction() -> Scenario {
    Scenario::new(
        "split-lands-on-prepared-transaction",
        "a membership change landing on a transaction already prepared for commit \
         neither loses nor duplicates its documents",
        Class::DocumentCounter,
    )
    // The overlap between this stall and the split is *not* synchronized, and cannot usefully be.
    // That was reviewed as a flaw and measured instead, and the measurements say the race is the
    // scenario rather than a defect in it.
    //
    // Two attempts to force the overlap both *closed* the window. Waiting for the stall to begin
    // before issuing the split, and lengthening the stall to twenty seconds, each made the run
    // pass — because a runtime handed a shard that will hold still finishes the stalled
    // transaction and hands over at a quiet point, which is a committed transaction and no hazard
    // at all. Asking the runtime to hand over at a moment of the harness's choosing is asking for
    // the very guarantee under test, so this is back to four seconds and an unordered split, the
    // configuration with observed hits.
    //
    // And when it does hit, it hits *narrowly*: a caught run delivered 2072 log rows against 2070
    // documents — two rows delivered twice, both of them documents the expectation holds, with
    // nothing ahead of it. So the gap below is intermittent, not per-run, and a passing run is
    // evidence about that run only. `split-during-commit` reaches the same destination state
    // deterministically by crashing rather than stalling, so coverage of the
    // prepared-but-uncommitted state does not rest on winning this race.
    .fault(FaultRule {
        on: Trigger::StartCommit,
        nth: 4,
        arm_after: 3,
        shard: ShardTarget::Any,
        action: Action::Stall { millis: 4_000 },
    })
    .catches(Defect::DropDocumentCounter)
    // This exemption shapes the *report* rather than any verdict, and the reasoning is worth
    // stating exactly because it is easy to get wrong. This scenario does **not** narrow
    // `applies_to`: every exactly-once class runs it, and the ones the gap does not expose
    // must pass. For the exposed class the `RuntimeGap` panic below fires before any
    // violation-based assertion, so the exemption decides nothing there either. What it does
    // is keep monotonicity noise out of the violation list that panic prints, so what remains
    // measures the gap. For a real subject of another class the blanket external monotonicity
    // exemption would cover the same violations anyway.
    .declaring(Invariant::Monotonicity, APPENDS_DURING_STORE_REORDERS)
    .at_most(REORDERING_CEILING)
    // Written against the counted-channel class, which the gap below leaves unable to pass
    // it, but the perturbation is not class-specific: a split landing on a prepared
    // transaction is something every class must survive. A class that only *stages* during
    // Store has nothing in the destination for the children to append twice, so it is
    // expected to pass — and this is the scenario that says so.
    .blocked_on_runtime(
        &[Class::DocumentCounter],
        "Intermittently — the runtime usually completes the transaction it is in before handing \
         the range over, so this fails on some runs and not others, and a caught run duplicated \
         two rows of 2070. Read a pass as evidence about that run and nothing more. \
         The runtime does not yet guarantee that a transaction started under a given shard \
         split is replayed under that same split before a scale up or down takes effect, a \
         capability named as a requirement in estuary/flow discussion 2581. A counted \
         channel cannot work around it: it writes during Store, so the rows of a prepared \
         transaction are already in the destination when the split lands, and the children \
         open fresh channels at offset zero and append them again. Scaling down has the \
         mirror image — a survivor reads one departing channel's counter, skips too few, \
         and duplicates.",
    )
    .splitting(5)
}

/// Scaling back down. A join is not a split run backwards: one shard absorbs
/// another's key range and the other is deleted, so every key the departing shard
/// still owed work for has to be picked up by the survivor — and the survivor's
/// destination state was accumulated under a narrower range than it now owns.
///
/// The connector cannot inherit a checkpoint for the widened range, because two
/// ranges collapsing into one leaves no single range that contained it; recovery
/// falls back to the recovery log. That asymmetry with a split is real, which is
/// why this asserts only on the destination and never on which checkpoint the
/// connector chose.
fn join_after_split() -> Scenario {
    Scenario::new(
        "join-after-split",
        "joining a task's shards back together preserves exactly-once semantics",
        Class::RemoteAuthoritative,
    )
    .catches(Defect::IgnoreKeyRange)
    .declaring(Invariant::Monotonicity, MEMBERSHIP_CHANGE_REORDERS)
    .at_most(REORDERING_CEILING)
    .applies_to(MEMBERSHIP_CHANGE_FAIRLY_ASKED)
    // Settles longer than a split alone: this waits out a split, then a join, and each
    // membership change costs the task a recovery.
    .splitting_then_joining(8)
}

/// Fencing under real concurrency rather than in isolation: two real connector processes over one
/// destination, both fed the runtime's own messages, the older thawed to commit a transaction the
/// newer has already superseded.
fn zombie_at_start_commit() -> Scenario {
    Scenario::new(
        "zombie-at-start-commit",
        "a zombie instance racing the active one at StartCommit cannot corrupt the \
         destination",
        Class::RemoteAuthoritative,
    )
    // Frozen at `Open` because that is the only point a fenced instance is certainly still
    // alive; see `Action::Zombie`. It was keyed at `Store` #10 of the second transaction, which
    // read as "let the zombie work for a while first" and was in fact "freeze whatever is left of
    // it": the zombie had been refused at the first transaction's commit and exited, so the freeze
    // suspended nothing and the thaw resumed nothing. The scenario passed by racing no one.
    //
    // Frozen at `Open` it has taken its fence and nothing more, and everything the runtime sends
    // afterwards is queued up to the first `StartCommit`. On thaw it replays that transaction
    // whole — loads and stores against a destination that has moved on two commits — and only
    // then attempts the commit its fence must refuse.
    .fault(FaultRule {
        on: Trigger::Open,
        nth: 1,
        arm_after: 0,
        shard: ShardTarget::Any,
        action: Action::Zombie {
            thaw_after_commits: 2,
        },
    })
    .catches(Defect::SkipFenceCheck)
    // The only scenario a single class can be asked, and the reason is in `Zombie`: both
    // instances are expected to fence at `Open`, and the live one waits for the zombie to
    // get there first so that it holds the newer nonce. A class that does not fence gives
    // the harness nothing to order the two by, and they proceed as two live writers to one
    // destination for the whole run — which is not a zombie, and against a real warehouse
    // simply contends until neither makes progress. Idempotency is the other classes'
    // answer to a zombie, and `crash-between-commits` is where they are held to it.
    .applies_to(&[Class::RemoteAuthoritative])
}

/// The document-counter class's central claim, and the reason it can offer
/// exactly-once at all: the destination's channel advanced without a recovery-log
/// commit, so recovery must skip exactly what the destination already holds.
///
/// The crash is at `StartedCommit` — the connector has appended and reported its
/// count, and the recovery log has *not* committed — so the runtime replays that
/// transaction's documents into a destination that already holds them.
fn destination_ahead_of_checkpoint() -> Scenario {
    Scenario::new(
        "destination-ahead-of-checkpoint",
        "a destination holding rows the committed checkpoint does not cover has them \
         applied once, not twice",
        Class::DocumentCounter,
    )
    .fault(FaultRule::crash_at(Trigger::StartedCommit, 4))
    .catches(Defect::DropDocumentCounter)
    // No monotonicity exemption, unlike the membership-change scenarios. Nothing here
    // reorders delivery: there is no membership change, the replayed input is byte-identical
    // journal order, and the recovery skip is a per-binding prefix count. Measured over three
    // runs, an exemption here suppressed nothing — and one that suppresses nothing makes the
    // exemption list a worse map of where the fleet is actually weak.
}

/// The same interruption, against a connector that trusts its own checkpoint
/// instead of reconciling it with the destination.
///
/// Note what this does *not* cover: a destination genuinely *behind* the
/// checkpoint, which a correct connector refuses rather than guesses at. No fault
/// the shim can inject produces that state — it needs the destination tampered with
/// from outside — so the refusal path is implemented and unexercised. See the
/// design document.
fn recovery_reconciles_with_destination() -> Scenario {
    Scenario::new(
        "recovery-reconciles-with-destination",
        "recovery reconciles what the destination actually holds against the \
         checkpoint rather than trusting the checkpoint alone",
        Class::DocumentCounter,
    )
    .fault(FaultRule::crash_at(Trigger::StartedCommit, 5))
    .catches(Defect::ResetCounterOnOpen)
    // No monotonicity exemption, for the same reason as `destination-ahead-of-checkpoint`.
}

/// The two membership-change scenarios a counted channel can actually survive.
///
/// A counted channel resumes by asking the *destination* how far it got, so a shard
/// that has just been created — with a fresh channel and therefore a zero offset —
/// needs no inherited state at all. That is the property post-commit-apply staging
/// cannot have (see the design document: a child inheriting staged work cannot tell
/// whether its own resume point precedes it), and it is why Snowpipe Streaming v2
/// uses a channel rather than staged files.
///
/// The two scenarios below crash a split shard, and they are kept apart because the
/// two shards fail in different ways and conflating them makes a result unreadable.
/// The split alone is a weak perturbation, though not for the reason first written here: a
/// split does *not* reliably land at a transaction boundary — the harness cannot ask for one
/// there, and a transaction is nearly always in flight when a split takes effect (see "Any
/// split scenario passes through that window" in the design document). What is true is that
/// a split-only scenario cannot be *relied on* to create the replay these defects need, so it
/// passes in both halves often enough to establish nothing. Adding a crash makes the replay
/// certain rather than incidental.
///
/// Neither is the prepared-transaction window: the split has fully landed before the
/// crash in both, so a correct connector recovers and the limitation recorded in the
/// design document does not apply here.

/// Crashing the shard a split produced which is *also* shard zero. It owns half the
/// keyspace and holds the task's recovery log, so the runtime restarts it the way it
/// restarts an unsplit shard, and what is being tested is the connector: its own
/// channel, its own offset, crashing with appends the checkpoint does not know about.
/// Recovery has to ask the destination how far *this* channel got.
fn crash_in_split_leader() -> Scenario {
    Scenario::new(
        "crash-in-split-leader",
        "a shard created by a split, whose leader then crashes, delivers each document \
         exactly once — inheriting neither its parent's resume point nor a blank one",
        Class::DocumentCounter,
    )
    .fault(FaultRule::crash_at(Trigger::StartedCommit, 2).in_shard(ShardTarget::SplitLeader))
    .catches(Defect::DropDocumentCounter)
    .declaring(Invariant::Monotonicity, APPENDS_DURING_STORE_REORDERS)
    .at_most(REORDERING_CEILING)
    .splitting(5)
}

/// Crashing a *non-zero* shard a split produced, then bringing the task back up.
///
/// This is the harder question, and a different one from the leader's. A non-zero
/// shard of a V2 task is stateless: no recovery log, its state arriving by leader
/// broadcast. Killing its connector EOFs the fan-in stream and fails the leader too,
/// so the whole task goes down — `expected leader message ... unexpected EOF` — and
/// the shard is then rebuilt from nothing rather than replayed from a log.
///
/// So the scenario asks two things at once, and both are worth knowing. Can the task
/// be brought back at all after losing a participant; and once back, does the
/// connector still hold exactly-once, given the rebuilt shard has to rediscover from
/// the destination how far its channel got.
fn crash_in_split_non_leader() -> Scenario {
    Scenario::new(
        "crash-in-split-non-leader",
        "a stateless non-zero shard rebuilt after its crash still delivers each \
         document exactly once",
        Class::DocumentCounter,
    )
    .fault(FaultRule::crash_at(Trigger::StartedCommit, 2).in_shard(ShardTarget::SplitNonLeader))
    .catches(Defect::DropDocumentCounter)
    .declaring(Invariant::Monotonicity, APPENDS_DURING_STORE_REORDERS)
    .at_most(REORDERING_CEILING)
    .splitting(5)
}

/// A connector that makes a weaker guarantee is still held to the guarantee it does
/// make. The exemptions are the whole point: declared, justified, and narrow — and
/// loss is not among them.
///
/// The crash is at `StartedCommit`, before the recovery log commits, so the
/// transaction *is* replayed and this class *does* duplicate. A clean run therefore
/// exercises the exemptions rather than passing because nothing happened.
fn at_least_once_never_loses() -> Scenario {
    Scenario::new(
        "at-least-once-never-loses",
        "an at-least-once connector never loses data, though it may duplicate",
        Class::AtLeastOnce,
    )
    // The weakest ask in the suite: with duplication and everything downstream of it
    // exempt, what remains is "lose nothing", which every class claims. A stronger class
    // passes without using the exemptions, and that is worth running rather than assuming.
    .applies_to(EVERY_CLASS)
    .fault(FaultRule::crash_at(Trigger::StartedCommit, 4))
    .catches(Defect::DropDocuments)
    // Every exemption below is licensed by *one* replayed transaction, and all four are therefore
    // tied to `NoDuplicates` with `caused_by`: a replay that re-delivers documents leaves duplicate
    // rows, so if no duplicate row appears anywhere in the run then nothing was replayed and a
    // divergence has some other cause. Without that tie, this scenario licensed an oracle
    // disagreement from *any* cause — and a subject whose replay path corrupted merged values
    // while emitting no extra rows broke only oracle agreement and conservation, both exempt,
    // and passed the entire suite.
    //
    // Only the duplicate count carries a ceiling, and that is the other half of the same lesson.
    // One transaction is tens of documents: a reference run measures 59-69 duplicates over ~5,200,
    // so 500 is an order-of-magnitude guard — far above any single replay, far below the
    // systematic re-delivery of a whole workload. The oracle-agreement count cannot be bounded
    // usefully because the *checker* bounds it: at most three violations per account in
    // `check_standard` and two in `check_merged_delta` over forty accounts, so nothing above ~200
    // can ever bind and the 500 that used to sit there was decoration. Monotonicity is per-row and
    // could carry one, but its cause is now stated exactly, which is the stronger claim.
    .declaring(
        Invariant::NoDuplicates,
        "At-least-once by construction: this class commits during Store with no \
         record of what it applied, so an interrupted transaction is re-applied on \
         replay. Declared rather than fixed, because the weaker guarantee is the one \
         the connector offers.",
    )
    .at_most(500)
    .declaring(
        Invariant::Conservation,
        "Conservation is arithmetic over delivered documents, so a duplicate can break it \
         as surely as a loss would — but only sometimes, and the difference is worth \
         knowing before anyone deletes this. The workload is double-entry: every transfer \
         is a matched pair of legs, and replaying a whole transaction re-applies both, so \
         the sum still balances and nothing fires. It fires when a pair straddles the \
         replayed transaction's boundary, because then one leg is duplicated without its \
         partner. Transaction boundaries are time-based, so that is uncommon rather than \
         impossible, and this exemption measures zero on most runs. Zero here means rare, \
         not unnecessary: removing it would make this scenario fail intermittently.",
    )
    .caused_by(Invariant::NoDuplicates)
    .declaring(
        Invariant::OracleAgreement,
        "A duplicated document leaves the reduced balance disagreeing with its own \
         oracle. Same cause as the duplication exemption above.",
    )
    .caused_by(Invariant::NoDuplicates)
    .declaring(
        Invariant::Monotonicity,
        "Re-applying an interrupted transaction re-delivers sequences the sink has \
         already seen. Same cause as the duplication exemption above.",
    )
    .caused_by(Invariant::NoDuplicates)
}

impl Scenario {
    /// Declare an invariant this scenario's subject is not held to, with the
    /// reasoning. The justification is a required argument rather than an optional
    /// field, so an exemption cannot be added without stating why.
    fn declaring(mut self, invariant: Invariant, justification: &str) -> Self {
        self.exempt.push(Exemption {
            invariant,
            justification: justification.to_string(),
            max_suppressed: None,
            conditional_on: None,
        });
        self
    }

    /// Bound the exemption just declared; see [`Exemption::max_suppressed`].
    ///
    /// Worth setting only where the *checker* does not already bound the count. A per-account
    /// invariant cannot exceed the workload's forty accounts however wrong the subject is, so a
    /// ceiling there measures the workload rather than the subject.
    fn at_most(mut self, max_suppressed: usize) -> Self {
        self.last_exemption().max_suppressed = Some(max_suppressed);
        self
    }

    /// Hold the exemption just declared to its stated cause; see [`Exemption::conditional_on`].
    fn caused_by(mut self, invariant: Invariant) -> Self {
        self.last_exemption().conditional_on = Some(invariant);
        self
    }

    /// Panics rather than erroring: a modifier with nothing to modify is a typo in this file, not
    /// a condition a run could encounter.
    fn last_exemption(&mut self) -> &mut Exemption {
        self.exempt
            .last_mut()
            .expect("a modifier follows the `declaring` whose exemption it modifies")
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /// The operative rule of the whole suite, enforced mechanically so coverage
    /// cannot quietly erode as scenarios are added.
    #[test]
    fn every_scenario_but_the_baseline_pairs_with_a_defect() {
        for scenario in all() {
            if scenario.name == "baseline" {
                assert!(scenario.defect.is_none(), "the baseline injects nothing");
                continue;
            }
            assert!(
                scenario.defect.is_some(),
                "scenario {} has no paired defect, so it is not finished",
                scenario.name,
            );
        }
    }

    #[test]
    fn every_scenario_either_injects_a_fault_or_reconfigures_shards() {
        for scenario in all() {
            if scenario.name == "baseline" {
                continue;
            }
            assert!(
                !scenario.faults.is_empty() || scenario.split_shards || scenario.join_shards,
                "scenario {} perturbs nothing",
                scenario.name,
            );
        }
    }

    // No test that a joining scenario splits first, though a join does need more than one shard
    // and a task starts with one. `splitting_then_joining` is the only way to set `join_shards`
    // and it sets `split_shards` too, so the property holds by construction.

    /// No fault may be able to fire before the warmup gate is satisfied.
    ///
    /// `await_commits` for the warmup has no recovery step — deliberately, since nothing
    /// has been perturbed yet — so a crash landing inside that window leaves the shard
    /// FAILED with nobody to unassign it, and the run waits out its deadline instead of
    /// testing anything. Only a crash does this: a stall or a zombie leaves the
    /// shard running and the warmup still completes.
    ///
    /// The failure is intermittent, which is why this is a test and not a review note: a
    /// crash armed one commit short of the warmup fires only when the last warmup
    /// transaction happens to reach the occurrence count first.
    #[test]
    fn no_fault_can_fire_before_the_warmup_completes() {
        for scenario in all() {
            for rule in &scenario.faults {
                // Only a crash matters. A stall or a zombie leaves the shard
                // running, so the warmup gate keeps making progress through them —
                // `zombie-at-start-commit` fires at the session's `Open` and is fine.
                if rule.action != Action::Crash {
                    continue;
                }
                // A rule restricted to a split shard cannot fire before the warmup: the
                // split happens after it, so those sessions do not exist yet.
                if rule.shard != ShardTarget::Any {
                    continue;
                }
                // `arm_after: N` arms the rule once N transactions have committed, so
                // the earliest it can fire is transaction N+1.
                let armed_at = match rule.on {
                    // Counted per transaction: the occurrence recurs every transaction,
                    // so arming alone decides where it lands.
                    Trigger::Store | Trigger::Load => rule.arm_after + 1,
                    // Counted per session, once per transaction, so occurrence `nth`
                    // falls in transaction `nth`.
                    _ => rule.nth.max(rule.arm_after + 1),
                };
                assert!(
                    armed_at > scenario.warmup_commits,
                    "scenario {} arms a {:?} fault at transaction {armed_at}, which is not \
                     past its warmup of {} commits. The warmup gate does not recover a \
                     failed shard, so the run would wait out its deadline rather than \
                     verify anything.",
                    scenario.name,
                    rule.on,
                    scenario.warmup_commits,
                );
            }
        }
    }

    /// Every scenario that reconfigures shards must be in the nextest group that caps
    /// how many of them run at once.
    ///
    /// These are the scenarios that stress shard membership, and the bound leaves the
    /// broker allocator headroom an unbounded fan-out would not. Checked here rather than
    /// left to review, because the cost of forgetting lands on whoever is debugging the
    /// unrelated-looking scenario that flakes.
    #[test]
    fn every_shard_reconfiguring_scenario_is_capped() {
        let config = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.config/nextest.toml"),
        )
        .expect("reading the workspace nextest configuration");

        for scenario in all() {
            if !(scenario.split_shards || scenario.join_shards) {
                continue;
            }
            // Test function names are the scenario name with dashes replaced.
            let test_fn = scenario.name.replace('-', "_");
            assert!(
                config.contains(&format!("test({test_fn})")),
                "scenario {} reconfigures shards but is not in the \
                 `capped-shard-reconfiguration` group in .config/nextest.toml, so an \
                 unbounded number of them can reconfigure at once and starve the allocator",
                scenario.name,
            );
        }
    }

    // No test that the counted-channel class never takes a merge binding:
    // `Scenario::standard_binding` derives it from the class, so the property holds by
    // construction and a test of it could not fail. It used to be a stored field, and the test
    // read as a real guard while asserting that one line of `new` did what it says.

    /// A zombie rule must fire at `Open`, and this is where that is enforced.
    ///
    /// `Action::Zombie` documents it, and documentation is not enough: the freeze was keyed at a
    /// `Store` for a long time, which reads as letting the zombie work first and is in fact
    /// freezing whatever is left of it. A fenced instance does not survive being run — its first
    /// commit is refused and the process exits — so the freeze suspended a corpse, the thaw
    /// resumed nothing, and the clean half of the scenario raced no one while still passing.
    ///
    /// The shim logs that case to its trace, which is read only when a gate times out, so on a
    /// vacuous pass nobody sees it. This fails at compile-and-test time instead.
    #[test]
    fn a_zombie_is_frozen_at_open() {
        for scenario in all() {
            for rule in &scenario.faults {
                if matches!(rule.action, Action::Zombie { .. }) {
                    assert_eq!(
                        (rule.on, rule.nth),
                        (Trigger::Open, 1),
                        "{}: a zombie must be frozen at the session's Open, where it has taken \
                         its fence and cannot yet have been refused a commit",
                        scenario.name,
                    );
                }
            }
        }
    }

    #[test]
    fn scenario_names_are_unique() {
        let mut names: Vec<_> = all().iter().map(|s| s.name).collect();
        let count = names.len();
        names.sort();
        names.dedup();
        assert_eq!(names.len(), count, "duplicate scenario names");
    }

    /// Every defect the reference connector implements is reachable by some
    /// scenario. An unpaired defect is dead code that suggests coverage the suite
    /// does not have.
    #[test]
    fn every_defect_is_paired_with_a_scenario() {
        // Only pairings that can actually run. A scenario blocked on a runtime gap for its
        // own class panics before its defective half, so counting its `catches` here would let
        // a defect look covered by a pairing that never executes — which is exactly what this
        // guard claimed to prevent while collecting from every scenario.
        let paired: Vec<Defect> = all()
            .iter()
            .filter(|s| {
                !s.known_limitation
                    .as_ref()
                    .is_some_and(|gap| gap.classes.contains(&s.class))
            })
            .filter_map(|s| s.defect)
            .collect();

        // Iterated rather than re-listed: `Defect::ALL` exists because a copy at each use
        // site drifts, and a copy here would silently stop covering a defect added later.
        // A scenario blocked on a runtime gap for its *own* class never reaches its defect
        // pairing: `both_ways` panics with EXPECTED FAILURE before running the defective half.
        // So its `catches` is a claim nothing tests, and counting it here would let a defect
        // look covered by a pairing that cannot execute. Named rather than filtered silently,
        // so the inventory of what is genuinely unpaired stays visible.
        let unexercised: Vec<Defect> = all()
            .iter()
            .filter(|s| {
                s.known_limitation
                    .as_ref()
                    .is_some_and(|gap| gap.classes.contains(&s.class))
            })
            .filter_map(|s| s.defect)
            .collect();
        assert_eq!(
            unexercised,
            vec![Defect::DropDocumentCounter],
            "the set of defects whose only pairing cannot run has changed; if a defect is now \
             paired only with a scenario that is an expected failure for its own class, that \
             defect is effectively unpaired",
        );

        for defect in Defect::ALL {
            assert!(paired.contains(&defect), "no scenario catches {defect:?}",);
        }
    }
}

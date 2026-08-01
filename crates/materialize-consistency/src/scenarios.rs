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

/// The connector under test.
pub struct Subject {
    /// The connector binary and its arguments, as the shim will `exec` it.
    pub connector: Vec<String>,
    /// Endpoint configuration. The harness overwrites `path` with the run's own
    /// destination.
    pub config: serde_json::Value,
}

pub struct Scenario {
    pub name: &'static str,
    /// The invariant this scenario exists to verify, in one line. Reported on
    /// failure so the result names the property rather than the mechanism.
    pub verifies: &'static str,
    /// Class the subject must implement for the scenario to mean anything.
    pub class: Class,
    /// Whether the task materializes a standard (merge) binding as well as the two
    /// delta ones.
    ///
    /// The document-counter class cannot take one: a counted channel's offset is a
    /// count of rows the destination *accepted*, which says nothing about an upsert.
    /// Snowpipe Streaming v2 handles delta-updates bindings only, and folding a merge
    /// binding into that model would emulate something no such connector does.
    pub standard_binding: bool,
    pub faults: Vec<FaultRule>,
    /// The defect this scenario must catch. `None` only for the baseline, whose
    /// job is to fail when the harness itself is miswired — it has no defect to
    /// pair with because it injects nothing.
    pub defect: Option<Defect>,
    /// Split every shard of the task in two, after the warmup.
    pub split_shards: bool,
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
    /// currently expected to expose, and which no connector can work around.
    ///
    /// Set only where the runtime is known to violate a guarantee a correct connector
    /// depends on. Such a scenario is an *expected failure*: it runs, and it fails with
    /// its violation count, which is the measurement of the gap. It is deliberately not
    /// silenced — a scenario excused from failing is one nobody reads again — and the
    /// marker is removed once the runtime closes the gap, at which point it becomes an
    /// ordinary passing scenario.
    pub known_limitation: Option<&'static str>,
}

impl Scenario {
    fn new(name: &'static str, verifies: &'static str, class: Class) -> Self {
        Self {
            name,
            verifies,
            class,
            standard_binding: !matches!(class, Class::DocumentCounter),
            faults: Vec::new(),
            defect: None,
            split_shards: false,
            join_shards: false,
            warmup_commits: 3,
            settle_commits: 3,
            exempt: Vec::new(),
            known_limitation: None,
        }
    }

    fn fault(mut self, rule: FaultRule) -> Self {
        self.faults.push(rule);
        self
    }

    /// See [`Scenario::known_limitation`]. Takes the same shape as an exemption —
    /// a justification long enough to have said something — because the cost of a
    /// scenario that cannot fail is that someone must be able to audit why.
    fn blocked_on_runtime(mut self, limitation: &'static str) -> Self {
        assert!(
            limitation.len() >= 40,
            "state which runtime guarantee is missing, not just that one is",
        );
        self.known_limitation = Some(limitation);
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
        replayed_acknowledge_is_a_no_op(),
        crash_mid_store(),
        crash_at_flush(),
        split_during_store(),
        split_during_commit(),
        counter_split_during_commit(),
        join_after_split(),
        zombie_at_start_commit(),
        counter_resumes_from_the_destination(),
        counter_reconciles_rather_than_trusting_its_checkpoint(),
        counter_crash_in_split_leader(),
        counter_crash_in_split_non_leader(),
        delta_replay_is_deduplicated(),
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
}

/// The window the Snowpipe Streaming v2 work verified by hand in production: the
/// connector's work is durable and the recovery log has committed, but the process
/// dies before the two are reconciled.
///
/// The crash is keyed on the `Acknowledged` *response*, which is the only point
/// where the connector has finished applying a transaction and the shim can still
/// kill it. Restarting there replays the same `Acknowledge`, and only an idempotent
/// one leaves the destination unchanged.
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

/// The runtime is free to retry `Acknowledge` as many times as it needs, so every
/// replay after the first must be a no-op — with no crash involved at all.
fn replayed_acknowledge_is_a_no_op() -> Scenario {
    Scenario::new(
        "replayed-acknowledge",
        "Acknowledge replayed repeatedly with no crash is a no-op after the first",
        Class::PostCommitApply,
    )
    .fault(FaultRule {
        on: Trigger::Acknowledge,
        nth: 4,
        arm_after: 0,
        shard: ShardTarget::Any,
        action: Action::Replay { times: 3 },
    })
    .catches(Defect::NonIdempotentAcknowledge)
}

/// A transaction that never reached `StartCommit` never happened. Anything it left
/// in the destination must not be applied a second time by the replay.
///
/// Armed after two commits so the crash lands in a transaction of a task that has
/// established a rhythm, rather than in its first.
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
    let mut scenario = Scenario::new(
        "split-during-store",
        "splitting a task's shards mid-transaction preserves exactly-once semantics",
        Class::RemoteAuthoritative,
    )
    .catches(Defect::IgnoreKeyRange)
    .declaring(
        Invariant::Monotonicity,
        "A membership change does not preserve delivery *order* at the sink, only \
         exactly-once delivery of the set. A split child resumes from its inherited \
         checkpoint and may deliver a sequence the departing parent had already \
         raced past, so an id's rows can land out of order while remaining exactly \
         one row per document. The set-based checks — no-loss, no-duplicates, \
         conservation and oracle agreement — carry the exactly-once claim here and \
         are NOT exempt.",
    );
    scenario.split_shards = true;
    scenario.settle_commits = 5;
    scenario
}

/// The same, with the split landing while a transaction is being committed rather
/// than accumulated — the rule the scale-out design depends on.
fn split_during_commit() -> Scenario {
    let mut scenario = Scenario::new(
        "split-during-commit",
        "a transaction prepared under one shard split is replayed under that same \
         split before a membership change takes effect",
        Class::PostCommitApply,
    )
    .fault(FaultRule {
        on: Trigger::StartCommit,
        nth: 4,
        arm_after: 0,
        shard: ShardTarget::Any,
        action: Action::Stall { millis: 4_000 },
    })
    .catches(Defect::IgnoreKeyRange)
    .declaring(
        Invariant::Monotonicity,
        "A membership change does not preserve delivery *order* at the sink, only \
         exactly-once delivery of the set. A split child resumes from its inherited \
         checkpoint and may deliver a sequence the departing parent had already \
         raced past, so an id's rows can land out of order while remaining exactly \
         one row per document. The set-based checks — no-loss, no-duplicates, \
         conservation and oracle agreement — carry the exactly-once claim here and \
         are NOT exempt.",
    );
    scenario.split_shards = true;
    scenario.settle_commits = 5;
    scenario
}

/// The same window, against a counted channel — and this one cannot survive it.
///
/// A counted channel writes during `Store`, before the transaction commits, so rows of a
/// prepared-but-uncommitted transaction are already in the destination and cannot be taken
/// back. When the split lands inside that window the children open fresh channels at offset
/// zero, replay the same input, and append it a second time.
///
/// Post-commit-apply is not exposed to this, which is why its own `split-during-commit`
/// scenario is held to a clean result: it applies only at `Acknowledge`, after the log has
/// committed, so an uncommitted transaction was never applied and the replay is clean.
fn counter_split_during_commit() -> Scenario {
    let mut scenario = Scenario::new(
        "counter-split-during-commit",
        "a counted channel is exposed to a membership change landing on a prepared \
         transaction, which no connector of that class can close",
        Class::DocumentCounter,
    )
    .fault(FaultRule {
        on: Trigger::StartCommit,
        nth: 4,
        arm_after: 3,
        shard: ShardTarget::Any,
        action: Action::Stall { millis: 4_000 },
    })
    .catches(Defect::DropDocumentCounter)
    .declaring(
        Invariant::Monotonicity,
        "This class appends during Store, so rows of a transaction that never commits \
         stay visible until recovery skips past them, and a membership change re-opens \
         channels at offsets the sink has already passed. Delivery order at the sink is \
         therefore not guaranteed to advance monotonically; the set-based checks carry \
         the exactly-once claim and are NOT exempt.",
    )
    .blocked_on_runtime(
        "The runtime does not yet guarantee that a transaction started under a given shard \
         split is replayed under that same split before a scale up or down takes effect, a \
         capability named as a requirement in estuary/flow discussion 2581. A counted \
         channel cannot work around it: it writes during Store, so the rows of a prepared \
         transaction are already in the destination when the split lands, and the children \
         open fresh channels at offset zero and append them again. Scaling down has the \
         mirror image — a survivor reads one departing channel's counter, skips too few, \
         and duplicates.",
    );
    scenario.split_shards = true;
    scenario.settle_commits = 5;
    scenario
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
    let mut scenario = Scenario::new(
        "join-after-split",
        "joining a task's shards back together preserves exactly-once semantics",
        Class::RemoteAuthoritative,
    )
    .catches(Defect::IgnoreKeyRange)
    .declaring(
        Invariant::Monotonicity,
        "A membership change does not preserve delivery *order* at the sink, only \
         exactly-once delivery of the set. A split child resumes from its inherited \
         checkpoint and may deliver a sequence the departing parent had already \
         raced past, so an id's rows can land out of order while remaining exactly \
         one row per document. The set-based checks — no-loss, no-duplicates, \
         conservation and oracle agreement — carry the exactly-once claim here and \
         are NOT exempt.",
    );

    scenario.split_shards = true;
    scenario.join_shards = true;
    // Longer than the split scenarios: this one waits out a split, then a join, and
    // each membership change costs the task a recovery.
    scenario.settle_commits = 8;
    scenario
}

/// Fencing under real concurrency rather than in isolation: two real connector
/// processes, both handling real runtime messages, the older one thawed after the
/// newer has committed.
fn zombie_at_start_commit() -> Scenario {
    Scenario::new(
        "zombie-at-start-commit",
        "a zombie instance racing the active one at StartCommit cannot corrupt the \
         destination",
        Class::RemoteAuthoritative,
    )
    .fault(FaultRule {
        on: Trigger::Store,
        nth: 10,
        arm_after: 1,
        shard: ShardTarget::Any,
        action: Action::Zombie {
            thaw_after_commits: 2,
        },
    })
    .catches(Defect::SkipFenceCheck)
}

/// The document-counter class's central claim, and the reason it can offer
/// exactly-once at all: the destination's channel advanced without a recovery-log
/// commit, so recovery must skip exactly what the destination already holds.
///
/// The crash is at `StartedCommit` — the connector has appended and reported its
/// count, and the recovery log has *not* committed — so the runtime replays that
/// transaction's documents into a destination that already holds them.
fn counter_resumes_from_the_destination() -> Scenario {
    Scenario::new(
        "counter-resumes-from-destination",
        "a destination ahead of the connector's checkpoint counter causes recovery \
         to skip exactly what it already holds",
        Class::DocumentCounter,
    )
    .fault(FaultRule::crash_at(Trigger::StartedCommit, 4))
    .catches(Defect::DropDocumentCounter)
    .declaring(
        Invariant::Monotonicity,
        "This class appends during Store, so rows of a transaction that never \
         commits are visible until recovery skips past them. Delivery order at the \
         sink across that boundary is therefore not guaranteed to advance \
         monotonically, though the contents of a committed transaction are still \
         exactly-once.",
    )
}

/// The same interruption, against a connector that trusts its own checkpoint
/// instead of reconciling it with the destination.
///
/// Note what this does *not* cover: a destination genuinely *behind* the
/// checkpoint, which a correct connector refuses rather than guesses at. No fault
/// the shim can inject produces that state — it needs the destination tampered with
/// from outside — so the refusal path is implemented and unexercised. See the
/// design document.
fn counter_reconciles_rather_than_trusting_its_checkpoint() -> Scenario {
    Scenario::new(
        "counter-reconciles-with-destination",
        "recovery reconciles the destination's committed count against the \
         checkpoint rather than trusting the checkpoint alone",
        Class::DocumentCounter,
    )
    .fault(FaultRule::crash_at(Trigger::StartedCommit, 5))
    .catches(Defect::ResetCounterOnOpen)
    .declaring(
        Invariant::Monotonicity,
        "Same cause as counter-resumes-from-destination: this class makes rows of an \
         uncommitted transaction visible, so sink delivery order across a recovery \
         boundary may not advance monotonically.",
    )
}

/// The one membership-change scenario whose class can actually survive it.
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
/// The split alone perturbs nothing worth checking: it lands at a transaction
/// boundary, so nothing is replayed, so no channel has anything to skip — and
/// skipping is the whole of this class's behaviour. Both `drop-document-counter` and
/// `ignore-key-range` are invisible to a split-only scenario, which passes in both halves
/// and establishes nothing. The fault has to create a replay for either defect to have
/// anything to get wrong.
///
/// Neither is the prepared-transaction window: the split has fully landed before the
/// crash in both, so a correct connector recovers and the limitation recorded in the
/// design document does not apply here.

/// Crashing the shard a split produced which is *also* shard zero. It owns half the
/// keyspace and holds the task's recovery log, so the runtime restarts it the way it
/// restarts an unsplit shard, and what is being tested is the connector: its own
/// channel, its own offset, crashing with appends the checkpoint does not know about.
/// Recovery has to ask the destination how far *this* channel got.
fn counter_crash_in_split_leader() -> Scenario {
    let mut scenario = Scenario::new(
        "counter-crash-in-split-leader",
        "a channel created by a shard split resumes from its own destination offset \
         after the leader crashes, not from its parent's and not from zero",
        Class::DocumentCounter,
    )
    .fault(FaultRule::crash_at(Trigger::StartedCommit, 2).in_shard(ShardTarget::SplitLeader))
    .catches(Defect::DropDocumentCounter)
    .declaring(
        Invariant::Monotonicity,
        "This class appends during Store, so rows of a transaction that never commits \
         stay visible until recovery skips past them, and a membership change re-opens \
         channels at offsets the sink has already passed. Delivery order at the sink is \
         therefore not guaranteed to advance monotonically; the set-based checks carry \
         the exactly-once claim and are NOT exempt.",
    );
    scenario.split_shards = true;
    scenario.settle_commits = 5;
    scenario
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
fn counter_crash_in_split_non_leader() -> Scenario {
    let mut scenario = Scenario::new(
        "counter-crash-in-split-non-leader",
        "a stateless non-zero shard rebuilt after its crash still delivers each \
         document exactly once",
        Class::DocumentCounter,
    )
    .fault(FaultRule::crash_at(Trigger::StartedCommit, 2).in_shard(ShardTarget::SplitNonLeader))
    .catches(Defect::DropDocumentCounter)
    .declaring(
        Invariant::Monotonicity,
        "This class appends during Store, so rows of a transaction that never commits \
         stay visible until recovery skips past them, and a membership change re-opens \
         channels at offsets the sink has already passed. Delivery order at the sink is \
         therefore not guaranteed to advance monotonically; the set-based checks carry \
         the exactly-once claim and are NOT exempt.",
    );
    scenario.split_shards = true;
    scenario.settle_commits = 5;
    scenario
}

/// Delta-updates bindings are where duplication is directly visible, as an extra
/// row. A connector claiming exactly-once has to deduplicate.
fn delta_replay_is_deduplicated() -> Scenario {
    Scenario::new(
        "delta-replay-deduplicated",
        "a replayed transaction does not duplicate rows of a delta-updates binding",
        Class::PostCommitApply,
    )
    .fault(FaultRule {
        on: Trigger::Acknowledge,
        nth: 3,
        arm_after: 0,
        shard: ShardTarget::Any,
        action: Action::Replay { times: 2 },
    })
    .catches(Defect::NonIdempotentAcknowledge)
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
    .fault(FaultRule::crash_at(Trigger::StartedCommit, 4))
    .catches(Defect::DropDocuments)
    .declaring(
        Invariant::NoDuplicates,
        "At-least-once by construction: this class commits during Store with no \
         record of what it applied, so an interrupted transaction is re-applied on \
         replay. Declared rather than fixed, because the weaker guarantee is the one \
         the connector offers.",
    )
    .declaring(
        Invariant::Conservation,
        "Conservation is arithmetic over delivered documents, so a duplicate breaks \
         it as surely as a loss would. Exempt for the same cause as duplication \
         itself.",
    )
    .declaring(
        Invariant::OracleAgreement,
        "A duplicated document leaves the reduced balance disagreeing with its own \
         oracle. Same cause as the duplication exemption above.",
    )
    .declaring(
        Invariant::StandardDeltaAgreement,
        "A duplicate applied to one binding and not the other leaves the two views \
         of the collection disagreeing. Same cause as the duplication exemption \
         above.",
    )
    .declaring(
        Invariant::Monotonicity,
        "Re-applying an interrupted transaction re-delivers sequences the sink has \
         already seen. Same cause as the duplication exemption above.",
    )
}

impl Scenario {
    /// Declare an invariant this scenario's subject is not held to, with the
    /// reasoning. The justification is a required argument rather than an optional
    /// field, so an exemption cannot be added without stating why.
    fn declaring(mut self, invariant: Invariant, justification: &str) -> Self {
        self.exempt.push(Exemption {
            invariant,
            justification: justification.to_string(),
            scope: crate::harness::Scope::Connector,
        });
        self
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

    /// A join needs more than one shard, and a task starts with one.
    #[test]
    fn a_join_scenario_splits_first() {
        for scenario in all() {
            if scenario.join_shards {
                assert!(
                    scenario.split_shards,
                    "scenario {} joins without splitting, so it has nothing to join",
                    scenario.name,
                );
            }
        }
    }

    /// No fault may be able to fire before the warmup gate is satisfied.
    ///
    /// `await_commits` for the warmup has no recovery step — deliberately, since nothing
    /// has been perturbed yet — so a crash landing inside that window leaves the shard
    /// FAILED with nobody to unassign it, and the run waits out its deadline instead of
    /// testing anything. Only a crash does this: a stall, replay or zombie leaves the
    /// shard running and the warmup still completes. `crash-mid-store` did exactly this: armed after 2 commits with a
    /// warmup of 3, it fired in the third transaction whenever that transaction reached
    /// 25 stores, which made it fail perhaps one run in three.
    #[test]
    fn no_fault_can_fire_before_the_warmup_completes() {
        for scenario in all() {
            for rule in &scenario.faults {
                // Only a crash matters. A stall, a replay or a zombie leaves the shard
                // running, so the warmup gate keeps making progress through them —
                // `zombie-at-start-commit` fires in the second transaction and is fine.
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

    /// Every scenario that reconfigures shards must be in the nextest group that
    /// serialises them against each other.
    ///
    /// Two concurrent reconfigurations contend badly on one stack — `split-shards`
    /// fails outright, or a crashed task cannot get a primary back inside its deadline
    /// while another scenario is republishing shards of its own — and the symptom is a
    /// flake in an unrelated-looking scenario. Checked here rather than left to review,
    /// because the cost of forgetting lands on whoever is debugging something else.
    #[test]
    fn every_shard_reconfiguring_scenario_is_serialised() {
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
                 `serial-shard-reconfiguration` group in .config/nextest.toml, so it will \
                 run concurrently with another that does and flake",
                scenario.name,
            );
        }
    }

    /// The document-counter class models a connector that handles delta-updates
    /// bindings only, so no scenario may hand it a merge binding.
    #[test]
    fn the_counter_class_never_takes_a_standard_binding() {
        for scenario in all() {
            if scenario.class == Class::DocumentCounter {
                assert!(
                    !scenario.standard_binding,
                    "scenario {} gives the counted-channel class a merge binding",
                    scenario.name,
                );
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
        let paired: Vec<Defect> = all().iter().filter_map(|s| s.defect).collect();

        for defect in [
            Defect::NonIdempotentAcknowledge,
            Defect::CommitDuringStore,
            Defect::IgnoreKeyRange,
            Defect::SkipFenceCheck,
            Defect::DropDocumentCounter,
            Defect::ResetCounterOnOpen,
            Defect::DropDocuments,
        ] {
            assert!(paired.contains(&defect), "no scenario catches {defect:?}",);
        }
    }
}

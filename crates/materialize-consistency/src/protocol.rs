//! The contract between the shim and the harness that drives it.
//!
//! The shim is spawned by the reactor, not by the harness, so everything the
//! harness wants to say to it must travel through the `local:` endpoint's
//! environment, and everything the shim reports back travels through files in
//! a run directory. There is deliberately no socket or control server: every
//! scenario in the suite is expressible as "perturb the Nth occurrence of a
//! protocol event", which the shim can decide entirely on its own.

use serde::{Deserialize, Serialize};

/// Directory the shim writes its trace and fault markers into. One per run;
/// every connector process of that run shares it.
pub const ENV_RUN_DIR: &str = "FLOW_CONSISTENCY_RUN_DIR";

/// JSON-encoded `Vec<FaultRule>`. Absent or `[]` means run faultlessly, which
/// is what the baseline scenario relies on.
pub const ENV_FAULTS: &str = "FLOW_CONSISTENCY_FAULTS";

/// Set to anything to have the reference connector trace every `Load` and `Store`
/// of a merged key, and every recovery decision, into `reduce.jsonl`.
///
/// Off by default and forwarded from the suite's own environment, because it is the
/// only record of what a reduction *read* before writing — the delivered rows show
/// what the connector was told, never the base it reduced onto.
pub const ENV_TRACE_REDUCE: &str = "FLOW_CONSISTENCY_TRACE_REDUCE";

/// A protocol event the shim can key a fault on.
///
/// Triggers name *events the shim observes*, never documents: which documents
/// land in which transaction varies between runs (transaction boundaries are
/// shaped by the runtime's duration policy and a rate-paced capture, not by
/// document count), so a rule keyed on document identity would be a flake.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
pub enum Trigger {
    /// A `Request.Open` — the start of a session.
    Open,
    /// A `Request.Load`, counted within the current transaction.
    Load,
    /// A `Request.Flush`.
    Flush,
    /// A `Request.Store`, counted within the current transaction.
    Store,
    /// A `Request.StartCommit`.
    StartCommit,
    /// A `Response.StartedCommit` — the connector has committed, and the
    /// runtime is about to commit its recovery log.
    StartedCommit,
    /// A `Request.Acknowledge` — the runtime's recovery log has committed.
    /// Faulting here is the crash-between-commits case: the connector's
    /// destination work for the transaction is durable-but-unapplied, and only
    /// an idempotent replay on restart can repair it.
    Acknowledge,
    /// A `Response.Acknowledged`.
    Acknowledged,
}

/// What to do when a trigger matches.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase", tag = "action")]
pub enum Action {
    /// Kill the connector and exit non-zero, before forwarding the matched
    /// message. The runtime sees a connector failure and restarts the shard.
    Crash,
    /// Delay the matched message. Extends a transaction without failing it.
    Stall { millis: u64 },
    /// Run a second connector process against the same messages, frozen at the
    /// match point while the live instance proceeds, then thawed so its stale
    /// commit races. The zombie opened first, so it holds the older fence.
    Zombie {
        /// Live-instance `StartedCommit` responses to await before thawing.
        thaw_after_commits: u64,
    },
}

/// "On the `nth` occurrence of `on`, do `action`."
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FaultRule {
    pub on: Trigger,
    /// 1-based occurrence to match. `Store` and `Load` are counted within the
    /// current transaction, and everything else within the session; either way the
    /// counters reset when a new connector process opens.
    #[serde(default = "one")]
    pub nth: u64,
    /// Transactions the session must have committed before this rule is armed.
    ///
    /// This is why `Store` and `Load` count per transaction rather than per
    /// session: `nth` only ever rises, so a rule not yet armed when occurrence
    /// `nth` went past could never fire at all. Counting them per transaction makes
    /// the occurrence recur, and `arm_after` then chooses *which* transaction — so a
    /// fault lands in a task that has established a rhythm rather than in its
    /// startup, without the rule needing to know how large a transaction is.
    #[serde(default)]
    pub arm_after: u64,
    /// Which shard of the task this rule may fire in.
    #[serde(default)]
    pub shard: ShardTarget,
    #[serde(flatten)]
    pub action: Action,
}

fn one() -> u64 {
    1
}

/// Which shard a rule is allowed to fire in.
///
/// `arm_after` cannot express any of this. It counts a session's own committed
/// transactions, and a split child starts at zero, so any threshold low enough for a
/// child to reach is one the pre-split parent reaches first — the fault lands while
/// the split is still being applied, kills the shard, and the split never lands.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ShardTarget {
    /// Any shard, including a task that has never been split.
    #[default]
    Any,
    /// A shard a split produced which is *also* shard zero: it owns part of the
    /// keyspace and holds the task's recovery log.
    ///
    /// This is the shard whose crash tests a connector's own recovery, because the
    /// runtime can restart it from its log the way it restarts an unsplit shard.
    SplitLeader,
    /// A shard a split produced which is *not* shard zero.
    ///
    /// A non-zero shard of a V2 task is stateless — no recovery log, state arriving
    /// by leader broadcast — so its crash is a different question: the connector's
    /// exactly-once claim has to survive the shard being rebuilt from nothing, and
    /// the runtime has to bring the task back at all. Killing one EOFs the fan-in
    /// stream and fails the leader too, so the whole task goes down with it.
    SplitNonLeader,
}

impl ShardTarget {
    /// Whether a session over this range may fire a rule aimed at `self`.
    ///
    /// Shard zero is the origin of both axes. A split has happened when the range is
    /// narrower than the whole keyspace — which has to be tested on *both* bounds:
    /// the upper child of a split owns `[mid, MAX]`, so its `key_end` alone is
    /// indistinguishable from an unsplit shard's.
    pub fn admits(&self, key_begin: u32, key_end: u32, r_clock_begin: u32) -> bool {
        let split = !(key_begin == 0 && key_end == u32::MAX);
        let zero = key_begin == 0 && r_clock_begin == 0;

        match self {
            ShardTarget::Any => true,
            ShardTarget::SplitLeader => split && zero,
            ShardTarget::SplitNonLeader => split && !zero,
        }
    }
}

impl FaultRule {
    /// Crash on the `nth` occurrence of `on` in the first transaction.
    pub fn crash_at(on: Trigger, nth: u64) -> Self {
        Self {
            on,
            nth,
            arm_after: 0,
            shard: ShardTarget::Any,
            action: Action::Crash,
        }
    }

    /// Arm this rule only once the session has committed `commits` transactions.
    pub fn armed_after(mut self, commits: u64) -> Self {
        self.arm_after = commits;
        self
    }

    /// Restrict this rule to one kind of shard; see [`ShardTarget`].
    pub fn in_shard(mut self, shard: ShardTarget) -> Self {
        self.shard = shard;
        self
    }
}

/// One line of the shim's protocol trace.
///
/// The trace is what makes a scenario observable: the harness waits on it to
/// know that a fault fired, that the connector recovered, and that the
/// materialization made further progress afterwards.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TraceEvent {
    /// Process that emitted this event. A new pid after a `Crash` is the
    /// signal that the runtime restarted the connector.
    pub pid: u32,
    pub event: Event,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum Event {
    /// A session opened over `[key_begin, key_end)`. Shard identity as the
    /// connector sees it, which is how the harness correlates a trace with a
    /// shard split.
    Opened {
        key_begin: u32,
        key_end: u32,
        bindings: usize,
    },
    /// A phase transition, with the shim's running count of that phase.
    Phase { trigger: Trigger, nth: u64 },
    /// Documents stored per binding, reported at each `StartCommit` so a
    /// transaction's shape is visible without a line per document.
    Stored { per_binding: Vec<u64> },
    /// A rule matched and its action was taken.
    Fault { rule: usize, action: Action },
    /// The shim is giving up. Recorded so a harness timeout can report the
    /// shim's own reason rather than just "nothing happened".
    Failed { error: String },
}

/// Layout of a run directory. Both sides construct paths through this rather
/// than duplicating string literals.
pub struct RunDir {
    pub root: std::path::PathBuf,
}

impl RunDir {
    pub fn new(root: impl Into<std::path::PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The trace, as newline-delimited `TraceEvent`. Appended to by every
    /// connector process of the run, so it spans restarts.
    pub fn trace(&self) -> std::path::PathBuf {
        self.root.join("trace.jsonl")
    }

    /// Marker recording that rule `idx` has fired.
    ///
    /// This is what keeps a crash fault one-shot. Without it the restarted
    /// connector would reach the same point and crash again, forever: the
    /// shard would never recover and the scenario could never check anything.
    /// Created with `create_new`, so the first process to reach the trigger
    /// wins even if several are racing.
    pub fn fired(&self, idx: usize) -> std::path::PathBuf {
        self.root.join(format!("fired-{idx}"))
    }
}

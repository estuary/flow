use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Policy {
    // Min/max desired combiner disk byte utilization.
    pub combiner_usage_bytes: std::ops::Range<u64>,
    // Min/max desired age of the last transaction (elapsed since last txn close).
    pub last_close_age: std::ops::Range<std::time::Duration>,
    // Min/max desired duration of an open transaction (elapsed since first ready checkpoint).
    pub open_duration: std::ops::Range<std::time::Duration>,
    // Min/max desired bytes read in a transaction.
    pub read_bytes: std::ops::Range<u64>,
    // Min/max desired documents read in a transaction.
    pub read_docs: std::ops::Range<u64>,
}

/// Aggregated measures and flags driving an extend-vs-close evaluation.
#[derive(Debug, Clone)]
pub struct Inputs {
    pub close_requested: bool,
    pub idempotent_replay: bool,
    pub last_age: Duration,
    pub combiner_bytes: u64,
    pub open_age: Duration,
    pub read_bytes: u64,
    pub read_docs: u64,
    pub stopping: bool,
    pub tail_done: bool,
    pub unresolved_hints: bool,
}

/// Outcome of an extend-vs-close evaluation. Both flags may be true: the
/// caller extends if a Frontier is ready and otherwise closes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision {
    pub may_extend: bool,
    pub may_close: bool,
    pub wake_after: Option<Duration>,
}

impl Policy {
    pub fn new(min_txn_duration: Duration, max_txn_duration: Duration) -> Self {
        // TODO: thread these through from the spec once they're supported there.
        let combiner_usage_bytes = 0..(30 * 1024 * 1024 * 1024);
        let last_close_age = Duration::ZERO..Duration::MAX;
        let open_duration = min_txn_duration..max_txn_duration;
        let read_bytes = 0..u64::MAX;
        let read_docs = 0..u64::MAX;

        Self {
            combiner_usage_bytes,
            last_close_age,
            open_duration,
            read_bytes,
            read_docs,
        }
    }

    /// Evaluate whether an open transaction may extend, close, or hold.
    ///
    /// Threshold policy with a hysteresis band per dimension:
    /// - `policy_extend` while every measure is below its `range.end`.
    /// - `policy_close` once every measure is above its `range.start`.
    ///   Usage-based measures saturate below `start` once `policy_extend` is false
    ///   (otherwise we'd live-lock because the threshold cannot be reached).
    ///
    /// Overrides:
    /// - `close_requested`, or `idempotent_replay && !unresolved_hints`: force close.
    /// - `combiner_bytes >= combiner_usage_bytes.end`: force close, bypassing the
    ///   min-duration floor. A combiner at its max byte budget must flush promptly
    ///   to bound its memory/disk footprint, much like an idempotent replay forces
    ///   a close. Reads are already halted (this also drives `policy_extend` false),
    ///   so the combiner cannot grow further while awaiting Tail to finish.
    /// - `unresolved_hints`: forces extend; suppresses close until hints resolve.
    /// - `idempotent_replay`: suppresses extend (replay is one-shot).
    /// - `close_requested` or `stopping` with `may_close=true`: suppresses extend so
    ///   the current txn closes promptly (and Head can stop after the next commit).
    ///   With Tail still draining, extend is permitted to keep the pipeline full.
    /// - `!tail_done`: suppresses close (must hold open while Tail finishes).
    pub fn evaluate(&self, inputs: Inputs) -> Decision {
        let Inputs {
            open_age,
            last_age,
            combiner_bytes,
            read_bytes,
            read_docs,
            close_requested,
            idempotent_replay,
            unresolved_hints,
            stopping,
            tail_done,
        } = inputs;

        let policy_extend = open_age < self.open_duration.end
            && last_age < self.last_close_age.end
            && combiner_bytes < self.combiner_usage_bytes.end
            && read_bytes < self.read_bytes.end
            && read_docs < self.read_docs.end;

        let mut policy_close = open_age >= self.open_duration.start
            && last_age >= self.last_close_age.start
            && (!policy_extend || combiner_bytes >= self.combiner_usage_bytes.start)
            && (!policy_extend || read_bytes >= self.read_bytes.start)
            && (!policy_extend || read_docs >= self.read_docs.start);
        policy_close |= idempotent_replay && !unresolved_hints;
        policy_close |= close_requested;
        // A combiner at its max byte budget must flush promptly to bound its
        // memory/disk footprint, even if the min-duration floor hasn't elapsed.
        policy_close |= combiner_bytes >= self.combiner_usage_bytes.end;

        let may_close = policy_close && !unresolved_hints && tail_done;

        // A requested or stopping close stops extending the current txn once
        // we're actually able to close it, so the txn finishes promptly. While
        // we cannot yet close (Tail still draining, or unresolved hints), we
        // keep extending if policy allows — maximizing parallelism as Tail works.
        let finishing = close_requested || stopping;
        let may_extend =
            (!idempotent_replay && policy_extend && (!finishing || !may_close)) || unresolved_hints;

        let wake_after = [
            self.open_duration.start.checked_sub(open_age),
            self.open_duration.end.checked_sub(open_age),
            self.last_close_age.start.checked_sub(last_age),
            self.last_close_age.end.checked_sub(last_age),
        ]
        .into_iter()
        .filter_map(|d| d.filter(|d| !d.is_zero()))
        .min();

        Decision {
            may_extend,
            may_close,
            wake_after,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Table-driven coverage of `Policy::evaluate`. The policy's hysteresis
    /// bands are 1..5 (s/bytes/docs); `mid` sits in-band on every dimension.
    #[test]
    fn close_policy_table() {
        let policy = Policy {
            combiner_usage_bytes: 1..5,
            last_close_age: Duration::from_secs(1)..Duration::from_secs(5),
            open_duration: Duration::from_secs(1)..Duration::from_secs(5),
            read_bytes: 1..5,
            read_docs: 1..5,
        };

        // `mid` is permissive across dimensions and flags: every measure is
        // inside its band, no overrides are active, and Tail is done. From
        // here, individual cases nudge one or two fields to exercise each
        // policy / override branch.
        let mid = Inputs {
            open_age: Duration::from_secs(3),
            last_age: Duration::from_secs(3),
            combiner_bytes: 3,
            read_bytes: 3,
            read_docs: 3,
            close_requested: false,
            idempotent_replay: false,
            unresolved_hints: false,
            stopping: false,
            tail_done: true,
        };

        struct Case {
            name: &'static str,
            inputs: Inputs,
            want: (bool, bool), // (may_extend, may_close)
        }

        let cases = [
            Case {
                name: "in-band: may extend or close",
                inputs: mid.clone(),
                want: (true, true),
            },
            Case {
                name: "below all minima: extend only",
                inputs: Inputs {
                    open_age: Duration::ZERO,
                    last_age: Duration::ZERO,
                    combiner_bytes: 0,
                    read_bytes: 0,
                    read_docs: 0,
                    ..mid.clone()
                },
                want: (true, false),
            },
            Case {
                name: "saturated combiner: close only",
                inputs: Inputs {
                    combiner_bytes: 10,
                    ..mid.clone()
                },
                want: (false, true),
            },
            Case {
                name: "saturated combiner below min duration: force close, bypass min floor",
                inputs: Inputs {
                    open_age: Duration::ZERO,
                    combiner_bytes: 10,
                    ..mid.clone()
                },
                want: (false, true),
            },
            Case {
                name: "saturated combiner but tail still busy: hold (cannot close yet)",
                inputs: Inputs {
                    open_age: Duration::ZERO,
                    combiner_bytes: 10,
                    tail_done: false,
                    ..mid.clone()
                },
                want: (false, false),
            },
            Case {
                name: "close_requested with may_close: extend suppressed, close",
                inputs: Inputs {
                    open_age: Duration::ZERO,
                    last_age: Duration::ZERO,
                    read_bytes: 0,
                    read_docs: 0,
                    combiner_bytes: 0,
                    close_requested: true,
                    ..mid.clone()
                },
                want: (false, true),
            },
            Case {
                name: "close_requested but tail still busy: hold open",
                inputs: Inputs {
                    close_requested: true,
                    tail_done: false,
                    ..mid.clone()
                },
                want: (true, false),
            },
            Case {
                name: "close_requested but unresolved hints: extend forced, close suppressed",
                inputs: Inputs {
                    close_requested: true,
                    unresolved_hints: true,
                    ..mid.clone()
                },
                want: (true, false),
            },
            Case {
                name: "idempotent_replay with hints resolved: close only",
                inputs: Inputs {
                    open_age: Duration::ZERO,
                    idempotent_replay: true,
                    ..mid.clone()
                },
                want: (false, true),
            },
            Case {
                name: "idempotent_replay with unresolved hints: extend forced",
                inputs: Inputs {
                    open_age: Duration::ZERO,
                    idempotent_replay: true,
                    unresolved_hints: true,
                    ..mid.clone()
                },
                want: (true, false),
            },
            Case {
                name: "stopping with may_close: extend suppressed",
                inputs: Inputs {
                    close_requested: true,
                    stopping: true,
                    ..mid.clone()
                },
                want: (false, true),
            },
            Case {
                name: "stopping with tail busy: keep pipeline full",
                inputs: Inputs {
                    stopping: true,
                    tail_done: false,
                    ..mid.clone()
                },
                want: (true, false),
            },
            Case {
                name: "unresolved hints: extend forced, close suppressed",
                inputs: Inputs {
                    unresolved_hints: true,
                    ..mid.clone()
                },
                want: (true, false),
            },
        ];

        for case in cases {
            let got = policy.evaluate(case.inputs.clone());
            assert_eq!(
                (got.may_extend, got.may_close),
                case.want,
                "case `{}` failed: inputs={:?}",
                case.name,
                case.inputs,
            );
        }
    }
}

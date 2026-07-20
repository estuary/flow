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

    /// Decide whether an open transaction may extend, may close, or must hold —
    /// the batching heuristic at the heart of a V2 task.
    ///
    /// Bigger transactions amortize the fixed cost of a commit, but must be
    /// bounded: too large risks memory and disk pressure, too long hurts latency,
    /// and too small or too frequent wastes overhead. Each measure carries a
    /// `start..end` band naming the transaction sizes we're content with:
    ///
    /// - Under every floor we're too small: extend only.
    /// - Inside the bands we're content either way, so both flags are set and the
    ///   caller extends if more input is ready and otherwise closes. The gap
    ///   between floor and ceiling is what keeps us from flapping at a threshold.
    /// - At any ceiling we're done: close now, even if other measures sit below
    ///   their floor. A saturated measure means no further useful progress is
    ///   possible, so honoring the minimum would only idle a doomed transaction.
    ///
    /// Lifecycle flags override the size heuristic:
    /// - `unresolved_hints` pins us open: we may close only on a coherent boundary.
    /// - `idempotent_replay` is one-shot: never extend, and close once hints clear.
    /// - `close_requested` forces a close, and with `stopping` also stops extending
    ///   once we can actually close, so the shard stops promptly — though we keep
    ///   extending while Tail drains, to leave the pipeline full.
    /// - `tail_done` gates close: hold open until Tail finishes the prior txn.
    ///
    /// `wake_after` reports when the nearest time threshold would next change this
    /// decision, so an idle caller knows when to re-evaluate.
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

        // Are all time-based measures above their minimum?
        let time_above_min =
            open_age >= self.open_duration.start && last_age >= self.last_close_age.start;
        // Are any time-based measures above their maximum?
        let time_above_max =
            open_age >= self.open_duration.end || last_age >= self.last_close_age.end;

        // Are all usage-based measures above their minimum?
        let usage_above_min = combiner_bytes >= self.combiner_usage_bytes.start
            && read_bytes >= self.read_bytes.start
            && read_docs >= self.read_docs.start;
        // Are any usage-based measures above their maximum?
        let usage_above_max = combiner_bytes >= self.combiner_usage_bytes.end
            || read_bytes >= self.read_bytes.end
            || read_docs >= self.read_docs.end;

        // Are we trying to close quickly due to user request or graceful stop?
        let finishing = close_requested || stopping;

        // We want to extend while neither time-max nor usage-max is breached.
        let policy_extend = !time_above_max && !usage_above_max;
        // We want to close if we're not extending, or time-min and usage-min is reached.
        let mut policy_close = !policy_extend || (usage_above_min && time_above_min);

        // Overrides: also close if we've completed an idempotent replay.
        policy_close |= idempotent_replay && !unresolved_hints;
        policy_close |= close_requested; // Or if requested.

        // Structurally, we cannot close if there are unresolved hints (we must
        // only close at a coherent transaction boundary), or if the Tail FSM
        // is still processing.
        let may_close = policy_close && !unresolved_hints && tail_done;

        // Structurally, we must cease extending an idempotent replay having
        // no unresolved hints, or if we're trying to close and are allowed to do so.
        // However we must extend if unresolved hints remain.
        let may_extend =
            (policy_extend && !idempotent_replay && (!finishing || !may_close)) || unresolved_hints;

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
    /// A ceiling is any value >= 5, a below-floor value is 0.
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
                name: "below every floor: extend only",
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
                // In-band usage but below the minimum transaction duration: hold
                // open to keep batching. Pins that the min-duration floor gates
                // close even when every usage measure is already above its floor.
                name: "below min duration only: hold for min_txn_duration",
                inputs: Inputs {
                    open_age: Duration::ZERO,
                    last_age: Duration::ZERO,
                    ..mid.clone()
                },
                want: (true, false),
            },
            Case {
                // The last-close age is a distinct time floor from open duration.
                name: "below last-close floor: hold",
                inputs: Inputs {
                    last_age: Duration::ZERO,
                    ..mid.clone()
                },
                want: (true, false),
            },
            // Each usage ceiling independently forces close, even in-band on time.
            Case {
                name: "combiner at ceiling: close only",
                inputs: Inputs {
                    combiner_bytes: 9,
                    ..mid.clone()
                },
                want: (false, true),
            },
            Case {
                name: "read_bytes at ceiling: close only",
                inputs: Inputs {
                    read_bytes: 9,
                    ..mid.clone()
                },
                want: (false, true),
            },
            Case {
                name: "read_docs at ceiling: close only",
                inputs: Inputs {
                    read_docs: 9,
                    ..mid.clone()
                },
                want: (false, true),
            },
            Case {
                // A usage ceiling closes even below the min-duration floor.
                name: "usage ceiling below min duration: bypass floor, close",
                inputs: Inputs {
                    open_age: Duration::ZERO,
                    combiner_bytes: 9,
                    ..mid.clone()
                },
                want: (false, true),
            },
            Case {
                // The max-duration ceiling likewise closes with usage below floor.
                name: "max duration reached, usage below floor: close",
                inputs: Inputs {
                    open_age: Duration::from_secs(9),
                    combiner_bytes: 0,
                    read_bytes: 0,
                    read_docs: 0,
                    ..mid.clone()
                },
                want: (false, true),
            },
            Case {
                // ...but no ceiling may close while Tail still drains.
                name: "usage ceiling but Tail busy: hold",
                inputs: Inputs {
                    combiner_bytes: 9,
                    tail_done: false,
                    ..mid.clone()
                },
                want: (false, false),
            },
            Case {
                name: "close_requested, able to close: extend suppressed, close",
                inputs: Inputs {
                    close_requested: true,
                    ..mid.clone()
                },
                want: (false, true),
            },
            Case {
                name: "close_requested but Tail busy: keep pipeline full",
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
                name: "stopping, able to close: extend suppressed",
                inputs: Inputs {
                    stopping: true,
                    ..mid.clone()
                },
                want: (false, true),
            },
            Case {
                name: "stopping but Tail busy: keep pipeline full",
                inputs: Inputs {
                    stopping: true,
                    tail_done: false,
                    ..mid.clone()
                },
                want: (true, false),
            },
            Case {
                name: "idempotent_replay, hints resolved: close only",
                inputs: Inputs {
                    open_age: Duration::ZERO,
                    idempotent_replay: true,
                    ..mid.clone()
                },
                want: (false, true),
            },
            Case {
                name: "idempotent_replay, unresolved hints: extend forced",
                inputs: Inputs {
                    open_age: Duration::ZERO,
                    idempotent_replay: true,
                    unresolved_hints: true,
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

        // `wake_after` targets the nearest future time threshold — here the min
        // duration and last-close floors, both 1s out from a fresh transaction.
        let fresh = Inputs {
            open_age: Duration::ZERO,
            last_age: Duration::ZERO,
            ..mid.clone()
        };
        assert_eq!(
            policy.evaluate(fresh).wake_after,
            Some(Duration::from_secs(1))
        );
    }
}

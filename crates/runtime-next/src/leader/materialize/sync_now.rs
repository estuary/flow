//! Sync-now evaluation: the pure decision at the heart of
//! `TaskControl.SyncNow`. The tonic service (`leader/service.rs`) delivers a
//! [`SyncNow`] request to the task's leader Actor, which gathers [`Inputs`]
//! from its live FSM state (`fsm::sync_now_inputs`) and applies [`evaluate`]
//! to decide what the caller's stream then awaits.

use crate::proto;
use tokio::sync::mpsc;

/// A sync-now request delivered to a materialize leader Actor, carrying the
/// reply channel that feeds the caller's SyncNow response stream.
pub(crate) struct SyncNow {
    pub reply_tx: mpsc::UnboundedSender<tonic::Result<proto::SyncNowResponse>>,
}

/// POD inputs of a sync-now decision.
#[derive(Debug, Copy, Clone)]
pub(crate) struct Inputs {
    /// An in-flight transaction exists: opened, and its commit not yet fully
    /// persisted.
    pub head_open: bool,
    /// The close decision is still ahead (Head is Idle or Extend), so setting
    /// `close_requested` can shorten the transaction.
    pub head_deciding: bool,
    /// Tail is Done: the prior transaction is fully acknowledged.
    pub tail_done: bool,
}

/// What a sync-now request decided: whether to arm `close_requested`, and how
/// many future `Tail::Done` transitions the caller's stream awaits before Done
/// (zero resolves immediately).
#[derive(Debug, Copy, Clone, PartialEq)]
pub(crate) struct Decision {
    pub set_close_requested: bool,
    pub await_dones: u64,
}

/// Decide a sync-now request from POD state.
///
/// One wait rule covers every case: the caller awaits one `Tail::Done` per
/// pipeline stage that's ahead of it — the in-flight Head transaction (if any)
/// and the draining Tail transaction (if not already Done). A task which is
/// fully current awaits nothing and resolves immediately.
pub(crate) fn evaluate(inputs: Inputs) -> Decision {
    let Inputs {
        head_open,
        head_deciding,
        tail_done,
    } = inputs;

    Decision {
        // Arm `close_requested` only for an open transaction whose close
        // decision is still ahead. Past that point the transaction is already
        // closing and arming it would be harmless but pointless.
        set_close_requested: head_open && head_deciding,
        await_dones: head_open as u64 + !tail_done as u64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Table-driven coverage of `evaluate`, in the style of
    /// `close_policy_table`: every row of the design mapping, plus the
    /// pipelined Head-open-while-Tail-drains cases.
    #[test]
    fn decision_table() {
        struct Case {
            name: &'static str,
            inputs: Inputs,
            want: Decision,
        }
        let mk = |head_open, head_deciding, tail_done| Inputs {
            head_open,
            head_deciding,
            tail_done,
        };

        let cases = [
            Case {
                name: "open transaction before its close decision: shorten it and wait",
                inputs: mk(true, true, true),
                want: Decision {
                    set_close_requested: true,
                    await_dones: 1,
                },
            },
            Case {
                name: "pipelined: Head extends while Tail drains, await both",
                inputs: mk(true, true, false),
                want: Decision {
                    set_close_requested: true,
                    await_dones: 2,
                },
            },
            Case {
                name: "Head past the close decision (flushing / committing): don't arm close",
                inputs: mk(true, false, true),
                want: Decision {
                    set_close_requested: false,
                    await_dones: 1,
                },
            },
            Case {
                name: "Head committing while Tail still drains: await both",
                inputs: mk(true, false, false),
                want: Decision {
                    set_close_requested: false,
                    await_dones: 2,
                },
            },
            Case {
                name: "no open transaction, Tail draining acknowledgement: wait, don't arm close",
                inputs: mk(false, false, false),
                want: Decision {
                    set_close_requested: false,
                    await_dones: 1,
                },
            },
            Case {
                name: "no open transaction, Tail done: fully current, nothing to await",
                inputs: mk(false, false, true),
                want: Decision {
                    set_close_requested: false,
                    await_dones: 0,
                },
            },
        ];

        for case in cases {
            let got = evaluate(case.inputs);
            assert_eq!(
                got, case.want,
                "case `{}` failed: inputs={:?}",
                case.name, case.inputs,
            );
        }
    }
}

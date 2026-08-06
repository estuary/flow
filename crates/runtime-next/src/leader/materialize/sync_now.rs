//! Sync-now evaluation: the pure decision at the heart of
//! `TaskControl.SyncNow`. The tonic service (`leader/service.rs`) delivers a
//! [`SyncNow`] request to the task's leader Actor, which gathers [`Inputs`]
//! from its live FSM state (`fsm::sync_now_inputs`) and applies [`evaluate`]
//! to decide the acknowledged outcome and what the caller's stream then
//! awaits.

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
    /// A sync-schedule hold is collapsing the open-duration band onto a
    /// future commit instant.
    pub held: bool,
}

/// What a sync-now request decided: the acknowledged Outcome, whether to arm
/// `close_requested`, and how many future `Tail::Done` transitions the
/// caller's stream awaits before Done (zero resolves immediately).
#[derive(Debug, Copy, Clone, PartialEq)]
pub(crate) struct Decision {
    pub outcome: proto::sync_now_response::Outcome,
    pub set_close_requested: bool,
    pub await_dones: u64,
}

/// Decide a sync-now request's outcome from POD state.
///
/// One wait rule covers every outcome: the caller awaits one `Tail::Done`
/// per pipeline stage that's ahead of it — the in-flight Head transaction
/// (if any) and the draining Tail transaction (if not already Done). IDLE
/// alone awaits nothing.
pub(crate) fn evaluate(inputs: Inputs) -> Decision {
    let Inputs {
        head_open,
        head_deciding,
        tail_done,
        held,
    } = inputs;
    use proto::sync_now_response::Outcome;

    let (outcome, set_close_requested) = match (head_open, head_deciding, tail_done, held) {
        // An open transaction we can still tell to close.
        (true, true, _, true) => (Outcome::HeldCollapsed, true),
        (true, true, _, false) => (Outcome::CloseRequested, true),
        // An open transaction already past the close decision: setting
        // `close_requested` would be harmless but pointless.
        (true, false, _, _) => (Outcome::AlreadyClosing, false),
        // No open transaction, but the prior one still drains acknowledgement.
        (false, _, false, _) => (Outcome::AlreadyClosing, false),
        // Fully current: nothing to await.
        (false, _, true, _) => (Outcome::Idle, false),
    };

    Decision {
        outcome,
        set_close_requested,
        await_dones: head_open as u64 + !tail_done as u64,
    }
}

/// Build the stream's Ack message.
pub(crate) fn ack_response(
    outcome: proto::sync_now_response::Outcome,
    status: proto::sync_now_response::Status,
) -> proto::SyncNowResponse {
    proto::SyncNowResponse {
        response: Some(proto::sync_now_response::Response::Ack(
            proto::sync_now_response::Ack {
                outcome: outcome as i32,
                status: Some(status),
            },
        )),
    }
}

/// Build a Progress heartbeat message.
pub(crate) fn progress_response(
    status: proto::sync_now_response::Status,
) -> proto::SyncNowResponse {
    proto::SyncNowResponse {
        response: Some(proto::sync_now_response::Response::Progress(status)),
    }
}

/// Build the stream's final Done message.
pub(crate) fn done_response(done: proto::sync_now_response::Done) -> proto::SyncNowResponse {
    proto::SyncNowResponse {
        response: Some(proto::sync_now_response::Response::Done(done)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::sync_now_response::Outcome;

    /// Table-driven coverage of `evaluate`, in the style of
    /// `close_policy_table`: every outcome row of the design mapping, plus
    /// the pipelined Head-open-while-Tail-drains cases.
    #[test]
    fn decision_table() {
        struct Case {
            name: &'static str,
            inputs: Inputs,
            want: Decision,
        }
        let mk = |head_open, head_deciding, tail_done, held| Inputs {
            head_open,
            head_deciding,
            tail_done,
            held,
        };

        let cases = [
            Case {
                name: "held open transaction: collapse the hold and wait",
                inputs: mk(true, true, true, true),
                want: Decision {
                    outcome: Outcome::HeldCollapsed,
                    set_close_requested: true,
                    await_dones: 1,
                },
            },
            Case {
                name: "open transaction, no hold: request close and wait",
                inputs: mk(true, true, true, false),
                want: Decision {
                    outcome: Outcome::CloseRequested,
                    set_close_requested: true,
                    await_dones: 1,
                },
            },
            Case {
                name: "pipelined: held Head extends while Tail drains, await both",
                inputs: mk(true, true, false, true),
                want: Decision {
                    outcome: Outcome::HeldCollapsed,
                    set_close_requested: true,
                    await_dones: 2,
                },
            },
            Case {
                name: "pipelined: unheld Head extends while Tail drains, await both",
                inputs: mk(true, true, false, false),
                want: Decision {
                    outcome: Outcome::CloseRequested,
                    set_close_requested: true,
                    await_dones: 2,
                },
            },
            Case {
                name: "Head past the close decision (flushing / committing): already closing",
                inputs: mk(true, false, true, false),
                want: Decision {
                    outcome: Outcome::AlreadyClosing,
                    set_close_requested: false,
                    await_dones: 1,
                },
            },
            Case {
                name: "Head committing while Tail still drains: await both",
                inputs: mk(true, false, false, false),
                want: Decision {
                    outcome: Outcome::AlreadyClosing,
                    set_close_requested: false,
                    await_dones: 2,
                },
            },
            Case {
                name: "no open transaction, Tail draining acknowledgement: wait, don't arm close",
                inputs: mk(false, false, false, false),
                want: Decision {
                    outcome: Outcome::AlreadyClosing,
                    set_close_requested: false,
                    await_dones: 1,
                },
            },
            Case {
                name: "no open transaction, Tail done: idle, nothing to await",
                inputs: mk(false, false, true, false),
                want: Decision {
                    outcome: Outcome::Idle,
                    set_close_requested: false,
                    await_dones: 0,
                },
            },
            Case {
                name: "a hold is irrelevant once past the close decision",
                inputs: mk(true, false, true, true),
                want: Decision {
                    outcome: Outcome::AlreadyClosing,
                    set_close_requested: false,
                    await_dones: 1,
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

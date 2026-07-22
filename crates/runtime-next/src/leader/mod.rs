// Protocol primitives for joining over connecting shards.
mod join;
use join::{JoinOutcome, JoinSlot, PendingJoin, validate as validate_join};

pub mod close_policy;
pub mod frontier_mapping;
mod service;
mod shuffle;

// Task-specific handling.
pub mod capture; // `pub` because it's directly used by shard actor.
mod derive;
mod materialize;

pub use service::Service;
pub use shuffle::{ShuffleServiceFactory, ShuffleSession, ShuffleSessionFactory};

#[cfg(test)]
pub(crate) mod fixtures;

/// Converge recovered RocksDB state toward its authoritative checkpoints:
/// each iteration `reconcile`s the scanned baseline, and the first step whose
/// trigger fires returns the incremental Persist that clears it, which is
/// `apply`d (stamped with `seq_no` and `rescan`) before the loop continues
/// with the fresh scan it returns. It converges once `reconcile` yields no
/// Persist, meaning no trigger fires against the scanned state. Every
/// reconcile step clears its own trigger and fires at most once per startup,
/// so the iteration bound is a backstop against implementation error rather
/// than a convergence heuristic.
///
/// `state` is caller context threaded through `apply` by value, so `apply`'s
/// Future owns its inputs (keeping it `Send` when they are).
async fn reconcile_loop<B, S, Fut>(
    mut state: S,
    mut scanned: B,
    mut reconcile: impl FnMut(&B) -> anyhow::Result<Option<crate::proto::Persist>>,
    mut apply: impl FnMut(S, crate::proto::Persist) -> Fut,
) -> anyhow::Result<(S, B)>
where
    Fut: std::future::Future<Output = anyhow::Result<(S, B)>>,
{
    const MAX_RECONCILE_ITERATIONS: u64 = 5;

    for iteration in 1..=MAX_RECONCILE_ITERATIONS {
        let Some(mut persist) = reconcile(&scanned)? else {
            return Ok((state, scanned));
        };
        persist.seq_no = iteration;
        persist.rescan = true;

        service_kit::event!(
            tracing::Level::INFO,
            "leader",
            iteration,
            "persisting reconciled startup baseline and re-scanning",
        );

        (state, scanned) = apply(state, persist).await?;
    }
    anyhow::bail!(
        "startup reconciliation did not converge after {MAX_RECONCILE_ITERATIONS} iterations"
    )
}

/// Shard-label feature flag (under the `estuary.dev/flag/` prefix) that, when
/// set to `"true"`, tells the leader to drop V1 rollback support for the task.
const DROP_V1_ROLLBACK_FLAG: &str = "drop-runtime-v1-rollback";

/// Reports whether `flags` (an `ops::ShardLabeling.flags` map) sets `flag` to
/// `"true"`, mirroring the Go runtime's feature-flag convention.
fn flag_enabled(flags: &std::collections::BTreeMap<String, String>, flag: &str) -> bool {
    flags.get(flag).map(String::as_str) == Some("true")
}

#[cfg(test)]
mod tests {
    /// A reconcile step that never clears its trigger hits the iteration cap:
    /// the backstop against a step failing to self-clear.
    #[tokio::test]
    async fn reconcile_loop_bails_when_not_converging() {
        let err = super::reconcile_loop(
            (),
            (),
            |()| Ok(Some(crate::proto::Persist::default())),
            |(), _persist| async move { Ok(((), ())) },
        )
        .await
        .unwrap_err();

        assert!(
            format!("{err:?}").contains("did not converge"),
            "expected non-convergence bail, got {err:?}"
        );
    }
}

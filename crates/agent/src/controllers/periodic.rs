//! Periodically re-publishes live specs. We do this for a few reasons:
//! - to ensure that all built specs in use are generated using reasonably recent versions of the code
//! - to allow configuring a retention policy on build databases
//!
//! There's no special status associated with this, since the publication status
//! can already record any useful information, and the `live_specs` table
//! already records the last update time of each spec.
use anyhow::Context;
use chrono::{DateTime, Utc};
use models::{status::publications::PublicationStatus, ModelDef};

use crate::ControlPlane;

use super::{publication_status::PendingPublication, ControllerErrorExt, ControllerState, NextRun};

/// 20 days was chosen because we'd like to have a 30 day retention on our
/// builds, so this gives us 10 days to notice and correct any problems before
/// the build is liable to dissapear.
const PERIODIC_PUBLISH_INTERVAL: chrono::Duration = chrono::Duration::days(20);

/// Returns a `NextRun` for the next scheduled periodic publication, unless the
/// spec is disabled.
pub fn next_periodic_publish(state: &ControllerState) -> Option<NextRun> {
    // Tiny jitter because we're dealing with pretty long durations.
    next_pub_time(state).map(|(when, _)| NextRun::after(when).with_jitter_percent(2))
}

/// Computes the wall clock time of the next desired periodic publication,
/// and returns it along with a count of recent publication failures.
/// Returns None if the spec is disabled.
fn next_pub_time(state: &ControllerState) -> Option<(DateTime<Utc>, u32)> {
    if !is_enabled_task(state) {
        return None;
    }

    let next = state.live_spec_updated_at + PERIODIC_PUBLISH_INTERVAL;
    if let Some((last_attempt, fail_count)) =
        state
            .current_status
            .publication_status()
            .and_then(|pub_status| {
                // We look at the most recent entry in the publication history
                // to determine the last attempt and number of failures.
                // Technically, this entry might not be from a periodic
                // publication attemt (it could be due to dependency changes,
                // etc), and that's OK. Our goal is to limit the overall rate of
                // publication attempts. Also ignore prior attempts that were
                // before the periodic pulication came due.
                super::last_pub_failed(pub_status, "").filter(|(last, _)| *last > next)
            })
    {
        Some((last_attempt + chrono::TimeDelta::hours(1), fail_count))
    } else {
        Some((next, 0))
    }
}

/// Publishes the spec if necessary, and returns a boolean indicating whether it
/// was published. If this returns `true`, then the controller must immediately
/// return and schedule a subsequent run. Returns an error if a periodic publication
/// is needed, but can't yet be attempted due to the backoff.
pub async fn update_periodic_publish<C: ControlPlane>(
    state: &ControllerState,
    pub_status: &mut PublicationStatus,
    control_plane: &C,
) -> anyhow::Result<bool> {
    let mut pending = start_periodic_publish_update(state, control_plane)?;
    if !pending.has_pending() {
        return Ok(false);
    }

    let pub_result = pending
        .finish(state, pub_status, control_plane)
        .await
        .context("executing periodic publication")?;
    pub_result
        .error_for_status()
        .with_retry(NextRun::after_minutes(180))?;
    Ok(true)
}

/// Starts the update and returns a `PendingPublication`. If no publication is
/// necessary at this time, then the pending draft will be empty. Otherwise, it
/// will contain a touch publication of the spec. Will return an error if a periodic
/// publication is needed, but shouldn't be attempted yet due to the backoff.
pub fn start_periodic_publish_update<C: ControlPlane>(
    state: &ControllerState,
    control_plane: &C,
) -> anyhow::Result<PendingPublication> {
    let mut pending = PendingPublication::new();
    if let Some((next_attempt, failures)) = next_pub_time(state) {
        if next_attempt < control_plane.current_time() {
            pending.start_touch(state, "periodic publication");
        } else if failures > 0 {
            // `next_attempt` is in the future, error with a retry that's set to the next attempt.
            return super::backoff_err(
                NextRun::after(next_attempt).with_jitter_percent(5),
                "periodic publication",
                failures,
            );
        }
    }
    Ok(pending)
}

/// Returns true if the live spec is a task that is enabled. False for all
/// captured collections, tests, and disabled tasks.
pub fn is_enabled_task(state: &ControllerState) -> bool {
    state
        .live_spec
        .as_ref()
        .map(|ls| match ls {
            models::AnySpec::Capture(c) => c.is_enabled(),
            models::AnySpec::Materialization(m) => m.is_enabled(),
            models::AnySpec::Collection(c) => c.derive.is_some() && c.is_enabled(),
            models::AnySpec::Test(_) => false,
        })
        .unwrap_or(true)
}

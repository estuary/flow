//! Periodically re-publishes live specs. We do this for a few reasons:
//! - to ensure that all built specs in use are generated using reasonably recent versions of the code
//! - to allow configuring a retention policy on build databases
//!
//! There's no special status associated with this, since the publication status
//! can already record any useful information, and the `live_specs` table
//! already records the last update time of each spec.
use anyhow::Context;
use models::{status::publications::PublicationStatus, ModelDef};

use crate::ControlPlane;

use super::{publication_status::PendingPublication, ControllerState, NextRun};

/// 20 days was chosen because we'd like to have a 30 day retention on our
/// builds, so this gives us 10 days to notice and correct any problems before
/// the build is liable to dissapear.
const PERIODIC_PUBLISH_INTERVAL: chrono::Duration = chrono::Duration::days(20);

/// Returns a `NextRun` for the next scheduled periodic publication, unless the
/// spec is disabled.
pub fn next_periodic_publish(state: &ControllerState) -> Option<NextRun> {
    if !is_enabled_task(state) {
        return None;
    }
    let next = state.live_spec_updated_at + PERIODIC_PUBLISH_INTERVAL;
    // Tiny jitter because we're dealing with pretty long durations.
    Some(NextRun::after(next).with_jitter_percent(2))
}

/// Publishes the spec if necessary, and returns a boolean indicating whether it
/// was published. If this returns `true`, then the controller must immediately
/// return and schedule a subsequent run.
pub async fn update_periodic_publish<C: ControlPlane>(
    state: &ControllerState,
    pub_status: &mut PublicationStatus,
    control_plane: &mut C,
) -> anyhow::Result<bool> {
    let mut pending = start_periodic_publish_update(state, control_plane);
    if !pending.has_pending() {
        return Ok(false);
    }

    let pub_result = pending
        .finish(state, pub_status, control_plane)
        .await
        .context("executing periodic publication")?;
    pub_result.error_for_status()?;
    Ok(true)
}

/// Starts the update and returns a `PendingPublication`. If no publication is
/// necessary at this time, then the pending draft will be empty. Otherwise, it
/// will contain a touch publication of the spec.
pub fn start_periodic_publish_update<C: ControlPlane>(
    state: &ControllerState,
    control_plane: &mut C,
) -> PendingPublication {
    let mut pending = PendingPublication::new();
    if is_enabled_task(state)
        && control_plane.current_time() - state.live_spec_updated_at > PERIODIC_PUBLISH_INTERVAL
    {
        pending.start_touch(state, "periodic publication");
    }
    pending
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

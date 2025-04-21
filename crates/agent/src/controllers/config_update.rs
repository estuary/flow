use super::{ControlPlane, ControllerState, Event, Inbox, NextRun};
use crate::controllers::{periodic, publication_status};
use models::status::{
    config_updates::PendingConfigUpdateStatus, connector::ConfigUpdate,
    publications::PublicationStatus,
};

pub const CONFIG_UPDATE_PUBLICATION_DETAIL: &str =
    "in response to an updated config emitted by the connector";

pub async fn updated_config_publish<C: ControlPlane, F>(
    state: &ControllerState,
    config_update_status: &mut PendingConfigUpdateStatus,
    publication_status: &mut PublicationStatus,
    events: &Inbox,
    control_plane: &C,
    update_config: F,
) -> anyhow::Result<bool>
where
    C: ControlPlane,
    F: FnOnce(&ConfigUpdate) -> anyhow::Result<publication_status::PendingPublication>,
{
    // If the task is disabled, don't try to publish any config updates.
    if !periodic::is_enabled_task(state) {
        config_update_status.next_attempt = None;
        return Ok(false);
    }

    // If next_attempt is in the future, return a backoff error with a retry time set to next_attempt.
    if let Some(next_attempt) = config_update_status.next_attempt {
        if control_plane.current_time() < next_attempt {
            let fail_count = pub_failure_count(state);
            return super::backoff_err(
                NextRun::after(next_attempt).with_jitter_percent(10),
                "config update publication",
                fail_count,
            );
        }
        // Return early if there's no scheduled next_attempt and the inbox does not contain a ConfigUpdated event.
    } else if !events
        .iter()
        .any(|(_, e)| matches!(e, Some(Event::ConfigUpdated)))
    {
        return Ok(false);
    }

    let Some(log) = control_plane
        .get_config_updates(state.catalog_name.clone(), state.last_build_id.clone())
        .await?
    else {
        // No config updates were found for the current build.
        // Set the next_attempt to None and return.
        config_update_status.next_attempt = None;
        return Ok(false);
    };

    // Use the update_config closure to create a new publication with the updated config.
    let mut pending = update_config(&log)?;

    // Attempt to finish the publication.
    let pub_result = pending
        .finish(state, publication_status, control_plane)
        .await?
        .error_for_status();

    match pub_result {
        Ok(_) => {
            // The publication succeeded, so clear out status' next_attempt and delete the row.
            config_update_status.next_attempt = None;

            control_plane
                .delete_config_updates(state.catalog_name.clone(), state.last_build_id)
                .await?;

            return Ok(true);
        }
        Err(_) => {
            // The publication failed, so return an error to retry in 10 minutes.
            let fail_count = pub_failure_count(state);
            let next_attempt = chrono::Utc::now() + chrono::Duration::minutes(10);
            config_update_status.next_attempt = Some(next_attempt);

            return super::backoff_err(
                NextRun::after(next_attempt).with_jitter_percent(10),
                "config update publication",
                fail_count,
            );
        }
    }
}

// Returns the count of recent config update publication failures.
fn pub_failure_count(state: &ControllerState) -> u32 {
    if let Some((_, fail_count)) = state
        .current_status
        .publication_status()
        .and_then(|pub_status| super::last_pub_failed(pub_status, CONFIG_UPDATE_PUBLICATION_DETAIL))
    {
        fail_count
    } else {
        0
    }
}

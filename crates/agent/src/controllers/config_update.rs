use super::{ControlPlane, ControllerState, Event, Inbox, NextRun};
use crate::controllers::publication_status;
use models::status::{
    connector::ConfigUpdate, publications::PublicationStatus, PendingConfigUpdateStatus,
};

pub const CONFIG_UPDATE_PUBLICATION_DETAIL: &str =
    "in response to an updated config emitted by the connector";

pub async fn updated_config_publish<C: ControlPlane, F>(
    state: &ControllerState,
    config_update_status: &mut Option<PendingConfigUpdateStatus>,
    publication_status: &mut PublicationStatus,
    events: &Inbox,
    control_plane: &C,
    update_config: F,
) -> anyhow::Result<bool>
where
    C: ControlPlane,
    F: FnOnce(&ConfigUpdate) -> anyhow::Result<publication_status::PendingPublication>,
{
    // If there's a config_update_status from an old build, clear it out.
    if let Some(status) = config_update_status.as_ref() {
        if status.build < state.last_build_id {
            *config_update_status = None;
        }
    }

    let has_config_update_event = events
        .iter()
        .any(|(_, e)| matches!(e, Some(Event::ConfigUpdated)));

    // If there's no config_update_status from a previous ConfigUpdated event
    // and there's no ConfigUpdated event in the inbox, return early.
    if config_update_status.is_none() && !has_config_update_event {
        return Ok(false);
    }

    // Since there's either a previous config update that failed or a new config update that needs
    // published, the current config update from Supabase is fetched.
    let Some(log) = control_plane
        .get_config_updates(state.catalog_name.clone(), state.last_build_id.clone())
        .await?
    else {
        // No config updates were found for the current build.
        // Set the next_attempt to None and return.
        *config_update_status = None;
        return Ok(false);
    };

    // If there was a previous attempt to publish this config update but the next
    // scheduled attempt is in the future, return a backoff error.
    if let Some(status) = config_update_status.as_ref() {
        let next_attempt = status.next_attempt;
        let fail_count = pub_failure_count(state);
        if control_plane.current_time() < next_attempt {
            return super::backoff_err(
                NextRun::after(next_attempt).with_jitter_percent(10),
                "config update publication",
                fail_count,
            );
        }
    }

    // Use the update_config closure to start a new publication with the updated config.
    let mut pending = update_config(&log)?;

    // Attempt to finish the publication.
    let pub_result = pending
        .finish(state, publication_status, control_plane)
        .await?
        .error_for_status();

    match pub_result {
        Ok(_) => {
            // The publication succeeded, so clear out the config_update_status and delete the row.
            *config_update_status = None;

            control_plane
                .delete_config_updates(state.catalog_name.clone(), state.last_build_id)
                .await?;

            return Ok(true);
        }
        Err(_) => {
            // The publication failed, so set config_update_status and return a backoff
            // error to retry in 10 minutes.
            let fail_count = pub_failure_count(state);
            let next_attempt = chrono::Utc::now() + chrono::Duration::minutes(10);
            *config_update_status = Some(PendingConfigUpdateStatus {
                next_attempt: next_attempt,
                build: log.shard.build,
            });

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
        1
    }
}

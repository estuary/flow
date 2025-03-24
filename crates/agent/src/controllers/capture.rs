mod auto_discover;
use super::{
    backoff_data_plane_activate, dependencies::Dependencies, ControlPlane, ControllerErrorExt,
    ControllerState, Inbox, NextRun,
};
use crate::controllers::{activation, periodic, publication_status};
use anyhow::Context;
use itertools::Itertools;
use models::status::capture::{AutoDiscoverStatus, CaptureStatus};

pub async fn update<C: ControlPlane>(
    status: &mut CaptureStatus,
    state: &ControllerState,
    events: &Inbox,
    control_plane: &C,
    model: &models::CaptureDef,
) -> anyhow::Result<Option<NextRun>> {
    let published = maybe_publish(status, state, control_plane, model).await;

    // Return immediately if we've successfully published. If a publication was
    // attempted, but failed, then we'll still attempt to update activation, so
    // that we can activate new builds and restart failed shards.
    if Some(&true) == published.as_ref().ok() {
        return Ok(Some(NextRun::immediately()));
    }

    let activate_next_run =
        activation::update_activation(&mut status.activation, state, events, control_plane)
            .await
            .with_retry(backoff_data_plane_activate(state.failures))?;

    // Return the publication error now, if there is one.
    let _ = published?;

    publication_status::update_notify_dependents(&mut status.publications, state, control_plane)
        .await
        .context("failed to notify dependents")?;

    let ad_next = status
        .auto_discover
        .as_ref()
        .and_then(auto_discover::next_run);
    let periodic_next = periodic::next_periodic_publish(state);
    Ok(NextRun::earliest([
        ad_next,
        periodic_next,
        activate_next_run,
    ]))
}

async fn maybe_publish<C: ControlPlane>(
    status: &mut CaptureStatus,
    state: &ControllerState,
    control_plane: &C,
    model: &models::CaptureDef,
) -> anyhow::Result<bool> {
    let mut dependencies = Dependencies::resolve(state, control_plane).await?;
    let published = dependencies
        .update(state, control_plane, &mut status.publications, |deleted| {
            let mut draft_capture = model.clone();
            let mut disabled_count = 0;
            for binding in draft_capture.bindings.iter_mut() {
                if deleted.contains(binding.target.as_str()) && !binding.disable {
                    disabled_count += 1;
                    binding.disable = true;
                }
            }

            let detail = format!(
                "disabled {disabled_count} binding(s) in response to deleted collections: [{}]",
                deleted.iter().format(", ")
            );
            Ok((detail, draft_capture))
        })
        .await?;
    if published {
        return Ok(true);
    }

    if periodic::update_periodic_publish(state, &mut status.publications, control_plane).await? {
        return Ok(true);
    }

    if model.auto_discover.is_some() {
        let ad_status = status
            .auto_discover
            .get_or_insert_with(AutoDiscoverStatus::default);
        let published = auto_discover::update(
            ad_status,
            state,
            model,
            control_plane,
            &mut status.publications,
        )
        .await
        .context("updating auto-discover")?;
        tracing::debug!(%published, "auto-discover status updated successfully");
        if published {
            return Ok(true);
        }
    } else {
        // Clear auto-discover status to avoid confusion, but only if
        // auto-discover is disabled. We leave the auto-discover status if
        // shards are disabled, since it's still useful for debugging.
        status.auto_discover = None;
    };
    Ok(false)
}

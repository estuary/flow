mod auto_discover;
use super::{
    backoff_data_plane_activate, dependencies::Dependencies, ControlPlane, ControllerErrorExt,
    ControllerState, NextRun,
};
use crate::controllers::{periodic, publication_status};
use anyhow::Context;
use itertools::Itertools;
use models::status::capture::{AutoDiscoverStatus, CaptureStatus};

pub async fn update<C: ControlPlane>(
    status: &mut CaptureStatus,
    state: &ControllerState,
    control_plane: &mut C,
    model: &models::CaptureDef,
) -> anyhow::Result<Option<NextRun>> {
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
    tracing::debug!(%published, "dependencies status updated successfully");
    if published {
        return Ok(Some(NextRun::immediately()));
    }

    publication_status::update_activation(&mut status.activation, state, control_plane)
        .await
        .with_retry(backoff_data_plane_activate(state.failures))?;

    publication_status::update_notify_dependents(&mut status.publications, state, control_plane)
        .await
        .context("failed to notify dependents")?;

    if periodic::update_periodic_publish(state, &mut status.publications, control_plane).await? {
        return Ok(Some(NextRun::immediately()));
    }

    let ad_next = if model.auto_discover.is_some() {
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
            return Ok(Some(NextRun::immediately()));
        }
        auto_discover::next_run(ad_status)
    } else {
        // Clear auto-discover status to avoid confusion, but only if
        // auto-discover is disabled. We leave the auto-discover status if
        // shards are disabled, since it's still useful for debugging.
        status.auto_discover = None;
        None
    };

    let periodic_next = periodic::next_periodic_publish(state);
    Ok(NextRun::earliest([ad_next, periodic_next]))
}

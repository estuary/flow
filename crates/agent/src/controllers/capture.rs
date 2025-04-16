mod auto_discover;
use super::{
    backoff_data_plane_activate, coalesce_results, dependencies::Dependencies, ControlPlane,
    ControllerErrorExt, ControllerState, Inbox, NextRun,
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
    let auto_discover_result = if model.auto_discover.is_some() && !model.shards.disable {
        let ad_status = status
            .auto_discover
            .get_or_insert_with(AutoDiscoverStatus::default);
        let result = auto_discover::update(
            ad_status,
            state,
            model,
            control_plane,
            &mut status.publications,
        )
        .await
        .context("updating auto-discover");
        tracing::debug!(?result, "auto-discover status updated");
        if result.as_ref().ok() == Some(&true) {
            return Ok(Some(NextRun::immediately()));
        }
        result.map(|_| auto_discover::next_run(ad_status))
    } else {
        // Clear auto-discover status to avoid confusion, but only if
        // auto-discover is disabled. We leave the auto-discover status if
        // shards are disabled, since it's still useful for debugging.
        if !model.shards.disable {
            status.auto_discover = None;
        }
        // Otherwise, just clear the `next_at` time.
        if let Some(ad) = status.auto_discover.as_mut() {
            ad.next_at.take();
        }
        Ok(None)
    };

    let mut dependencies = Dependencies::resolve(state, control_plane).await?;
    let dependencies_published = dependencies
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
        .await;
    if dependencies_published.as_ref().ok() == Some(&true) {
        return Ok(Some(NextRun::immediately()));
    }
    let dependencies_result = dependencies_published.map(|_| None);
    let periodic_published =
        periodic::update_periodic_publish(state, &mut status.publications, control_plane).await;
    if periodic_published.as_ref().ok() == Some(&true) {
        return Ok(Some(NextRun::immediately()));
    }
    let periodic_result = periodic_published.map(|_| periodic::next_periodic_publish(state));

    let activate_result =
        activation::update_activation(&mut status.activation, state, events, control_plane)
            .await
            .with_retry(backoff_data_plane_activate(state.failures))
            .map_err(Into::into);

    let notify_result = publication_status::update_notify_dependents(
        &mut status.publications,
        state,
        control_plane,
    )
    .await
    .context("failed to notify dependents")
    .map(|_| None);

    coalesce_results(
        state.failures,
        [
            auto_discover_result,
            dependencies_result,
            periodic_result,
            activate_result,
            notify_result,
        ],
    )
}

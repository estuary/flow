mod auto_discover;
use super::{
    backoff_data_plane_activate, coalesce_results, dependencies::Dependencies, ControlPlane,
    ControllerErrorExt, ControllerState, Event, Inbox, NextRun,
};
use crate::controllers::{activation, periodic, publication_status};
use anyhow::Context;
use itertools::Itertools;
use models::{
    status::capture::{AutoDiscoverStatus, CaptureStatus},
    CaptureEndpoint, RawValue,
};

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

    let updated_config_published =
        updated_config_publish(status, state, model, events, control_plane).await;
    if updated_config_published.as_ref().ok() == Some(&true) {
        return Ok(Some(NextRun::immediately()));
    }
    let updated_config_result = updated_config_published.map(|_| None);

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
            updated_config_result,
            dependencies_result,
            periodic_result,
            activate_result,
            notify_result,
        ],
    )
}

async fn updated_config_publish<C: ControlPlane>(
    status: &mut CaptureStatus,
    state: &ControllerState,
    model: &models::CaptureDef,
    events: &Inbox,
    control_plane: &C,
) -> anyhow::Result<bool> {
    // Return early if the inbox does not contain a ConfigUpdated event.
    if events
        .iter()
        .any(|(_, e)| !matches!(e, Some(Event::ConfigUpdated)))
    {
        return Ok(false);
    }

    let updated_config = control_plane
        .get_config_updates(state.catalog_name.clone())
        .await?;

    let Some(log) = updated_config.first() else {
        anyhow::bail!("expected value in updated config event.");
    };

    // Ignore config updates not from the current build.
    if log.shard.build != state.last_build_id {
        tracing::info!("Ignoring ConfigUpdated event due to build ID mismatch. Current build: {}. ConfigUpdated build: {}.", state.last_build_id, log.shard.build);
        return Ok(false);
    }

    let Some(updated_config) = log.fields.get("config") else {
        anyhow::bail!("expected config to be present in fields.");
    };

    let mut updated_model = model.clone();
    match &mut updated_model.endpoint {
        CaptureEndpoint::Connector(connector) => {
            // Overwrite the connector's config with the updated config.
            connector.config = RawValue::from_string(updated_config.to_string())?;
        }
        _ => {
            anyhow::bail!("expected Connector endpoint for config update event.");
        }
    }

    let capture_name = models::Capture::new(&state.catalog_name);
    let mut pending = publication_status::PendingPublication::new();
    let draft = pending.start_spec_update(
        state,
        "in response to an updated config emitted by the connector",
    );

    let draft_row = draft
        .captures
        .get_or_insert_with(&capture_name, || tables::DraftCapture {
            capture: capture_name.clone(),
            scope: tables::synthetic_scope(models::CatalogType::Capture, &capture_name),
            expect_pub_id: Some(state.last_pub_id),
            model: Some(updated_model.clone()),
            is_touch: false,
        });
    draft_row.is_touch = false;
    draft_row.model = Some(updated_model);

    let pub_result = pending
        .finish(state, &mut status.publications, control_plane)
        .await?
        .error_for_status()
        .do_not_retry()?;

    control_plane
        .delete_config_updates(state.catalog_name.clone(), state.last_build_id)
        .await?;

    Ok(true)
}

use crate::controllers::{
    ControlPlane, ControllerErrorExt, ControllerState, Inbox, NextRun, activation,
    backoff_data_plane_activate, backoff_publication_failure, coalesce_results, config_update,
    dependencies::Dependencies,
    periodic,
    publication_status::{self, PendingPublication},
    republish,
};
use anyhow::Context;
use itertools::Itertools;
use models::{
    MaterializationEndpoint, ModelDef, RawValue, SourceType,
    status::{
        Alerts,
        connector::ConfigUpdate,
        materialization::{MaterializationStatus, SourceCaptureStatus},
        publications::PublicationStatus,
    },
};
use std::collections::BTreeSet;
use tables::{LiveRow, utils::pointer_for_schema};

pub async fn update<C: ControlPlane>(
    status: &mut MaterializationStatus,
    state: &ControllerState,
    events: &Inbox,
    control_plane: &C,
    model: &models::MaterializationDef,
) -> anyhow::Result<Option<NextRun>> {
    publication_status::clear_pending_publication_next_after(&mut status.publications);

    let updated_config_published = config_update::updated_config_publish(
        state,
        &mut status.config_updates,
        &mut status.publications,
        &mut status.alerts,
        events,
        control_plane,
        |config_update: &ConfigUpdate| -> anyhow::Result<PendingPublication> {
            let Some(updated_config) = config_update.fields.get("config") else {
                anyhow::bail!("expected config to be present in fields");
            };

            let mut updated_model = model.clone();
            match &mut updated_model.endpoint {
                MaterializationEndpoint::Connector(connector) => {
                    // Overwrite the connector's config with the updated config.
                    connector.config = RawValue::from_string(updated_config.to_string())?;
                }
                _ => {
                    anyhow::bail!("expected Connector endpoint for config update event");
                }
            }

            let materialization_name = models::Materialization::new(&state.catalog_name);
            let mut pending = PendingPublication::new();
            let draft =
                pending.start_spec_update(state, config_update::CONFIG_UPDATE_PUBLICATION_DETAIL);

            let draft_row =
                draft
                    .materializations
                    .get_or_insert_with(&materialization_name, || tables::DraftMaterialization {
                        materialization: materialization_name.clone(),
                        scope: tables::synthetic_scope(
                            models::CatalogType::Materialization,
                            &materialization_name,
                        ),
                        expect_pub_id: Some(state.last_pub_id),
                        model: Some(updated_model.clone()),
                        is_touch: false,
                    });

            draft_row.is_touch = false;
            draft_row.model = Some(updated_model);

            return Ok(pending);
        },
    )
    .await;
    if updated_config_published.as_ref().is_ok_and(|r| *r) {
        return Ok(Some(NextRun::immediately()));
    }

    let mut dependencies = Dependencies::resolve(state, control_plane).await?;
    let dep_result = dependencies
        .update(
            state,
            control_plane,
            &mut status.publications,
            &mut status.alerts,
            |deleted| Ok(handle_deleted_dependencies(deleted, model.clone())),
        )
        .await;
    if dep_result.as_ref().is_ok_and(|d| d.is_some()) {
        return Ok(Some(NextRun::immediately()));
    }

    let source_capture_published =
        source_capture_update(&mut dependencies, status, control_plane, state, model).await;
    if source_capture_published.as_ref().is_ok_and(|r| *r) {
        return Ok(Some(NextRun::immediately()));
    }

    let republish_result = republish::update_republish(
        events,
        state,
        &mut status.publications,
        &mut status.alerts,
        control_plane,
    )
    .await;
    if republish_result.as_ref().is_ok_and(|r| r.is_some()) {
        return Ok(Some(NextRun::immediately()));
    }

    let periodic_published = periodic::update_periodic_publish(
        state,
        &mut status.publications,
        &mut status.alerts,
        control_plane,
    )
    .await;
    if periodic_published.as_ref().is_ok_and(|r| r.is_some()) {
        return Ok(Some(NextRun::immediately()));
    }
    let periodic_result = periodic_published.map(|_| periodic::next_periodic_publish(state));

    let pending_publish = status.publications.next_after;
    let MaterializationStatus {
        activation,
        alerts,
        publications,
        ..
    } = status;
    let activation_result = activation::update_activation(
        activation,
        alerts,
        pending_publish,
        state,
        events,
        control_plane,
    )
    .await
    .with_retry(backoff_data_plane_activate(state.failures))
    .map_err(Into::into);

    let observe_result =
        publication_status::update_observed_pub_id(publications, alerts, state, control_plane)
            .await
            .map(|_| None);

    // There isn't any call to notify dependents because nothing currently can depend on a materialization.
    coalesce_results(
        state.failures,
        [
            observe_result,
            periodic_result,
            activation_result,
            source_capture_published.map(|_| None),
            updated_config_published.map(|_| None),
            dep_result.map(|_| None),
            republish_result.map(|_| None),
        ],
    )
}

async fn source_capture_update<C: ControlPlane>(
    dependencies: &mut Dependencies,
    status: &mut MaterializationStatus,
    control_plane: &C,
    state: &ControllerState,
    model: &models::MaterializationDef,
) -> anyhow::Result<bool> {
    if let Some(model_source_capture) = model.source.as_ref().filter(|s| s.capture_name().is_some())
    {
        let capture_name = model_source_capture
            .capture_name()
            .expect("capture must be Some");
        let MaterializationStatus {
            source_capture,
            publications,
            alerts,
            ..
        } = status;
        // If the source capture has been deleted, we should have already
        // removed the models sourceCapture as a part of
        // `handle_deleted_dependencies`.
        let Some(capture_model) = dependencies.live.captures.get_by_key(capture_name) else {
            anyhow::bail!("sourceCapture spec was missing from live dependencies");
        };
        let source_capture_status = source_capture.get_or_insert_with(Default::default);
        update_source_capture(
            source_capture_status,
            publications,
            alerts,
            state,
            control_plane,
            capture_model,
            model,
        )
        .await
    } else {
        status.source_capture.take();
        Ok(false)
    }
}

fn handle_deleted_dependencies(
    deleted: &BTreeSet<String>,
    mut model: models::MaterializationDef,
) -> (String, models::MaterializationDef) {
    let mut description = String::new();
    if let Some(source_model) = model.source.as_mut() {
        if let Some(deleted_capture) = source_model
            .capture_name()
            .filter(|c| deleted.contains(c.as_str()))
        {
            description = format!("removed source Capture: '{deleted_capture}'");
            *source_model = models::SourceType::Configured(
                source_model.to_normalized_def().without_source_capture(),
            );
        }
    }
    (description, model)
}

/// Adds bindings to match the sourceCapture if necessary, and returns a boolean indicating
/// whether the materialization was published. If `true`, then the controller should immediately
/// return and schedule a subsequent run.
pub async fn update_source_capture<C: ControlPlane>(
    status: &mut SourceCaptureStatus,
    pub_status: &mut PublicationStatus,
    alerts: &mut Alerts,
    state: &ControllerState,
    control_plane: &C,
    live_capture: &tables::LiveCapture,
    model: &models::MaterializationDef,
) -> anyhow::Result<bool> {
    const DETAIL_PREFIX: &str = "adding";
    let capture_spec = live_capture.model();

    // Did a prior attempt to add bindings fail?
    let prev_failed = !status.up_to_date;

    // Record the bindings that we plan to add. This will remain if we
    // return an error while trying to add them, so that we can see the new
    // bindings in the status if something goes wrong. If all goes well,
    // we'll clear this at the end.
    status.add_bindings = get_bindings_to_add(capture_spec, model);
    status.up_to_date = status.add_bindings.is_empty();
    if status.up_to_date {
        return Ok(false);
    }

    // Avoid generating a detail with hundreds of collection names
    let detail = if status.add_bindings.len() > 10 {
        format!(
            "{DETAIL_PREFIX} {} bindings to match the sourceCapture",
            status.add_bindings.len()
        )
    } else {
        format!(
            "{DETAIL_PREFIX} binding(s) to match the sourceCapture: [{}]",
            status.add_bindings.iter().join(", ")
        )
    };

    // Check whether the prior attempt failed, and whether we need to backoff
    // before trying again. Note that if the detail message changes (i.e. the
    // source capture bindings changed), then the backoff will effectively be
    // reset, because we match on the detail message. This is intentional, so
    // that changes which may allow the publication to succeed will get retried
    // immediately.
    publication_status::check_can_publish(pub_status, control_plane)?;
    if let Some((last_attempt, fail_count)) =
        super::last_pub_failed(pub_status, &detail).filter(|_| prev_failed)
    {
        let backoff = backoff_failed_source_capture_pub(fail_count);
        let next = last_attempt + backoff.with_jitter_percent(0).compute_duration();
        if next > control_plane.current_time() {
            return super::backoff_err(backoff, &detail, fail_count);
        }
    }

    // We need to update the materialization model to add the bindings. This
    // requires the `resource_spec_schema` of the connector so that we can
    // generate valid `resource`s for the new bindings.
    let image = match &model.endpoint {
        models::MaterializationEndpoint::Connector(config) => config.image.clone(),
        models::MaterializationEndpoint::Dekaf(dekaf) => dekaf.image_name(),
        _ => anyhow::bail!(
            "unexpected materialization endpoint type, only image and dekaf connectors are supported"
        ),
    };
    let connector_spec = control_plane
        .get_connector_spec(image.clone())
        .await
        .context("failed to fetch connector spec")?;
    let resource_spec_pointers = pointer_for_schema(connector_spec.resource_config_schema.get())
        .with_context(|| format!("fetching resource spec pointers for {}", image))?;

    let mut new_model = model.clone();
    update_linked_materialization(
        model.source.as_ref().unwrap(),
        resource_spec_pointers,
        &status.add_bindings,
        &mut new_model,
    )?;
    let mut pending_pub =
        PendingPublication::update_model(&state.catalog_name, state.last_pub_id, new_model, detail);
    let result = pending_pub
        .finish(state, pub_status, Some(alerts), control_plane)
        .await
        .context("executing source capture update publication")?;
    let prev_failures = super::last_pub_failed(&*pub_status, DETAIL_PREFIX)
        .map(|(_, count)| count)
        .unwrap_or(0);
    result
        .error_for_status()
        .with_retry(backoff_publication_failure(
            prev_failures + 1,
            control_plane.controller_publication_cooldown(),
        ))?;

    status.add_bindings.clear();
    status.up_to_date = true;

    Ok(true)
}

fn backoff_failed_source_capture_pub(failures: u32) -> NextRun {
    let mins = match failures {
        0..3 => failures * 3,
        _ => (failures * 10).min(300),
    };
    NextRun::after_minutes(mins)
}

fn get_bindings_to_add(
    capture_spec: &models::CaptureDef,
    materialization_spec: &models::MaterializationDef,
) -> BTreeSet<models::Collection> {
    // The set of collection names of the capture bindings.
    // Note that disabled bindings are not included here.
    let mut bindings_to_add = capture_spec.writes_to();

    // Remove any that are already present in the materialization, regardless of whether they are
    // disabled in the materialization. The goal is to preserve any changes that users have made to
    // the materialization bindings, so we only ever add new bindings to the materialization. We
    // don't remove or disable materialization bindings, as long as their source collections continue
    // to exist.
    for mat_binding in materialization_spec.bindings.iter() {
        bindings_to_add.remove(mat_binding.source.collection());
    }
    bindings_to_add
}

fn update_linked_materialization(
    source_capture: &SourceType,
    resource_spec_pointers: tables::utils::ResourceSpecPointers,
    bindings_to_add: &BTreeSet<models::Collection>,
    materialization: &mut models::MaterializationDef,
) -> anyhow::Result<()> {
    for collection_name in bindings_to_add {
        let mut resource_spec = serde_json::json!({});
        tables::utils::update_materialization_resource_spec(
            source_capture,
            &mut resource_spec,
            &resource_spec_pointers,
            &collection_name,
        )?;

        let binding = models::MaterializationBinding {
            resource: models::RawValue::from_value(&resource_spec),
            source: models::Source::Collection(collection_name.clone()),
            disable: false,
            fields: models::MaterializationFields {
                recommended: source_capture.to_normalized_def().fields_recommended,
                ..Default::default()
            },
            priority: Default::default(),
            backfill: 0,
            on_incompatible_schema_change: None,
        };
        materialization.bindings.push(binding);
    }

    Ok(())
}

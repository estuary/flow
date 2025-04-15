use super::{
    activation, backoff_data_plane_activate, coalesce_results, dependencies::Dependencies,
    periodic, publication_status::PendingPublication, ControlPlane, ControllerErrorExt,
    ControllerState, Inbox, NextRun,
};
use crate::publications::{PublicationResult, RejectedField};
use anyhow::Context;
use itertools::Itertools;
use models::{
    status::{
        materialization::{MaterializationStatus, SourceCaptureStatus},
        publications::PublicationStatus,
    },
    ModelDef, OnIncompatibleSchemaChange, SourceCapture,
};
use proto_flow::materialize::response::validated::constraint::Type as ConstraintType;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use tables::{utils::pointer_for_schema, LiveRow};

pub async fn update<C: ControlPlane>(
    status: &mut MaterializationStatus,
    state: &ControllerState,
    events: &Inbox,
    control_plane: &C,
    model: &models::MaterializationDef,
) -> anyhow::Result<Option<NextRun>> {
    let published = maybe_publish(status, state, control_plane, model).await;
    // Return immediately if we've successfully published. If a publication was
    // attempted, but failed, then we'll still attempt to update activation, so
    // that we can activate new builds and restart failed shards.
    if Some(&true) == published.as_ref().ok() {
        return Ok(Some(NextRun::immediately()));
    }

    let activation_next =
        activation::update_activation(&mut status.activation, state, events, control_plane)
            .await
            .with_retry(backoff_data_plane_activate(state.failures))
            .map_err(Into::into);

    // There isn't any call to notify dependents because nothing currently can depend on a materialization.
    coalesce_results([
        published.map(|_| None),
        activation_next,
        Ok(periodic::next_periodic_publish(state)),
    ])
}

async fn maybe_publish<C: ControlPlane>(
    status: &mut MaterializationStatus,
    state: &ControllerState,
    control_plane: &C,
    model: &models::MaterializationDef,
) -> anyhow::Result<bool> {
    let mut dependencies = Dependencies::resolve(state, control_plane).await?;

    // Materializations use a slightly different process for updating based on changes in dependencies,
    // because we need to handle the schema evolution whenever we publish. The collection schemas could have changed
    // since the last publish, and we might need to apply `onIncompatibleSchemaChange` actions.
    let dependency_pub = dependencies
        .start_update(state, control_plane.current_time(), |deleted| {
            Ok(handle_deleted_dependencies(deleted, model.clone()))
        })
        .await?;
    if dependency_pub.has_pending() {
        do_publication(
            &mut status.publications,
            state,
            dependency_pub,
            control_plane,
        )
        .await?;
        return Ok(true);
    }

    if let Some(model_source_capture) = &model.source_capture {
        let MaterializationStatus {
            source_capture,
            publications,
            ..
        } = status;
        // If the source capture has been deleted, we should have already
        // removed the models sourceCapture as a part of
        // `handle_deleted_dependencies`.
        let Some(capture_model) = dependencies
            .live
            .captures
            .get_by_key(&model_source_capture.capture_name())
        else {
            anyhow::bail!("sourceCapture spec was missing from live dependencies");
        };
        let source_capture_status = source_capture.get_or_insert_with(Default::default);
        if update_source_capture(
            source_capture_status,
            publications,
            state,
            control_plane,
            capture_model,
            model,
        )
        .await?
        {
            // If the sourceCapture update published, then return and schedule another run immediately
            return Ok(true);
        }
    } else {
        status.source_capture.take();
    }

    let periodic = periodic::start_periodic_publish_update(state, control_plane)?;
    if periodic.has_pending() {
        do_publication(&mut status.publications, state, periodic, control_plane).await?;
        return Ok(true);
    }
    Ok(false)
}

/// Publishes, and handles any incompatibleCollections by automatically
/// applying `onIncompatibleSchemaChange` actions. Returns successful if
/// _either_ the initial publication, or a subsequent attempt after applying
/// evolutions actions was successful. Returns an error if the publication
/// was not successful.
async fn do_publication<C: ControlPlane>(
    pub_status: &mut PublicationStatus,
    state: &ControllerState,
    mut pending_pub: PendingPublication,
    control_plane: &C,
) -> anyhow::Result<()> {
    let mut result = pending_pub
        .finish(state, pub_status, control_plane)
        .await
        .context("failed to execute publication")?;

    if result.status.has_incompatible_collections() {
        let PublicationResult {
            built,
            mut detail,
            mut draft,
            ..
        } = result;
        let mut detail = detail
            .take()
            .expect("detail must be set for controller-initiated publications");
        detail.push_str(", and applying onIncompatibleSchemaChange actions");
        apply_evolution_actions(state, built, &mut draft).context("applying evolution actions")?;

        let new_result = PendingPublication::of(vec![detail], draft)
            .finish(state, pub_status, control_plane)
            .await
            .context("failed to execute publication")?;
        if !new_result.status.is_success() {
            tracing::warn!(
                publication_status = ?new_result.status,
                "publication failed after applying evolution actions"
            );
        }
        result = new_result;
    }

    // We retry materialization publication failures, because they primarily depend on the
    // availability and state of an external system. But we don't retry indefinitely, since
    // oftentimes users will abandon live tasks after deleting those external systems.
    result
        .error_for_status()
        .with_maybe_retry(backoff_publication_failure(state.failures))?;
    Ok(())
}

fn apply_evolution_actions(
    state: &ControllerState,
    built: tables::Validations,
    draft: &mut tables::DraftCatalog,
) -> anyhow::Result<usize> {
    let mat_name = models::Materialization::new(state.catalog_name.as_str());
    let built_row = built
        .built_materializations
        .get_by_key(&mat_name)
        .ok_or_else(|| anyhow::anyhow!("missing built row for materialization"))?;
    let draft_materialization = draft
        .materializations
        .get_mut_by_key(&mat_name)
        .ok_or_else(|| anyhow::anyhow!("missing draft row for materialization"))?;

    // We intend to modify the spec
    draft_materialization.is_touch = false;
    let draft_model = draft_materialization
        .model
        .as_mut()
        .ok_or_else(|| anyhow::anyhow!("missing draft model for materialization"))?;

    let mut updated = 0;
    for (i, binding) in built_row
        .validated
        .iter()
        .flat_map(|v| v.bindings.iter())
        .enumerate()
    {
        let naughty_fields: Vec<RejectedField> = binding
            .constraints
            .iter()
            .filter(|(_, constraint)| constraint.r#type == ConstraintType::Unsatisfiable as i32)
            .map(|(field, constraint)| RejectedField {
                field: field.clone(),
                reason: constraint.reason.clone(),
            })
            .collect();

        if naughty_fields.is_empty() {
            continue;
        }

        // find the draft materialization binding that corresponds to the validated binding.
        // We'd ideally like to use the resource path pointers to do this, but we don't
        // have them yet for materialization connectors. So instead we'll use the index, which
        // means we need to account for disabled bindings in the model.
        let Some(draft_binding) = draft_model
            .bindings
            .iter_mut()
            .filter(|b| !b.disable)
            .nth(i)
        else {
            panic!("model is missing binding corresponding to validated binding {i}");
        };

        let behavior = draft_binding
            .on_incompatible_schema_change
            .unwrap_or(draft_model.on_incompatible_schema_change);
        match behavior {
            OnIncompatibleSchemaChange::Abort => {
                let resource_path = binding.resource_path.iter().format(", ");
                // We still consider this a retryable error, since technically
                // the external system could change and the connector might not
                // return an unsatisfiable constraint on a subsequent attempt.
                return Err(anyhow::anyhow!(
                        "incompatible schema changes observed for binding [{resource_path}] and onIncompatibleSchemaChange is 'abort'"
                    ));
            }
            OnIncompatibleSchemaChange::Backfill => {
                draft_binding.backfill += 1;
            }
            OnIncompatibleSchemaChange::DisableBinding => {
                draft_binding.disable = true;
            }
            OnIncompatibleSchemaChange::DisableTask => {
                draft_model.shards.disable = true;
            }
        };
        tracing::info!(
            resource_path = ?binding.resource_path,
            incompatible_fields = ?naughty_fields,
            resolution_action = ?behavior,
            "applied evolution action to binding"
        );
        updated += 1;
    }
    Ok(updated)
}

fn backoff_publication_failure(prev_failures: i32) -> Option<NextRun> {
    if prev_failures < 3 {
        Some(NextRun::after_minutes(prev_failures.max(1) as u32))
    } else if prev_failures < 10 {
        Some(NextRun::after_minutes(prev_failures as u32 * 60))
    } else {
        None
    }
}

fn handle_deleted_dependencies(
    deleted: &BTreeSet<String>,
    mut model: models::MaterializationDef,
) -> (String, models::MaterializationDef) {
    let mut descriptions = Vec::new();
    let mut deleted_collections = BTreeSet::new();
    let mut disable_count = 0;
    for binding in model.bindings.iter_mut() {
        if deleted.contains(binding.source.collection().as_str()) && !binding.disable {
            disable_count += 1;
            deleted_collections.insert(binding.source.collection().as_str());
            binding.disable = true;
        }
    }
    if disable_count > 0 {
        descriptions.push(format!(
            "disabled {disable_count} binding(s) in response to deleted collections: [{}]",
            deleted_collections.iter().format(", ")
        ));
    }
    if let Some(source_capture) = model
        .source_capture
        .take_if(|sc| deleted.contains(sc.capture_name().as_str()))
    {
        descriptions.push(format!(
            r#"removed sourceCapture: "{}" because the capture was deleted"#,
            source_capture.capture_name()
        ))
    };
    (descriptions.iter().join(", "), model)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum Action {
    Added(Vec<String>),
    Removed(Vec<String>),
}

/// Adds bindings to match the sourceCapture if necessary, and returns a boolean indicating
/// whether the materialization was published. If `true`, then the controller should immediately
/// return and schedule a subsequent run.
pub async fn update_source_capture<C: ControlPlane>(
    status: &mut SourceCaptureStatus,
    pub_status: &mut PublicationStatus,
    state: &ControllerState,
    control_plane: &C,
    live_capture: &tables::LiveCapture,
    model: &models::MaterializationDef,
) -> anyhow::Result<bool> {
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
            "adding {} bindings to match the sourceCapture",
            status.add_bindings.len()
        )
    } else {
        format!(
            "adding binding(s) to match the sourceCapture: [{}]",
            status.add_bindings.iter().join(", ")
        )
    };

    // Check whether the prior attempt failed, and whether we need to backoff
    // before trying again. Note that if the detail message changes (i.e. the
    // source capture bindings changed), then the backoff will effectively be
    // reset, because we match on the detail message. This is intentional, so
    // that changes which may allow the publication to succeed will get retried
    // immediately.
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
    let models::MaterializationEndpoint::Connector(config) = &model.endpoint else {
        anyhow::bail!(
            "unexpected materialization endpoint type, only image connectors are supported"
        );
    };
    let connector_spec = control_plane
        .get_connector_spec(config.image.clone())
        .await
        .context("failed to fetch connector spec")?;
    let resource_spec_pointers = pointer_for_schema(connector_spec.resource_config_schema.get())?;

    let mut new_model = model.clone();
    update_linked_materialization(
        model.source_capture.as_ref().unwrap(),
        resource_spec_pointers,
        &status.add_bindings,
        &mut new_model,
    )?;
    let pending_pub =
        PendingPublication::update_model(&state.catalog_name, state.last_pub_id, new_model, detail);
    do_publication(pub_status, state, pending_pub, control_plane)
        .await
        .context("publishing changes from sourceCapture")?;
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
    source_capture: &SourceCapture,
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
            fields: Default::default(),
            priority: Default::default(),
            backfill: 0,
            on_incompatible_schema_change: None,
        };
        materialization.bindings.push(binding);
    }

    Ok(())
}

use std::collections::BTreeSet;

use super::{
    backoff_data_plane_activate, coalesce_results, dependencies::Dependencies, periodic,
    publication_status::PendingPublication, ControlPlane, ControllerErrorExt, ControllerState,
    Inbox, NextRun,
};
use crate::controllers::{activation, publication_status};
use anyhow::Context;
use itertools::Itertools;
use models::status::{
    collection::{CollectionStatus, InferredSchemaStatus},
    publications::PublicationStatus,
};

pub async fn update<C: ControlPlane>(
    status: &mut CollectionStatus,
    state: &ControllerState,
    events: &Inbox,
    control_plane: &C,
    model: &models::CollectionDef,
) -> anyhow::Result<Option<NextRun>> {
    let published = maybe_publish(status, state, control_plane, model).await;
    // Return now only if a publication was performed successfully. If the
    // publication failed, then we still attempt to update the activation.
    if Some(&true) == published.as_ref().ok() {
        return Ok(Some(NextRun::immediately()));
    }

    let activation_result =
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
    .map(|_| None);

    // Use an infrequent periodic check for inferred schema updates, just in case the database trigger gets
    // bypassed for some reason.
    let inferred_schema_next = if uses_inferred_schema(model) {
        Some(NextRun::after_minutes(240))
    } else {
        None
    };
    coalesce_results(
        state.failures,
        [
            published.map(|_| periodic::next_periodic_publish(state)),
            Ok(inferred_schema_next),
            activation_result,
            notify_result,
        ],
    )
}

/// Performs a publication of the spec, if necessary.
/// Collections may be published in order to
/// - Update the inferred schema
/// - Update inlined spec dependencies
/// - Periodically rebuild the spec
///
/// Returns a boolean indicating whether a publication was performed.
/// Note that unlike captures and materializations, all collection publications
/// are practically equivalent, meaning that there's no reason to attempt
/// another publication in the same controller run if one of these fails. And
/// any successful publication of the collection will satisfy all of the reasons
/// why we might publish. This is why we can have a single `maybe_publish` function
/// instead of needing separate results like captures and materializations.
async fn maybe_publish<C: ControlPlane>(
    status: &mut CollectionStatus,
    state: &ControllerState,
    control_plane: &C,
    model: &models::CollectionDef,
) -> anyhow::Result<bool> {
    // We don't care whether derivation shards are disabled, because the collection is
    // still usable as a regular collection in that case.
    let uses_inferred_schema = uses_inferred_schema(model);
    if uses_inferred_schema {
        let inferred_schema_status = status.inferred_schema.get_or_insert_with(Default::default);
        if update_inferred_schema(
            inferred_schema_status,
            state,
            control_plane,
            model,
            &mut status.publications,
        )
        .await?
        {
            return Ok(true);
        }
    } else {
        status.inferred_schema = None;
    };

    let mut dependencies = Dependencies::resolve(state, control_plane).await?;
    if dependencies
        .update(state, control_plane, &mut status.publications, |deleted| {
            handle_deleted_dependencies(model.clone(), deleted)
        })
        .await?
    {
        return Ok(true);
    }

    if periodic::update_periodic_publish(state, &mut status.publications, control_plane).await? {
        return Ok(true);
    }

    Ok(false)
}

/// Disables transforms that source from deleted collections.
/// Expects the draft to already contain the collection spec, which must be a derivation.
fn handle_deleted_dependencies(
    mut model: models::CollectionDef,
    deleted: &BTreeSet<String>,
) -> anyhow::Result<(String, models::CollectionDef)> {
    let derive = model
        .derive
        .as_mut()
        .expect("must be a derivation if it has dependencies");
    let mut disable_count = 0;
    for transform in derive.transforms.iter_mut() {
        if deleted.contains(transform.source.collection().as_str()) && !transform.disable {
            disable_count += 1;
            transform.disable = true;
        }
    }
    let detail = format!(
        "disabled {disable_count} transform(s) in response to deleted collections: [{}]",
        deleted.iter().format(", ")
    );
    Ok((detail, model))
}

pub async fn update_inferred_schema<C: ControlPlane>(
    status: &mut InferredSchemaStatus,
    state: &ControllerState,
    control_plane: &C,
    collection_def: &models::CollectionDef,
    publication_status: &mut PublicationStatus,
) -> anyhow::Result<bool> {
    let collection_name = models::Collection::new(&state.catalog_name);

    let maybe_inferred_schema = control_plane
        .get_inferred_schema(collection_name.clone())
        .await
        .context("fetching inferred schema")?;

    if let Some(inferred_schema) = maybe_inferred_schema {
        let mut pending_pub = PendingPublication::new();
        let tables::InferredSchema {
            collection_name,
            schema: _, // we let the publications handler set the inferred schema
            md5,
        } = inferred_schema;

        if status.schema_md5.as_ref() != Some(&md5) {
            tracing::info!(
                %collection_name,
                prev_md5 = ?status.schema_md5,
                new_md5 = ?md5,
                "updating inferred schema"
            );
            let draft = pending_pub.start_spec_update(state, "updating inferred schema");
            let draft_row = draft.collections.get_or_insert_with(&collection_name, || {
                tables::DraftCollection {
                    collection: collection_name.clone(),
                    scope: tables::synthetic_scope(
                        models::CatalogType::Collection,
                        &collection_name,
                    ),
                    expect_pub_id: Some(state.last_pub_id),
                    model: Some(collection_def.clone()),
                    is_touch: false, // We intend to update the model
                }
            });
            // The inferred schema is always updated as part of any non-touch publication,
            // so we don't need to actually update the model here.
            draft_row.is_touch = false;

            let pub_result = pending_pub
                .finish(state, publication_status, control_plane)
                .await?
                .error_for_status()
                .do_not_retry()?;

            status.schema_md5 = Some(md5);
            status.schema_last_updated = Some(pub_result.started_at);
            return Ok(true);
        }
    } else {
        tracing::debug!(%collection_name, "No inferred schema available yet");
    }

    Ok(false)
}

// I'm not sure this worked in the first place because the schema is wrapped in an allOf with
// REF_RELAXED_WRITE_SCHEMA_URL, but it's certainly broken now.
pub fn uses_inferred_schema(collection: &models::CollectionDef) -> bool {
    collection
        .read_schema
        .as_ref()
        .map(models::Schema::references_inferred_schema)
        .unwrap_or(false)
}

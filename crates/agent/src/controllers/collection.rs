use super::{
    backoff_data_plane_activate,
    publication_status::{ActivationStatus, Dependencies, PendingPublication},
    ControlPlane, ControllerErrorExt, ControllerState, NextRun,
};
use crate::controllers::publication_status::PublicationStatus;
use anyhow::Context;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The status of a collection controller
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, JsonSchema)]
pub struct CollectionStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inferred_schema: Option<InferredSchemaStatus>,
    #[serde(default)]
    pub publications: PublicationStatus,
    #[serde(default)]
    pub activation: ActivationStatus,
}

impl CollectionStatus {
    pub async fn update<C: ControlPlane>(
        &mut self,
        state: &ControllerState,
        control_plane: &mut C,
        model: &models::CollectionDef,
    ) -> anyhow::Result<Option<NextRun>> {
        let mut pending_pub = PendingPublication::new();
        let dependencies = self
            .publications
            .resolve_dependencies(state, control_plane)
            .await?;
        if let Some(pub_id) = dependencies.next_pub_id {
            let draft = pending_pub.start_spec_update(
                pub_id,
                state,
                format!("in response to publication of one or more depencencies"),
            );
            if !dependencies.deleted.is_empty() {
                let add_detail = handle_deleted_dependencies(draft, state, dependencies);
                pending_pub.update_pending_draft(add_detail);
            }
        }

        let inferred_schema_next_run = if uses_inferred_schema(model) {
            if self.inferred_schema.is_none() {
                self.inferred_schema = Some(InferredSchemaStatus::default());
            }
            self.inferred_schema
                .as_mut()
                .unwrap()
                .update(
                    state,
                    control_plane,
                    model,
                    &mut self.publications,
                    &mut pending_pub,
                )
                .await?
        } else {
            self.inferred_schema = None;
            None
        };

        if pending_pub.has_pending() {
            // If the publication fails, then it's quite unlikely to succeed if
            // we were to retry it. So consider this a terminal error.
            let _result = pending_pub
                .finish(state, &mut self.publications, control_plane)
                .await?;
        } else {
            // Not much point in activating if we just published, since we're going to be
            // immediately invoked again.
            self.activation
                .update(state, control_plane)
                .await
                .with_retry(backoff_data_plane_activate(state.failures))?;
            // Wait until after activation is complete to notify dependents.
            // This just helps encourage more orderly rollouts.
            self.publications
                .notify_dependents(state, control_plane)
                .await?;
        }

        Ok(inferred_schema_next_run)
    }
}

/// Disables transforms that source from deleted collections.
/// Expects the draft to already contain the collection spec, which must be a derivation.
fn handle_deleted_dependencies(
    draft: &mut tables::DraftCatalog,
    state: &ControllerState,
    dependencies: Dependencies,
) -> String {
    let drafted = draft
        .collections
        .get_mut_by_key(&models::Collection::new(&state.catalog_name))
        .expect("collection must have been drafted");
    let model = drafted
        .model
        .as_mut()
        .expect("model must be Some since collection is not deleted");
    let derive = model
        .derive
        .as_mut()
        .expect("must be a derivation if it has dependencies");
    let mut disable_count = 0;
    for transform in derive.transforms.iter_mut() {
        if dependencies
            .deleted
            .contains(transform.source.collection().as_str())
            && !transform.disable
        {
            disable_count += 1;
            transform.disable = true;
        }
    }
    format!(
        "disabled {disable_count} transform(s) in response to deleted collections: [{}]",
        dependencies.deleted.iter().format(", ")
    )
}

/// Status of the inferred schema
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, JsonSchema)]
pub struct InferredSchemaStatus {
    /// The time at which the inferred schema was last published. This will only
    /// be present if the inferred schema was published at least once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "super::datetime_schema")]
    pub schema_last_updated: Option<DateTime<Utc>>,
    /// The md5 sum of the inferred schema that was last published
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_md5: Option<String>,
}

impl InferredSchemaStatus {
    pub async fn update<C: ControlPlane>(
        &mut self,
        state: &ControllerState,
        control_plane: &mut C,
        collection_def: &models::CollectionDef,
        publication_status: &mut PublicationStatus,
        pending_pub: &mut PendingPublication,
    ) -> anyhow::Result<Option<NextRun>> {
        let collection_name = models::Collection::new(&state.catalog_name);

        if !uses_inferred_schema(collection_def) {
            self.schema_md5 = None;
            return Ok(None);
        }

        let maybe_inferred_schema = control_plane
            .get_inferred_schema(collection_name.clone())
            .await
            .context("fetching inferred schema")?;

        if let Some(inferred_schema) = maybe_inferred_schema {
            let tables::InferredSchema {
                collection_name,
                schema,
                md5,
            } = inferred_schema;

            if self.schema_md5.as_ref() != Some(&md5) {
                tracing::info!(
                    %collection_name,
                    prev_md5 = ?self.schema_md5,
                    new_md5 = ?md5,
                    "updating inferred schema"
                );
                let draft =
                    pending_pub.update_pending_draft("updating inferred schema".to_string());
                let draft_row = draft.collections.get_or_insert_with(&collection_name, || {
                    tables::DraftCollection {
                        collection: collection_name.clone(),
                        scope: tables::synthetic_scope(
                            models::CatalogType::Collection,
                            &collection_name,
                        ),
                        expect_pub_id: Some(state.last_pub_id),
                        model: Some(collection_def.clone()),
                    }
                });
                update_inferred_schema(draft_row, &schema)?;

                let pub_result = pending_pub
                    .finish(state, publication_status, control_plane)
                    .await?
                    .error_for_status()
                    .do_not_retry()?;

                self.schema_md5 = Some(md5);
                self.schema_last_updated = Some(pub_result.started_at);
            }
        } else {
            tracing::debug!(%collection_name, "No inferred schema available yet");
        }

        // Keep an infrequent periodic check, as a fallback in case the database trigger
        // gets disabled.
        Ok(Some(NextRun::after_minutes(180)))
    }
}

fn update_inferred_schema(
    collection: &mut tables::DraftCollection,
    inferred_schema: &models::Schema,
) -> anyhow::Result<()> {
    let Some(model) = collection.model.as_mut() else {
        anyhow::bail!("missing model to update inferred schema");
    };
    let new_read_schema = {
        let Some(read_schema) = model.read_schema.as_ref() else {
            anyhow::bail!("model is missing read schema");
        };
        let Some(write_schema) = model.write_schema.as_ref() else {
            anyhow::bail!("model is missing write schema");
        };
        models::Schema::extend_read_bundle(read_schema, write_schema, Some(inferred_schema))
    };

    model.read_schema = Some(new_read_schema);
    Ok(())
}

pub fn uses_inferred_schema(collection: &models::CollectionDef) -> bool {
    collection
        .read_schema
        .as_ref()
        .map(models::Schema::references_inferred_schema)
        .unwrap_or(false)
}

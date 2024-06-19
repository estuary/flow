use super::{
    backoff_data_plane_activate,
    publication_status::{ActivationStatus, Dependencies},
    ControlPlane, ControllerErrorExt, ControllerState, NextRun,
};
use crate::controllers::publication_status::PublicationStatus;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
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
        let dependencies = Dependencies::resolve(&state.live_spec, control_plane).await?;
        if dependencies.is_publication_required(state) {
            let draft = self
                .publications
                .start_spec_update(
                    dependencies.next_pub_id(control_plane),
                    state,
                    format!("in response to publication of one or more depencencies"),
                )
                .await;
            if !dependencies.deleted.is_empty() {
                let add_detail = handle_deleted_dependencies(draft, state, dependencies);
                self.publications
                    .update_pending_draft(add_detail, control_plane);
            }
        }

        let inferred_schema_next_run = if uses_inferred_schema(model) {
            if self.inferred_schema.is_none() {
                self.inferred_schema = Some(InferredSchemaStatus::default());
            }
            self.inferred_schema
                .as_mut()
                .unwrap()
                .update(state, control_plane, model, &mut self.publications)
                .await?
        } else {
            self.inferred_schema = None;
            None
        };

        if self.publications.has_pending() {
            // If the publication fails, then it's quite unlikely to succeed if
            // we were to retry it. So consider this a terminal error.
            let _result = self
                .publications
                .finish_pending_publication(state, control_plane)
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

#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
pub struct InferredSchemaStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_last_updated: Option<DateTime<Utc>>,
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
    ) -> anyhow::Result<Option<NextRun>> {
        let collection_name = models::Collection::new(&state.catalog_name);

        if !uses_inferred_schema(collection_def) {
            self.schema_md5 = None;
            return Ok(None);
        }

        let maybe_inferred_schema = control_plane
            .get_inferred_schema(collection_name.clone())
            .await
            .expect("failed to fetch inferred schema");

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
                let publication = publication_status
                    .update_pending_draft("updating inferred schema".to_string(), control_plane);
                let draft_row =
                    publication
                        .draft
                        .collections
                        .get_or_insert_with(&collection_name, || tables::DraftCollection {
                            collection: collection_name.clone(),
                            scope: tables::synthetic_scope(
                                models::CatalogType::Collection,
                                &collection_name,
                            ),
                            expect_pub_id: Some(state.last_pub_id),
                            model: Some(collection_def.clone()),
                        });
                update_inferred_schema(draft_row, &schema)?;

                // Don't retry publications, since they're unlikely to succeed.
                let pub_result = publication_status
                    .finish_pending_publication(state, control_plane)
                    .await
                    .expect("failed to execute publication")
                    .error_for_status()?;

                self.schema_md5 = Some(md5);
                self.schema_last_updated = Some(pub_result.started_at);
            }
        } else {
            tracing::debug!(%collection_name, "No inferred schema available yet");
        }

        let next_run = Some(self.next_run(state.updated_at, control_plane.current_time()));
        Ok(next_run)
    }

    fn next_run(&self, last_update: DateTime<Utc>, now: DateTime<Utc>) -> NextRun {
        // The idea here is to check frequently if there isn't an inferred schema at all yet,
        // so we can quickly start materializing some data. But after it works at least once,
        // then we can slow down a little.
        let min_backoff_minutes = if self.schema_md5.is_none() {
            1i64
        } else {
            10i64
        };

        // We use a simple heuristic to determine how long to wait before
        // checking the inferred schema again: how long has it been since
        // the last time the inferred schema was updated. Then clamp that
        // duration into a reasonable bounds. Collections that see very
        // frequent inferred schema updates will be checked much more
        // frequently, while those that are updated infrequently will be
        // checked somewhat less frequently.
        let start_time = self.schema_last_updated.unwrap_or(last_update);
        let after_minutes = now
            .signed_duration_since(start_time)
            .num_minutes()
            .max(min_backoff_minutes)
            .min(120);

        NextRun::after_minutes(after_minutes as u32).with_jitter_percent(25)
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

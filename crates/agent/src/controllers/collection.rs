use super::{
    publication_status::Dependencies, reduce_next_run, ControlPlane, ControllerState, NextRun,
};
use crate::controllers::publication_status::PublicationStatus;
use chrono::{DateTime, Duration, Utc};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct CollectionStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inferred_schema: Option<InferredSchemaStatus>,
    #[serde(default)]
    pub publications: PublicationStatus,
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
                .await?;
            if !dependencies.deleted.is_empty() {
                let add_detail = handle_deleted_dependencies(draft, state, dependencies);
                self.publications
                    .update_pending_draft(add_detail, control_plane);
            }
        }

        let uses_inferred_schema = uses_inferred_schema(model);
        let may_update_inferred_schema = self
            .inferred_schema
            .as_ref()
            .map(|status| status.may_update(control_plane.current_time()))
            .unwrap_or(true);
        if uses_inferred_schema && may_update_inferred_schema {
            if self.inferred_schema.is_none() {
                self.inferred_schema = Some(InferredSchemaStatus::default());
            }
            self.inferred_schema
                .as_mut()
                .unwrap()
                .update(state, control_plane, model, &mut self.publications)
                .await?;
        } else if !uses_inferred_schema {
            self.inferred_schema = None;
        };
        let inferred_schema_next_run = self
            .inferred_schema
            .as_ref()
            .map(|status| status.next_run(state.updated_at, control_plane.current_time()));

        if self.publications.has_pending() {
            // If the publication fails, then the only recourse is to retry later. We'll defer to the
            // `self.publications` to determine when that should be. In the future, we may want to
            // handle schema incompatibilities for derivations, and this is where we would do that.
            let _result = self
                .publications
                .finish_pending_publication(state, control_plane)
                .await?;
        } else {
            self.publications
                .notify_dependents(state, control_plane)
                .await?;
        }

        Ok(reduce_next_run(&[
            inferred_schema_next_run,
            self.publications.next_run(state),
        ]))
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
            .await?;

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

                let pub_result = publication_status
                    .finish_pending_publication(state, control_plane)
                    .await?;

                if pub_result.publication_status.is_success() {
                    self.schema_md5 = Some(md5);
                    self.schema_last_updated = Some(pub_result.started_at);
                } else {
                    anyhow::bail!(
                        "Failed to publish inferred schema: {:?}",
                        pub_result.publication_status
                    );
                }
            }
        } else {
            tracing::debug!(%collection_name, "No inferred schema available yet");
        }

        let next_run = Some(self.next_run(state.updated_at, control_plane.current_time()));
        Ok(next_run)
    }

    fn may_update(&self, current_time: DateTime<Utc>) -> bool {
        if let Some(last_update) = self.schema_last_updated {
            let elapsed = current_time.signed_duration_since(last_update);
            elapsed > MIN_SCHEMA_UPDATE_BACKOFF
        } else {
            true
        }
    }

    fn next_run(&self, last_update: DateTime<Utc>, now: DateTime<Utc>) -> NextRun {
        // The idea here is to check frequently if there isn't an inferred schema at all yet,
        // so we can quickly start materializing some data. But after it works at least once,
        // then we want to use a longer duration in order to coalesce more schema updates into
        // each publication to prevent undue churn.
        // TODO: we might want to account for `last_backfill` times here
        let min_backoff_minutes = if self.schema_md5.is_none() {
            1i64
        } else {
            10i64
        };

        let start_time = self.schema_last_updated.unwrap_or(last_update);
        let after_minutes = now
            .signed_duration_since(start_time)
            .num_minutes()
            .max(min_backoff_minutes)
            .min(90);

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
    // TODO: we might want to change how we determine whether a collection uses an inferred schema
    collection
        .read_schema
        .as_ref()
        .map(models::Schema::references_inferred_schema)
        .unwrap_or(false)
}

/// The minimum time to wait in between inferred schema updates.
/// We want to avoid having too much churn due to inferred schemas that are constantly changing.
const MIN_SCHEMA_UPDATE_BACKOFF: Duration = Duration::minutes(5);

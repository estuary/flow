use super::{publication_status::PublicationInfo, ControlPlane, ControllerState, NextRun};
use crate::{
    controllers::publication_status::{Dependencies, PublicationStatus},
    publications::{builds::RejectedField, PublicationResult},
};
use anyhow::Context;
use itertools::Itertools;
use models::OnIncompatibleSchemaChange;
use proto_flow::materialize::response::validated::constraint::Type as ConstraintType;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use tables::{LiveRow, SpecExt};

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MaterializationStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_capture: Option<SourceCaptureStatus>,
    #[serde(default)]
    pub publications: PublicationStatus,
}
impl MaterializationStatus {
    pub async fn update<C: ControlPlane>(
        &mut self,
        state: &ControllerState,
        control_plane: &mut C,
        model: &models::MaterializationDef,
    ) -> anyhow::Result<Option<NextRun>> {
        let dependencies = Dependencies::resolve(&state.live_spec, control_plane).await?;
        if dependencies.is_publication_required(state) {
            let pub_id = dependencies.next_pub_id(control_plane);
            let draft = self
                .publications
                .start_spec_update(
                    pub_id,
                    state,
                    format!("in response to publication of one or more depencencies"),
                )
                .await?;
            if !dependencies.deleted.is_empty() {
                let mat_name = models::Materialization::new(&state.catalog_name);
                let draft_row = draft.materializations.get_mut_by_key(&mat_name);
                let model = draft_row.unwrap().model.as_mut().unwrap();
                let add_detail = handle_deleted_dependencies(
                    &dependencies.deleted,
                    model,
                    &mut self.source_capture,
                );
                self.publications
                    .update_pending_draft(add_detail, control_plane);
            }
        }

        if let Some(source_capture_name) = &model.source_capture {
            // If the source capture has been deleted, we will have already handled that as a
            // part of `handle_deleted_dependencies`.
            if let Some(source_capture_model) =
                dependencies.live.captures.get_by_key(source_capture_name)
            {
                if self.source_capture.is_none() {
                    self.source_capture = Some(SourceCaptureStatus::default());
                }
                let source_capture_status = self.source_capture.as_mut().unwrap();
                source_capture_status
                    .update(
                        state,
                        control_plane,
                        source_capture_model,
                        model,
                        &mut self.publications,
                    )
                    .await?;
            }
        }

        if self.publications.has_pending() {
            let result = self
                .publications
                .finish_pending_publication(state, control_plane)
                .await?;
            if result.publication_status.has_incompatible_collections() {
                let PublicationResult {
                    publication_id,
                    built,
                    mut detail,
                    mut draft,
                    ..
                } = result;
                detail
                    .as_mut()
                    .unwrap()
                    .push_str(", and incrementing backfill counter(s)");
                self.apply_evolution_actions(state, built, &mut draft)
                    .context("applying evolution actions")?;

                let new_result = control_plane
                    .publish(publication_id, detail, state.logs_token, draft)
                    .await?;
                self.publications
                    .record_result(PublicationInfo::observed(&new_result));
                if !new_result.publication_status.is_success() {
                    tracing::warn!(
                        publication_status = ?new_result.publication_status,
                        "publication failed after applying evolution actions (will retry)"
                    );
                }
            }
            // If the publication was successful, update the source capture status to reflect that it's up-to-date.
            match self.source_capture.as_mut() {
                Some(status) if result.publication_status.is_success() => status.publish_success(),
                _ => {}
            }
        }

        Ok(self.publications.next_run(state))
    }

    fn apply_evolution_actions(
        &mut self,
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
                    return Err(anyhow::anyhow!(
                        "unsatisfiable constraints on binding {i} after evolution actions"
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
}

fn handle_deleted_dependencies(
    deleted: &BTreeSet<String>,
    drafted: &mut models::MaterializationDef,
    source_capture: &mut Option<SourceCaptureStatus>,
) -> String {
    let mut descriptions = Vec::new();
    let mut deleted_collections = BTreeSet::new();
    let mut disable_count = 0;
    for binding in drafted.bindings.iter_mut() {
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
    if drafted
        .source_capture
        .as_ref()
        .map(|sc| deleted.contains(sc.as_str()))
        .unwrap_or(false)
    {
        let capture_name = drafted.source_capture.take().unwrap();
        source_capture.take();
        descriptions.push(format!(
            r#"removed sourceCapture: "{capture_name}" because the capture was deleted"#
        ))
    }
    descriptions.iter().join(", ")
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum Action {
    Added(Vec<String>),
    Removed(Vec<String>),
}

fn is_false(b: &bool) -> bool {
    !*b
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct SourceCaptureStatus {
    #[serde(default, skip_serializing_if = "is_false")]
    up_to_date: bool,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    add_bindings: BTreeSet<models::Collection>,
}

impl SourceCaptureStatus {
    fn publish_success(&mut self) {
        self.up_to_date = true;
        self.add_bindings.clear();
    }

    pub async fn update<C: ControlPlane>(
        &mut self,
        state: &ControllerState,
        control_plane: &mut C,
        live_capture: &tables::LiveCapture,
        model: &models::MaterializationDef,
        pub_status: &mut PublicationStatus,
    ) -> anyhow::Result<Option<NextRun>> {
        let capture_spec = live_capture.model();

        self.add_bindings = get_bindings_to_add(capture_spec, model);
        self.up_to_date = self.add_bindings.is_empty();
        if self.up_to_date {
            return Ok(None);
        }

        // Check the materialization bindings against those of the source capture and see if we need to update
        let models::MaterializationEndpoint::Connector(config) = &model.endpoint else {
            anyhow::bail!(
                "unexpected materialization endpoint type, only image connectors are supported"
            );
        };
        let connector_spec = control_plane
            .get_connector_spec(config.image.clone())
            .await?;
        let collection_name_pointer = crate::resource_configs::pointer_for_schema(
            connector_spec.resource_config_schema.get(),
        )?;

        // Avoid generating a detail with hundreds of collection names
        let detail = if self.add_bindings.len() > 10 {
            format!(
                "adding {} binding to match the sourceCapture",
                self.add_bindings.len()
            )
        } else {
            format!(
                "adding binding(s) to match the sourceCapture: [{}]",
                self.add_bindings.iter().join(", ")
            )
        };

        let draft = pub_status.update_pending_draft(detail, control_plane);
        let materialization_name = models::Materialization::new(&state.catalog_name);
        let draft_row =
            draft
                .draft
                .materializations
                .get_or_insert_with(&materialization_name, || tables::DraftMaterialization {
                    materialization: materialization_name.clone(),
                    scope: tables::synthetic_scope(
                        models::CatalogType::Materialization,
                        &state.catalog_name,
                    ),
                    expect_pub_id: Some(state.last_pub_id),
                    model: Some(model.clone()),
                });

        update_linked_materialization(
            collection_name_pointer,
            &self.add_bindings,
            draft_row.model.as_mut().unwrap(),
        )?;
        Ok(None)
    }
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
    resource_collection_name_ptr: doc::Pointer,
    bindings_to_add: &BTreeSet<models::Collection>,
    materialization: &mut models::MaterializationDef,
) -> anyhow::Result<()> {
    for collection_name in bindings_to_add {
        let mut resource_spec = serde_json::json!({});
        crate::resource_configs::update_materialization_resource_spec(
            &mut resource_spec,
            &resource_collection_name_ptr,
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

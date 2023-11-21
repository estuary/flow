use std::collections::{HashMap, HashSet};
use std::fmt::Write;

use crate::{draft::Error, Id};

use agent_sql::linked_materializations::{
    get_linked_materializations, CaptureSpecRow, InvalidSourceCapture, ValidateSourceCaptureInput,
};
use agent_sql::{drafts, publications::SpecRow};
use agent_sql::{Capability, CatalogType};
use anyhow::Context;
use models::{Collection, MaterializationBinding, MaterializationDef, Source};

#[cfg(test)]
mod test;

/// Returns the set of all capture names to use in querying for linked materializations.
/// This will include all captures that were:
/// - updated or deleted by this publication
/// - used as the `sourceCapture` of a materialization that was updated by this publication
/// The `spec_rows` should include only the specs that were modified by the publication, not
/// the "expanded" specs. The `draft_catalog` is only used to avoid parsing the materialization
/// specs again.
pub fn collect_source_capture_names(
    spec_rows: &[SpecRow],
    draft_catalog: &models::Catalog,
) -> Vec<String> {
    let maybe_linked_captures = spec_rows
        .iter()
        .filter_map(|row| {
            if row.live_type == Some(CatalogType::Capture) {
                Some(row.catalog_name.clone())
            } else if row.draft_type == Some(CatalogType::Materialization) {
                if let Some(materialization) = draft_catalog
                    .materializations
                    .get(&models::Materialization::new(&row.catalog_name))
                {
                    materialization
                        .source_capture
                        .as_ref()
                        .map(|sc| sc.to_string())
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect::<std::collections::HashSet<_>>();
    maybe_linked_captures.into_iter().collect()
}

/// Validates the `sourceCapture` field of any materializations in the draft.
/// It ensures that the materialization spec is read-authorized to the prefix
/// of the `sourceCapture`, and that it exists and is actually a capture.
/// For example, given `sourceCapture: acmeCo/foo/bar`, we'd require that the
/// materialization spec be read authorized to all of `acmeCo/foo/`. This is
/// because the capture may discover new collections, which would be put under
/// that prefix. This is not strictly required for correctness, since we'd still
/// enforce authZ when the materialization is published with the new bindings.
/// But without this check, we might leak information about collections that
/// a user doesn't have access to, since they'd be able to see them in the
/// draft_specs.
pub async fn validate_source_captures(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    draft_catalog: &models::Catalog,
    spec_rows: &[SpecRow],
) -> anyhow::Result<Vec<Error>> {
    let mut errors = Vec::new();
    let mut other_checks = Vec::new();

    for row in spec_rows
        .iter()
        .filter(|r| r.draft_type == Some(CatalogType::Materialization))
    {
        let spec = draft_catalog
            .materializations
            .get(&models::Materialization::new(row.catalog_name.as_str()))
            .expect("draft catalog must contain items from spec_rows");
        let Some(source_capture) = spec.source_capture.as_ref() else {
            continue;
        };
        let starting_error_count = errors.len();
        let catalog_name = row.catalog_name.as_str();
        let spec_capabilities = &row.spec_capabilities;
        // Check the permissions before checking that the sourceCapture exists
        // and is a capture, so we avoid leaking information about unauthorized
        // entities.
        if let Some(last_slash_index) = source_capture.rfind('/') {
            let capture_prefix = &source_capture[..(last_slash_index + 1)];
            if !spec_capabilities.iter().any(|cap| {
                capture_prefix.starts_with(&cap.object_role) && cap.capability >= Capability::Read
            }) {
                errors.push(Error {
                    catalog_name: row.catalog_name.clone(),
                    detail: format!(
                        "Specification '{catalog_name}' is not read-authorized to `sourceCapture` prefix: '{capture_prefix}'.\nAvailable grants are: {}",
                        serde_json::to_string_pretty(&spec_capabilities.0).unwrap(),
                    ),
                    ..Default::default()
                });
            }
        } else {
            // The value of sourceCapture is invalid, since all catalog names must include
            // at least one "/". Note that it would still be correct to just query for this
            // and return a "does not exist" error, but this specific error type seemed like
            // it might be more helpful.
            errors.push(Error {
                catalog_name: catalog_name.to_string(),
                detail: format!("Materialization '{catalog_name}' `sourceCapture` '{source_capture}' is not a valid catalog name"),
                ..Default::default()
            });
        }

        // If the draft doesn't contain the referenced capture, we'll need to
        // query the database in order to validate that the sourceCapture is
        // really a capture. We'll build up a list of capture-materialization
        // pairs, so we can check them all in a single query. We only do this if
        // we _didn't_ already add an authZ error.
        if !draft_catalog.captures.contains_key(source_capture)
            && errors.len() == starting_error_count
        {
            other_checks.push(ValidateSourceCaptureInput {
                source_capture_name: source_capture.as_str().to_owned(),
                materialization_name: row.catalog_name.clone(),
            });
        }
    }

    tracing::debug!(
        ?other_checks,
        "finished checking materialization sourceCapture permissions"
    );
    // We've already validated all the spec permissions, so all we're doing here
    // is validating that the sourceCaptures exist and have a `spec_type` of `capture`.
    let invalid =
        agent_sql::linked_materializations::find_invalid_captures(txn, other_checks).await?;
    for row in invalid {
        let InvalidSourceCapture {
            materialization_name,
            source_capture_name,
            live_type,
        } = row;

        let mut detail = format!(
            "Materialization '{materialization_name}' has invalid `sourceCapture`: expected '{source_capture_name}' to be a Capture, but "
        );
        if let Some(ty) = live_type {
            write!(&mut detail, "it is a {ty}").unwrap();
        } else {
            detail.push_str("it does not exist (or was deleted)");
        }
        errors.push(Error {
            catalog_name: materialization_name,
            scope: None,
            detail,
        });
    }
    Ok(errors)
}

/// Creates publications as needed in order to keep materializations in sync
/// with their `sourceCaptures`. It first queries for any materializations
/// having a `sourceCapture` that may have been updated by the current
/// publication, as provided by the `maybe_linked_captures` argument. For each
/// such materialization, it checks to ensure that the materialization bindings
/// are in sync with the capture. If not, then it updates the bindings in the
/// materialization spec and creates a new draft and publication.
///
/// Note that if you publish a materialization with a sourceCapture by itself
/// (without the capture in the draft), then this function will, by design,
/// create _another_ publication if the current bindings don't match those of
/// the capture. Materialization bindings are only ever added or enabled. They
/// are never disabled or removed when they are disabled or removed from the
/// source capture.
///
/// This will query for individual `sourceCapture`s if they are not part of the
/// `built_captures` from this publication.
pub async fn create_linked_materialization_publications(
    agent_user_email: &str,
    built_captures: &tables::BuiltCaptures,
    maybe_linked_captures: Vec<String>,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<Vec<Id>> {
    let n_captures = maybe_linked_captures.len();
    let linked_materializations = get_linked_materializations(txn, maybe_linked_captures).await?;
    tracing::debug!(%n_captures, n_linked_materializations = %linked_materializations.len(), "fetched linked materializations for captures");

    let parsed_materializations = linked_materializations
        .into_iter()
        .map(|row| {
            let spec: MaterializationDef = serde_json::from_str(row.materialization_spec.get())
                .with_context(|| {
                    format!(
                        "parsing materialization spec of: '{}'",
                        row.materialization_name
                    )
                })?;
            Ok((row.materialization_name, row.last_pub_id, spec))
        })
        .collect::<anyhow::Result<Vec<(String, Id, MaterializationDef)>>>()?;

    // Index all the captures in the build output by name for easy lookups.
    let mut captures_by_name = built_captures
        .iter()
        .map(|row| (row.capture.as_str(), &row.spec))
        .collect::<HashMap<_, _>>();

    // Determine which capture specs we'll need but don't yet have available.
    let query_captures = parsed_materializations
        .iter()
        .map(|(_, _, spec)| spec.source_capture.as_ref().unwrap().as_str())
        .filter(|source_capture| !captures_by_name.contains_key(source_capture))
        .collect::<HashSet<_>>();
    // This is here so that the specs live long enough, since we're storing references
    let moar_captures: Vec<CaptureSpecRow>;
    if !query_captures.is_empty() {
        let input = query_captures.into_iter().collect::<Vec<_>>();
        moar_captures = agent_sql::linked_materializations::get_source_capture_specs(txn, &input)
            .await
            .with_context(|| format!("querying for sourceCaptures: {:?}", input))?;
        for capture in moar_captures.iter() {
            if let Some(spec) = capture.spec.as_ref() {
                captures_by_name.insert(spec.0.name.as_str(), &spec.0);
            }
        }
    }

    let mut resource_ptr_cache = ResourcePointerCache::default();
    let mut pub_ids = Vec::new();
    for (materialization_name, last_pub_id, mut spec) in parsed_materializations {
        // safe unwrap because we just queried for materializations having sourceCaptures.
        let capture_name = spec.source_capture.as_ref().unwrap().as_str();
        let maybe_capture_spec = captures_by_name.get(capture_name).map(|cs| *cs);
        let Some(capture_spec) = maybe_capture_spec else {
            tracing::debug!(%materialization_name, %capture_name, "ignoring linked materialization because sourceCapture is being deleted");
            continue;
        };

        let was_updated = update_linked_materialization(
            &mut resource_ptr_cache,
            txn,
            capture_spec,
            &materialization_name,
            &mut spec,
        )
        .await?;
        let maybe_pub_id = if was_updated {
            let pub_id = create_publication(
                agent_user_email,
                materialization_name.clone(),
                spec,
                last_pub_id,
                txn,
            )
            .await?;
            pub_ids.push(pub_id);
            Some(pub_id)
        } else {
            None
        };
        tracing::debug!(%was_updated, materialization_name = %materialization_name, create_publication = ?maybe_pub_id, "checked linked materialization due to capture publication");
    }
    Ok(pub_ids)
}

/// Updates the bindings of a materialization spec to reflect those of the given capture.
/// Bindings are matched based on the `target` of the capture binding and the `source` of the
/// materialization binding. For each binding in the capture, a corresponding binding will be
/// created in the materialization, if it does not already exist. The return value indicates
/// whether the materialization spec was actually modified by this process, to allow avoiding
/// unnecessary publications.
async fn update_linked_materialization(
    resource_pointer_cache: &mut ResourcePointerCache,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    capture_spec: &proto_flow::flow::CaptureSpec,
    materialization_name: &str,
    materialization: &mut MaterializationDef,
) -> anyhow::Result<bool> {
    // The set of collection names of the capture bindings.
    // Note that the built spec never contains disabled bindings, so
    // inclusion in this set indicates that the capture binding is enabled.
    let mut bindings_to_add = capture_spec
        .bindings
        .iter()
        .map(|b| b.collection.as_ref().unwrap().name.as_str())
        .collect::<std::collections::BTreeSet<_>>();

    // Remove any that are already present in the materialization, regardless of
    // whether they are disabled in the materialization.
    for mat_binding in materialization.bindings.iter() {
        bindings_to_add.remove(mat_binding.source.collection().as_str());
    }
    let changed = !bindings_to_add.is_empty();

    for collection_name in bindings_to_add {
        let models::MaterializationEndpoint::Connector(conn) = &materialization.endpoint else {
            panic!("unexpected materialization endpoint type for '{materialization_name}'");
        };
        let mut resource_spec = serde_json::json!({});
        let collection_name_ptr = resource_pointer_cache.get_pointer(txn, &conn.image).await?;
        crate::resource_configs::update_materialization_resource_spec(
            materialization_name,
            &mut resource_spec,
            &collection_name_ptr,
            Some(collection_name.to_string()),
        )?;

        let binding = MaterializationBinding {
            resource: models::RawValue::from_value(&resource_spec),
            source: Source::Collection(Collection::new(collection_name)),
            disable: false,
            fields: Default::default(),
            priority: Default::default(),
            backfill: 0,
        };
        materialization.bindings.push(binding);
    }

    Ok(changed)
}

async fn create_publication(
    agent_user_email: &str,
    materialization_name: String,
    materialization_spec: MaterializationDef,
    last_pub_id: Id,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<Id> {
    let detail =
        format!("system created publication in response to publication of the `sourceCapture`");
    let draft_id = drafts::create(agent_user_email, detail.clone(), txn).await?;

    drafts::upsert_spec(
        draft_id,
        &materialization_name,
        materialization_spec,
        agent_sql::CatalogType::Materialization,
        Some(last_pub_id),
        txn,
    )
    .await?;

    let pub_id = agent_sql::publications::create_with_user_email(
        txn,
        agent_user_email,
        draft_id,
        false,
        detail,
    )
    .await?;
    Ok(pub_id)
}

/// A cache of JSON pointers corresponding to the `x-collection-name` schema annotation
/// in resource spec schemas. This is populated lazily.
#[derive(Debug, Default)]
struct ResourcePointerCache {
    pointers_by_image: HashMap<String, doc::Pointer>,
}

impl ResourcePointerCache {
    async fn get_pointer(
        &mut self,
        txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        image: &str,
    ) -> anyhow::Result<doc::Pointer> {
        if let Some(ptr) = self.pointers_by_image.get(image) {
            Ok(ptr.clone())
        } else {
            let schema = crate::resource_configs::fetch_resource_spec_schema(image, txn).await?;
            let ptr = crate::resource_configs::pointer_for_schema(schema.get())?;
            self.pointers_by_image
                .insert(image.to_string(), ptr.clone());
            Ok(ptr)
        }
    }
}

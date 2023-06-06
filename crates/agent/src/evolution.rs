use super::{draft, Handler, HandlerStatus, Id};
use agent_sql::{
    evolutions::{DraftSpecRow, Row},
    Capability,
};
use anyhow::Context;
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};

#[cfg(test)]
mod test;

pub struct EvolutionHandler;

/// Rust struct corresponding to each array element of the `collections` JSON
/// input of an `evolutions` row.
#[derive(Serialize, Deserialize, Clone, PartialEq)]
struct EvolveRequest {
    old_name: String,
    new_name: Option<String>,
}

#[derive(Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum JobStatus {
    EvolutionFailed {
        error: String,
    },
    Success {
        evolved_collections: Vec<EvolvedCollection>,
    },
}

#[derive(Serialize, Deserialize, PartialEq)]
pub struct EvolvedCollection {
    /// Original name of the collection
    pub old_name: String,
    /// The new name of the collection, which may be the same as the original name if only materialization bindings were updated
    pub new_name: String,
    /// The names of any materializations that were updated as a result of evolving this collection
    pub updated_materializations: Vec<String>,
    /// The names of any captures that were updated as a result of evolving this collection
    pub updated_captures: Vec<String>,
}

fn error_status(err: impl Into<String>) -> anyhow::Result<JobStatus> {
    Ok(JobStatus::EvolutionFailed { error: err.into() })
}

#[async_trait::async_trait]
impl Handler for EvolutionHandler {
    async fn handle(&mut self, pg_pool: &sqlx::PgPool) -> anyhow::Result<HandlerStatus> {
        let mut txn = pg_pool.begin().await?;

        let Some(row) = agent_sql::evolutions::dequeue(&mut txn).await? else {
            return Ok(HandlerStatus::Idle);
        };

        let id: Id = row.id;
        let status = process_row(row, &mut txn).await?;
        let status = serde_json::to_value(status)?;

        tracing::info!(%id, %status, "evolution finished");
        agent_sql::evolutions::resolve(id, &status, &mut txn).await?;
        txn.commit().await?;

        Ok(HandlerStatus::Active)
    }

    fn table_name(&self) -> &'static str {
        "evolutions"
    }
}

#[tracing::instrument(err, skip_all, fields(id=?row.id, draft_id=?row.draft_id))]
async fn process_row(
    row: Row,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<JobStatus> {
    let Row {
        draft_id,
        user_id,
        collections,
        ..
    } = row;
    let collections_requests: Vec<EvolveRequest> =
        serde_json::from_str(collections.get()).context("invalid 'collections' input")?;

    if collections_requests.is_empty() {
        return error_status("evolution collections parameter is empty");
    }

    // collect the requests into a map of old_name to new_name
    let collections: BTreeMap<String, Option<String>> = collections_requests
        .into_iter()
        .map(|EvolveRequest { old_name, new_name }| (old_name, new_name))
        .collect();

    // Fetch all the specs from the draft that we're operating on
    let draft_specs = agent_sql::evolutions::fetch_draft_specs(draft_id, user_id, txn)
        .await
        .context("fetching draft specs")?;
    if draft_specs.is_empty() {
        return error_status("draft is either empty or owned by another user");
    }
    if let Err(error) = validate_evolving_collections(&draft_specs, &collections) {
        return error_status(error);
    }

    // Get the live_spec.id of each collection that's requested to be re-created.
    let seed_ids = draft_specs
        .iter()
        .filter_map(|row| {
            if collections.contains_key(&row.catalog_name) {
                row.live_spec_id // may still be None, and that's OK
            } else {
                None
            }
        })
        .collect::<Vec<Id>>();

    // Fetch all of the live_specs that directly read from or write to any of these collections.
    let expanded_rows = agent_sql::publications::resolve_expanded_rows(user_id, seed_ids, txn)
        .await
        .context("expanding specifications")?;

    // Build up a `models::Catalog` that includes all of the non-deleted specs in the current
    // draft, as well as all of the connected `live_specs` that the user has admin capability for.
    let mut before_catalog = models::Catalog::default();
    let errors = draft::extend_catalog(
        &mut before_catalog,
        draft_specs.iter().filter_map(|r| {
            r.draft_type.map(|t| {
                (
                    t,
                    r.catalog_name.as_str(),
                    r.draft_spec.as_ref().unwrap().0.as_ref(),
                )
            })
        }),
    );
    if !errors.is_empty() {
        anyhow::bail!("unexpected errors from live specs: {errors:?}");
    }
    let errors = draft::extend_catalog(
        &mut before_catalog,
        expanded_rows.iter().filter_map(|row| {
            if row.user_capability == Some(Capability::Admin) {
                Some((
                    row.live_type,
                    row.catalog_name.as_str(),
                    row.live_spec.0.as_ref(),
                ))
            } else {
                tracing::info!(catalog_name=%row.catalog_name, user_capability=?row.user_capability, "filtering out expanded live_spec because the user does not have admin capability for it");
                None
            }
        }),
    );
    if !errors.is_empty() {
        anyhow::bail!("unexpected errors from extended live specs: {errors:?}");
    }

    // Create our helper for updating resource specs of affected materialization
    // bindings. This needs to fetch all of the resource_spec_schemas for each
    // of the  materialization connectors involved
    let update_helper = match ResourceSpecUpdater::for_catalog(txn, &before_catalog).await {
        Ok(help) => help,
        Err(err) => return error_status(format!("processing resource spec schemas: {err}")),
    };

    let EvolvedCollections {
        draft_catalog,
        changed_collections,
    } = evolve_collections(&before_catalog, &collections, &update_helper)?;
    draft::upsert_specs(draft_id, draft_catalog, txn)
        .await
        .context("inserting draft specs")?;

    // Remove any of the old collection versions from the draft if we've created
    // new versions of them. The old draft specs are likely to be rejected
    // during publication due to having incompatible changes, so removing
    // them is likely necessary in order to allow publication to proceed after
    // evolution. We only remove specs that have been re-added with new names,
    // though. If the collection is _not_ being re-created, then the draft spec
    // is left in place since it may contain desired schema updates.
    for ds in draft_specs.iter().filter(|ds| {
        changed_collections
            .iter()
            .any(|cc| cc.old_name == ds.catalog_name && cc.old_name != cc.new_name)
    }) {
        agent_sql::drafts::delete_spec(ds.draft_spec_id, txn).await?;
    }

    // TODO: Update the `expect_pub_id` of any specs that we've added to the draft.
    // This is important to do, but is something that I think we can safely defer
    // until a future commit.
    Ok(JobStatus::Success {
        evolved_collections: changed_collections,
    })
}

fn validate_evolving_collections(
    spec_rows: &Vec<DraftSpecRow>,
    evolving_collections: &BTreeMap<String, Option<String>>,
) -> Result<(), String> {
    let collection_name_regex = models::Collection::regex();
    let mut seen = BTreeSet::new();
    for row in spec_rows
        .iter()
        .filter(|r| evolving_collections.contains_key(r.catalog_name.as_str()))
    {
        let old_name = row.catalog_name.as_str();
        seen.insert(old_name.to_owned());

        // Validate that the new collection name is a valid catalog name.
        // This results in a better error message, since an invalid name could
        // otherwise result in a database error due to a constraint violation.
        if let Some(new_name) = evolving_collections
            .get(old_name)
            .expect("already checked contains_key")
        {
            if !collection_name_regex.is_match(new_name.as_str()) {
                return Err(format!("requested collection name '{new_name}' is invalid"));
            }
        }

        // We have to validate that the collection has not been deleted in the draft because
        // deletions cannot currently be represented in a `models::Catalog`. But other validations
        // of the type will be handled later, when we try to re-name the collections.
        if row.draft_type.is_none() {
            return Err(format!(
                "cannot re-create collection '{old_name}' which was already deleted in the draft"
            ));
        }
        // This validation isn't technically necessary. Nothing will break if we re-create a collection
        // that only exists in the draft. But it seems likely to be unintentional, so probably good to
        // error out here.
        if row.live_spec_id.is_none() {
            return Err(format!(
                "cannot re-create collection '{old_name}' because it has never been published"
            ));
        }
    }

    if seen.len() != evolving_collections.len() {
        let mut missing = evolving_collections
            .keys()
            .filter(|n| !seen.contains(n.as_str()));
        return Err(format!(
            "the collections: {} are not present in the draft",
            missing.join(", ")
        ));
    }
    Ok(())
}

pub struct EvolvedCollections {
    /// A new draft catalog that reflects all the changes required to evolve the collections.
    pub draft_catalog: models::Catalog,
    /// A structured summary of all the changes that were applied.
    pub changed_collections: Vec<EvolvedCollection>,
}

fn evolve_collections(
    catalog: &models::Catalog,
    collections: &BTreeMap<String, Option<String>>,
    update_helper: &ResourceSpecUpdater,
) -> anyhow::Result<EvolvedCollections> {
    let mut new_catalog = models::Catalog::default();
    let mut changed_collections = Vec::new();
    for (old_name, new_name) in collections.iter() {
        let result = evolve_collection(
            &mut new_catalog,
            catalog,
            old_name.as_str(),
            new_name.as_deref(),
            update_helper,
        )
        .with_context(|| format!("processing collection '{old_name}'"))?;
        changed_collections.push(result);
    }

    Ok(EvolvedCollections {
        draft_catalog: new_catalog,
        changed_collections,
    })
}

/// This returns true if the given collection uses an inferred schema. For now,
/// this must be determined by parsing the schema and checking for the presence
/// of an `x-infer-schema` annotation, which is fallible. But the future plan is
/// to hoist that annotation into a top-level collection property, which would
/// eliminate the need for this function.
fn uses_schema_inference(collection: &models::CollectionDef) -> anyhow::Result<bool> {
    let effective_read_schema = collection
        .schema
        .as_ref()
        .or(collection.read_schema.as_ref())
        .ok_or_else(|| anyhow::anyhow!("invalid collection spec missing schema"))?;

    let schema = doc::validation::build_bundle(effective_read_schema.get())?;
    let mut builder = doc::SchemaIndexBuilder::new();
    builder.add(&schema)?;
    let index = builder.into_index();
    let shape = doc::inference::Shape::infer(&schema, &index);

    let uses_inference = shape.annotations.contains_key("x-infer-schema");
    Ok(uses_inference)
}

fn evolve_collection(
    new_catalog: &mut models::Catalog,
    prev_catalog: &models::Catalog,
    old_collection_name: &str,
    new_collection_name: Option<&str>,
    update_helper: &ResourceSpecUpdater,
) -> anyhow::Result<EvolvedCollection> {
    let old_collection = models::Collection::new(old_collection_name);
    let Some(draft_collection_spec) = prev_catalog.collections.get(&old_collection) else {
        anyhow::bail!("catalog does not contain a collection named '{old_collection_name}'");
    };

    let uses_inference = uses_schema_inference(draft_collection_spec)?;
    let re_create_collection = new_collection_name.is_some() || !uses_inference;

    // If the collection uses schema inference, then only re-create it if explicitly requested to do so.
    // Otherwise, if it uses schema inference, then just update the materialization binding to use a new resource name.
    let new_name = if re_create_collection {
        new_collection_name
            .map(|s| s.to_owned())
            .unwrap_or_else(|| next_name(old_collection_name))
    } else {
        old_collection_name.to_owned()
    };
    let new_name = models::Collection::new(new_name);

    if re_create_collection {
        new_catalog
            .collections
            .insert(new_name.clone(), draft_collection_spec.clone());
    }

    let mut updated_materializations = Vec::new();

    for (mat_name, mat_spec) in prev_catalog
        .materializations
        .iter()
        .filter(|m| has_mat_binding(m.1, &old_collection))
    {
        updated_materializations.push(mat_name.as_str().to_owned());
        let new_spec = new_catalog
            .materializations
            .entry(mat_name.clone())
            .or_insert_with(|| mat_spec.clone());

        for binding in new_spec
            .bindings
            .iter_mut()
            .filter(|b| b.source.collection() == &old_collection)
        {
            // If we're re-creating the collection, then update the source in place.
            if re_create_collection {
                binding
                    .source
                    .set_collection(models::Collection::new(new_name.clone()));
            }

            // Next we need to update the resource spec of the binding. This updates, for instance,
            // a sql materialization to point to a new table name.
            let models::MaterializationEndpoint::Connector(conn) = &mat_spec.endpoint else {
                continue;
            };
            // Parse the current resource spec into a `Value` that we can mutate
            let mut resource_spec: Value = serde_json::from_str(binding.resource.get())
                .with_context(|| {
                    format!(
                        "parsing materialization resource spec of '{}' binding for '{}",
                        mat_name, &new_name
                    )
                })?;
            update_helper
                .update_resource_spec(
                    &conn.image,
                    mat_name.as_str(),
                    new_collection_name.map(|s| s.to_owned()),
                    &mut resource_spec,
                )
                .with_context(|| {
                    format!("updating resource spec of '{mat_name}' binding '{old_collection}'")
                })?;
            binding.resource = models::RawValue::from_value(&resource_spec);
        }
    }

    let mut updated_captures = Vec::new();
    // We don't need to update any captures if the collection isn't being re-created.
    if re_create_collection {
        for (cap_name, cap_spec) in prev_catalog
            .captures
            .iter()
            .filter(|c| has_cap_binding(c.1, &old_collection))
        {
            updated_captures.push(cap_name.as_str().to_owned());
            let new_spec = new_catalog
                .captures
                .entry(cap_name.clone())
                .or_insert_with(|| cap_spec.clone());

            for binding in new_spec.bindings.iter_mut() {
                if binding.target.as_ref() == Some(&old_collection) {
                    binding.target = Some(new_name.clone()).into();
                }
            }
        }
    }

    tracing::debug!(?updated_materializations, ?updated_captures, %new_name, old_name=%old_collection_name, "renaming collection in draft");

    Ok(EvolvedCollection {
        old_name: old_collection.into(),
        new_name: new_name.into(),
        updated_materializations,
        updated_captures,
    })
}

fn has_cap_binding(spec: &models::CaptureDef, collection: &models::Collection) -> bool {
    spec.bindings
        .iter()
        .any(|b| b.target.as_ref() == Some(collection))
}

fn has_mat_binding(spec: &models::MaterializationDef, collection: &models::Collection) -> bool {
    spec.bindings
        .iter()
        .any(|b| b.source.collection() == collection)
}

lazy_static! {
    static ref NAME_VERSION_RE: Regex = Regex::new(r#".*[_-][vV](\d+)$"#).unwrap();
}

fn next_name(current_name: &str) -> String {
    // Does the name already have a version suffix?
    // We try to work with whatever suffix is already present. This way, if a user
    // is starting with a collection like `acmeCo/foo-V3`, they'll end up with
    // `acmeCo/foo-V4` instead of `acmeCo/foo_v4`.
    if let Some(capture) = NAME_VERSION_RE.captures_iter(current_name).next() {
        if let Ok(current_version_num) = capture[1].parse::<u32>() {
            // wrapping_add is just to ensure we don't panic if someone passes
            // a naughty name with a u32::MAX version.
            return format!(
                "{}{}",
                current_name.strip_suffix(&capture[1]).unwrap(),
                // We don't really care what the collection name ends up as if
                // the old name is suffixed by "V-${u32::MAX}", as long as we don't panic.
                current_version_num.wrapping_add(1)
            );
        }
    }
    // We always use an underscore as the separator. This might look a bit
    // unseemly if dashes are already used as separators elsewhere in the
    // name, but any sort of heuristic for determining whether to use dashes
    // or underscores is rife with edge cases and doesn't seem worth the
    // complexity.
    format!("{current_name}_v2")
}

struct ResourceSpecUpdater {
    pointers_by_image: HashMap<String, doc::Pointer>,
}

impl ResourceSpecUpdater {
    async fn for_catalog(
        txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        catalog: &models::Catalog,
    ) -> anyhow::Result<ResourceSpecUpdater> {
        let mut pointers_by_image = HashMap::new();
        for mat_spec in catalog.materializations.values() {
            let models::MaterializationEndpoint::Connector(conn) = &mat_spec.endpoint else {
                continue;
            };
            if pointers_by_image.contains_key(&conn.image) {
                continue;
            }
            let image = conn.image.as_str();
            let Some(colon_idx) = conn.image.find(':') else {
                anyhow::bail!("connector image '{image}' is missing a version tag");
            };
            let image_name = &image[..colon_idx];
            let image_tag = &image[colon_idx..];

            let schema_json = agent_sql::evolutions::fetch_resource_spec_schema(
                image_name.to_owned(),
                image_tag.to_owned(),
                txn,
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("no resource spec schema found for image '{image}"))?;
            let pointer = Self::pointer_for_schema(schema_json.get())
                .with_context(|| format!("inspecting resource_spec_schema for image '{image}'"))?;

            tracing::debug!(%image_name, %image_tag, %pointer, "parsed resource spec schema");
            pointers_by_image.insert(image.to_owned(), pointer);
        }
        Ok(ResourceSpecUpdater { pointers_by_image })
    }

    /// Updates the given resource spec in place based on the x-collection-name annotation.
    /// The new name
    fn update_resource_spec(
        &self,
        image_name: &str,
        materialization_name: &str,
        new_collection_name: Option<String>,
        resource_spec: &mut Value,
    ) -> anyhow::Result<()> {
        if let Some(pointer) = self.pointers_by_image.get(image_name) {
            let Some(existing) = pointer
                .query(&*resource_spec)
                .and_then(|v| v.as_str()) else {
                // Log noisily about this, but it's not clear that it shoudl be an error. It's
                // possible, at least in principle, that a connector allows the field to be empty
                // and will apply it's own default value based on the collection name.
                tracing::warn!(%materialization_name, ?new_collection_name, %pointer, "not updating resource spec because there is no existing value at that location");
                return Ok(())
            };

            let new_val = new_collection_name
                .map(|n| {
                    n.rsplit('/')
                        .next()
                        .expect("collection name must contain a slash")
                        .to_owned()
                })
                .unwrap_or_else(|| next_name(existing).to_owned());

            if let Some(prev_val) = pointer.create_value(resource_spec) {
                tracing::info!(%prev_val, %new_val, %materialization_name, "updating resource spec");
                *prev_val = Value::String(new_val);
            } else {
                anyhow::bail!("creating x-collection-name JSON location failed");
            }
        } else {
            anyhow::bail!(
                "no resource spec x-collection-name location exists for image '{image_name}'"
            );
        }
        Ok(())
    }

    /// Runs inference on the given schema and searches for a location within the resource spec
    /// that bears the `x-collection-name` annotation. Returns the pointer to that location, or an
    /// error if no such location exists. Errors from parsing the schema are returned directly.
    fn pointer_for_schema(schema_json: &str) -> anyhow::Result<doc::Pointer> {
        // While all known connector resource spec schemas are self-contained, we don't
        // actually do anything to guarantee that they are. This function may fail in that case.
        let schema = doc::validation::build_bundle(schema_json)?;
        let mut builder = doc::SchemaIndexBuilder::new();
        builder.add(&schema)?;
        let index = builder.into_index();
        let shape = doc::inference::Shape::infer(&schema, &index);

        for (ptr, _, prop_shape, _) in shape.locations() {
            if prop_shape.annotations.contains_key("x-collection-name") {
                return Ok(doc::Pointer::from_str(&ptr));
            }
        }
        Err(anyhow::anyhow!(
            "resource spec schema does not contain any location annotated with x-collection-name"
        ))
    }
}

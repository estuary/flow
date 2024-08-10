use super::{draft, HandleResult, Handler, Id};
use agent_sql::{
    evolutions::{Row, SpecRow},
    Capability,
};
use anyhow::Context;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
mod test;

pub struct EvolutionHandler;

/// Rust struct corresponding to each array element of the `collections` JSON
/// input of an `evolutions` row.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct EvolveRequest {
    /// The current name of the collection.
    #[serde(alias = "old_name")]
    // alias can be removed after UI code is updated to use current_name
    pub current_name: String,
    /// Optional new name for the collection. If provided, the collection will be re-created.
    /// Otherwise, only materialization bindings will be updated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_name: Option<String>,
    /// Optionally restrict updates to only the provided materializations. This conflicts with
    /// `new_name`, and at most one of the two may be provided.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub materializations: Vec<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum JobStatus {
    EvolutionFailed {
        error: String,
    },
    Success {
        evolved_collections: Vec<EvolvedCollection>,
        publication_id: Option<Id>,
    },
    Queued,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
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
    async fn handle(
        &mut self,
        pg_pool: &sqlx::PgPool,
        allow_background: bool,
    ) -> anyhow::Result<HandleResult> {
        loop {
            let mut txn = pg_pool.begin().await?;

            let Some(row) = agent_sql::evolutions::dequeue(&mut txn, allow_background).await?
            else {
                return Ok(HandleResult::NoJobs);
            };

            let time_queued = chrono::Utc::now().signed_duration_since(row.updated_at);
            let id: Id = row.id;
            let process_result = process_row(row, &mut txn).await;
            let job_status = match process_result {
                Ok(s) => s,
                Err(err) if crate::is_acquire_lock_error(&err) => {
                    tracing::info!(%id, %time_queued, "cannot acquire all row locks for evolution (will retry)");
                    // Since we failed to acquire a necessary row lock, wait a short
                    // while and then try again.
                    txn.rollback().await?;
                    // The sleep is really just so we don't spam the DB in a busy
                    // loop.  I arrived at these values via the very scientific 😉
                    // process of reproducing failures using a couple of different
                    // values and squinting at the logs in my terminal. In
                    // practice, it's common for another agent process to pick up
                    // the job while this one is sleeping, which is why I didn't
                    // see a need for jitter. All agents process the job queue in
                    // the same order, so the next time any agent polls the
                    // handler, it should get this same job, since we've released
                    // the lock on the job row. Evolutions jobs will fail _quite_
                    // quickly in this scenario, hence the full second.
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    continue;
                }
                Err(other_err) => return Err(other_err),
            };
            let status = serde_json::to_value(job_status)?;
            tracing::info!(%id, %time_queued, %status, "evolution finished");
            agent_sql::evolutions::resolve(id, &status, &mut txn).await?;
            txn.commit().await?;

            return Ok(HandleResult::HadJob);
        }
    }

    fn table_name(&self) -> &'static str {
        "evolutions"
    }
}

#[tracing::instrument(err, skip_all, fields(?row.id, ?row.draft_id, %row.background))]
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

    let collection_names: Vec<String> = collections_requests
        .iter()
        .map(|r| r.current_name.clone())
        .collect();
    // We're expecting to have a spec_row for each collection that's evolving,
    // regardless of whether it's in the draft. We should also have a row for
    // every spec that's in the draft, so that we can mutate the drafted version
    // instead of the live version if we need to update the bindings.
    let spec_rows: Vec<SpecRow> =
        agent_sql::evolutions::resolve_specs(user_id, draft_id, collection_names, txn).await?;

    if let Err(err) = validate_evolving_collections(&spec_rows, &collections_requests) {
        return error_status(err);
    }

    // Get the live_spec.id of each collection that's requested to be evolved.
    let seed_ids = collections_requests
        .iter()
        .filter_map(|req| {
            spec_rows
                .iter()
                .find(|r| r.catalog_name == req.current_name)
                .and_then(|r| r.live_spec_id)
        })
        .collect::<Vec<Id>>();

    // Fetch all of the live_specs that directly read from or write to any of these collections.
    let expanded_rows = agent_sql::publications::resolve_expanded_rows(user_id, seed_ids, txn)
        .await
        .context("expanding specifications")?;

    // Build up catalog of all the possibly affected entities, for easy lookups.
    // Note that we put `expanded_rows` first so that `spec_rows` will overwrite
    // them, so that the resulting catalog will include the drafted specs, if
    // present, and otherwise the `live_specs` version.
    let mut before_catalog = models::Catalog::default();
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

    let errors = draft::extend_catalog(
        &mut before_catalog,
        spec_rows.iter().filter_map(|r| {
            r.spec_type.map(|t| {
                (
                    t,
                    r.catalog_name.as_str(),
                    r.spec.as_ref().unwrap().0.as_ref(),
                )
            })
        }),
    );
    if !errors.is_empty() {
        anyhow::bail!("unexpected errors from drafted specs: {errors:?}");
    }

    let mut new_catalog = models::Catalog::default();
    let mut changed_collections = Vec::new();
    for req in collections_requests.iter() {
        let specific_materializations = req
            .materializations
            .iter()
            .map(|m| m.as_str())
            .collect::<BTreeSet<_>>();
        let result = evolve_collection(
            &mut new_catalog,
            &before_catalog,
            req.current_name.as_str(),
            req.new_name.as_deref(),
            &specific_materializations,
        );
        match result {
            Ok(summary) => {
                changed_collections.push(summary);
            }
            Err(err) => {
                return error_status(err.to_string());
            }
        }
    }

    tracing::info!(changes=?changed_collections, "evolved catalog");

    // Determine the value of `expect_pub_id` to use for each draft spec.
    // For existing draft specs that already have an `expect_pub_id`, we'll
    // use that value exactly. Otherwise, we'll use the `last_pub_id` from the live spec.
    let mut expect_pub_ids = BTreeMap::new();
    for row in expanded_rows.iter() {
        expect_pub_ids.insert(row.catalog_name.as_str(), row.last_pub_id);
    }
    for row in spec_rows.iter() {
        // It's possible for spec rows to have neither of these values, since
        // they may include drafted specs that are new and unaffected by this
        // evolution
        if let Some(id) = row.expect_pub_id.or(row.last_pub_id) {
            expect_pub_ids.insert(row.catalog_name.as_str(), id);
        }
    }

    draft::upsert_specs(draft_id, new_catalog, &expect_pub_ids, txn)
        .await
        .context("inserting draft specs")?;

    // Remove any of the old collection versions from the draft if we've created
    // new versions of them. The old draft specs are likely to be rejected
    // during publication due to having incompatible changes, so removing
    // them is likely necessary in order to allow publication to proceed after
    // evolution. We only remove specs that have been re-added with new names.
    // It's possible that there is no draft spec to delete, even if we're re-creating
    // the collection, if the collection spec wasn't in the draft to begin with.
    for prev_name in changed_collections
        .iter()
        .filter(|c| c.old_name != c.new_name)
        .map(|c| c.old_name.as_str())
    {
        let delete_id = spec_rows
            .iter()
            .find(|r| r.catalog_name == prev_name)
            .and_then(|r| r.draft_spec_id);
        if let Some(draft_spec_id) = delete_id {
            agent_sql::drafts::delete_spec(draft_spec_id, txn).await?;
        }
    }

    // Create a publication of the draft, if desired.
    let publication_id = if row.auto_publish {
        let detail = format!(
            "system created publication as a result of evolution: {}",
            row.id
        );
        // So that we don't create an infinite loop in case there's continued errors.
        let auto_evolve = false;
        let id = agent_sql::publications::create(
            txn,
            row.user_id,
            row.draft_id,
            auto_evolve,
            detail,
            row.background,
            String::new(), // New data-plane assignments are never required for evolutions.
        )
        .await?;
        Some(id)
    } else {
        None
    };
    Ok(JobStatus::Success {
        evolved_collections: changed_collections,
        publication_id,
    })
}

fn validate_evolving_collections(
    spec_rows: &Vec<SpecRow>,
    reqs: &[EvolveRequest],
) -> Result<(), String> {
    let collection_name_regex = models::Collection::regex();
    let mut seen = BTreeSet::new();
    for req in reqs {
        // Make sure there's only one request per collection
        let old_name = req.current_name.as_str();
        if !seen.insert(old_name) {
            return Err(format!("duplicate request for collection '{old_name}'"));
        }

        if req.new_name.is_some() && !req.materializations.is_empty() {
            return Err(format!("cannot specify both materializations and new_name"));
        }

        // ensure that there's a corresponding spec row for each evolving collection
        let spec_row = spec_rows
            .iter()
            .find(|r| r.catalog_name == req.current_name)
            .ok_or_else(|| {
                format!(
                    "the collection '{}' does not exist or you do not have access to it",
                    req.current_name
                )
            })?;
        // This validation isn't technically necessary. Nothing will break if we re-create a collection
        // that only exists in the draft. But it seems likely to be unintentional, so probably good to
        // error out here.
        if spec_row.live_spec_id.is_none() {
            return Err(format!(
                "cannot evolve collection '{old_name}' because it has never been published"
            ));
        }

        // Validate that the new collection name is a valid catalog name.
        // This results in a better error message, since an invalid name could
        // otherwise result in a database error due to a constraint violation.
        if let Some(new_name) = req.new_name.as_deref() {
            if !collection_name_regex.is_match(new_name) {
                return Err(format!("requested collection name '{new_name}' is invalid"));
            }
        }

        // Validate that the collection has not already been deleted, because we need the spec if
        // we're re-creating the collection (and it's non-sensical to increment backfill counters
        // for deleted collections).
        if spec_row.spec.is_none() {
            // Was the live_spec deleted already, or is the deletion still pending in the draft?
            let in_draft = spec_row
                .draft_spec_id
                .map(|_| " in the draft")
                .unwrap_or_default();
            return Err(format!(
                "cannot evolve collection '{old_name}' which was already deleted{in_draft}"
            ));
        }
    }

    Ok(())
}

#[tracing::instrument(skip(new_catalog, prev_catalog))]
fn evolve_collection(
    new_catalog: &mut models::Catalog,
    prev_catalog: &models::Catalog,
    old_collection_name: &str,
    new_collection_name: Option<&str>,
    specific_materializations: &BTreeSet<&str>,
) -> anyhow::Result<EvolvedCollection> {
    let old_collection = models::Collection::new(old_collection_name);

    // We only re-create collections if explicitly requested.
    let (re_create_collection, new_name) = match new_collection_name {
        Some(n) => (true, n.to_owned()),
        None => (false, old_collection_name.to_owned()),
    };
    let new_name = models::Collection::new(new_name);

    if re_create_collection {
        // We also validate this when we validate the request
        assert!(
            specific_materializations.is_empty(),
            "specific_materializations argument must be empty if collection is being re-created"
        );
        let Some(collection_spec) = prev_catalog.collections.get(&old_collection) else {
            panic!("prev_catalog does not contain a collection named '{old_collection_name}'");
        };

        new_catalog
            .collections
            .insert(new_name.clone(), collection_spec.clone());
    }

    let mut updated_materializations = Vec::new();

    for (mat_name, mat_spec) in prev_catalog
        .materializations
        .iter()
        .filter(|m| has_mat_binding(m.1, &old_collection))
    {
        if !specific_materializations.is_empty()
            && !specific_materializations.contains(mat_name.as_str())
        {
            tracing::debug!(materialization = %mat_name, "skipping materialization because it was not requested to be updated");
            continue;
        }
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
            // We do this even for disabled bindings, so that the spec is up to date
            // with the latest changes to the rest of the catalog.
            if re_create_collection {
                binding
                    .source
                    .set_collection(models::Collection::new(new_name.clone()));
            }

            // Don't update resources for disabled bindings.
            if binding.disable {
                tracing::debug!(materialization = %mat_name,
                    "skipping materialization because the binding is disabled");
                continue;
            }

            // Finally, we need to increment the backfill counter of the binding.
            // This is not _technically_ required for materializations when
            // re-creating the collection, since they'll backfill when the
            // collection name changes, anyway. But it may help to make it more
            // obvious and explicit, and certainly won't hurt anything.
            binding.backfill += 1;
        }
    }
    // If specific materializations were requested to be updated, ensure that
    // we were actually able to update all of the given materializations.
    if !specific_materializations.is_empty()
        && specific_materializations.len() != updated_materializations.len()
    {
        let actual = updated_materializations
            .iter()
            .map(|u| u.as_str())
            .collect::<BTreeSet<_>>();
        let diff = specific_materializations.difference(&actual).format(", ");
        anyhow::bail!("requested to update the materialization(s) [{diff}], but no such materializations were found that source from the collection '{old_collection_name}'");
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
                if &binding.target == &old_collection {
                    binding.target = new_name.clone();
                    // When re-creating collections, it's quite likely that
                    // users will also want to trigger a new backfill. Unlike
                    // materializations, capture connectors will only backfill
                    // when the counter is incremented, not when only the
                    // collection name is changed.
                    binding.backfill += 1;
                }
            }
        }
    }

    tracing::debug!(?updated_materializations, ?updated_captures, %re_create_collection, %new_name, old_name=%old_collection_name, "evolved collection in draft");

    Ok(EvolvedCollection {
        old_name: old_collection.into(),
        new_name: new_name.into(),
        updated_materializations,
        updated_captures,
    })
}

fn has_cap_binding(spec: &models::CaptureDef, collection: &models::Collection) -> bool {
    spec.bindings.iter().any(|b| &b.target == collection)
}

fn has_mat_binding(spec: &models::MaterializationDef, collection: &models::Collection) -> bool {
    spec.bindings
        .iter()
        .any(|b| b.source.collection() == collection)
}

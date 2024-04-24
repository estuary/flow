use super::{LockFailure, UncommittedBuild};
use agent_sql::publications::{LiveRevision, LiveSpecUpdate};
use agent_sql::Capability;
use anyhow::Context;
use itertools::Itertools;
use models::Id;
use serde_json::value::RawValue;
use sqlx::types::Uuid;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use tables::{BuiltRow, DraftRow, SpecExt};

pub async fn persist_updates(
    uncommitted: &mut UncommittedBuild,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<Vec<LockFailure>> {
    let UncommittedBuild {
        ref publication_id,
        ref output,
        ref mut live_spec_ids,
        ref user_id,
        ref detail,
        ..
    } = uncommitted;

    let live_spec_updates = update_live_specs(*publication_id, &output, txn).await?;
    let lock_failures = live_spec_updates
        .iter()
        .filter_map(|r| {
            if r.last_pub_id != r.expect_pub_id {
                Some(LockFailure {
                    catalog_name: r.catalog_name.clone(),
                    last_pub_id: Some(r.last_pub_id.into()).filter(|id: &models::Id| !id.is_zero()),
                    expect_pub_id: r.expect_pub_id.into(),
                })
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    if !lock_failures.is_empty() {
        return Ok(lock_failures);
    }
    let lock_failures = verify_unchanged_revisions(output, txn).await?;
    if !lock_failures.is_empty() {
        return Ok(lock_failures);
    }

    // Update `live_spec_ids` to include the ids of any newly created live specs
    for update in live_spec_updates {
        let LiveSpecUpdate {
            catalog_name,
            live_spec_id,
            ..
        } = update;
        let prev_value = live_spec_ids
            .get_mut(&catalog_name)
            .ok_or_else(|| anyhow::anyhow!("missing live_spec_ids entry for {catalog_name:?} while processing LiveSpecUpdate for {live_spec_id}"))?;
        if prev_value.is_zero() {
            *prev_value = live_spec_id.into();
        } else {
            // Just a sanity check to ensure our handling of live spec ids is correct
            assert_eq!(
                *prev_value,
                live_spec_id.into(),
                "live_specs.id changed mid-publication for {catalog_name:?}"
            );
        }
    }

    update_drafted_live_spec_flows(live_spec_ids, output, txn)
        .await
        .context("updating live spec flows")?;

    // TODO: update `presist_updates` to insert all of them in one go
    insert_publication_specs(
        *publication_id,
        *user_id,
        detail.as_ref(),
        &*live_spec_ids,
        output,
        txn,
    )
    .await
    .context("inserting publication specs")?;

    Ok(Vec::new())
}

#[tracing::instrument(skip(model, live_spec_ids, txn))]
async fn update_live_spec_flows<M: SpecExt>(
    catalog_name: &str,
    catalog_type: agent_sql::CatalogType,
    model: Option<&M>,
    live_spec_ids: &BTreeMap<String, models::Id>,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<()> {
    let live_spec_id = live_spec_ids
        .get(catalog_name)
        .ok_or_else(|| anyhow::anyhow!("missing live_spec_ids entry for {catalog_name:?}"))?;
    let live_spec_id: agent_sql::Id = (*live_spec_id).into();
    agent_sql::publications::delete_stale_flow(live_spec_id, catalog_type, txn).await?;

    let Some(model) = model else {
        return Ok(());
    };

    let reads_from = model.reads_from();
    let writes_to = model.writes_to();
    let source_capture = model.materialization_source_capture();
    agent_sql::publications::insert_live_spec_flows(
        live_spec_id,
        catalog_type,
        Some(reads_from.iter().map(|c| c.as_str()).collect::<Vec<_>>()).filter(|a| !a.is_empty()),
        Some(writes_to.iter().map(|c| c.as_str()).collect::<Vec<_>>()).filter(|a| !a.is_empty()),
        source_capture.as_ref().map(|c| c.as_str()),
        txn,
    )
    .await?;
    Ok(())
}

async fn update_drafted_live_spec_flows(
    live_spec_ids: &BTreeMap<String, models::Id>,
    build: &build::Output,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<()> {
    for r in build.built_captures().iter().filter(|r| !r.is_unchanged()) {
        update_live_spec_flows(
            &r.catalog_name(),
            agent_sql::CatalogType::Capture,
            r.model(),
            live_spec_ids,
            txn,
        )
        .await
        .with_context(|| format!("updating live_spec_flows for '{}'", r.catalog_name()))?;
    }
    for r in build
        .built_collections()
        .iter()
        .filter(|r| !r.is_unchanged())
    {
        update_live_spec_flows(
            &r.catalog_name(),
            agent_sql::CatalogType::Collection,
            r.model(),
            live_spec_ids,
            txn,
        )
        .await
        .with_context(|| format!("updating live_spec_flows for '{}'", r.catalog_name()))?;
    }
    for r in build
        .built_materializations()
        .iter()
        .filter(|r| !r.is_unchanged())
    {
        update_live_spec_flows(
            &r.catalog_name(),
            agent_sql::CatalogType::Materialization,
            r.model(),
            live_spec_ids,
            txn,
        )
        .await
        .with_context(|| format!("updating live_spec_flows for '{}'", r.catalog_name()))?;
    }
    for r in build.built_tests().iter().filter(|r| !r.is_unchanged()) {
        update_live_spec_flows(
            &r.catalog_name(),
            agent_sql::CatalogType::Test,
            r.model(),
            live_spec_ids,
            txn,
        )
        .await
        .with_context(|| format!("updating live_spec_flows for '{}'", r.catalog_name()))?;
    }
    Ok(())
}

async fn update_live_specs(
    pub_id: Id,
    output: &build::Output,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<Vec<agent_sql::publications::LiveSpecUpdate>> {
    let n_specs = output.built.count();
    let mut catalog_names = Vec::with_capacity(n_specs);
    let mut spec_types: Vec<agent_sql::CatalogType> = Vec::with_capacity(n_specs);
    let mut models = Vec::with_capacity(n_specs);
    let mut built_specs = Vec::with_capacity(n_specs);
    let mut expect_pub_ids: Vec<agent_sql::Id> = Vec::with_capacity(n_specs);
    let mut reads_froms = Vec::with_capacity(n_specs);
    let mut writes_tos = Vec::with_capacity(n_specs);
    let mut images = Vec::with_capacity(n_specs);
    let mut image_tags = Vec::with_capacity(n_specs);

    for r in output.built_captures().iter().filter(|r| !r.is_unchanged()) {
        catalog_names.push(r.catalog_name().to_string());
        spec_types.push(agent_sql::CatalogType::Capture);
        models.push(to_raw_value(r.model(), agent_sql::TextJson));
        built_specs.push(to_raw_value(r.spec(), agent_sql::TextJson));
        expect_pub_ids.push(r.expect_pub_id().into());
        reads_froms.push(get_dependencies(r.model(), SpecExt::reads_from));
        writes_tos.push(get_dependencies(r.model(), SpecExt::writes_to));
        let (image_name, image_tag) = image_and_tag(r.model());
        images.push(image_name);
        image_tags.push(image_tag);
    }
    for r in output
        .built_collections()
        .iter()
        .filter(|r| !r.is_unchanged())
    {
        catalog_names.push(r.catalog_name().to_string());
        spec_types.push(agent_sql::CatalogType::Collection);
        models.push(to_raw_value(r.model(), agent_sql::TextJson));
        built_specs.push(to_raw_value(r.spec(), agent_sql::TextJson));
        expect_pub_ids.push(r.expect_pub_id().into());
        reads_froms.push(get_dependencies(r.model(), SpecExt::reads_from));
        writes_tos.push(get_dependencies(r.model(), SpecExt::writes_to));
        let (image_name, image_tag) = image_and_tag(r.model());
        images.push(image_name);
        image_tags.push(image_tag);
    }
    for r in output
        .built_materializations()
        .iter()
        .filter(|r| !r.is_unchanged())
    {
        catalog_names.push(r.catalog_name().to_string());
        spec_types.push(agent_sql::CatalogType::Materialization);
        models.push(to_raw_value(r.model(), agent_sql::TextJson));
        built_specs.push(to_raw_value(r.spec(), agent_sql::TextJson));
        expect_pub_ids.push(r.expect_pub_id().into());
        reads_froms.push(get_dependencies(r.model(), SpecExt::reads_from));
        writes_tos.push(get_dependencies(r.model(), SpecExt::writes_to));
        let (image_name, image_tag) = image_and_tag(r.model());
        images.push(image_name);
        image_tags.push(image_tag);
    }
    for r in output.built_tests().iter().filter(|r| !r.is_unchanged()) {
        catalog_names.push(r.catalog_name().to_string());
        spec_types.push(agent_sql::CatalogType::Test);
        models.push(to_raw_value(r.model(), agent_sql::TextJson));
        built_specs.push(to_raw_value(r.spec(), agent_sql::TextJson));
        expect_pub_ids.push(r.expect_pub_id().into());
        reads_froms.push(get_dependencies(r.model(), SpecExt::reads_from));
        writes_tos.push(get_dependencies(r.model(), SpecExt::writes_to));
        let (image_name, image_tag) = image_and_tag(r.model());
        images.push(image_name);
        image_tags.push(image_tag);
    }

    let controller_next_run = chrono::Utc::now();
    tracing::warn!(
        ?catalog_names,
        %controller_next_run,
        "persisting live_specs updates"
    );
    // We currently set `controller_next_run` to now for all affected specs, which can include
    // the spec corresponding to a currently running controller. Thus a controller that publishes
    // its own spec will be run again immediately. We could prevent that if we want, but I'm not
    // worrying about it for now.
    let updates = agent_sql::publications::persist_updates(
        pub_id.into(),
        &catalog_names,
        &spec_types,
        &models,
        &built_specs,
        &expect_pub_ids,
        &reads_froms,
        &writes_tos,
        &images,
        &image_tags,
        controller_next_run, // run controllers immediately
        txn,
    )
    .await?;

    Ok(updates)
}

pub async fn check_connector_images(
    draft: &tables::DraftCatalog,
    pool: &sqlx::PgPool,
) -> anyhow::Result<tables::Errors> {
    let mut by_image: BTreeMap<String, bool> = BTreeMap::new();
    let mut errors = tables::Errors::default();

    for capture in draft.captures.iter() {
        let Some(err) = check_connector_image(
            capture.capture.as_str(),
            capture.model(),
            &mut by_image,
            pool,
        )
        .await?
        else {
            continue;
        };
        errors.insert(err);
    }
    for collection in draft.collections.iter() {
        let Some(err) = check_connector_image(
            collection.collection.as_str(),
            collection.model(),
            &mut by_image,
            pool,
        )
        .await?
        else {
            continue;
        };
        errors.insert(err);
    }
    for materialization in draft.materializations.iter() {
        let Some(err) = check_connector_image(
            materialization.materialization.as_str(),
            materialization.model(),
            &mut by_image,
            pool,
        )
        .await?
        else {
            continue;
        };
        errors.insert(err);
    }
    Ok(errors)
}

async fn check_connector_image(
    catalog_name: &str,
    model: Option<&impl SpecExt>,
    cached: &mut BTreeMap<String, bool>,
    pool: &sqlx::PgPool,
) -> anyhow::Result<Option<tables::Error>> {
    let Some(model) = model else {
        return Ok(None);
    };
    let Some(image) = model.connector_image() else {
        return Ok(None);
    };
    let (image_name, _) = split_tag(image);
    if !cached.contains_key(&image_name) {
        let exists = agent_sql::connector_tags::does_connector_exist(&image_name, pool).await?;
        cached.insert(image_name.clone(), exists);
    }
    if !cached[&image_name] {
        Ok(Some(tables::Error {
            scope: tables::synthetic_scope(model.catalog_type(), catalog_name),
            error: anyhow::anyhow!("Forbidden connector image '{image_name}'"),
        }))
    } else {
        Ok(None)
    }
}

fn image_and_tag<M: SpecExt>(model: Option<&M>) -> (Option<String>, Option<String>) {
    let Some(full_image) = model.and_then(SpecExt::connector_image) else {
        return (None, None);
    };
    let (image_name, image_tag) = split_tag(full_image);
    (Some(image_name), Some(image_tag))
}

async fn insert_publication_specs(
    publication_id: models::Id,
    user_id: Uuid,
    detail: Option<&String>,
    live_spec_ids: &BTreeMap<String, models::Id>,
    built: &build::Output,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<()> {
    for r in built.built_captures().iter().filter(|r| !r.is_unchanged()) {
        let spec_id = *live_spec_ids
            .get(r.catalog_name().as_str())
            .expect("live_spec_id must be Some if spec is changed");
        let spec = to_raw_value(r.model(), agent_sql::TextJson);
        agent_sql::publications::insert_publication_spec(
            spec_id.into(),
            publication_id.into(),
            detail,
            &spec,
            &Some(agent_sql::CatalogType::Capture),
            user_id,
            txn,
        )
        .await
        .with_context(|| format!("inserting spec for '{}'", r.catalog_name()))?;
    }
    for r in built
        .built_collections()
        .iter()
        .filter(|r| !r.is_unchanged())
    {
        let spec_id = *live_spec_ids
            .get(r.catalog_name().as_str())
            .expect("live_spec_id must be Some if spec is changed");
        let spec = to_raw_value(r.model(), agent_sql::TextJson);
        agent_sql::publications::insert_publication_spec(
            spec_id.into(),
            publication_id.into(),
            detail,
            &spec,
            &Some(agent_sql::CatalogType::Collection),
            user_id,
            txn,
        )
        .await
        .with_context(|| format!("inserting spec for '{}'", r.catalog_name()))?;
    }
    for r in built
        .built_materializations()
        .iter()
        .filter(|r| !r.is_unchanged())
    {
        let spec_id = *live_spec_ids
            .get(r.catalog_name().as_str())
            .expect("live_spec_id must be Some if spec is changed");
        let spec = to_raw_value(r.model(), agent_sql::TextJson);
        agent_sql::publications::insert_publication_spec(
            spec_id.into(),
            publication_id.into(),
            detail,
            &spec,
            &Some(agent_sql::CatalogType::Materialization),
            user_id,
            txn,
        )
        .await
        .with_context(|| format!("inserting spec for '{}'", r.catalog_name()))?;
    }
    for r in built.built_tests().iter().filter(|r| !r.is_unchanged()) {
        let spec_id = *live_spec_ids
            .get(r.catalog_name().as_str())
            .expect("live_spec_id must be Some if spec is changed");
        let spec = to_raw_value(r.model(), agent_sql::TextJson);
        agent_sql::publications::insert_publication_spec(
            spec_id.into(),
            publication_id.into(),
            detail,
            &spec,
            &Some(agent_sql::CatalogType::Test),
            user_id,
            txn,
        )
        .await
        .with_context(|| format!("inserting spec for '{}'", r.catalog_name()))?;
    }
    Ok(())
}

fn get_dependencies<M, F>(model: Option<&M>, get: F) -> Option<agent_sql::TextJson<Vec<String>>>
where
    M: SpecExt,
    F: Fn(&M) -> BTreeSet<models::Collection>,
{
    model.map(|m| agent_sql::TextJson(get(m).into_iter().map(Into::into).collect()))
}

async fn verify_unchanged_revisions(
    output: &build::Output,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<Vec<LockFailure>> {
    let mut expected: BTreeMap<&str, Id> = output
        .built_captures()
        .iter()
        .filter(|r| r.is_unchanged())
        .map(|r| (r.catalog_name().as_str(), r.expect_pub_id()))
        .chain(
            output
                .built_collections()
                .iter()
                .filter(|r| r.is_unchanged())
                .map(|r| (r.catalog_name().as_str(), r.expect_pub_id())),
        )
        .chain(
            output
                .built_materializations()
                .iter()
                .filter(|r| r.is_unchanged())
                .map(|r| (r.catalog_name().as_str(), r.expect_pub_id())),
        )
        .chain(
            output
                .built_tests()
                .iter()
                .filter(|r| r.is_unchanged())
                .map(|r| (r.catalog_name().as_str(), r.expect_pub_id())),
        )
        .collect();
    let catalog_names = expected.keys().map(|k| *k).collect::<Vec<_>>();
    let live_revisions = agent_sql::publications::lock_live_specs(&catalog_names, txn).await?;

    let mut errors = Vec::new();
    for LiveRevision {
        catalog_name,
        last_pub_id,
    } in live_revisions
    {
        if let Some(expect_pub_id) = expected.remove(catalog_name.as_str()) {
            if expect_pub_id != last_pub_id.into() {
                errors.push(LockFailure {
                    catalog_name,
                    last_pub_id: Some(last_pub_id.into()),
                    expect_pub_id,
                });
            }
        }
    }
    // Remaining expected pub ids are for `live_specs` rows which have been deleted since we started the publication.
    for (catalog_name, expect_pub_id) in expected {
        if !expect_pub_id.is_zero() {}
        errors.push(LockFailure {
            catalog_name: catalog_name.to_string(),
            last_pub_id: None,
            expect_pub_id,
        });
    }
    Ok(errors)
}

fn to_raw_value<T: serde::Serialize, W, F>(maybe_spec: Option<&T>, wrap: F) -> Option<W>
where
    F: Fn(Box<RawValue>) -> W,
{
    if let Some(value) = maybe_spec {
        let json = serde_json::value::to_raw_value(value).expect("must serialize spec to json");
        Some(wrap(json))
    } else {
        None
    }
}

/// This is a temporary standin for a function that will lookup the ops collection names based on
/// the data plane that's associated with the tenants of published tasks.
fn get_ops_collection_names() -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    names.insert("ops.us-central1.v1/logs".to_string());
    names.insert("ops.us-central1.v1/stats".to_string());
    names
}

pub async fn resolve_live_specs(
    user_id: Uuid,
    draft: &tables::DraftCatalog,
    db: &sqlx::PgPool,
) -> anyhow::Result<(tables::LiveCatalog, BTreeMap<String, models::Id>)> {
    // We're expecting to get a row for catalog name that's either drafted or referenced
    // by a drafted spec, even if the live spec does not exist. In that case, the row will
    // still contain information on the user and spec capabilities.
    // Note that `all_catalog_names` returns a sorted and deduplicated list of catalog names.
    let mut all_spec_names = draft
        .all_catalog_names()
        .iter()
        .map(|n| n.to_string())
        .collect::<Vec<_>>();

    // Ops collections must be injected as part of the `LiveCatalog`, so that they can be included
    // in the build. Users do not need any permissions to these collections, as long as they
    // haven't drafted them. Note that it's not a build error for these ops collections to be
    // missing, but the resulting build will not function properly in the data plane without them.
    // We may wish to validate their presence in the future, but for now we let it slide so that we
    // don't need to bootstrap ops collections as part of unit/integration tests.
    let ops_collection_names = get_ops_collection_names();
    for ops_collection in ops_collection_names.iter() {
        // `all_spec_names` is sorted, so we can use binary search to avoid duplicating the ops
        // collection names.
        if let Err(i) = all_spec_names.binary_search(ops_collection) {
            all_spec_names.insert(i, ops_collection.clone());
        }
    }

    let rows = agent_sql::live_specs::fetch_live_specs(user_id, &all_spec_names, db)
        .await
        .context("fetching live specs")?;

    let spec_ids = rows
        .iter()
        .map(|r| (r.catalog_name.clone(), r.id.into()))
        .collect();

    // Check the user and spec authorizations.
    // Start by making an easy way to lookup whether each row was drafted or not.
    let drafted_names = draft.all_spec_names().collect::<HashSet<_>>();
    // AuthZ errors will be pushed to the live catalog
    let mut live = tables::LiveCatalog::default();
    for spec_row in rows {
        let catalog_name = spec_row.catalog_name.as_str();
        let n_errors = live.errors.len();

        if drafted_names.contains(catalog_name) {
            // Get the metadata about the draft spec that matches this catalog name.
            // This must exist in `draft`, otherwise `spec_meta` will panic.
            let (catalog_type, reads_from, writes_to) = spec_meta(draft, catalog_name);
            let scope = tables::synthetic_scope(catalog_type, catalog_name);

            // If the spec is included in the draft, then the user must have admin capability to it.
            if !matches!(spec_row.user_capability, Some(Capability::Admin)) {
                live.errors.push(tables::Error {
                    scope: scope.clone(),
                    error: anyhow::anyhow!(
                        "User is not authorized to create or change this catalog name"
                    ),
                });
                // Continue because we'll otherwise produce superfluous auth errors
                // of referenced collections.
                continue;
            }
            for source in reads_from {
                if !spec_row.spec_capabilities.iter().any(|c| {
                    source.starts_with(&c.object_role)
                        && matches!(
                            c.capability,
                            Capability::Read | Capability::Write | Capability::Admin
                        )
                }) {
                    live.errors.push(tables::Error {
                        scope: scope.clone(),
                        error: anyhow::anyhow!(
                            "Specification '{catalog_name}' is not read-authorized to '{source}'.\nAvailable grants are: {}",
                            serde_json::to_string_pretty(&spec_row.spec_capabilities.0).unwrap(),
                        ),
                    });
                }
            }
            for target in writes_to {
                if !spec_row.spec_capabilities.iter().any(|c| {
                    target.starts_with(&c.object_role)
                        && matches!(c.capability, Capability::Write | Capability::Admin)
                }) {
                    live.errors.push(tables::Error {
                        scope: scope.clone(),
                        error: anyhow::anyhow!(
                            "Specification is not write-authorized to '{target}'.\nAvailable grants are: {}",
                            serde_json::to_string_pretty(&spec_row.spec_capabilities.0).unwrap(),
                        ),
                    });
                }
            }
        // Ops collections are automatically injected, and the user does not need (or have) any
        // access capability to them as long as they are not drafted.
        } else if !ops_collection_names.contains(&spec_row.catalog_name) {
            // This is a live spec that is not included in the draft.
            // The user needs read capability to it because it was referenced by one of the specs
            // in their draft. Note that the _user_ does not need `Capability::Write` as long as
            // the _spec_ is authorized to do what it needs. The user just needs to be allowed to
            // know it exists.
            if !spec_row
                .user_capability
                .map(|c| c >= Capability::Read)
                .unwrap_or(false)
            {
                // TODO: consolidate into `tables::synthetic_scope`
                let mut scope = url::Url::parse("flow://unauthorized/").unwrap();
                scope.set_path(&spec_row.catalog_name);
                live.errors.push(tables::Error {
                    scope,
                    error: anyhow::anyhow!("User is not authorized to read this catalog name"),
                });
                continue;
            }
        }

        // Don't add the spec if the row had authorization errors, just as an extra precaution in
        // case the user isn't authorized to know about a spec.
        if live.errors.len() > n_errors {
            continue;
        }

        if let Some(model) = spec_row.spec.as_ref() {
            let catalog_type = spec_row.spec_type.unwrap().into();
            let scope = tables::synthetic_scope(catalog_type, &spec_row.catalog_name);
            live.add_spec(
                spec_row.spec_type.unwrap().into(),
                &spec_row.catalog_name,
                scope,
                spec_row.last_pub_id.into(),
                &model,
                &spec_row
                    .built_spec
                    .as_ref()
                    .expect("must have built spec if spec exists"),
            )
            .with_context(|| format!("adding live spec for {:?}", spec_row.catalog_name))?;
        }
    }

    // Note that we don't need storage mappings for live specs, only the drafted ones.
    let all_names = drafted_names.iter().map(|s| *s).collect_vec();
    let storage_rows = agent_sql::publications::resolve_storage_mappings(all_names, db).await?;
    for row in storage_rows {
        // TODO: consolidate with `tables::synthetic_scope`
        let mut scope = url::Url::parse("flow://storage-mappings/").unwrap();
        scope.set_path(&row.catalog_prefix);

        let store: models::StorageDef = match serde_json::from_value(row.spec) {
            Ok(s) => s,
            Err(err) => {
                live.errors.push(tables::Error {
                    scope: scope.clone(),
                    error: anyhow::Error::from(err).context("deserializing storage mapping spec"),
                });
                continue;
            }
        };
        live.storage_mappings.insert(tables::StorageMapping {
            catalog_prefix: models::Prefix::new(row.catalog_prefix),
            scope,
            stores: store.stores,
        });
    }

    // TODO(phil): remove once we no longer need to inline inferred schemas as part of validation
    resolve_inferred_schemas(draft, &mut live, db).await?;

    Ok((live, spec_ids))
}

/// Resolves inferred schemas and adds them to the live catalog.
/// This will no longer be neccessary once we stop inlining inferred schemas as part of validation.
/// We're continuing that behavior just during a transition period, but should be able to remove it
/// as soon as controllers have run for all the collections in production.
async fn resolve_inferred_schemas(
    draft: &tables::DraftCatalog,
    live: &mut tables::LiveCatalog,
    db: &sqlx::PgPool,
) -> anyhow::Result<()> {
    let collection_names = draft
        .collections
        .iter()
        .map(|r| r.collection.as_str())
        .collect::<Vec<_>>();
    let rows = agent_sql::live_specs::fetch_inferred_schemas(&collection_names, db).await?;
    for row in rows {
        let agent_sql::live_specs::InferredSchemaRow {
            collection_name,
            schema,
            md5,
        } = row;
        live.inferred_schemas.insert(tables::InferredSchema {
            collection_name: models::Collection::new(collection_name),
            schema: models::Schema::new(models::RawValue::from(schema.0)),
            md5,
        });
    }
    Ok(())
}

fn spec_meta(
    draft: &tables::DraftCatalog,
    catalog_name: &str,
) -> (
    models::CatalogType,
    BTreeSet<models::Collection>,
    BTreeSet<models::Collection>,
) {
    let capture = models::Capture::new(catalog_name);
    if let Some(s) = draft.captures.get_by_key(&capture) {
        return (
            models::CatalogType::Capture,
            Default::default(),
            s.model.as_ref().map(|m| m.writes_to()).unwrap_or_default(),
        );
    }
    let collection = models::Collection::new(capture);
    if let Some(s) = draft.collections.get_key(&collection) {
        return (
            models::CatalogType::Collection,
            s.model.as_ref().map(|m| m.reads_from()).unwrap_or_default(),
            s.model.as_ref().map(|m| m.writes_to()).unwrap_or_default(),
        );
    }
    let materialization = models::Materialization::new(collection);
    if let Some(s) = draft.materializations.get_key(&materialization) {
        return (
            models::CatalogType::Materialization,
            s.model.as_ref().map(|m| m.reads_from()).unwrap_or_default(),
            Default::default(),
        );
    }
    let test = models::Test::new(materialization);
    if let Some(s) = draft.tests.get_key(&test) {
        return (
            models::CatalogType::Test,
            s.model.as_ref().map(|m| m.reads_from()).unwrap_or_default(),
            s.model.as_ref().map(|m| m.writes_to()).unwrap_or_default(),
        );
    }
    panic!("draft is missing spec for '{catalog_name}'");
}

pub async fn load_draft(
    draft_id: Id,
    db: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
) -> anyhow::Result<tables::DraftCatalog> {
    let rows = agent_sql::drafts::fetch_draft_specs(draft_id.into(), db).await?;
    let mut draft = tables::DraftCatalog::default();
    for row in rows {
        let Some(spec_type) = row.spec_type.map(Into::into) else {
            let mut scope = url::Url::parse("flow://deletion/").unwrap();
            scope.set_path(&row.catalog_name); // url-encodes the name if needed
            draft.errors.push(tables::Error {
                scope,
                error: anyhow::anyhow!(
                    "draft contains a deletion of {:?}, but no such live spec exists",
                    row.catalog_name
                ),
            });
            continue;
        };
        let scope = tables::synthetic_scope(spec_type, &row.catalog_name);
        let expect_pub_id = row.expect_pub_id.map(Into::into);
        if let Err(err) = draft.add_spec(
            spec_type,
            &row.catalog_name,
            scope,
            expect_pub_id,
            row.spec.as_deref().map(|j| &**j),
        ) {
            draft.errors.push(err);
        }
    }
    Ok(draft)
}

// add_built_specs_to_draft_specs adds the built spec and validated response to the draft_specs row
// for all tasks included in build_output if they are in the list of specifications which are
// changing in this publication per the list of spec_rows.
pub async fn add_built_specs_to_draft_specs(
    draft_id: agent_sql::Id,
    build_output: &build::Output,
    db: &sqlx::PgPool,
) -> Result<(), sqlx::Error> {
    // Possible optimization, which I'm not doing right now: collect vecs of all the
    // prepared statement parameters and update all draft specs in a single query.
    for collection in build_output.built_collections().iter() {
        if !collection.is_delete() {
            agent_sql::drafts::add_built_spec(
                draft_id,
                collection.catalog_name().as_str(),
                &collection.spec,
                collection.validated(),
                db,
            )
            .await?;
        }
    }

    for capture in build_output.built_captures().iter() {
        if !capture.is_delete() {
            agent_sql::drafts::add_built_spec(
                draft_id,
                capture.catalog_name().as_str(),
                &capture.spec,
                capture.validated(),
                db,
            )
            .await?;
        }
    }

    for materialization in build_output.built_materializations().iter() {
        if !materialization.is_delete() {
            agent_sql::drafts::add_built_spec(
                draft_id,
                materialization.catalog_name().as_str(),
                &materialization.spec,
                materialization.validated(),
                db,
            )
            .await?;
        }
    }

    for test in build_output.built_tests().iter() {
        if !test.is_delete() {
            agent_sql::drafts::add_built_spec(
                draft_id,
                test.catalog_name().as_str(),
                &test.spec,
                test.validated(),
                db,
            )
            .await?;
        }
    }

    Ok(())
}

fn split_tag(image_full: &str) -> (String, String) {
    let mut image = image_full.to_string();

    if let Some(pivot) = image.find("@sha256:").or_else(|| image.find(":")) {
        let tag = image.split_off(pivot);
        (image, tag)
    } else {
        (image, String::new())
    }
}

// #[cfg(test)]
// mod test {
//     use crate::{publications::JobStatus, FIXED_DATABASE_URL};

//     use super::super::Publisher;
//     use agent_sql::Id;
//     use reqwest::Url;
//     use serde::Deserialize;
//     use serde_json::Value;
//     use sqlx::{Connection, Postgres, Transaction};

//     // Squelch warnings about struct fields never being read.
//     // They actually are read by insta when snapshotting.
//     #[allow(dead_code)]
//     #[derive(Debug, Deserialize)]
//     struct LiveSpec {
//         catalog_name: String,
//         connector_image_name: Option<String>,
//         connector_image_tag: Option<String>,
//         reads_from: Option<Vec<String>>,
//         writes_to: Option<Vec<String>>,
//         spec: Option<Value>,
//         spec_type: Option<String>,
//     }
//     #[allow(dead_code)]
//     #[derive(Debug)]
//     struct ScenarioResult {
//         draft_id: Id,
//         status: JobStatus,
//         errors: Vec<String>,
//         live_specs: Vec<LiveSpec>,
//     }

//     #[tokio::test]
//     #[serial_test::parallel]
//     async fn test_happy_path() {
//         let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
//             .await
//             .unwrap();
//         let mut txn = conn.begin().await.unwrap();

//         sqlx::query(include_str!("test_resources/happy_path.sql"))
//             .execute(&mut txn)
//             .await
//             .unwrap();

//         let results = execute_publications(&mut txn).await;

//         insta::assert_debug_snapshot!(results, @r###"
//         [
//             ScenarioResult {
//                 draft_id: 1110000000000000,
//                 status: Success {
//                     linked_materialization_publications: [],
//                 },
//                 errors: [],
//                 live_specs: [
//                     LiveSpec {
//                         catalog_name: "usageB/DerivationA",
//                         connector_image_name: None,
//                         connector_image_tag: None,
//                         reads_from: Some(
//                             [
//                                 "usageB/CollectionA",
//                             ],
//                         ),
//                         writes_to: None,
//                         spec: Some(
//                             Object {
//                                 "derive": Object {
//                                     "transforms": Array [
//                                         Object {
//                                             "name": String("my-name"),
//                                             "shuffle": String("any"),
//                                             "source": String("usageB/CollectionA"),
//                                         },
//                                     ],
//                                     "using": Object {
//                                         "sqlite": Object {},
//                                     },
//                                 },
//                                 "key": Array [
//                                     String("foo"),
//                                 ],
//                                 "schema": Object {},
//                             },
//                         ),
//                         spec_type: Some(
//                             "collection",
//                         ),
//                     },
//                 ],
//             },
//         ]
//         "###);
//     }

//     #[tokio::test]
//     #[serial_test::serial]
//     async fn test_source_capture_validation() {
//         let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
//             .await
//             .unwrap();
//         let mut txn = conn.begin().await.unwrap();

//         sqlx::query(include_str!("test_resources/linked_materializations.sql"))
//             .execute(&mut txn)
//             .await
//             .unwrap();

//         sqlx::query(r#"
//             with p1 as (
//                 insert into drafts (id, user_id) values ('00:01:02:03:00:00:00:00', '43a18a3e-5a59-11ed-9b6a-0242ac120002')
//             ),
//             p2 as (
//                 insert into draft_specs (draft_id, spec_type, catalog_name, spec) values
//                 ('00:01:02:03:00:00:00:00', 'materialization', 'acmeCo/from-captureA', '{
//                     "endpoint": {"connector":{"image":"matImage:v1","config":{}}},
//                     "sourceCapture": "acmeCo/captureA/source-happy",
//                     "bindings": [ ]
//                 }'::json),
//                 ('00:01:02:03:00:00:00:00', 'materialization', 'acmeCo/from-wrong-spec-type', '{
//                     "endpoint": {"connector":{"image":"matImage:v1","config":{}}},
//                     "sourceCapture": "acmeCo/matB/other-bindings",
//                     "bindings": [ ]
//                 }'::json),
//                 ('00:01:02:03:00:00:00:00', 'materialization', 'acmeCo/from-non-existant', '{
//                     "endpoint": {"connector":{"image":"matImage:v1","config":{}}},
//                     "sourceCapture": "acmeCo/not/a/real/thing",
//                     "bindings": [ ]
//                 }'::json),
//                 ('00:01:02:03:00:00:00:00', 'materialization', 'acmeCo/from-unauthorized', '{
//                     "endpoint": {"connector":{"image":"matImage:v1","config":{}}},
//                     "sourceCapture": "coyoteCo/not/authorized",
//                     "bindings": [ ]
//                 }'::json),
//                 ('00:01:02:03:00:00:00:00', 'materialization', 'acmeCo/from-invalid-name', '{
//                     "endpoint": {"connector":{"image":"matImage:v1","config":{}}},
//                     "sourceCapture": "no-slash",
//                     "bindings": [ ]
//                 }'::json),
//                 ('00:01:02:03:00:00:00:00', 'materialization', 'acmeCo/from-deleted', '{
//                     "endpoint": {"connector":{"image":"matImage:v1","config":{}}},
//                     "sourceCapture": "acmeCo/deleted/thing",
//                     "bindings": [ ]
//                 }'::json)
//             ),
//             p3 as (
//                 insert into publications (draft_id, user_id) values ('00:01:02:03:00:00:00:00', '43a18a3e-5a59-11ed-9b6a-0242ac120002')
//             )
//             select 1;
//             "#).execute(&mut txn).await.unwrap();

//         let results = execute_publications(&mut txn).await;
//         insta::assert_debug_snapshot!(results);
//     }

//     #[tokio::test]
//     #[serial_test::parallel]
//     async fn test_incompatible_collections() {
//         let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
//             .await
//             .unwrap();
//         let mut txn = conn.begin().await.unwrap();

//         sqlx::query(r#"
//             with p1 as (
//               insert into auth.users (id) values
//               ('43a18a3e-5a59-11ed-9b6a-0242ac120003')
//             ),
//             p2 as (
//               insert into drafts (id, user_id) values
//               ('2220000000000000', '43a18a3e-5a59-11ed-9b6a-0242ac120003')
//             ),
//             p3 as (
//                 insert into live_specs (id, catalog_name, spec, spec_type, last_build_id, last_pub_id) values
//                 ('6000000000000000', 'compat-test/CollectionA', '{"schema": {},"key": ["/foo"]}'::json, 'collection', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
//                 ('7000000000000000', 'compat-test/CollectionB', '{
//                     "schema": {},
//                     "key": ["/foo"],
//                     "projections": {
//                         "foo": { "location": "/foo", "partition": true }
//                     }
//                 }'::json, 'collection', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb')
//             ),
//             p4 as (
//               insert into draft_specs (id, draft_id, catalog_name, spec, spec_type) values
//               (
//                 '2222000000000000',
//                 '2220000000000000',
//                 'compat-test/CollectionA',
//                 '{ "schema": {}, "key": ["/new_key"] }'::json,
//                 'collection'
//               ),
//               (
//                 '3333000000000000',
//                 '2220000000000000',
//                 'compat-test/CollectionB',
//                 -- missing partition definition, which should be an error
//                 '{ "schema": {}, "key": ["/foo"] }'::json,
//                 'collection'
//               )

//             ),
//             p5 as (
//               insert into publications (id, job_status, user_id, draft_id) values
//               ('2222200000000000', '{"type": "queued"}'::json, '43a18a3e-5a59-11ed-9b6a-0242ac120003', '2220000000000000')
//             ),
//             p6 as (
//               insert into role_grants (subject_role, object_role, capability) values
//               ('compat-test/', 'compat-test/', 'admin')
//             ),
//             p7 as (
//               insert into user_grants (user_id, object_role, capability) values
//               ('43a18a3e-5a59-11ed-9b6a-0242ac120003', 'compat-test/', 'admin')
//             )
//             select 1;"#,
//         )
//         .execute(&mut txn)
//         .await
//         .unwrap();

//         let results = execute_publications(&mut txn).await;

//         insta::assert_debug_snapshot!(results, @r###"
//         [
//             ScenarioResult {
//                 draft_id: 2220000000000000,
//                 status: BuildFailed {
//                     incompatible_collections: [
//                         IncompatibleCollection {
//                             collection: "compat-test/CollectionA",
//                             requires_recreation: [
//                                 KeyChange,
//                             ],
//                             affected_materializations: [],
//                         },
//                         IncompatibleCollection {
//                             collection: "compat-test/CollectionB",
//                             requires_recreation: [
//                                 PartitionChange,
//                             ],
//                             affected_materializations: [],
//                         },
//                     ],
//                     evolution_id: None,
//                 },
//                 errors: [
//                     "Cannot change key of an established collection from CompositeKey([JsonPointer(\"/foo\")]) to CompositeKey([JsonPointer(\"/new_key\")])",
//                     "Cannot change partitions of an established collection (from [\"foo\"] to [])",
//                 ],
//                 live_specs: [],
//             },
//         ]
//         "###);
//     }

//     #[tokio::test]
//     #[serial_test::parallel]
//     async fn test_allowed_connector() {
//         let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
//             .await
//             .unwrap();
//         let mut txn = conn.begin().await.unwrap();

//         sqlx::query(r#"
//             with p1 as (
//               insert into auth.users (id) values
//               ('43a18a3e-5a59-11ed-9b6a-0242ac120002')
//             ),
//             p2 as (
//               insert into drafts (id, user_id) values
//               ('1110000000000000', '43a18a3e-5a59-11ed-9b6a-0242ac120002')
//             ),
//             p3 as (
//               insert into draft_specs (id, draft_id, catalog_name, spec, spec_type) values
//               ('1111000000000000', '1110000000000000', 'usageB/CaptureC', '{
//                   "bindings": [{"target": "usageB/CaptureC", "resource": {"binding": "foo", "syncMode": "incremental"}}],
//                   "endpoint": {"connector": {"image": "allowed_connector", "config": {}}}
//               }'::json, 'capture')
//             ),
//             p4 as (
//               insert into publications (id, job_status, user_id, draft_id) values
//               ('1111100000000000', '{"type": "queued"}'::json, '43a18a3e-5a59-11ed-9b6a-0242ac120002', '1110000000000000')
//             ),
//             p5 as (
//               insert into role_grants (subject_role, object_role, capability) values
//               ('usageB/', 'usageB/', 'admin')
//             ),
//             p6 as (
//               insert into user_grants (user_id, object_role, capability) values
//               ('43a18a3e-5a59-11ed-9b6a-0242ac120002', 'usageB/', 'admin')
//             ),
//             p7 as (
//                 insert into connectors (external_url, image_name, title, short_description, logo_url) values
//                     ('http://example.com', 'allowed_connector', '{"en-US": "foo"}'::json, '{"en-US": "foo"}'::json, '{"en-US": "foo"}'::json)
//             )
//             select 1;
//             "#).execute(&mut txn).await.unwrap();

//         let results = execute_publications(&mut txn).await;

//         insta::assert_debug_snapshot!(results, @r###"
//         [
//             ScenarioResult {
//                 draft_id: 1110000000000000,
//                 status: Success {
//                     linked_materialization_publications: [],
//                 },
//                 errors: [],
//                 live_specs: [
//                     LiveSpec {
//                         catalog_name: "usageB/CaptureC",
//                         connector_image_name: Some(
//                             "allowed_connector",
//                         ),
//                         connector_image_tag: Some(
//                             "",
//                         ),
//                         reads_from: None,
//                         writes_to: Some(
//                             [
//                                 "usageB/CaptureC",
//                             ],
//                         ),
//                         spec: Some(
//                             Object {
//                                 "bindings": Array [
//                                     Object {
//                                         "resource": Object {
//                                             "binding": String("foo"),
//                                             "syncMode": String("incremental"),
//                                         },
//                                         "target": String("usageB/CaptureC"),
//                                     },
//                                 ],
//                                 "endpoint": Object {
//                                     "connector": Object {
//                                         "config": Object {},
//                                         "image": String("allowed_connector"),
//                                     },
//                                 },
//                             },
//                         ),
//                         spec_type: Some(
//                             "capture",
//                         ),
//                     },
//                 ],
//             },
//         ]
//         "###);
//     }

//     #[tokio::test]
//     #[serial_test::serial]
//     async fn test_quota_single_task() {
//         let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
//             .await
//             .unwrap();
//         let mut txn = conn.begin().await.unwrap();

//         sqlx::query(r#"
//             with p1 as (
//                 insert into live_specs (id, catalog_name, spec, spec_type, last_build_id, last_pub_id) values
//                 ('1000000000000000', 'usageB/CollectionA', '{}'::json, 'collection', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
//                 ('1100000000000000', 'usageB/CollectionB', '{}'::json, 'collection', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
//                 ('2000000000000000', 'usageB/CaptureA', '{"endpoint": {},"bindings": []}'::json, 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
//                 ('3000000000000000', 'usageB/CaptureB', '{"endpoint": {},"bindings": []}'::json, 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
//                 ('4000000000000000', 'usageB/CaptureDisabled', '{"shards": {"disable": true}}'::json, 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb')
//               ),
//               p2 as (
//                   insert into tenants (tenant, tasks_quota, collections_quota) values
//                   ('usageB/', 2, 2)
//               ),
//               p3 as (
//                 insert into auth.users (id) values
//                 ('43a18a3e-5a59-11ed-9b6a-0242ac120002')
//               ),
//               p4 as (
//                 insert into drafts (id, user_id) values
//                 ('1110000000000000', '43a18a3e-5a59-11ed-9b6a-0242ac120002')
//               ),
//               p5 as (
//                 insert into draft_specs (id, draft_id, catalog_name, spec, spec_type) values
//                 ('1111000000000000', '1110000000000000', 'usageB/CaptureC', '{
//                     "bindings": [{"target": "usageB/CaptureC", "resource": {"binding": "foo", "syncMode": "incremental"}}],
//                     "endpoint": {"connector": {"image": "foo", "config": {}}}
//                 }'::json, 'capture'),
//                 -- This collection should be pruned, and thus _not_ count against the quota of 2 collections.
//                 ('1111200000000000', '1110000000000000', 'usageB/UnboundCollection', '{
//                     "schema": {},
//                     "key": ["/id"]
//                 }'::json, 'collection')
//               ),
//               p6 as (
//                 insert into publications (id, job_status, user_id, draft_id) values
//                 ('1111100000000000', '{"type": "queued"}'::json, '43a18a3e-5a59-11ed-9b6a-0242ac120002', '1110000000000000')
//               ),
//               p7 as (
//                 insert into role_grants (subject_role, object_role, capability) values
//                 ('usageB/', 'usageB/', 'admin')
//               ),
//               p8 as (
//                 insert into user_grants (user_id, object_role, capability) values
//                 ('43a18a3e-5a59-11ed-9b6a-0242ac120002', 'usageB/', 'admin')
//               )
//               select 1;
//               "#).execute(&mut txn).await.unwrap();

//         let results = execute_publications(&mut txn).await;

//         insta::assert_debug_snapshot!(results, @r###"
//         [
//             ScenarioResult {
//                 draft_id: 1110000000000000,
//                 status: BuildFailed {
//                     incompatible_collections: [],
//                     evolution_id: None,
//                 },
//                 errors: [
//                     "Request to add 1 task(s) would exceed tenant 'usageB/' quota of 2. 2 are currently in use.",
//                 ],
//                 live_specs: [],
//             },
//         ]
//         "###);
//     }

//     #[tokio::test]
//     #[serial_test::serial]
//     async fn test_quota_derivations() {
//         let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
//             .await
//             .unwrap();
//         let mut txn = conn.begin().await.unwrap();

//         sqlx::query(r#"
//             with p1 as (
//                 insert into live_specs (id, catalog_name, spec, spec_type, last_build_id, last_pub_id) values
//                 ('1000000000000000', 'usageB/CollectionA', '{}'::json, 'collection', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
//                 ('1100000000000000', 'usageB/CollectionB', '{}'::json, 'collection', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
//                 ('2000000000000000', 'usageB/CaptureA', '{"endpoint": {},"bindings": []}'::json, 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
//                 ('3000000000000000', 'usageB/CaptureB', '{"endpoint": {},"bindings": []}'::json, 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
//                 ('4000000000000000', 'usageB/CaptureDisabled', '{"shards": {"disable": true}}'::json, 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb')
//               ),
//               p2 as (
//                   insert into tenants (tenant, tasks_quota, collections_quota) values
//                   ('usageB/', 2, 2)
//               ),
//               p3 as (
//                 insert into auth.users (id) values
//                 ('43a18a3e-5a59-11ed-9b6a-0242ac120002')
//               ),
//               p4 as (
//                 insert into drafts (id, user_id) values
//                 ('1120000000000000', '43a18a3e-5a59-11ed-9b6a-0242ac120002')
//               ),
//               p5 as (
//                 insert into draft_specs (id, draft_id, catalog_name, spec, spec_type) values
//                 ('1112000000000000', '1120000000000000', 'usageB/DerivationA', '{
//                   "schema": {},
//                   "key": ["foo"],
//                   "derive": {
//                     "using":{"typescript": {"module": "foo.ts"}},
//                     "transforms": [{"source":"usageB/CollectionA","shuffle":"any","name":"foo"}]
//                   }
//                 }'::json, 'collection')
//               ),
//               p6 as (
//                 insert into publications (id, job_status, user_id, draft_id) values
//                 ('1111200000000000', '{"type": "queued"}'::json, '43a18a3e-5a59-11ed-9b6a-0242ac120002', '1120000000000000')
//               ),
//               p7 as (
//                 insert into role_grants (subject_role, object_role, capability) values
//                 ('usageB/', 'usageB/', 'admin')
//               ),
//               p8 as (
//                 insert into user_grants (user_id, object_role, capability) values
//                 ('43a18a3e-5a59-11ed-9b6a-0242ac120002', 'usageB/', 'admin')
//               )
//               select 1;
//               "#).execute(&mut txn).await.unwrap();

//         let results = execute_publications(&mut txn).await;

//         insta::assert_debug_snapshot!(results, @r###"
//         [
//             ScenarioResult {
//                 draft_id: 1120000000000000,
//                 status: BuildFailed {
//                     incompatible_collections: [],
//                     evolution_id: None,
//                 },
//                 errors: [
//                     "Request to add 1 collections(s) would exceed tenant 'usageB/' quota of 2. 2 are currently in use.",
//                     "Request to add 1 task(s) would exceed tenant 'usageB/' quota of 2. 2 are currently in use.",
//                 ],
//                 live_specs: [],
//             },
//         ]
//         "###);
//     }

//     // Testing that we can disable tasks to reduce usage when at quota
//     #[tokio::test]
//     #[serial_test::parallel]
//     async fn test_disable_when_over_quota() {
//         let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
//             .await
//             .unwrap();
//         let mut txn = conn.begin().await.unwrap();

//         sqlx::query(r#"
//             with p1 as (
//                 insert into live_specs (id, catalog_name, spec, spec_type, last_build_id, last_pub_id) values
//                 ('a100000000000000', 'usageC/CollectionA', '{"schema": {}, "key": ["foo"]}'::json, 'collection', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
//                 ('a200000000000000', 'usageC/CaptureA', '{
//                     "bindings": [{"target": "usageC/CollectionA", "resource": {"binding": "foo", "syncMode": "incremental"}}],
//                     "endpoint": {"connector": {"image": "foo", "config": {}}}
//                 }'::json, 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
//                 ('a300000000000000', 'usageC/CaptureB', '{
//                     "bindings": [{"target": "usageC/CollectionA", "resource": {"binding": "foo", "syncMode": "incremental"}}],
//                     "endpoint": {"connector": {"image": "foo", "config": {}}}
//                 }'::json, 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb')
//               ),
//               p2 as (
//                   insert into tenants (tenant, tasks_quota, collections_quota) values
//                   ('usageC/', 1, 1)
//               ),
//               p3 as (
//                 insert into auth.users (id) values
//                 ('43a18a3e-5a59-11ed-9b6a-0242ac120002')
//               ),
//               p4 as (
//                 insert into drafts (id, user_id) values
//                 ('1130000000000000', '43a18a3e-5a59-11ed-9b6a-0242ac120002')
//               ),
//               p5 as (
//                 insert into draft_specs (id, draft_id, catalog_name, spec, spec_type) values
//                 ('1113000000000000', '1130000000000000', 'usageC/CaptureA', '{
//                     "bindings": [{"target": "usageC/CollectionA", "resource": {"binding": "foo", "syncMode": "incremental"}}],
//                     "endpoint": {"connector": {"image": "foo", "config": {}}},
//                     "shards": {"disable": true}
//                 }'::json, 'capture')
//               ),
//               p6 as (
//                 insert into publications (id, job_status, user_id, draft_id) values
//                 ('1111300000000000', '{"type": "queued"}'::json, '43a18a3e-5a59-11ed-9b6a-0242ac120002', '1130000000000000')
//               ),
//               p7 as (
//                 insert into role_grants (subject_role, object_role, capability) values
//                 ('usageC/', 'usageC/', 'admin')
//               ),
//               p8 as (
//                 insert into user_grants (user_id, object_role, capability) values
//                 ('43a18a3e-5a59-11ed-9b6a-0242ac120002', 'usageC/', 'admin')
//               ),
//               p9 as (
//                 insert into connectors (external_url, image_name, title, short_description, logo_url) values
//                     ('http://example.com', 'foo', '{"en-US": "foo"}'::json, '{"en-US": "foo"}'::json, '{"en-US": "foo"}'::json)
//             )
//               select 1;
//               "#).execute(&mut txn).await.unwrap();

//         let results = execute_publications(&mut txn).await;

//         insta::assert_debug_snapshot!(results, @r###"
//         [
//             ScenarioResult {
//                 draft_id: 1130000000000000,
//                 status: Success {
//                     linked_materialization_publications: [],
//                 },
//                 errors: [],
//                 live_specs: [
//                     LiveSpec {
//                         catalog_name: "usageC/CaptureA",
//                         connector_image_name: Some(
//                             "foo",
//                         ),
//                         connector_image_tag: Some(
//                             "",
//                         ),
//                         reads_from: None,
//                         writes_to: Some(
//                             [
//                                 "usageC/CollectionA",
//                             ],
//                         ),
//                         spec: Some(
//                             Object {
//                                 "bindings": Array [
//                                     Object {
//                                         "resource": Object {
//                                             "binding": String("foo"),
//                                             "syncMode": String("incremental"),
//                                         },
//                                         "target": String("usageC/CollectionA"),
//                                     },
//                                 ],
//                                 "endpoint": Object {
//                                     "connector": Object {
//                                         "config": Object {},
//                                         "image": String("foo"),
//                                     },
//                                 },
//                                 "shards": Object {
//                                     "disable": Bool(true),
//                                 },
//                             },
//                         ),
//                         spec_type: Some(
//                             "capture",
//                         ),
//                     },
//                 ],
//             },
//         ]
//         "###);
//     }

//     #[tokio::test]
//     #[serial_test::parallel]
//     async fn test_prune_unbound_collections_publication() {
//         let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
//             .await
//             .unwrap();
//         let mut txn = conn.begin().await.unwrap();

//         sqlx::query(include_str!("test_resources/prune_collections.sql"))
//             .execute(&mut txn)
//             .await
//             .unwrap();

//         let results = execute_publications(&mut txn).await;
//         insta::assert_debug_snapshot!(results);
//     }

//     #[tokio::test]
//     #[serial_test::parallel]
//     async fn test_publish_error_when_all_collections_are_pruned() {
//         let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
//             .await
//             .unwrap();
//         let mut txn = conn.begin().await.unwrap();

//         sqlx::query(r#"
//           with setup_user as (
//               insert into auth.users (id) values
//               ('43a18a3e-5a59-11ed-9b6a-0242ac120002')
//             ),
//             setup_user_grants as (
//               insert into user_grants (user_id, object_role, capability) values
//               ('43a18a3e-5a59-11ed-9b6a-0242ac120002', 'acmeCo/', 'admin')
//             ),
//             setup_role_grants as (
//               insert into role_grants (subject_role, object_role, capability) values
//               ('acmeCo/', 'acmeCo/', 'admin')
//             ),
//             setup_draft as (
//               insert into drafts (id, user_id) values
//               ('1111000000000000', '43a18a3e-5a59-11ed-9b6a-0242ac120002')
//             ),
//             setup_draft_specs as (
//               insert into draft_specs (id, draft_id, catalog_name, spec, spec_type) values
//               ('1111111111111111', '1111000000000000', 'acmeCo/should_prune', '{
//                 "schema": { "type": "object" },
//                 "key": ["/id"]
//               }', 'collection')
//             ),
//             setup_publications as (
//               insert into publications (id, job_status, user_id, draft_id) values
//               ('1111100000000000', '{"type": "queued"}'::json, '43a18a3e-5a59-11ed-9b6a-0242ac120002', '1111000000000000')
//             )
//             select 1;
//             "#)
//             .execute(&mut txn)
//             .await
//             .unwrap();

//         let results = execute_publications(&mut txn).await;
//         insta::assert_debug_snapshot!(results, @r###"
//         [
//             ScenarioResult {
//                 draft_id: 1111000000000000,
//                 status: EmptyDraft,
//                 errors: [],
//                 live_specs: [],
//             },
//         ]
//         "###);
//     }
// }

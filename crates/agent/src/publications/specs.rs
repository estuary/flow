use super::{LockFailure, UncommittedBuild};
use agent_sql::publications::{LiveRevision, LiveSpecUpdate};
use agent_sql::Capability;
use anyhow::Context;
use models::{split_image_tag, Id, ModelDef, SourceType, TargetNaming};
use serde_json::value::RawValue;
use sqlx::types::Uuid;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use tables::{utils, BuiltRow, DraftRow};

pub async fn persist_updates(
    uncommitted: &UncommittedBuild,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<Vec<LockFailure>> {
    let UncommittedBuild {
        ref publication_id,
        ref build_id,
        ref output,
        ref user_id,
        ref detail,
        ..
    } = uncommitted;

    let lock_failures = update_live_specs(*publication_id, *build_id, output, txn).await?;
    if !lock_failures.is_empty() {
        return Ok(lock_failures);
    }
    let lock_failures = verify_unchanged_revisions(output, txn).await?;
    if !lock_failures.is_empty() {
        return Ok(lock_failures);
    }

    update_drafted_live_spec_flows(output, txn)
        .await
        .context("updating live spec flows")?;

    insert_publication_specs(
        *publication_id,
        *user_id,
        detail.as_ref(),
        &output.built,
        txn,
    )
    .await
    .context("inserting publication specs")?;

    Ok(Vec::new())
}

#[tracing::instrument(skip(built, txn))]
async fn update_live_spec_flows<B: tables::BuiltRow>(
    catalog_name: &str,
    catalog_type: agent_sql::CatalogType,
    built: &B,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<()> {
    agent_sql::publications::delete_stale_flow(built.control_id().into(), catalog_type, txn)
        .await?;

    let Some(model) = built.model() else {
        return Ok(());
    };

    let reads_from = model.reads_from();
    let writes_to = model.writes_to();
    let source_capture = model.materialization_source_capture_name();

    agent_sql::publications::insert_live_spec_flows(
        built.control_id().into(),
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
    build: &build::Output,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<()> {
    for r in build
        .built
        .built_captures
        .iter()
        .filter(|r| !r.is_passthrough())
    {
        update_live_spec_flows(&r.catalog_name(), agent_sql::CatalogType::Capture, r, txn)
            .await
            .with_context(|| format!("updating live_spec_flows for '{}'", r.catalog_name()))?;
    }
    for r in build
        .built
        .built_collections
        .iter()
        .filter(|r| !r.is_passthrough())
    {
        update_live_spec_flows(
            &r.catalog_name(),
            agent_sql::CatalogType::Collection,
            r,
            txn,
        )
        .await
        .with_context(|| format!("updating live_spec_flows for '{}'", r.catalog_name()))?;
    }
    for r in build
        .built
        .built_materializations
        .iter()
        .filter(|r| !r.is_passthrough())
    {
        update_live_spec_flows(
            &r.catalog_name(),
            agent_sql::CatalogType::Materialization,
            r,
            txn,
        )
        .await
        .with_context(|| format!("updating live_spec_flows for '{}'", r.catalog_name()))?;
    }
    for r in build
        .built
        .built_tests
        .iter()
        .filter(|r| !r.is_passthrough())
    {
        update_live_spec_flows(&r.catalog_name(), agent_sql::CatalogType::Test, r, txn)
            .await
            .with_context(|| format!("updating live_spec_flows for '{}'", r.catalog_name()))?;
    }
    Ok(())
}

async fn update_live_specs(
    pub_id: Id,
    build_id: Id,
    output: &build::Output,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<Vec<LockFailure>> {
    let n_specs = output.built.spec_count();
    let mut control_ids = Vec::with_capacity(n_specs);
    let mut catalog_names = Vec::with_capacity(n_specs);
    let mut spec_types: Vec<agent_sql::CatalogType> = Vec::with_capacity(n_specs);
    let mut models = Vec::with_capacity(n_specs);
    let mut built_specs = Vec::with_capacity(n_specs);
    let mut expect_build_ids: Vec<agent_sql::Id> = Vec::with_capacity(n_specs);
    let mut reads_froms = Vec::with_capacity(n_specs);
    let mut writes_tos = Vec::with_capacity(n_specs);
    let mut images = Vec::with_capacity(n_specs);
    let mut image_tags = Vec::with_capacity(n_specs);
    let mut data_plane_ids: Vec<Id> = Vec::with_capacity(n_specs);
    let mut is_touches = Vec::with_capacity(n_specs);
    let mut dependency_hashes = Vec::with_capacity(n_specs);

    for r in output
        .built
        .built_captures
        .iter()
        .filter(|r| !r.is_passthrough())
    {
        control_ids.push(r.control_id);
        catalog_names.push(r.catalog_name().to_string());
        spec_types.push(agent_sql::CatalogType::Capture);
        models.push(to_raw_value(r.model(), agent_sql::TextJson)?);
        built_specs.push(to_raw_value(r.spec(), agent_sql::TextJson)?);
        expect_build_ids.push(r.expect_build_id());
        reads_froms.push(None);
        writes_tos.push(get_dependencies(r.model(), ModelDef::writes_to));
        let (image_name, image_tag) = image_and_tag(r.model());
        images.push(image_name);
        image_tags.push(image_tag);
        data_plane_ids.push(r.data_plane_id);
        is_touches.push(r.is_touch());
        dependency_hashes.push(r.dependency_hash.as_deref());
    }
    for r in output
        .built
        .built_collections
        .iter()
        .filter(|r| !r.is_passthrough())
    {
        control_ids.push(r.control_id);
        catalog_names.push(r.catalog_name().to_string());
        spec_types.push(agent_sql::CatalogType::Collection);
        models.push(to_raw_value(r.model(), agent_sql::TextJson)?);
        built_specs.push(to_raw_value(r.spec(), agent_sql::TextJson)?);
        expect_build_ids.push(r.expect_build_id());
        reads_froms.push(get_dependencies(
            // reads_from should be null for regular collections
            r.model().filter(|m| m.derive.is_some()),
            ModelDef::reads_from,
        ));
        writes_tos.push(None);
        let (image_name, image_tag) = image_and_tag(r.model());
        images.push(image_name);
        image_tags.push(image_tag);
        data_plane_ids.push(r.data_plane_id);
        is_touches.push(r.is_touch());
        dependency_hashes.push(r.dependency_hash.as_deref());
    }
    for r in output
        .built
        .built_materializations
        .iter()
        .filter(|r| !r.is_passthrough())
    {
        control_ids.push(r.control_id);
        catalog_names.push(r.catalog_name().to_string());
        spec_types.push(agent_sql::CatalogType::Materialization);
        models.push(to_raw_value(r.model(), agent_sql::TextJson)?);
        built_specs.push(to_raw_value(r.spec(), agent_sql::TextJson)?);
        expect_build_ids.push(r.expect_build_id());
        reads_froms.push(get_dependencies(r.model(), ModelDef::reads_from));
        writes_tos.push(None);
        let (image_name, image_tag) = image_and_tag(r.model());
        images.push(image_name);
        image_tags.push(image_tag);
        data_plane_ids.push(r.data_plane_id);
        is_touches.push(r.is_touch());
        dependency_hashes.push(r.dependency_hash.as_deref());
    }
    for r in output
        .built
        .built_tests
        .iter()
        .filter(|r| !r.is_passthrough())
    {
        control_ids.push(r.control_id);
        catalog_names.push(r.catalog_name().to_string());
        spec_types.push(agent_sql::CatalogType::Test);
        models.push(to_raw_value(r.model(), agent_sql::TextJson)?);
        built_specs.push(to_raw_value(r.spec(), agent_sql::TextJson)?);
        expect_build_ids.push(r.expect_build_id());
        reads_froms.push(get_dependencies(r.model(), ModelDef::reads_from));
        writes_tos.push(get_dependencies(r.model(), ModelDef::writes_to));
        let (image_name, image_tag) = image_and_tag(r.model());
        images.push(image_name);
        image_tags.push(image_tag);
        data_plane_ids.push(models::Id::zero());
        is_touches.push(r.is_touch());
        dependency_hashes.push(r.dependency_hash.as_deref());
    }

    let updates = agent_sql::publications::update_live_specs(
        pub_id,
        build_id,
        &control_ids,
        &catalog_names,
        &spec_types,
        &models,
        &built_specs,
        &expect_build_ids,
        &reads_froms,
        &writes_tos,
        &images,
        &image_tags,
        &data_plane_ids,
        &is_touches,
        &dependency_hashes,
        txn,
    )
    .await?;

    let mut lock_failures = Vec::new();

    for update in updates {
        let LiveSpecUpdate {
            catalog_name,
            expect_build_id,
            last_build_id,
            live_spec_id: _,
        } = update;

        if last_build_id != expect_build_id {
            lock_failures.push(LockFailure {
                catalog_name,
                actual: Some(last_build_id.into()).filter(|id: &models::Id| !id.is_zero()),
                expected: expect_build_id.into(),
            })
        }
    }

    Ok(lock_failures)
}

pub async fn check_source_capture_annotations(
    draft: &tables::DraftCatalog,
    pool: &sqlx::PgPool,
) -> anyhow::Result<tables::Errors> {
    let mut errors = tables::Errors::default();

    for materialization in draft.materializations.iter() {
        let Some(model) = materialization.model() else {
            continue;
        };
        let Some(image) = model.connector_image() else {
            continue;
        };
        let (image_name, image_tag) = split_image_tag(&image);

        let Some(source_capture) = &model.source else {
            continue;
        };

        // SourceCaptures require a connector_tags row in any case. To avoid an error down the line
        // in the controller we validate that here. This should only happen for test connector
        // tags, hence the technical error message
        let Some(connector_spec) =
            agent_sql::connector_tags::fetch_connector_spec(&image_name, &image_tag, pool).await?
        else {
            errors.insert(tables::Error {
                scope: tables::synthetic_scope(model.catalog_type(), materialization.catalog_name()),
                error: anyhow::anyhow!("materializations with a sourceCapture only work for known connector tags. {image} is not known to the control plane"),
            });
            continue;
        };
        if let SourceType::Configured(source_capture_def) = source_capture {
            let resource_config_schema = connector_spec.resource_config_schema;
            let resource_spec_pointers = utils::pointer_for_schema(resource_config_schema.0.get())?;

            if source_capture_def.delta_updates && resource_spec_pointers.x_delta_updates.is_none()
            {
                errors.insert(tables::Error {
                    scope: tables::synthetic_scope(model.catalog_type(), materialization.catalog_name()),
                    error: anyhow::anyhow!("sourceCapture.deltaUpdates set but the connector '{image_name}' does not support delta updates"),
                });
            }

            if source_capture_def.target_naming == TargetNaming::WithSchema
                && resource_spec_pointers.x_schema_name.is_none()
            {
                errors.insert(tables::Error {
                    scope: tables::synthetic_scope(model.catalog_type(), materialization.catalog_name()),
                    error: anyhow::anyhow!("sourceCapture.targetSchema set but the connector '{image_name}' does not support resource schemas"),
                });
            }
        }
    }
    Ok(errors)
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
    model: Option<&impl ModelDef>,
    cached: &mut BTreeMap<String, bool>,
    pool: &sqlx::PgPool,
) -> anyhow::Result<Option<tables::Error>> {
    let Some(model) = model else {
        return Ok(None);
    };
    let Some(image) = model.connector_image() else {
        return Ok(None);
    };
    let (image_name, _) = split_image_tag(&image);
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

fn image_and_tag<M: ModelDef>(model: Option<&M>) -> (Option<String>, Option<String>) {
    let Some(full_image) = model.and_then(ModelDef::connector_image) else {
        return (None, None);
    };
    let (image_name, image_tag) = split_image_tag(&full_image);
    (Some(image_name), Some(image_tag))
}

// TODO(phil): update `insert_publication_specs` to insert all of them in one go
async fn insert_publication_specs(
    publication_id: models::Id,
    user_id: Uuid,
    detail: Option<&String>,
    built: &tables::Validations,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<()> {
    let build_detail = |model_fixes: &[String]| -> String {
        let mut out = detail.map(String::clone).unwrap_or_default();

        for fix in model_fixes {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str("- ");
            out.extend(fix.chars());
        }
        out
    };

    for r in built
        .built_captures
        .iter()
        .filter(|r| !r.is_passthrough() && !r.is_touch())
    {
        let spec = to_raw_value(r.model(), agent_sql::TextJson)?;
        agent_sql::publications::insert_publication_spec(
            r.control_id().into(),
            publication_id.into(),
            build_detail(&r.model_fixes),
            &spec,
            &Some(agent_sql::CatalogType::Capture),
            user_id,
            txn,
        )
        .await
        .with_context(|| format!("inserting spec for '{}'", r.catalog_name()))?;
    }
    for r in built
        .built_collections
        .iter()
        .filter(|r| !r.is_passthrough() && !r.is_touch())
    {
        let spec = to_raw_value(r.model(), agent_sql::TextJson)?;
        agent_sql::publications::insert_publication_spec(
            r.control_id().into(),
            publication_id.into(),
            build_detail(&r.model_fixes),
            &spec,
            &Some(agent_sql::CatalogType::Collection),
            user_id,
            txn,
        )
        .await
        .with_context(|| format!("inserting spec for '{}'", r.catalog_name()))?;
    }
    for r in built
        .built_materializations
        .iter()
        .filter(|r| !r.is_passthrough() && !r.is_touch())
    {
        let spec = to_raw_value(r.model(), agent_sql::TextJson)?;
        agent_sql::publications::insert_publication_spec(
            r.control_id().into(),
            publication_id.into(),
            build_detail(&r.model_fixes),
            &spec,
            &Some(agent_sql::CatalogType::Materialization),
            user_id,
            txn,
        )
        .await
        .with_context(|| format!("inserting spec for '{}'", r.catalog_name()))?;
    }
    for r in built
        .built_tests
        .iter()
        .filter(|r| !r.is_passthrough() && !r.is_touch())
    {
        let spec = to_raw_value(r.model(), agent_sql::TextJson)?;
        agent_sql::publications::insert_publication_spec(
            r.control_id().into(),
            publication_id.into(),
            build_detail(&r.model_fixes),
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
    M: ModelDef,
    F: Fn(&M) -> BTreeSet<models::Collection>,
{
    model.map(|m| agent_sql::TextJson(get(m).into_iter().map(Into::into).collect()))
}

async fn verify_unchanged_revisions(
    output: &build::Output,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<Vec<LockFailure>> {
    let mut expected: BTreeMap<&str, Id> = output
        .built
        .built_captures
        .iter()
        .filter(|r| r.is_passthrough())
        .map(|r| (r.catalog_name().as_str(), r.expect_pub_id()))
        .chain(
            output
                .built
                .built_collections
                .iter()
                .filter(|r| r.is_passthrough())
                .map(|r| (r.catalog_name().as_str(), r.expect_pub_id())),
        )
        .chain(
            output
                .built
                .built_materializations
                .iter()
                .filter(|r| r.is_passthrough())
                .map(|r| (r.catalog_name().as_str(), r.expect_pub_id())),
        )
        .chain(
            output
                .built
                .built_tests
                .iter()
                .filter(|r| r.is_passthrough())
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
                    actual: Some(last_pub_id.into()),
                    expected: expect_pub_id,
                });
            }
        }
    }
    // Remaining expected pub ids are for `live_specs` rows which have been deleted since we started the publication.
    for (catalog_name, expect_pub_id) in expected {
        if !expect_pub_id.is_zero() {}
        errors.push(LockFailure {
            catalog_name: catalog_name.to_string(),
            actual: None,
            expected: expect_pub_id,
        });
    }
    Ok(errors)
}

fn to_raw_value<T: serde::Serialize, W, F>(
    maybe_spec: Option<&T>,
    wrap: F,
) -> anyhow::Result<Option<W>>
where
    F: Fn(Box<RawValue>) -> W,
{
    if let Some(value) = maybe_spec {
        let json = serde_json::value::to_raw_value(value).expect("must serialize spec to json");
        if includes_escaped_null(&json) {
            anyhow::bail!(
                "a string in the spec contains a disallowed unicode null escape (\\x00 or \\u0000)"
            );
        }
        Ok(Some(wrap(json)))
    } else {
        Ok(None)
    }
}

/// Checks the given `RawValue` to see if any of the string values contain
/// escape sequences for null bytes (\u0000). Nulls are valid in any keys or
/// strings in JSON, and Postgres will accept them as part of a JSON (but not
/// JSONB) column. But Postgres will error if a query ever needs to parse such
/// a JSON column, for example to evaluate a filter that reaches into the JSON
/// using `->`. So, even though `\u0000` is technically valid JSON, we disallow
/// any live specs to contain the null escape sequence, since it causes many of
/// our queries to error.
///
/// In order to properly identify such escape sequences, we need to also handle
/// the case where the backslash itself is escaped, for example `"\\u0000"`.
/// There can be arbitrarily many backslashes in front of the `u0000`, so we
/// look for an odd number of them, which indicates that the final `\` is not
/// itself escaped.
fn includes_escaped_null(json: &RawValue) -> bool {
    lazy_static::lazy_static! {
        static ref ESCAPE_RE: regex::Regex = regex::Regex::new(r#"\\+u0000"#).unwrap();
    }

    for maybe_escape in ESCAPE_RE.find_iter(json.get()) {
        let preceeding_backslash_count = maybe_escape
            .as_str()
            .chars()
            .take_while(|c| *c == '\\')
            .count();
        if preceeding_backslash_count % 2 == 1 {
            return true;
        }
    }
    false
}

/// This is a temporary standin for a function that will lookup the ops collection names based on
/// the data plane that's associated with the tenants of published tasks.
pub fn get_ops_collection_names() -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    names.insert("ops.us-central1.v1/logs".to_string());
    names.insert("ops.us-central1.v1/stats".to_string());
    names
}

pub async fn resolve_live_specs(
    user_id: Uuid,
    draft: &tables::DraftCatalog,
    db: &sqlx::PgPool,
    default_data_plane_name: &str,
    verify_user_authz: bool,
) -> anyhow::Result<tables::LiveCatalog> {
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

    let rows = agent_sql::live_specs::fetch_live_specs(
        user_id,
        &all_spec_names,
        verify_user_authz,
        true, // always fetch spec capabilities
        db,
    )
    .await
    .context("fetching live specs")?;

    // Check the user and spec authorizations.
    // Start by making an easy way to lookup whether each row was drafted or not.
    let drafted_names = draft.all_spec_names().collect::<HashSet<_>>();

    // Gather IDs of data-planes in use by live specs.
    let mut data_plane_ids = Vec::new();

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
            if verify_user_authz && !matches!(spec_row.user_capability, Some(Capability::Admin)) {
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
            // Spec authz must always be checked, even if we're not checking user authz
            for source in reads_from {
                if !spec_row.spec_capabilities.iter().any(|c| {
                    source.starts_with(c.object_role.as_str()) && c.capability >= Capability::Read
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
                    target.starts_with(c.object_role.as_str())
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
            if verify_user_authz
                && !spec_row
                    .user_capability
                    .map(|c| c >= Capability::Read)
                    .unwrap_or(false)
            {
                let scope = tables::synthetic_scope("unauthorized", &spec_row.catalog_name);
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
            let catalog_type: models::CatalogType = spec_row.spec_type.unwrap().into();
            live.add_spec(
                catalog_type,
                &spec_row.catalog_name,
                spec_row.id.into(),
                spec_row.data_plane_id.into(),
                spec_row.last_pub_id.into(),
                spec_row.last_build_id.into(),
                &model,
                &spec_row
                    .built_spec
                    .as_ref()
                    .ok_or_else(|| {
                        anyhow::anyhow!("row has non-null spec, but null built_spec: catalog_name: {:?}, live_spec_id: {}", &spec_row.catalog_name, spec_row.id)
                    })?,
                spec_row.dependency_hash,
            )
            .with_context(|| format!("adding live spec for {:?}", spec_row.catalog_name))?;
        }

        data_plane_ids.push(spec_row.data_plane_id);
    }

    // Note that we don't need storage mappings for live specs, only the drafted ones.
    let mut tenant_names = drafted_names
        .iter()
        .flat_map(|name| tenant(name))
        .collect::<Vec<_>>();
    tenant_names.sort();
    tenant_names.dedup();

    let storage_rows = agent_sql::publications::resolve_storage_mappings(tenant_names, db).await?;
    for row in storage_rows {
        let store: models::StorageDef = match serde_json::from_value(row.spec) {
            Ok(s) => s,
            Err(err) => {
                live.errors.push(tables::Error {
                    scope: tables::synthetic_scope("storageMapping", &row.catalog_prefix),
                    error: anyhow::Error::from(err).context("deserializing storage mapping spec"),
                });
                continue;
            }
        };
        live.storage_mappings.insert(tables::StorageMapping {
            control_id: row.id.into(),
            catalog_prefix: models::Prefix::new(row.catalog_prefix),
            stores: store.stores,
        });
    }

    live.data_planes = agent_sql::data_plane::fetch_data_planes(
        db,
        data_plane_ids,
        default_data_plane_name,
        user_id,
    )
    .await?;

    // TODO(phil): remove once we no longer need to inline inferred schemas as part of validation
    resolve_inferred_schemas(draft, &mut live, db).await?;

    Ok(live)
}

/// Returns an option because `catalog_name` is from a drafted spec, and we've yet to
/// fully validate the name. Returns the tenant name with the trailing `/`.
fn tenant(catalog_name: &impl AsRef<str>) -> Option<&str> {
    let Some(idx) = catalog_name.as_ref().find('/') else {
        return None;
    };
    Some(catalog_name.as_ref().split_at(idx + 1).0)
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

// add_built_specs_to_draft_specs adds the built spec and validated response to the draft_specs row
// for all tasks included in build_output if they are in the list of specifications which are
// changing in this publication per the list of spec_rows.
pub async fn add_built_specs_to_draft_specs(
    draft_id: agent_sql::Id,
    build_output: &tables::Validations,
    db: &sqlx::PgPool,
) -> Result<(), sqlx::Error> {
    // Possible optimization, which I'm not doing right now: collect vecs of all the
    // prepared statement parameters and update all draft specs in a single query.
    for collection in build_output.built_collections.iter() {
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

    for capture in build_output.built_captures.iter() {
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

    for materialization in build_output.built_materializations.iter() {
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

    for test in build_output.built_tests.iter() {
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_null_bytes_in_json() {
        let bad = vec![
            r##"{"naughty\u0000Key": "1st val"}"##,
            r##"{"naughty\\\u0000Key": "2nd val"}"##,
            r##"{"ok\\u0000Key": "val\\\u0000"}"##,
        ];
        for example in bad {
            let rv = serde_json::value::RawValue::from_string(example.to_string()).unwrap();
            let Err(error) = to_raw_value(Some(&rv), |x| x) else {
                panic!("expected error for example: {example} but was success");
            };
            assert!(error
                .to_string()
                .contains("a string in the spec contains a disallowed unicode null escape"));
        }

        let good = vec![
            r##"{"ok\\u0000Key": "ok val\\\\u0000"}"##,
            r##"{"ok\u0051Key": "ok val\u0072"}"##,
        ];
        for example in good {
            if let Err(error) = to_raw_value(Some(&example), |x| x) {
                panic!("expected success for example: {example}, but got error: {error:?}");
            }
        }
    }
}

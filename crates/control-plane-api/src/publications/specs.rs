use super::{LockFailure, UncommittedBuild};
use crate::draft;
use crate::publications::db::{self, LiveRevision, LiveSpecUpdate};
use anyhow::Context;
use itertools::Itertools;
use models::Capability;
use models::{Id, ModelDef, SourceType, TargetNaming, split_image_tag};
use serde_json::value::RawValue;
use sqlx::types::Uuid;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use tables::{BuiltRow, DraftRow, utils};

pub async fn persist_updates(
    uncommitted: &UncommittedBuild,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<Vec<LockFailure>> {
    let UncommittedBuild {
        publication_id,
        build_id,
        output,
        user_id,
        detail,
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
    catalog_type: models::CatalogType,
    built: &B,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<()> {
    db::delete_stale_flow(built.control_id().into(), catalog_type, txn).await?;

    let Some(model) = built.model() else {
        return Ok(());
    };

    let reads_from = model.reads_from();
    let writes_to = model.writes_to();
    let source_capture = model.materialization_source_capture_name();

    db::insert_live_spec_flows(
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
        update_live_spec_flows(&r.catalog_name(), models::CatalogType::Capture, r, txn)
            .await
            .with_context(|| format!("updating live_spec_flows for '{}'", r.catalog_name()))?;
    }
    for r in build
        .built
        .built_collections
        .iter()
        .filter(|r| !r.is_passthrough())
    {
        update_live_spec_flows(&r.catalog_name(), models::CatalogType::Collection, r, txn)
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
            models::CatalogType::Materialization,
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
        update_live_spec_flows(&r.catalog_name(), models::CatalogType::Test, r, txn)
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
    let mut spec_types: Vec<models::CatalogType> = Vec::with_capacity(n_specs);
    let mut models = Vec::with_capacity(n_specs);
    let mut built_specs = Vec::with_capacity(n_specs);
    let mut expect_build_ids: Vec<models::Id> = Vec::with_capacity(n_specs);
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
        spec_types.push(models::CatalogType::Capture);
        models.push(to_raw_value(r.model(), crate::TextJson)?);
        built_specs.push(to_raw_value(r.spec(), crate::TextJson)?);
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
        spec_types.push(models::CatalogType::Collection);
        models.push(to_raw_value(r.model(), crate::TextJson)?);
        built_specs.push(to_raw_value(r.spec(), crate::TextJson)?);
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
        spec_types.push(models::CatalogType::Materialization);
        models.push(to_raw_value(r.model(), crate::TextJson)?);
        built_specs.push(to_raw_value(r.spec(), crate::TextJson)?);
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
        spec_types.push(models::CatalogType::Test);
        models.push(to_raw_value(r.model(), crate::TextJson)?);
        built_specs.push(to_raw_value(r.spec(), crate::TextJson)?);
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

    let updates = db::update_live_specs(
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

pub async fn check_connector_annotations(
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

        // Skip materializations that have neither sourceCapture nor targetNaming.
        if model.source.is_none() && model.target_naming.is_none() {
            continue;
        }

        // We need the connector's resource config schema to validate x-schema-name support.
        // This requires a connector_tags row. Missing rows should only occur for test
        // connector tags, hence the technical error message.
        let Some(connector_spec) =
            crate::connector_tags::fetch_connector_spec(&image_name, &image_tag, pool).await?
        else {
            errors.insert(tables::Error {
                scope: tables::synthetic_scope(model.catalog_type(), materialization.catalog_name()),
                error: anyhow::anyhow!("materializations with a sourceCapture or targetNaming only work for known connector tags. {image} is not known to the control plane"),
            });
            continue;
        };
        let resource_config_schema = connector_spec.resource_config_schema;
        let resource_spec_pointers = utils::pointer_for_schema(resource_config_schema.0.get())?;

        // Blanket check: TargetNamingStrategy requires x-schema-name support.
        if model.target_naming.is_some() && resource_spec_pointers.x_schema_name.is_none() {
            errors.insert(tables::Error {
                scope: tables::synthetic_scope(model.catalog_type(), materialization.catalog_name()),
                error: anyhow::anyhow!("targetNaming requires the connector '{image_name}' to support x-schema-name in its resource config"),
            });
        }

        if let Some(SourceType::Configured(source_capture_def)) = &model.source {
            if source_capture_def.delta_updates && resource_spec_pointers.x_delta_updates.is_none()
            {
                errors.insert(tables::Error {
                    scope: tables::synthetic_scope(model.catalog_type(), materialization.catalog_name()),
                    error: anyhow::anyhow!("sourceCapture.deltaUpdates set but the connector '{image_name}' does not support delta updates"),
                });
            }

            // TODO(js): Remove this check once we finish the target naming migration
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
        let exists = crate::connector_tags::does_connector_exist(&image_name, pool).await?;
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
        let spec = to_raw_value(r.model(), crate::TextJson)?;
        db::insert_publication_spec(
            r.control_id().into(),
            publication_id.into(),
            build_detail(&r.model_fixes),
            &spec,
            &Some(models::CatalogType::Capture),
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
        let spec = to_raw_value(r.model(), crate::TextJson)?;
        db::insert_publication_spec(
            r.control_id().into(),
            publication_id.into(),
            build_detail(&r.model_fixes),
            &spec,
            &Some(models::CatalogType::Collection),
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
        let spec = to_raw_value(r.model(), crate::TextJson)?;
        db::insert_publication_spec(
            r.control_id().into(),
            publication_id.into(),
            build_detail(&r.model_fixes),
            &spec,
            &Some(models::CatalogType::Materialization),
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
        let spec = to_raw_value(r.model(), crate::TextJson)?;
        db::insert_publication_spec(
            r.control_id().into(),
            publication_id.into(),
            build_detail(&r.model_fixes),
            &spec,
            &Some(models::CatalogType::Test),
            user_id,
            txn,
        )
        .await
        .with_context(|| format!("inserting spec for '{}'", r.catalog_name()))?;
    }
    Ok(())
}

fn get_dependencies<M, F>(model: Option<&M>, get: F) -> Option<crate::TextJson<Vec<String>>>
where
    M: ModelDef,
    F: Fn(&M) -> BTreeSet<models::Collection>,
{
    model.map(|m| crate::TextJson(get(m).into_iter().map(Into::into).collect()))
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

    // Never lock or revision-check ops collections which are present only through
    // injection. `resolve_live_specs` injects ops.us-central1.v1/{logs,stats} into
    // *every* build so the runtime knows where to write telemetry; the publication does
    // not modify them and has no correctness dependency on their `last_pub_id`. Locking
    // them made every publication in the fleet take `FOR UPDATE` on the same two rows,
    // serializing all publishes globally (estuary/sre#54). They stay in the build; we
    // only skip locking them. An ops collection which is a genuine dependency of a
    // drafted spec (e.g. a reporting derivation reading ops stats) is still locked and
    // revision-checked, as is a drafted ops collection itself (via `update_live_specs`).
    let injected_ops = get_ops_collection_names();
    if expected.keys().any(|name| injected_ops.contains(*name)) {
        let drafted_deps = drafted_dependencies(output);
        expected.retain(|name, _| !injected_ops.contains(*name) || drafted_deps.contains(*name));
    }

    let catalog_names = expected.keys().map(|k| *k).collect::<Vec<_>>();
    let live_revisions = db::lock_live_specs(&catalog_names, txn).await?;

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
        errors.push(LockFailure {
            catalog_name: catalog_name.to_string(),
            actual: None,
            expected: expect_pub_id,
        });
    }
    Ok(errors)
}

/// Returns the names of all collections and captures which drafted specs of this build
/// read from or write to.
fn drafted_dependencies(output: &build::Output) -> BTreeSet<String> {
    let mut deps = BTreeSet::new();
    for model in output
        .built
        .built_captures
        .iter()
        .filter(|r| !r.is_passthrough())
        .filter_map(|r| r.model())
    {
        deps.extend(model.all_dependencies());
    }
    for model in output
        .built
        .built_collections
        .iter()
        .filter(|r| !r.is_passthrough())
        .filter_map(|r| r.model())
    {
        deps.extend(model.all_dependencies());
    }
    for model in output
        .built
        .built_materializations
        .iter()
        .filter(|r| !r.is_passthrough())
        .filter_map(|r| r.model())
    {
        deps.extend(model.all_dependencies());
    }
    for model in output
        .built
        .built_tests
        .iter()
        .filter(|r| !r.is_passthrough())
        .filter_map(|r| r.model())
    {
        deps.extend(model.all_dependencies());
    }
    deps
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
///
/// NOTE: `resolve_live_specs` (which injects these collections into every build) and
/// `verify_unchanged_revisions` (which skips locking of injected collections at commit)
/// must agree on this set. If this lookup becomes data-plane-dependent, both call sites
/// need the same data-plane context, or the commit-time skip will diverge from what was
/// actually injected at build time.
pub fn get_ops_collection_names() -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    names.insert("ops.us-central1.v1/logs".to_string());
    names.insert("ops.us-central1.v1/stats".to_string());
    names
}

/// Builds the retryable `AuthorizationSnapshotStale` error returned when an
/// authorization denial was evaluated against a snapshot that isn't yet
/// authoritative for the operation being denied.
fn authz_snapshot_stale(catalog_name: &str) -> anyhow::Error {
    validation::Error::AuthorizationSnapshotStale {
        catalog_name: catalog_name.to_string(),
    }
    .into()
}

/// Resolves the live specs which a draft drafts or references, authorizing each
/// against `snapshot`.
///
/// `started` is the instant the publication was queued, and decides whether an
/// authorization denial is terminal or merely not-yet-observed: a denial is
/// authoritative only once `snapshot` was taken after it. It must therefore be
/// durable across retries — a value re-stamped per attempt (`now()`) can never
/// be overtaken by a snapshot, so denials would retry forever.
///
/// `None` is for callers with no such durable instant: controllers and ad-hoc
/// system publications, which construct a fresh publication per attempt and
/// carry their own retry/backoff. They fall back to anchoring on each denied
/// spec's own last publication, which bounds the window in which grants could
/// have been committed alongside the spec.
pub async fn resolve_live_specs(
    user_id: uuid::Uuid,
    draft: &tables::DraftCatalog,
    db: &sqlx::PgPool,
    verify_user_authz: bool,
    explicit_plane_name: Option<&str>,
    snapshot: &crate::Snapshot,
    started: Option<tokens::DateTime>,
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

    let rows = crate::live_specs::fetch_live_specs(&all_spec_names, db)
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

        // An authorization denial may be spurious — a grant committed
        // concurrently that this snapshot hasn't observed yet. When that's
        // possible we short-circuit with a retryable stale error so the
        // publication is retried against a fresher snapshot, rather than
        // reporting a hard (and possibly wrong) authorization failure.
        //
        // The reference instant is `started`, the moment the operation was
        // queued: a grant committed before then is necessarily reflected in any
        // snapshot taken after then, however old the denied spec happens to be.
        // Anchoring on the spec instead would be unsound in both directions —
        // a snapshot postdating an old spec still can't rule out a grant
        // committed just before the request. This is the same test
        // `envelope.rs` and `authorize_task.rs` apply to decide whether a
        // denial is terminal or provisional.
        //
        // `started` must be durable across attempts for the retry to converge;
        // see `resolve_live_specs`' contract for callers which have no such
        // instant and fall back to the spec's own publication time.
        //
        // `taken_after` (rather than a bare comparison) is deliberate: it is the
        // single definition of "this snapshot is authoritative for that instant"
        // used across the control plane, and it allows for `TEMPORAL_SKEW`
        // between the snapshot's clock and the ID generator's.
        let spec_stale = match started {
            Some(started) => !snapshot.taken_after(started),
            None => !snapshot.taken_after(spec_row.last_pub_id.timestamp()),
        };

        if drafted_names.contains(catalog_name) {
            // Get the metadata about the draft spec that matches this catalog name.
            // This must exist in `draft`, otherwise `spec_meta` will panic.
            let (catalog_type, reads_from, writes_to) = spec_meta(draft, catalog_name);
            let scope = tables::synthetic_scope(catalog_type, catalog_name);

            // If the spec is included in the draft, then the user must have admin capability to it.
            if verify_user_authz
                && !tables::UserGrant::is_authorized(
                    &snapshot.role_grants,
                    &snapshot.user_grants,
                    user_id,
                    &spec_row.catalog_name,
                    models::Capability::Admin,
                )
            {
                if spec_stale {
                    return Err(authz_snapshot_stale(catalog_name));
                }
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
                if !tables::RoleGrant::is_authorized(
                    &snapshot.role_grants,
                    &spec_row.catalog_name,
                    &source,
                    Capability::Read,
                ) {
                    if spec_stale {
                        return Err(authz_snapshot_stale(catalog_name));
                    }
                    live.errors.push(tables::Error {
                        scope: scope.clone(),
                        error: anyhow::anyhow!(
                            "Specification '{catalog_name}' is not read-authorized to '{source}'.\nAvailable grants are: {}",
                            serde_json::to_string_pretty(&snapshot.spec_capabilities(&spec_row.catalog_name)).unwrap(),
                        ),
                    });
                }
            }
            for target in writes_to {
                if !tables::RoleGrant::is_authorized(
                    &snapshot.role_grants,
                    &spec_row.catalog_name,
                    &target,
                    Capability::Write,
                ) {
                    if spec_stale {
                        return Err(authz_snapshot_stale(catalog_name));
                    }
                    live.errors.push(tables::Error {
                        scope: scope.clone(),
                        error: anyhow::anyhow!(
                            "Specification is not write-authorized to '{target}'.\nAvailable grants are: {}",
                            serde_json::to_string_pretty(&snapshot.spec_capabilities(&spec_row.catalog_name)).unwrap(),
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
                && !tables::UserGrant::is_authorized(
                    &snapshot.role_grants,
                    &snapshot.user_grants,
                    user_id,
                    &spec_row.catalog_name,
                    Capability::Read,
                )
            {
                if spec_stale {
                    return Err(authz_snapshot_stale(catalog_name));
                }
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

    let storage_rows = db::resolve_storage_mappings(tenant_names, db).await?;
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
            data_planes: store.data_planes,
        });
    }

    // Fetch data planes that are referenced by live specs (`data_plane_ids`),
    // or by storage mappings (`data_plane_names`), or by `explicit_plane_name`.
    let data_plane_names: Vec<&str> = live
        .storage_mappings
        .iter()
        .flat_map(|m| m.data_planes.iter().map(String::as_str))
        .chain(explicit_plane_name.into_iter())
        .sorted()
        .dedup()
        .collect();

    let data_plane_names: Vec<&str> = data_plane_names
        .into_iter()
        .filter(|name| {
            tables::UserGrant::is_authorized(
                &snapshot.role_grants,
                &snapshot.user_grants,
                user_id,
                *name,
                models::Capability::Read,
            )
        })
        .collect();

    data_plane_ids.sort();
    data_plane_ids.dedup();

    live.data_planes = sqlx::query_as!(
        tables::DataPlane,
        r#"
        WITH
        data_plane_ids AS (
            SELECT id
            FROM UNNEST($1::flowid[]) AS t(id)
        ),
        data_plane_names AS (
            -- Names are pre-filtered to those the user is read-authorized to,
            -- so no in-SQL authorization check is needed here.
            SELECT name
            FROM UNNEST($2::text[]) AS t(name)
        )
        SELECT
            d.id AS "control_id: Id",
            d.data_plane_name,
            d.closed,
            d.hmac_keys,
            d.encrypted_hmac_keys AS "encrypted_hmac_keys: models::RawValue",
            d.data_plane_fqdn,
            d.broker_address,
            d.reactor_address,
            d.dekaf_address,
            d.dekaf_registry_address,
            d.ops_logs_name AS "ops_logs_name: models::Collection",
            d.ops_stats_name AS "ops_stats_name: models::Collection"
        FROM data_planes d
        WHERE
            d.id IN (select id from data_plane_ids) OR
            d.data_plane_name in (select name from data_plane_names)
        "#,
        &data_plane_ids as &[Id],
        &data_plane_names as &[&str],
    )
    .fetch_all(db)
    .await?
    .into_iter()
    .collect();

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
    let rows = crate::live_specs::fetch_inferred_schemas(&collection_names, db).await?;
    for row in rows {
        let crate::live_specs::InferredSchemaRow {
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
    draft_id: models::Id,
    build_output: &tables::Validations,
    db: &sqlx::PgPool,
) -> Result<(), sqlx::Error> {
    // Possible optimization, which I'm not doing right now: collect vecs of all the
    // prepared statement parameters and update all draft specs in a single query.
    for collection in build_output.built_collections.iter() {
        if !collection.is_delete() {
            draft::add_built_spec(
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
            draft::add_built_spec(
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
            draft::add_built_spec(
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
            draft::add_built_spec(
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
            assert!(
                error
                    .to_string()
                    .contains("a string in the spec contains a disallowed unicode null escape")
            );
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

/// `resolve_live_specs` makes four independent authorization decisions per row —
/// the drafter must admin a drafted spec; a drafted spec must itself be
/// read-authorized to each source and write-authorized to each target; and the
/// user must be able to read any *referenced* spec. Each of those denials is now
/// evaluated against a `Snapshot`, and each short-circuits with a retryable
/// `AuthorizationSnapshotStale` when that Snapshot predates the spec it denies.
///
/// These tests pin both halves of every branch: what a stale Snapshot returns,
/// and the (unchanged) error text an authoritative one reports.
#[cfg(test)]
mod resolve_tests {
    use super::*;

    // From `fixtures/authz_specs.sql`.
    const CAROL: uuid::Uuid = uuid::uuid!("33333333-3333-3333-3333-333333333333");
    const DAN: uuid::Uuid = uuid::uuid!("44444444-4444-4444-4444-444444444444");
    // From `fixtures/attenuated_grants.sql`.
    const ERIN: uuid::Uuid = uuid::uuid!("55555555-5555-5555-5555-555555555555");
    const FRANK: uuid::Uuid = uuid::uuid!("66666666-6666-6666-6666-666666666666");
    const COLLECTION: &str = "carolCo/data/foo";
    const CAPTURE: &str = "carolCo/in/capture-foo";
    const MATERIALIZATION: &str = "carolCo/out/materialize-bar";
    const PLANE: &str = "ops/dp/public/aws-us-west-2-c1";

    fn draft_of(catalog_json: serde_json::Value) -> tables::DraftCatalog {
        let catalog: models::Catalog =
            serde_json::from_value(catalog_json).expect("failed to parse catalog");
        tables::DraftCatalog::from(catalog)
    }

    /// A materialization drafted under `carolCo/out/`, which holds no grants and
    /// so is not read-authorized to `sources`.
    fn materialization_draft(sources: &[&str]) -> tables::DraftCatalog {
        draft_of(serde_json::json!({
            "materializations": {
                MATERIALIZATION: {
                    "endpoint": { "connector": { "image": "materialize/test:test", "config": {} } },
                    "bindings": sources.iter().map(|source| serde_json::json!({
                        "resource": { "table": "t" },
                        "source": source,
                    })).collect::<Vec<_>>(),
                }
            }
        }))
    }

    /// A capture drafted under `carolCo/in/`, which may write to `carolCo/data/`
    /// but nowhere else.
    fn capture_draft(targets: &[&str]) -> tables::DraftCatalog {
        draft_of(serde_json::json!({
            "captures": {
                CAPTURE: {
                    "endpoint": { "connector": { "image": "source/test:test", "config": {} } },
                    "bindings": targets.iter().map(|target| serde_json::json!({
                        "resource": { "id": "r" },
                        "target": target,
                    })).collect::<Vec<_>>(),
                }
            }
        }))
    }

    /// Staleness compares the Snapshot's `taken` against the timestamp embedded
    /// in a spec's `last_pub_id`, so read that back rather than recomputing it —
    /// `flowid` is `macaddr8`, which silently widens short literals.
    async fn published_at(pool: &sqlx::PgPool) -> tokens::DateTime {
        sqlx::query_scalar!(
            r#"select last_pub_id as "last_pub_id: models::Id"
            from live_specs where catalog_name = $1"#,
            COLLECTION,
        )
        .fetch_one(pool)
        .await
        .expect("fixture collection should exist")
        .timestamp()
    }

    async fn snapshot_offset(pool: &sqlx::PgPool, offset: chrono::TimeDelta) -> crate::Snapshot {
        let mut decrypted_hmac_keys = std::collections::HashMap::new();
        let data = crate::snapshot::try_fetch(pool, &mut decrypted_hmac_keys)
            .await
            .expect("failed to fetch snapshot");
        crate::Snapshot::new(published_at(pool).await + offset, data)
    }

    /// Taken clear of the publication plus `TEMPORAL_SKEW`: denials are definitive.
    async fn authoritative(pool: &sqlx::PgPool) -> crate::Snapshot {
        snapshot_offset(pool, crate::Snapshot::TEMPORAL_SKEW * 4).await
    }

    /// Taken before the publication it would judge: denials are retryable.
    async fn stale(pool: &sqlx::PgPool) -> crate::Snapshot {
        snapshot_offset(pool, -crate::Snapshot::TEMPORAL_SKEW * 4).await
    }

    /// Renders `live.errors` as `(scope, message)` pairs for snapshot assertions.
    fn error_pairs(live: &tables::LiveCatalog) -> Vec<(String, String)> {
        live.errors
            .iter()
            .map(|e| (e.scope.to_string(), format!("{:#}", e.error)))
            .collect()
    }

    fn assert_stale_for(err: anyhow::Error, catalog_name: &str) {
        assert!(
            validation::is_authz_snapshot_stale(&err),
            "expected a retryable stale-snapshot error, got: {err:#}"
        );
        assert!(
            err.to_string().contains(catalog_name),
            "stale error should name the offending spec, got: {err:#}"
        );
    }

    /// Branch 1: a user drafting an existing spec must admin it. Dan does not,
    /// but the denial is only definitive once the Snapshot outlives the spec.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "authz_specs"))
    )]
    async fn test_drafted_spec_requires_admin(pool: sqlx::PgPool) {
        let draft = draft_of(serde_json::json!({
            "collections": {
                COLLECTION: {
                    "schema": { "type": "object", "properties": { "id": { "type": "string" } } },
                    "key": ["/id"]
                }
            }
        }));

        let err = resolve_live_specs(DAN, &draft, &pool, true, None, &stale(&pool).await, None)
            .await
            .expect_err("a denial against a stale Snapshot should be retryable");
        assert_stale_for(err, COLLECTION);

        let live = resolve_live_specs(
            DAN,
            &draft,
            &pool,
            true,
            None,
            &authoritative(&pool).await,
            None,
        )
        .await
        .expect("an authoritative denial is reported, not raised");
        insta::assert_debug_snapshot!(error_pairs(&live), @r#"
        [
            (
                "flow://collection/carolCo/data/foo",
                "User is not authorized to create or change this catalog name",
            ),
        ]
        "#);
    }

    /// Branch 2: a drafted spec must itself be read-authorized to each source.
    /// Carol admins the whole tenant, so the user check passes and only the
    /// *spec's* own role grants are at issue.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "authz_specs"))
    )]
    async fn test_drafted_spec_reads_from_authz(pool: sqlx::PgPool) {
        let draft = materialization_draft(&[COLLECTION]);

        let err = resolve_live_specs(CAROL, &draft, &pool, true, None, &stale(&pool).await, None)
            .await
            .expect_err("a denial against a stale Snapshot should be retryable");
        assert_stale_for(err, MATERIALIZATION);

        let live = resolve_live_specs(
            CAROL,
            &draft,
            &pool,
            true,
            None,
            &authoritative(&pool).await,
            None,
        )
        .await
        .expect("an authoritative denial is reported, not raised");
        // The rendered grant list comes from `Snapshot::spec_capabilities`, which
        // replaced a SQL-computed column; pin it so the two can't drift.
        insta::assert_debug_snapshot!(error_pairs(&live), @r#"
        [
            (
                "flow://materialization/carolCo/out/materialize-bar",
                "Specification 'carolCo/out/materialize-bar' is not read-authorized to 'carolCo/data/foo'.\nAvailable grants are: [\n  {\n    \"subject_role\": \"carolCo/\",\n    \"object_role\": \"ops/dp/public/\",\n    \"capability\": \"read\",\n    \"bundles\": []\n  }\n]",
            ),
        ]
        "#);
    }

    /// Branch 3: a drafted spec must be write-authorized to each target.
    /// `carolCo/in/` may write to `carolCo/data/` but nowhere else.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "authz_specs"))
    )]
    async fn test_drafted_spec_writes_to_authz(pool: sqlx::PgPool) {
        let draft = capture_draft(&["carolCo/elsewhere/thing"]);

        let err = resolve_live_specs(CAROL, &draft, &pool, true, None, &stale(&pool).await, None)
            .await
            .expect_err("a denial against a stale Snapshot should be retryable");
        assert_stale_for(err, CAPTURE);

        let live = resolve_live_specs(
            CAROL,
            &draft,
            &pool,
            true,
            None,
            &authoritative(&pool).await,
            None,
        )
        .await
        .expect("an authoritative denial is reported, not raised");
        insta::assert_debug_snapshot!(error_pairs(&live), @r#"
        [
            (
                "flow://capture/carolCo/in/capture-foo",
                "Specification is not write-authorized to 'carolCo/elsewhere/thing'.\nAvailable grants are: [\n  {\n    \"subject_role\": \"carolCo/\",\n    \"object_role\": \"ops/dp/public/\",\n    \"capability\": \"read\",\n    \"bundles\": []\n  },\n  {\n    \"subject_role\": \"carolCo/in/\",\n    \"object_role\": \"carolCo/data/\",\n    \"capability\": \"write\",\n    \"bundles\": []\n  }\n]",
            ),
        ]
        "#);
    }

    /// The write-authorized target resolves cleanly, confirming the branch above
    /// fails for the reason claimed rather than incidentally.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "authz_specs"))
    )]
    async fn test_authorized_draft_resolves_without_errors(pool: sqlx::PgPool) {
        let draft = capture_draft(&[COLLECTION]);

        for snapshot in [stale(&pool).await, authoritative(&pool).await] {
            let live = resolve_live_specs(CAROL, &draft, &pool, true, Some(PLANE), &snapshot, None)
                .await
                .expect("an authorized draft resolves");

            assert!(
                live.errors.is_empty(),
                "unexpected errors: {:?}",
                error_pairs(&live)
            );
            assert_eq!(1, live.captures.len());
            assert_eq!(1, live.collections.len());
            assert_eq!(
                vec![PLANE],
                live.data_planes
                    .iter()
                    .map(|d| d.data_plane_name.as_str())
                    .collect::<Vec<_>>(),
            );
        }
    }

    /// Branch 4: a *referenced* (non-drafted) spec only requires read. Dan admins
    /// `danCo/`, so his own drafted spec passes, and the denial lands on
    /// `carolCo/data/foo` — which, being an existing spec, can be stale.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "authz_specs"))
    )]
    async fn test_referenced_spec_requires_read(pool: sqlx::PgPool) {
        let draft = draft_of(serde_json::json!({
            "materializations": {
                "danCo/materialize-x": {
                    "endpoint": { "connector": { "image": "materialize/test:test", "config": {} } },
                    "bindings": [ { "resource": { "table": "t" }, "source": COLLECTION } ],
                }
            }
        }));

        let err = resolve_live_specs(DAN, &draft, &pool, true, None, &stale(&pool).await, None)
            .await
            .expect_err("a denial against a stale Snapshot should be retryable");
        assert_stale_for(err, COLLECTION);

        let live = resolve_live_specs(
            DAN,
            &draft,
            &pool,
            true,
            None,
            &authoritative(&pool).await,
            None,
        )
        .await
        .expect("an authoritative denial is reported, not raised");
        insta::assert_debug_snapshot!(error_pairs(&live), @r#"
        [
            (
                "flow://unauthorized/carolCo/data/foo",
                "User is not authorized to read this catalog name",
            ),
            (
                "flow://materialization/danCo/materialize-x",
                "Specification 'danCo/materialize-x' is not read-authorized to 'carolCo/data/foo'.\nAvailable grants are: []",
            ),
        ]
        "#);
    }

    /// A brand-new spec has no `last_pub_id`, so nothing about it can be stale:
    /// its denial is definitive even against the oldest possible Snapshot. This
    /// keeps a first publication from looping instead of reporting its error.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "authz_specs"))
    )]
    async fn test_new_spec_denial_is_never_stale(pool: sqlx::PgPool) {
        let draft = draft_of(serde_json::json!({
            "collections": {
                "carolCo/data/brand-new": {
                    "schema": { "type": "object", "properties": { "id": { "type": "string" } } },
                    "key": ["/id"]
                }
            }
        }));

        let live = resolve_live_specs(DAN, &draft, &pool, true, None, &stale(&pool).await, None)
            .await
            .expect("a spec with no publication history cannot be stale");
        insta::assert_debug_snapshot!(error_pairs(&live), @r#"
        [
            (
                "flow://collection/carolCo/data/brand-new",
                "User is not authorized to create or change this catalog name",
            ),
        ]
        "#);
    }

    /// The spec-level (`reads_from` / `writes_to`) checks run even when user
    /// authorization is skipped, which is how controller and other system
    /// publications are built. They therefore inherit the retryable error too —
    /// worth pinning, because those callers have no reschedule handling of their
    /// own and will surface it as a failed publication.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "authz_specs"))
    )]
    async fn test_spec_authz_staleness_applies_without_user_authz(pool: sqlx::PgPool) {
        let draft = capture_draft(&["carolCo/elsewhere/thing"]);

        let err = resolve_live_specs(
            uuid::Uuid::nil(),
            &draft,
            &pool,
            false, // verify_user_authz
            None,
            &stale(&pool).await,
            None,
        )
        .await
        .expect_err("spec authorization is checked regardless of verify_user_authz");
        assert_stale_for(err, CAPTURE);
    }

    /// The data-plane name filter is the one snapshot-backed authorization check
    /// here with *no* staleness gate: an unauthorized (or not-yet-granted) plane
    /// is silently dropped rather than retried. Pinned as current behavior so a
    /// future change to it is a deliberate one.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "authz_specs"))
    )]
    async fn test_unauthorized_data_plane_name_is_silently_dropped(pool: sqlx::PgPool) {
        let draft = draft_of(serde_json::json!({
            "collections": {
                "danCo/thing": {
                    "schema": { "type": "object", "properties": { "id": { "type": "string" } } },
                    "key": ["/id"]
                }
            }
        }));

        // Dan admins `danCo/` but was granted nothing on `ops/dp/public/`.
        let live = resolve_live_specs(
            DAN,
            &draft,
            &pool,
            true,
            Some(PLANE),
            &stale(&pool).await,
            None,
        )
        .await
        .expect("an unauthorized data-plane name is not an error");
        assert!(
            live.errors.is_empty(),
            "unexpected errors: {:?}",
            error_pairs(&live)
        );
        assert!(
            live.data_planes.is_empty(),
            "an unauthorized data-plane should be dropped, not retried"
        );

        // Carol holds `carolCo/ -> ops/dp/public/ read`, so the same plane resolves.
        let carol_draft = draft_of(serde_json::json!({
            "collections": {
                "carolCo/thing": {
                    "schema": { "type": "object", "properties": { "id": { "type": "string" } } },
                    "key": ["/id"]
                }
            }
        }));
        let live = resolve_live_specs(
            CAROL,
            &carol_draft,
            &pool,
            true,
            Some(PLANE),
            &stale(&pool).await,
            None,
        )
        .await
        .expect("carol is authorized to the plane");
        assert_eq!(1, live.data_planes.len());
    }

    /// The data-plane name filter must be decided by *effective* (attenuated)
    /// authority, not the raw legacy capability of the edge which reached the
    /// prefix. Erin and frank traverse the identical 2-hop path through
    /// `sharedCo/` to a raw-`admin` grant on `ops/dp/public/`; only frank's
    /// root grant delegates the Viewer bits, so only frank sees the plane. A
    /// regression to raw-capability filtering makes the plane visible to erin.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../fixtures",
            scripts("data_planes", "authz_specs", "attenuated_grants")
        )
    )]
    async fn test_attenuated_data_plane_grant_is_not_visible(pool: sqlx::PgPool) {
        let snapshot = authoritative(&pool).await;

        // The premise that makes this attenuation rather than simple absence:
        // erin's raw reachable capability at the plane is Admin, and yet her
        // effective authority does not satisfy Read.
        assert_eq!(
            Some(models::Capability::Admin),
            tables::UserGrant::get_user_capability(
                &snapshot.role_grants,
                &snapshot.user_grants,
                ERIN,
                PLANE,
            ),
        );
        assert!(!tables::UserGrant::is_authorized(
            &snapshot.role_grants,
            &snapshot.user_grants,
            ERIN,
            PLANE,
            models::Capability::Read,
        ));

        let erin_draft = draft_of(serde_json::json!({
            "collections": {
                "erinCo/thing": {
                    "schema": { "type": "object", "properties": { "id": { "type": "string" } } },
                    "key": ["/id"]
                }
            }
        }));
        let live = resolve_live_specs(ERIN, &erin_draft, &pool, true, Some(PLANE), &snapshot, None)
            .await
            .expect("an unauthorized data-plane name is not an error");
        assert!(
            live.errors.is_empty(),
            "unexpected errors: {:?}",
            error_pairs(&live)
        );
        assert!(
            live.data_planes.is_empty(),
            "a plane reached with raw admin but attenuated effective authority must not be visible"
        );

        let frank_draft = draft_of(serde_json::json!({
            "collections": {
                "frankCo/thing": {
                    "schema": { "type": "object", "properties": { "id": { "type": "string" } } },
                    "key": ["/id"]
                }
            }
        }));
        let live = resolve_live_specs(
            FRANK,
            &frank_draft,
            &pool,
            true,
            Some(PLANE),
            &snapshot,
            None,
        )
        .await
        .expect("frank is authorized to the plane");
        assert!(
            live.errors.is_empty(),
            "unexpected errors: {:?}",
            error_pairs(&live)
        );
        assert_eq!(1, live.data_planes.len());
    }

    /// Scenario 2: Request-relative staleness anchoring allows retries when the
    /// snapshot predates the request, even if the spec is old. This is the
    /// "old-spec late-grant" case: a grant might exist but arrive in the system
    /// after the snapshot was taken but before the request was queued.
    ///
    /// This test shows that with request-relative anchoring, a denial is:
    /// - Retried if snapshot.taken_before(request_start) (grant might exist but not in snapshot)
    /// - Terminal if snapshot.taken_after(request_start) (grant would be in snapshot if it existed)
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("authz_specs"))
    )]
    async fn test_old_spec_stale_snapshot_relative_to_request(pool: sqlx::PgPool) {
        let draft = capture_draft(&[CAPTURE]);

        // A snapshot taken well before "now" is stale relative to any request
        // queued around "now". This should trigger a retry even though the spec
        // itself is old.
        let stale_snapshot = stale(&pool).await;
        let now = published_at(&pool).await + chrono::TimeDelta::seconds(3600);

        let err = resolve_live_specs(
            uuid::Uuid::nil(),
            &draft,
            &pool,
            false,
            None,
            &stale_snapshot,
            // Request was queued at `now`, well after the stale snapshot.
            Some(now),
        )
        .await
        .expect_err("spec authorization required even without user authz");

        // The denial should be stale relative to the request time, so retryable.
        assert_stale_for(err, CAPTURE);
    }

    /// When the snapshot is authoritative relative to the request start time,
    /// an authorization denial is terminal (not retried), even for an old spec.
    /// This shows the request-relative anchor is properly applied.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("authz_specs"))
    )]
    async fn test_old_spec_authoritative_snapshot_relative_to_request(pool: sqlx::PgPool) {
        let draft = capture_draft(&[CAPTURE]);

        // An authoritative snapshot is taken well after "now", so it's always
        // authoritative regardless of request start time.
        let authoritative_snapshot = authoritative(&pool).await;
        let now = published_at(&pool).await + chrono::TimeDelta::seconds(3600);

        let live = resolve_live_specs(
            uuid::Uuid::nil(),
            &draft,
            &pool,
            false,
            None,
            &authoritative_snapshot,
            // Request was queued at `now`, before the authoritative snapshot.
            Some(now),
        )
        .await
        .expect("resolve should not error with authoritative snapshot");

        // The denial should be terminal (not stale) because the snapshot is
        // authoritative relative to the request start time. The capture spec
        // lacks authorization, so we get a hard error, not a retry.
        assert!(!live.errors.is_empty(), "expected authorization denial");
        assert!(
            !validation::is_authz_snapshot_stale(
                &live.errors.iter().next().unwrap().error.as_ref().unwrap()
            ),
            "error should not be stale-snapshot error"
        );
    }

    /// When `started` is None (no durable request queue time), the staleness
    /// anchor falls back to the spec's own publication time. This is the
    /// fallback path for operations like controllers that don't have a
    /// queued row to anchor to.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("authz_specs"))
    )]
    async fn test_started_none_uses_spec_relative_anchor(pool: sqlx::PgPool) {
        let draft = capture_draft(&[CAPTURE]);

        // A snapshot taken before the spec's publication time.
        let stale_snapshot = stale(&pool).await;

        let err = resolve_live_specs(
            uuid::Uuid::nil(),
            &draft,
            &pool,
            false,
            None,
            &stale_snapshot,
            // No started time provided: should fall back to spec-relative anchoring.
            None,
        )
        .await
        .expect_err("spec authorization required");

        // Even with None, a truly stale snapshot (before spec) should be retried.
        assert_stale_for(err, CAPTURE);
    }
}

use futures::future::BoxFuture;
use itertools::{EitherOrBoth, Itertools};
use sources::Scope;
use std::collections::BTreeMap;

mod capture;
mod collection;
mod derivation;
mod errors;
mod indexed;
mod materialization;
mod noop;
mod reference;
mod schema;
mod storage_mapping;
mod test_step;

pub use errors::Error;
pub use noop::{NoOpConnectors, NoOpControlPlane};

/// Connectors is a delegated trait -- provided to validate -- through which
/// connector validation RPCs are dispatched. Request and Response must always
/// be Validate / Validated variants, but may include `internal` fields.
pub trait Connectors: Send + Sync {
    fn validate_capture<'a>(
        &'a self,
        request: proto_flow::capture::Request,
    ) -> BoxFuture<'a, anyhow::Result<proto_flow::capture::Response>>;

    fn validate_derivation<'a>(
        &'a self,
        request: proto_flow::derive::Request,
    ) -> BoxFuture<'a, anyhow::Result<proto_flow::derive::Response>>;

    fn validate_materialization<'a>(
        &'a self,
        request: proto_flow::materialize::Request,
    ) -> BoxFuture<'a, anyhow::Result<proto_flow::materialize::Response>>;
}

pub trait ControlPlane: Send + Sync {
    // Resolve a set of collection names into pre-built CollectionSpecs from
    // the control plane. Resolution may be fuzzy: if there is a spec that's
    // *close* to a provided name, it will be returned so that a suitable spelling
    // hint can be surfaced to the user. This implies we must account for possible
    // overlap with locally-built collections even if none were asked for.
    fn resolve_collections<'a>(
        &'a self,
        collections: Vec<models::Collection>,
    ) -> BoxFuture<'a, anyhow::Result<Vec<proto_flow::flow::CollectionSpec>>>;

    /// Retrieve the inferred schema of each of the given `collections`.
    /// Collections for which a schema is not found should be omitted from the response.
    fn get_inferred_schemas<'a>(
        &'a self,
        collections: Vec<models::Collection>,
    ) -> BoxFuture<'a, anyhow::Result<BTreeMap<models::Collection, models::Schema>>>;
}

pub async fn validate(
    build_id: &str,
    project_root: &url::Url,
    connectors: &dyn Connectors,
    control_plane: &dyn ControlPlane,
    captures: &[tables::Capture],
    collections: &[tables::Collection],
    fetches: &[tables::Fetch],
    imports: &[tables::Import],
    materializations: &[tables::Materialization],
    storage_mappings: &[tables::StorageMapping],
    tests: &[tables::Test],
) -> tables::Validations {
    let mut errors = tables::Errors::new();

    // Fetches order on fetch depth, so the first element is the root source.
    let root_scope = Scope::new(&fetches[0].resource);

    // At least one storage mapping is required iff this isn't a
    // build of a JSON schema.
    if storage_mappings.is_empty() && !collections.is_empty() {
        Error::NoStorageMappings {}.push(root_scope, &mut errors);
    }
    storage_mapping::walk_all_storage_mappings(storage_mappings, &mut errors);

    // Names of collection which use inferred schemas.
    let inferred_collections = reference::gather_inferred_collections(collections);
    // Names of collections which are referenced, but are not being validated themselves.
    let remote_collections =
        reference::gather_referenced_collections(captures, collections, materializations, tests);

    // Concurrently fetch referenced collections and inferred schemas from the control-plane.
    let (inferred_schemas, remote_collections) = match futures::try_join!(
        control_plane.get_inferred_schemas(inferred_collections),
        control_plane.resolve_collections(remote_collections),
        // TODO(johnny): Also fetch storage mappings here.
    ) {
        Ok(ok) => ok,
        Err(err) => {
            // If we failed to fetch from the control-plane then further validation
            // will generate lots of misleading errors, so fail now.
            Error::ControlPlane { detail: err }.push(root_scope, &mut errors);
            return tables::Validations {
                built_captures: tables::BuiltCaptures::new(),
                built_collections: tables::BuiltCollections::new(),
                built_materializations: tables::BuiltMaterializations::new(),
                built_tests: tables::BuiltTests::new(),
                errors,
            };
        }
    };

    let remote_collections = remote_collections
        .into_iter()
        .map(|mut spec| {
            tracing::debug!(collection=%spec.name, "resolved referenced remote collection");

            // Clear a derivation (if there is one), as we do not need it
            // when embedding a referenced collection.
            spec.derivation = None;

            tables::BuiltCollection {
                collection: models::Collection::new(&spec.name),
                scope: url::Url::parse("flow://control-plane").unwrap(),
                spec,
                validated: None,
            }
        })
        .collect::<tables::BuiltCollections>();

    if remote_collections.is_empty() {
        tracing::debug!("there were no remote collections to resolve");
    }

    // Build all local collections.
    let built_collections = collection::walk_all_collections(
        build_id,
        collections,
        &inferred_schemas,
        storage_mappings,
        &mut errors,
    );

    // If we failed to build one or more collections then further validation
    // will generate lots of misleading "not found" errors.
    if built_collections.len() != collections.len() {
        return tables::Validations {
            built_captures: tables::BuiltCaptures::new(),
            built_collections,
            built_materializations: tables::BuiltMaterializations::new(),
            built_tests: tables::BuiltTests::new(),
            errors,
        };
    }

    // Merge local and remote BuiltCollections. On conflict, keep the local one.
    let mut built_collections = built_collections
        .into_iter()
        .merge_join_by(remote_collections.into_iter(), |b, r| {
            b.collection.cmp(&r.collection)
        })
        .map(|eob| match eob {
            EitherOrBoth::Left(local) | EitherOrBoth::Both(local, _) => local,
            EitherOrBoth::Right(remote) => remote,
        })
        .collect::<tables::BuiltCollections>();

    let built_tests = test_step::walk_all_tests(&built_collections, tests, &mut errors);

    // Look for name collisions among all top-level catalog entities.
    // This is deliberately but arbitrarily ordered after granular
    // validations of collections, but before captures and materializations,
    // as a heuristic to report more useful errors before less useful errors.
    let collections_it = built_collections
        .iter()
        .map(|c| ("collection", c.collection.as_str(), Scope::new(&c.scope)));
    let captures_it = captures
        .iter()
        .map(|c| ("capture", c.capture.as_str(), Scope::new(&c.scope)));
    let materializations_it = materializations.iter().map(|m| {
        (
            "materialization",
            m.materialization.as_str(),
            Scope::new(&m.scope),
        )
    });
    let tests_it = tests
        .iter()
        .map(|t| ("test", t.test.as_str(), Scope::new(&t.scope)));

    indexed::walk_duplicates(
        captures_it
            .chain(collections_it)
            .chain(materializations_it)
            .chain(tests_it),
        &mut errors,
    );

    let built_captures = capture::walk_all_captures(
        build_id,
        &built_collections,
        captures,
        connectors,
        storage_mappings,
        &mut errors,
    );

    let mut derive_errors = tables::Errors::new();
    let built_derivations = derivation::walk_all_derivations(
        build_id,
        &built_collections,
        collections,
        connectors,
        imports,
        project_root,
        storage_mappings,
        &mut derive_errors,
    );

    let mut materialize_errors = tables::Errors::new();
    let built_materializations = materialization::walk_all_materializations(
        build_id,
        &built_collections,
        connectors,
        materializations,
        storage_mappings,
        &mut materialize_errors,
    );

    // Concurrently validate captures and materializations.
    let (built_captures, built_materializations, built_derivations) =
        futures::join!(built_captures, built_materializations, built_derivations);
    errors.extend(derive_errors.into_iter());
    errors.extend(materialize_errors.into_iter());

    for (built_index, validated, derivation) in built_derivations {
        let row = &mut built_collections[built_index];
        row.validated = Some(validated);
        row.spec.derivation = Some(derivation);
    }

    tables::Validations {
        built_captures,
        built_collections,
        built_materializations,
        built_tests,
        errors,
    }
}

// This pattern lets us cheaply detect if a read schema references the inferred
// of it's collection. Assuming an otherwise well-formed JSON schema,
// it can neither false-positive nor false-negative:
// * It must detect an actual property because a the same pattern within a JSON
//   string would be quote-escaped.
// * It must be a schema keyword ($ref cannot be, say, a property) because
//   "flow://inferred-schema" is not a valid JSON schema and would error at build time.
const REF_INFERRED_SCHEMA_PATTERN: &str = "\"$ref\":\"flow://inferred-schema\"";
// This pattern lets us cheaply detect if a read schema references the write
// schema of its collection.
const REF_WRITE_SCHEMA_PATTERN: &str = "\"$ref\":\"flow://write-schema\"";

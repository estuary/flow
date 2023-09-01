use futures::future::BoxFuture;
use itertools::{EitherOrBoth, Itertools};
use sources::Scope;

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
    // the control plane. Resolution is fuzzy: if there is a spec that's *close*
    // to a provided name, it will be returned so that a suitable spelling
    // hint can be surfaced to the user. This implies we must account for possible
    // overlap with locally-built collections even if none were asked for.
    fn resolve_collections<'a, 'b: 'a>(
        &'a self,
        collections: Vec<models::Collection>,
        // These parameters are currently required, but can be removed once we're
        // actually resolving fuzzy pre-built CollectionSpecs from the control plane.
        temp_build_id: &'b str,
        temp_storage_mappings: &'b [tables::StorageMapping],
    ) -> BoxFuture<'a, anyhow::Result<Vec<proto_flow::flow::CollectionSpec>>>;

    // TODO(johnny): this is a temporary helper which supports the transition
    // to the control-plane holding built specifications.
    fn temp_build_collection_helper(
        &self,
        name: String,
        spec: models::CollectionDef,
        build_id: &str,
        storage_mappings: &[tables::StorageMapping],
    ) -> anyhow::Result<proto_flow::flow::CollectionSpec> {
        let mut errors = tables::Errors::new();

        if let Some(built_collection) = collection::walk_collection(
            build_id,
            &tables::Collection {
                scope: url::Url::parse("flow://control-plane").unwrap(),
                collection: models::Collection::new(name),
                spec,
            },
            storage_mappings,
            &mut errors,
        ) {
            Ok(built_collection)
        } else {
            anyhow::bail!("unexpected failure building remote collection: {errors:?}")
        }
    }
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

    // Build all local collections.
    let built_collections =
        collection::walk_all_collections(build_id, collections, storage_mappings, &mut errors);

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

    // Next resolve all referenced collections which are not in local `collections`.
    let remote_collections = match control_plane
        .resolve_collections(
            reference::gather_referenced_collections(
                captures,
                collections,
                materializations,
                tests,
            ),
            build_id,
            storage_mappings,
        )
        .await
    {
        Err(err) => {
            // If we failed to complete the resolve operation then further validation
            // will generate lots of misleading "not found" errors. This is distinct
            // from collections not being found, which is communicated by their absence
            // and/or presence of nearly-matched names in the resolved set.
            Error::ResolveCollections { detail: err }.push(root_scope, &mut errors);
            return tables::Validations {
                built_captures: tables::BuiltCaptures::new(),
                built_collections,
                built_materializations: tables::BuiltMaterializations::new(),
                built_tests: tables::BuiltTests::new(),
                errors,
            };
        }
        Ok(c) => c
            .into_iter()
            .map(|spec| tables::BuiltCollection {
                collection: models::Collection::new(&spec.name),
                scope: url::Url::parse("flow://control-plane").unwrap(),
                spec,
                validated: None,
            })
            .collect::<tables::BuiltCollections>(),
    };
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

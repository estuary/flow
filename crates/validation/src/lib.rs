use futures::future::LocalBoxFuture;
use itertools::{EitherOrBoth, Itertools};

mod capture;
mod collection;
mod derivation;
mod errors;
mod indexed;
mod materialization;
mod npm_dependency;
mod reference;
mod schema;
mod storage_mapping;
mod test_step;
use errors::Error;

/// Drivers is a delegated trait -- provided to validate -- through which runtime
/// driver validation RPCs are dispatched.
pub trait Drivers {
    fn validate_materialization<'a>(
        &'a self,
        request: proto_flow::materialize::ValidateRequest,
    ) -> LocalBoxFuture<'a, Result<proto_flow::materialize::ValidateResponse, anyhow::Error>>;

    fn validate_capture<'a>(
        &'a self,
        request: proto_flow::capture::ValidateRequest,
    ) -> LocalBoxFuture<'a, Result<proto_flow::capture::ValidateResponse, anyhow::Error>>;
}

/// Tables produced by validate.
#[derive(Default, Debug)]
pub struct Tables {
    pub built_captures: tables::BuiltCaptures,
    pub built_collections: tables::BuiltCollections,
    pub built_derivations: tables::BuiltDerivations,
    pub built_materializations: tables::BuiltMaterializations,
    pub built_tests: tables::BuiltTests,
    pub errors: tables::Errors,
    pub implicit_projections: tables::Projections,
    pub inferences: tables::Inferences,
}

pub async fn validate<D: Drivers>(
    build_config: &proto_flow::flow::build_api::Config,
    drivers: &D,
    capture_bindings: &[tables::CaptureBinding],
    captures: &[tables::Capture],
    collections: &[tables::Collection],
    derivations: &[tables::Derivation],
    fetches: &[tables::Fetch],
    foreign_collections: tables::BuiltCollections,
    imports: &[tables::Import],
    materialization_bindings: &[tables::MaterializationBinding],
    materializations: &[tables::Materialization],
    named_schemas: &[tables::NamedSchema],
    npm_dependencies: &[tables::NPMDependency],
    projections: &[tables::Projection],
    resources: &[tables::Resource],
    schema_docs: &[tables::SchemaDoc],
    storage_mappings: &[tables::StorageMapping],
    test_steps: &[tables::TestStep],
    transforms: &[tables::Transform],
) -> Tables {
    let mut errors = tables::Errors::new();

    // Fetches order on the fetch depth, so take the first (lowest-depth)
    // element as the root scope.
    let mut root_scope = &url::Url::parse("root://").unwrap();
    if let Some(f) = fetches.first() {
        root_scope = &f.resource;
    }
    let root_scope = root_scope;

    let compiled_schemas = match tables::SchemaDoc::compile_all(schema_docs) {
        Ok(c) => c,
        Err(err) => {
            Error::from(err).push(root_scope, &mut errors);

            return Tables {
                errors,
                ..Default::default()
            };
        }
    };
    let schema_index = schema::index_compiled_schemas(&compiled_schemas, root_scope, &mut errors);

    // Filter from |foreign_collections| any entries that *exactly* match local
    // |collections|, as the local instances take precedence.
    let foreign_collections = foreign_collections
        .into_iter()
        .merge_join_by(collections.iter(), |l, r| l.collection.cmp(&r.collection))
        .filter_map(|eob| match eob {
            EitherOrBoth::Left(foreign) => Some(foreign),
            _ => None,
        })
        .collect::<tables::BuiltCollections>();

    let schema_refs = schema::Ref::from_tables(
        collections,
        derivations,
        &foreign_collections,
        named_schemas,
        projections,
        resources,
        root_scope,
        transforms,
    );

    let (schema_shapes, inferences) = schema::walk_all_schema_refs(
        imports,
        schema_docs,
        &schema_index,
        &schema_refs,
        &mut errors,
    );

    schema::walk_all_named_schemas(named_schemas, &mut errors);
    npm_dependency::walk_all_npm_dependencies(npm_dependencies, &mut errors);
    storage_mapping::walk_all_storage_mappings(storage_mappings, &mut errors);

    // At least one storage mapping is required iff this isn't a
    // build of a JSON schema.
    if storage_mappings.is_empty() && !collections.is_empty() {
        Error::NoStorageMappings {}.push(root_scope, &mut errors);
    }

    let (built_collections, implicit_projections) = collection::walk_all_collections(
        build_config,
        collections,
        imports,
        projections,
        &schema_shapes,
        storage_mappings,
        &mut errors,
    );

    // Merge locally-built collections with foreign definitions.
    let built_collections = built_collections
        .into_iter()
        .chain(foreign_collections.into_iter())
        .collect::<tables::BuiltCollections>();

    let built_derivations = derivation::walk_all_derivations(
        build_config,
        &built_collections,
        derivations,
        imports,
        &schema_index,
        &schema_shapes,
        storage_mappings,
        transforms,
        &mut errors,
    );

    let built_tests = test_step::walk_all_test_steps(
        &built_collections,
        imports,
        resources,
        &schema_index,
        &schema_shapes,
        test_steps,
        &mut errors,
    );

    // Look for name collisions among all top-level catalog entities.
    // This is deliberately but arbitrarily ordered after granular
    // validations of collections, but before captures and materializations,
    // as a heuristic to report more useful errors before less useful errors.
    let collections_it = built_collections
        .iter()
        .map(|c| ("collection", c.collection.as_str(), &c.scope));
    let captures_it = captures
        .iter()
        .map(|c| ("capture", c.capture.as_str(), &c.scope));
    let materializations_it = materializations
        .iter()
        .map(|m| ("materialization", m.materialization.as_str(), &m.scope));
    let tests_it = test_steps
        .iter()
        .filter_map(|t| (t.step_index == 0).then(|| ("test", t.test.as_str(), &t.scope)));

    indexed::walk_duplicates(
        captures_it
            .chain(collections_it)
            .chain(materializations_it)
            .chain(tests_it),
        &mut errors,
    );

    let built_captures = capture::walk_all_captures(
        build_config,
        drivers,
        &built_collections,
        capture_bindings,
        captures,
        imports,
        storage_mappings,
        &mut errors,
    );

    let mut tmp_errors = tables::Errors::new();
    let built_materializations = materialization::walk_all_materializations(
        build_config,
        drivers,
        &built_collections,
        imports,
        materialization_bindings,
        materializations,
        storage_mappings,
        &mut tmp_errors,
    );

    // Concurrently validate captures and materializations.
    let (built_captures, built_materializations) =
        futures::future::join(built_captures, built_materializations).await;
    errors.extend(tmp_errors.into_iter());

    Tables {
        built_captures,
        built_collections,
        built_derivations,
        built_materializations,
        built_tests,
        errors,
        implicit_projections,
        inferences,
    }
}

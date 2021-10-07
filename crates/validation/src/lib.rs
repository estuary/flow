use futures::future::LocalBoxFuture;
use models::tables;
use protocol;

mod capture;
mod collate;
mod collection;
mod derivation;
mod errors;
mod indexed;
mod journal_rule;
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
        request: protocol::materialize::ValidateRequest,
    ) -> LocalBoxFuture<'a, Result<protocol::materialize::ValidateResponse, anyhow::Error>>;

    fn validate_capture<'a>(
        &'a self,
        request: protocol::capture::ValidateRequest,
    ) -> LocalBoxFuture<'a, Result<protocol::capture::ValidateResponse, anyhow::Error>>;
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
    drivers: &D,
    capture_bindings: &[tables::CaptureBinding],
    captures: &[tables::Capture],
    collections: &[tables::Collection],
    derivations: &[tables::Derivation],
    fetches: &[tables::Fetch],
    imports: &[tables::Import],
    journal_rules: &[tables::JournalRule],
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

    let compiled_schemas = match tables::SchemaDoc::compile_all(schema_docs) {
        Ok(c) => c,
        Err(err) => {
            errors.insert_row(root_scope, anyhow::anyhow!(err));
            return Tables {
                errors,
                ..Default::default()
            };
        }
    };
    let schema_index = schema::index_compiled_schemas(&compiled_schemas, root_scope, &mut errors);

    let schema_refs = schema::Ref::from_tables(
        collections,
        derivations,
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
    journal_rule::walk_all_journal_rules(journal_rules, &mut errors);
    storage_mapping::walk_all_storage_mappings(storage_mappings, &mut errors);

    let (built_collections, implicit_projections) = collection::walk_all_collections(
        collections,
        imports,
        projections,
        &schema_shapes,
        storage_mappings,
        &mut errors,
    );

    let built_derivations = derivation::walk_all_derivations(
        &built_collections,
        collections,
        derivations,
        imports,
        projections,
        &schema_index,
        &schema_shapes,
        storage_mappings,
        transforms,
        &mut errors,
    );

    // Look for name collisions among all top-level catalog entities.
    // This is deliberately but arbitrarily ordered after granular
    // validations of collections, but before captures and materializations.
    let collections_it = collections
        .iter()
        .map(|c| ("collection", c.collection.as_str(), &c.scope));
    let captures_it = captures
        .iter()
        .map(|c| ("capture", c.capture.as_str(), &c.scope));
    let materializations_it = materializations
        .iter()
        .map(|m| ("materialization", m.materialization.as_str(), &m.scope));

    indexed::walk_duplicates(
        captures_it.chain(collections_it).chain(materializations_it),
        &mut errors,
    );

    let built_captures = capture::walk_all_captures(
        drivers,
        &built_collections,
        capture_bindings,
        captures,
        collections,
        derivations,
        imports,
        storage_mappings,
        &mut errors,
    );

    let mut tmp_errors = tables::Errors::new();
    let built_materializations = materialization::walk_all_materializations(
        drivers,
        &built_collections,
        collections,
        imports,
        materialization_bindings,
        materializations,
        projections,
        &schema_shapes,
        storage_mappings,
        &mut tmp_errors,
    );

    // Concurrently validate captures and materializations.
    let (built_captures, built_materializations) =
        futures::future::join(built_captures, built_materializations).await;
    errors.extend(tmp_errors.into_iter());

    let built_tests = test_step::walk_all_test_steps(
        collections,
        imports,
        projections,
        &schema_index,
        &schema_shapes,
        test_steps,
        &mut errors,
    );

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

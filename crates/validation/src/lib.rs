use futures::future::LocalBoxFuture;
use itertools::Itertools;
use models::tables;
use protocol::materialize;

mod capture;
mod collate;
mod collection;
mod derivation;
mod endpoint;
mod errors;
mod indexed;
mod journal_rule;
mod materialization;
mod npm_dependency;
mod reference;
mod schema;
mod test_step;
use errors::Error;

/// Drivers is a delegated trait -- provided to validate -- through which runtime
/// driver validation RPCs are dispatched.
pub trait Drivers {
    fn validate_materialization<'a>(
        &'a self,
        request: materialize::ValidateRequest,
    ) -> LocalBoxFuture<'a, Result<materialize::ValidateResponse, anyhow::Error>>;
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
    captures: &[tables::Capture],
    collections: &[tables::Collection],
    derivations: &[tables::Derivation],
    endpoints: &[tables::Endpoint],
    imports: &[tables::Import],
    journal_rules: &[tables::JournalRule],
    materializations: &[tables::Materialization],
    named_schemas: &[tables::NamedSchema],
    npm_dependencies: &[tables::NPMDependency],
    projections: &[tables::Projection],
    resources: &[tables::Resource],
    schema_docs: &[tables::SchemaDoc],
    _shard_rules: &[tables::ShardRule], // TODO.
    test_steps: &[tables::TestStep],
    transforms: &[tables::Transform],
) -> Tables {
    let mut errors = tables::Errors::new();
    let root_scope = &resources[0].resource;

    // Index for future binary searches of the import graph.
    let imports = imports
        .iter()
        .sorted_by_key(|i| (&i.from_resource, &i.to_resource))
        .collect::<Vec<_>>();

    let compiled_schemas = match tables::SchemaDoc::compile_all(schema_docs) {
        Ok(c) => c,
        Err(err) => {
            errors.push_row(root_scope, anyhow::anyhow!(err));
            return Tables {
                errors,
                ..Default::default()
            };
        }
    };
    let schema_index = schema::index_compiled_schemas(&compiled_schemas, root_scope, &mut errors);

    let schema_refs = schema::Ref::from_tables(named_schemas, collections, derivations, transforms);

    let (schema_shapes, inferences) =
        schema::walk_all_schema_refs(&schema_index, projections, &schema_refs, &mut errors);

    schema::walk_all_named_schemas(named_schemas, &mut errors);
    npm_dependency::walk_all_npm_dependencies(npm_dependencies, &mut errors);
    journal_rule::walk_all_journal_rules(journal_rules, &mut errors);

    let (built_collections, implicit_projections) =
        collection::walk_all_collections(collections, projections, &schema_shapes, &mut errors);

    let built_derivations = derivation::walk_all_derivations(
        &built_collections,
        collections,
        derivations,
        &imports,
        projections,
        &schema_index,
        &schema_shapes,
        transforms,
        &mut errors,
    );

    endpoint::walk_all_endpoints(endpoints, &mut errors);

    let built_captures = capture::walk_all_captures(
        &built_collections,
        captures,
        collections,
        derivations,
        endpoints,
        &imports,
        &mut errors,
    );

    let built_materializations = materialization::walk_all_materializations(
        drivers,
        &built_collections,
        collections,
        endpoints,
        &imports,
        materializations,
        &mut errors,
    )
    .await;

    let built_tests = test_step::walk_all_test_steps(
        collections,
        &imports,
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

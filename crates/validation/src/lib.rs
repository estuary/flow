use std::collections::{BTreeMap, BTreeSet, HashMap};

use futures::future::LocalBoxFuture;

mod capture;
mod collection;
mod derivation;
mod errors;
mod indexed;
mod materialization;
mod noop;
mod npm_dependency;
mod reference;
mod schema;
mod storage_mapping;
mod test_step;

pub use errors::Error;
pub use noop::NoOpDrivers;

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

pub async fn validate<D: Drivers>(
    build_config: &proto_flow::flow::build_api::Config,
    drivers: &D,
    capture_bindings: &[tables::CaptureBinding],
    captures: &[tables::Capture],
    collections: &[tables::Collection],
    derivations: &[tables::Derivation],
    fetches: &[tables::Fetch],
    imports: &[tables::Import],
    materialization_bindings: &[tables::MaterializationBinding],
    materializations: &[tables::Materialization],
    npm_dependencies: &[tables::NPMDependency],
    projections: &[tables::Projection],
    resources: &[tables::Resource],
    storage_mappings: &[tables::StorageMapping],
    test_steps: &[tables::TestStep],
    transforms: &[tables::Transform],
) -> tables::Validations {
    let mut errors = tables::Errors::new();

    // Fetches order on the fetch depth, so take the first (lowest-depth)
    // element as the root scope.
    let mut root_scope = &url::Url::parse("root://").unwrap();
    if let Some(f) = fetches.first() {
        root_scope = &f.resource;
    }
    let root_scope = root_scope;

    let compiled_schemas = match tables::Resource::compile_all_json_schemas(resources) {
        Ok(c) => c,
        Err(_) => {
            Error::SchemaBuild.push(root_scope, &mut errors);

            return tables::Validations {
                errors,
                ..Default::default()
            };
        }
    };

    let schema_refs = schema::Ref::from_tables(
        collections,
        derivations,
        projections,
        resources,
        root_scope,
        transforms,
    );

    let (schema_shapes, inferences) = schema::walk_all_schema_refs(
        &compiled_schemas,
        imports,
        resources,
        &schema_refs,
        &mut errors,
    );

    npm_dependency::walk_all_npm_dependencies(npm_dependencies, &mut errors);
    storage_mapping::walk_all_storage_mappings(storage_mappings, &mut errors);

    // At least one storage mapping is required iff this isn't a
    // build of a JSON schema.
    if storage_mappings.is_empty() && !collections.is_empty() {
        Error::NoStorageMappings {}.push(root_scope, &mut errors);
    }

    let built_collections = collection::walk_all_collections(
        build_config,
        collections,
        projections,
        &schema_shapes,
        storage_mappings,
        &mut errors,
    );

    let built_derivations = derivation::walk_all_derivations(
        build_config,
        &built_collections,
        derivations,
        &schema_shapes,
        storage_mappings,
        transforms,
        &mut errors,
    );

    let built_tests = test_step::walk_all_test_steps(
        &built_collections,
        resources,
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
        resources,
        storage_mappings,
        &mut errors,
    );

    let mut tmp_errors = tables::Errors::new();
    let built_materializations = materialization::walk_all_materializations(
        build_config,
        drivers,
        &built_collections,
        materialization_bindings,
        materializations,
        resources,
        storage_mappings,
        &mut tmp_errors,
    );

    // Concurrently validate captures and materializations.
    let (built_captures, built_materializations) =
        futures::future::join(built_captures, built_materializations).await;
    errors.extend(tmp_errors.into_iter());

    tables::Validations {
        built_captures,
        built_collections,
        built_derivations,
        built_materializations,
        built_tests,
        errors,
        inferences,
    }
}

pub fn validate_ports(
    scope: &url::Url,
    ports: &BTreeMap<models::PortName, models::PortSpec>,
    errors: &mut tables::Errors,
) {
    let mut used_numbers = BTreeSet::new();
    for (port_name, port_config) in ports {
        // TODO: maybe also validate that the port number isn't 0?

        indexed::walk_name(
            scope,
            "port",
            port_name.as_str(),
            models::PortName::regex(),
            errors,
        );
        if !used_numbers.insert(port_config.port) {
            Error::PortNumberCollision {
                port: port_config.port,
                port_name: port_name.to_string(),
            }
            .push(scope, errors);
        }
    }
}

/*
pub fn models_to_proto_ports(
    proto_ports: &BTreeMap<models::PortName, models::PortSpec>,
) -> HashMap<String, proto_flow::flow::PortSpec> {
    proto_ports
        .iter()
        .map(|(name, spec)| {
            (
                name.to_string(),
                proto_flow::flow::PortSpec {
                    container_port: spec.port as u32,
                    alpn_protocol: spec.alpn_protocol.clone().unwrap_or_default(),
                },
            )
        })
        .collect()
}
*/

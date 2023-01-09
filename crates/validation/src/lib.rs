use std::collections::BTreeMap;

use assemble::{PortConfig, PortMap};
use futures::future::LocalBoxFuture;

mod capture;
mod collection;
mod derivation;
mod errors;
mod images;
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
    fn inspect_image<'a>(
        &'a self,
        image: String,
    ) -> LocalBoxFuture<'a, Result<Vec<u8>, anyhow::Error>>;

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

    let image_inspections = images::walk_all_images(drivers, captures, materializations).await;

    let built_captures = capture::walk_all_captures(
        build_config,
        drivers,
        &image_inspections,
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
        &image_inspections,
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
        image_inspections,
        built_captures,
        built_collections,
        built_derivations,
        built_materializations,
        built_tests,
        errors,
        inferences,
    }
}

/// Parses the image inspection json into an `assemble::PortMap`, which can be used to create the shard template.
fn parse_image_inspection(
    image: &str,
    image_inspections: &[tables::ImageInspection],
) -> Result<PortMap, Error> {
    let row = image_inspections
        .iter()
        .find(|r| r.image == image)
        .ok_or_else(|| Error::ImageInspectFailed {
            image: image.to_owned(),
            error: anyhow::anyhow!("image inspection results missing"),
        })?;
    if let Some(err) = row.inspect_error.as_ref() {
        return Err(Error::ImageInspectFailed {
            image: image.to_owned(),
            error: anyhow::format_err!(err.clone()),
        });
    }

    let deserialized: Vec<InspectJson> =
        serde_json::from_slice(&row.inspect_output).map_err(|err| {
            let output_str = String::from_utf8_lossy(&row.inspect_output);
            eprintln!("deserializing docker inspect output failed: {}", output_str);
            Error::ImageInspectFailed {
                image: image.to_owned(),
                error: anyhow::Error::from(err),
            }
        })?;

    if deserialized.len() != 1 {
        return Err(Error::ImageInspectFailed {
            image: image.to_owned(),
            error: anyhow::anyhow!("expected 1 image, got {}", deserialized.len()),
        });
    }
    let mut ports = BTreeMap::new();
    for (port_config, _) in deserialized[0].config.exposed_ports.iter() {
        // We're unable to support UDP at this time.
        if port_config.ends_with("/udp") {
            continue;
        }
        // Technically, the ports are allowed to appear without the '/tcp' suffix, though
        // I haven't actually observed that in practice.
        let port_str = port_config.strip_suffix("/tcp").unwrap_or(port_config);
        let port_num = port_str.parse::<u16>().map_err(|_| {
            let error = anyhow::anyhow!("invalid port value in ExposedPorts: '{}'", port_config,);
            Error::ImageInspectFailed {
                image: image.to_string(),
                error,
            }
        })?;
        let mut config = PortConfig::default();
        let proto_key = format!("dev.estuary.port-proto.{port_num}");
        config.protocol = deserialized[0].config.labels.get(&proto_key).cloned();
        let public_key = format!("dev.estuary.port-public.{port_num}");
        if let Some(visibility) = deserialized[0].config.labels.get(&public_key) {
            config.public = visibility.parse::<bool>().map_err(|_| {
                let error = anyhow::anyhow!(
                    "invalid '{}' label value: '{}', must be either 'true' or 'false'",
                    public_key,
                    visibility
                );
                Error::ImageInspectFailed {
                    image: image.to_string(),
                    error,
                }
            })?;
        };
        ports.insert(port_num, config);
    }

    Ok(ports)
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct InspectConfig {
    /// According to the [OCI spec](https://github.com/opencontainers/image-spec/blob/d60099175f88c47cd379c4738d158884749ed235/config.md?plain=1#L125)
    /// `ExposedPorts` is a map where the keys are in the format `1234/tcp`, `456/udp`, or `789` (implicit default of tcp), and the values are
    /// empty objects. The choice of `serde_json::Value` here is meant to convey that the actual values are irrelevant.
    #[serde(default)]
    exposed_ports: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    labels: BTreeMap<String, String>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct InspectJson {
    config: InspectConfig,
}

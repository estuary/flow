use anyhow::Context;
use doc::{SchemaIndexBuilder, Shape};
use json::schema::{build::build_schema, types};
use models::{
    Capture, CaptureBinding, CaptureDef, CaptureEndpoint, Catalog, Collection, CollectionDef,
    CompositeKey, ConnectorConfig, JsonPointer, Schema, ShardTemplate,
};
use proto_flow::{
    capture::{request, Request},
    flow::capture_spec::ConnectorType,
};
use serde_json::{json, value::RawValue};
use std::collections::BTreeMap;
use url::Url;

use crate::connector::docker_run;
use crate::local_specs;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Discover {
    /// Connector image to discover
    image: String,

    /// Prefix
    #[clap(default_value_t = String::from("acmeCo"))]
    prefix: String,

    /// Should existing specs be over-written by specs from the Flow control plane?
    #[clap(long)]
    overwrite: bool,

    /// Should specs be written to the single specification file, or written in the canonical layout?
    #[clap(long)]
    flat: bool,
}

pub async fn do_discover(
    _ctx: &mut crate::CliContext,
    Discover {
        image,
        prefix,
        overwrite,
        flat,
    }: &Discover,
) -> anyhow::Result<()> {
    let connector_name = image
        .rsplit_once('/')
        .expect("image must include slashes")
        .1
        .split_once(':')
        .expect("image must include tag")
        .0;

    let catalog_file = format!("{connector_name}.flow.yaml");

    let target = build::arg_source_to_url(&catalog_file, true)?;
    let mut sources = local_specs::surface_errors(local_specs::load(&target).await.into_result())?;

    let capture_name = format!("{prefix}/{connector_name}");

    let cfg = sources
        .captures
        .first()
        .and_then(|c| match &c.spec.endpoint {
            CaptureEndpoint::Connector(conn) => Some(conn.config.clone()),
        });

    // If config file exists, try a Discover RPC with the config file
    if let Some(config) = cfg {
        let discover_output = docker_run(
            image,
            Request {
                discover: Some(request::Discover {
                    connector_type: ConnectorType::Image.into(),
                    config_json: config.to_string(),
                }),
                ..Default::default()
            },
        )
        .await
        .context("connector discover")?;

        let bindings = discover_output.discovered.unwrap().bindings;

        let mut capture_bindings: Vec<CaptureBinding> = Vec::with_capacity(bindings.len());
        let mut collections: BTreeMap<Collection, CollectionDef> = BTreeMap::new();

        // Create a catalog with the discovered bindings
        for binding in bindings.iter() {
            let collection_name = format!("{prefix}/{}", binding.recommended_name);
            let collection = Collection::new(collection_name);

            capture_bindings.push(CaptureBinding {
                target: collection.clone(),
                disable: false,
                resource: RawValue::from_string(binding.resource_config_json.clone())?.into(),
            });

            collections.insert(
                collection,
                CollectionDef {
                    schema: Some(Schema::new(
                        RawValue::from_string(binding.document_schema_json.clone())?.into(),
                    )),
                    write_schema: None,
                    read_schema: None,
                    key: CompositeKey::new(
                        binding
                            .key
                            .iter()
                            .map(JsonPointer::new)
                            .collect::<Vec<JsonPointer>>(),
                    ),
                    derive: None,
                    derivation: None,
                    projections: Default::default(),
                    journals: Default::default(),
                },
            );
        }

        let catalog = Catalog {
            captures: BTreeMap::from([(
                Capture::new(capture_name),
                CaptureDef {
                    auto_discover: None,
                    endpoint: CaptureEndpoint::Connector(ConnectorConfig {
                        image: image.to_string(),
                        config,
                    }),
                    bindings: capture_bindings,
                    interval: CaptureDef::default_interval(),
                    shards: ShardTemplate::default(),
                },
            )]),
            collections,
            ..Default::default()
        };

        let count = local_specs::extend_from_catalog(
            &mut sources,
            catalog,
            // We need to overwrite here to allow for bindings to be added to the capture
            local_specs::pick_policy(true, *flat),
        );

        local_specs::indirect_and_write_resources(sources)?;
        println!("Wrote {count} specifications under {target}.");
    } else {
        // Otherwise send a Spec RPC and use that to write a sample config file
        let spec_output = docker_run(
            image,
            Request {
                spec: Some(request::Spec {
                    connector_type: ConnectorType::Image.into(),
                    config_json: "{}".to_string(),
                }),
                ..Default::default()
            },
        )
        .await?;

        let config_schema_json = serde_json::from_str::<serde_json::Value>(
            &spec_output.spec.unwrap().config_schema_json,
        )?;

        // Run inference on the schema
        let curi = Url::parse("https://example/schema").unwrap();
        let schema_root =
            build_schema(curi, &config_schema_json).context("failed to build JSON schema")?;

        let mut index = SchemaIndexBuilder::new();
        index.add(&schema_root).unwrap();
        index.verify_references().unwrap();
        let index = index.into_index();
        let shape = Shape::infer(&schema_root, &index);

        // Create a stub config file
        let config = schema_to_sample_json(&shape)?;

        let catalog = Catalog {
            captures: BTreeMap::from([(
                Capture::new(capture_name),
                CaptureDef {
                    auto_discover: None,
                    endpoint: CaptureEndpoint::Connector(ConnectorConfig {
                        image: image.to_string(),
                        config: serde_json::from_value(config)?,
                    }),
                    bindings: Vec::new(),
                    interval: CaptureDef::default_interval(),
                    shards: ShardTemplate::default(),
                },
            )]),
            ..Default::default()
        };

        let count = local_specs::extend_from_catalog(
            &mut sources,
            catalog,
            local_specs::pick_policy(*overwrite, *flat),
        );
        local_specs::indirect_and_write_resources(sources)?;

        println!("Wrote {count} specifications under {target}.");
    }

    Ok(())
}

fn schema_to_sample_json(schema_shape: &Shape) -> Result<serde_json::Value, anyhow::Error> {
    let mut config = json!({});
    let locs = schema_shape.locations();

    for (p, _is_pattern, shape, _exists) in locs.iter() {
        let v = p
            .create_value(&mut config)
            .expect("structure must be valid");

        // If there is a default value for this location, use that
        if let Some(default_value) = &shape.default {
            *v = default_value.0.clone()
        }
        // Otherwise set a value depending on the type

        let value = if shape.type_.overlaps(types::STRING) {
            json!("")
        } else if shape.type_.overlaps(types::INTEGER) {
            json!(0)
        } else if shape.type_.overlaps(types::BOOLEAN) {
            json!(false)
        } else if shape.type_.overlaps(types::FRACTIONAL) {
            json!(0.0)
        } else if shape.type_.overlaps(types::ARRAY) {
            json!([])
        } else if shape.type_.overlaps(types::OBJECT) {
            json!({})
        } else {
            json!(null)
        };

        *v = value;
    }

    Ok(config)
}

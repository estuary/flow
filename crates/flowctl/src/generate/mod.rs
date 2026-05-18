use crate::local_specs;
use anyhow::Context;
use futures::{FutureExt, StreamExt};
use itertools::Itertools;
use proto_flow::{capture, derive, flow, materialize};

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Generate {
    /// Path or URL to a Flow specification file to generate development files for.
    #[clap(long)]
    source: String,
}

impl Generate {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        let source = build::arg_source_to_url(&self.source, false)?;
        let project_root = build::project_root(&source);

        let mut draft = local_specs::load(&source).await;
        sources::inline_draft_catalog(&mut draft);

        // Find config URLs we were unable to load and generate stubs for each.
        let (stubs, errors): (
            Vec<(url::Url, models::RawValue, doc::Shape)>,
            Vec<(url::Url, anyhow::Error)>,
        ) = generate_missing_configs(&mut draft)
            .await
            .partition_result();

        for (scope, error) in errors {
            draft.errors.insert_row(scope, error);
        }

        // TODO(johnny): We could render a nice table view of the _shape,
        // to provide live documentation for how to fill out each stubbed `url`.
        let files = stubs
            .into_iter()
            .map(|(url, dom, _shape)| {
                let content_raw = sources::Format::from_scope(&url).serialize(&dom);
                (url, content_raw)
            })
            .collect();

        build::write_files(&project_root, files)?;

        let () = local_specs::generate_files(&ctx.client, draft).await?;
        Ok(())
    }
}

// Generates stubs for all missing connector configuration files,
// returning tuples of:
// * The missing config file URL.
// * It's generated stub document.
// * It's schema Shape.
// Or, if generation fails, then return the error and its scope.
async fn generate_missing_configs(
    draft: &tables::DraftCatalog,
) -> impl Iterator<Item = Result<(url::Url, models::RawValue, doc::Shape), (url::Url, anyhow::Error)>>
{
    let tables::DraftCatalog {
        captures,
        collections,
        materializations,
        ..
    } = draft;

    let captures = captures.iter().map(|capture| {
        async move {
            match generate_missing_capture_configs(capture).await {
                Ok(ok) => Ok(ok),
                Err(error) => Err((capture.scope.clone(), error)),
            }
        }
        .boxed()
    });
    let collections = collections.iter().map(|collection| {
        async move {
            match generate_missing_collection_configs(collection).await {
                Ok(ok) => Ok(ok),
                Err(error) => Err((collection.scope.clone(), error)),
            }
        }
        .boxed()
    });
    let materializations = materializations.iter().map(|materialization| {
        async move {
            match generate_missing_materialization_configs(materialization).await {
                Ok(ok) => Ok(ok),
                Err(error) => Err((materialization.scope.clone(), error)),
            }
        }
        .boxed()
    });

    let results = captures
        .chain(collections)
        .chain(materializations)
        .collect::<futures::stream::FuturesUnordered<_>>()
        .collect::<Vec<_>>()
        .await;

    results.into_iter().flatten_ok()
}

async fn generate_missing_capture_configs(
    capture: &tables::DraftCapture,
) -> anyhow::Result<Vec<(url::Url, models::RawValue, doc::Shape)>> {
    let tables::DraftCapture {
        capture,
        model: Some(models::CaptureDef {
            endpoint, bindings, ..
        }),
        ..
    } = capture
    else {
        return Ok(Vec::new());
    };

    let (spec, missing_config_url) = match endpoint {
        models::CaptureEndpoint::Connector(config) => (
            capture::request::Spec {
                connector_type: flow::capture_spec::ConnectorType::Image as i32,
                config_json: serde_json::to_string(config).unwrap().into(),
            },
            serde_json::from_str::<url::Url>(config.config.get()).ok(),
        ),
        models::CaptureEndpoint::Local(config) => (
            capture::request::Spec {
                connector_type: flow::capture_spec::ConnectorType::Local as i32,
                config_json: serde_json::to_string(config).unwrap().into(),
            },
            serde_json::from_str::<url::Url>(config.config.get()).ok(),
        ),
    };
    let missing_resource_urls: Vec<(url::Url, models::Collection)> = bindings
        .iter()
        .filter_map(
            |models::CaptureBinding {
                 resource, target, ..
             }| {
                serde_json::from_str(resource.get())
                    .ok()
                    .map(|u| (u, target.clone()))
            },
        )
        .collect();

    if missing_config_url.is_none() && missing_resource_urls.is_empty() {
        return Ok(Vec::new()); // No need to spec the connector.
    }

    let capture::response::Spec {
        config_schema_json,
        resource_config_schema_json,
        ..
    } = runtime::Runtime::new(
        runtime::Plane::Local,
        String::new(), // Default network.
        ops::tracing_log_handler,
        None,
        format!("spec/{capture}"),
    )
    .unary_capture(capture::Request {
        spec: Some(spec),
        ..Default::default()
    })
    .await?
    .spec
    .context("connector didn't send expected Spec response")?;

    stub_missing_configs(
        &config_schema_json,
        &resource_config_schema_json,
        missing_config_url,
        missing_resource_urls,
    )
}

async fn generate_missing_collection_configs(
    collection: &tables::DraftCollection,
) -> anyhow::Result<Vec<(url::Url, models::RawValue, doc::Shape)>> {
    let tables::DraftCollection {
        collection,
        model: Some(models::CollectionDef { derive, .. }),
        ..
    } = collection
    else {
        return Ok(Vec::new());
    };

    let Some(models::Derivation {
        using, transforms, ..
    }) = derive
    else {
        return Ok(Vec::new()); // Not a derivation.
    };

    let (spec, missing_config_url) = match using {
        models::DeriveUsing::Connector(config) => (
            derive::request::Spec {
                connector_type: flow::collection_spec::derivation::ConnectorType::Image as i32,
                config_json: serde_json::to_string(config).unwrap().into(),
            },
            serde_json::from_str::<url::Url>(config.config.get()).ok(),
        ),
        models::DeriveUsing::Local(config) => (
            derive::request::Spec {
                connector_type: flow::collection_spec::derivation::ConnectorType::Local as i32,
                config_json: serde_json::to_string(config).unwrap().into(),
            },
            serde_json::from_str::<url::Url>(config.config.get()).ok(),
        ),
        // TypeScript, Python, and SQLite always generate their own configs.
        // Other connectors may as well, and they'll override those generated here.
        models::DeriveUsing::Sqlite(_)
        | models::DeriveUsing::Typescript(_)
        | models::DeriveUsing::Python(_) => {
            return Ok(Vec::new());
        }
    };
    let missing_resource_urls: Vec<(url::Url, models::Collection)> = transforms
        .iter()
        .filter_map(|models::TransformDef { lambda, source, .. }| {
            serde_json::from_str(lambda.get())
                .ok()
                .map(|u| (u, source.collection().clone()))
        })
        .collect();

    if missing_config_url.is_none() && missing_resource_urls.is_empty() {
        return Ok(Vec::new()); // No need to spec the connector.
    }

    let derive::response::Spec {
        config_schema_json,
        resource_config_schema_json,
        ..
    } = runtime::Runtime::new(
        runtime::Plane::Local,
        String::new(), // Default network.
        ops::tracing_log_handler,
        None,
        format!("spec/{collection}"),
    )
    .unary_derive(derive::Request {
        spec: Some(spec),
        ..Default::default()
    })
    .await?
    .spec
    .context("connector didn't send expected Spec response")?;

    stub_missing_configs(
        &config_schema_json,
        &resource_config_schema_json,
        missing_config_url,
        missing_resource_urls,
    )
}

async fn generate_missing_materialization_configs(
    materialization: &tables::DraftMaterialization,
) -> anyhow::Result<Vec<(url::Url, models::RawValue, doc::Shape)>> {
    let tables::DraftMaterialization {
        materialization,
        model:
            Some(models::MaterializationDef {
                endpoint,
                bindings,
                source,
                target_naming,
                ..
            }),
        ..
    } = materialization
    else {
        return Ok(Vec::new());
    };

    let (spec, missing_config_url) = match endpoint {
        models::MaterializationEndpoint::Connector(config) => (
            materialize::request::Spec {
                connector_type: flow::materialization_spec::ConnectorType::Image as i32,
                config_json: serde_json::to_string(config).unwrap().into(),
            },
            serde_json::from_str::<url::Url>(config.config.get()).ok(),
        ),
        models::MaterializationEndpoint::Local(config) => (
            materialize::request::Spec {
                connector_type: flow::materialization_spec::ConnectorType::Local as i32,
                config_json: serde_json::to_string(config).unwrap().into(),
            },
            serde_json::from_str::<url::Url>(config.config.get()).ok(),
        ),
        models::MaterializationEndpoint::Dekaf(config) => (
            materialize::request::Spec {
                connector_type: flow::materialization_spec::ConnectorType::Dekaf as i32,
                config_json: serde_json::to_string(config).unwrap().into(),
            },
            serde_json::from_str::<url::Url>(config.config.get()).ok(),
        ),
    };
    let missing_resource_urls: Vec<(url::Url, models::Collection)> = bindings
        .iter()
        .filter_map(
            |models::MaterializationBinding {
                 resource, source, ..
             }| {
                serde_json::from_str(resource.get())
                    .ok()
                    .map(|u| (u, source.collection().clone()))
            },
        )
        .collect();

    if missing_config_url.is_none() && missing_resource_urls.is_empty() {
        return Ok(Vec::new()); // No need to spec the connector.
    }

    let materialize::response::Spec {
        config_schema_json,
        resource_config_schema_json,
        ..
    } = runtime::Runtime::new(
        runtime::Plane::Local,
        String::new(), // Default network.
        ops::tracing_log_handler,
        None,
        format!("spec/{materialization}"),
    )
    .unary_materialize(materialize::Request {
        spec: Some(spec),
        ..Default::default()
    })
    .await?
    .spec
    .context("connector didn't send expected Spec response")?;

    let mut out = stub_missing_configs(
        &config_schema_json,
        &resource_config_schema_json,
        missing_config_url,
        Vec::new(), // Resource configs handled below with naming strategy.
    )?;

    if !missing_resource_urls.is_empty() {
        let resource_config_schema = std::str::from_utf8(&resource_config_schema_json)
            .context("resource config schema is not valid UTF-8")?;
        let resource_spec_pointers = tables::utils::pointer_for_schema(resource_config_schema)?;

        let build_shape = |schema: &[u8]| -> anyhow::Result<doc::Shape> {
            let schema = doc::validation::build_bundle(schema)?;
            let mut index = doc::SchemaIndexBuilder::new();
            index.add(&schema)?;
            index.verify_references()?;
            let index = index.into_index();
            Ok(doc::Shape::infer(&schema, &index))
        };
        let resource_config_shape = build_shape(&resource_config_schema_json)
            .context("connector sent invalid resource config schema")?;

        for (resource, collection) in missing_resource_urls {
            // Without either a naming strategy or a source capture, fall back
            // to stub_config's default annotation-driven behavior, which fills
            // x-collection-name and x-schema-name from the collection path.
            let stub = if target_naming.is_none() && source.is_none() {
                stub_config(&resource_config_shape, Some(&collection))
            } else {
                let mut stub = stub_config(&resource_config_shape, None);
                tables::utils::update_materialization_resource_spec(
                    target_naming.as_ref(),
                    source.as_ref(),
                    &mut stub,
                    &resource_spec_pointers,
                    collection.as_str(),
                )?;
                stub
            };
            out.push((
                resource,
                models::RawValue::from_value(&stub),
                resource_config_shape.clone(),
            ));
        }
    }

    Ok(out)
}

fn stub_missing_configs(
    config_schema_json: &[u8],
    resource_config_schema_json: &[u8],
    missing_config_url: Option<url::Url>,
    missing_resource_urls: Vec<(url::Url, models::Collection)>,
) -> anyhow::Result<Vec<(url::Url, models::RawValue, doc::Shape)>> {
    // Closure which builds a doc::Shape.
    let build_shape = |schema: &[u8]| -> anyhow::Result<doc::Shape> {
        let schema = doc::validation::build_bundle(schema)?;

        let mut index = doc::SchemaIndexBuilder::new();
        index.add(&schema)?;
        index.verify_references()?;
        let index = index.into_index();

        Ok(doc::Shape::infer(&schema, &index))
    };

    let config_shape =
        build_shape(&config_schema_json).context("connector sent invalid config schema")?;
    let resource_config_shape = build_shape(&resource_config_schema_json)
        .context("connector sent invalid resource config schema")?;

    let mut out = Vec::new();

    if let Some(resource) = missing_config_url {
        let content_dom = models::RawValue::from_value(&stub_config(&config_shape, None));
        out.push((resource, content_dom, config_shape));
    }

    for (resource, collection) in missing_resource_urls {
        let content_dom =
            models::RawValue::from_value(&stub_config(&resource_config_shape, Some(&collection)));
        out.push((resource, content_dom, resource_config_shape.clone()));
    }

    Ok(out)
}

fn stub_config(shape: &doc::Shape, collection: Option<&models::Collection>) -> serde_json::Value {
    use json::schema::types;
    use serde_json::json;

    if let Some(default) = &shape.default {
        default.0.clone()
    } else if let Some(variants) = &shape.enum_ {
        variants[0].clone()
    } else if shape.type_.overlaps(types::OBJECT) {
        let mut properties = serde_json::Map::new();

        for p in &shape.object.properties {
            if p.is_required || p.shape.annotations.get("x-schema-name").is_some() {
                properties.insert(p.name.to_string(), stub_config(&p.shape, collection));
            }
        }
        serde_json::Value::Object(properties)
    } else if shape.type_.overlaps(types::ARRAY) {
        let arr = &shape.array;
        let mut items = Vec::new();

        for (index, elem) in arr.tuple.iter().enumerate() {
            if index < arr.min_items as usize {
                items.push(stub_config(elem, collection));
            }
        }
        if let Some(p) = &arr.additional_items {
            if arr.tuple.len() < arr.min_items as usize {
                items.push(stub_config(p, collection))
            }
        }
        serde_json::Value::Array(items)
    } else if shape
        .annotations
        .get("x-collection-name")
        .is_some_and(|v| matches!(v, serde_json::Value::Bool(true)))
        && collection.is_some()
    {
        json!(
            collection
                .unwrap()
                .rsplit("/")
                .next()
                .expect("collection names always have a slash")
        )
    } else if shape
        .annotations
        .get("x-schema-name")
        .is_some_and(|v| matches!(v, serde_json::Value::Bool(true)))
        && collection.is_some()
    {
        json!(
            collection
                .unwrap()
                .rsplit("/")
                .nth(1)
                .expect("collection names always have a slash")
        )
    } else if shape.type_.overlaps(types::STRING) {
        json!("")
    } else if shape.type_.overlaps(types::INTEGER) {
        json!(0)
    } else if shape.type_.overlaps(types::BOOLEAN) {
        json!(false)
    } else if shape.type_.overlaps(types::FRACTIONAL) {
        json!(0.0)
    } else {
        json!(null)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    // Map a JSON schema, in YAML form, into a Shape.
    fn shape_from(schema_yaml: &str) -> doc::Shape {
        let url = url::Url::parse("http://example/schema").unwrap();
        let schema: serde_json::Value = serde_yaml::from_str(schema_yaml).unwrap();
        let schema = json::schema::build(&url, &schema).unwrap();

        let validator = doc::Validator::new(schema).unwrap();
        doc::Shape::infer(validator.schema(), validator.schema_index())
    }

    #[test]
    fn test_stub_config_resource_spec_pointers() {
        let obj = shape_from(
            r#"
        type: object
        properties:
            stream:
                type: string
                x-collection-name: true
            schema:
                type: string
                x-schema-name: true
        required:
            - stream
        "#,
        );

        let cfg = stub_config(
            &obj,
            Some(&models::Collection::new("my-tenant/my-task/my-collection")),
        );

        insta::assert_json_snapshot!(cfg);
    }

    /// Tests the two-step stub generation flow from generate_missing_materialization_configs:
    /// stub_config(&shape, None) produces a blank stub, then update_materialization_resource_spec
    /// populates x-schema-name and x-collection-name according to the TargetNamingStrategy.
    #[test]
    fn test_stub_config_with_target_naming_strategy() {
        let schema_json = r#"{
            "type": "object",
            "properties": {
                "table": { "type": "string", "x-collection-name": true },
                "schema": { "type": "string", "x-schema-name": true }
            },
            "required": ["table"]
        }"#;

        let shape = shape_from(
            r#"
        type: object
        properties:
            table:
                type: string
                x-collection-name: true
            schema:
                type: string
                x-schema-name: true
        required:
            - table
        "#,
        );

        let pointers = tables::utils::pointer_for_schema(schema_json).unwrap();

        // SingleSchema: x-schema-name comes from the strategy, x-collection-name
        // is the last collection component.
        let mut stub = stub_config(&shape, None);
        tables::utils::update_materialization_resource_spec(
            Some(&models::TargetNamingStrategy::SingleSchema {
                schema: "my_dataset".to_string(),
                table_template: None,
            }),
            None,
            &mut stub,
            &pointers,
            "tenant/task/my_table",
        )
        .unwrap();
        insta::assert_json_snapshot!(stub, @r#"
        {
          "schema": "my_dataset",
          "table": "my_table"
        }
        "#);

        // MatchSourceStructure: x-schema-name derived from 2nd-to-last collection component.
        let mut stub = stub_config(&shape, None);
        tables::utils::update_materialization_resource_spec(
            Some(&models::TargetNamingStrategy::MatchSourceStructure {
                table_template: None,
                schema_template: None,
            }),
            None,
            &mut stub,
            &pointers,
            "tenant/some_schema/my_table",
        )
        .unwrap();
        insta::assert_json_snapshot!(stub, @r#"
        {
          "schema": "some_schema",
          "table": "my_table"
        }
        "#);

        // PrefixTableNames: x-collection-name gets schema prefix, x-schema-name
        // is the strategy's schema.
        let mut stub = stub_config(&shape, None);
        tables::utils::update_materialization_resource_spec(
            Some(&models::TargetNamingStrategy::PrefixTableNames {
                schema: "default".to_string(),
                skip_common_defaults: true,
                table_template: None,
            }),
            None,
            &mut stub,
            &pointers,
            "tenant/custom_schema/my_table",
        )
        .unwrap();
        insta::assert_json_snapshot!(stub, @r#"
        {
          "schema": "default",
          "table": "custom_schema_my_table"
        }
        "#);

        // Legacy path: no target_naming, source capture with NoSchema.
        // x-schema-name left empty, x-collection-name is last component.
        let source = models::SourceType::Configured(models::SourceDef {
            capture: None,
            target_naming: models::TargetNaming::NoSchema,
            delta_updates: false,
            fields_recommended: Default::default(),
        });
        let mut stub = stub_config(&shape, None);
        tables::utils::update_materialization_resource_spec(
            None,
            Some(&source),
            &mut stub,
            &pointers,
            "tenant/task/my_table",
        )
        .unwrap();
        insta::assert_json_snapshot!(stub, @r#"
        {
          "schema": "",
          "table": "my_table"
        }
        "#);

        // No target_naming and no source capture: generate_missing_materialization_configs
        // falls back to annotation-driven stub_config, which fills x-collection-name
        // and x-schema-name from the collection path.
        let stub = stub_config(
            &shape,
            Some(&models::Collection::new("tenant/some_schema/my_table")),
        );
        insta::assert_json_snapshot!(stub, @r#"
        {
          "schema": "some_schema",
          "table": "my_table"
        }
        "#);
    }
}

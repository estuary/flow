use crate::{local_connector, local_specs};
use anyhow::Context;
use futures::{FutureExt, StreamExt};
use itertools::Itertools;
use proto_flow::{capture, derive, materialize};

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Generate {
    /// Path or URL to a Flow specification file to generate development files for.
    #[clap(long)]
    source: String,
    /// Name of the task to generate files for within the Flow specification file.
    /// Required if there are multiple tasks in --source specifications.
    #[clap(long)]
    task: Option<String>,
}

impl Generate {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        let source = build::arg_source_to_url(&self.source, false)?;

        let mut draft = local_specs::load(&source).await;
        sources::inline_draft_catalog(&mut draft);
        let task = local_specs::select_task(&mut draft, self.task.as_deref())?;
        let connector_router = ctx.local_connector_router();

        // Find config URLs we were unable to load and generate stubs for each.
        let (stubs, errors): (
            Vec<(url::Url, models::RawValue, doc::Shape)>,
            Vec<(url::Url, anyhow::Error)>,
        ) = generate_missing_configs(&mut draft, &task, &*connector_router)
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

        build::write_files(&source, files)?;

        let () = local_specs::generate_files(ctx, draft).await?;
        Ok(())
    }
}

// Generates stubs for all missing connector configuration files of `task`,
// returning tuples of:
// * The missing config file URL.
// * It's generated stub document.
// * It's schema Shape.
// Or, if generation fails, then return the error and its scope.
async fn generate_missing_configs(
    draft: &tables::DraftCatalog,
    task: &str,
    connector_router: &dyn proto_grpc::connector::Router,
) -> impl Iterator<Item = Result<(url::Url, models::RawValue, doc::Shape), (url::Url, anyhow::Error)>>
{
    let tables::DraftCatalog {
        captures,
        collections,
        materializations,
        ..
    } = draft;

    let captures = captures
        .iter()
        .filter(|c| c.capture.as_str() == task)
        .map(|capture| {
            async move {
                match generate_missing_capture_configs(capture, connector_router).await {
                    Ok(ok) => Ok(ok),
                    Err(error) => Err((capture.scope.clone(), error)),
                }
            }
            .boxed()
        });
    let collections = collections
        .iter()
        .filter(|c| c.collection.as_str() == task)
        .map(|collection| {
            async move {
                match generate_missing_collection_configs(collection, connector_router).await {
                    Ok(ok) => Ok(ok),
                    Err(error) => Err((collection.scope.clone(), error)),
                }
            }
            .boxed()
        });
    let materializations = materializations
        .iter()
        .filter(|m| m.materialization.as_str() == task)
        .map(|materialization| {
            async move {
                match generate_missing_materialization_configs(materialization, connector_router)
                    .await
                {
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
    connector_router: &dyn proto_grpc::connector::Router,
) -> anyhow::Result<Vec<(url::Url, models::RawValue, doc::Shape)>> {
    let tables::DraftCapture {
        model: Some(models::CaptureDef {
            endpoint, bindings, ..
        }),
        ..
    } = capture
    else {
        return Ok(Vec::new());
    };

    let config = match endpoint {
        models::CaptureEndpoint::Connector(config) => &config.config,
        models::CaptureEndpoint::Local(config) => &config.config,
    };
    let missing_config_url = missing_url(config);
    let spec = validation::capture_spec_request(endpoint);
    let missing_resource_urls: Vec<(url::Url, models::Collection)> = bindings
        .iter()
        .filter_map(
            |models::CaptureBinding {
                 resource, target, ..
             }| { missing_url(resource).map(|u| (u, target.clone())) },
        )
        .collect();

    if missing_config_url.is_none() && missing_resource_urls.is_empty() {
        return Ok(Vec::new()); // No need to spec the connector.
    }

    let spec = local_connector::spec_capture(connector_router, None, spec).await?;
    let capture::response::Spec {
        config_schema_json,
        resource_config_schema_json,
        ..
    } = spec;

    stub_missing_configs(
        &config_schema_json,
        &resource_config_schema_json,
        missing_config_url,
        missing_resource_urls,
    )
}

async fn generate_missing_collection_configs(
    collection: &tables::DraftCollection,
    connector_router: &dyn proto_grpc::connector::Router,
) -> anyhow::Result<Vec<(url::Url, models::RawValue, doc::Shape)>> {
    let tables::DraftCollection {
        model: Some(models::CollectionDef { derive, .. }),
        ..
    } = collection
    else {
        return Ok(Vec::new());
    };

    let Some(models::Derivation {
        using,
        transforms,
        shards,
        ..
    }) = derive
    else {
        return Ok(Vec::new()); // Not a derivation.
    };

    let missing_config_url = match using {
        models::DeriveUsing::Connector(config) => missing_url(&config.config),
        models::DeriveUsing::Local(config) => missing_url(&config.config),
        models::DeriveUsing::Python(models::DeriveUsingPython {
            module: None,
            config,
            ..
        }) => missing_url(config),
        // TypeScript, SQLite, and a legacy Python module generate their own configs.
        // Other connectors may as well, and they'll override those generated here.
        models::DeriveUsing::Sqlite(_)
        | models::DeriveUsing::Typescript(_)
        | models::DeriveUsing::Python(_) => {
            return Ok(Vec::new());
        }
    };
    let spec = validation::derive_spec_request(using, shards);
    let missing_resource_urls: Vec<(url::Url, models::Collection)> = transforms
        .iter()
        .filter_map(|models::TransformDef { lambda, source, .. }| {
            missing_url(lambda).map(|u| (u, source.collection().clone()))
        })
        .collect();

    if missing_config_url.is_none() && missing_resource_urls.is_empty() {
        return Ok(Vec::new()); // No need to spec the connector.
    }

    let spec = local_connector::spec_derive(connector_router, None, spec).await?;
    let derive::response::Spec {
        config_schema_json,
        resource_config_schema_json,
        ..
    } = spec;

    stub_missing_configs(
        &config_schema_json,
        &resource_config_schema_json,
        missing_config_url,
        missing_resource_urls,
    )
}

async fn generate_missing_materialization_configs(
    materialization: &tables::DraftMaterialization,
    connector_router: &dyn proto_grpc::connector::Router,
) -> anyhow::Result<Vec<(url::Url, models::RawValue, doc::Shape)>> {
    let tables::DraftMaterialization {
        model: Some(models::MaterializationDef {
            endpoint, bindings, ..
        }),
        ..
    } = materialization
    else {
        return Ok(Vec::new());
    };

    let config = match endpoint {
        models::MaterializationEndpoint::Connector(config) => &config.config,
        models::MaterializationEndpoint::Local(config) => &config.config,
        models::MaterializationEndpoint::Dekaf(config) => &config.config,
    };
    let missing_config_url = missing_url(config);
    let spec = validation::materialization_spec_request(endpoint);
    let missing_resource_urls: Vec<(url::Url, models::Collection)> = bindings
        .iter()
        .filter_map(
            |models::MaterializationBinding {
                 resource, source, ..
             }| { missing_url(resource).map(|u| (u, source.collection().clone())) },
        )
        .collect();

    if missing_config_url.is_none() && missing_resource_urls.is_empty() {
        return Ok(Vec::new()); // No need to spec the connector.
    }

    let spec = local_connector::spec_materialize(connector_router, None, spec).await?;
    let materialize::response::Spec {
        config_schema_json,
        resource_config_schema_json,
        ..
    } = spec;

    stub_missing_configs(
        &config_schema_json,
        &resource_config_schema_json,
        missing_config_url,
        missing_resource_urls,
    )
}

/// URL of a missing config: inlining leaves an import which couldn't be loaded
/// as the absolute URL it would have loaded. Only a value which `sources`
/// treats as an import qualifies, so other strings which happen to parse as
/// URLs aren't mistaken for files to write.
fn missing_url(config: &models::RawValue) -> Option<url::Url> {
    url::Url::parse(config.as_import_url()?).ok()
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
}

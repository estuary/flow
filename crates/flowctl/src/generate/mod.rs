use crate::local_specs;
use anyhow::Context;
use futures::{FutureExt, StreamExt};
use itertools::Itertools;
use proto_flow::{capture, derive, flow, materialize};
use std::str::FromStr;
use url::Url;

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

        let client = ctx.controlplane_client().await?;
        let () = local_specs::generate_files(client, draft).await?;
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
                config_json: serde_json::to_string(config).unwrap(),
            },
            serde_json::from_str::<url::Url>(config.config.get()).ok(),
        ),
        models::CaptureEndpoint::Local(config) => (
            capture::request::Spec {
                connector_type: flow::capture_spec::ConnectorType::Local as i32,
                config_json: serde_json::to_string(config).unwrap(),
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
        true,          // All local.
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
                config_json: serde_json::to_string(config).unwrap(),
            },
            serde_json::from_str::<url::Url>(config.config.get()).ok(),
        ),
        models::DeriveUsing::Local(config) => (
            derive::request::Spec {
                connector_type: flow::collection_spec::derivation::ConnectorType::Local as i32,
                config_json: serde_json::to_string(config).unwrap(),
            },
            serde_json::from_str::<url::Url>(config.config.get()).ok(),
        ),
        // TypeScript and SQLite always generate their own configs.
        // Other connectors may as well, and they'll override those generated here.
        models::DeriveUsing::Sqlite(_) | models::DeriveUsing::Typescript(_) => {
            return Ok(Vec::new())
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
        true,          // All local.
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
        model: Some(models::MaterializationDef {
            endpoint, bindings, ..
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
                config_json: serde_json::to_string(config).unwrap(),
            },
            serde_json::from_str::<url::Url>(config.config.get()).ok(),
        ),
        models::MaterializationEndpoint::Local(config) => (
            materialize::request::Spec {
                connector_type: flow::materialization_spec::ConnectorType::Local as i32,
                config_json: serde_json::to_string(config).unwrap(),
            },
            serde_json::from_str::<url::Url>(config.config.get()).ok(),
        ),
        models::MaterializationEndpoint::Dekaf(config) => (
            materialize::request::Spec {
                connector_type: flow::materialization_spec::ConnectorType::Dekaf as i32,
                config_json: serde_json::to_string(config).unwrap(),
            },
            match &config {
                models::DekafConfigContainer::Indirect(s) => Url::from_str(s.as_str()).ok(),
                _ => None,
            },
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
        true,          // All local.
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

    stub_missing_configs(
        &config_schema_json,
        &resource_config_schema_json,
        missing_config_url,
        missing_resource_urls,
    )
}

fn stub_missing_configs(
    config_schema_json: &str,
    resource_config_schema_json: &str,
    missing_config_url: Option<url::Url>,
    missing_resource_urls: Vec<(url::Url, models::Collection)>,
) -> anyhow::Result<Vec<(url::Url, models::RawValue, doc::Shape)>> {
    // Closure which builds a doc::Shape.
    let build_shape = |schema: &str| -> anyhow::Result<doc::Shape> {
        let schema = doc::validation::build_bundle(&schema)?;

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
            if p.is_required {
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
        json!(collection
            .unwrap()
            .rsplit("/")
            .next()
            .expect("collection names always have a slash"))
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

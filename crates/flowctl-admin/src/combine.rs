use anyhow::Context;
use doc::{combine, ptr::Pointer, SchemaIndex, SchemaIndexBuilder, Validator};
use futures::future::LocalBoxFuture;
use protocol::flow::{self, build_api};
use std::io;
use url::Url;

#[derive(Debug, clap::Args)]
pub struct CombineArgs {
    #[clap(flatten)]
    build_source: SchemaAndKeySource,
}

/// How to get the schema and key
#[derive(Debug, clap::Args)]
pub struct SchemaAndKeySource {
    /// Build directory
    #[clap(long, default_value = ".")]
    pub directory: String,
    /// Path or URL to a JSON schema, which may include reduction annotations.
    #[clap(long, conflicts_with_all(&["source", "collection"]), requires("key"))]
    pub schema: Option<String>,
    /// The key to reduce on, provided as JSON pointer(s). Can be provided multiple times for compound keys.
    #[clap(long, conflicts_with_all(&["source", "collection"]), requires("schema"))]
    pub key: Vec<String>,

    /// Catalog source file or URL to build
    #[clap(long, conflicts_with_all(&["schema", "key"]), requires("collection"))]
    pub source: Option<String>,
    /// The name of the collection within the given `--source` from which to take the schema and key
    #[clap(long, conflicts_with_all(&["schema", "key"]), requires("source"))]
    pub collection: Option<String>,
}

pub fn run(CombineArgs { build_source }: CombineArgs) -> Result<(), anyhow::Error> {
    let (index, schema_url, key_pointers) = get_indexed_schemas_and_key(build_source)?;

    let mut accumulator = combine::Accumulator::new(
        key_pointers.into(),
        schema_url,
        tempfile::tempfile().context("opening tempfile")?,
    )?;
    let mut validator = Validator::new(&index);

    let mut in_docs = 0usize;
    let mut in_bytes = 0usize;
    let mut out_docs = 0usize;
    // We don't track out_bytes because it's awkward to do so
    // and the user can trivially measure for themselves.

    for line in io::stdin().lines() {
        let line = line?;

        let memtable = accumulator.memtable()?;
        let rhs = doc::HeapNode::from_serde(
            &mut serde_json::Deserializer::from_str(&line),
            memtable.alloc(),
            memtable.dedup(),
        )?;

        in_docs += 1;
        in_bytes = line.len() + 1;
        memtable.combine_right(rhs, &mut validator)?;
    }

    let mut out = io::BufWriter::new(io::stdout().lock());

    let mut drainer = accumulator.into_drainer()?;
    assert_eq!(
        false,
        drainer.drain_while(&mut validator, |node, _fully_reduced| {
            serde_json::to_writer(&mut out, &node).context("writing document to stdout")?;
            out_docs += 1;
            Ok::<_, anyhow::Error>(true)
        })?,
        "implementation error: drain_while exited with remaining items to drain"
    );

    tracing::info!(
        input_docs = in_docs,
        input_bytes = in_bytes,
        output_docs = out_docs,
        "completed combine"
    );

    Ok(())
}

fn get_indexed_schemas_and_key(
    build_source: SchemaAndKeySource,
) -> Result<(SchemaIndex<'static>, Url, Vec<Pointer>), anyhow::Error> {
    let SchemaAndKeySource {
        directory,
        schema,
        key,
        source,
        collection,
    } = build_source;

    let (src, src_type) = if schema.is_none() {
        (source.clone().unwrap(), flow::ContentType::Catalog as i32)
    } else {
        (
            schema.clone().unwrap(),
            flow::ContentType::JsonSchema as i32,
        )
    };

    let build_config = build_api::Config {
        build_id: "flowctl-combine".to_string(),
        directory,
        source: src,
        source_type: src_type,
        typescript_generate: false,
        typescript_compile: false,
        typescript_package: false,
        connector_network: String::new(),
    };

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let fut = build::configured_build(build_config, Fetcher, NoOpDrivers);
    let output = rt.block_on(fut)?;

    if !output.errors.is_empty() {
        for err in output.errors.iter() {
            tracing::error!(scope = %err.scope, error = ?err.error, "catalog build error");
        }
        anyhow::bail!("catalog build failed");
    }

    let idx = build_schema_index(&output.resources)?;

    let (schema_url, key_pointers) = if schema.is_none() {
        let target_collection = collection.as_ref().unwrap();
        let resolved_collection = output
            .built_collections
            .into_iter()
            .find(|c| c.collection.as_str() == target_collection)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "could not find collection '{}' in catalog",
                    target_collection
                )
            })?;

        let keys = resolved_collection
            .spec
            .key_ptrs
            .iter()
            .map(|kp| Pointer::from_str(&kp))
            .collect::<Vec<_>>();
        let url = Url::parse(&resolved_collection.spec.schema_uri)?;

        (url, keys)
    } else {
        let url = build::source_to_url(schema.as_ref().unwrap())?;
        let keys = key
            .iter()
            .map(|kp| Pointer::from_str(kp))
            .collect::<Vec<_>>();
        (url, keys)
    };

    Ok((idx, schema_url, key_pointers))
}

fn build_schema_index(
    resources: &[tables::Resource],
) -> Result<SchemaIndex<'static>, anyhow::Error> {
    let mut index_builder = SchemaIndexBuilder::new();
    let all_compiled = tables::Resource::compile_all_json_schemas(resources)?;

    for (_, compiled) in all_compiled {
        let leaked = Box::leak(Box::new(compiled));
        index_builder.add(leaked)?;
    }
    Ok(index_builder.into_index())
}

pub struct NoOpDrivers;
impl validation::Drivers for NoOpDrivers {
    fn validate_materialization<'a>(
        &'a self,
        request: protocol::materialize::ValidateRequest,
    ) -> LocalBoxFuture<'a, Result<protocol::materialize::ValidateResponse, anyhow::Error>> {
        use protocol::materialize::{
            constraint::Type, validate_response::Binding, Constraint, ValidateResponse,
        };
        use std::collections::HashMap;

        Box::pin(async move {
            let response_bindings = request
                .bindings
                .into_iter()
                .enumerate()
                .map(|(i, b)| {
                    let resource_path = vec![format!("binding-{}", i)];
                    let constraints = b
                        .collection
                        .expect("collection must exist")
                        .projections
                        .into_iter()
                        .map(|proj| {
                            (
                                proj.field,
                                Constraint {
                                    r#type: Type::FieldOptional as i32,
                                    reason: "builds for flowctl-combine allow everything"
                                        .to_string(),
                                },
                            )
                        })
                        .collect::<HashMap<_, _>>();
                    Binding {
                        constraints,
                        resource_path,
                        delta_updates: true,
                    }
                })
                .collect::<Vec<_>>();
            Ok(ValidateResponse {
                bindings: response_bindings,
            })
        })
    }

    fn validate_capture<'a>(
        &'a self,
        request: protocol::capture::ValidateRequest,
    ) -> LocalBoxFuture<'a, Result<protocol::capture::ValidateResponse, anyhow::Error>> {
        use protocol::capture::{validate_response::Binding, ValidateResponse};
        Box::pin(async move {
            let bindings = request
                .bindings
                .into_iter()
                .enumerate()
                .map(|(i, _)| Binding {
                    resource_path: vec![format!("binding-{}", i)],
                })
                .collect::<Vec<_>>();
            Ok(ValidateResponse { bindings })
        })
    }
}

pub struct Fetcher;

impl sources::Fetcher for Fetcher {
    fn fetch<'a>(
        &'a self,
        // Resource to fetch.
        resource: &'a url::Url,
        // Expected content type of the resource.
        _content_type: flow::ContentType,
    ) -> sources::FetchFuture<'a> {
        tracing::debug!(url = %resource, "fetching resource");
        let url = resource.clone();
        Box::pin(fetch_async(url))
    }
}

async fn fetch_async(resource: Url) -> Result<bytes::Bytes, anyhow::Error> {
    match resource.scheme() {
        "http" | "https" => {
            let url = resource.to_string();
            let result = reqwest::get(url).await?;
            let bytes = result.bytes().await?;
            Ok(bytes)
        }
        "file" => {
            let path = resource
                .to_file_path()
                .map_err(|err| anyhow::anyhow!("failed to convert file uri to path: {:?}", err))?;

            let bytes = std::fs::read(path)?;
            Ok(bytes.into())
        }
        _ => Err(anyhow::anyhow!(
            "cannot fetch unsupported URI scheme: '{}'",
            resource
        )),
    }
}

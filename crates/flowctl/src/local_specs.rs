use anyhow::Context;
use futures::{FutureExt, StreamExt, TryStreamExt};
use proto_flow::{derive, flow};
use std::collections::BTreeMap;

pub(crate) async fn load_and_validate(
    client: crate::controlplane::Client,
    source: &str,
) -> anyhow::Result<(tables::Sources, tables::Validations)> {
    let source = arg_source_to_url(source, false)?;
    let sources = surface_errors(load(&source).await)?;
    let (sources, validations) = surface_errors(inline_and_validate(client, sources).await)?;
    write_generated_files(&sources, &validations)?;
    Ok((sources, validations))
}

// Map a "--source" argument to a corresponding URL, optionally creating an empty
// file if one doesn't exist, which is required when producing a canonical file:///
// URL for a local file.
pub(crate) fn arg_source_to_url(
    source: &str,
    create_if_not_exists: bool,
) -> anyhow::Result<url::Url> {
    // Special case that maps stdin into a URL constant.
    if source == "-" {
        return Ok(url::Url::parse(STDIN_URL).unwrap());
    }
    match url::Url::parse(source) {
        Ok(url) => Ok(url),
        Err(err) => {
            tracing::debug!(
                source = %source,
                ?err,
                "source is not a URL; assuming it's a filesystem path",
            );

            let source = match std::fs::canonicalize(source) {
                Ok(p) => p,
                Err(err)
                    if matches!(err.kind(), std::io::ErrorKind::NotFound)
                        && create_if_not_exists =>
                {
                    std::fs::write(source, "{}")
                        .with_context(|| format!("failed to create new file {source}"))?;
                    std::fs::canonicalize(source).expect("can canonicalize() a file we just wrote")
                }
                Err(err) => {
                    return Err(err)
                        .context(format!("could not find {source} in the local filesystem"));
                }
            };

            // Safe unwrap since we've canonical-ized the path.
            Ok(url::Url::from_file_path(&source).unwrap())
        }
    }
}

// Load all sources into tables.
// Errors are returned but not inspected.
// Loaded specifications are unmodified from their fetch representations.
pub(crate) async fn load(source: &url::Url) -> (tables::Sources, tables::Errors) {
    let loader = sources::Loader::new(tables::Sources::default(), Fetcher {});
    loader
        .load_resource(
            sources::Scope::new(&source),
            &source,
            flow::ContentType::Catalog,
        )
        .await;
    let mut sources = loader.into_tables();
    let errors = std::mem::take(&mut sources.errors);

    (sources, errors)
}

// Map sources into their inline form and validate them.
// Errors are returned but are not inspected.
pub(crate) async fn inline_and_validate(
    client: crate::controlplane::Client,
    mut sources: tables::Sources,
) -> ((tables::Sources, tables::Validations), tables::Errors) {
    ::sources::inline_sources(&mut sources);

    let source = &sources.fetches[0].resource;
    let project_root = project_root(source);

    let mut validations = validation::validate(
        &flow::build_api::Config {
            build_db: String::new(),
            build_id: "local-build".to_string(),
            connector_network: "default".to_string(),
            project_root: project_root.to_string(),
            source: source.to_string(),
            source_type: flow::ContentType::Catalog as i32,
        },
        &LocalConnectors(validation::NoOpDrivers {}),
        &Resolver { client },
        &sources.captures,
        &sources.collections,
        &sources.fetches,
        &sources.imports,
        &sources.materializations,
        &sources.storage_mappings,
        &sources.tests,
    )
    .await;

    // Local specs are not expected to satisfy all referential integrity checks.
    // Filter out errors which are not really "errors" for the Flow CLI.
    let errors = std::mem::take(&mut validations.errors)
        .into_iter()
        .filter(|err| match err.error.downcast_ref() {
            // Ok if a referenced collection doesn't exist
            // (it may within the control-plane).
            Some(
                validation::Error::NoSuchEntity { ref_entity, .. }
                | validation::Error::NoSuchEntitySuggest { ref_entity, .. },
            ) if *ref_entity == "collection" => false,
            // Ok if *no* storage mappings are defined.
            // If at least one mapping is defined, then we do require that all
            // collections have appropriate mappings.
            Some(validation::Error::NoStorageMappings { .. }) => false,
            // All other validation errors bubble up as top-level errors.
            _ => true,
        })
        .collect::<tables::Errors>();

    ((sources, validations), errors)
}

pub(crate) fn surface_errors<T>(result: (T, tables::Errors)) -> anyhow::Result<T> {
    let (t, errors) = result;

    for tables::Error { scope, error } in errors.iter() {
        tracing::error!(%scope, ?error);
    }
    if !errors.is_empty() {
        Err(anyhow::anyhow!("failed due to encountered errors"))
    } else {
        Ok(t)
    }
}

pub(crate) fn write_generated_files(
    sources: &tables::Sources,
    validations: &tables::Validations,
) -> anyhow::Result<()> {
    let source = &sources.fetches[0].resource;
    let project_root = project_root(source);

    // Gather and write generated files from successful connector validations.
    let mut generated_files = BTreeMap::new();
    for row in validations.built_collections.iter() {
        let Some(validated) = &row.validated else { continue };
        for (url, content) in &validated.generated_files {
            let url = url::Url::parse(&url)
                .context("derive connector returns invalid generated file URL")?;
            generated_files.insert(url, content.as_bytes());
        }
    }

    write_files(
        &project_root,
        generated_files
            .into_iter()
            .map(|(resource, content)| (resource, content.to_vec()))
            .collect(),
    )?;

    Ok(())
}

// Indirect specifications so that larger configurations, etc become reference
// resources, then write them out if they're under the project root.
pub(crate) fn indirect_and_write_resources(
    mut sources: tables::Sources,
) -> anyhow::Result<tables::Sources> {
    ::sources::indirect_large_files(&mut sources, 1 << 9);
    ::sources::rebuild_catalog_resources(&mut sources);

    let project_root = project_root(&sources.fetches[0].resource);

    write_files(
        &project_root,
        sources
            .resources
            .iter()
            .map(
                |tables::Resource {
                     resource, content, ..
                 }| (resource.clone(), content.to_vec()),
            )
            .collect(),
    )?;

    Ok(sources)
}

fn write_files(project_root: &url::Url, files: Vec<(url::Url, Vec<u8>)>) -> anyhow::Result<()> {
    for (resource, content) in files {
        let Ok(path) = resource.to_file_path() else {
            tracing::info!(%resource, "not writing the resource because it's remote and not local");
            continue;
        };
        if !resource.as_str().starts_with(project_root.as_str()) {
            tracing::info!(%resource, %project_root,
                "not writing local resource because it's not under the project root");
            continue;
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(path.parent().unwrap()).with_context(|| {
                format!("failed to create directory {}", parent.to_string_lossy())
            })?;
        }
        std::fs::write(&path, content).with_context(|| format!("failed to write {resource}"))?;

        tracing::info!(path=%path.to_str().unwrap_or(resource.as_str()), "wrote file");
    }
    Ok(())
}

pub(crate) fn into_catalog(sources: tables::Sources) -> models::Catalog {
    let tables::Sources {
        captures,
        collections,
        fetches: _,
        imports: _,
        materializations,
        resources: _,
        storage_mappings: _,
        tests,
        errors,
    } = sources;

    assert!(errors.is_empty());

    models::Catalog {
        _schema: None,
        import: Vec::new(), // Fully inline and requires no imports.
        captures: captures
            .into_iter()
            .map(|tables::Capture { capture, spec, .. }| (capture, spec))
            .collect(),
        collections: collections
            .into_iter()
            .map(
                |tables::Collection {
                     collection, spec, ..
                 }| (collection, spec),
            )
            .collect(),
        materializations: materializations
            .into_iter()
            .map(
                |tables::Materialization {
                     materialization,
                     spec,
                     ..
                 }| (materialization, spec),
            )
            .collect(),
        tests: tests
            .into_iter()
            .map(|tables::Test { test, spec, .. }| (test, spec))
            .collect(),

        // We deliberately omit storage mappings.
        // The control plane will inject these during its builds.
        storage_mappings: BTreeMap::new(),
    }
}

pub(crate) fn extend_from_catalog<P>(
    sources: &mut tables::Sources,
    catalog: models::Catalog,
    policy: P,
) -> usize
where
    P: Fn(&str, &url::Url, Option<&url::Url>) -> Vec<url::Url>,
{
    ::sources::merge::extend_from_catalog(sources, catalog, policy)
}

pub(crate) fn pick_policy(
    overwrite: bool,
    flat: bool,
) -> fn(&str, &url::Url, Option<&url::Url>) -> Vec<url::Url> {
    match (overwrite, flat) {
        (true, true) => ::sources::merge::flat_layout_replace,
        (true, false) => ::sources::merge::canonical_layout_replace,
        (false, true) => ::sources::merge::flat_layout_keep,
        (false, false) => ::sources::merge::canonical_layout_keep,
    }
}

struct LocalConnectors(validation::NoOpDrivers);

impl validation::Connectors for LocalConnectors {
    fn validate_capture<'a>(
        &'a self,
        request: proto_flow::capture::request::Validate,
    ) -> futures::future::LocalBoxFuture<
        'a,
        Result<proto_flow::capture::response::Validated, anyhow::Error>,
    > {
        self.0.validate_capture(request)
    }

    fn validate_derivation<'a>(
        &'a self,
        request: proto_flow::derive::request::Validate,
    ) -> futures::future::LocalBoxFuture<
        'a,
        Result<proto_flow::derive::response::Validated, anyhow::Error>,
    > {
        let middleware = runtime::derive::Middleware::new(ops::tracing_log_handler, None);

        async move {
            let request = derive::Request {
                validate: Some(request.clone()),
                ..Default::default()
            };
            let request_rx = futures::stream::once(async move { Ok(request) }).boxed();
            let response = middleware.serve(request_rx).await?.try_next().await;

            let validated = response
                .map_err(|status| anyhow::Error::msg(status.message().to_string()))?
                .context("derive connector did not return a response")?
                .validated
                .context("derive Response is not Validated")?;

            Ok(validated)
        }
        .boxed_local()
    }

    fn validate_materialization<'a>(
        &'a self,
        request: proto_flow::materialize::request::Validate,
    ) -> futures::future::LocalBoxFuture<
        'a,
        Result<proto_flow::materialize::response::Validated, anyhow::Error>,
    > {
        self.0.validate_materialization(request)
    }

    fn inspect_image<'a>(
        &'a self,
        image: String,
    ) -> futures::future::LocalBoxFuture<'a, Result<Vec<u8>, anyhow::Error>> {
        self.0.inspect_image(image)
    }
}

pub(crate) fn project_root(source: &url::Url) -> url::Url {
    let current_dir =
        std::env::current_dir().expect("failed to determine current working directory");
    let source_path = source.to_file_path();

    let dir = if let Ok(source_path) = &source_path {
        let mut dir = source_path
            .parent()
            .expect("source path is an absolute filesystem path");

        while let Some(parent) = dir.parent() {
            if ["flow.yaml", "flow.yml", "flow.json"]
                .iter()
                .any(|name| parent.join(name).exists())
            {
                dir = parent;
            } else {
                break;
            }
        }
        dir
    } else {
        // `source` isn't local. Use the current working directory.
        &current_dir
    };

    url::Url::from_file_path(dir).expect("cannot map project directory into a URL")
}

/// Fetcher fetches resource URLs from the local filesystem or over the network.
struct Fetcher;

impl sources::Fetcher for Fetcher {
    fn fetch<'a>(
        &'a self,
        // Resource to fetch.
        resource: &'a url::Url,
        // Expected content type of the resource.
        content_type: flow::ContentType,
    ) -> sources::FetchFuture<'a> {
        tracing::debug!(%resource, ?content_type, "fetching resource");
        let url = resource.clone();
        Box::pin(fetch_async(url))
    }
}

async fn fetch_async(resource: url::Url) -> Result<bytes::Bytes, anyhow::Error> {
    match resource.scheme() {
        "http" | "https" => {
            let resp = reqwest::get(resource.as_str()).await?;
            let status = resp.status();

            if status.is_success() {
                Ok(resp.bytes().await?)
            } else {
                let body = resp.text().await?;
                anyhow::bail!("{status}: {body}");
            }
        }
        "file" => {
            let path = resource
                .to_file_path()
                .map_err(|err| anyhow::anyhow!("failed to convert file uri to path: {:?}", err))?;

            let bytes =
                std::fs::read(path).with_context(|| format!("failed to read {resource}"))?;
            Ok(bytes.into())
        }
        "stdin" => {
            use tokio::io::AsyncReadExt;

            let mut bytes = Vec::new();
            tokio::io::stdin()
                .read_to_end(&mut bytes)
                .await
                .context("reading stdin")?;

            Ok(bytes.into())
        }
        _ => Err(anyhow::anyhow!(
            "cannot fetch unsupported URI scheme: '{resource}'"
        )),
    }
}

struct Resolver {
    client: crate::controlplane::Client,
}

impl validation::ControlPlane for Resolver {
    fn resolve_collections<'a, 'b: 'a>(
        &'a self,
        collections: Vec<models::Collection>,
        // These parameters are currently required, but can be removed once we're
        // actually resolving fuzzy pre-built CollectionSpecs from the control plane.
        temp_build_config: &'b proto_flow::flow::build_api::Config,
        temp_storage_mappings: &'b [tables::StorageMapping],
    ) -> futures::future::LocalBoxFuture<'a, anyhow::Result<Vec<proto_flow::flow::CollectionSpec>>>
    {
        async move {
            // TODO(johnny): Introduce a new RPC for doing fuzzy-search given the list of
            // collection names, and use that instead to surface mis-spelt name suggestions.
            // Pair this with a transition to having built specifications in the live_specs table?

            // NameSelector will return *all* collections, rather than *no*
            // collections, if its selector is empty.
            if collections.is_empty() {
                tracing::info!("there are no remote collections to resolve");
                return Ok(vec![]);
            }

            let list = crate::catalog::List {
                flows: false,
                name_selector: crate::catalog::NameSelector {
                    name: collections.into_iter().map(|c| c.to_string()).collect(),
                    prefix: Vec::new(),
                },
                type_selector: crate::catalog::SpecTypeSelector {
                    captures: Some(false),
                    collections: Some(true),
                    materializations: Some(false),
                    tests: Some(false),
                },
                deleted: false,
            };

            let columns = vec![
                "catalog_name",
                "id",
                "spec",
                "spec_type",
                "updated_at",
                "last_pub_user_email",
            ];
            let rows = crate::catalog::fetch_live_specs(self.client.clone(), &list, columns)
                .await
                .context("failed to fetch collection specs")?;

            tracing::info!(name=?list.name_selector.name, rows=?rows.len(), "resolved remote collections");

            rows.into_iter()
                .map(|row| {
                    use crate::catalog::SpecRow;
                    let def = row
                        .parse_spec::<models::CollectionDef>()
                        .context("parsing specification")?;

                    Ok(Self::temp_build_collection_helper(
                        row.catalog_name,
                        def,
                        temp_build_config,
                        temp_storage_mappings,
                    )?)
                })
                .collect::<anyhow::Result<_>>()
        }
        .boxed_local()
    }
}

const STDIN_URL: &str = "stdin://root/flow.yaml";

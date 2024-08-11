use anyhow::Context;
use futures::{future::BoxFuture, FutureExt};
use proto_flow::{capture, derive, flow, materialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

/// Map a "--source" argument to a corresponding URL, optionally creating an empty
/// file if one doesn't exist, which is required when producing a canonical file:///
/// URL for a local file.
pub fn arg_source_to_url(source: &str, create_if_not_exists: bool) -> anyhow::Result<url::Url> {
    // Special case that maps stdin into a URL constant.
    if source == "-" {
        return Ok(url::Url::parse(STDIN_URL).unwrap());
    } else if let Ok(url) = url::Url::parse(source) {
        return Ok(url);
    }

    tracing::debug!(
        source = %source,
        "source is not a URL; assuming it's a filesystem path",
    );

    let source = match std::fs::canonicalize(source) {
        Ok(p) => p,
        Err(err) if matches!(err.kind(), std::io::ErrorKind::NotFound) && create_if_not_exists => {
            std::fs::write(source, "{}")
                .with_context(|| format!("failed to create new file {source}"))?;
            std::fs::canonicalize(source).expect("can canonicalize() a file we just wrote")
        }
        Err(err) => {
            return Err(err).context(format!("could not find {source} in the local filesystem"));
        }
    };

    // Safe unwrap since we've canonical-ized the path.
    Ok(url::Url::from_file_path(&source).unwrap())
}

/// Map a `source` into a suitable project root directory.
///
/// If `source` is a local file:// URL, its parent directories are examined
/// for a contained `flow.yaml`, `flow.yml`, or `flow.json` file, and the URL
/// of the root-most directory having such a file is returned.
///
/// Or, if `source` is not a local file://, then the current working directory is returned.
pub fn project_root(source: &url::Url) -> url::Url {
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

/// Load a Flow specification `source` into a tables::DraftCatalog.
/// All file:// resources are rooted ("jailed") to the given `file_root`.
pub async fn load(source: &url::Url, file_root: &Path) -> tables::DraftCatalog {
    let loader = sources::Loader::new(tables::DraftCatalog::default(), Fetcher::new(file_root));

    loader
        .load_resource(
            sources::Scope::new(&source),
            &source,
            flow::ContentType::Catalog,
        )
        .await;

    loader.into_tables()
}

/// Perform validations and produce built specifications for `draft` and `live`.
/// * If `generate_ops_collections` is set, then ops collections are added into `sources`.
/// * If any of `noop_*` is true, then validations are skipped for connectors of that type.
pub async fn validate(
    pub_id: models::Id,
    build_id: models::Id,
    allow_local: bool,
    connector_network: &str,
    log_handler: impl runtime::LogHandler,
    noop_captures: bool,
    noop_derivations: bool,
    noop_materializations: bool,
    project_root: &url::Url,
    mut draft: tables::DraftCatalog,
    live: tables::LiveCatalog,
) -> Output {
    ::sources::inline_draft_catalog(&mut draft);

    let runtime = runtime::Runtime::new(
        allow_local,
        connector_network.to_string(),
        log_handler,
        None,
        format!("build/{build_id:#}"),
    );
    let connectors = Connectors {
        noop_captures,
        noop_derivations,
        noop_materializations,
        runtime,
    };

    let built = validation::validate(
        pub_id,
        build_id,
        project_root,
        &connectors,
        &draft,
        &live,
        true, // Fail-fast.
    )
    .await;

    Output::new(draft, live, built)
}

/// The output of a build, which can be either successful, failed, or anything
/// in between. The "in between" may seem silly, but may be important for
/// some use cases. For example, you may be executing a build for the purpose
/// of getting the collection projections, in which case you may not want to
/// consider errors from materialization validations to be terminal.
#[derive(Default)]
pub struct Output {
    pub draft: tables::DraftCatalog,
    pub live: tables::LiveCatalog,
    pub built: tables::Validations,
}

impl Output {
    pub fn new(
        draft: tables::DraftCatalog,
        live: tables::LiveCatalog,
        built: tables::Validations,
    ) -> Self {
        Output { draft, live, built }
    }

    pub fn into_parts(
        self,
    ) -> (
        tables::DraftCatalog,
        tables::LiveCatalog,
        tables::Validations,
    ) {
        (self.draft, self.live, self.built)
    }

    pub fn into_result(mut self) -> Result<Self, tables::Errors> {
        let mut errors = tables::Errors::default();

        errors.extend(std::mem::take(&mut self.draft.errors).into_iter());
        errors.extend(std::mem::take(&mut self.live.errors).into_iter());
        errors.extend(std::mem::take(&mut self.built.errors).into_iter());

        if errors.is_empty() {
            Ok(self)
        } else {
            Err(errors)
        }
    }

    /// Returns an iterator of all errors that have occurred during any phase of the build.
    pub fn errors(&self) -> impl Iterator<Item = &tables::Error> {
        self.draft
            .errors
            .iter()
            .chain(self.live.errors.iter())
            .chain(self.built.errors.iter())
    }
}

/// Persist a managed build Result into the SQLite tables commonly known as a "build DB".
pub fn persist(
    build_config: proto_flow::flow::build_api::Config,
    db_path: &Path,
    output: &Output,
) -> anyhow::Result<()> {
    let db = rusqlite::Connection::open(db_path).context("failed to open catalog database")?;

    tables::persist_tables(&db, &output.draft.as_tables())
        .context("failed to persist draft catalog")?;
    tables::persist_tables(&db, &output.live.as_tables())
        .context("failed to persist live catalog")?;
    tables::persist_tables(&db, &output.built.as_tables())
        .context("failed to persist built catalog")?;

    // Legacy support: encode and persist a deprecated protobuf build Config.
    // At the moment, these are still covered by Go snapshot tests.
    let mut meta = tables::Meta::new();
    meta.insert_row(build_config);
    tables::persist_tables(&db, &[&meta]).context("failed to persist catalog meta")?;

    tracing::info!(?db_path, "wrote build database");
    Ok(())
}

/// Gather all file URLs and contents generated by validations.
/// Malformed URLs are ignored, as they're already surfaced as validation errors.
pub fn generate_files(
    project_root: &url::Url,
    validations: &tables::Validations,
) -> anyhow::Result<()> {
    let mut files = BTreeMap::new();

    for row in validations.built_collections.iter() {
        let Some(validated) = &row.validated else {
            continue;
        };

        for (url, content) in &validated.generated_files {
            if let Ok(url) = url::Url::parse(&url) {
                files.insert(url, content.as_bytes());
            }
        }
    }
    let files = files
        .into_iter()
        .map(|(resource, content)| (resource, content.to_vec()))
        .collect();

    write_files(project_root, files)
}

/// Write out files which are located underneath the `project_root`.
pub fn write_files(project_root: &url::Url, files: Vec<(url::Url, Vec<u8>)>) -> anyhow::Result<()> {
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

/// Fetcher is a general-purpose implementation of sources::Fetcher.
struct Fetcher {
    client: reqwest::Result<reqwest::Client>,
    file_root: PathBuf,
}

impl Fetcher {
    fn new(file_root: impl Into<PathBuf>) -> Self {
        let client = reqwest::ClientBuilder::new().timeout(FETCH_TIMEOUT).build();

        Self {
            client,
            file_root: file_root.into(),
        }
    }

    async fn fetch_inner(
        &self,
        resource: url::Url,
        mut file_path: PathBuf,
    ) -> anyhow::Result<bytes::Bytes> {
        match resource.scheme() {
            "http" | "https" => {
                let client = match &self.client {
                    Ok(ok) => ok,
                    Err(err) => anyhow::bail!("failed to initialize HTTP client: {err}"),
                };

                let resp = client.get(resource).send().await?;
                let status = resp.status();

                if status.is_success() {
                    Ok(resp.bytes().await?)
                } else {
                    let body = resp.text().await?;
                    anyhow::bail!("{status}: {body}");
                }
            }
            "file" => {
                let rel_path = resource.to_file_path().map_err(|err| {
                    anyhow::anyhow!("failed to convert file uri to path: {:?}", err)
                })?;

                // `rel_path` is absolute, so we must extend `file_path` rather than joining.
                // Skip the first component, which is a RootDir token.
                file_path.extend(rel_path.components().skip(1));

                let bytes = std::fs::read(&file_path)
                    .with_context(|| format!("failed to read {file_path:?}"))?;
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
}

impl sources::Fetcher for Fetcher {
    fn fetch<'a>(
        &'a self,
        resource: &'a url::Url,
        content_type: flow::ContentType,
    ) -> BoxFuture<'a, anyhow::Result<bytes::Bytes>> {
        tracing::debug!(%resource, ?content_type, file_root=?self.file_root, "fetching resource");
        self.fetch_inner(resource.clone(), self.file_root.clone())
            .boxed()
    }
}

/// Connectors is a general-purpose implementation of validation::Connectors
/// that dispatches to its contained runtime::Runtime.
pub struct Connectors<L: runtime::LogHandler> {
    noop_captures: bool,
    noop_derivations: bool,
    noop_materializations: bool,
    runtime: runtime::Runtime<L>,
}

impl<L: runtime::LogHandler> Connectors<L> {
    pub fn new(runtime: runtime::Runtime<L>) -> Self {
        Self {
            noop_captures: false,
            noop_derivations: false,
            noop_materializations: false,
            runtime,
        }
    }

    pub fn with_noop_validations(self) -> Self {
        Self {
            noop_captures: true,
            noop_derivations: true,
            noop_materializations: true,
            runtime: self.runtime,
        }
    }
}

impl<L: runtime::LogHandler> validation::Connectors for Connectors<L> {
    fn validate_capture<'a>(
        &'a self,
        request: capture::Request,
        data_plane: &'a tables::DataPlane,
    ) -> BoxFuture<'a, anyhow::Result<capture::Response>> {
        async move {
            if self.noop_captures {
                validation::NoOpConnectors
                    .validate_capture(request, data_plane)
                    .await
            } else {
                Ok(self
                    .runtime
                    .clone()
                    .unary_capture(request, CONNECTOR_TIMEOUT)
                    .await?)
            }
        }
        .boxed()
    }

    fn validate_derivation<'a>(
        &'a self,
        request: derive::Request,
        data_plane: &'a tables::DataPlane,
    ) -> BoxFuture<'a, anyhow::Result<derive::Response>> {
        async move {
            if self.noop_derivations {
                validation::NoOpConnectors
                    .validate_derivation(request, data_plane)
                    .await
            } else {
                Ok(self
                    .runtime
                    .clone()
                    .unary_derive(request, CONNECTOR_TIMEOUT)
                    .await?)
            }
        }
        .boxed()
    }

    fn validate_materialization<'a>(
        &'a self,
        request: materialize::Request,
        data_plane: &'a tables::DataPlane,
    ) -> BoxFuture<'a, anyhow::Result<materialize::Response>> {
        async move {
            if self.noop_materializations {
                validation::NoOpConnectors
                    .validate_materialization(request, data_plane)
                    .await
            } else {
                Ok(self
                    .runtime
                    .clone()
                    .unary_materialize(request, CONNECTOR_TIMEOUT)
                    .await?)
            }
        }
        .boxed()
    }
}

/// NoOpCatalogResolver is a CatalogResolver which does nothing, for use by
/// test cases which want to build catalogs without an integrated control plane.
pub struct NoOpCatalogResolver;

impl tables::CatalogResolver for NoOpCatalogResolver {
    fn resolve<'a>(
        &'a self,
        _catalog_names: Vec<&'a str>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = tables::LiveCatalog> + Send + 'a>> {
        async move {
            let mut live = tables::LiveCatalog::default();

            live.storage_mappings.insert_row(
                models::Prefix::new(""),
                models::Id::zero(),
                vec![models::Store::Gcs(models::GcsBucketAndPrefix {
                    bucket: "example-bucket".to_string(),
                    prefix: None,
                })],
            );

            live.data_planes.insert_row(
                models::Id::zero(),
                "public/noop-data-plane".to_string(),
                true,
                "no-op.dp.estuary-data.com".to_string(),
                vec!["hmac-key".to_string()],
                models::Collection::new("ops/logs"),
                models::Collection::new("ops/stats"),
                "broker:address".to_string(),
                "reactor:address".to_string(),
            );

            live
        }
        .boxed()
    }
}

pub const FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
pub const CONNECTOR_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300); // Five minutes.
pub const STDIN_URL: &str = "stdin://root/flow.yaml";

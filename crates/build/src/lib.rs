use anyhow::Context;
use proto_flow::flow;
use url::Url;

mod api;
pub use api::API;

// TODO(johnny): consolidate with local_specs.rs of crate `flowctl`.
/// Resolves a source argument to a canonical URL. If `source` is already a url, then it's simply
/// parsed and returned. If source is a filesystem path, then it is canonical-ized and returned as a
/// `file:///` URL. Will return an error if the filesystem path does not exist.
pub fn source_to_url(source: &str) -> Result<Url, anyhow::Error> {
    match Url::parse(source) {
        Ok(url) => Ok(url),
        Err(err) => {
            tracing::debug!(
                "{:?} is not a URL; assuming it's a filesystem path (parse error: {})",
                source,
                err
            );
            let source = std::fs::canonicalize(source)
                .context(format!("finding {:?} in the local filesystem", source))?;
            // Safe unwrap since we've canonicalized the path.
            Ok(url::Url::from_file_path(&source).unwrap())
        }
    }
}

pub async fn configured_build<F, D>(
    config: flow::build_api::Config,
    fetcher: F,
    drivers: D,
) -> Result<tables::All, anyhow::Error>
where
    F: sources::Fetcher,
    D: validation::Connectors,
{
    let root_url = source_to_url(config.source.as_str())?;

    let root_spec = match flow::ContentType::from_i32(config.source_type) {
        Some(flow::ContentType::Catalog) => flow::ContentType::Catalog,
        Some(flow::ContentType::JsonSchema) => flow::ContentType::JsonSchema,
        _ => anyhow::bail!("unexpected content type (must be Catalog or JsonSchema)"),
    };

    let mut all_tables =
        load_and_validate(root_url.clone(), root_spec, fetcher, drivers, &config).await;
    all_tables.meta.insert_row(config.clone());

    // Output database path is implied from the configured directory and ID.
    if !config.build_db.is_empty() {
        let db = rusqlite::Connection::open(&config.build_db)
            .context("failed to open catalog database")?;

        tables::persist_tables(&db, &all_tables.as_tables())
            .context("failed to persist catalog tables")?;
        tracing::info!(build_db=?config.build_db, "wrote build database");
    }

    Ok(all_tables)
}

async fn load_and_validate<F, D>(
    root: Url,
    root_type: flow::ContentType,
    fetcher: F,
    connectors: D,
    build_config: &flow::build_api::Config,
) -> tables::All
where
    F: sources::Fetcher,
    D: validation::Connectors,
{
    let loader = sources::Loader::new(tables::Sources::default(), fetcher);
    loader
        .load_resource(sources::Scope::new(&root), &root, root_type.into())
        .await;

    let mut tables = loader.into_tables();
    assemble::generate_ops_collections(&mut tables);
    sources::inline_sources(&mut tables);

    let tables::Sources {
        captures,
        collections,
        mut errors,
        fetches,
        imports,
        materializations,
        resources,
        storage_mappings,
        tests,
    } = tables;

    let tables::Validations {
        built_captures,
        built_collections,
        built_materializations,
        built_tests,
        errors: validation_errors,
    } = validation::validate(
        build_config,
        &connectors,
        // TODO(johnny): Plumb through collection resolution.
        // At the moment we get away with not having this because the control-plane agent
        // includes all connected collections in the build.
        &validation::NoOpControlPlane {},
        &captures,
        &collections,
        &fetches,
        &imports,
        &materializations,
        &storage_mappings,
        &tests,
    )
    .await;

    errors.extend(validation_errors.into_iter());

    tables::All {
        built_captures,
        built_collections,
        built_materializations,
        built_tests,
        captures,
        collections,
        errors,
        fetches,
        imports,
        materializations,
        meta: tables::Meta::new(),
        resources,
        storage_mappings,
        tests,
    }
}

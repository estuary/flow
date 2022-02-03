use anyhow::Context;
use models::tables;
use protocol::flow;
use std::path::Path;
use url::Url;

mod api;
pub use api::API;

mod nodejs;
mod ops;

/// Resolves a source argument to a canonical URL. If `source` is already a url, then it's simply
/// parsed and returned. If source is a filesystem path, then it is canonicalized and returned as a
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
    config: protocol::flow::build_api::Config,
    fetcher: F,
    drivers: D,
) -> Result<tables::All, anyhow::Error>
where
    F: sources::Fetcher,
    D: validation::Drivers,
{
    let root_url = source_to_url(config.source.as_str())?;

    let root_spec = match flow::ContentType::from_i32(config.source_type) {
        Some(flow::ContentType::CatalogSpec) => flow::ContentType::CatalogSpec,
        Some(flow::ContentType::JsonSchema) => flow::ContentType::JsonSchema,
        _ => anyhow::bail!("unexpected content type (must be CatalogSpec or JsonSchema)"),
    };

    // Ensure the build directory exists and is canonical.
    std::fs::create_dir_all(&config.directory).context("failed to create build directory")?;
    let directory = std::fs::canonicalize(&config.directory)
        .context("failed to canonicalize build directory")?;

    let mut all_tables = load_and_validate(root_url, root_spec, fetcher, drivers, &config).await;
    all_tables.meta.insert_row(config.clone());

    // Output database path is implied from the configured directory and ID.
    let output_path = directory.join(&config.build_id);
    // Create or truncate the output database.
    std::fs::write(&output_path, &[]).context("failed to create catalog database")?;

    let db = rusqlite::Connection::open(&output_path).context("failed to open catalog database")?;
    tables::persist_tables(&db, &all_tables.as_tables())
        .context("failed to persist catalog tables")?;
    tracing::info!(?output_path, "wrote build database");

    if !all_tables.errors.is_empty() {
        // Skip follow-on build steps if errors were encountered.
        return Ok(all_tables);
    }

    if config.typescript_generate || config.typescript_compile || config.typescript_package {
        generate_typescript_package(&all_tables, &directory)
            .context("failed to generate TypeScript package")?;
    }
    if config.typescript_compile || config.typescript_package {
        nodejs::compile_package(&directory).context("failed to compile TypeScript package")?;
    }
    if config.typescript_package {
        let npm_resources =
            nodejs::pack_package(&directory).context("failed to pack TypeScript package")?;
        tables::persist_tables(&db, &[&npm_resources]).context("failed to persist NPM package")?;
    }

    Ok(all_tables)
}

pub async fn load_and_validate<F, D>(
    root: Url,
    root_type: flow::ContentType,
    fetcher: F,
    drivers: D,
    config: &flow::build_api::Config,
) -> tables::All
where
    F: sources::Fetcher,
    D: validation::Drivers,
{
    let loader = sources::Loader::new(sources::Tables::default(), fetcher);
    loader
        .load_resource(sources::Scope::new(&root), &root, root_type.into())
        .await;

    let mut tables = loader.into_tables();
    ops::generate_ops_collections(&mut tables);

    let sources::Tables {
        capture_bindings,
        captures,
        collections,
        derivations,
        mut errors,
        fetches,
        imports,
        materialization_bindings,
        materializations,
        named_schemas,
        npm_dependencies,
        mut projections,
        resources,
        schema_docs,
        storage_mappings,
        test_steps,
        transforms,
    } = tables;

    let validation::Tables {
        built_captures,
        built_collections,
        built_derivations,
        built_materializations,
        built_tests,
        errors: validation_errors,
        implicit_projections,
        inferences,
    } = validation::validate(
        config,
        &drivers,
        &capture_bindings,
        &captures,
        &collections,
        &derivations,
        &fetches,
        &imports,
        &materialization_bindings,
        &materializations,
        &named_schemas,
        &npm_dependencies,
        &projections,
        &resources,
        &schema_docs,
        &storage_mappings,
        &test_steps,
        &transforms,
    )
    .await;

    errors.extend(validation_errors.into_iter());
    projections.extend(implicit_projections.into_iter());

    tables::All {
        built_captures,
        built_collections,
        built_derivations,
        built_materializations,
        built_tests,
        capture_bindings,
        captures,
        collections,
        derivations,
        errors,
        fetches,
        imports,
        inferences,
        materialization_bindings,
        materializations,
        meta: tables::Meta::new(),
        named_schemas,
        npm_dependencies,
        projections,
        resources,
        schema_docs,
        storage_mappings,
        test_steps,
        transforms,
    }
}

pub fn generate_typescript_package(tables: &tables::All, dir: &Path) -> Result<(), anyhow::Error> {
    assert!(dir.is_absolute() && dir.is_dir());

    // Generate and write the NPM package.
    let write_intents = nodejs::generate_package(
        &dir,
        &tables.collections,
        &tables.derivations,
        &tables.named_schemas,
        &tables.npm_dependencies,
        &tables.resources,
        &tables.schema_docs,
        &tables.transforms,
    )?;
    nodejs::write_package(&dir, write_intents)?;
    Ok(())
}

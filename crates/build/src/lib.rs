use anyhow::Context;
use models::tables;
use protocol::flow;
use std::path::Path;
use url::Url;

mod api;
pub use api::API;

mod nodejs;
mod ops;

pub async fn configured_build<F, D>(
    config: protocol::flow::build_api::Config,
    fetcher: F,
    drivers: D,
) -> Result<tables::All, anyhow::Error>
where
    F: sources::Fetcher,
    D: validation::Drivers,
{
    let root_url = match Url::parse(&config.source) {
        Ok(url) => url,
        Err(err) => {
            tracing::debug!(
                "{:?} is not a URL; assuming it's a filesystem path (parse error: {})",
                config.source,
                err
            );
            let source = std::fs::canonicalize(&config.source).context(format!(
                "finding {:?} in the local filesystem",
                config.source
            ))?;
            // Safe unwrap since we've canonicalized the path.
            url::Url::from_file_path(&source).unwrap()
        }
    };

    let root_spec = match flow::ContentType::from_i32(config.source_type) {
        Some(flow::ContentType::CatalogSpec) => flow::ContentType::CatalogSpec,
        Some(flow::ContentType::JsonSchema) => flow::ContentType::JsonSchema,
        _ => anyhow::bail!("unexpected content type (must be CatalogSpec or JsonSchema)"),
    };

    // Ensure the build directory exists and is canonical.
    std::fs::create_dir_all(&config.directory).context("failed to create build directory")?;
    let directory = std::fs::canonicalize(&config.directory)
        .context("failed to canonicalize build directory")?;
    // Create or truncate the database at |path|.
    std::fs::write(&config.catalog_path, &[]).context("failed to create catalog database")?;

    let mut all_tables = load_and_validate(root_url, root_spec, fetcher, drivers).await;
    all_tables
        .meta
        .insert_row(uuid::Uuid::new_v4(), config.clone());

    tracing::info!(?config.catalog_path, "persisting catalog database");
    let db = rusqlite::Connection::open(&config.catalog_path)
        .context("failed to open catalog database")?;
    tables::persist_tables(&db, &all_tables.as_tables())
        .context("failed to persist catalog tables")?;

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
) -> tables::All
where
    F: sources::Fetcher,
    D: validation::Drivers,
{
    let loader = sources::Loader::new(sources::Tables::default(), fetcher);
    loader
        .load_resource(sources::Scope::new(&root), &root, root_type)
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
        journal_rules,
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
        &drivers,
        &capture_bindings,
        &captures,
        &collections,
        &derivations,
        &fetches,
        &imports,
        &journal_rules,
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
        journal_rules,
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

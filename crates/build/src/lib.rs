use anyhow::Context;
use models::tables;
use protocol::flow;
use std::path::Path;
use url::Url;

mod web_fetcher;
pub use web_fetcher::WebFetcher;

mod drivers;
pub use drivers::Drivers;

mod api;
pub use api::API;

mod nodejs;

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

    // Ensure the build directory exists and is canonical.
    std::fs::create_dir_all(&config.directory).context("failed to create build directory")?;
    let directory = std::fs::canonicalize(&config.directory)
        .context("failed to canonicalize build directory")?;
    // Create or truncate the database at |path|.
    std::fs::write(&config.catalog_path, &[]).context("failed to create catalog database")?;

    let mut all_tables = load_and_validate(root_url, fetcher, drivers).await;

    if config.add_development_rules {
        // Install a local development JournalRule which orders after all other rules.
        let rule = models::names::Rule::new("\u{FFFF}\u{FFFF}-testing-overrides");
        all_tables.journal_rules.push_row(
            url::Url::parse("test://journal-rule")?,
            rule.clone(),
            protocol::flow::journal_rules::Rule {
                rule: rule.to_string(),
                selector: None, // Match all journals.
                template: Some(protocol::protocol::JournalSpec {
                    replication: 1,
                    fragment: Some(protocol::protocol::journal_spec::Fragment {
                        stores: vec!["file:///".to_string()],
                        compression_codec: protocol::protocol::CompressionCodec::None as i32,
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            },
        );
    }

    tracing::info!(?config.catalog_path, "persisting catalog database");
    let db = rusqlite::Connection::open(&config.catalog_path)
        .context("failed to open catalog database")?;
    tables::persist_tables(&db, &all_tables.as_tables())
        .context("failed to persist catalog tables")?;

    if !all_tables.errors.is_empty() {
        // Skip follow-on build steps if errors were encountered.
        return Ok(all_tables);
    }

    generate_typescript_package(&all_tables, &directory)
        .context("failed to generate TypeScript package")?;

    if config.typescript_compile {
        nodejs::compile_package(&directory).context("failed to compile TypeScript package")?;
    }
    if config.typescript_package {
        let npm_resources =
            nodejs::pack_package(&directory).context("failed to pack TypeScript package")?;
        tables::persist_tables(&db, &[&npm_resources]).context("failed to persist NPM package")?;
    }

    Ok(all_tables)
}

pub async fn load_and_validate<F, D>(root: Url, fetcher: F, drivers: D) -> tables::All
where
    F: sources::Fetcher,
    D: validation::Drivers,
{
    let loader = sources::Loader::new(sources::Tables::default(), fetcher);
    loader
        .load_resource(
            sources::Scope::new(&root),
            &root,
            flow::ContentType::CatalogSpec,
        )
        .await;

    let sources::Tables {
        captures,
        collections,
        derivations,
        endpoints,
        mut errors,
        fetches,
        imports,
        journal_rules,
        materializations,
        named_schemas,
        npm_dependencies,
        mut projections,
        resources,
        schema_docs,
        test_steps,
        transforms,
    } = loader.into_tables();

    let validation::Tables {
        built_collections,
        built_derivations,
        built_materializations,
        built_tests,
        built_transforms,
        errors: validation_errors,
        implicit_projections,
        inferences,
    } = validation::validate(
        &drivers,
        &captures,
        &collections,
        &derivations,
        &endpoints,
        &imports,
        &journal_rules,
        &materializations,
        &named_schemas,
        &npm_dependencies,
        &projections,
        &resources,
        &schema_docs,
        &test_steps,
        &transforms,
    );

    errors.extend(validation_errors.into_iter());
    projections.extend(implicit_projections.into_iter());

    tables::All {
        built_collections,
        built_derivations,
        built_materializations,
        built_tests,
        built_transforms,
        captures,
        collections,
        derivations,
        endpoints,
        errors,
        fetches,
        imports,
        inferences,
        journal_rules,
        materializations,
        named_schemas,
        npm_dependencies,
        projections,
        resources,
        schema_docs,
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

// TODO remove with Rust flowctl.
pub fn compile_typescript_package(dir: &Path) -> Result<(), anyhow::Error> {
    nodejs::compile_package(dir)
}

// TODO remove with Rust flowctl.
pub fn pack_typescript_package(dir: &Path) -> Result<tables::Resources, anyhow::Error> {
    Ok(nodejs::pack_package(&dir)?)
}

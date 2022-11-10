use anyhow::Context;
use models::ContentType;
use protocol::flow;
use serde_json::value::RawValue;
use std::{
    io::{stderr, stdout, Write},
    path::Path,
};
use url::Url;

mod api;
pub use api::API;

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
        Some(flow::ContentType::Catalog) => flow::ContentType::Catalog,
        Some(flow::ContentType::JsonSchema) => flow::ContentType::JsonSchema,
        _ => anyhow::bail!("unexpected content type (must be Catalog or JsonSchema)"),
    };

    // Ensure the build directory exists and is canonical.
    std::fs::create_dir_all(&config.directory).context("failed to create build directory")?;
    let directory = std::fs::canonicalize(&config.directory)
        .context("failed to canonicalize build directory")?;

    let mut all_tables =
        load_and_validate(root_url.clone(), root_spec, fetcher, drivers, &config).await;
    all_tables.meta.insert_row(config.clone());

    let has_typescript_derivations = all_tables
        .derivations
        .iter()
        .any(|derivation| derivation.typescript_module.is_some());

    let has_npm_resources = all_tables
        .resources
        .iter()
        .any(|resource| resource.content_type == protocol::flow::ContentType::TypescriptModule);

    let typescript_enabled = has_typescript_derivations || has_npm_resources;

    // Output database path is implied from the configured directory and ID.
    let output_path = directory.join(&config.build_id);
    let db = rusqlite::Connection::open(&output_path).context("failed to open catalog database")?;

    // Generate TypeScript package? Generation should always succeed if the input catalog is valid.
    if all_tables.errors.is_empty()
        && (config.typescript_generate || config.typescript_compile || config.typescript_package)
        && typescript_enabled
    {
        if let Err(err) = generate_npm_package(&all_tables, &directory)
            .context("failed to generate TypeScript package")
        {
            all_tables.errors.insert_row(&root_url, err);
        }
    }
    // Compile TypeScript? This may fail due to a user-caused error.
    if all_tables.errors.is_empty()
        && (config.typescript_compile || config.typescript_package)
        && typescript_enabled
    {
        if let Err(err) = compile_npm(&directory) {
            all_tables.errors.insert_row(&root_url, err);
        }
    }
    // Package TypeScript?
    if all_tables.errors.is_empty() && config.typescript_package && typescript_enabled {
        let npm_resources = pack_npm(&directory).context("failed to pack TypeScript package")?;
        tables::persist_tables(&db, &[&npm_resources]).context("failed to persist NPM package")?;
    }

    tables::persist_tables(&db, &all_tables.as_tables())
        .context("failed to persist catalog tables")?;
    tracing::info!(?output_path, "wrote build database");

    Ok(all_tables)
}

async fn load_and_validate<F, D>(
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
    let loader = sources::Loader::new(tables::Sources::default(), fetcher);
    loader
        .load_resource(sources::Scope::new(&root), &root, root_type.into())
        .await;

    let mut tables = loader.into_tables();
    assemble::generate_ops_collections(&mut tables);

    let tables::Sources {
        capture_bindings,
        captures,
        collections,
        derivations,
        mut errors,
        fetches,
        imports,
        materialization_bindings,
        materializations,
        npm_dependencies,
        projections,
        resources,
        schema_docs,
        storage_mappings,
        test_steps,
        transforms,
    } = tables;

    let tables::Validations {
        built_captures,
        built_collections,
        built_derivations,
        built_materializations,
        built_tests,
        errors: validation_errors,
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
        &npm_dependencies,
        &projections,
        &resources,
        &storage_mappings,
        &test_steps,
        &transforms,
    )
    .await;

    errors.extend(validation_errors.into_iter());

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
        npm_dependencies,
        projections,
        resources,
        schema_docs,
        storage_mappings,
        test_steps,
        transforms,
    }
}

fn generate_npm_package(tables: &tables::All, dir: &Path) -> Result<(), anyhow::Error> {
    assert!(dir.is_absolute() && dir.is_dir());

    // Generate and write the NPM package.
    let write_intents = assemble::generate_npm_package(
        &dir,
        &tables.collections,
        &tables.derivations,
        &tables.imports,
        &tables.npm_dependencies,
        &tables.resources,
        &tables.transforms,
    )?;
    assemble::write_npm_package(&dir, write_intents)?;
    Ok(())
}

fn compile_npm(package_dir: &std::path::Path) -> Result<(), anyhow::Error> {
    if !package_dir.join("node_modules").exists() {
        npm_cmd(package_dir, &["install", "--no-audit", "--no-fund"])?;
    }
    npm_cmd(package_dir, &["run", "compile"])?;
    npm_cmd(package_dir, &["run", "lint"])?;
    Ok(())
}

fn pack_npm(package_dir: &std::path::Path) -> Result<tables::Resources, anyhow::Error> {
    npm_cmd(package_dir, &["pack"])?;

    let pack = package_dir.join("catalog-js-transformer-0.0.0.tgz");
    let pack = std::fs::canonicalize(&pack)?;

    tracing::info!("built NodeJS pack {:?}", pack);

    let mut resources = tables::Resources::new();
    resources.insert_row(
        Url::from_file_path(&pack).unwrap(),
        flow::ContentType::NpmPackage,
        bytes::Bytes::from(std::fs::read(&pack)?),
        RawValue::from_string("null".to_string()).unwrap(),
    );
    std::fs::remove_file(&pack)?;

    Ok(resources)
}

fn npm_cmd(package_dir: &std::path::Path, args: &[&str]) -> Result<(), anyhow::Error> {
    let mut cmd = std::process::Command::new("npm");

    for &arg in args.iter() {
        cmd.arg(arg);
    }
    cmd.current_dir(package_dir);

    tracing::info!(?package_dir, ?args, "invoking `npm`");

    let output = cmd.output().context("failed to spawn `npm` command")?;

    if !output.status.success() {
        stdout()
            .write(output.stdout.as_slice())
            .context("failed to write `npm` output to stdout")?;
        stderr()
            .write(output.stderr.as_slice())
            .context("failed to write `npm` output to stderr")?;
        anyhow::bail!("npm command {:?} failed, output logged", args.join(" "))
    }
    Ok(())
}

use models::tables;
use protocol::flow;
use std::path::Path;
use url::Url;

mod web_fetcher;
pub use web_fetcher::WebFetcher;

mod drivers;
pub use drivers::Drivers;

mod nodejs;

pub async fn load_and_validate<F, D>(
    root: &Url,
    fetcher: F,
    drivers: D,
) -> Result<tables::All, anyhow::Error>
where
    F: sources::Fetcher,
    D: validation::Drivers,
{
    let loader = sources::Loader::new(sources::Tables::default(), fetcher);
    loader
        .load_resource(
            sources::Scope::new(root),
            root,
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

    Ok(tables::All {
        built_collections,
        built_derivations,
        built_materializations,
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
    })
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

pub fn compile_typescript_package(dir: &Path) -> Result<(), anyhow::Error> {
    nodejs::compile_package(dir)
}

pub fn pack_typescript_package(dir: &Path) -> Result<tables::Resources, anyhow::Error> {
    Ok(nodejs::pack_package(&dir)?)
}

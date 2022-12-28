use crate::{catalog::CatalogSpecType, source::SourceArgs};
use anyhow::Context;
use proto_flow::flow;
use std::path::Path;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct TypeScript {
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, clap::Subcommand)]
#[clap(rename_all = "kebab-case")]
pub enum Command {
    /// Generate TypeScript project files and implementation stubs.
    ///
    /// Generate walks your local Flow catalog source file and its imports
    /// to gather collections, derivations, and associated JSON schemas.
    /// It writes a TypeScript project template in the directory of your
    /// `source` Flow catalog file, and then generates TypeScript types and
    /// implementation stubs.
    ///
    /// You then edit the generated stubs in your preferred editor to fill
    /// out implementations for your TypeScript lambdas.
    Generate(Generate),
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Generate {
    #[clap(flatten)]
    source: SourceArgs,
}

impl TypeScript {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> Result<(), anyhow::Error> {
        match &self.cmd {
            Command::Generate(Generate { source }) => do_generate(ctx, &source).await,
        }
    }
}

fn is_within_current_dir(cwd: &Path, resource_uri: &url::Url) -> bool {
    if resource_uri.scheme() != "file" {
        return false;
    }
    match resource_uri.to_file_path() {
        Ok(resource_path) => resource_path.starts_with(cwd),
        Err(_) => {
            tracing::error!(%resource_uri, "failed to convert file URI into a local path");
            false
        }
    }
}

pub async fn do_generate(
    ctx: &mut crate::CliContext,
    source_args: &SourceArgs,
) -> anyhow::Result<()> {
    let source_tables = source_args.load().await?;

    let cwd = std::env::current_dir().context("cannot determine current working directory")?;
    // When generating TypeScript, users may reference TypeScript modules under
    // their current directory that don't (yet) exist. Squelch these errors.
    // generate_npm_package() will produce a stub implementation that we'll write out.
    // The idea is to disallow generating Typescript stubs at remote URLs or
    // files outside of the current directory, which might come up if a source
    // file has an `import`.
    let errors = source_tables.errors
        .iter()
        .filter( |err| {
            if let Some(sources::LoadError::Fetch { uri, content_type, .. }) = err.error.downcast_ref() {
                if *content_type == flow::ContentType::TypescriptModule && is_within_current_dir(&cwd, uri) {
                    println!("Generating implementation stub for {uri}");
                    false
                } else if *content_type == flow::ContentType::TypescriptModule {
                    tracing::error!(%uri, current_dir = %cwd.display(), "refusing to generate typescript module for URI because it is not within the current working directory");
                    true
                } else {
                    true
                }
            } else {
                true
            }
        })
        .collect::<Vec<_>>();

    // Bail if errors occurred while resolving sources.
    if !errors.is_empty() {
        for tables::Error { scope, error } in errors.iter() {
            tracing::error!(%scope, ?error);
        }
        anyhow::bail!("errors while loading catalog sources");
    }

    // It's possible that the sources may reference a collection that's meant to be resolved against live_specs.
    // We'll collect a set of unresolved collection references, and then attempt to resolve them by fetching their
    // specs from `live_specs`, and including them in the build.
    let mut live_specs_catalog = models::Catalog::default();

    for transform_row in source_tables.transforms.iter() {
        let source_name = &transform_row.spec.source.name;
        let is_included_in_sources = source_tables
            .collections
            .iter()
            .find(|c| &c.collection == source_name)
            .is_some();
        let is_fetched = live_specs_catalog.collections.contains_key(source_name);
        if !is_included_in_sources && !is_fetched {
            let resolved = try_resolve_collection(ctx.controlplane_client()?, source_name).await.with_context(|| {
                anyhow::anyhow!("resolving the collection '{source_name}', referenced from the transform '{}' in derivation '{}'", transform_row.transform, transform_row.derivation)
            })?;
            live_specs_catalog
                .collections
                .insert(source_name.clone(), resolved);
        }
    }

    // If we've fetched any collection specs, then we'll need to load them now.
    let resolved_tables = if !live_specs_catalog.collections.is_empty() {
        // Add the new specs to the existing ones using a new `Loader` with
        // the existing `source_tables`. This requires serializing the bundled
        // catalog because the loader doesn't expose a function to side-load
        // resources from a parsed DOM.
        let loader = sources::Loader::new(source_tables, crate::Fetcher);
        let catalog = serde_json::to_vec(&live_specs_catalog)
            .context("failed to serialize fetched collection specs")?;
        let resource_url = url::Url::parse("flowctl://resolved-catalog-sources").unwrap();
        let scope = sources::Scope::new(&resource_url);
        loader
            .load_resource_from_bytes(
                scope,
                &resource_url,
                catalog.into(),
                flow::ContentType::Catalog,
            )
            .await;
        let tables = loader.into_tables();
        // check the errors table one more time to make sure there wasn't an issue loading the fetched resources
        let has_any_err = tables.errors.iter().filter(|err| {
            err.scope.host_str() == Some("resolved-catalog-sources")
        }).fold(false, |_, err| {
            tracing::error!(error = ?err, "failed to load automatically resolved catalog sources");
            true
        });
        anyhow::ensure!(
            !has_any_err,
            "failed to load one or more fetched collection specs"
        );
        tables
    } else {
        source_tables
    };

    let tables::Sources {
        collections,
        derivations,
        imports,
        npm_dependencies,
        resources,
        transforms,
        ..
    } = resolved_tables;

    let files = assemble::generate_npm_package(
        &cwd,
        &collections,
        &derivations,
        &imports,
        &npm_dependencies,
        &resources,
        &transforms,
    )
    .context("generating TypeScript package")?;

    let files_len = files.len();
    assemble::write_npm_package(&cwd, files)?;

    println!(
        "Wrote {files_len} TypeScript project files under {}.",
        cwd.display()
    );
    Ok(())
}

async fn try_resolve_collection(
    client: crate::controlplane::Client,
    name: &str,
) -> anyhow::Result<models::CollectionDef> {
    tracing::info!(collection_name = %name, "attempting resolve collection from live_specs");
    let list = crate::catalog::List::single(name);

    let columns = vec![
        "catalog_name",
        "id",
        "spec",
        "spec_type",
        "updated_at",
        "last_pub_user_email",
    ];
    let mut rows = crate::catalog::fetch_live_specs(client, &list, columns)
        .await
        .context("fetching live spec")?;

    let Some(row) = rows.pop() else {
        anyhow::bail!("the collection does not exist or you do not have access to it");
    };
    tracing::debug!(catalog_name = %row.catalog_name, id = %row.id, last_pub_user_email = ?row.last_pub_user_email, updated_at = ?row.updated_at, spec_type = ?row.spec_type, "fetched live_spec");

    let Some(ty) = row.spec_type else {
        anyhow::bail!("the collection has been deleted");
    };
    if ty != CatalogSpecType::Collection {
        anyhow::bail!(
            "catalog spec must be a collection (you cannot use a '{ty}' as a transform source)"
        );
    }
    let parsed_spec = row
        .parse_spec::<models::CollectionDef>()
        .context("deserializing spec")?;
    Ok(parsed_spec)
}

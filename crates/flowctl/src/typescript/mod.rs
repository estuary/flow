use super::config;
use anyhow::Context;
use proto_flow::flow;

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
    /// Path to a local Flow catalog source file.
    ///
    /// TypeScript project files will be generated within its parent directory.
    #[clap(long)]
    source: std::path::PathBuf,
}

impl TypeScript {
    pub async fn run(&self, cfg: &mut config::Config) -> Result<(), anyhow::Error> {
        match &self.cmd {
            Command::Generate(Generate { source }) => do_generate(cfg, &source).await,
        }
    }
}

pub async fn do_generate(
    _cfg: &config::Config,
    source_path: &std::path::Path,
) -> anyhow::Result<()> {
    let source_path = std::fs::canonicalize(source_path)
        .context(format!("finding {source_path:?} in the local filesystem"))?;
    let source_url = url::Url::from_file_path(&source_path).unwrap();

    let package_dir = source_path.parent().unwrap().to_owned();
    let package_url = url::Url::from_file_path(&package_dir).unwrap();

    // Load all catalog sources.
    let loader = sources::Loader::new(tables::Sources::default(), crate::Fetcher {});
    loader
        .load_resource(
            sources::Scope::new(&source_url),
            &source_url,
            flow::ContentType::Catalog,
        )
        .await;

    let tables::Sources {
        collections,
        derivations,
        errors,
        imports,
        npm_dependencies,
        resources,
        transforms,
        ..
    } = loader.into_tables();

    // When generating TypeScript, users may reference TypeScript modules under
    // their Flow catalog root file that don't (yet) exist. Squelch these errors.
    // generate_npm_package() will produce a stub implementation that we'll write out.
    let errors = errors
        .into_iter()
        .filter(
            |err| match (err.scope.fragment(), err.error.downcast_ref()) {
                (Some(frag), Some(sources::LoadError::Fetch { uri, .. }))
                    if frag.ends_with("typescript/module")
                        && uri.starts_with(package_url.as_str()) =>
                {
                    println!("Generating implementation stub for {uri}");
                    false
                }
                _ => true,
            },
        )
        .collect::<Vec<_>>();

    // Bail if errors occurred while resolving sources.
    if !errors.is_empty() {
        for tables::Error { scope, error } in errors.iter() {
            tracing::error!(%scope, ?error);
        }
        anyhow::bail!("errors while loading catalog sources");
    }

    let files = assemble::generate_npm_package(
        &package_dir,
        &collections,
        &derivations,
        &imports,
        &npm_dependencies,
        &resources,
        &transforms,
    )
    .context("generating TypeScript package")?;

    let files_len = files.len();
    assemble::write_npm_package(&package_dir, files)?;

    println!("Wrote {files_len} TypeScript project files under {package_url}.");
    Ok(())
}

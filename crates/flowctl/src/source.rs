//! Utilities for working with catalog specifications
mod bundle;
mod unbundle;

pub use bundle::bundle;
pub use unbundle::unbundle;

use anyhow::Context;
use proto_flow::flow;
use std::collections::BTreeMap;
use std::fmt::{self, Display};
use std::path::PathBuf;

pub const DEFAULT_SPEC_FILENAME: &str = "flow.yaml";

/// Common arguments for naming a set of sources to be included in an operation such as
/// `bundle` or `publish`.
#[derive(Debug, Default, clap::Args)]
pub struct SourceArgs {
    /// Path or URL to a Flow specificiation file, commonly 'flow.yaml'
    #[clap(long, required_unless_present = "source-dir")]
    pub source: Vec<String>,
    /// Path to a local directory, which will be recursively searched for files ending with `flow.yaml`.
    #[clap(long)]
    pub source_dir: Vec<String>,

    /// Maximum depth of a recursive directory search. Only used with `--source-dir`.
    #[clap(long, default_value_t = 20u32, requires = "source-dir")]
    pub max_depth: u32,

    /// Follow symlinks when searching for Flow specs with `--source-dir`.
    #[clap(long, requires = "source-dir")]
    pub follow_symlinks: bool,
}

impl SourceArgs {
    /// Attempts to resolve the provided source argument(s) into a list of all the spec paths/URLs
    /// that should be loaded. This function does not check or guarantee that any of the returned
    /// paths or URLs actually exist. It also does not attempt to resolve imports between specs
    /// (this is left up to the `sources::Loader`). This function _will_ search the local filesystem
    /// for Flow specs (any file having a name that ends with `flow.yaml`) if the `--source-dir`
    /// argument was provided.
    pub async fn resolve_sources(&self) -> anyhow::Result<Vec<String>> {
        let mut sources = Vec::new();
        sources.extend(self.source.iter().cloned());

        for dir in self.source_dir.iter() {
            let specs = find_all_sources(dir.clone(), self.max_depth, self.follow_symlinks)
                .await
                .context("finding sources")?;
            sources.extend(specs);
        }
        anyhow::ensure!(
            !sources.is_empty(),
            "no source files were found in any of the given directories"
        );
        Ok(sources)
    }

    /// Loads all resources identified by the arguments, returning the `tables::Sources`.
    /// Errors from resolving the initial input sources will be returned directly, but all
    /// other errors will _only_ be returned as part of the `errors` table. This allows for
    /// fine grained error handling, or ignoring certain types of errors.
    pub async fn load(&self) -> anyhow::Result<::tables::Sources> {
        let sources = self.resolve_sources().await?;
        let loader = sources::Loader::new(tables::Sources::default(), crate::Fetcher {});
        // Load all catalog sources.
        for source in sources {
            let source = source.as_ref();
            // Resolve source to a canonicalized filesystem path or URL.
            let source_url = match url::Url::parse(source) {
                Ok(url) => url,
                Err(err) => {
                    tracing::debug!(
                        source = %source,
                        ?err,
                        "source is not a URL; assuming it's a filesystem path",
                    );
                    let source = std::fs::canonicalize(source)
                        .context(format!("finding {source} in the local filesystem"))?;
                    // Safe unwrap since we've canonicalized the path.
                    url::Url::from_file_path(&source).unwrap()
                }
            };

            loader
                .load_resource(
                    sources::Scope::new(&source_url),
                    &source_url,
                    flow::ContentType::Catalog,
                )
                .await;
        }

        Ok(loader.into_tables())
    }
}

fn should_consider_entry(entry: &walkdir::DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        // We filter out any hidden files or directories, but the case of "."
        // handles the case where the user passes `--source-dir .`, which definitely
        // shouldn't be filtered out.
        .map(|s| !s.starts_with(".") || s == ".")
        .unwrap_or_else(|| {
            tracing::error!(path = %entry.path().display(), "ignoring non-UTF-8 path");
            false
        })
}

async fn find_all_sources(
    dir: String,
    max_depth: u32,
    follow_symlinks: bool,
) -> anyhow::Result<Vec<String>> {
    let results = tokio::task::spawn_blocking::<_, anyhow::Result<Vec<String>>>(move || {
        let iter = walkdir::WalkDir::new(dir)
            .max_depth(max_depth as usize)
            .follow_links(follow_symlinks)
            .same_file_system(true)
            .into_iter()
            .filter_entry(should_consider_entry);
        let mut paths = Vec::new();
        for result in iter {
            let entry = result?;
            if !entry.file_type().is_file() {
                continue;
            }
            // we check this in th
            let name = entry.file_name().to_str().expect("filename must be UTF-8");
            if name.ends_with(DEFAULT_SPEC_FILENAME) {
                tracing::info!(path = %entry.path().display(), "found source file");
                paths.push(entry.path().display().to_string());
            }
        }
        Ok(paths)
    })
    .await??;
    Ok(results)
}

/// Common arguments for writing catalog specs into a directory.
#[derive(Debug, clap::Args)]
pub struct LocalSpecsArgs {
    /// The directory to output specs into.
    ///
    /// By default, specs are written into nested subdirectories based on
    /// the catalog name of each spec. For example, a catalog spec named
    /// `acmeCo/sales/anvils` would be written to `${output_dir}/acmeCo/sales/flow.yaml`,
    /// as would any other specs under the `acmeCo/sales/` prefix. This enables
    /// you to easily work with specs spanning multiple different namespaces.
    #[clap(long, default_value = ".")]
    pub output_dir: PathBuf,
    /// The filename of the root flow spec, typically 'flow.yaml'.
    #[clap(long, default_value = DEFAULT_SPEC_FILENAME)]
    pub spec_filename: String,
    /// Write specs directly into `--output-dir` without creating any
    /// nested subdirectories.
    #[clap(long)]
    pub no_expand_dirs: bool,

    /// Determines how to handle the case where a local file already exists
    /// when a new file would be written.
    ///
    /// - The default (`abort`) causes an error to be returned upon encountering
    /// the first such file.
    /// - `keep` will leave all existing files unchanged, and will only write new files.
    /// - `overwrite` will replace existing files with the new ones.
    /// - `merge-spec` is like `overwrite`, except that Flow catalog specifications will be
    ///   merged instead of overwritten.
    ///
    /// Merging flow catalog specifications results in a superset of all the individual specs
    /// (captures, collections, etc.) in the file, with new specs taking precedent over existing
    /// ones. Individual specs are never merged.
    #[clap(long, value_enum, default_value_t = Existing::Abort)]
    pub existing: Existing,

    /// Write JSON files instead of YAML.
    #[clap(long)]
    pub json: bool,
}

/// Determines how to handle the case where a local file already exists when a new file is about to be written.
#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq)]
pub enum Existing {
    /// Like overwrite, except that flow catalog specifications will be merged insead of overwritten.
    ///
    /// Merging flow catalog specifications results in a superset of all the individual specs
    /// (captures, collections, etc.) in the file, with new specs taking precedent over existing
    /// ones. Individual specs are never merged.
    MergeSpec,
    /// Overwrite contents of any existing files.
    Overwrite,
    /// Returns an error on encountering any file that already exists.
    Abort,
    /// Skip writing any files if there is already an existing file at the same location.
    /// In that case, do not consider it an error, and continue to write all other files.
    Keep,
}

impl Display for Existing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Existing::MergeSpec => "merge-spec",
            Existing::Overwrite => "overwrite",
            Existing::Abort => "error",
            Existing::Keep => "skip",
        };
        f.write_str(s)
    }
}

/// Writes a set of flow specs from a (typically, but not necessarily) bundled catalog into a directory.
/// See `LocalSpecsArgs` for more info.
pub async fn write_local_specs(
    new_catalog: models::Catalog,
    args: &LocalSpecsArgs,
) -> anyhow::Result<()> {
    let output_dir = args.output_dir.as_path();
    if args.no_expand_dirs {
        // the user doesn't want us to expand the catalog into nested subdirectories.
        return unbundle(
            new_catalog,
            output_dir,
            &args.spec_filename,
            args.json,
            args.existing,
        )
        .await;
    }
    // We're going to split the catalog into separate catalogs for prefix.
    // Always use the "flow.yaml" when writing out the specs within subdirectories.
    // The filename from the args is only intended to be used for the root spec.
    let OrganizedSpecs { by_prefix, root } =
        partition_by_prefix(new_catalog, DEFAULT_SPEC_FILENAME);

    for (prefix, catalog) in by_prefix {
        let path = output_dir.join(&prefix);
        unbundle(
            catalog,
            &path,
            DEFAULT_SPEC_FILENAME,
            args.json,
            args.existing,
        )
        .await?;
        tracing::debug!(prefix = %prefix, path = %path.display(), "unbundled specs for prefix");
    }

    // Now unbundle the root spec, which will minimally contain imports for each subdirectory that we wrote.
    unbundle(
        root,
        &output_dir,
        &args.spec_filename,
        args.json,
        args.existing,
    )
    .await?;
    tracing::debug!(path = %output_dir.display(), "unbundled root spec");
    Ok(())
}

struct OrganizedSpecs {
    by_prefix: BTreeMap<String, models::Catalog>,
    root: models::Catalog,
}

/// Splits a single catalog into separate catalogs by prefix. There will always be a top-level
/// catalog that imports the others. Any storage mappings will also only appear in the top-level.
fn partition_by_prefix(catalog: models::Catalog, leaf_spec_filename: &str) -> OrganizedSpecs {
    let mut root = catalog;

    let mut by_prefix: BTreeMap<String, models::Catalog> = BTreeMap::new();

    for (name, capture) in std::mem::take(&mut root.captures) {
        let prefix = prefix_path(&name);
        let catalog = by_prefix.entry(prefix).or_default();
        catalog.captures.insert(name, capture);
    }
    for (name, collection) in std::mem::take(&mut root.collections) {
        let prefix = prefix_path(&name);
        let catalog = by_prefix.entry(prefix).or_default();
        catalog.collections.insert(name, collection);
    }
    for (name, materialization) in std::mem::take(&mut root.materializations) {
        let prefix = prefix_path(&name);
        let catalog = by_prefix.entry(prefix).or_default();
        catalog.materializations.insert(name, materialization);
    }
    for (name, test) in std::mem::take(&mut root.tests) {
        let prefix = prefix_path(&name);
        let catalog = by_prefix.entry(prefix).or_default();
        catalog.tests.insert(name, test);
    }

    // Add an `import` to the root catalog for each prefix that we separated out.
    for path in by_prefix.keys() {
        // The prefix can never end with a `/`, so we don't need a fancy join function.
        let import_path = format!("{path}/{leaf_spec_filename}");
        root.import
            .push(models::Import::Url(models::RelativeUrl::new(import_path)));
    }

    OrganizedSpecs { by_prefix, root }
}

fn prefix_path(catalog_name: &impl AsRef<str>) -> String {
    catalog_name
        .as_ref()
        .rsplit_once("/")
        .expect("catalog name must contain at least one '/'")
        .0
        .to_string()
}

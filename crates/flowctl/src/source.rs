//! Utilities for working with catalog specifications
mod bundle;
mod unbundle;

pub use bundle::bundle;
pub use unbundle::unbundle;

use std::collections::BTreeMap;
use std::fmt::{self, Display};
use std::path::PathBuf;
use tokio::fs;

pub const DEFAULT_SPEC_FILENAME: &str = "flow.yaml";

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
    /// The default (`abort`) causes an error to be returned upon encountering
    /// the first such file.
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

pub async fn write_local_specs(
    new_catalog: models::Catalog,
    args: &LocalSpecsArgs,
) -> anyhow::Result<()> {
    let output_dir = args.output_dir.as_path();
    fs::create_dir_all(output_dir).await?;
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
        fs::create_dir_all(&path).await?;
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

fn partition_by_prefix(catalog: models::Catalog, leaf_spec_filename: &str) -> OrganizedSpecs {
    let mut root = catalog;

    let mut by_prefix: BTreeMap<String, models::Catalog> = BTreeMap::new();

    for (name, capture) in std::mem::take(&mut root.captures) {
        let prefix = prefix_path(&name);
        let mut catalog = by_prefix.entry(prefix).or_default();
        catalog.captures.insert(name, capture);
    }
    for (name, collection) in std::mem::take(&mut root.collections) {
        let prefix = prefix_path(&name);
        let mut catalog = by_prefix.entry(prefix).or_default();
        catalog.collections.insert(name, collection);
    }
    for (name, materialization) in std::mem::take(&mut root.materializations) {
        let prefix = prefix_path(&name);
        let mut catalog = by_prefix.entry(prefix).or_default();
        catalog.materializations.insert(name, materialization);
    }
    for (name, test) in std::mem::take(&mut root.tests) {
        let prefix = prefix_path(&name);
        let mut catalog = by_prefix.entry(prefix).or_default();
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

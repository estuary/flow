use anyhow::{Context, Error};
use estuary::catalog;
use rusqlite::Connection as DB;
use std::fs;
use std::path::{Path, PathBuf};
use structopt::StructOpt;
use url;

#[derive(StructOpt, Debug)]
#[structopt(about = "Command-line interface for working with Estuary projects",
            author = env!("CARGO_PKG_AUTHORS"))]
struct Args {
    #[structopt(subcommand)]
    command: SubCommand,
    #[structopt(flatten)]
    build_args: BuildArgs,
}

#[derive(StructOpt, Debug)]
enum SubCommand {
    /// Builds a Catalog spec into a catalog database that can be deployed or inspected.
    Build,

    /// Shows outputs from a built catalog.
    Show(ShowArgs),
}

#[derive(StructOpt, Debug)]
struct ShowArgs {
    /// The thing to show
    #[structopt(subcommand)]
    target: ShowTarget,
}

#[derive(StructOpt, Debug)]
enum ShowTarget {
    /// Print the DDL (SQL "CREATE TABLE" statement) for a given materialization
    DDL {
        /// The name of the collection
        collection: Option<String>,
        /// The name of a specific materialization for the given collection
        materialization: Option<String>,
    },
}

#[derive(StructOpt, Debug)]
struct BuildArgs {
    /// Path to input specification file. Defaults to a file named 'catalog.[yaml|yml|json]' in the current
    /// directory.
    #[structopt(long, global = true)]
    path: Option<String>,

    /// Allow re-using an existing catalog database if one exists.
    ///
    /// Estctl does not currently account for files that have been modified since the catalog was
    /// built, so using this option could cause a build to be skipped, even when an input manifest
    /// has been modified since the last build. The default behavior is to always delete an
    /// existing catalog database and rebuild it from scratch.
    #[structopt(long, global = true)]
    no_rebuild: bool,

    /// Path to output catalog database
    #[structopt(long, default_value = "catalog.db", global = true)]
    catalog: String,

    /// Path to NodeJS package which will hold JavaScript lambdas. If this directory
    /// doesn't exist, it will be automatically created from a template. The package
    /// is used temporarily during the catalog build process -- it's compiled and
    /// then packed into the output catalog database -- but re-using the same directory
    /// across invocations will save time otherwise spent fetching npm packages.
    #[structopt(long = "nodejs", default_value = "catalog-nodejs", global = true)]
    nodejs_package_path: String,
}

impl BuildArgs {
    /// Resloves the path to the input catalog spec. If the `--path` argument was provided,then
    /// that is always returned directly. Otherwise, this will look in the current directory for a
    /// file named `catalog.yaml|yml|json` and return the path to the first one that exists.
    fn get_catalog_spec_path(&self) -> Result<PathBuf, NoCatalogSpecError> {
        if let Some(path) = self.path.as_ref() {
            Ok(PathBuf::from(path))
        } else {
            let possible_filenames = &["catalog.yaml", "catalog.yml", "catalog.json"];
            possible_filenames
                .into_iter()
                .filter(|path| fs::metadata(*path).is_ok())
                .next()
                .map(|p| PathBuf::from(*p))
                .ok_or(NoCatalogSpecError)
        }
    }
}

fn main() {
    pretty_env_logger::init();
    let args = Args::from_args();
    log::debug!("{:?}", args);
    let result = match &args.command {
        SubCommand::Build => do_build(&args.build_args),
        SubCommand::Show(show_args) => do_show(show_args, &args.build_args),
    };

    match result {
        Ok(_) => (),
        Err(e) => eprintln!("Error: {:#}", e),
    };
}

fn do_build(args: &BuildArgs) -> Result<DB, Error> {
    let spec_path = args.get_catalog_spec_path()?;
    let root = fs::canonicalize(spec_path.as_path())?;
    log::debug!("Building catalog spec: '{}'", root.display());

    // If we know that we won't be re-using the existing database, then delete the
    // whole file now. This lets us only open the database once.
    if !args.no_rebuild {
        if delete_ignore_missing(args.catalog.as_str())? {
            log::debug!("deleted '{}' so that we can rebuild it", args.catalog);
        }
    }

    let db = catalog::open(args.catalog.as_str())?;
    if args.no_rebuild && catalog::database_is_built(&db) {
        log::info!("no need to rebuild the catalog");
        return Ok(db);
    }

    // safe unwrap since we've canonicalized the path above
    let root = url::Url::from_file_path(&root).unwrap();
    let node = Path::new(args.nodejs_package_path.as_str());

    catalog::build(&db, root, node)?;
    log::debug!("Successfully built catalog");
    Ok(db)
}

fn delete_ignore_missing(path: impl AsRef<Path>) -> Result<bool, std::io::Error> {
    match fs::remove_file(path) {
        Ok(_) => Ok(true),
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                Ok(false)
            } else {
                Err(e)
            }
        }
    }
}

fn do_show(show_args: &ShowArgs, build_args: &BuildArgs) -> Result<DB, Error> {
    let db = do_build(build_args).context("failed to build catalog")?;

    match &show_args.target {
        ShowTarget::DDL {
            collection,
            materialization,
        } => show_materialialization_ddl(
            &db,
            collection.as_ref().map(String::as_str),
            materialization.as_ref().map(String::as_str),
        )?,
    };
    Ok(db)
}

fn show_materialialization_ddl(
    db: &DB,
    collection: Option<&str>,
    materialization: Option<&str>,
) -> Result<(), Error> {
    // We're ordering by target_uri so we can print out the sql for each target database grouped
    // together.
    let sql = "SELECT c.collection_name, m.materialization_name, m.target_uri, ddl
        FROM collections AS c
        NATURAL JOIN materializations AS m
        NATURAL JOIN materialization_ddl
        WHERE c.collection_name LIKE ? AND m.materialization_name LIKE ?
        ORDER BY m.target_uri ASC, c.collection_name ASC";
    let mut stmt = db.prepare(sql)?;
    let mut rows = stmt.query(rusqlite::params![
        collection.unwrap_or("%"),
        materialization.unwrap_or("%")
    ])?;

    let mut current_uri: Option<String> = None;
    while let Some(row) = rows.next()? {
        let target_uri: String = row.get(0)?;
        let ddl: String = row.get(1)?;

        if current_uri.as_ref() != Some(&target_uri) {
            // print a big separator if we're going from one target uri to another
            let newlines = if current_uri.is_some() { "\n\n" } else { "" };
            println!(
                "{}-- Materializaions for the target: {}\n",
                newlines, target_uri
            );
        }
        current_uri = Some(target_uri);
        println!("{}", ddl);
    }

    // If current_uri is None then the query must not have returned any rows,
    // so we'll return an error here.
    if current_uri.is_none() {
        anyhow::bail!(
            "No materializations exist for collection: {} and materialization: {}",
            collection.unwrap_or("<all>"),
            materialization.unwrap_or("<all>")
        );
    }

    Ok(())
}

#[derive(Debug, thiserror::Error)]
#[error("Missing input catalog spec. Either provide the `--path` option or re-run from the parent directory of a file named catalog.yaml|yml|json")]
struct NoCatalogSpecError;

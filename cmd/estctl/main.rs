use anyhow::{Context, Error};
use estuary::catalog;
use rusqlite::Connection as DB;
use std::fs;
use std::path::Path;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(about = "Command-line interface for working with Estuary projects",
            author = env!("CARGO_PKG_AUTHORS"))]
enum Command {
    /// Builds a Catalog spec into a catalog database that can be deployed or inspected.
    Build(BuildArgs),
    /// Shows outputs from a built catalog.
    Show(ShowArgs),
}

#[derive(StructOpt, Debug)]
struct ShowArgs {
    /// Path to input catalog database.
    #[structopt(long, default_value = "catalog.db")]
    catalog: String,
    /// The thing to show.
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
    /// URL or filesystem path of the input specification source file.
    #[structopt(long, default_value = "catalog.yaml")]
    source: String,
    /// Path to output catalog database.
    #[structopt(long, default_value = "catalog.db")]
    catalog: String,
    /// Path to NodeJS package which will hold JavaScript lambdas. If this directory
    /// doesn't exist, it will be automatically created from a template. The package
    /// is used temporarily during the catalog build process -- it's compiled and
    /// then packed into the output catalog database -- but re-using the same directory
    /// across invocations will save time otherwise spent fetching npm packages.
    #[structopt(long = "nodejs", default_value = "catalog-nodejs")]
    nodejs_package_path: String,
}

fn main() {
    pretty_env_logger::init_timed();

    let args = Command::from_args();
    log::debug!("{:?}", args);

    let result = match args {
        Command::Build(build) => do_build(build),
        Command::Show(show) => do_show(show),
    };

    match result {
        Ok(_) => (),
        Err(e) => eprintln!("Error: {:#}", e),
    };
}

fn do_build(args: BuildArgs) -> Result<DB, Error> {
    let spec_url = match url::Url::parse(&args.source) {
        Ok(url) => url,
        Err(err) => {
            log::debug!(
                "--source {:?} is not a URL; assuming it's a filesystem path (parse error: {})",
                &args.source,
                err
            );
            let source = fs::canonicalize(&args.source).context(format!(
                "finding --source {:?} in the local filesystem",
                &args.source
            ))?;
            // Safe unwrap since we've canonicalized the path.
            url::Url::from_file_path(&source).unwrap()
        }
    };
    let db = catalog::create(&args.catalog)
        .context(format!("creating --catalog {:?}", &args.catalog))?;

    log::info!(
        "Building --source {:?} into --catalog {:?}",
        spec_url,
        fs::canonicalize(&args.catalog)?
    );
    let nodejs_dir = Path::new(args.nodejs_package_path.as_str());

    catalog::build(&db, spec_url, nodejs_dir).context("building catalog")?;
    log::info!("Successfully built catalog");
    Ok(db)
}

fn do_show(args: ShowArgs) -> Result<DB, Error> {
    let db = catalog::open(&args.catalog).context("opening --catalog")?;

    match &args.target {
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
    let sql = "SELECT m.target_uri, m.ddl
        FROM collections AS c
        NATURAL JOIN materializations AS m
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

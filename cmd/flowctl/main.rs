use anyhow::{Context, Error};
use estuary::{catalog, doc, runtime, testing};
use rusqlite::Connection as DB;
use std::convert::TryFrom;
use std::fs;
use std::path::Path;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(about = "Command-line interface for working with Estuary Flow projects",
            author = env!("CARGO_PKG_AUTHORS"))]
enum Command {
    /// Builds a Catalog spec into a catalog database that can be deployed or inspected.
    Build(BuildArgs),
    /// Shows outputs from a built catalog.
    Show(ShowArgs),
    /// Runs catalog tests.
    Test(TestArgs),
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

#[derive(StructOpt, Debug)]
struct TestArgs {
    /// Path to input catalog database.
    #[structopt(long, default_value = "catalog.db")]
    catalog: String,
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init_timed();

    let args = Command::from_args();
    log::debug!("{:?}", args);

    let result = match args {
        Command::Build(build) => do_build(build),
        Command::Show(show) => do_show(show),
        Command::Test(test) => do_test(test).await,
    };

    if let Err(err) = result {
        log::error!("{:?}", err);
        std::process::exit(-1);
    };
}

fn do_build(args: BuildArgs) -> Result<(), Error> {
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
    Ok(())
}

fn do_show(args: ShowArgs) -> Result<(), Error> {
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
    Ok(())
}

async fn do_test(args: TestArgs) -> Result<(), Error> {
    let db = catalog::open(&args.catalog).context("opening --catalog")?;

    let cluster = runtime::Cluster::new();

    // Upsert shards for all derivations.
    {
        let shards = cluster.list_shards(None).await?;
        let mut derivations = runtime::DerivationSet::try_from(shards).unwrap();
        derivations.update_from_catalog(&db)?;

        let apply = derivations.build_recovery_log_apply_request();
        cluster.apply_journals(apply).await?;

        let apply = derivations.build_shard_apply_request(&args.catalog);
        cluster.apply_shards(apply).await?;
    }

    let collections =
        testing::Collection::load_all(&db).context("failed to load catalog collections")?;
    let collection_dependencies = testing::Collection::load_transitive_dependencies(&db)
        .context("failed to load collection dependencies")?;
    let transforms =
        testing::Transform::load_all(&db).context("failed to load catalog transforms")?;
    let schema_index = build_schema_index(&db).context("failed to build schema index")?;

    let ctx = testing::Context {
        cluster,
        collections,
        collection_dependencies,
        schema_index,
        transforms,
    };

    // Load test case IDs. We may want to support regex, etc here.
    let mut stmt = db.prepare("SELECT test_case_id FROM test_cases;")?;
    let case_ids = stmt
        .query_map(rusqlite::NO_PARAMS, |row| row.get(0))?
        .collect::<Result<Vec<i64>, _>>()
        .context("failed to load test cases")?;

    for id in case_ids {
        let case = catalog::TestCase { id };
        let (name, steps) = case.load(&db)?;
        log::info!("starting test case {:?}", name);

        ctx.run_test_case(&steps)
            .await
            .context(format!("test case {} failed", name))?
    }
    Ok(())
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

// TODO -- copy/paste from flow-worker.
fn build_schema_index(
    db: &rusqlite::Connection,
) -> Result<&'static doc::SchemaIndex<'static>, Error> {
    // Compile the bundle of catalog schemas. Then, deliberately "leak" the
    // immutable Schema bundle for the remainder of program in order to achieve
    // a 'static lifetime, which is required for use in spawned tokio Tasks (and
    // therefore in TxnCtx).
    let schemas = catalog::Schema::compile_all(&db)?;
    let schemas = Box::leak(Box::new(schemas));

    let mut schema_index = doc::SchemaIndex::<'static>::new();
    for schema in schemas.iter() {
        schema_index.add(schema)?;
    }
    schema_index.verify_references()?;

    // Also leak a &'static SchemaIndex.
    let schema_index = Box::leak(Box::new(schema_index));

    log::info!("loaded {} JSON-Schemas from catalog", schemas.len());

    Ok(schema_index)
}

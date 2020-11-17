use anyhow::{Context, Error};
use futures::FutureExt;
use itertools::Itertools;
use std::convert::TryFrom;
use std::fs;
use std::path::Path;
use structopt::StructOpt;
use tokio::signal::unix;
use url::Url;

#[derive(StructOpt, Debug)]
#[structopt(name = env!("CARGO_BIN_NAME"),
            about = "Command-line interface for working with Estuary Flow projects",
            author = env!("CARGO_PKG_AUTHORS"))]
struct Args {
    /// The command to run
    #[structopt(subcommand)]
    command: Command,
    /// Make log output quieter. Can be used multiple times, with `-qq` suppressing all output.
    #[structopt(long, short = "q", global = true, parse(from_occurrences))]
    quiet: i32,
    /// Make log output more verbose. Can be used multiple times, with `-vvv` enabling all output.
    #[structopt(long, short = "v", global = true, parse(from_occurrences))]
    verbose: i32,
}

#[derive(StructOpt, Debug)]
enum Command {
    /// Builds a Catalog spec into a catalog database that can be deployed or inspected.
    Build(BuildArgs),
    /// Run a local development environment for the catalog.
    Develop(DevelopArgs),
    /// Runs catalog tests.
    Test(TestArgs),
    /// Print the catalog JSON schema.
    JsonSchema,

    /// Materialize a view of a Collection into a target database.
    Materialize(MaterializeArgs),
}

#[derive(StructOpt, Debug)]
struct BuildArgs {
    /// URL or filesystem path of the input specification source file.
    #[structopt(long, default_value = "flow.yaml")]
    source: String,
    /// URL or filesystem path of an existing catalog to use as the source. The resources in this
    /// catalog will be used to build a new catalog from scratch.
    #[structopt(long, conflicts_with("source"))]
    source_catalog: Option<String>,
    /// Path to output catalog database.
    #[structopt(long, default_value = "catalog.db")]
    catalog: String,
    /// Path to NodeJS package which will hold JavaScript lambdas. If this directory
    /// doesn't exist, it will be automatically created from a template. The package
    /// is used temporarily during the catalog build process -- it's compiled and
    /// then packed into the output catalog database -- but re-using the same directory
    /// across invocations will save time otherwise spent fetching npm packages.
    #[structopt(
        long = "nodejs-dir",
        default_value = "catalog-nodejs",
        env("FLOW_NODEJS_DIR")
    )]
    nodejs_package_path: String,
}

#[derive(StructOpt, Debug)]
struct DevelopArgs {
    /// Path to input catalog database.
    #[structopt(long, default_value = "catalog.db")]
    catalog: String,
}

#[derive(StructOpt, Debug)]
struct TestArgs {
    /// Path to input catalog database.
    #[structopt(long, default_value = "catalog.db")]
    catalog: String,
}

#[derive(StructOpt, Debug)]
struct MaterializeArgs {
    /// Path to input catalog database.
    #[structopt(long, default_value = "catalog.db")]
    catalog: String,

    /// The name of the materializationTarget to materialize to. This should match one of the
    /// `materializationTargets` from the catalog, and is used to specify the connection
    /// information that will be used by the materialization.
    #[structopt(long)]
    target: String,
    /// The name of the Flow Collection to materialize
    #[structopt(long)]
    collection: String,
    /// The name of the table within the target system that will hold the materialized records.
    /// This can be created automatically if it doesn't already exist.
    #[structopt(long)]
    table_name: String,

    /// Include all projected fields.
    #[structopt(long, conflicts_with("fields"))]
    all_fields: bool,
    /// Include a specific field. This option may be specified multiple times to specify the
    /// complete set of fields to include in the materialization. If you use --field, then you must
    /// explicitly specify all fields to materialize. These fields must include the collection's
    /// key. If the collection uses a composite key, then all of the pointers that constitute the
    /// key must be materialized.
    #[structopt(short = "f", long = "field", conflicts_with("all-fields"))]
    fields: Vec<String>,

    /// URL of the consumer. The default value is the localhost address that's used by `flowctl
    /// develop`.
    #[structopt(long, default_value = "http://localhost:9000")]
    consumer_address: String,

    /// Apply the SQL and the Shard Spec without asking for confirmation. Normally, you'll get an
    /// interactive confirmation asking if you'd like to apply these items. Passing `--yes`
    /// will skip that confirmation, making this command usable from a script.
    #[structopt(long)]
    yes: bool,

    /// Print out a summary of what would be done, without modifying anything. This will always
    /// take precedencs over `--yes`, if both arguments are provided.
    #[structopt(long)]
    dry_run: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::from_args();
    init_logging(&args);
    log::debug!("{:?}", args);

    let result = match args.command {
        Command::Build(build) => do_build(build),
        Command::Develop(develop) => do_develop(develop).await,
        Command::Test(test) => do_test(test).await,
        Command::JsonSchema => do_dump_schema(),
        Command::Materialize(materialize) => do_materialize(materialize).await,
    };

    if let Err(err) = result {
        log::error!("{:?}", err);
        std::process::exit(1);
    };
}

fn init_logging(args: &Args) {
    let mut builder = pretty_env_logger::formatted_timed_builder();

    // We subtract these so that each will cancel out occurrences of one another. This is sometimes
    // useful when the cli is being invoked by a script and allows passing additional arguments.
    let verbosity = args.verbose - args.quiet;

    // We use a different variable than RUST_LOG so that we
    let log_var = ::std::env::var("FLOWCTL_LOG");
    let log_filters = if let Ok(s) = &log_var {
        s
    } else {
        match verbosity {
            i32::MIN..=-2 => "off",
            -1 => "off,warn,flowctl=warn",
            0 => "warn,flowctl=info",
            1 => "info",
            2 => "debug",
            3..=i32::MAX => "trace",
        }
    };
    builder.parse_filters(log_filters);

    let _ = builder.try_init();

    if log_var.is_ok() && verbosity != 0 {
        log::warn!("The --quiet and --verbose arguments are being ignored since the `FLOWCTL_LOG` env variable is set");
    }
}

async fn do_materialize(args: MaterializeArgs) -> Result<(), Error> {
    use materialization::{CollectionSelection, FieldSelection};

    let db = catalog::open(args.catalog.as_str())?;
    let catalog_path = tokio::fs::canonicalize(args.catalog.as_str()).await?;
    let catalog_path = catalog_path.display().to_string();

    let collection_selection = CollectionSelection::Named(args.collection.clone());
    let collection = materialization::resolve_collection(&db, collection_selection)
        .context("unable to find a --collection with the given name")?;

    // TODO: follow the same selection pattern for looking up the materialization target
    let target = catalog::MaterializationTarget::get_by_name(&db, args.target.as_str())
        .context("unable to find a materialization --target with the given name")?;

    let field_selection = if !args.fields.is_empty() {
        FieldSelection::Named(args.fields.clone())
    } else if args.all_fields {
        FieldSelection::DefaultAll
    } else if is_interactive() {
        FieldSelection::InteractiveSelect
    } else {
        // We can't show the interactive selection UI since we're not running interactively, so the
        // only thing to do at this point is to return an error.
        anyhow::bail!("no fields were specified in the arguments. Please specify specific fields using --field arguments, or use --all-fields to materialize all of them")
    };
    let selected_projections =
        materialization::resolve_projections(collection.clone(), field_selection)?;
    let mut resolved_collection = collection;
    resolved_collection.projections = selected_projections;

    let initialization_string = materialization::generate_target_initializer(
        &db,
        target,
        args.target.as_str(),
        args.table_name.as_str(),
        &resolved_collection,
    )?;

    // This initialization text (SQL DDL, typically) is printed directly to stdout, bypassing the
    // log filters. This allows the output to be redirected, if desired.
    println!("{}", initialization_string);
    if !should_do(
        "apply the materialization ddl to the target database",
        &args,
    ) {
        log::info!("Skipping application of materialization");
        return Ok(());
    }
    // Ok, we're go for launch, but we need to do things in a sensible order so that we don't leave
    // things in a weird state if one of these steps fails. We'll first try to list shards, in
    // order to validate that we can connect to the flow-consumer successfully. If this works, then
    // applying the shards is likely to also work. What we want to avoid is a situation where we
    // apply the target initialization (which is _not_ idempotent) successfully, and then fail to
    // apply the shard specs. That would be problematic because if the user re-tried the same
    // command again, applying the target initialization may fail, since it's not idempotent.
    let cluster = runtime::Cluster {
        broker_address: String::new(),
        ingester_address: String::new(),
        consumer_address: args.consumer_address.clone(),
    };
    let response = cluster
        .list_shards(None)
        .await
        .context("connecting to flow-consumer")?;
    log::debug!(
        "Successfully connected to the consumer and listed {} existing shards",
        response.shards.len()
    );

    // Apply the materialization initialization payload (SQL DDL). We'll first write the
    // initialization text to a file, then pass that path over to `create_apply_command`, which
    // will return the command to run to apply it. For example, for postgres, this will return a
    // psql invocation.
    let payload_file = tempfile::NamedTempFile::new()?.into_temp_path().keep()?;
    tokio::fs::write(&payload_file, initialization_string.as_bytes()).await?;
    let apply_command = materialization::create_apply_command(
        &db,
        target,
        args.table_name.as_str(),
        payload_file.as_path(),
    )?;
    // print out the apply command arguments using the debug representation, so that strings
    // will be double quoted and internal quote characters will be escaped. This is helpful
    // since individual arguments may contain spaces, which would otherwise make this output
    // impossible to parse correctly.
    log::info!(
        "Materialization target apply command:\n{}",
        apply_command.iter().map(|s| format!("{:?}", s)).join(" ")
    );
    exec_external_command(apply_command.as_slice())
        .await
        .context("Failed to apply payload to materialization target")?;
    log::info!(
        "Successfully applied materialization DDL to the target '{}'",
        args.target.as_str()
    );

    // Finally, we're ready to apply the shards, which will actually start materializing into the
    // target system.
    let apply_shards_request = materialization::create_shard_apply_request(
        catalog_path.as_str(),
        args.collection.as_str(),
        args.target.as_str(),
        args.table_name.as_str(),
    );

    log::debug!("Gazette Shard Spec: {:#?}", apply_shards_request);
    let response = cluster
        .apply_shards(apply_shards_request)
        .await
        .context("updating shard specs")?;
    log::debug!("Successfully applied shards: {:?}", response);

    // TODO: consider polling the shard status until it indicates that it's running
    Ok(())
}

async fn exec_external_command(command: &[String]) -> Result<(), Error> {
    log::info!("Executing command: {}", command.iter().join(" "));
    let mut cmd = tokio::process::Command::new(&command[0]);
    for arg in command.iter().skip(1) {
        cmd.arg(arg);
    }
    let exit_status = cmd.spawn()?.await?;
    if exit_status.success() {
        Ok(())
    } else {
        Err(anyhow::format_err!(
            "command exited with failure: {:?}",
            exit_status
        ))
    }
}

// Returns true if _both_ stdin and stdout are a TTY, otherwise false.
fn is_interactive() -> bool {
    atty::is(atty::Stream::Stdin) && atty::is(atty::Stream::Stdout)
}

fn should_do(thing: &str, args: &MaterializeArgs) -> bool {
    if args.dry_run {
        false
    } else if args.yes {
        true
    } else if is_interactive() {
        let message = format!("Would you like to {}?", thing);
        get_user_confirmation(message.as_str())
    } else {
        false
    }
}

// Requests confirmation from the user, asking them to enter "y" to confirm or anything else to
// cancel. IO errors encountered while getting user input are implicitly interpreted as a desire to
// cancel. This function does not check whether stdin or stdout are a TTY, so **that check must be
// done before calling this**.
fn get_user_confirmation(message: &str) -> bool {
    use std::io::Write;
    print!(
        "{}\nEnter y to confirm, ctrl-c to abort, or anything else to skip: ",
        message
    );
    std::io::stdout()
        .flush()
        .expect("failed to write to stdout");
    let mut text = String::new();
    if let Err(err) = std::io::stdin().read_line(&mut text) {
        log::debug!("io error reading user input: {}", err);
        log::error!("Failed to read user input, cancelling action");
        false
    } else {
        text.as_str().trim().eq_ignore_ascii_case("y")
    }
}

fn do_build(args: BuildArgs) -> Result<(), Error> {
    let nodejs_dir = Path::new(args.nodejs_package_path.as_str());

    // Try to ensure that the source catalog is not the same as the destination. We do this before
    // calling `catalog::create` so that we don't truncate the `--source-catalog` if they are the
    // same. This check is not at all fool proof, since we're only checking the values of the
    // arguments, but it should be good enough to prevent someone from accidentally passing
    // `--source=catalog.db`.
    if args
        .source_catalog
        .as_deref()
        .map(|p| p.trim_start_matches("./"))
        == Some(args.catalog.as_str().trim_start_matches("./"))
    {
        anyhow::bail!(
            "invalid --source-catalog is the same as --catalog '{}'",
            args.catalog
        );
    }

    let db = catalog::create(&args.catalog)
        .context(format!("creating --catalog {:?}", &args.catalog))?;

    // Are we using an existing catalog or yaml as the source?
    if let Some(source_path) = args.source_catalog.as_deref() {
        let source = catalog::open_unchecked(source_path)
            .context(format!("opening --source-catalog {:?}", source_path))?;
        log::info!(
            "Building --source-catalog {:?} into --catalog {:?}",
            source_path,
            args.catalog
        );
        catalog::build_from_catalog(&db, &source, nodejs_dir)?;
    } else {
        // We're building from yaml
        let source_url = resolve_extant_url_or_file_arg("--source", &args.source)?;
        log::info!(
            "Building --source {:?} into --catalog {:?}",
            source_url,
            fs::canonicalize(&args.catalog)?
        );
        catalog::build(&db, source_url, nodejs_dir).context("building catalog")?;
    }

    log::info!("Successfully built catalog");
    Ok(())
}

fn resolve_extant_url_or_file_arg(arg_name: &str, value: impl AsRef<str>) -> Result<Url, Error> {
    let value = value.as_ref();
    let url = match url::Url::parse(value) {
        Ok(url) => url,
        Err(err) => {
            log::debug!(
                "{} {:?} is not a URL; assuming it's a filesystem path (parse error: {})",
                arg_name,
                value,
                err
            );
            let source = fs::canonicalize(value).context(format!(
                "finding {} {:?} in the local filesystem",
                arg_name, value,
            ))?;
            // Safe unwrap since we've canonicalized the path.
            url::Url::from_file_path(&source).unwrap()
        }
    };
    Ok(url)
}

async fn install_shards(
    catalog_path: &str,
    db: &rusqlite::Connection,
    cluster: &runtime::Cluster,
) -> Result<(), Error> {
    // Upsert shards for all derivations.
    let shards = cluster.list_shards(None).await?;
    let mut derivations = runtime::DerivationSet::try_from(shards).unwrap();
    derivations.update_from_catalog(&db)?;

    let apply = derivations.build_recovery_log_apply_request();
    cluster.apply_journals(apply).await?;

    let apply = derivations.build_shard_apply_request(&catalog_path);
    cluster.apply_shards(apply).await?;

    Ok(())
}

async fn start_local_runtime(
    gazette_port: u16,
    ingester_port: u16,
    consumer_port: u16,
    catalog_path: &str,
) -> Result<(runtime::Local, rusqlite::Connection), Error> {
    let catalog_path = std::fs::canonicalize(&catalog_path).context("opening --catalog")?;
    let catalog_path = catalog_path.to_string_lossy().to_string();
    let db = catalog::open(&catalog_path).context("opening --catalog")?;

    let local =
        runtime::Local::start(gazette_port, ingester_port, consumer_port, &catalog_path).await?;
    install_shards(&catalog_path, &db, &local.cluster)
        .await
        .context("failed to install specifications")?;

    Ok((local, db))
}

async fn do_develop(args: DevelopArgs) -> Result<(), Error> {
    let mut sigterm = unix::signal(unix::SignalKind::terminate())?;
    let mut sigint = unix::signal(unix::SignalKind::interrupt())?;

    let (local, _db) = start_local_runtime(8080, 8081, 9000, &args.catalog).await?;

    futures::select!(
        _ = sigterm.recv().fuse() => log::info!("caught SIGTERM; stopping"),
        _ = sigint.recv().fuse() => log::info!("caught SIGINT; stopping"),
    );
    local.stop().await.context("failed to stop local runtime")?;
    Ok(())
}

async fn do_test(args: TestArgs) -> Result<(), Error> {
    let (local, db) = start_local_runtime(0, 0, 0, &args.catalog).await?;

    let collections =
        testing::Collection::load_all(&db).context("failed to load catalog collections")?;
    let collection_dependencies = testing::Collection::load_transitive_dependencies(&db)
        .context("failed to load collection dependencies")?;
    let transforms =
        testing::Transform::load_all(&db).context("failed to load catalog transforms")?;
    let schema_index = build_schema_index(&db).context("failed to build schema index")?;

    let ctx = testing::Context {
        cluster: local.cluster.clone(),
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
            .context(format!("test case {} failed", name))?;
    }

    local.stop().await.context("failed to stop local runtime")?;
    Ok(())
}

fn do_dump_schema() -> Result<(), Error> {
    let settings = schemars::gen::SchemaSettings::draft07();
    let gen = schemars::gen::SchemaGenerator::new(settings);
    let schema = gen.into_root_schema_for::<catalog::specs::Catalog>();

    serde_json::to_writer_pretty(std::io::stdout(), &schema)?;
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

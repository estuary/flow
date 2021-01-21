use anyhow::{Context, Error};
use futures::FutureExt;
use itertools::Itertools;
use models::tables;
use std::convert::TryFrom;
use std::fs;
use std::path;
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
    Develop(BuildArgs),
    /// Runs catalog tests.
    Test(BuildArgs),
    /// Print the catalog JSON schema.
    JsonSchema,
    // Materialize a view of a Collection into a target database.
    // Materialize(MaterializeArgs),
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
    /// Path to the base build directory.
    #[structopt(long, default_value = ".")]
    base_directory: String,
}

/*
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
*/

#[tokio::main]
async fn main() {
    let args = Args::from_args();
    init_logging(&args);
    tracing::trace!("{:?}", args);

    let result = match args.command {
        Command::Build(build) => do_build(build).await,
        Command::Develop(develop) => do_develop(develop).await,
        Command::Test(test) => do_test(test).await,
        Command::JsonSchema => do_dump_schema(),
        // Command::Materialize(materialize) => do_materialize(materialize).await,
    };

    if let Err(err) = result {
        tracing::error!("{:?}", err);
        std::process::exit(1);
    };
}

fn init_logging(args: &Args) {
    // We subtract these so that each will cancel out occurrences of one another. This is sometimes
    // useful when the cli is being invoked by a script and allows passing additional arguments.
    let verbosity = args.verbose - args.quiet;

    // We use a different variable than RUST_LOG so we can avoid getting overwhelmed with output
    // from other tools that happen to stick with the default of RUST_LOG.
    let log_var = ::std::env::var("FLOW_LOG");
    let log_filters = if let Ok(s) = &log_var {
        s
    } else {
        match verbosity {
            i32::MIN..=-2 => "off",
            -1 => "off,warn,flowctl=warn",
            0 => "warn,flowctl=info",
            1 => "info,flowctl=debug",
            2 => "debug",
            3..=i32::MAX => "trace",
        }
    };

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(tracing_subscriber::EnvFilter::new(log_filters))
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("failed to set global tracing logger");

    // Pass `log` crate logs through as tracing events.
    tracing_log::LogTracer::init().expect("failed to set log => tracing shim");

    if log_var.is_ok() && verbosity != 0 {
        tracing::warn!("The --quiet and --verbose arguments are being ignored since the `FLOWCTL_LOG` env variable is set");
    }
}

/*
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
        tracing::info!("Skipping application of materialization");
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
    tracing::debug!(
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
    tracing::info!(
        "Materialization target apply command:\n{}",
        apply_command.iter().map(|s| format!("{:?}", s)).join(" ")
    );
    exec_external_command(apply_command.as_slice())
        .await
        .context("Failed to apply payload to materialization target")?;
    tracing::info!(
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

    tracing::debug!("Gazette Shard Spec: {:#?}", apply_shards_request);
    let response = cluster
        .apply_shards(apply_shards_request)
        .await
        .context("updating shard specs")?;
    tracing::debug!("Successfully applied shards: {:?}", response);

    // TODO: consider polling the shard status until it indicates that it's running
    Ok(())
}

async fn exec_external_command(command: &[String]) -> Result<(), Error> {
    tracing::info!("Executing command: {}", command.iter().join(" "));
    let mut cmd = tokio::process::Command::new(&command[0]);
    for arg in command.iter().skip(1) {
        cmd.arg(arg);
    }
    let exit_status = cmd.spawn()?.wait().await?;
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
        tracing::debug!("io error reading user input: {}", err);
        tracing::error!("Failed to read user input, cancelling action");
        false
    } else {
        text.as_str().trim().eq_ignore_ascii_case("y")
    }
}
*/

/// Common build steps which load and validate all tables, and generate & compile
/// the TypeScript package.
async fn build_common(args: BuildArgs) -> Result<(path::PathBuf, tables::All), Error> {
    let source_url = resolve_extant_url_or_file_arg("--source", &args.source)?;

    // TODO(johnny): args.source_path mode should use an existing catalog DB
    // to back a source::Fetcher, rather that using WebFetcher.

    let dir = &args.base_directory;
    std::fs::create_dir_all(&dir).context("failed to create package directory")?;
    let dir = std::fs::canonicalize(dir)?;

    let all_tables =
        build::load_and_validate(&source_url, build::WebFetcher::new(), build::Drivers::new())
            .await
            .context("build failed")?;

    if !all_tables.errors.is_empty() {
        for tables::Error { scope, error } in all_tables.errors.iter() {
            tracing::error!("{:?}\n    At: {}", error, scope);
        }
        return Ok((dir, all_tables));
    }

    build::generate_typescript_package(&all_tables, &dir)?;
    build::compile_typescript_package(&dir)?;

    Ok((dir, all_tables))
}

fn persist_database(
    all_tables: &tables::All,
    path: &path::Path,
) -> Result<rusqlite::Connection, anyhow::Error> {
    tracing::info!(?path, "writing catalog database");
    // Create or truncate the database at |path|.
    std::fs::write(&path, &[])?;
    let db = rusqlite::Connection::open(&path)?;

    tables::persist_tables(&db, &all_tables.as_tables())?;
    Ok(db)
}

async fn local_stack(
    args: BuildArgs,
    dynamic_ports: bool,
) -> Result<(runtime::Local, tables::All), anyhow::Error> {
    let temp_dir = tempfile::TempDir::new()?;
    let (package_dir, mut all_tables) = build_common(args).await?;

    // Install a testing JournalRule which orders after all other rules.
    let rule = models::names::Rule::new("\u{FFFF}\u{FFFF}-testing-overrides");
    all_tables.journal_rules.push_row(
        url::Url::parse("test://journal-rule")?,
        rule.clone(),
        protocol::flow::journal_rules::Rule {
            rule: rule.to_string(),
            selector: None, // Match all journals.
            template: Some(protocol::protocol::JournalSpec {
                replication: 1,
                fragment: Some(protocol::protocol::journal_spec::Fragment {
                    stores: vec!["file:///".to_string()],
                    compression_codec: protocol::protocol::CompressionCodec::None as i32,
                    ..Default::default()
                }),
                ..Default::default()
            }),
        },
    );

    let db_path = temp_dir.path().join("catalog.db");
    persist_database(&all_tables, &db_path)?;
    // Path is already absolute, so it's always safe to convert to Url.
    let db_url = Url::from_file_path(db_path.canonicalize()?).unwrap();

    if !all_tables.errors.is_empty() {
        anyhow::bail!("errors occurred while building catalog");
    }

    let (gazette_port, ingester_port, consumer_port) = if dynamic_ports {
        (0, 0, 0)
    } else {
        (8080, 8081, 9000)
    };

    let local = runtime::Local::start(
        temp_dir,
        &package_dir,
        &db_url,
        gazette_port,
        ingester_port,
        consumer_port,
    )
    .await?;

    // Upsert shards for all derivations.
    let shards = local.cluster.list_shards(None).await?;
    let mut shards = runtime::DerivationSet::try_from(shards).unwrap();
    shards.update_from_catalog(&all_tables.built_derivations);

    // TODO -- we must switch responsibility such that flow-consumer
    // creates recovery logs as needed, using journal rules.
    let apply = shards.build_recovery_log_apply_request();
    local.cluster.apply_journals(apply).await?;

    let apply = shards.build_shard_apply_request(db_url.path());
    local.cluster.apply_shards(apply).await?;

    Ok((local, all_tables))
}

fn resolve_extant_url_or_file_arg(arg_name: &str, value: impl AsRef<str>) -> Result<Url, Error> {
    let value = value.as_ref();
    let url = match url::Url::parse(value) {
        Ok(url) => url,
        Err(err) => {
            tracing::debug!(
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

async fn do_build(args: BuildArgs) -> Result<(), Error> {
    let (package_dir, mut all_tables) = build_common(args).await?;

    if all_tables.errors.is_empty() {
        let npm_resources = build::pack_typescript_package(&package_dir)?;
        all_tables.resources.extend(npm_resources.into_iter());
    }
    persist_database(&all_tables, &package_dir.join("catalog.db"))?;
    Ok(())
}

async fn do_develop(args: BuildArgs) -> Result<(), Error> {
    let (local, _) = local_stack(args, false).await?;

    let mut sigterm = unix::signal(unix::SignalKind::terminate())?;
    let mut sigint = unix::signal(unix::SignalKind::interrupt())?;

    futures::select!(
        _ = sigterm.recv().fuse() => tracing::info!("caught SIGTERM; stopping"),
        _ = sigint.recv().fuse() => tracing::info!("caught SIGINT; stopping"),
    );
    local.stop().await.context("failed to stop local runtime")?;
    Ok(())
}

async fn do_test(args: BuildArgs) -> Result<(), Error> {
    let (local, all_tables) = local_stack(args, true).await?;

    let mut graph = testing::Graph::new(&all_tables.transforms);
    let schema_index = tables::SchemaDoc::leak_index(&all_tables.schema_docs)?;

    for (test_name, steps) in all_tables
        .test_steps
        .iter()
        .sorted_by_key(|s| (s.test.to_string(), s.step_index))
        .group_by(|s| s.test.to_string())
        .into_iter()
    {
        tracing::info!(?test_name, "starting test case");
        let steps_vec: Vec<_> = steps.collect();

        testing::run_test_case(
            testing::Case(&steps_vec),
            &local.cluster,
            &all_tables.built_collections,
            &mut graph,
            schema_index,
        )
        .await
        .context(format!("test case {} failed", test_name))?;
    }

    local.stop().await.context("failed to stop local runtime")?;
    Ok(())
}

fn do_dump_schema() -> Result<(), Error> {
    let settings = schemars::gen::SchemaSettings::draft07();
    let gen = schemars::gen::SchemaGenerator::new(settings);
    let schema = gen.into_root_schema_for::<sources::Catalog>();

    serde_json::to_writer_pretty(std::io::stdout(), &schema)?;
    Ok(())
}

use clap;
use estuary::{
    catalog::{self, sql_params},
    derive, doc,
    specs::derive as specs,
};
use futures::{select, FutureExt};
use log::{error, info};
use pretty_env_logger;
use std::sync::{Arc, Mutex};
use tokio;
use tokio::signal::unix::{signal, SignalKind};
use url::Url;
use warp::Filter;

type Error = Box<dyn std::error::Error + 'static>;

#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    // Takes: recovery DIR.
    let matches = clap::App::new("derive-worker")
        .author("Estuary Technologies, Inc. \u{00A9}2020")
        .about("Worker process for deriving new collections from source collections")
        .subcommand(
            clap::SubCommand::with_name("run")
                .about("Runs the worker with a given configuration")
                .arg(
                    clap::Arg::with_name("config")
                        .short("c")
                        .long("config")
                        .takes_value(true)
                        .required(true)
                        .value_name("FILE")
                        .help("Path to JSON config specification"),
                ),
        )
        .get_matches();

    let result: Result<(), Error> = match matches.subcommand() {
        ("run", Some(sub)) => do_run(sub).await,
        _ => Ok(()),
    };
    if let Err(err) = result {
        error!("exiting with error: {}", err);
    };
}

fn parse_config(args: &clap::ArgMatches) -> Result<specs::Config, Error> {
    let cfg = args.value_of("config").unwrap();
    let cfg = std::fs::read(cfg).map_err(|e| format!("parsing config {:?}: {}", cfg, e))?;
    Ok(serde_json::from_slice::<specs::Config>(&cfg)?)
}

async fn do_run<'a>(args: &'a clap::ArgMatches<'a>) -> Result<(), Error> {
    let cfg = parse_config(args)?;

    // Open catalog DB.
    let db = catalog::open(&cfg.catalog)?;

    let derivation_id = db
        .prepare(
            "SELECT collection_id
                FROM collections NATURAL JOIN derivations
                WHERE name = ?",
        )?
        .query_row(sql_params![cfg.derivation], |r| r.get(0))
        .map_err(|err| catalog::Error::At {
            loc: format!("querying for derived collection {:?}", cfg.derivation),
            detail: Box::new(err.into()),
        })?;

    // "Open" recovered state store, instrumented with a Recorder.
    // TODO rocksdb, sqlite, Go CGO bindings to client / Recorder, blah blah.
    let store = Box::new(derive::state::MemoryStore::new());
    let store = Arc::new(Mutex::new(store));

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

    log::info!("loaded {} JSON-Schemas from catalog", schemas.len());

    // Start NodeJS transform worker.
    let loopback = Url::from_file_path(&cfg.socket_path).unwrap();
    let node = derive::nodejs::Service::new(&db, loopback)?;

    let txn_ctx = derive::transform::Context::new(&db, derivation_id, node, schema_index)?;
    let txn_ctx = Arc::new(Box::new(txn_ctx));

    // Build service.
    let service = derive::state::build_service(store)
        .or(derive::build_service(txn_ctx.clone()))
        .boxed();

    // Register for shutdown signals and wire up a future.
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;

    let stop = async move {
        select!(
            _ = sigterm.recv().fuse() => info!("caught SIGTERM; stopping"),
            _ = sigint.recv().fuse() => info!("caught SIGINT; stopping"),
        );
    };

    // Bind local listener and begin serving.
    let server = estuary::serve::unix_domain_socket(service, &cfg.socket_path, stop);
    let server_handle = tokio::spawn(server);

    // Invoke derivation bootstraps.
    txn_ctx.node.bootstrap(derivation_id).await?;

    // Signal to host process that we're ready to accept connections.
    println!("READY");
    Ok(server_handle.await?)
}

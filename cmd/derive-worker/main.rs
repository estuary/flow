use clap;
use estuary::derive;
use estuary::specs::derive as specs;
use log::{error, info};
use pretty_env_logger;
use std::sync::{Arc, Mutex};
use tokio;
use tokio::signal::unix::{signal, SignalKind};
use futures::{select, FutureExt};

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
    let cfg = std::fs::read(cfg)
        .map_err(|e| format!("parsing config {:?}: {}", cfg, e))?;
    Ok(serde_json::from_slice::<specs::Config>(&cfg)?)
}

async fn do_run<'a>(args: &'a clap::ArgMatches<'a>) -> Result<(), Error> {
    let cfg = parse_config(args)?;

    // "Open" recovered state store, instrumented with a Recorder.
    // TODO rocksdb, sqlite, Go CGO bindings to client / Recorder, blah blah.
    let store = Box::new(derive::state::MemoryStore::new());
    let store = Arc::new(Mutex::new(store));

    // Build service.
    // TODO include message transaction flow filters in this service.
    let service = derive::state::build_service(store);

    // Register for shutdown signals and wire up a future.
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;

    let stop = async move {
        select!(
            _ = sigterm.recv().fuse() => {
                info!("caught SIGTERM; stopping");
            }
            _ = sigint.recv().fuse() => {
                info!("caught SIGINT; stopping");
            }
        );
    };

    // Bind local listener and begin serving.
    let server = estuary::serve::unix_domain_socket(service, cfg.socket_path, stop);
    let server_handle = tokio::spawn(server);

    // Signal to host process that we're ready to accept connections.
    println!("READY");
    Ok(server_handle.await?)
}

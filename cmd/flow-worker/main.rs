use estuary::{
    catalog::{self},
    derive, doc,
};
use estuary_protocol::flow;
use futures::{select, FutureExt};
use log::{error, info};
use std::path::PathBuf;
use std::sync::Arc;
use structopt::StructOpt;
use tokio::signal::unix::{signal, SignalKind};
use tower::Service;

#[derive(StructOpt, Debug)]
struct ExtractCommand {
    #[structopt(
        long,
        parse(from_os_str),
        help = "Unix domain socket to listen on for gRPC connections."
    )]
    grpc_socket_path: PathBuf,
}

#[derive(StructOpt, Debug)]
struct CombineCommand {
    #[structopt(long, parse(from_os_str), help = "Path to the catalog database.")]
    catalog: PathBuf,
    #[structopt(
        long,
        parse(from_os_str),
        help = "Unix domain socket to listen on for gRPC connections."
    )]
    grpc_socket_path: PathBuf,
}

#[derive(StructOpt, Debug)]
struct DeriveCommand {
    #[structopt(long, parse(from_os_str), help = "Path to the catalog database.")]
    catalog: PathBuf,
    #[structopt(long, help = "Name of the collection to derive.")]
    derivation: String,
    #[structopt(
        long,
        parse(from_os_str),
        help = "Unix domain socket to listen on for gRPC connections."
    )]
    grpc_socket_path: PathBuf,
    #[structopt(long, help = "Directory of the local state database.")]
    dir: String,
    #[structopt(long, help = "Author under which recovery log operations are fenced.")]
    author: u32,
    #[structopt(long, help = "Path to JSON-encoded recovery-log FSM state.")]
    fsm_path: String,
}

#[derive(StructOpt, Debug)]
#[structopt(
    author = "Estuary Technologies, Inc. \u{00A9}2020",
    about = "Worker side-car process of Estuary Flow, for deriving and extracting documents"
)]
enum Command {
    Extract(ExtractCommand),
    Combine(CombineCommand),
    Derive(DeriveCommand),
}

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

#[tokio::main]
async fn main() {
    pretty_env_logger::init();
    let cmd = Command::from_args();

    let result = match cmd {
        Command::Extract(cmd) => cmd.run().await,
        Command::Combine(cmd) => cmd.run().await,
        Command::Derive(cmd) => cmd.run().await,
    };
    if let Err(err) = result {
        error!("exiting with error: {}", err);
    };
}

impl ExtractCommand {
    async fn run(&self) -> Result<(), Error> {
        let mut extract_api = flow::extract_server::ExtractServer::new(derive::extract_api::API {});
        let service =
            tower::service_fn(move |req: hyper::Request<hyper::Body>| extract_api.call(req));

        // Bind local listener and begin serving.
        let server = estuary::serve::unix_domain_socket(
            service,
            &self.grpc_socket_path,
            register_signal_handlers()?,
        );
        let server_handle = tokio::spawn(server);

        // Signal to host process that we're ready to accept connections.
        println!("READY");
        server_handle.await?;

        Ok(())
    }
}

impl CombineCommand {
    async fn run(&self) -> Result<(), Error> {
        // Open catalog DB & build schema index.
        let db = catalog::open(&self.catalog)?;
        let schema_index = build_schema_index(&db)?;

        let mut combine_api =
            flow::combine_server::CombineServer::new(derive::combine_api::API::new(schema_index));
        let service =
            tower::service_fn(move |req: hyper::Request<hyper::Body>| combine_api.call(req));

        // Bind local listener and begin serving.
        let server = estuary::serve::unix_domain_socket(
            service,
            &self.grpc_socket_path,
            register_signal_handlers()?,
        );
        let server_handle = tokio::spawn(server);

        // Signal to host process that we're ready to accept connections.
        println!("READY");
        server_handle.await?;

        Ok(())
    }
}

impl DeriveCommand {
    async fn run(&self) -> Result<(), Error> {
        // Open catalog DB & build schema index.
        let db = catalog::open(&self.catalog)?;
        let schema_index = build_schema_index(&db)?;

        // Start NodeJS transform worker.
        let node = derive::nodejs::NodeRuntime::start(&db)?;

        // Build derivation context.
        let ctx = derive::context::Context::build_from_catalog(
            &db,
            &self.derivation,
            schema_index,
            &node,
        )?;
        let ctx = Arc::new(ctx);

        // Open local RocksDB.
        let mut rocks_opts = rocksdb::Options::default();
        rocks_opts.create_if_missing(true);
        rocks_opts.create_missing_column_families(true);
        rocks_opts.set_env(&rocksdb::Env::default()?);

        let rocks_db = rocksdb::DB::open_cf(
            &rocks_opts,
            &self.dir,
            [
                rocksdb::DEFAULT_COLUMN_FAMILY_NAME,
                crate::derive::registers::REGISTERS_CF,
            ]
            .iter(),
        )?;

        let registers = derive::registers::Registers::new(
            rocks_db,
            schema_index,
            &ctx.register_schema,
            ctx.register_initial.clone(),
        );

        let mut derive_api = flow::derive_server::DeriveServer::new(derive::derive_api::API::new(
            ctx.clone(),
            registers,
        ));
        let service =
            tower::service_fn(move |req: hyper::Request<hyper::Body>| derive_api.call(req));

        // Bind local listener and begin serving.
        let server = estuary::serve::unix_domain_socket(
            service,
            &self.grpc_socket_path,
            register_signal_handlers()?,
        );
        let server_handle = tokio::spawn(server);

        // Invoke any user-provide runtime bootstraps.
        node.invoke_bootstrap(ctx.derivation_id).await?;

        // Signal to host process that we're ready to accept connections.
        println!("READY");
        server_handle.await?;

        Ok(())
    }
}

fn register_signal_handlers() -> Result<impl std::future::Future<Output = ()>, Error> {
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;

    Ok(async move {
        select!(
            _ = sigterm.recv().fuse() => info!("caught SIGTERM; stopping"),
            _ = sigint.recv().fuse() => info!("caught SIGINT; stopping"),
        );
    })
}

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

    info!("loaded {} JSON-Schemas from catalog", schemas.len());

    Ok(schema_index)
}

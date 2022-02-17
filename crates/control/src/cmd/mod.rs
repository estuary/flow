use std::path::PathBuf;

use tokio::runtime::Runtime;

pub mod id_mask;
pub mod seed;
pub mod serve;
pub mod setup;

#[derive(clap::Args, Debug)]
pub struct ControlPlaneArgs {
    #[clap(subcommand)]
    pub cmd: Cmd,
}

#[derive(Debug, clap::Subcommand)]
pub enum Cmd {
    /// Encode and decode id values for debugging.
    IdMask(id_mask::Args),
    /// Seeds the database with data required for the server to function.
    Seed(seed::Args),
    /// Runs the api server.
    Serve(serve::Args),
    /// Setup a new Control Plane
    Setup(setup::Args),
}

pub fn run(args: ControlPlaneArgs) -> anyhow::Result<()> {
    match args.cmd {
        Cmd::IdMask(args) => id_mask::run(args),
        Cmd::Seed(args) => seed::run(args),
        Cmd::Serve(args) => serve::run(args),
        Cmd::Setup(args) => setup::run(args),
    }
}

#[derive(clap::Args, Debug)]
pub struct ConfigArgs {
    /// Load the application configuration from the supplied path.
    #[clap(short, long = "config")]
    config_path: PathBuf,
}

fn async_runtime() -> std::io::Result<Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
}

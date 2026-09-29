// Links in the allocator crate, which sets the global allocator to jemalloc,
// as it is for runtime-next within production's reactor (through `bindings`).
extern crate allocator;

use clap::Parser;

/// A reproduction lab for runtime-next and shuffle. See crates/runtime-lab/README.md.
#[derive(Debug, clap::Parser)]
#[clap(rename_all = "kebab-case")]
enum Command {
    /// Run an experiment's topology.
    Controller(runtime_lab::controller::Args),
    /// Serve a host's shards (started by the controller).
    Reactor(runtime_lab::reactor::Args),
    /// Serve a host's Leader and shuffle services (started by the controller).
    Sidecar(runtime_lab::sidecar::Args),
}

fn main() -> anyhow::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

    let command = Command::parse();
    let registry = service_kit::Registry::new();
    runtime_lab::install_tracing(registry.clone());

    match command {
        Command::Controller(args) => runtime_lab::controller::run(args, registry),
        Command::Reactor(args) => runtime_lab::reactor::run(args, registry),
        Command::Sidecar(args) => runtime_lab::sidecar::run(args, registry),
    }
}

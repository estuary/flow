use clap::Parser;

/// Release abandoned VMM resources in one host state directory.
#[derive(clap::Parser)]
struct Args {
    /// The launcher's private state directory, also used as its Podman label.
    #[arg(long, env = "CONNECTOR_VMM_STATE_DIR")]
    state_dir: String,
    /// The Podman executable for the launcher's rootful engine.
    #[arg(long, env = "CONNECTOR_VMM_PODMAN", default_value = "podman")]
    podman: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let podman = if args.podman.is_empty() {
        "podman"
    } else {
        &args.podman
    };
    if !connector::prune_vmm(podman, &args.state_dir).await? {
        eprintln!("prune already running");
    }
    Ok(())
}

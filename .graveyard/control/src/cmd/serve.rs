use futures::TryFutureExt;
use std::net::TcpListener;

use crate::cmd::{async_runtime, ConfigArgs};
use crate::config;
use crate::context::AppContext;
pub use crate::services::builds_root::init_builds_root;
use crate::{shutdown, startup};

#[derive(clap::Args, Debug)]
pub struct Args {
    #[clap(flatten)]
    config: ConfigArgs,
}

/// Runs the control plane server.
pub fn run(args: Args) -> anyhow::Result<()> {
    config::load_settings(args.config.config_path)?;
    let runtime = async_runtime()?;
    let listener = TcpListener::bind(config::settings().application.address())?;

    runtime.block_on(async move {
        // Run the server until it decides to shut down
        serve(listener).await
    })
}

async fn serve(listener: TcpListener) -> anyhow::Result<()> {
    let db = startup::connect_to_postgres(&config::settings().database).await;
    let (put_builds, fetch_builds) = init_builds_root(&config::settings().builds_root)?;
    let ctx = AppContext::new(db, put_builds, fetch_builds);

    // TODO(johnny): For now, we run the API server and builder daemon together.
    // We'll probably want to separate and independently deploy & scale these.
    let server = startup::run(listener, ctx.clone())?.map_err(Into::into);

    let builder_daemon =
        crate::services::builder::serve_builds(ctx.clone(), shutdown::signal()).map_err(Into::into);

    // Run until the builder_daemon and server both exit.
    let out: Result<_, anyhow::Error> = futures::try_join!(server, builder_daemon);
    out?;

    Ok(())
}

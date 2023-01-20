use anyhow::Context;
use codec::Codec;
use tokio::signal::unix;

use firecracker_init::init_firecracker;

mod capture;
mod codec;
mod config;
mod firecracker_init;
mod materialize;
mod rpc;
mod util;

#[derive(clap::Parser, Debug)]
#[clap(about = "Command to start connector proxies for Flow runtime.")]
pub struct Args {
    /// The path (in the container) to the JSON file that contains the
    /// inspection results from the connector image.
    /// Normally produced via command "docker inspect <image>".
    #[clap(short, long)]
    pub image_inspect_json_path: String,

    /// Port on which to listen for requests from the runtime.
    #[clap(short, long, default_value = "8080")]
    pub port: u16,

    /// Whether or not we're running as the init program inside a firecracker VM
    #[clap(
        short,
        long,
        default_value = "false",
        requires = "guest_config_json_path"
    )]
    pub firecracker: bool,

    /// The path to the JSON file that contains the [GuestConfig] for this task
    #[clap(short, long)]
    pub guest_config_json_path: Option<String>,
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    let mut image = config::Image::parse_from_json_file(&args.image_inspect_json_path)
        .context("reading image inspect JSON")?;

    if args.firecracker {
        let guest_config = config::GuestConfig::parse_from_json_file(
            &args
                .guest_config_json_path
                .expect("Must provide a guest config when running in firecracker mode"),
        )
        .context("reading image inspect JSON")?;

        init_firecracker(&mut image.config, &guest_config).await?;
    }

    let mut entrypoint = image.get_argv()?;

    // TODO(johnny): Remove this in preference of always using the LOG_LEVEL variable.
    if let Ok(log_level) = std::env::var("LOG_LEVEL") {
        entrypoint.push(format!("--log.level={log_level}"));
    }

    let codec = match image.config.labels.get("FLOW_RUNTIME_CODEC") {
        Some(protocol) if protocol == "json" => Codec::Json,
        _ => Codec::Proto,
    };
    let capture = proto_grpc::capture::driver_server::DriverServer::new(capture::Driver {
        entrypoint: entrypoint.clone(),
        codec,
        envs: image.config.env.clone(),
    });
    let materialize =
        proto_grpc::materialize::driver_server::DriverServer::new(materialize::Driver {
            entrypoint: entrypoint.clone(),
            codec,
            envs: image.config.env.clone(),
        });

    let addr = format!("0.0.0.0:{}", args.port).parse().unwrap();

    // Gracefully exit on either SIGINT (ctrl-c) or SIGTERM.
    let mut sigint = unix::signal(unix::SignalKind::interrupt()).unwrap();
    let mut sigterm = unix::signal(unix::SignalKind::terminate()).unwrap();

    let signal = async move {
        tokio::select! {
            _ = sigint.recv() => (),
            _ = sigterm.recv() => (),
        }
        tracing::info!("caught signal to exit");
    };

    let () = tonic::transport::Server::builder()
        .add_service(capture)
        .add_service(materialize)
        .serve_with_shutdown(addr, signal)
        .await?;

    Ok(())
}

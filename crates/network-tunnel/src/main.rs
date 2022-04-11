pub mod interface;
pub mod sshforwarding;
pub mod errors;
pub mod networktunnel;

use errors::Error;
use flow_cli_common::{init_logging, LogArgs, LogFormat};
use std::io::{self, Write};

use interface::NetworkTunnelConfig;

#[tokio::main]
async fn main() -> io::Result<()> {
    init_logging(&LogArgs{level: "info".to_string(), format: Some(LogFormat::Json)});
    if let Err(err) = run().await.as_ref() {
        tracing::error!(error = ?err, "network tunnel failed.");
        std::process::exit(1);
    }
    Ok(())
}

async fn run() -> Result<(), Error> {
    let tunnel_config: NetworkTunnelConfig = serde_json::from_reader(io::stdin())?;
    let mut tunnel = tunnel_config.new_tunnel();

    tunnel.prepare().await?;

    // Write "READY" to stdio to unblock Go logic.
    // The current workflow assumes that
    //   1. After tunnel.prepare() is called, the network tunnel is able to accept requests from clients without sending errors back to clients.
    //   2. The network tunnel is able to process client requests immediately after `tunnel.start_serve` is called.
    // If either of the assumptions is invalid for any new tunnel type, the READY-logic need to be moved to a separate task, which
    //    sends out the "READY" signal after making sure the network tunnel is started and working properly.
    println!("READY");

    tunnel.start_serve().await?;

    Ok(())
}
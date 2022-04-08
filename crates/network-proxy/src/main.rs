pub mod interface;
pub mod sshforwarding;
pub mod errors;
pub mod networkproxy;

use errors::Error;
use flow_cli_common::{init_logging, LogArgs, LogFormat};
use std::io::{self, Write};

use interface::NetworkProxyConfig;

#[tokio::main]
async fn main() -> io::Result<()> {
    init_logging(&LogArgs{level: "info".to_string(), format: Some(LogFormat::Json)});
    if let Err(err) = run().await.as_ref() {
        tracing::error!(error = ?err, "network proxy failed.");
        std::process::exit(1);
    }
    Ok(())
}

async fn run() -> Result<(), Error> {
    let proxy_config: NetworkProxyConfig = serde_json::from_reader(io::stdin())?;
    let mut proxy = proxy_config.new_proxy();

    proxy.prepare().await?;

    // Write "READY" to stdio to unblock Go logic.
    // The current workflow assumes that
    //   1. After proxy.prepare() is called, the network proxy is able to accept requests from clients without sending errors back to clients.
    //   2. The network proxy is able to process client requests immediately after `proxy.start_serve` is called.
    // If either of the assumptions is invalid for any new proxy type, the READY-logic need to be moved to a separate task, which
    //    sends out the "READY" signal after making sure the network proxy is started and working properly.
    println!("READY");

    proxy.start_serve().await?;

    Ok(())
}
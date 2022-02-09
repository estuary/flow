pub mod interface;
pub mod sshforwarding;
pub mod errors;
pub mod logging;
pub mod networkproxy;

use std::io::{self, Write};

use interface::NetworkProxyConfig;
use logging::{init_tracing, Must};

#[tokio::main]
async fn main() -> io::Result<()> {
    init_tracing();
    let proxy_config: NetworkProxyConfig = serde_json::from_reader(io::stdin()).or_bail("Failed to deserialize network proxy config.");
    let mut proxy = proxy_config.new_proxy();

    proxy.prepare().await.or_bail("Failed to prepare network proxy.");

    // Write "READY" to stdio to unblock Go logic.
    // The current workflow assumes that
    //   1. After proxy.prepare() is called, the network proxy is able to accept requests from clients without sending errors back to clients.
    //   2. The network proxy is able to process client requests immediately after `proxy.start_serve` is called.
    // If either of the assumptions is invalid for any new proxy type, the READY-logic need to be moved to a separate task, which
    //    sends out the "READY" signal after making sure the network proxy is started and working properly.
    println!("READY");
    io::stdout().flush().or_bail("Failed flushing output.");

    proxy.start_serve().await.or_bail("Failed to start proxy service.");

    Ok(())
}
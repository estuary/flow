pub mod errors;
pub mod interface;
pub mod networktunnel;
pub mod sshforwarding;

use errors::Error;
use flow_cli_common::{init_logging, LogArgs, LogFormat, LogLevel};
use futures::future::{self, TryFutureExt};
use std::io::{self};

use interface::NetworkTunnelConfig;

#[tokio::main]
async fn main() -> io::Result<()> {
    init_logging(&LogArgs {
        level: LogLevel::Info,
        format: Some(LogFormat::Json),
    });
    if let Err(err) = run().await.as_ref() {
        tracing::error!(error = ?err, "network tunnel failed.");
        std::process::exit(1);
    }
    Ok(())
}

async fn run_and_cleanup(tunnel: &mut Box<dyn networktunnel::NetworkTunnel>) -> Result<(), Error> {
    let tunnel_block = {
        let prep = tunnel.prepare().await;

        // Write "READY" to stdio to unblock Go logic.
        // The current workflow assumes that
        //   1. After tunnel.prepare() is called, the network tunnel is able to accept requests from clients without sending errors back to clients.
        //   2. The network tunnel is able to process client requests immediately after `tunnel.start_serve` is called.
        // If either of the assumptions is invalid for any new tunnel type, the READY-logic need to be moved to a separate task, which
        //    sends out the "READY" signal after making sure the network tunnel is started and working properly.
        println!("READY");

        future::ready(prep).and_then(|()| {
            tunnel.start_serve()
        }).await
    };

    // We must make sure we cleanup the child process. This is specially important
    // as processes that are not `wait`ed on can end up as zombies in some operating
    // systems (see https://doc.rust-lang.org/std/process/struct.Child.html#warning)
    tunnel.cleanup().await?;

    tunnel_block
}

async fn run() -> Result<(), Error> {
    let tunnel_config: NetworkTunnelConfig = serde_json::from_reader(io::stdin())?;
    let mut tunnel = tunnel_config.new_tunnel();

    run_and_cleanup(&mut tunnel).await
}

#[cfg(test)]
mod test {
    use std::any::Any;

    use async_trait::async_trait;
    use crate::errors::Error;
    use crate::networktunnel::NetworkTunnel;

    use crate::run_and_cleanup;

    #[derive(Debug)]
    struct TestTunnel {
        cleanup_called: bool,
        error_in_prepare: bool,
        error_in_serve: bool,
    }

    #[async_trait]
    impl NetworkTunnel for TestTunnel {
        fn adjust_endpoint_spec(&mut self, endpoint_spec: serde_json::Value) -> Result<serde_json::Value, Error> {
            Ok(endpoint_spec)
        }

        async fn prepare(&mut self) -> Result<(), Error> {
            if self.error_in_prepare {
                return Err(Error::TunnelExitNonZero("prepare-error".to_string()))
            }

            Ok(())
        }

        async fn start_serve(&mut self) -> Result<(), Error> {
            if self.error_in_serve {
                return Err(Error::TunnelExitNonZero("serve-error".to_string()))
            }

            Ok(())
        }

        async fn cleanup(&mut self) -> Result<(), Error> {
            self.cleanup_called = true;

            Ok(())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }


    #[tokio::test]
    async fn test_cleanup_call_error_in_prepare() {
        let mut tunnel : Box<dyn NetworkTunnel> = Box::new(TestTunnel {
            cleanup_called: false,
            error_in_prepare: true,
            error_in_serve: false,
        });

        let result = run_and_cleanup(&mut tunnel).await;
        assert!(result.is_err());

        let test_tunnel = tunnel.as_any().downcast_ref::<TestTunnel>().unwrap();
        assert!(test_tunnel.cleanup_called);
    }

    #[tokio::test]
    async fn test_cleanup_call_error_in_serve() {
        let mut tunnel : Box<dyn NetworkTunnel> = Box::new(TestTunnel {
            cleanup_called: false,
            error_in_prepare: false,
            error_in_serve: true,
        });

        let result = run_and_cleanup(&mut tunnel).await;
        assert!(result.is_err());

        let test_tunnel = tunnel.as_any().downcast_ref::<TestTunnel>().unwrap();
        assert!(test_tunnel.cleanup_called);
    }

    #[tokio::test]
    async fn test_cleanup_call_success() {
        let mut tunnel : Box<dyn NetworkTunnel> = Box::new(TestTunnel {
            cleanup_called: false,
            error_in_prepare: false,
            error_in_serve: false,
        });

        let result = run_and_cleanup(&mut tunnel).await;
        assert!(result.is_ok());

        let test_tunnel = tunnel.as_any().downcast_ref::<TestTunnel>().unwrap();
        assert!(test_tunnel.cleanup_called);
    }
}

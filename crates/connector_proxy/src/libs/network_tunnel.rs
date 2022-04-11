use crate::errors::Error;
use crate::libs::json::{create_root_schema, remove_subobject};
use network_tunnel::interface::NetworkTunnelConfig;

use schemars::schema::{RootSchema, Schema};
use serde_json::value::RawValue;
use tokio::sync::oneshot::{self, Receiver};
use tokio::time::timeout;

pub struct NetworkTunnel {}
pub const NETWORK_TUNNEL_KEY: &str = "networkTunnel";

impl NetworkTunnel {
    pub fn extend_endpoint_schema(
        endpoint_spec_schema: Box<RawValue>,
    ) -> Result<Box<RawValue>, Error> {
        let network_tunnel_schema = create_root_schema::<NetworkTunnelConfig>();

        let mut modified_schema: RootSchema = serde_json::from_str(endpoint_spec_schema.get())?;
        if let Some(ref mut o) = &mut modified_schema.schema.object {
            if o.as_ref().properties.contains_key(NETWORK_TUNNEL_KEY) {
                return Err(Error::DuplicatedKeyError(NETWORK_TUNNEL_KEY));
            }
            o.as_mut().properties.insert(
                NETWORK_TUNNEL_KEY.to_string(),
                Schema::Object(network_tunnel_schema.schema),
            );
        }

        let json = serde_json::to_string_pretty(&modified_schema)?;
        RawValue::from_string(json).map_err(Into::into)
    }

    // Start the network tunnel. The receiver rx will be dropped to indicate the network tunnel
    // is ready to accept requests.
    async fn start_network_tunnel(
        config: NetworkTunnelConfig,
        rx: Receiver<()>,
    ) -> Result<(), Error> {
        let mut network_tunnel = config.new_tunnel();
        tokio::task::spawn(async move {
            let result: Result<(), Error> = match network_tunnel.prepare().await {
                Ok(()) => {
                    drop(rx);
                    network_tunnel.start_serve().await.map_err(Into::into)
                }
                Err(e) => Err(e.into()),
            };

            if let Err(ref err) = result {
                tracing::error!(error=?err, "failed starting network tunnel.");
                std::process::exit(1);
            }
        })
        .await?;

        Ok(())
    }

    pub async fn consume_network_tunnel_config(
        endpoint_spec_json: Box<RawValue>,
    ) -> Result<Box<RawValue>, Error> {
        if endpoint_spec_json.get().is_empty() {
            return Ok(endpoint_spec_json);
        }

        let endpoint_spec = serde_json::from_str(endpoint_spec_json.get())?;
        let (network_tunnel_config, endpoint_spec) =
            remove_subobject(endpoint_spec, NETWORK_TUNNEL_KEY);

        let network_tunnel_config: NetworkTunnelConfig = match network_tunnel_config {
            None => return Ok(endpoint_spec_json),
            Some(c) => serde_json::from_value(c)?,
        };

        let (mut tx, rx) = oneshot::channel();
        tokio::spawn(Self::start_network_tunnel(network_tunnel_config, rx));

        // TODO: Refact the network-tunnel and remove the timeout logic here after all connectors are converted to work with connector-proxy.

        // Block for at most 5 seconds for network tunnel to be prepared.
        if let Err(_) = timeout(std::time::Duration::from_secs(5), tx.closed()).await {
            return Err(Error::ChannelTimeoutError);
        };

        tracing::info!("network tunnel started.");

        let json = serde_json::to_string_pretty(&endpoint_spec)?;
        RawValue::from_string(json).map_err(Into::into)
    }
}

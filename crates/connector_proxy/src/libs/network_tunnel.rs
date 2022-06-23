use crate::errors::Error;
use crate::libs::json::{create_root_schema, remove_subobject};
use network_tunnel::interface::NetworkTunnelConfig;

use schemars::schema::{RootSchema, Schema, SchemaObject};
use schemars::visit::{visit_root_schema, visit_schema_object, Visitor};
use serde_json::value::RawValue;
use tokio::sync::oneshot::{self, Receiver};
use tokio::time::timeout;

pub struct NetworkTunnel {}
pub const NETWORK_TUNNEL_KEY: &str = "networkTunnel";
pub const ENDPOINT_ADDRESS_KEY: &str = "address";

// The RemoveForwardHost visitor is used to conditionally remove some of
// the SSH Forwarding config properties if (and only if) we detect that the
// underlying connector has an 'address' property which makes them redundant.
//
// There are very few ways of *conditionally* including/excluding a struct
// field from the generated JSON schemas, so using a visitor to remove them
// after the fact was the best way of accomplishing this.
#[derive(Debug, Clone)]
pub struct RemoveSSHForwardHost;
impl Visitor for RemoveSSHForwardHost {
    fn visit_schema_object(&mut self, schema: &mut SchemaObject) {
        if let Some(metadata) = &schema.metadata {
            if metadata.title == Some("SSH Tunnel".to_string()) {
                if let Some(obj) = &mut schema.object {
                    obj.properties.remove("forwardHost");
                    obj.properties.remove("forwardPort");
                    obj.properties.remove("localPort");
                }
            }
        }
        visit_schema_object(self, schema);
    }
}

impl NetworkTunnel {
    pub fn extend_endpoint_schema(
        endpoint_spec_schema: Box<RawValue>,
    ) -> Result<Box<RawValue>, Error> {
        let mut network_tunnel_schema = create_root_schema::<NetworkTunnelConfig>();
        let mut modified_schema: RootSchema = serde_json::from_str(endpoint_spec_schema.get())?;
        if let Some(ref mut o) = &mut modified_schema.schema.object {
            if o.as_ref().properties.contains_key(NETWORK_TUNNEL_KEY) {
                return Err(Error::DuplicatedKeyError(NETWORK_TUNNEL_KEY));
            }

            if o.as_ref().properties.contains_key(ENDPOINT_ADDRESS_KEY) {
                visit_root_schema(&mut RemoveSSHForwardHost, &mut network_tunnel_schema);
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
        mut network_tunnel: Box<dyn network_tunnel::networktunnel::NetworkTunnel>,
        rx: Receiver<()>,
    ) {
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
            Some(serde_json::Value::Null) => return Ok(serde_json::from_value(endpoint_spec)?),
            Some(c) => serde_json::from_value(c)?,
        };

        tracing::info!("starting network tunnel");
        let (mut tx, rx) = oneshot::channel();

        // TODO: Refact the network-tunnel and remove the timeout logic here after all connectors are converted to work with connector-proxy.
        let mut network_tunnel = network_tunnel_config.new_tunnel();
        let endpoint_spec = network_tunnel.adjust_endpoint_spec(endpoint_spec)?;
        tokio::spawn(Self::start_network_tunnel(network_tunnel, rx));

        // Block for at most 6 seconds for network tunnel to be prepared. This
        // is one second longer than the SSH client is given, so in the common
        // case of an unresponsive SSH server the timeout should come from that.
        if let Err(_) = timeout(std::time::Duration::from_secs(6), tx.closed()).await {
            tracing::error!("network tunnel timeout expired before startup finished");
            return Err(Error::ChannelTimeoutError);
        };

        tracing::info!("network tunnel started.");

        let json = serde_json::to_string_pretty(&endpoint_spec)?;
        RawValue::from_string(json).map_err(Into::into)
    }
}

#[cfg(test)]
mod test {
    use serde_json::{json, value::RawValue};

    use super::NetworkTunnel;

    #[tokio::test]
    async fn test_consume_network_tunnel_config_null() {
        let raw_value: Box<RawValue> =
            serde_json::from_value(json!({ "x": true, "networkTunnel": null })).unwrap();
        let result = NetworkTunnel::consume_network_tunnel_config(raw_value.clone())
            .await
            .unwrap();

        assert_eq!(result.get(), json!({"x": true}).to_string());
    }
}

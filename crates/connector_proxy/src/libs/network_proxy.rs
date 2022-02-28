use crate::errors::Error;
use crate::libs::json::{create_root_schema, remove_subobject};
use network_proxy::interface::NetworkProxyConfig;

use schemars::schema::{RootSchema, Schema};
use tokio::sync::oneshot::{self, Receiver};
use tokio::time::timeout;

pub struct NetworkProxy {}
pub const NETWORK_PROXY_KEY: &str = "networkProxy";

impl NetworkProxy {
    pub fn extend_endpoint_schema(endpoint_spec_schema_str: &str) -> Result<String, Error> {
        let network_proxy_schema = create_root_schema::<NetworkProxyConfig>();

        let mut modified_schema: RootSchema = serde_json::from_str(endpoint_spec_schema_str)?;
        if let Some(ref mut o) = &mut modified_schema.schema.object {
            if o.as_ref().properties.contains_key(NETWORK_PROXY_KEY) {
                return Err(Error::DuplicatedKeyError(NETWORK_PROXY_KEY));
            }
            o.as_mut().properties.insert(
                NETWORK_PROXY_KEY.to_string(),
                Schema::Object(network_proxy_schema.schema),
            );
        }
        serde_json::to_string_pretty(&modified_schema).map_err(Into::into)
    }

    // Start the network proxy. A flag will be sent to the channel of tx once the network proxy
    // is prepared to accept requests.
    async fn start_network_proxy(
        config: NetworkProxyConfig,
        tx: Receiver<()>,
    ) -> Result<(), Error> {
        let mut network_proxy = config.new_proxy();
        tokio::task::spawn(async move {
            let result: Result<(), Error> = match network_proxy.prepare().await {
                Ok(()) => {
                    drop(tx);
                    network_proxy.start_serve().await.map_err(Into::into)
                }
                Err(e) => Err(e.into()),
            };

            if let Err(err) = result {
                tracing::error!(error=%err, "failed starting network proxy.");
                std::process::exit(1);
            }
        })
        .await?;

        Ok(())
    }

    pub async fn consume_network_proxy_config(
        endpoint_spec_json_str: &str,
    ) -> Result<String, Error> {
        if endpoint_spec_json_str.is_empty() {
            return Ok(endpoint_spec_json_str.to_string());
        }

        let endpoint_spec_json = serde_json::from_str(endpoint_spec_json_str)?;
        let (network_proxy_config, endpoint_spec_json) =
            remove_subobject(endpoint_spec_json, NETWORK_PROXY_KEY);

        let network_proxy_config: NetworkProxyConfig = match network_proxy_config {
            None => return Ok(endpoint_spec_json_str.to_string()),
            Some(c) => serde_json::from_value(c)?,
        };

        let (mut tx, rx) = oneshot::channel();
        tokio::spawn(Self::start_network_proxy(network_proxy_config, rx));

        // Block for at most 5 seconds for network proxy to be prepared.
        if let Err(_) = timeout(std::time::Duration::from_secs(5), tx.closed()).await {
            return Err(Error::ChannelTimeoutError);
        };

        tracing::info!("network proxy started.");

        serde_json::to_string_pretty(&endpoint_spec_json).map_err(Into::into)
    }
}

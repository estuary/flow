use crate::errors::Error;
use crate::libs::json::{create_root_schema, remove_subobject};
use network_proxy::interface::NetworkProxyConfig;

use schemars::schema::{RootSchema, Schema};
use serde_json::value::RawValue;
use tokio::sync::oneshot::{self, Receiver};
use tokio::time::timeout;

pub struct NetworkProxy {}
pub const NETWORK_PROXY_KEY: &str = "networkProxy";

impl NetworkProxy {
    pub fn extend_endpoint_schema(
        endpoint_spec_schema: Box<RawValue>,
    ) -> Result<Box<RawValue>, Error> {
        let network_proxy_schema = create_root_schema::<NetworkProxyConfig>();

        let mut modified_schema: RootSchema = serde_json::from_str(endpoint_spec_schema.get())?;
        if let Some(ref mut o) = &mut modified_schema.schema.object {
            if o.as_ref().properties.contains_key(NETWORK_PROXY_KEY) {
                return Err(Error::DuplicatedKeyError(NETWORK_PROXY_KEY));
            }
            o.as_mut().properties.insert(
                NETWORK_PROXY_KEY.to_string(),
                Schema::Object(network_proxy_schema.schema),
            );
        }

        let json = serde_json::to_string_pretty(&modified_schema)?;
        RawValue::from_string(json).map_err(Into::into)
    }

    // Start the network proxy. The receiver rx will be dropped to indicate the network proxy
    // is ready to accept requests.
    async fn start_network_proxy(
        config: NetworkProxyConfig,
        rx: Receiver<()>,
    ) -> Result<(), Error> {
        let mut network_proxy = config.new_proxy();
        tokio::task::spawn(async move {
            let result: Result<(), Error> = match network_proxy.prepare().await {
                Ok(()) => {
                    drop(rx);
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
        endpoint_spec_json: Box<RawValue>,
    ) -> Result<Box<RawValue>, Error> {
        if endpoint_spec_json.get().is_empty() {
            return Ok(endpoint_spec_json);
        }

        let endpoint_spec = serde_json::from_str(endpoint_spec_json.get())?;
        let (network_proxy_config, endpoint_spec) =
            remove_subobject(endpoint_spec, NETWORK_PROXY_KEY);

        let network_proxy_config: NetworkProxyConfig = match network_proxy_config {
            None => return Ok(endpoint_spec_json),
            Some(c) => serde_json::from_value(c)?,
        };

        let (mut tx, rx) = oneshot::channel();
        tokio::spawn(Self::start_network_proxy(network_proxy_config, rx));

        // Block for at most 5 seconds for network proxy to be prepared.
        if let Err(_) = timeout(std::time::Duration::from_secs(5), tx.closed()).await {
            return Err(Error::ChannelTimeoutError);
        };

        tracing::info!("network proxy started.");

        let json = serde_json::to_string_pretty(&endpoint_spec)?;
        RawValue::from_string(json).map_err(Into::into)
    }
}

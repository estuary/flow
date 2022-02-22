use crate::errors::Error;
use crate::flow_capture_api::FlowCapturePlugin;
use crate::flow_materialize_api::FlowMaterializePlugin;
use crate::plugins::jsonutils::{create_root_schema, remove_subobject};

use network_proxy::interface::NetworkProxyConfig;

use schemars::schema::{RootSchema, Schema};
use std::sync::mpsc::{self, Receiver, Sender};

pub struct NetworkProxyPlugin {}
pub const NETWORK_PROXY_KEY: &str = "networkProxy";

impl NetworkProxyPlugin {
    fn extend_endpoint_schema(endpoint_spec_schema_str: &str) -> Result<String, Error> {
        let network_proxy_schema = create_root_schema::<NetworkProxyConfig>();

        let mut ond_schema: RootSchema = serde_json::from_str(endpoint_spec_schema_str)?;
        if let Some(ref mut o) = &mut ond_schema.schema.object {
            if o.as_ref().properties.contains_key(NETWORK_PROXY_KEY) {
                return Err(Error::DuplicatedKeyError(NETWORK_PROXY_KEY));
            }
            o.as_mut().properties.insert(
                NETWORK_PROXY_KEY.to_string(),
                Schema::Object(network_proxy_schema.schema),
            );
        }
        serde_json::to_string_pretty(&ond_schema).map_err(Into::into)
    }

    // Start the network proxy. A flag will be sent to the channel of tx once the network proxy
    // is prepared to accept requests.
    #[tokio::main]
    async fn start_network_proxy(
        config: NetworkProxyConfig,
        tx: Sender<bool>,
    ) -> Result<(), Error> {
        let mut network_proxy = config.new_proxy();
        tokio::task::spawn(async move {
            let result: Result<(), Error> = match network_proxy.prepare().await {
                Ok(()) => {
                    let send_result = tx.send(true);
                    match send_result {
                        Err(e) => Err(e.into()),
                        Ok(_) => network_proxy.start_serve().await.map_err(Into::into),
                    }
                }
                Err(e) => Err(e.into()),
            };

            if let Err(err) = result {
                tracing::error!(error = ?err, "failed starting network proxy." )
            }
        })
        .await?;

        Ok(())
    }

    fn consume_network_proxy_config(endpoint_spec_json_str: &str) -> Result<String, Error> {
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

        let (tx, rx): (Sender<bool>, Receiver<bool>) = mpsc::channel();
        std::thread::spawn(|| Self::start_network_proxy(network_proxy_config, tx));

        // Block for 5 seconds until network proxy is ready;
        if let Err(_) = rx.recv_timeout(std::time::Duration::from_secs(5)) {
            return Err(Error::ChannelTimeoutError);
        };

        tracing::info!("network proxy started.");

        serde_json::to_string_pretty(&endpoint_spec_json).map_err(Into::into)
    }
}

impl FlowCapturePlugin for NetworkProxyPlugin {
    fn on_spec_response(
        &self,
        response: &mut protocol::capture::SpecResponse,
    ) -> Result<(), Error> {
        response.endpoint_spec_schema_json =
            Self::extend_endpoint_schema(response.endpoint_spec_schema_json.as_str())?;
        Ok(())
    }

    fn on_discover_request(
        &self,
        request: &mut protocol::capture::DiscoverRequest,
    ) -> Result<(), Error> {
        request.endpoint_spec_json =
            Self::consume_network_proxy_config(request.endpoint_spec_json.as_str())?;
        Ok(())
    }

    fn on_validate_request(
        &self,
        request: &mut protocol::capture::ValidateRequest,
    ) -> Result<(), Error> {
        request.endpoint_spec_json =
            Self::consume_network_proxy_config(request.endpoint_spec_json.as_str())?;
        Ok(())
    }

    fn on_apply_upsert_request(
        &self,
        request: &mut protocol::capture::ApplyRequest,
    ) -> Result<(), Error> {
        if let Some(ref mut c) = request.capture {
            c.endpoint_spec_json =
                Self::consume_network_proxy_config(c.endpoint_spec_json.as_str())?;
        }
        Ok(())
    }

    fn on_apply_delete_request(
        &self,
        request: &mut protocol::capture::ApplyRequest,
    ) -> Result<(), Error> {
        if let Some(ref mut c) = request.capture {
            c.endpoint_spec_json =
                Self::consume_network_proxy_config(c.endpoint_spec_json.as_str())?;
        }
        Ok(())
    }

    fn on_pull_request(&self, request: &mut protocol::capture::PullRequest) -> Result<(), Error> {
        if let Some(ref mut open) = request.open {
            if let Some(ref mut c) = open.capture {
                c.endpoint_spec_json = Self::consume_network_proxy_config(&c.endpoint_spec_json)?;
            }
        }
        Ok(())
    }
}

impl FlowMaterializePlugin for NetworkProxyPlugin {
    fn on_spec_response(
        &self,
        response: &mut protocol::materialize::SpecResponse,
    ) -> Result<(), Error> {
        response.endpoint_spec_schema_json =
            Self::extend_endpoint_schema(response.endpoint_spec_schema_json.as_str())?;
        Ok(())
    }

    fn on_validate_request(
        &self,
        request: &mut protocol::materialize::ValidateRequest,
    ) -> Result<(), Error> {
        request.endpoint_spec_json =
            Self::consume_network_proxy_config(request.endpoint_spec_json.as_str())?;
        Ok(())
    }

    fn on_apply_upsert_request(
        &self,
        request: &mut protocol::materialize::ApplyRequest,
    ) -> Result<(), Error> {
        if let Some(ref mut m) = request.materialization {
            m.endpoint_spec_json = Self::consume_network_proxy_config(&m.endpoint_spec_json)?;
        }
        Ok(())
    }

    fn on_apply_delete_request(
        &self,
        request: &mut protocol::materialize::ApplyRequest,
    ) -> Result<(), Error> {
        if let Some(ref mut m) = request.materialization {
            m.endpoint_spec_json = Self::consume_network_proxy_config(&m.endpoint_spec_json)?;
        }
        Ok(())
    }

    fn on_transactions_request(
        &self,
        request: &mut protocol::materialize::TransactionRequest,
    ) -> Result<(), Error> {
        if let Some(ref mut open) = request.open {
            if let Some(ref mut m) = open.materialization {
                m.endpoint_spec_json = Self::consume_network_proxy_config(&m.endpoint_spec_json)?;
            }
        }
        Ok(())
    }
}

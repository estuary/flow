//! Derivation protocol: request unwrapping, endpoint extraction, the
//! `derive.Connector` RPC, and in-process `derive-sqlite`. See `protocol.rs`
//! for the start pipeline.
use crate::{
    proto,
    protocol::{Endpoint, Extracted, Protocol, StartRpcFuture},
};
use anyhow::Context;
use futures::{FutureExt, StreamExt};
use proto_flow::{
    derive::{Request, Response, request, response},
    flow::collection_spec::derivation::ConnectorType,
};

pub(crate) enum Derive {}

impl Protocol for Derive {
    type Request = Request;
    type Response = Response;

    const NAME: &'static str = "Derive";
    const TASK_TYPE: ops::TaskType = ops::TaskType::Derivation;

    fn spec_request(connector_type: i32) -> Request {
        Request {
            kind: Some(request::Kind::Spec(request::Spec {
                config_json: "{}".into(),
                connector_type,
            })),
            ..Default::default()
        }
    }

    fn unwrap_request(kind: proto::request::Kind) -> Option<Request> {
        match kind {
            proto::request::Kind::Derive(request) => Some(request),
            _ => None,
        }
    }

    fn wrap_response(response: Response) -> proto::Response {
        proto::Response {
            kind: Some(proto::response::Kind::Derive(response)),
        }
    }

    fn unwrap_spec(response: Response) -> Result<proto::response::started::Spec, Response> {
        match response {
            Response {
                kind: Some(response::Kind::Spec(spec)),
                ..
            } => Ok(proto::response::started::Spec::Derive(spec)),
            response => Err(response),
        }
    }

    fn open_rpc<S>(channel: tonic::transport::Channel, requests: S) -> StartRpcFuture<Response>
    where
        S: futures::Stream<Item = Request> + Send + 'static,
    {
        async move {
            proto_grpc::derive::connector_client::ConnectorClient::new(channel)
                .max_decoding_message_size(proto_grpc::MAX_MESSAGE_SIZE)
                .max_encoding_message_size(usize::MAX)
                .derive(requests)
                .await
        }
        .boxed()
    }

    fn extract_endpoint<'r>(
        request: &'r mut Request,
        sqlite_vfs_uri: Option<String>,
    ) -> anyhow::Result<Extracted<'r, Self>> {
        let (connector_type, config_json) = match &mut request.kind {
            Some(request::Kind::Spec(spec)) => (spec.connector_type, &mut spec.config_json),
            Some(request::Kind::Validate(validate)) => {
                (validate.connector_type, &mut validate.config_json)
            }
            Some(request::Kind::Open(open)) => {
                let inner = open
                    .collection
                    .as_mut()
                    .expect("checked by task_name")
                    .derivation
                    .as_mut()
                    .ok_or_else(|| {
                        crate::invalid_argument(
                            "`collection` missing required `derivation`".to_string(),
                        )
                    })?;

                (inner.connector_type, &mut inner.config_json)
            }
            _ => unreachable!("checked by task_name"),
        };

        if sqlite_vfs_uri.is_some() && connector_type != ConnectorType::Sqlite as i32 {
            return Err(crate::protocol::sqlite_vfs_uri_error());
        }

        let endpoint = if connector_type == ConnectorType::Image as i32 {
            let models::ConnectorConfig { image, config } =
                serde_json::from_slice(config_json).context("parsing connector config")?;

            Endpoint::Image { image, config }
        } else if connector_type == ConnectorType::Local as i32 {
            Endpoint::Local {
                config: serde_json::from_slice(config_json).context("parsing local config")?,
            }
        } else if connector_type == ConnectorType::Sqlite as i32 {
            let config: models::RawValue =
                serde_json::from_slice(config_json).context("parsing sqlite config")?;

            // This is the sole endpoint which may record into a recovery log.
            Endpoint::InProcess {
                connector: Box::new(move |requests| {
                    derive_sqlite::connector(requests, sqlite_vfs_uri)
                        .map(|response| response.map_err(proto_grpc::anyhow_to_status))
                        .boxed()
                }),
                config,
            }
        } else if connector_type == ConnectorType::Typescript as i32
            || connector_type == ConnectorType::Python as i32
        {
            // The runtime requires a built-in connector image to be resolved by the
            // control-plane build maps TypeScript / Python derivations to a concrete
            // image (selecting the tag from the task's feature flags) so that Validate
            // and the runtime agree on the connector interface. Encountering an
            // unresolved built-in here means the spec was built without that mapping.
            anyhow::bail!(
                "derive connector type {connector_type} should have been resolved to an image at build time"
            );
        } else {
            anyhow::bail!("invalid derive connector type: {connector_type}");
        };

        Ok(Extracted {
            connector_type,
            endpoint,
            initial_config_slot: config_json,
            initial_sealed_config_slot: None,
        })
    }
}

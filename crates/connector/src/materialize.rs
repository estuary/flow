//! Materialization protocol: request unwrapping, endpoint extraction, the
//! `materialize.Connector` RPC, and in-process Dekaf. See `protocol.rs` for the
//! start pipeline.
use crate::{
    proto,
    protocol::{Endpoint, Extracted, Protocol, StartRpcFuture},
};
use anyhow::Context;
use futures::{FutureExt, StreamExt, TryStreamExt};
use proto_flow::{
    flow,
    materialize::{Request, Response, request, response},
};

pub(crate) enum Materialize {}

impl Protocol for Materialize {
    type Request = Request;
    type Response = Response;

    const NAME: &'static str = "Materialize";
    const TASK_TYPE: ops::TaskType = ops::TaskType::Materialization;

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
            proto::request::Kind::Materialize(request) => Some(request),
            _ => None,
        }
    }

    fn wrap_response(response: Response) -> proto::Response {
        proto::Response {
            kind: Some(proto::response::Kind::Materialize(response)),
        }
    }

    fn unwrap_spec(response: Response) -> Result<proto::response::started::Spec, Response> {
        match response {
            Response {
                kind: Some(response::Kind::Spec(spec)),
                ..
            } => Ok(proto::response::started::Spec::Materialize(spec)),
            response => Err(response),
        }
    }

    fn open_rpc<S>(channel: tonic::transport::Channel, requests: S) -> StartRpcFuture<Response>
    where
        S: futures::Stream<Item = Request> + Send + 'static,
    {
        async move {
            proto_grpc::materialize::connector_client::ConnectorClient::new(channel)
                .max_decoding_message_size(proto_grpc::MAX_MESSAGE_SIZE)
                .max_encoding_message_size(usize::MAX)
                .materialize(requests)
                .await
        }
        .boxed()
    }

    fn extract_endpoint<'r>(
        request: &'r mut Request,
        sqlite_vfs_uri: Option<String>,
    ) -> anyhow::Result<Extracted<'r, Self>> {
        if sqlite_vfs_uri.is_some() {
            return Err(crate::protocol::sqlite_vfs_uri_error());
        }
        let (connector_type, config_json, sealed_config_json) = match &mut request.kind {
            Some(request::Kind::Spec(spec)) => (spec.connector_type, &mut spec.config_json, None),
            Some(request::Kind::Validate(validate)) => {
                (validate.connector_type, &mut validate.config_json, None)
            }
            Some(request::Kind::Apply(apply)) => {
                let inner = apply
                    .materialization
                    .as_mut()
                    .expect("checked by task_name");
                (inner.connector_type, &mut inner.config_json, None)
            }
            Some(request::Kind::Open(open)) => {
                let sealed_config_json = &mut open.sealed_config_json;
                let inner = open.materialization.as_mut().expect("checked by task_name");
                (
                    inner.connector_type,
                    &mut inner.config_json,
                    Some(sealed_config_json),
                )
            }
            _ => unreachable!("checked by task_name"),
        };

        let endpoint = if connector_type == flow::materialization_spec::ConnectorType::Image as i32
        {
            let models::ConnectorConfig { image, config } =
                serde_json::from_slice(config_json).context("parsing connector config")?;

            Endpoint::Image { image, config }
        } else if connector_type == flow::materialization_spec::ConnectorType::Local as i32 {
            Endpoint::Local {
                config: serde_json::from_slice(config_json).context("parsing local config")?,
            }
        } else if connector_type == flow::materialization_spec::ConnectorType::Dekaf as i32 {
            let models::DekafConfig { variant, config } =
                serde_json::from_slice(config_json).context("parsing dekaf config")?;

            Endpoint::InProcess {
                connector: Box::new(move |requests| {
                    // `dekaf_connector` still parses the `{variant, config}`
                    // wrapper, because the V1 runtime also calls it with one.
                    // Startup sends its internal Spec before the resolved
                    // initial request, so restore the wrapper for that request
                    // only; subsequent requests already have their wire
                    // configuration. Both go away with the V1 runtime.
                    let requests =
                        requests
                            .enumerate()
                            .map(move |(index, mut request): (_, Request)| {
                                if let (1, Some(request::Kind::Validate(validate))) =
                                    (index, &mut request.kind)
                                {
                                    validate.config_json =
                                        serde_json::to_vec(&models::DekafConfig {
                                            variant: variant.clone(),
                                            config: serde_json::from_slice(&validate.config_json)
                                                .expect("resolved configuration is valid JSON"),
                                        })
                                        .expect("Dekaf configuration is serializable")
                                        .into();
                                }
                                request
                            });
                    dekaf_connector::connector(requests)
                        .map_err(proto_grpc::anyhow_to_status)
                        .boxed()
                }),
                config,
            }
        } else {
            anyhow::bail!("invalid connector type: {connector_type}");
        };

        Ok(Extracted {
            connector_type,
            endpoint,
            initial_config_slot: config_json,
            initial_sealed_config_slot: sealed_config_json,
        })
    }
}

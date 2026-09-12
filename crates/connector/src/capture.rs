//! Capture protocol: request unwrapping, endpoint extraction, and the
//! `capture.Connector` RPC. See `protocol.rs` for the start pipeline.
use crate::{
    proto,
    protocol::{Endpoint, Extracted, Protocol, StartRpcFuture},
};
use anyhow::Context;
use futures::FutureExt;
use proto_flow::{
    capture::{Request, Response, request, response},
    flow,
};

pub(crate) enum Capture {}

impl Protocol for Capture {
    type Request = Request;
    type Response = Response;

    const NAME: &'static str = "Capture";
    const TASK_TYPE: ops::TaskType = ops::TaskType::Capture;

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
            proto::request::Kind::Capture(request) => Some(request),
            _ => None,
        }
    }

    fn wrap_response(response: Response) -> proto::Response {
        proto::Response {
            kind: Some(proto::response::Kind::Capture(response)),
        }
    }

    fn unwrap_spec(response: Response) -> Result<proto::response::started::Spec, Response> {
        match response {
            Response {
                kind: Some(response::Kind::Spec(spec)),
                ..
            } => Ok(proto::response::started::Spec::Capture(spec)),
            response => Err(response),
        }
    }

    fn open_rpc<S>(channel: tonic::transport::Channel, requests: S) -> StartRpcFuture<Response>
    where
        S: futures::Stream<Item = Request> + Send + 'static,
    {
        async move {
            proto_grpc::capture::connector_client::ConnectorClient::new(channel)
                .max_decoding_message_size(proto_grpc::MAX_MESSAGE_SIZE)
                .max_encoding_message_size(usize::MAX)
                .capture(requests)
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
        let (connector_type, config_json, sealed_config_json, secrets) = match &mut request.kind {
            Some(request::Kind::Spec(spec)) => (
                spec.connector_type,
                &mut spec.config_json,
                None,
                &super::EMPTY_SECRETS,
            ),
            Some(request::Kind::Discover(discover)) => (
                discover.connector_type,
                &mut discover.config_json,
                None,
                &discover.secrets,
            ),
            Some(request::Kind::Validate(validate)) => (
                validate.connector_type,
                &mut validate.config_json,
                None,
                &validate.secrets,
            ),
            Some(request::Kind::Apply(apply)) => {
                let inner = apply.capture.as_mut().expect("checked by task_name");
                (
                    inner.connector_type,
                    &mut inner.config_json,
                    None,
                    &inner.secrets,
                )
            }
            Some(request::Kind::Open(open)) => {
                let sealed_config_json = &mut open.sealed_config_json;
                let inner = open.capture.as_mut().expect("checked by task_name");
                (
                    inner.connector_type,
                    &mut inner.config_json,
                    Some(sealed_config_json),
                    &inner.secrets,
                )
            }
            _ => unreachable!("checked by task_name"),
        };

        let endpoint = if connector_type == flow::capture_spec::ConnectorType::Image as i32 {
            let models::ConnectorConfig { image, config } =
                serde_json::from_slice(config_json).context("parsing connector config")?;

            Endpoint::Image { image, config }
        } else if connector_type == flow::capture_spec::ConnectorType::Local as i32 {
            Endpoint::Local {
                config: serde_json::from_slice(config_json).context("parsing local config")?,
            }
        } else {
            anyhow::bail!("invalid connector type: {connector_type}");
        };

        Ok(Extracted {
            initial_config_slot: config_json,
            connector_type,
            endpoint,
            initial_sealed_config_slot: sealed_config_json,
            secrets,
        })
    }
}

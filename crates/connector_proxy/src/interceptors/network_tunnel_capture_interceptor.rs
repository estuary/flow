use crate::apis::{FlowCaptureOperation, InterceptorStream};
use crate::errors::{create_custom_error, Error};
use crate::libs::network_tunnel::NetworkTunnel;
use crate::libs::protobuf::{decode_message, encode_message};
use crate::libs::stream::get_decoded_message;
use futures::{future, stream, StreamExt, TryStreamExt};
use proto_flow::capture::{
    ApplyRequest, DiscoverRequest, PullRequest, SpecResponse, ValidateRequest,
};

use serde_json::value::RawValue;
use tokio_util::io::{ReaderStream, StreamReader};

pub struct NetworkTunnelCaptureInterceptor {}

impl NetworkTunnelCaptureInterceptor {
    fn adapt_discover_request_stream(in_stream: InterceptorStream) -> InterceptorStream {
        Box::pin(stream::once(async {
            let mut request = get_decoded_message::<DiscoverRequest>(in_stream).await?;

            request.endpoint_spec_json = NetworkTunnel::consume_network_tunnel_config(
                RawValue::from_string(request.endpoint_spec_json)?,
            )
            .await
            .map_err(|err| {
                create_custom_error(&format!("error consuming tunnel configuration {:?}", err))
            })?
            .to_string();

            encode_message(&request)
        }))
    }

    fn adapt_validate_request_stream(in_stream: InterceptorStream) -> InterceptorStream {
        Box::pin(stream::once(async {
            let mut request = get_decoded_message::<ValidateRequest>(in_stream).await?;

            request.endpoint_spec_json = NetworkTunnel::consume_network_tunnel_config(
                RawValue::from_string(request.endpoint_spec_json)?,
            )
            .await
            .map_err(|err| {
                create_custom_error(&format!("error consuming tunnel configuration {:?}", err))
            })?
            .to_string();

            encode_message(&request)
        }))
    }

    fn adapt_apply_request(in_stream: InterceptorStream) -> InterceptorStream {
        Box::pin(stream::once(async {
            let mut request = get_decoded_message::<ApplyRequest>(in_stream).await?;

            if let Some(ref mut c) = request.capture {
                c.endpoint_spec_json = NetworkTunnel::consume_network_tunnel_config(
                    RawValue::from_string(c.endpoint_spec_json.clone())?,
                )
                .await
                .map_err(|err| {
                    create_custom_error(&format!("error consuming tunnel configuration {:?}", err))
                })?
                .to_string();
            }

            encode_message(&request)
        }))
    }

    fn adapt_pull_request_stream(in_stream: InterceptorStream) -> InterceptorStream {
        Box::pin(
            stream::once(async {
                let mut reader = StreamReader::new(in_stream);
                let mut request = decode_message::<PullRequest, _>(&mut reader)
                    .await
                    .map_err(|err| {
                        create_custom_error(&format!("decoding pull_request failed {:?}", err))
                    })?
                    .expect("expected request is not received.");
                if let Some(ref mut o) = request.open {
                    if let Some(ref mut c) = o.capture {
                        c.endpoint_spec_json = NetworkTunnel::consume_network_tunnel_config(
                            RawValue::from_string(c.endpoint_spec_json.clone())?,
                        )
                        .await
                        .map_err(|err| {
                            create_custom_error(&format!(
                                "error consuming tunnel configuration {:?}",
                                err
                            ))
                        })?
                        .to_string();
                    }
                }

                let first = stream::once(future::ready(encode_message(&request)));
                let rest = ReaderStream::new(reader);

                // We need to set explicit error type, see https://github.com/rust-lang/rust/issues/63502
                Ok::<_, std::io::Error>(first.chain(rest))
            })
            .try_flatten(),
        )
    }
}

impl NetworkTunnelCaptureInterceptor {
    pub fn adapt_request_stream(
        op: &FlowCaptureOperation,
        in_stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        Ok(match op {
            FlowCaptureOperation::Discover => Self::adapt_discover_request_stream(in_stream),
            FlowCaptureOperation::Validate => Self::adapt_validate_request_stream(in_stream),
            FlowCaptureOperation::ApplyUpsert | FlowCaptureOperation::ApplyDelete => {
                Self::adapt_apply_request(in_stream)
            }
            FlowCaptureOperation::Pull => Self::adapt_pull_request_stream(in_stream),
            _ => in_stream,
        })
    }

    pub fn adapt_response_stream(
        op: &FlowCaptureOperation,
        in_stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        Ok(match op {
            FlowCaptureOperation::Spec => Box::pin(stream::once(async move {
                let mut response = get_decoded_message::<SpecResponse>(in_stream).await?;
                response.endpoint_spec_schema_json = NetworkTunnel::extend_endpoint_schema(
                    RawValue::from_string(response.endpoint_spec_schema_json)?,
                )
                .map_err(|err| create_custom_error(&format!("extend endpoint schema {:?}", err)))?
                .to_string();
                encode_message(&response)
            })),
            _ => in_stream,
        })
    }
}

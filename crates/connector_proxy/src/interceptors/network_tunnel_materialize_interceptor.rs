use crate::apis::{FlowMaterializeOperation, InterceptorStream};
use crate::errors::{create_custom_error, Error};
use crate::libs::network_tunnel::NetworkTunnel;
use crate::libs::protobuf::{decode_message, encode_message};
use crate::libs::stream::get_decoded_message;

use futures::{future, stream, StreamExt, TryStreamExt};
use proto_flow::materialize::{ApplyRequest, SpecResponse, TransactionRequest, ValidateRequest};

use serde_json::value::RawValue;
use tokio_util::io::{ReaderStream, StreamReader};

pub struct NetworkTunnelMaterializeInterceptor {}

impl NetworkTunnelMaterializeInterceptor {
    fn adapt_spec_request(in_stream: InterceptorStream) -> InterceptorStream {
        Box::pin(stream::once(async {
            let mut request = get_decoded_message::<ValidateRequest>(in_stream).await?;

            request.endpoint_spec_json = NetworkTunnel::consume_network_tunnel_config(
                RawValue::from_string(request.endpoint_spec_json)?,
            )
            .await
            .expect("failed to start network tunnel")
            .to_string();
            encode_message(&request)
        }))
    }

    fn adapt_apply_request(in_stream: InterceptorStream) -> InterceptorStream {
        Box::pin(stream::once(async {
            let mut request = get_decoded_message::<ApplyRequest>(in_stream).await?;

            if let Some(ref mut m) = request.materialization {
                m.endpoint_spec_json = NetworkTunnel::consume_network_tunnel_config(
                    RawValue::from_string(m.endpoint_spec_json.clone())?,
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

    fn adapt_transactions_request(in_stream: InterceptorStream) -> InterceptorStream {
        Box::pin(
            stream::once(async {
                let mut reader = StreamReader::new(in_stream);
                let mut request = decode_message::<TransactionRequest, _>(&mut reader)
                    .await
                    .map_err(|err| {
                        create_custom_error(&format!(
                            "decoding TransactionRequest failed {:?}",
                            err
                        ))
                    })?
                    .expect("expected request is not received.");
                if let Some(ref mut o) = request.open {
                    if let Some(ref mut m) = o.materialization {
                        m.endpoint_spec_json = NetworkTunnel::consume_network_tunnel_config(
                            RawValue::from_string(m.endpoint_spec_json.clone())?,
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

impl NetworkTunnelMaterializeInterceptor {
    pub fn adapt_request_stream(
        op: &FlowMaterializeOperation,
        in_stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        Ok(match op {
            FlowMaterializeOperation::Validate => Self::adapt_spec_request(in_stream),
            FlowMaterializeOperation::ApplyUpsert | FlowMaterializeOperation::ApplyDelete => {
                Self::adapt_apply_request(in_stream)
            }
            FlowMaterializeOperation::Transactions => Self::adapt_transactions_request(in_stream),
            _ => in_stream,
        })
    }

    pub fn adapt_response_stream(
        op: &FlowMaterializeOperation,
        in_stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        Ok(match op {
            FlowMaterializeOperation::Spec => Box::pin(stream::once(async {
                let mut response = get_decoded_message::<SpecResponse>(in_stream).await?;

                response.endpoint_spec_schema_json = NetworkTunnel::extend_endpoint_schema(
                    RawValue::from_string(response.endpoint_spec_schema_json)?,
                )
                .map_err(|err| {
                    create_custom_error(&format!("extending endpoint schema {:?}", err))
                })?
                .to_string();
                encode_message(&response)
            })),
            _ => in_stream,
        })
    }
}

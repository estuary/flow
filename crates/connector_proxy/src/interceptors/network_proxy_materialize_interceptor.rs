use crate::apis::{FlowMaterializeOperation, InterceptorStream};
use crate::errors::{Error, Must};
use crate::libs::network_proxy::NetworkProxy;
use crate::libs::protobuf::{decode_message, encode_message};
use crate::libs::stream::stream_all_bytes;

use protocol::materialize::{ApplyRequest, SpecResponse, TransactionRequest, ValidateRequest};

use async_stream::stream;
use futures_util::pin_mut;
use futures_util::StreamExt;
use serde_json::value::RawValue;
use tokio_util::io::StreamReader;

pub struct NetworkProxyMaterializeInterceptor {}

impl NetworkProxyMaterializeInterceptor {
    fn convert_spec_request(in_stream: InterceptorStream) -> InterceptorStream {
        Box::pin(stream! {
            let mut reader = StreamReader::new(in_stream);
            let mut request = decode_message::<ValidateRequest, _>(&mut reader).await.or_bail().expect("expected request is not received.");
            request.endpoint_spec_json =
                NetworkProxy::consume_network_proxy_config(RawValue::from_string(request.endpoint_spec_json)?)
                    .await
                    .expect("failed to start network proxy")
                    .to_string();
            yield encode_message(&request);
        })
    }

    fn convert_apply_request(in_stream: InterceptorStream) -> InterceptorStream {
        Box::pin(stream! {
            let mut reader = StreamReader::new(in_stream);
            let mut request = decode_message::<ApplyRequest, _>(&mut reader).await.or_bail().expect("expected request is not received.");
            if let Some(ref mut m) = request.materialization {
                m.endpoint_spec_json =
                    NetworkProxy::consume_network_proxy_config(
                        RawValue::from_string(m.endpoint_spec_json.clone())?,
                    ).await.or_bail().to_string();
            }
            yield encode_message(&request);
        })
    }

    fn convert_transactions_request(in_stream: InterceptorStream) -> InterceptorStream {
        Box::pin(stream! {
            let mut reader = StreamReader::new(in_stream);
            let mut request = decode_message::<TransactionRequest, _>(&mut reader).await.or_bail().expect("expected request is not received.");
            if let Some(ref mut o) = request.open {
                if let Some(ref mut m) = o.materialization {
                    m.endpoint_spec_json = NetworkProxy::consume_network_proxy_config(
                        RawValue::from_string(m.endpoint_spec_json.clone())?,
                    ).await.or_bail().to_string();
                }
            }
            yield encode_message(&request);
            // deliver the remaining messages in the stream.
            let s = stream_all_bytes(reader);
            pin_mut!(s);
            while let Some(bytes) = s.next().await {
                yield bytes;
            }
        })
    }
}

impl NetworkProxyMaterializeInterceptor {
    pub fn adapt_request_stream(
        op: &FlowMaterializeOperation,
        in_stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        Ok(match op {
            FlowMaterializeOperation::Validate => Self::convert_spec_request(in_stream),
            FlowMaterializeOperation::ApplyUpsert | FlowMaterializeOperation::ApplyDelete => {
                Self::convert_apply_request(in_stream)
            }
            FlowMaterializeOperation::Transactions => Self::convert_transactions_request(in_stream),
            _ => in_stream,
        })
    }

    pub fn adapt_response_stream(
        op: &FlowMaterializeOperation,
        in_stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        Ok(match op {
            FlowMaterializeOperation::Spec => Box::pin(stream! {
                let mut reader = StreamReader::new(in_stream);
                let mut response = decode_message::<SpecResponse, _>(&mut reader).await.or_bail().expect("expected response is not received.");
                response.endpoint_spec_schema_json = NetworkProxy::extend_endpoint_schema(
                    RawValue::from_string(response.endpoint_spec_schema_json)?,
                ).or_bail().to_string();
                yield encode_message(&response);
            }),
            _ => in_stream,
        })
    }
}

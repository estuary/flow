use crate::apis::{
    FlowMaterializeOperation, Interceptor, InterceptorStream, RequestResponseConverterPair,
};
use crate::errors::{Error, Must};
use crate::libs::network_proxy::NetworkProxy;
use crate::libs::protobuf::{decode_message, encode_message};
use crate::libs::stream::stream_all_bytes;

use protocol::materialize::{ApplyRequest, SpecResponse, TransactionRequest, ValidateRequest};

use async_stream::stream;
use futures_util::pin_mut;
use futures_util::StreamExt;
use tokio_util::io::StreamReader;
pub struct NetworkProxyMaterializeInterceptor {}

impl NetworkProxyMaterializeInterceptor {
    fn convert_spec_request(in_stream: InterceptorStream) -> InterceptorStream {
        Box::pin(stream! {
            let mut reader = StreamReader::new(in_stream);
            let mut request = decode_message::<ValidateRequest, _>(&mut reader).await.or_bail().expect("expected request is not received.");
            request.endpoint_spec_json =
                NetworkProxy::consume_network_proxy_config(request.endpoint_spec_json.as_str())
                    .await
                    .expect("failed to start network proxy");
            yield encode_message(&request);
        })
    }

    fn convert_apply_request(in_stream: InterceptorStream) -> InterceptorStream {
        Box::pin(stream! {
            let mut reader = StreamReader::new(in_stream);
            let mut request = decode_message::<ApplyRequest, _>(&mut reader).await.or_bail().expect("expected request is not received.");
            if let Some(ref mut m) = request.materialization {
                m.endpoint_spec_json =
                    NetworkProxy::consume_network_proxy_config(m.endpoint_spec_json.as_str()).await.or_bail();
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
                        m.endpoint_spec_json.as_str(),
                    ).await.or_bail();
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

    fn convert_request(
        operation: &FlowMaterializeOperation,
        in_stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        Ok(match operation {
            FlowMaterializeOperation::Validate => Self::convert_spec_request(in_stream),
            FlowMaterializeOperation::ApplyUpsert | FlowMaterializeOperation::ApplyDelete => {
                Self::convert_apply_request(in_stream)
            }
            FlowMaterializeOperation::Transactions => Self::convert_transactions_request(in_stream),
            _ => in_stream,
        })
    }

    fn convert_response(
        operation: &FlowMaterializeOperation,
        in_stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        Ok(match operation {
            FlowMaterializeOperation::Spec => Box::pin(stream! {
                let mut reader = StreamReader::new(in_stream);
                let mut response = decode_message::<SpecResponse, _>(&mut reader).await.or_bail().expect("expected response is not received.");
                response.endpoint_spec_schema_json = NetworkProxy::extend_endpoint_schema(
                    response.endpoint_spec_schema_json.as_str(),
                ).or_bail();
                yield encode_message(&response);
            }),
            _ => in_stream,
        })
    }
}

impl Interceptor<FlowMaterializeOperation> for NetworkProxyMaterializeInterceptor {
    fn get_converters() -> RequestResponseConverterPair<FlowMaterializeOperation> {
        (
            Box::new(Self::convert_request),
            Box::new(Self::convert_response),
        )
    }
}

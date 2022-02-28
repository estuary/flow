use crate::apis::{
    FlowCaptureOperation, Interceptor, InterceptorStream, RequestResponseConverterPair,
};
use crate::errors::{Error, Must};
use crate::libs::network_proxy::NetworkProxy;
use crate::libs::protobuf::{decode_message, encode_message};
use crate::libs::stream::stream_all_bytes;
use protocol::capture::{
    ApplyRequest, DiscoverRequest, PullRequest, SpecResponse, ValidateRequest,
};

use async_stream::stream;
use futures_util::pin_mut;
use futures_util::StreamExt;
use serde_json::value::RawValue;
use tokio_util::io::StreamReader;

pub struct NetworkProxyCaptureInterceptor {}

impl NetworkProxyCaptureInterceptor {
    fn convert_discover_request(in_stream: InterceptorStream) -> InterceptorStream {
        Box::pin(stream! {
            let mut reader = StreamReader::new(in_stream);
            let mut request = decode_message::<DiscoverRequest, _>(&mut reader).await.or_bail().expect("expected request is not received.");
            request.endpoint_spec_json = NetworkProxy::consume_network_proxy_config(
                RawValue::from_string(request.endpoint_spec_json)?,
            ).await.or_bail().to_string();
            yield encode_message(&request);
        })
    }

    fn convert_validate_request(in_stream: InterceptorStream) -> InterceptorStream {
        Box::pin(stream! {
            let mut reader = StreamReader::new(in_stream);
            let mut request = decode_message::<ValidateRequest, _>(&mut reader).await.or_bail().expect("expected request is not received.");
            request.endpoint_spec_json = NetworkProxy::consume_network_proxy_config(
                RawValue::from_string(request.endpoint_spec_json)?,
            ).await.or_bail().to_string();
            yield encode_message(&request);
        })
    }

    fn convert_apply_request(in_stream: InterceptorStream) -> InterceptorStream {
        Box::pin(stream! {
            let mut reader = StreamReader::new(in_stream);
            let mut request = decode_message::<ApplyRequest, _>(&mut reader).await.or_bail().expect("expected request is not received.");
            if let Some(ref mut c) = request.capture {
                c.endpoint_spec_json =
                    NetworkProxy::consume_network_proxy_config(
                        RawValue::from_string(c.endpoint_spec_json.clone())?,
                ).await.or_bail().to_string();
            }
            yield encode_message(&request);
        })
    }

    fn convert_pull_request(in_stream: InterceptorStream) -> InterceptorStream {
        Box::pin(stream! {
            let mut reader = StreamReader::new(in_stream);
            let mut request = decode_message::<PullRequest, _>(&mut reader).await.or_bail().expect("expected request is not received.");
            if let Some(ref mut o) = request.open {
                if let Some(ref mut c) = o.capture {
                    c.endpoint_spec_json = NetworkProxy::consume_network_proxy_config(
                        RawValue::from_string(c.endpoint_spec_json.clone())?,
                    ).await.or_bail().to_string();
                }
            }
            yield encode_message(&request);
            // deliver the rest messages in the stream.
            let s = stream_all_bytes(reader);
            pin_mut!(s);
            while let Some(value) = s.next().await {
                yield value;
            }
        })
    }

    fn convert_request(
        operation: &FlowCaptureOperation,
        in_stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        Ok(match operation {
            // converting it to another stream instead of stream.map, b/c the resulting stream breaks the boundaries between of the
            // source messages.
            FlowCaptureOperation::Discover => Self::convert_discover_request(in_stream),
            FlowCaptureOperation::Validate => Self::convert_validate_request(in_stream),
            FlowCaptureOperation::ApplyUpsert | FlowCaptureOperation::ApplyDelete => {
                Self::convert_apply_request(in_stream)
            }
            FlowCaptureOperation::Pull => Self::convert_pull_request(in_stream),
            _ => in_stream,
        })
    }

    fn convert_response(
        operation: &FlowCaptureOperation,
        in_stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        Ok(match operation {
            FlowCaptureOperation::Spec => Box::pin(stream! {
                let mut reader = StreamReader::new(in_stream);
                let mut response = decode_message::<SpecResponse, _>(&mut reader).await.or_bail().expect("No expected response received.");
                response.endpoint_spec_schema_json = NetworkProxy::extend_endpoint_schema(
                    RawValue::from_string(response.endpoint_spec_schema_json)?,
                ).or_bail().to_string();
                yield encode_message(&response);
            }),
            _ => in_stream,
        })
    }
}

impl Interceptor<FlowCaptureOperation> for NetworkProxyCaptureInterceptor {
    fn get_converters() -> RequestResponseConverterPair<FlowCaptureOperation> {
        (
            Box::new(Self::convert_request),
            Box::new(Self::convert_response),
        )
    }
}

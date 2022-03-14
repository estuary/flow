use crate::apis::{FlowCaptureOperation, InterceptorStream};
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
    fn adapt_discover_request_stream(in_stream: InterceptorStream) -> InterceptorStream {
        Box::pin(stream! {
            let mut reader = StreamReader::new(in_stream);
            let mut request = decode_message::<DiscoverRequest, _>(&mut reader).await.or_bail().expect("expected request is not received.");
            request.endpoint_spec_json = NetworkProxy::consume_network_proxy_config(
                RawValue::from_string(request.endpoint_spec_json)?,
            ).await.or_bail().to_string();
            yield encode_message(&request);
        })
    }

    fn adapt_validate_request_stream(in_stream: InterceptorStream) -> InterceptorStream {
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

    fn adapt_pull_request_stream(in_stream: InterceptorStream) -> InterceptorStream {
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
}

impl NetworkProxyCaptureInterceptor {
    pub fn adapt_request_stream(
        op: &FlowCaptureOperation,
        in_stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        Ok(match op {
            FlowCaptureOperation::Discover => Self::adapt_discover_request_stream(in_stream),
            FlowCaptureOperation::Validate => Self::adapt_validate_request_stream(in_stream),
            FlowCaptureOperation::ApplyUpsert | FlowCaptureOperation::ApplyDelete => {
                Self::convert_apply_request(in_stream)
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

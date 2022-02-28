use crate::apis::{
    FlowCaptureOperation, Interceptor, InterceptorStream, RequestResponseConverterPair,
};
use crate::errors::Error;

// A placeholder for real logic of airbyte connectors. Details might change during real-implementations.
pub struct AirbyteCaptureInterceptor {}

impl AirbyteCaptureInterceptor {
    fn convert_request(
        _operation: &FlowCaptureOperation,
        _in_stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        panic!("TBD AirbyteCaptureInterceptor")
    }

    fn convert_response(
        _operation: &FlowCaptureOperation,
        _in_stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        panic!("TBD AirbyteCaptureInterceptor")
    }
}

impl Interceptor<FlowCaptureOperation> for AirbyteCaptureInterceptor {
    fn get_converters() -> RequestResponseConverterPair<FlowCaptureOperation> {
        (
            Box::new(Self::convert_request),
            Box::new(Self::convert_response),
        )
    }
}

use crate::errors::Error;
use bytes::Bytes;
use clap::ArgEnum;
use futures_core::stream::Stream;
use std::pin::Pin;

// Flow Capture operations defined in
// https://github.com/estuary/flow/blob/master/go/protocols/capture/capture.proto
#[derive(Debug, strum_macros::Display, ArgEnum, Clone)]
#[strum(serialize_all = "kebab_case")]
pub enum FlowCaptureOperation {
    Spec,
    Discover,
    Validate,
    ApplyUpsert,
    ApplyDelete,
    Pull,
}

// Flow Materialize operations defined in
// https://github.com/estuary/flow/blob/master/go/protocols/materialize/materialize.proto
#[derive(Debug, strum_macros::Display, ArgEnum, Clone)]
#[strum(serialize_all = "kebab_case")]
pub enum FlowMaterializeOperation {
    Spec,
    Validate,
    ApplyUpsert,
    ApplyDelete,
    Transactions,
}

// To be used as a trait bound for interceptors.
pub trait FlowOperation {}
impl FlowOperation for FlowCaptureOperation {}
impl FlowOperation for FlowMaterializeOperation {}

// An interceptor modifies the request/response streams between Flow runtime and the connector.
// InterceptorStream defines the type of input and output streams handled by interceptors.
pub type InterceptorStream = Pin<Box<dyn Stream<Item = std::io::Result<Bytes>> + Send + Sync>>;

// The generic param "T" below is bounded by FlowOperation.
// A converter is a function that contains the specific stream-handling logic of an interceptor.
type ConverterFn<T> = Box<dyn Fn(&T, InterceptorStream) -> Result<InterceptorStream, Error>>;
// An intercept is characterized by a pair of converters, corresponding to the handling logic of request and response streams, respectively.
pub type RequestResponseConverterPair<T> = (ConverterFn<T>, ConverterFn<T>);
pub trait Interceptor<T: FlowOperation> {
    fn get_converters() -> RequestResponseConverterPair<T> {
        (
            Box::new(|_op, stream| Ok(stream)),
            Box::new(|_op, stream| Ok(stream)),
        )
    }
}

// Two converter pairs can be composed together to form a new converter pair.
pub fn compose<T: 'static + FlowOperation>(
    a: RequestResponseConverterPair<T>,
    b: RequestResponseConverterPair<T>,
) -> RequestResponseConverterPair<T> {
    let (req_a, resp_a) = a;
    let (req_b, resp_b) = b;
    (
        Box::new(move |o, stream| (req_b)(o, (req_a)(o, stream)?)),
        // Response conversions are applied in the reverse order of the request conversions.
        Box::new(move |o, stream| (resp_a)(o, (resp_b)(o, stream)?)),
    )
}

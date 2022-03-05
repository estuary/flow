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

pub trait Interceptor<T: FlowOperation> {
    fn convert_command_args(&self, op: &T, args: Vec<String>) -> Result<Vec<String>, Error> {
        Ok(args)
    }

    fn convert_request(
        &self,
        pid: Option<u32>,
        op: &T,
        stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        Ok(stream)
    }

    fn convert_response(
        &self,
        op: &T,
        stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        Ok(stream)
    }
}

struct ComposedInterceptor<T: 'static + FlowOperation> {
    a: Box<dyn Interceptor<T>>,
    b: Box<dyn Interceptor<T>>,
}
impl<T: 'static + FlowOperation> Interceptor<T> for ComposedInterceptor<T> {
    fn convert_command_args(&self, op: &T, args: Vec<String>) -> Result<Vec<String>, Error> {
        self.a
            .convert_command_args(op, self.b.convert_command_args(op, args)?)
    }

    fn convert_request(
        &self,
        pid: Option<u32>,
        op: &T,
        stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        // Suppressing pid for interceptor b to ensure that only the first interceptor
        // in the chain is responsible to start the connector by sending a SIGCONT signal
        // to that PID.
        // This satisfy the current requirements, and we can extend it with
        // more complex connector starting logic that involves multiple interceptors.
        self.a
            .convert_request(pid, op, self.b.convert_request(None, op, stream)?)
    }

    fn convert_response(
        &self,
        op: &T,
        stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        self.b
            .convert_response(op, self.a.convert_response(op, stream)?)
    }
}

// Two interceptors can be composed together to form a new interceptor.
pub fn compose<T: 'static + FlowOperation>(
    a: Box<dyn Interceptor<T>>,
    b: Box<dyn Interceptor<T>>,
) -> Box<dyn Interceptor<T>> {
    Box::new(ComposedInterceptor { a, b })
}

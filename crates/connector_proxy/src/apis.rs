use bytes::Bytes;
use clap::ArgEnum;
use futures_core::stream::Stream;
use std::pin::Pin;

// The protocol that FlowRuntime is speaking with.
#[derive(Debug, strum_macros::Display, ArgEnum, PartialEq, Clone)]
#[strum(serialize_all = "snake_case")]
pub enum FlowRuntimeProtocol {
    Capture,
    Materialize,
}

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

// An interceptor modifies the request/response streams between Flow runtime and the connector.
// InterceptorStream defines the type of input and output streams handled by interceptors.
pub type InterceptorStream = Pin<Box<dyn Stream<Item = std::io::Result<Bytes>> + Send + Sync>>;

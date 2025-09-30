use futures::Stream;
use proto_flow::runtime::{CombineRequest as Request, CombineResponse as Response};

mod protocol;
mod serve;

pub trait RequestStream: Stream<Item = anyhow::Result<Request>> + Send + Unpin + 'static {}
impl<T: Stream<Item = anyhow::Result<Request>> + Send + Unpin + 'static> RequestStream for T {}

pub trait ResponseStream: Stream<Item = anyhow::Result<Response>> + Send + 'static {}
impl<T: Stream<Item = anyhow::Result<Response>> + Send + 'static> ResponseStream for T {}

pub struct Binding {
    key: Vec<doc::Extractor>,
    ser_policy: doc::SerPolicy,
    uuid_ptr: Option<json::Pointer>,
    values: Vec<doc::Extractor>,
}

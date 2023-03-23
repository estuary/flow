use futures::{Stream, TryStreamExt};
use prost::Message;
use proto_flow::derive::Request;
use proto_flow::{
    ops,
    runtime::{derive_request_ext, DeriveRequestExt},
};
use std::sync::Arc;

pub fn adapt_requests<R>(
    request_rx: R,
    set_log_level: Option<Arc<dyn Fn(ops::log::Level) + Send + Sync>>,
) -> impl Stream<Item = tonic::Result<Request>>
where
    R: futures::stream::Stream<Item = tonic::Result<Request>> + Send + 'static,
{
    request_rx.inspect_ok(move |request| {
        let Some(_open) = &request.open else { return };

        let Ok(DeriveRequestExt { open: Some(derive_request_ext::Open{log_level, ..}) , .. }) = Message::decode(
            request
                .internal
                .as_ref()
                .map(|i| i.value.clone())
                .unwrap_or_default(),
        ) else { return };

        if let (Some(log_level), Some(set_log_level)) = (ops::log::Level::from_i32(log_level), &set_log_level) {
                (set_log_level)(log_level);
        }
    })
}

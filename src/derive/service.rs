use super::{executor::TxnCtx, data_into_record_batches};
use bytes::Buf;
use futures::stream::{Stream};
use std::convert::Infallible;
use std::sync::Arc;
use warp::{filters::BoxedFilter, Filter, Reply};

// POST /transform -> Sets a document (in POST body) within the store.
fn rt_post_transform(store: Arc<Box<TxnCtx>>) -> BoxedFilter<(impl Reply,)> {
    warp::post()
        .and(warp::path!("transform"))
        .and(warp::body::stream())
        .and_then(move |body| run_transform(store.clone(), body))
        .boxed()
}

async fn run_transform(
    _store: Arc<Box<TxnCtx>>,
    req_body: impl Stream<Item = Result<impl Buf + Send + Sync + 'static, warp::Error>> + Send + Sync + 'static,
) -> Result<impl Reply, Infallible> {

    let req_batches = data_into_record_batches(req_body);

    // TODO actually run transform

    let resp = hyper::Response::builder()
        .status(200)
        .body(hyper::Body::wrap_stream(req_batches))
        .unwrap();

    Ok(resp)
}

pub fn build(store: Arc<Box<TxnCtx>>) -> BoxedFilter<(impl Reply + 'static,)> {
    rt_post_transform(store.clone()).boxed()
}

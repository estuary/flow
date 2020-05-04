use super::{executor::TxnCtx, parse_record_batches, Error};
use bytes::{Buf, Bytes};
use futures::stream::{TryStream, TryStreamExt};
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
    req_body: impl TryStream<Ok = impl Buf, Error = warp::Error> + Send + Sync + 'static,
) -> Result<impl Reply, Infallible> {
    // Map from impl Buf -> Bytes. As these are already Bytes, it uses a zero-cost specialization.
    let req_body = req_body.map_ok(|mut b| b.to_bytes());
    // Decode out RecordBatches of the body stream.
    let req_batches = parse_record_batches(req_body);

    let out = req_batches.into_stream();
    let resp = hyper::Response::builder()
        .status(200)
        .body(hyper::Body::wrap_stream(out))
        .unwrap();

    Ok(resp)
}

pub fn build(store: Arc<Box<TxnCtx>>) -> BoxedFilter<(impl Reply + 'static,)> {
    rt_post_transform(store.clone()).boxed()
}

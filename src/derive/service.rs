use super::{
    combine::Combiner,
    parse_record_batch,
    transform::{Context, Invoker},
    Error,
};
use crate::derive::combine::process_derived_batch;
use crate::derive::transform::process_source_batch;
use bytes::{Buf, Bytes};
use futures::stream::{Stream, StreamExt, TryStreamExt};
use std::sync::Arc;
use warp::{filters::BoxedFilter, Filter, Reply};

/* TODO(johnny): When re-working the derive service, it'll use something like this:

use estuary_protocol::consumer;
use estuary_protocol::recoverylog;
use futures::stream::Stream;
use std::pin::Pin;

#[derive(Debug)]
pub struct DeriveService {}

#[tonic::async_trait]
impl flow::derive_server::Derive for DeriveService {
    async fn restore_checkpoint(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<tonic::Response<consumer::Checkpoint>, tonic::Status> {
        unimplemented!();
    }

    type DeriveStream = Pin<
        Box<dyn Stream<Item = Result<flow::DeriveResponse, tonic::Status>> + Send + Sync + 'static>,
    >;

    async fn derive(
        &self,
        _request: tonic::Request<tonic::Streaming<flow::DeriveRequest>>,
    ) -> Result<tonic::Response<Self::DeriveStream>, tonic::Status> {
        unimplemented!();
    }

    async fn build_hints(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<tonic::Response<recoverylog::FsmHints>, tonic::Status> {
        unimplemented!();
    }
}
*/

// POST /transform -> Sets a document (in POST body) within the store.
fn rt_post_transform(store: Arc<Box<Context>>) -> BoxedFilter<(impl Reply,)> {
    warp::post()
        .and(warp::path!("transform"))
        .and(warp::body::stream())
        .map(move |req_body| {
            let (sender, resp_body) = hyper::Body::channel();
            run_transform(store.clone(), req_body, sender);

            hyper::Response::builder()
                .status(200)
                .body(resp_body)
                .unwrap()
        })
        .boxed()
}

// Spawns tasks which drive the transform request & response loops.
// Sadly this absurd impl Stream<> annotation is required to help the compiler deduce types.
// It must be here, in a proper function, because impl traits can't be used in closure signatures.
fn run_transform(
    ctx: Arc<Box<Context>>,
    rx: impl Stream<Item = Result<impl Buf + Send, warp::Error>> + Send + Sync + 'static,
    mut sender: hyper::body::Sender,
) {
    let (derived_tx, mut derived_rx) = futures::channel::mpsc::channel(3);
    let ctx_rl = ctx.clone();
    let ctx_wl = ctx;

    let read_loop = move || async move {
        let mut rem = Bytes::new();
        pin_utils::pin_mut!(rx);

        let mut transforms = Invoker::new(derived_tx);

        while let Some(mut buf) = rx.try_next().await? {
            let bytes = buf.to_bytes(); // Zero-cost, as Buf is already Bytes.

            if let Some(batch) = parse_record_batch(&mut rem, Some(bytes))? {
                process_source_batch(&ctx_rl, &mut transforms, batch).await?;
            }
        }
        parse_record_batch(&mut rem, None)?;

        // Drain ongoing transforms, ensuring each completed without error.
        transforms.drain().await?;
        // All done!
        Result::<(), Error>::Ok(())
    };

    let read_handle = tokio::spawn(async move {
        match read_loop().await {
            Err(err) => {
                log::error!("transform read-loop failed: {:?}", err);
                Err(err)
            }
            Ok(()) => {
                log::info!("transform read-loop finished");
                Ok(())
            }
        }
    });

    let write_loop = move || async move {
        let mut combiner = Combiner::new();

        while let Some(batch) = derived_rx.next().await {
            process_derived_batch(&ctx_wl, &mut combiner, batch)?;
        }

        for doc in combiner.into_iter() {
            let mut v = serde_json::to_vec(&doc)?;
            v.push(b'\n');
            sender.send_data(Bytes::from(v)).await?;
        }

        if let Err(err) = read_handle.await.unwrap() {
            log::warn!("aborting write-loop due to read_handle err: {:?}", err);
            sender.abort();
        }

        Result::<(), Error>::Ok(())
    };

    let _write_handle = tokio::spawn(async move {
        match write_loop().await {
            Err(err) => {
                log::error!("transform write-loop failed: {:?}", err);
                Err(err)
            }
            Ok(()) => {
                log::info!("transform write-loop finished");
                Ok(())
            }
        }
    });
}

pub fn build(store: Arc<Box<Context>>) -> BoxedFilter<(impl Reply + 'static,)> {
    rt_post_transform(store.clone()).boxed()
}

use super::{connector, protocol::*, RequestStream, ResponseStream, Task, Transaction};
use crate::{rocksdb::RocksDB, verify, Accumulator, LogHandler, Runtime};
use anyhow::Context;
use futures::channel::oneshot;
use futures::future::FusedFuture;
use futures::stream::FusedStream;
use futures::{FutureExt, SinkExt, StreamExt, TryStreamExt};
use proto_flow::capture::{request, Request, Response};
use std::collections::{BTreeMap, HashSet};

#[tonic::async_trait]
impl<L: LogHandler> proto_grpc::capture::connector_server::Connector for Runtime<L> {
    type CaptureStream = futures::stream::BoxStream<'static, tonic::Result<Response>>;

    async fn capture(
        &self,
        request: tonic::Request<tonic::Streaming<Request>>,
    ) -> tonic::Result<tonic::Response<Self::CaptureStream>> {
        let conn_info = request
            .extensions()
            .get::<tonic::transport::server::UdsConnectInfo>();
        tracing::debug!(?request, ?conn_info, "started capture request");

        let request_rx = crate::stream_status_to_error(request.into_inner());
        let response_rx = crate::stream_error_to_status(self.clone().serve_capture(request_rx));

        Ok(tonic::Response::new(response_rx.boxed()))
    }
}

impl<L: LogHandler> Runtime<L> {
    pub fn serve_capture(self, mut request_rx: impl RequestStream) -> impl ResponseStream {
        coroutines::try_coroutine(move |mut co| async move {
            let Some(request) = request_rx.try_next().await? else {
                return Ok::<(), anyhow::Error>(());
            };
            self.set_log_level(request.get_internal()?.log_level());

            let db = RocksDB::open(request.get_internal()?.rocksdb_descriptor.clone()).await?;
            let mut shapes = BTreeMap::new();
            let mut next = Some(request);

            while let Some(request) = next {
                self.set_log_level(request.get_internal()?.log_level());

                if request.open.is_some() {
                    next =
                        serve_session(&mut co, &db, request, &mut request_rx, &self, &mut shapes)
                            .await?;
                } else {
                    serve_unary(&mut co, &db, request, &self).await?;
                    next = request_rx.try_next().await?;
                }
            }
            Ok(())
        })
    }
}

async fn serve_unary<L: LogHandler>(
    co: &mut coroutines::Suspend<Response, ()>,
    db: &RocksDB,
    mut request: Request,
    runtime: &Runtime<L>,
) -> anyhow::Result<()> {
    let mut wb = rocksdb::WriteBatch::default();
    recv_client_unary(db, &mut request, &mut wb).await?;

    let (connector_tx, mut connector_rx) = connector::start(runtime, request.clone()).await?;
    std::mem::drop(connector_tx); // Send EOF.

    let verify = verify("connector", "unary response");
    let response = verify.not_eof(connector_rx.try_next().await?)?;
    () = co.yield_(recv_connector_unary(request, response)?).await;
    () = verify.is_eof(connector_rx.try_next().await?)?;

    if !wb.is_empty() {
        db.write_opt(wb, Default::default()).await?;
    }

    Ok(())
}

async fn serve_session<L: LogHandler>(
    co: &mut coroutines::Suspend<Response, ()>,
    db: &RocksDB,
    mut open: Request,
    request_rx: &mut impl RequestStream,
    runtime: &Runtime<L>,
    shapes_by_key: &mut BTreeMap<String, doc::Shape>,
) -> anyhow::Result<Option<Request>> {
    recv_client_open(&mut open, &db).await?;

    // Start connector stream and read Opened.
    let (mut connector_tx, mut connector_rx) = connector::start(runtime, open.clone()).await?;
    let opened = TryStreamExt::try_next(&mut connector_rx).await?;

    let (task, task_clone, mut shapes, accumulator, mut next_accumulator, opened) =
        recv_connector_opened(db, open, opened, shapes_by_key).await?;

    () = co.yield_(opened).await;

    // Spawn a task that reads the first transaction from the connector.
    let (mut yield_tx, yield_rx) = oneshot::channel();
    let mut next_txn = tokio::spawn(read_transaction(
        accumulator,
        connector_rx.fuse(),
        task_clone,
        super::LONG_POLL_TIMEOUT,
        yield_rx,
    ));

    let mut bindings_with_sourced_schema = HashSet::new();
    let mut last_checkpoints: u32 = 0; // Checkpoints in the last transaction.
    let mut buf = bytes::BytesMut::new();
    loop {
        // Receive initial request of a transaction: Acknowledge, Open, or EOF.
        let _: request::Acknowledge = match request_rx.try_next().await? {
            // An Acknowledge:
            //   a) acknowledges the completion of all prior commits, and
            //   b) long-polls for a transaction to be drained.
            //
            // The caller must await our reply before sending its next message.
            Some(Request {
                acknowledge: Some(ack),
                ..
            }) => ack,
            // An Open or Apply gracefully ends this session.
            Some(request) if request.open.is_some() || request.apply.is_some() => {
                *shapes_by_key = task.binding_shapes_by_key(shapes);
                return Ok(Some(request));
            }
            // Caller sent EOF which gracefully ends this RPC.
            None => return Ok(None),
            // Anything else is a protocol error.
            Some(request) => {
                return verify("client", "Acknowledge, Open, Apply, or EOF").fail(request);
            }
        };

        // Acknowledge committed checkpoints to the connector.
        if let Some(ack) = send_connector_acknowledge(&mut last_checkpoints, &task) {
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => anyhow::bail!(
                    "connector requested acknowledgements but is not processing its input",
                ),
                _ = connector_tx.feed(ack) => (), // We don't error on disconnection, only timeout.
            }
        }

        // Signal that we're ready for a transaction to yield, and then wait for it.
        std::mem::drop(yield_tx);
        let (accumulator, connector_rx, task_clone, mut txn) =
            next_txn.await.expect("read_transaction doesn't panic")?;

        // Immediately start a concurrent read of the next transaction.
        let (next_yield_tx, yield_rx) = oneshot::channel();
        next_txn = tokio::spawn(read_transaction(
            next_accumulator,
            connector_rx,
            task_clone,
            super::LONG_POLL_TIMEOUT,
            yield_rx,
        ));
        yield_tx = next_yield_tx;

        let (ready, response) = send_client_poll_result(&mut buf, &task, &txn);
        () = co.yield_(response).await;

        if !ready {
            next_accumulator = accumulator;
            continue;
        }

        // Prepare to drain `accumulator`.
        let (mut drainer, parser) = accumulator
            .into_drainer()
            .context("preparing to drain combiner")?;

        // Atomic WriteBatch into which we'll stage connector and runtime state updates.
        let mut wb = rocksdb::WriteBatch::default();

        // Must do this before calling `apply_sourced_schemas` as that
        // function clears out `txn.sourced_schemas`.
        for (binding_id, _) in txn.sourced_schemas.iter() {
            tracing::debug!(binding_id, "tracking sourced schema");
            bindings_with_sourced_schema.insert(*binding_id);
        }

        // Apply sourced schemas to inference before we widen from documents.
        // Assuming documents fit the source shape, this prevents unnecessary
        // widening (consider a schema with tight minItems / maxItems bounds).
        apply_sourced_schemas(&mut shapes, &task, &mut txn)?;

        while let Some(drained) = drainer.drain_next()? {
            let response = send_client_captured_or_checkpoint(
                &mut buf,
                drained,
                &mut shapes,
                &task,
                &mut txn,
                &mut wb,
                &bindings_with_sourced_schema,
            );
            () = co.yield_(response).await;
        }

        let checkpoint = send_client_final_checkpoint(&mut buf, &task, &txn);
        () = co.yield_(checkpoint).await;

        let start_commit = request_rx.try_next().await?;
        recv_client_start_commit(
            &db,
            start_commit,
            &shapes,
            &task,
            &txn,
            wb,
            &bindings_with_sourced_schema,
        )
        .await?;

        () = co.yield_(send_client_started_commit()).await;

        last_checkpoints = txn.checkpoints;
        next_accumulator = Accumulator::from_drainer(drainer, parser)?;
    }
}

pub async fn read_transaction<R: ResponseStream + FusedStream + Unpin>(
    mut accumulator: Accumulator,
    mut connector_rx: R,
    task: Task,
    timeout: std::time::Duration, // How long we'll wait for a first checkpoint.
    yield_rx: oneshot::Receiver<()>, // Signaled when we should return.
) -> anyhow::Result<(Accumulator, R, Task, Transaction)> {
    let timeout = tokio::time::sleep(timeout).fuse();
    let mut txn = Transaction::new();
    let mut yield_rx = yield_rx.fuse();
    tokio::pin!(timeout);

    // Loop over one or more response checkpoints.
    loop {
        let (woken, initial) = tokio::select! {
            initial = connector_rx.try_next(), if !txn.connector_eof && txn.captured_bytes < super::COMBINER_BYTE_THRESHOLD => (false, initial?),
            _ = &mut timeout => (true, None),
            _ = &mut yield_rx => (true, None),
        };
        match (woken, initial) {
            (false, Some(initial)) => {
                if txn.checkpoints == 0 {
                    txn.started_at = std::time::SystemTime::now();
                }

                () = read_checkpoint(
                    &mut accumulator,
                    &mut connector_rx,
                    initial,
                    &task,
                    &mut txn,
                )
                .await?;

                // Were we previously asked to yield, and only now have a checkpoint to return?
                if yield_rx.is_terminated() {
                    return Ok((accumulator, connector_rx, task, txn));
                }
            }
            (false, None) => {
                txn.connector_eof = true;
            }
            (true, _none) => {
                // Have we been asked to yield, and either have a non-empty transaction or reached our timeout?
                if yield_rx.is_terminated() && (txn.checkpoints != 0 || timeout.is_terminated()) {
                    return Ok((accumulator, connector_rx, task, txn));
                }
            }
        };
    }
}

async fn read_checkpoint(
    accumulator: &mut Accumulator,
    connector_rx: &mut (impl ResponseStream + Unpin),
    mut response: Response,
    task: &Task,
    txn: &mut Transaction,
) -> anyhow::Result<()> {
    // Read all Captured and SourcedSchema responses of the checkpoint.
    loop {
        if let Some(captured) = response.captured {
            recv_connector_captured(accumulator, captured, task, txn)?;
        } else if let Some(sourced) = response.sourced_schema {
            recv_connector_sourced_schema(sourced, task, txn)?;
        } else {
            break;
        }

        // Read next response.
        response = match connector_rx.try_next().await? {
            Some(response) => response,
            None => anyhow::bail!("unexpected capture connector EOF while within a checkpoint"),
        };
    }

    recv_connector_checkpoint(accumulator, response, task, txn)
}

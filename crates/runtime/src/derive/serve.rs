use super::{connector, protocol::*, RequestStream, ResponseStream, Transaction};
use crate::{rocksdb::RocksDB, verify, Accumulator, LogHandler, Runtime};
use anyhow::Context;
use futures::channel::mpsc;
use futures::stream::BoxStream;
use futures::{SinkExt, StreamExt, TryStreamExt};
use proto_flow::derive::{Request, Response};

#[tonic::async_trait]
impl<L: LogHandler> proto_grpc::derive::connector_server::Connector for Runtime<L> {
    type DeriveStream = futures::stream::BoxStream<'static, tonic::Result<Response>>;

    async fn derive(
        &self,
        request: tonic::Request<tonic::Streaming<Request>>,
    ) -> tonic::Result<tonic::Response<Self::DeriveStream>> {
        let conn_info = request
            .extensions()
            .get::<tonic::transport::server::UdsConnectInfo>();
        tracing::debug!(?request, ?conn_info, "started derive request");

        let request_rx = crate::stream_status_to_error(request.into_inner());
        let response_rx = crate::stream_error_to_status(self.clone().serve_derive(request_rx));

        Ok(tonic::Response::new(response_rx.boxed()))
    }
}

impl<L: LogHandler> Runtime<L> {
    pub fn serve_derive(self, mut request_rx: impl RequestStream) -> impl ResponseStream {
        coroutines::try_coroutine(move |mut co| async move {
            let Some(request) = request_rx.try_next().await? else {
                return Ok::<(), anyhow::Error>(());
            };
            self.set_log_level(request.get_internal()?.log_level());

            let db = RocksDB::open(request.get_internal()?.rocksdb_descriptor.clone()).await?;
            let mut shape = doc::Shape::nothing();
            let mut next = Some(request);

            while let Some(request) = next {
                self.set_log_level(request.get_internal()?.log_level());

                if request.open.is_some() {
                    next = serve_session(&mut co, &db, request, &mut request_rx, &self, &mut shape)
                        .await?;
                } else {
                    serve_unary(&self, request, &mut co).await?;
                    next = request_rx.try_next().await?;
                }
            }
            Ok(())
        })
    }
}

async fn serve_unary<L: LogHandler>(
    runtime: &Runtime<L>,
    request: Request,
    co: &mut coroutines::Suspend<Response, ()>,
) -> anyhow::Result<()> {
    let (connector_tx, mut connector_rx) = connector::start(runtime, request.clone()).await?;
    std::mem::drop(connector_tx); // Send EOF.

    let verify = verify("connector", "unary response");
    let response = verify.not_eof(connector_rx.try_next().await?)?;
    () = co.yield_(recv_connector_unary(request, response)?).await;
    () = verify.is_eof(connector_rx.try_next().await?)?;
    Ok(())
}

async fn serve_session<L: LogHandler>(
    co: &mut coroutines::Suspend<Response, ()>,
    db: &RocksDB,
    mut open: Request,
    request_rx: &mut impl RequestStream,
    runtime: &Runtime<L>,
    shape: &mut doc::Shape,
) -> anyhow::Result<Option<Request>> {
    recv_client_open(&mut open, &db).await?;

    // Start connector stream and read Opened.
    let (mut connector_tx, mut connector_rx) = connector::start(runtime, open.clone()).await?;
    let opened = TryStreamExt::try_next(&mut connector_rx).await?;

    let (task, mut validators, mut accumulator, mut last_checkpoint, opened) =
        recv_connector_opened(&db, open, opened).await?;

    () = co.yield_(opened).await;

    // Attach the current generation ID to the derivation's inferred schema.
    let generation_id = serde_json::Value::String(task.collection_generation_id.to_string());
    if shape.annotations.get(crate::X_GENERATION_ID) != Some(&generation_id) {
        *shape = doc::Shape::nothing();
        shape
            .annotations
            .insert(crate::X_GENERATION_ID.to_string(), generation_id);
    }

    let mut buf = bytes::BytesMut::new();
    loop {
        let mut saw_flush = false;
        let mut saw_flushed = false;
        let mut saw_reset = false;
        let mut send_fut = None;
        let mut txn = Transaction::new();

        // Loop over client requests and connector responses until the transaction has flushed.
        while !saw_flushed {
            enum Step {
                ClientRx(Option<Request>),
                ConnectorRx(Option<Response>),
                ConnectorTx(Result<(), mpsc::SendError>),
            }

            let step = if let Some(forward) = &mut send_fut {
                tokio::select! {
                    result = forward => Step::ConnectorTx(result),
                    response = connector_rx.try_next() => Step::ConnectorRx(response?),
                }
            } else {
                tokio::select! {
                    request = request_rx.try_next(), if !saw_flush => Step::ClientRx(request?),
                    response = connector_rx.try_next() => Step::ConnectorRx(response?),
                }
            };

            match step {
                Step::ClientRx(None) if !txn.started => {
                    drain_connector(connector_tx, connector_rx).await?;
                    return Ok(None); // Clean EOF.
                }
                Step::ClientRx(Some(open @ Request { open: Some(_), .. })) if !txn.started => {
                    drain_connector(connector_tx, connector_rx).await?;
                    return Ok(Some(open)); // Restart a new session.
                }
                Step::ClientRx(Some(reset @ Request { reset: Some(_), .. })) if !txn.started => {
                    send_fut = Some(connector_tx.feed(reset));
                }
                Step::ClientRx(request) => {
                    if let Some(send) = recv_client_read_or_flush(
                        request,
                        &mut saw_flush,
                        &task,
                        &mut txn,
                        &mut validators,
                    )? {
                        send_fut = Some(connector_tx.feed(send));
                    }
                }
                Step::ConnectorRx(response) => {
                    recv_connector_published_or_flushed(
                        &mut accumulator,
                        response,
                        saw_flush,
                        &mut saw_flushed,
                        &task,
                        &mut txn,
                    )?;
                }
                Step::ConnectorTx(result) => {
                    if let Err(_send_err) = result {
                        saw_reset = true; // `connector_rx` will likely have an error.
                    }
                    send_fut = None;
                }
            }
        }

        if saw_reset {
            anyhow::bail!(
                "connector reset its connection unexpectedly but sent Flushed without an error"
            );
        }

        // Prepare to drain `accumulator`.
        let (mut drainer, parser) = accumulator
            .into_drainer()
            .context("preparing to drain combiner")?;

        while let Some(drained) = drainer.drain_next()? {
            let published = send_client_published(&mut buf, drained, shape, &task, &mut txn);
            () = co.yield_(published).await;
        }
        () = co.yield_(send_client_flushed(&mut buf, &task, &txn)).await;

        // Read StartCommit and forward to the connector.
        let start_commit = request_rx.try_next().await?;
        let (start_commit, wb) = recv_client_start_commit(last_checkpoint, start_commit, &mut txn)?;
        connector_tx
            .try_send(start_commit)
            .expect("sender is empty");

        // Read StartedCommit and forward to the client.
        let started_commit = connector_rx.try_next().await?;
        let started_commit =
            recv_connector_started_commit(&db, started_commit, shape, &task, &txn, wb).await?;
        () = co.yield_(started_commit).await;

        last_checkpoint = txn.checkpoint;
        accumulator = Accumulator::from_drainer(drainer, parser)?;
    }
}

async fn drain_connector(
    tx: mpsc::Sender<Request>,
    mut rx: BoxStream<'static, anyhow::Result<Response>>,
) -> anyhow::Result<()> {
    std::mem::drop(tx);
    verify("connector", "EOF").is_eof(rx.try_next().await?)
}

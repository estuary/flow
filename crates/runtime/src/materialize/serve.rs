use super::{connector, protocol::*, LoadKeySet, RequestStream, ResponseStream, Transaction};
use crate::Accumulator;
use crate::{rocksdb::RocksDB, verify, LogHandler, Runtime};
use anyhow::Context;
use futures::channel::mpsc;
use futures::stream::BoxStream;
use futures::{SinkExt, StreamExt, TryStreamExt};
use proto_flow::materialize::{Request, Response};

#[tonic::async_trait]
impl<L: LogHandler> proto_grpc::materialize::connector_server::Connector for Runtime<L> {
    type MaterializeStream = futures::stream::BoxStream<'static, tonic::Result<Response>>;

    async fn materialize(
        &self,
        request: tonic::Request<tonic::Streaming<Request>>,
    ) -> tonic::Result<tonic::Response<Self::MaterializeStream>> {
        let conn_info = request
            .extensions()
            .get::<tonic::transport::server::UdsConnectInfo>();
        tracing::debug!(?request, ?conn_info, "started materialize request");

        let request_rx = crate::stream_status_to_error(request.into_inner());
        let response_rx = crate::stream_error_to_status(self.clone().serve_materialize(request_rx));

        Ok(tonic::Response::new(response_rx.boxed()))
    }
}

impl<L: LogHandler> Runtime<L> {
    pub fn serve_materialize(self, mut request_rx: impl RequestStream) -> impl ResponseStream {
        coroutines::try_coroutine(move |mut co| async move {
            let Some(request) = request_rx.try_next().await? else {
                return Ok::<(), anyhow::Error>(());
            };
            self.set_log_level(request.get_internal()?.log_level());

            let db = RocksDB::open(request.get_internal()?.rocksdb_descriptor.clone()).await?;
            let mut next = Some(request);

            while let Some(request) = next {
                self.set_log_level(request.get_internal()?.log_level());

                if request.open.is_some() {
                    next = serve_session(&mut co, &db, request, &mut request_rx, &self).await?;
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
) -> anyhow::Result<Option<Request>> {
    recv_client_open(&mut open, &db).await?;

    // Start connector stream and read Opened.
    let (mut connector_tx, mut connector_rx) = connector::start(runtime, open.clone()).await?;
    let opened = TryStreamExt::try_next(&mut connector_rx).await?;

    let (task, mut accumulator, mut last_checkpoint, opened, mut max_keys) =
        recv_connector_opened(&db, open, opened).await?;

    () = co.yield_(opened).await;

    let mut buf = bytes::BytesMut::new();
    loop {
        // Read and forward Acknowledge.
        match request_rx.try_next().await? {
            Some(ack) if ack.acknowledge.is_some() => {
                connector_tx.try_send(ack).expect("sender is empty");
            }
            request => return verify("client", "Acknowledge").fail(request),
        }

        let mut load_keys = LoadKeySet::default();
        let mut saw_acknowledged = false;
        let mut saw_flush = false;
        let mut saw_flushed = false;
        let mut saw_reset = false;
        let mut send_fut = None;
        let mut txn = Transaction::new();
        let mut wb = rocksdb::WriteBatch::default();

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
                // An Open or Apply gracefully ends this session.
                Step::ClientRx(Some(request))
                    if !txn.started && (request.open.is_some() || request.apply.is_some()) =>
                {
                    drain_connector(connector_tx, connector_rx).await?;
                    return Ok(Some(request)); // Restart a new session.
                }
                Step::ClientRx(request) => {
                    if let Some(send) = recv_client_load_or_flush(
                        &mut accumulator,
                        &mut buf,
                        &mut load_keys,
                        &mut max_keys,
                        request,
                        &mut saw_acknowledged,
                        &mut saw_flush,
                        &task,
                        &mut txn,
                    )? {
                        send_fut = Some(connector_tx.feed(send));
                    }
                }
                Step::ConnectorRx(response) => {
                    if let Some(send) = recv_connector_acked_or_loaded_or_flushed(
                        &mut accumulator,
                        &db,
                        response,
                        &mut saw_acknowledged,
                        &mut saw_flush,
                        &mut saw_flushed,
                        &task,
                        &mut txn,
                        &mut wb,
                    )
                    .await?
                    {
                        () = co.yield_(send).await;
                    }
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

        // We must durably commit updates to `max_keys` now, before we send any Store
        // requests into the connector, because the connector may not be
        // transactional and could immediately Store sent documents.
        // `wb` may also have a connector state update from Flushed which is also written.
        persist_max_keys(db, &mut max_keys, &task, wb).await?;

        // Prepare to drain `accumulator`.
        let (mut drainer, parser) = accumulator
            .into_drainer()
            .context("preparing to drain combiner")?;

        while let Some(drained) = drainer.drain_next()? {
            let store = send_connector_store(&mut buf, drained, &task, &mut txn);

            tokio::select! {
                biased; // Prefer to feed the channel, and poll the connector only if there's no room.
                Ok(()) = connector_tx.feed(store) => (),
                response = connector_rx.try_next() => {
                    return verify("connector", "no response or EOF during Store phase").fail(response?);
                }
            }
        }
        () = co.yield_(send_client_flushed(&mut buf, &task, &txn)).await;

        // Read StartCommit and forward to the connector.
        let start_commit = request_rx.try_next().await?;
        let (start_commit, wb) = recv_client_start_commit(last_checkpoint, start_commit, &mut txn)?;

        tokio::select! {
            Ok(()) = connector_tx.feed(start_commit) => (),
            response = connector_rx.try_next() => {
                return verify("connector", "no response or EOF during Flush phase").fail(response?);
            }
        }

        // Read StartedCommit and forward to the client.
        let started_commit = connector_rx.try_next().await?;
        let started_commit = recv_connector_started_commit(&db, started_commit, wb).await?;
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

    match rx.try_next().await? {
        // Connector may immediately EOF.
        None => Ok(()),
        // Or it may return Acknowledged, which must be followed by EOF.
        Some(ack) if ack.acknowledged.is_some() => {
            verify("connector", "EOF").is_eof(rx.try_next().await?)
        }
        response => return verify("connector", "Acknowledged or EOF").fail(response),
    }
}

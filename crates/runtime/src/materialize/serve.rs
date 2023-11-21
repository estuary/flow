use super::{connector, protocol::*, RequestStream, ResponseStream, Transaction};
use crate::{rocksdb::RocksDB, verify, LogHandler, Runtime};
use anyhow::Context;
use futures::channel::mpsc;
use futures::stream::BoxStream;
use futures::{SinkExt, StreamExt, TryStreamExt};
use proto_flow::materialize::{Request, Response};
use std::collections::HashSet;

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
            let Some(mut open) = serve_unary(&self, &mut request_rx, &mut co).await? else {
                return Ok::<(), anyhow::Error>(());
            };

            let db = recv_client_first_open(&open)?;

            while let Some(next) = serve_session(&mut co, &db, open, &mut request_rx, &self).await?
            {
                open = next;
            }
            Ok(())
        })
    }
}

async fn serve_unary<L: LogHandler>(
    runtime: &Runtime<L>,
    request_rx: &mut impl RequestStream,
    co: &mut coroutines::Suspend<Response, ()>,
) -> anyhow::Result<Option<Request>> {
    while let Some(request) = request_rx.try_next().await? {
        if request.open.is_some() {
            return Ok(Some(request));
        }
        let (connector_tx, mut connector_rx) = connector::start(runtime, request.clone()).await?;
        std::mem::drop(connector_tx); // Send EOF.

        let verify = verify("connector", "unary response");
        let response = verify.not_eof(connector_rx.try_next().await?)?;
        () = co.yield_(recv_unary(request, response)?).await;
        () = verify.is_eof(connector_rx.try_next().await?)?;
    }
    Ok(None)
}

async fn serve_session<L: LogHandler>(
    co: &mut coroutines::Suspend<Response, ()>,
    db: &RocksDB,
    mut open: Request,
    request_rx: &mut impl RequestStream,
    runtime: &Runtime<L>,
) -> anyhow::Result<Option<Request>> {
    recv_client_open(&mut open, &db)?;

    // Start connector stream and read Opened.
    let (mut connector_tx, mut connector_rx) = connector::start(runtime, open.clone()).await?;
    let opened = TryStreamExt::try_next(&mut connector_rx).await?;

    let (task, mut accumulator, mut last_checkpoint, opened) =
        recv_connector_opened(&db, &open, opened)?;

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

        // Loop over EOF and Open until an initial Load or Flush.
        let initial: Request = loop {
            match request_rx.try_next().await? {
                None => {
                    drain_connector(connector_tx, connector_rx).await?;
                    return Ok(None);
                }
                Some(open @ Request { open: Some(_), .. }) => {
                    drain_connector(connector_tx, connector_rx).await?;
                    return Ok(Some(open));
                }
                Some(load @ Request { load: Some(_), .. }) => break load,
                Some(flush @ Request { flush: Some(_), .. }) => break flush,
                request => return verify("client", "EOF, Open, Load, or Flush").fail(request),
            }
        };

        let mut txn = Transaction::new();
        txn.started_at = std::time::SystemTime::now();

        // TODO(johnny): Use RocksDB to spill this to disk.
        let mut load_keys: HashSet<(u32, bytes::Bytes)> = HashSet::new();

        enum Step {
            ClientRx(Option<Request>),
            ConnectorRx(Option<Response>),
            ConnectorTx(Result<(), mpsc::SendError>),
        }
        let mut saw_acknowledged = false;
        let mut saw_flush = false;
        let mut saw_flushed = false;
        let mut saw_reset = false;
        let mut send_fut = None;
        let mut step = Step::ClientRx(Some(initial));

        // Loop over client requests and connector responses until the transaction has flushed.
        loop {
            match step {
                Step::ClientRx(request) => {
                    if let Some(send) = recv_client_load_or_flush(
                        &mut accumulator,
                        &mut buf,
                        &mut load_keys,
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
                        response,
                        &mut saw_acknowledged,
                        &mut saw_flush,
                        &mut saw_flushed,
                        &mut txn,
                    )? {
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

            if saw_flush && saw_flushed {
                break;
            }

            step = if let Some(forward) = &mut send_fut {
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
        }

        if saw_reset {
            anyhow::bail!(
                "connector reset its connection unexpectedly but sent Flushed without an error"
            );
        }

        // Prepare to drain `accumulator`.
        let mut drainer = accumulator
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
        let started_commit = recv_connector_started_commit(&db, started_commit, wb)?;
        () = co.yield_(started_commit).await;

        last_checkpoint = txn.checkpoint;
        accumulator = drainer.into_new_accumulator()?;
    }
}

async fn drain_connector(
    tx: mpsc::Sender<Request>,
    mut rx: BoxStream<'static, anyhow::Result<Response>>,
) -> anyhow::Result<()> {
    std::mem::drop(tx);

    match rx.try_next().await? {
        Some(ack) if ack.acknowledged.is_some() => (),
        response => return verify("connector", "Acknowledged").fail(response),
    }
    verify("connector", "EOF").is_eof(rx.try_next().await?)
}

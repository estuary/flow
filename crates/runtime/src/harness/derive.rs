use super::{Read, Reader};
use crate::derive::ResponseStream;
use crate::{rocksdb::RocksDB, verify, LogHandler, Runtime};
use anyhow::Context;
use futures::{channel::mpsc, TryStreamExt};
use proto_flow::derive::{request, response, Request, Response};
use proto_flow::flow;
use proto_flow::runtime::{self, derive_request_ext};
use std::pin::Pin;

pub fn run_derive<L: LogHandler>(
    reader: impl Reader,
    runtime: Runtime<L>,
    sessions: Vec<usize>,
    spec: &flow::CollectionSpec,
    state: models::RawValue,
    state_dir: &std::path::Path,
    timeout: std::time::Duration,
) -> impl ResponseStream {
    let spec = spec.clone();
    let state_dir = state_dir.to_owned();
    let mut state: String = state.into();

    // TODO(johnny): extract from spec?
    let version = super::unique_version();

    coroutines::try_coroutine(move |mut co| async move {
        let (mut request_tx, request_rx) = mpsc::channel(crate::CHANNEL_BUFFER);
        let response_rx = runtime.serve_derive(request_rx);
        tokio::pin!(response_rx);

        let state_dir = state_dir.to_str().context("tempdir is not utf8")?;
        let rocksdb_desc = Some(runtime::RocksDbDescriptor {
            rocksdb_env_memptr: 0,
            rocksdb_path: state_dir.to_owned(),
        });
        let open_ext = derive_request_ext::Open {
            rocksdb_descriptor: rocksdb_desc.clone(),
            sqlite_vfs_uri: format!("file://{state_dir}/sqlite.db"),
        };

        for target_transactions in sessions {
            () = run_session(
                &mut co,
                &open_ext,
                reader.clone(),
                &mut request_tx,
                &mut response_rx,
                &spec,
                &mut state,
                target_transactions,
                timeout,
                &version,
            )
            .await?;
        }

        std::mem::drop(request_tx);
        verify("runtime", "EOF").is_eof(response_rx.try_next().await?)?;

        // Re-open RocksDB.
        let rocksdb = RocksDB::open(rocksdb_desc)?;

        tracing::debug!(
            checkpoint = ?::ops::DebugJson(rocksdb.load_checkpoint()?),
            "final runtime checkpoint",
        );

        // Extract and yield the final connector state
        let state = rocksdb.load_connector_state()?;
        () = co
            .yield_(Response {
                started_commit: Some(response::StartedCommit {
                    state: state.map(|updated_json| flow::ConnectorState {
                        updated_json,
                        merge_patch: false,
                    }),
                }),
                ..Default::default()
            })
            .await;

        Ok(())
    })
}

async fn run_session(
    co: &mut coroutines::Suspend<Response, ()>,
    open_ext: &derive_request_ext::Open,
    reader: impl Reader,
    request_tx: &mut mpsc::Sender<anyhow::Result<Request>>,
    response_rx: &mut Pin<&mut impl ResponseStream>,
    spec: &flow::CollectionSpec,
    state: &mut String,
    target_transactions: usize,
    timeout: std::time::Duration,
    version: &str,
) -> anyhow::Result<()> {
    // Send Open.
    let open = Request {
        open: Some(request::Open {
            collection: Some(spec.clone()),
            version: version.to_string(),
            range: Some(flow::RangeSpec {
                key_begin: 0,
                key_end: u32::MAX,
                r_clock_begin: 0,
                r_clock_end: u32::MAX,
            }),
            state_json: std::mem::take(state),
        }),
        ..Default::default()
    }
    .with_internal(|internal| {
        internal.open = Some(open_ext.clone());
    });
    request_tx.try_send(Ok(open)).expect("sender is empty");

    // Receive Opened.
    let opened_ext = match response_rx.try_next().await? {
        Some(opened) if opened.opened.is_some() && !opened.internal.is_empty() => {
            let opened_ext = opened.get_internal()?;
            () = co.yield_(opened).await;
            opened_ext
        }
        response => return verify("runtime", "Opened").fail(response),
    };

    let checkpoint = opened_ext
        .opened
        .context("expected OpenedExt")?
        .runtime_checkpoint
        .unwrap_or_default();

    let read_rx = reader.start_for_derivation(&spec, checkpoint);
    tokio::pin!(read_rx);

    for _transaction in 0..target_transactions {
        let deadline = tokio::time::sleep(timeout);
        tokio::pin!(deadline);

        let mut started = false;

        // Read documents until a checkpoint.
        let checkpoint = loop {
            let read = tokio::select! {
                read = read_rx.try_next() => read?,
                () = deadline.as_mut(), if !started => {
                    tracing::info!(?timeout, "session ending upon reaching timeout");
                    return Ok(());
                },
            };
            started = true;

            let (forward, checkpoint) = match read {
                None => {
                    tracing::info!("session ending because reader returned EOF");
                    return Ok(());
                }
                // Forward a Read document to the runtime.
                Some(Read::Document { binding, doc }) => (
                    Request {
                        read: Some(request::Read {
                            doc_json: doc,
                            transform: binding,
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    None,
                ),
                // Forward a Flush to the runtime, then go on to commit a checkpoint.
                Some(Read::Checkpoint(checkpoint)) => (
                    Request {
                        flush: Some(request::Flush {}),
                        ..Default::default()
                    },
                    Some(checkpoint),
                ),
            };

            () = crate::exchange(Ok(forward), request_tx, response_rx)
                .try_for_each(
                    |response| async move { verify("runtime", "no response").fail(response) },
                )
                .await?;

            if let Some(checkpoint) = checkpoint {
                break checkpoint;
            }
        };

        // Receive Published and then Flushed.
        let mut done = false;
        while !done {
            let verify = verify("runtime", "Published or Flushed");
            let response = verify.not_eof(response_rx.try_next().await?)?;

            done = match &response {
                Response {
                    published: Some(_), ..
                } => false,
                Response {
                    flushed: Some(response::Flushed {}),
                    ..
                } => true,
                _ => return verify.fail(response),
            };
            () = co.yield_(response).await;
        }

        // Send StartCommit.
        request_tx
            .try_send(Ok(Request {
                start_commit: Some(request::StartCommit {
                    runtime_checkpoint: Some(checkpoint),
                }),
                ..Default::default()
            }))
            .expect("sender is empty");

        // Receive StartedCommit.
        match response_rx.try_next().await? {
            Some(
                started_commit @ Response {
                    started_commit: Some(_),
                    ..
                },
            ) => {
                () = co.yield_(started_commit).await;
            }
            response => return verify("runtime", "StartedCommit").fail(response),
        }
    }

    Ok(())
}

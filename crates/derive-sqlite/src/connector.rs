use super::{dbutil, do_validate, parse_validate, Config, Lambda, Param, Transform};
use anyhow::Context;
use futures::channel::mpsc;
use futures::TryStreamExt;
use futures::{SinkExt, Stream};
use prost::Message;
use proto_flow::runtime::{derive_response_ext, DeriveRequestExt, DeriveResponseExt};
use proto_flow::{
    derive::{request, response, Request, Response},
    flow, RuntimeCheckpoint,
};

pub fn connector<R>(
    _peek_request: &Request,
    request_rx: R,
) -> tonic::Result<impl Stream<Item = tonic::Result<Response>>>
where
    R: futures::stream::Stream<Item = tonic::Result<Request>> + Send + Unpin + 'static,
{
    let (mut response_tx, response_rx) = mpsc::channel(16);

    tokio::runtime::Handle::current().spawn_blocking(move || {
        futures::executor::block_on(async move {
            if let Err(status) = serve(request_rx, &mut response_tx).await {
                _ = response_tx.send(Err(status)).await;
            }
        })
    });

    Ok(response_rx)
}

async fn serve<R>(
    mut request_rx: R,
    response_tx: &mut mpsc::Sender<tonic::Result<Response>>,
) -> tonic::Result<()>
where
    R: futures::stream::Stream<Item = tonic::Result<Request>> + Send + Unpin + 'static,
{
    let tokio_handle = tokio::runtime::Handle::current();
    // Configured migration blocks of the last Request.Open.
    let mut migrations: Vec<String> = Vec::new();
    // Configured transform blocks and parameters of the last Request.Open.
    let mut transforms: Vec<Transform> = Vec::new();
    // A possibly opened Sqlite DB context.
    let mut maybe_handle: Option<Handle> = None;

    loop {
        match request_rx.try_next().await? {
            None => return Ok(()),
            Some(Request {
                validate: Some(validate),
                ..
            }) => {
                let validated = parse_validate(validate)
                    .and_then(|(migrations, transforms)| do_validate(&migrations, &transforms))
                    .map_err(anyhow_to_status)?;

                let _ = response_tx
                    .send(Ok(Response {
                        validated: Some(validated),
                        ..Default::default()
                    }))
                    .await;
            }
            Some(Request {
                open: Some(open),
                internal,
                ..
            }) => {
                let sqlite_uri: String;
                (sqlite_uri, migrations, transforms) =
                    parse_open(open, internal).map_err(anyhow_to_status)?;

                let (handle, runtime_checkpoint) =
                    Handle::new(&sqlite_uri, &migrations, &transforms).map_err(anyhow_to_status)?;

                // Send Opened extended with our recovered runtime checkpoint.
                let _ = response_tx
                    .send(Ok(Response {
                        opened: Some(response::Opened {}),
                        internal: Some(proto_flow::Any {
                            type_url: "flow://runtime.DeriveResponseExt".to_string(),
                            value: DeriveResponseExt {
                                opened: Some(derive_response_ext::Opened {
                                    runtime_checkpoint: Some(runtime_checkpoint),
                                }),
                                ..Default::default()
                            }
                            .encode_to_vec()
                            .into(),
                        }),

                        ..Default::default()
                    }))
                    .await;

                maybe_handle = Some(handle);
            }
            Some(Request {
                read: Some(read), ..
            }) => {
                let handle = maybe_handle
                    .as_mut()
                    .ok_or_else(|| tonic::Status::invalid_argument("Read without Open"))?;

                do_read(&mut handle.transforms, read, response_tx, &tokio_handle)
                    .map_err(anyhow_to_status)?;
            }
            Some(Request {
                flush: Some(request::Flush {}),
                ..
            }) => {
                // Send Flushed to runtime.
                let _ = response_tx
                    .send(Ok(Response {
                        flushed: Some(response::Flushed {}),
                        ..Default::default()
                    }))
                    .await;
            }
            Some(Request {
                start_commit: Some(request::StartCommit { runtime_checkpoint }),
                ..
            }) => {
                let handle = maybe_handle
                    .as_ref()
                    .ok_or_else(|| tonic::Status::invalid_argument("StartCommit without Open"))?;

                let started_commit =
                    do_commit(handle.conn, runtime_checkpoint).map_err(anyhow_to_status)?;

                // Send StartedCommit to runtime.
                let _ = response_tx
                    .send(Ok(Response {
                        started_commit: Some(started_commit),
                        ..Default::default()
                    }))
                    .await;
            }
            Some(Request {
                reset: Some(request::Reset {}),
                ..
            }) => {
                // Replace with a new :memory: database with the same configuration.
                let (db, _runtime_checkpoint) =
                    Handle::new(":memory:", &migrations, &transforms).map_err(anyhow_to_status)?;
                maybe_handle = Some(db);
            }
            Some(malformed) => {
                return Err(tonic::Status::invalid_argument(format!(
                    "invalid request {malformed:?}"
                )))
            }
        }
    }
}

fn parse_open(
    open: request::Open,
    internal: Option<proto_flow::Any>,
) -> anyhow::Result<(String, Vec<String>, Vec<Transform>)> {
    let request::Open {
        collection,
        range: _,
        state_json: _,
        version: _,
    } = open;

    let internal = internal.unwrap_or_default();
    let DeriveRequestExt { open: open_ext, .. } =
        Message::decode(internal.value).context("internal is a DeriveRequestExt")?;
    let open_ext = open_ext.unwrap_or_default();

    let mut sqlite_uri = open_ext.sqlite_vfs_uri;
    if sqlite_uri.is_empty() {
        sqlite_uri = ":memory:".to_string();
    }

    let flow::CollectionSpec { derivation, .. } = collection.unwrap();

    let flow::collection_spec::Derivation {
        config_json,
        transforms,
        ..
    } = derivation.unwrap();

    let config: Config = serde_json::from_str(&config_json)
        .with_context(|| format!("failed to parse SQLite configuration: {config_json}"))?;

    let transforms: Vec<Transform> = transforms
        .into_iter()
        .map(|transform| {
            let flow::collection_spec::derivation::Transform {
                name,
                collection: source,
                lambda_config_json,
                shuffle_lambda_config_json: _,
                ..
            } = transform;

            let source = source.unwrap();
            let params: Vec<_> = source.projections.iter().map(Param::new).collect();

            let block: String = serde_json::from_str(&lambda_config_json).with_context(|| {
                format!("failed to parse SQLite lambda block: {lambda_config_json}")
            })?;

            Ok(Transform {
                name,
                block,
                source: source.name,
                params,
            })
        })
        .collect::<Result<_, anyhow::Error>>()?;

    Ok((sqlite_uri, config.migrations, transforms))
}

fn do_read<'db>(
    transforms: &mut [Vec<Lambda<'db>>],
    read: request::Read,
    response_tx: &mut mpsc::Sender<tonic::Result<Response>>,
    tokio_handle: &tokio::runtime::Handle,
) -> anyhow::Result<()> {
    let request::Read {
        transform,
        doc_json,
        uuid: _,
        shuffle: _,
    } = read;

    let stack = transforms
        .get_mut(transform as usize)
        .with_context(|| format!("invalid transform index {transform}"))?;

    let doc: serde_json::Value = serde_json::from_str(&doc_json)
        .with_context(|| format!("couldn't parse read document as JSON: {doc_json}",))?;

    // Invoke each lambda of the stack in turn, streaming published documents into `response_tx`.
    // It's important that we don't block here -- these result sets could be very large.
    for (index, lambda) in stack.iter_mut().enumerate() {
        let it = lambda
            .invoke(&doc)
            .with_context(|| format!("failed to invoke lambda statement at offset {index}"))?;

        let it = it.map(|published| match published {
            Ok(published) => Ok(Ok(Response {
                published: Some(response::Published {
                    doc_json: published.to_string(),
                }),
                ..Default::default()
            })),
            Err(err) => Ok(Err(anyhow_to_status(err.into()))),
        });

        _ = tokio_handle.block_on(response_tx.send_all(&mut futures::stream::iter(it)));
    }
    Ok(())
}

fn do_commit(
    conn: &rusqlite::Connection,
    runtime_checkpoint: Option<RuntimeCheckpoint>,
) -> anyhow::Result<response::StartedCommit> {
    if let Some(runtime_checkpoint) = runtime_checkpoint {
        let () = dbutil::update_checkpoint(conn, runtime_checkpoint)?;
    }
    dbutil::commit_and_begin(conn)?;

    Ok(response::StartedCommit { state: None })
}

struct Handle {
    conn: &'static rusqlite::Connection,
    transforms: Vec<Vec<Lambda<'static>>>,
}

impl Handle {
    fn new(
        sqlite_uri: &str,
        migrations: &[String],
        transforms: &[Transform],
    ) -> anyhow::Result<(Handle, RuntimeCheckpoint)> {
        let (conn, runtime_checkpoint) = dbutil::open(sqlite_uri, &migrations)?;

        // Place into Self so it's covered by our Drop implementation.
        let mut db = Self {
            conn: Box::leak(Box::new(conn)),
            transforms: Vec::new(),
        };
        db.transforms = dbutil::build_transforms(&db.conn, &transforms)?;

        Ok((db, runtime_checkpoint))
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        // Force lambdas to drop first, before the database Connection.
        self.transforms.clear();

        // Take ownership of the boxed Connection to drop it.
        let db: *const _ = self.conn as *const _;
        let db: *mut _ = db as *mut rusqlite::Connection;
        unsafe { Box::from_raw(db) };
    }
}

fn anyhow_to_status(err: anyhow::Error) -> tonic::Status {
    tonic::Status::internal(format!("{err:?}"))
}

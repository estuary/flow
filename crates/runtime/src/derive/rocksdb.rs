use anyhow::Context;
use futures::SinkExt;
use futures::{channel::mpsc, Stream, StreamExt, TryStreamExt};
use prost::Message;
use proto_flow::derive::{request, Request, Response};
use proto_flow::flow;
use proto_flow::runtime::{derive_response_ext, RocksDbDescriptor};
use proto_gazette::consumer::Checkpoint;
use std::sync::Arc;

pub fn adapt_requests<R>(
    peek_request: &Request,
    request_rx: R,
) -> anyhow::Result<(impl Stream<Item = anyhow::Result<Request>>, ResponseArgs)>
where
    R: Stream<Item = anyhow::Result<Request>>,
{
    // Open RocksDB based on the request::Open internal descriptor.
    let db = Arc::new(RocksDB::open(
        peek_request
            .get_internal()?
            .open
            .and_then(|o| o.rocksdb_descriptor),
    )?);
    let response_db = db.clone();

    // Channel for passing a StartCommit checkpoint to the response stream.
    let (mut start_commit_tx, start_commit_rx) = mpsc::channel(1);

    let request_rx = coroutines::try_coroutine(move |mut co| async move {
        let mut request_rx = std::pin::pin!(request_rx);

        while let Some(mut request) = request_rx.try_next().await? {
            if let Some(open) = &mut request.open {
                // If found, decode and attach to `open`.
                if let Some(state) = db.load_connector_state()? {
                    open.state_json = state.to_string();
                    tracing::debug!(state=%open.state_json, "loaded and attached a persisted connector Open.state_json");
                } else {
                    tracing::debug!("no previously-persisted connector state was found");
                }
            } else if let Some(start_commit) = &request.start_commit {
                // Notify response loop of a pending StartCommit checkpoint.
                start_commit_tx
                    .feed(start_commit.clone())
                    .await
                    .context("failed to send request::StartCommit to response stream")?;
            }

            co.yield_(request).await;
        }
        Ok(())
    });

    Ok((
        request_rx,
        ResponseArgs {
            start_commit_rx,
            db: response_db,
        },
    ))
}

pub struct ResponseArgs {
    start_commit_rx: mpsc::Receiver<request::StartCommit>,
    db: Arc<RocksDB>,
}

pub fn adapt_responses<R>(
    args: ResponseArgs,
    response_rx: R,
) -> impl Stream<Item = anyhow::Result<Response>>
where
    R: Stream<Item = anyhow::Result<Response>>,
{
    let ResponseArgs {
        mut start_commit_rx,
        db,
    } = args;

    coroutines::try_coroutine(move |mut co| async move {
        let mut response_rx = std::pin::pin!(response_rx);

        while let Some(mut response) = response_rx.try_next().await? {
            if let Some(_opened) = &response.opened {
                // Load and attach the last consumer checkpoint.
                let runtime_checkpoint = db
                    .load_checkpoint()
                    .context("failed to load runtime checkpoint from RocksDB")?;

                tracing::debug!(
                    ?runtime_checkpoint,
                    "loaded and attached a persisted OpenedExt.runtime_checkpoint",
                );

                response.set_internal(|internal| {
                    internal.opened = Some(derive_response_ext::Opened {
                        runtime_checkpoint: Some(runtime_checkpoint),
                    });
                });
            } else if let Some(started_commit) = &response.started_commit {
                let mut batch = rocksdb::WriteBatch::default();

                let start_commit = start_commit_rx
                    .next()
                    .await
                    .context("failed to receive request::StartCommit from request loop")?;

                let runtime_checkpoint = start_commit
                    .runtime_checkpoint
                    .context("StartCommit without runtime checkpoint")?;

                tracing::debug!(
                    ?runtime_checkpoint,
                    "persisting StartCommit.runtime_checkpoint",
                );
                batch.put(RocksDB::CHECKPOINT_KEY, &runtime_checkpoint.encode_to_vec());

                // And add the connector checkpoint.
                if let Some(flow::ConnectorState {
                    merge_patch,
                    updated_json,
                }) = &started_commit.state
                {
                    let mut updated: serde_json::Value = serde_json::from_str(updated_json)
                        .context("failed to decode connector state as JSON")?;

                    if *merge_patch {
                        if let Some(mut previous) = db.load_connector_state()? {
                            json_patch::merge(&mut previous, &updated);
                            updated = previous;
                        }
                    }

                    tracing::debug!(%updated, %merge_patch, "persisting an updated StartedCommit.state");
                    batch.put(RocksDB::CONNECTOR_STATE_KEY, &updated.to_string());
                }

                db.write(batch)
                    .context("failed to write atomic RocksDB commit")?;
            }
            co.yield_(response).await;
        }
        Ok(())
    })
}

struct RocksDB {
    db: rocksdb::DB,
    _path: std::path::PathBuf,
    _tmp: Option<tempfile::TempDir>,
}

impl std::ops::Deref for RocksDB {
    type Target = rocksdb::DB;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl RocksDB {
    pub fn open(desc: Option<RocksDbDescriptor>) -> anyhow::Result<Self> {
        let (mut opts, path, _tmp) = match desc {
            Some(RocksDbDescriptor {
                rocksdb_path,
                rocksdb_env_memptr,
            }) => {
                tracing::debug!(
                    ?rocksdb_path,
                    ?rocksdb_env_memptr,
                    "opening hooked RocksDB database"
                );

                // Re-hydrate the provided memory address into rocksdb::Env wrapping
                // an owned *mut librocksdb_sys::rocksdb_env_t.
                let env = unsafe {
                    rocksdb::Env::from_raw(rocksdb_env_memptr as *mut librocksdb_sys::rocksdb_env_t)
                };

                let mut opts = rocksdb::Options::default();
                opts.set_env(&env);

                (opts, std::path::PathBuf::from(rocksdb_path), None)
            }
            _ => {
                let dir = tempfile::TempDir::new().unwrap();
                let opts = rocksdb::Options::default();

                tracing::debug!(
                    rocksdb_path = ?dir.path(),
                    "opening temporary RocksDB database"
                );

                (opts, dir.path().to_owned(), Some(dir))
            }
        };

        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let column_families = match rocksdb::DB::list_cf(&opts, &path) {
            Ok(cf) => cf,
            // Listing column families will fail if the DB doesn't exist.
            // Assume as such, as we'll otherwise fail when we attempt to open.
            Err(_) => vec![rocksdb::DEFAULT_COLUMN_FAMILY_NAME.to_string()],
        };
        let mut db = rocksdb::DB::open_cf(&opts, &path, column_families.iter())
            .context("failed to open RocksDB")?;

        for column_family in column_families {
            // We used to use a `registers` column family for derivations, but we no longer do
            // and they were never actually used in production. Rocks requires that all existing
            // column families are opened, so we just open and drop any of these legacy "registers"
            // column families.
            if column_family == "registers" {
                tracing::warn!(%column_family, "dropping legacy rocksdb column family");
                db.drop_cf(&column_family)
                    .context("dropping legacy column family")?;
            }
        }

        Ok(Self {
            db,
            _path: path,
            _tmp,
        })
    }

    pub fn load_checkpoint(&self) -> anyhow::Result<Checkpoint> {
        match self.db.get_pinned(Self::CHECKPOINT_KEY)? {
            Some(v) => {
                Ok(Checkpoint::decode(v.as_ref())
                    .context("failed to decode consumer checkpoint")?)
            }
            None => Ok(Checkpoint::default()),
        }
    }

    pub fn load_connector_state(&self) -> anyhow::Result<Option<serde_json::Value>> {
        let state = self
            .db
            .get_pinned(Self::CONNECTOR_STATE_KEY)
            .context("failed to load connector state")?;

        // If found, decode and attach to `open`.
        if let Some(state) = state {
            let state: serde_json::Value =
                serde_json::from_slice(&state).context("failed to decode connector state")?;

            Ok(Some(state))
        } else {
            Ok(None)
        }
    }

    // Key encoding under which a marshalled checkpoint is stored.
    pub const CHECKPOINT_KEY: &[u8] = b"checkpoint";
    // Key encoding under which a connector state is stored.
    pub const CONNECTOR_STATE_KEY: &[u8] = b"connector-state";
}

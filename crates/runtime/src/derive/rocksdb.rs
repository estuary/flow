use super::anyhow_to_status;
use anyhow::Context;
use futures::{channel::mpsc, Stream, StreamExt};
use prost::Message;
use proto_flow::derive::{request, Request, Response};
use proto_flow::flow;
use proto_flow::runtime::{
    derive_response_ext, DeriveRequestExt, DeriveResponseExt, RocksDbDescriptor,
};
use proto_gazette::consumer::Checkpoint;
use std::sync::Arc;

pub fn adapt_requests<R>(
    peek_request: &Request,
    request_rx: R,
) -> anyhow::Result<(impl Stream<Item = tonic::Result<Request>>, Backward)>
where
    R: Stream<Item = tonic::Result<Request>> + Send + 'static,
{
    let db: Arc<RocksDB> = match peek_request {
        Request {
            open: Some(_open),
            internal: Some(internal),
            ..
        } => {
            let DeriveRequestExt { open, .. } = Message::decode(internal.value.clone())
                .context("internal is a DeriveRequestExt")?;

            RocksDB::open(open.and_then(|open| open.rocksdb_descriptor))?
        }
        _ => RocksDB::open(None)?,
    }
    .into();

    let (start_commit_tx, start_commit_rx) = mpsc::channel(1);

    let mut fwd = Forward {
        start_commit_tx,
        db: db.clone(),
    };
    let back = Backward {
        start_commit_rx,
        db,
    };

    Ok((request_rx.map(move |request| fwd.on_request(request)), back))
}

struct Forward {
    start_commit_tx: mpsc::Sender<request::StartCommit>,
    db: Arc<RocksDB>,
}
pub struct Backward {
    start_commit_rx: mpsc::Receiver<request::StartCommit>,
    db: Arc<RocksDB>,
}

impl Forward {
    fn on_request(&mut self, request: tonic::Result<Request>) -> tonic::Result<Request> {
        let request = request?;

        if let Request {
            open: Some(mut open),
            internal,
            ..
        } = request
        {
            // If found, decode and attach to `open`.
            if let Some(state) = self.db.load_connector_state().map_err(anyhow_to_status)? {
                open.state_json = state.to_string();
                tracing::debug!(state=%open.state_json, "loaded and attached a persisted connector Open.state_json");
            } else {
                tracing::debug!("no previously-persisted connector state was found");
            }

            return Ok(Request {
                open: Some(open),
                internal,
                ..Default::default()
            });
        }

        if let Request {
            start_commit: Some(start_commit),
            ..
        } = &request
        {
            // Notify backwards loop of a pending StartCommit checkpoint.
            let () = self
                .start_commit_tx
                .try_send(start_commit.clone())
                .context("saw second StartCommit request before prior StartedCommit has been read")
                .map_err(anyhow_to_status)?;
        }

        Ok(request)
    }
}

impl Backward {
    pub fn adapt_responses<R>(
        mut self,
        inner_response_rx: R,
    ) -> impl Stream<Item = tonic::Result<Response>>
    where
        R: Stream<Item = tonic::Result<Response>> + Send + 'static,
    {
        inner_response_rx.map(move |response| self.on_response(response))
    }

    fn on_response(&mut self, response: tonic::Result<Response>) -> tonic::Result<Response> {
        let mut response = response?;

        if let Response {
            opened: Some(_), ..
        } = &response
        {
            // Load and attach the last consumer checkpoint.
            let runtime_checkpoint = self
                .db
                .load_checkpoint()
                .context("failed to load runtime checkpoint from RocksDB")
                .map_err(anyhow_to_status)?;

            tracing::debug!(
                ?runtime_checkpoint,
                "loaded and attached a persisted OpenedExt.runtime_checkpoint",
            );

            response.internal = Some(::pbjson_types::Any {
                type_url: "flow://runtime.DeriveResponseExt".to_string(),
                value: DeriveResponseExt {
                    opened: Some(derive_response_ext::Opened {
                        runtime_checkpoint: Some(runtime_checkpoint),
                    }),
                    ..Default::default()
                }
                .encode_to_vec()
                .into(),
            });
        }

        if let Response {
            started_commit: Some(started_commit),
            ..
        } = &response
        {
            let mut batch = rocksdb::WriteBatch::default();

            let start_commit = self
                .start_commit_rx
                .try_next()
                .context("saw StartedCommit without a preceding StartCommit")
                .map_err(anyhow_to_status)?
                .ok_or_else(|| tonic::Status::cancelled("start_commit_rx dropped"))?;

            let runtime_checkpoint = start_commit
                .runtime_checkpoint
                .context("StartCommit without runtime checkpoint")
                .map_err(anyhow_to_status)?;

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
                    .context("failed to decode connector state as JSON")
                    .map_err(anyhow_to_status)?;

                if *merge_patch {
                    if let Some(mut previous) =
                        self.db.load_connector_state().map_err(anyhow_to_status)?
                    {
                        json_patch::merge(&mut previous, &updated);
                        updated = previous;
                    }
                }

                tracing::debug!(%updated, %merge_patch, "persisting an updated StartedCommit.state");
                batch.put(RocksDB::CONNECTOR_STATE_KEY, &updated.to_string());
            }

            self.db
                .write(batch)
                .context("failed to write atomic RocksDB commit")
                .map_err(anyhow_to_status)?;
        }

        Ok(response)
    }
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
                tracing::info!(
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

                tracing::info!(
                    rocksdb_path = ?dir.path(),
                    "opening temporary RocksDB database"
                );

                (opts, dir.path().to_owned(), Some(dir))
            }
        };

        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let column_families =
            rocksdb::DB::list_cf(&opts, &path).context("listing rocksdb column families")?;

        let mut db = rocksdb::DB::open_cf(&opts, &path, column_families.iter())?;
        for column_family in column_families {
            // We used to use a `registers` column family for derivations, but we no longer do
            // and they were never actually used in production. Rocks requires that all existing
            // column families are opened, so we just open and drop any of these legacy "registers"
            // column families.
            if column_family.as_str() == "registers" {
                tracing::warn!(%column_family, "dropping legacy rocksdb column family");
                db.drop_cf(column_family.as_str())
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

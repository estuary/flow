//! Drive one task's shards through their sessions, as the Go controller does
//! (`go/runtime/capture_v2.go`, `materialize_v2.go`, `derive_v2.go`). A
//! capture's session is of exactly one shard, which has no Leader.
//!
//! Each shard's `Shard` stream opens with a `SessionLoop`, and then carries a
//! sequence of sessions: `Join` and `Task` to every shard, then responses until
//! every shard reports `Stopped`. A session which stops on its own (for
//! example an idempotent-recovery session, or a shard shedding pinned shuffle
//! segments) is followed by the next, at the next `Join` revision. A requested
//! stop (duration, SIGINT, SIGTERM) sends `Stop` to every shard.
//!
//! Anything else — a stream error or EOF, a stale Join revision, or an
//! unexpected message — is a failure of the run, returned without retry.

use anyhow::Context;
use proto_flow::runtime as proto;

/// The per-task-type framing of the `Shard` protocol.
pub trait Envelope: std::fmt::Debug + Sized + Send + 'static {
    const RPC: &'static str;

    fn session_loop(session_loop: proto::SessionLoop) -> Self;
    fn join(join: proto::Join) -> Self;
    fn task(task: proto::Task) -> Self;
    fn stop() -> Self;
    fn classify(self) -> Response<Self>;

    fn open(
        client: &mut Client,
        requests: RequestStream<Self>,
    ) -> impl std::future::Future<Output = tonic::Result<tonic::Streaming<Self>>> + Send;
}

pub enum Response<E> {
    Joined { max_etcd_revision: i64 },
    Opened,
    Synced,
    Stopped,
    Other(E),
}

pub type Client = proto_grpc::runtime::shard_client::ShardClient<tonic::transport::Channel>;
pub type RequestStream<E> = tokio_stream::wrappers::UnboundedReceiverStream<E>;

impl Envelope for proto::Materialize {
    const RPC: &'static str = "Materialize";

    fn session_loop(session_loop: proto::SessionLoop) -> Self {
        Self {
            session_loop: Some(session_loop),
            ..Default::default()
        }
    }
    fn join(join: proto::Join) -> Self {
        Self {
            join: Some(join),
            ..Default::default()
        }
    }
    fn task(task: proto::Task) -> Self {
        Self {
            task: Some(task),
            ..Default::default()
        }
    }
    fn stop() -> Self {
        Self {
            stop: Some(proto::Stop {}),
            ..Default::default()
        }
    }
    fn classify(self) -> Response<Self> {
        if let Some(joined) = &self.joined {
            Response::Joined {
                max_etcd_revision: joined.max_etcd_revision,
            }
        } else if self.opened.is_some() {
            Response::Opened
        } else if self.synced.is_some() {
            Response::Synced
        } else if self.stopped.is_some() {
            Response::Stopped
        } else {
            Response::Other(self)
        }
    }
    async fn open(
        client: &mut Client,
        requests: RequestStream<Self>,
    ) -> tonic::Result<tonic::Streaming<Self>> {
        Ok(client.materialize(requests).await?.into_inner())
    }
}

impl Envelope for proto::Capture {
    const RPC: &'static str = "Capture";

    fn session_loop(session_loop: proto::SessionLoop) -> Self {
        Self {
            session_loop: Some(session_loop),
            ..Default::default()
        }
    }
    fn join(join: proto::Join) -> Self {
        Self {
            join: Some(join),
            ..Default::default()
        }
    }
    fn task(task: proto::Task) -> Self {
        Self {
            task: Some(task),
            ..Default::default()
        }
    }
    fn stop() -> Self {
        Self {
            stop: Some(proto::Stop {}),
            ..Default::default()
        }
    }
    fn classify(self) -> Response<Self> {
        if let Some(joined) = &self.joined {
            Response::Joined {
                max_etcd_revision: joined.max_etcd_revision,
            }
        } else if self.opened.is_some() {
            Response::Opened
        } else if self.stopped.is_some() {
            Response::Stopped
        } else {
            Response::Other(self)
        }
    }
    async fn open(
        client: &mut Client,
        requests: RequestStream<Self>,
    ) -> tonic::Result<tonic::Streaming<Self>> {
        Ok(client.capture(requests).await?.into_inner())
    }
}

impl Envelope for proto::Derive {
    const RPC: &'static str = "Derive";

    fn session_loop(session_loop: proto::SessionLoop) -> Self {
        Self {
            session_loop: Some(session_loop),
            ..Default::default()
        }
    }
    fn join(join: proto::Join) -> Self {
        Self {
            join: Some(join),
            ..Default::default()
        }
    }
    fn task(task: proto::Task) -> Self {
        Self {
            task: Some(task),
            ..Default::default()
        }
    }
    fn stop() -> Self {
        Self {
            stop: Some(proto::Stop {}),
            ..Default::default()
        }
    }
    fn classify(self) -> Response<Self> {
        if let Some(joined) = &self.joined {
            Response::Joined {
                max_etcd_revision: joined.max_etcd_revision,
            }
        } else if self.opened.is_some() {
            Response::Opened
        } else if self.stopped.is_some() {
            Response::Stopped
        } else {
            Response::Other(self)
        }
    }
    async fn open(
        client: &mut Client,
        requests: RequestStream<Self>,
    ) -> tonic::Result<tonic::Streaming<Self>> {
        Ok(client.derive(requests).await?.into_inner())
    }
}

/// Everything needed to drive one task's shards.
pub struct TaskRun {
    pub name: String,
    pub spec: bytes::Bytes,
    /// Symlink to shard zero's RocksDB directory. Shard zero removes the
    /// symlink as its stream ends, and not the directory (see README.md).
    pub rocksdb_link: std::path::PathBuf,
    pub join_shards: Vec<proto::join::Shard>,
    pub shards: Vec<ShardRun>,
}

pub struct ShardRun {
    pub label: String,
    pub socket: std::path::PathBuf,
    pub shuffle_dir: String,
    pub shuffle_endpoint: String,
}

pub async fn drive<E: Envelope>(
    task: TaskRun,
    stop: tokio_util::sync::CancellationToken,
    events: crate::layout::Events,
) -> anyhow::Result<()> {
    let TaskRun {
        name,
        spec,
        rocksdb_link,
        join_shards,
        shards,
    } = task;
    let leader_endpoint = shards[0].shuffle_endpoint.clone();

    // Open every shard's stream, and a reader of each which funnels its
    // responses (and its end) into `response_rx` for as long as it lives.
    let (response_tx, mut response_rx) =
        tokio::sync::mpsc::unbounded_channel::<(usize, tonic::Result<Option<E>>)>();
    let mut request_txs = Vec::new();

    for (index, shard) in shards.iter().enumerate() {
        let mut client = connect_uds(&shard.socket).await?;
        let (request_tx, request_rx) = tokio::sync::mpsc::unbounded_channel::<E>();

        let rocksdb_descriptor = (index == 0).then(|| proto::RocksDbDescriptor {
            rocksdb_env_memptr: 0,
            rocksdb_path: rocksdb_link.to_string_lossy().into_owned(),
        });
        request_tx
            .send(E::session_loop(proto::SessionLoop {
                rocksdb_descriptor,
                initial_connector_state_json: bytes::Bytes::new(),
                report_final_state: false,
            }))
            .unwrap();

        let mut responses = E::open(&mut client, RequestStream::new(request_rx))
            .await
            .with_context(|| format!("opening {} stream of {}", E::RPC, shard.label))?;
        request_txs.push(request_tx);

        let response_tx = response_tx.clone();
        tokio::spawn(async move {
            loop {
                let next = responses.message().await;
                let done = !matches!(next, Ok(Some(_)));
                if response_tx.send((index, next)).is_err() || done {
                    return;
                }
            }
        });
    }
    std::mem::drop(response_tx);

    let broadcast = |msg: fn() -> E| {
        for tx in &request_txs {
            let _ = tx.send(msg());
        }
    };

    let mut revision: i64 = 1;
    let mut stopping = false;

    'sessions: while !stopping {
        for (index, (shard, tx)) in shards.iter().zip(&request_txs).enumerate() {
            let _ = tx.send(E::join(proto::Join {
                etcd_mod_revision: revision,
                shards: join_shards.clone(),
                shard_index: index as u32,
                shuffle_directory: shard.shuffle_dir.clone(),
                shuffle_endpoint: shard.shuffle_endpoint.clone(),
                leader_endpoint: leader_endpoint.clone(),
            }));
            let _ = tx.send(E::task(proto::Task {
                spec: spec.clone(),
                max_transactions: 0, // Unbounded, as in production.
                sqlite_vfs_uri: String::new(),
                publisher_id: Default::default(), // Shard zero fills it in.
            }));
        }
        events.record(
            "sessionStarted",
            serde_json::json!({"task": name, "revision": revision}),
        );

        let (mut opened, mut stopped) = (vec![false; shards.len()], vec![false; shards.len()]);
        loop {
            let (index, response) = tokio::select! {
                () = stop.cancelled(), if !stopping => {
                    stopping = true;
                    events.record("stopRequested", serde_json::json!({"task": name, "revision": revision}));
                    broadcast(E::stop);
                    continue;
                }
                next = response_rx.recv() => next.expect("readers outlive their streams"),
            };
            let label = &shards[index].label;

            let response = match response {
                Ok(Some(response)) => response,
                Ok(None) => anyhow::bail!("{label} {} stream ended unexpectedly", E::RPC),
                Err(status) => {
                    return Err(runtime_next::status_to_anyhow(status))
                        .with_context(|| format!("{label} {} stream failed", E::RPC));
                }
            };

            match response.classify() {
                Response::Joined { max_etcd_revision } if max_etcd_revision != 0 => {
                    anyhow::bail!(
                        "{label} Join revision {revision} is stale (the Leader holds {max_etcd_revision})"
                    );
                }
                Response::Joined { .. } | Response::Synced => {}
                Response::Opened => {
                    opened[index] = true;
                    if opened.iter().all(|o| *o) {
                        events.record(
                            "sessionOpened",
                            serde_json::json!({"task": name, "revision": revision}),
                        );
                    }
                }
                Response::Stopped => {
                    stopped[index] = true;
                    if stopped.iter().all(|s| *s) {
                        events.record(
                            "sessionStopped",
                            serde_json::json!({"task": name, "revision": revision, "requested": stopping}),
                        );
                        revision += 1;
                        continue 'sessions;
                    }
                }
                Response::Other(other) => {
                    anyhow::bail!("{label} sent unexpected {} response {other:?}", E::RPC);
                }
            }
        }
    }

    // A dropped request stream is the controller's graceful teardown: read
    // every stream through to its end, surfacing its last word.
    std::mem::drop(request_txs);
    while let Some((index, response)) = response_rx.recv().await {
        if let Err(status) = response {
            return Err(runtime_next::status_to_anyhow(status))
                .with_context(|| format!("{} teardown failed", shards[index].label));
        }
    }
    Ok(())
}

async fn connect_uds(socket: &std::path::Path) -> anyhow::Result<Client> {
    let socket = socket.to_path_buf();
    // The URI is required, but unused: every connection dials `socket`.
    let channel = tonic::transport::Endpoint::from_static("http://[::]:0")
        .connect_with_connector(tower::service_fn(move |_: tonic::transport::Uri| {
            let socket = socket.clone();
            async move {
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                    tokio::net::UnixStream::connect(socket).await?,
                ))
            }
        }))
        .await
        .context("dialing shard socket")?;

    Ok(Client::new(channel)
        .max_decoding_message_size(proto_grpc::MAX_MESSAGE_SIZE)
        .max_encoding_message_size(usize::MAX))
}

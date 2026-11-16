mod support;

use proto_flow::flow;
use proto_gazette::{broker, uuid};
use runtime_local::segments;
use runtime_next::proto;
use tokio::sync::mpsc;

/// Transactions of each session of a serve loop.
const SESSIONS: &[u32] = &[2, 2];

/// Events of each transaction of a derivation's or materialization's source.
const EVENTS: u64 = 3;

#[tokio::test]
async fn disk_runtime() {
    // The runtime's loopback services dial over rustls.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let fixture = support::Fixture::start().await;
    let daemon = support::Daemon::start(&fixture, "runtime").await;
    let runtime = Runtime::new(&daemon);
    let (capture, derivation, materialization) = build_catalog(&runtime).await;

    keeps_its_disk::<proto::Materialize>(
        &fixture,
        &runtime,
        shuffle::proto::task::Task::Materialization(materialization),
    )
    .await;
    keeps_its_disk::<proto::Derive>(
        &fixture,
        &runtime,
        shuffle::proto::task::Task::Derivation(proto_flow::Arc::new(derivation)),
    )
    .await;
    a_capture_keeps_its_disk(&fixture, &runtime, &capture).await;

    daemon.drain().await;
    fixture.stop().await;
}

type Shard = runtime_next::shard::Service<
    runtime_next::RecordingPublisherFactory,
    runtime_next::TracingLoggerFactory,
>;

/// What every shard of the test shares.
struct Runtime {
    registry: service_kit::Registry,
    router: std::sync::Arc<dyn proto_grpc::connector::Router>,
    uds_path: std::path::PathBuf,
}

impl Runtime {
    fn new(daemon: &support::Daemon) -> Self {
        Self {
            registry: service_kit::Registry::new(),
            router: runtime_local::local_test_router(),
            uds_path: daemon.uds_path.clone(),
        }
    }

    fn shard(&self, task_name: &str) -> Shard {
        runtime_next::shard::Service::new(
            self.router.clone(),
            None,
            task_name.to_string(),
            runtime_next::RecordingPublisherFactory,
            runtime_next::TracingLoggerFactory,
            self.registry.clone(),
            None,
            runtime_next::shard::Disks::Persistent {
                uds_path: Some(self.uds_path.clone()),
            },
        )
    }
}

/// Build the capture, derivation, and materialization of `fixtures/flow.yaml`,
/// each committing one checkpoint per transaction.
async fn build_catalog(
    runtime: &Runtime,
) -> (
    flow::CaptureSpec,
    flow::CollectionSpec,
    flow::MaterializationSpec,
) {
    let source = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/flow.yaml");
    let url = build::arg_source_to_url(source, false).unwrap();
    let draft = build::load(&url, std::path::Path::new("/")).await;

    let built = build::local(
        models::Id::new([32; 8]),
        models::Id::new([1; 8]),
        runtime.router.clone(),
        ops::tracing_log_handler,
        true,  // No-op captures.
        false, // Validate derivations, or a no-op gives them no transforms.
        false, // Validate materializations, or a no-op selects no root document.
        draft,
        build::no_op_live_catalog(),
    )
    .await
    .into_result()
    .unwrap_or_else(|errors| panic!("building {source}: {errors:?}"))
    .built;

    let mut capture = built.built_captures[0].spec.clone().unwrap();
    let mut derivation = built
        .built_collections
        .iter()
        .find_map(|row| row.spec.as_deref().filter(|spec| spec.derivation.is_some()))
        .unwrap()
        .clone();
    let mut materialization = built.built_materializations[0].spec.clone().unwrap();

    let derive_template = &mut derivation.derivation.as_mut().unwrap().shard_template;
    runtime_local::force_single_transaction(derive_template.as_mut());
    runtime_local::force_single_transaction(capture.shard_template.as_mut());
    runtime_local::force_single_transaction(materialization.shard_template.as_mut());

    (capture, derivation, materialization)
}

/// A capture's second serve loop mounts its disk fresh from the journal, which its
/// connector checks against what committed. A third serve loop, whose RocksDB lost
/// the disk's intent, refuses the disk rather than formatting it.
async fn a_capture_keeps_its_disk(
    fixture: &support::Fixture,
    runtime: &Runtime,
    spec: &flow::CaptureSpec,
) {
    let shards = runtime_local::shards::build_capture_join_shards(1, spec).unwrap();
    let template = spec.disk_journal_template.as_ref().unwrap();
    () = create_disk_journal(fixture, template, &shards[0]).await;
    let spec_bytes = prost::Message::encode_to_vec(spec).into();
    let rocksdb = tempfile::tempdir().unwrap();

    for _ in 0..2 {
        serve_shard::<proto::Capture>(
            runtime.shard(&spec.name),
            session_loop(rocksdb.path()),
            &spec_bytes,
            &shards,
            SESSIONS
                .iter()
                .map(|&n| (proto::Join::default(), n))
                .collect(),
        )
        .await
        .unwrap();
    }

    let lost = tempfile::tempdir().unwrap();
    let err = serve_shard::<proto::Capture>(
        runtime.shard(&spec.name),
        session_loop(lost.path()),
        &spec_bytes,
        &shards,
        vec![(proto::Join::default(), 1)],
    )
    .await
    .unwrap_err();
    assert!(
        format!("{err:#}").contains("no ACK intent was committed"),
        "{err:#}"
    );

    () = fixture.wait_for_teardown().await;
}

/// Run two serve loops of a derivation or materialization `task`. The second
/// mounts its disk fresh from the journal, which its connector checks against what
/// committed.
async fn keeps_its_disk<E: Envelope>(
    fixture: &support::Fixture,
    runtime: &Runtime,
    task: shuffle::proto::task::Task,
) {
    let (name, spec_bytes, template, shards) = match &task {
        shuffle::proto::task::Task::Materialization(spec) => (
            spec.name.clone(),
            prost::Message::encode_to_vec(spec),
            spec.disk_journal_template.clone(),
            runtime_local::shards::build_materialize_join_shards(1, spec),
        ),
        shuffle::proto::task::Task::Derivation(spec) => (
            spec.name.clone(),
            prost::Message::encode_to_vec(spec.as_ref()),
            spec.derivation
                .as_ref()
                .unwrap()
                .disk_journal_template
                .clone(),
            runtime_local::shards::build_derive_join_shards(1, spec),
        ),
        shuffle::proto::task::Task::CollectionPartitions(_) => unreachable!(),
    };
    let (spec_bytes, shards) = (bytes::Bytes::from(spec_bytes), shards.unwrap());
    () = create_disk_journal(fixture, &template.unwrap(), &shards[0]).await;
    let rocksdb = tempfile::tempdir().unwrap();
    let mut feed = Feed::new(&shuffle::proto::Task { task: Some(task) });

    for _ in 0..2 {
        let (opener, frontier_tx) = segments::fixture_opener();
        let run = runtime_local::services::Run::start_with_shuffle_leader(
            runtime.router.clone(),
            1,
            None,
            runtime.registry.clone(),
            runtime_next::RecordingPublisherFactory,
            runtime_next::TracingLoggerFactory,
            |_peer_endpoint| Ok((opener, None)),
        )
        .await
        .unwrap();

        let (directories, _segments) = feed.write(&run.shuffle_log_dir, &frontier_tx);
        let sessions = SESSIONS
            .iter()
            .zip(directories)
            .map(|(&n, shuffle_directory)| {
                let join = proto::Join {
                    shuffle_directory,
                    shuffle_endpoint: run.peer_endpoint.clone(),
                    leader_endpoint: run.peer_endpoint.clone(),
                    ..Default::default()
                };
                (join, n)
            });

        serve_shard::<E>(
            runtime.shard(&name),
            session_loop(rocksdb.path()),
            &spec_bytes,
            &shards,
            sessions.collect(),
        )
        .await
        .unwrap();
    }
    () = fixture.wait_for_teardown().await;
}

async fn create_disk_journal(
    fixture: &support::Fixture,
    template: &broker::JournalSpec,
    shard: &proto::join::Shard,
) {
    let range = shard.labeling.as_ref().unwrap().range.as_ref().unwrap();

    fixture
        .create_journal(broker::JournalSpec {
            name: labels::shard::disk_journal_name(&template.name, range),
            ..template.clone()
        })
        .await;
}

/// Build the `SessionLoop` of a serve loop whose RocksDB lives in `dir`. A shard
/// removes its RocksDB directory as its serve loop ends, so it's opened through a
/// symlink, whose removal leaves its target.
fn session_loop(dir: &std::path::Path) -> proto::SessionLoop {
    let db = dir.join("db");
    let link = dir.join("link");
    () = std::fs::create_dir_all(&db).unwrap();
    () = std::os::unix::fs::symlink(&db, &link).unwrap();

    proto::SessionLoop {
        rocksdb_descriptor: Some(proto::RocksDbDescriptor {
            rocksdb_path: link.to_str().unwrap().to_string(),
            rocksdb_env_memptr: 0,
        }),
        ..Default::default()
    }
}

/// Drive one shard through `sessions`, each a `Join` and its count of
/// transactions. Each `Join` gains `shards` and its session's revision.
async fn serve_shard<E: Envelope>(
    shard: Shard,
    session_loop: proto::SessionLoop,
    spec: &bytes::Bytes,
    shards: &[proto::join::Shard],
    sessions: Vec<(proto::Join, u32)>,
) -> anyhow::Result<()> {
    let (request_tx, request_rx) = mpsc::unbounded_channel();
    let mut response_rx = E::spawn(
        &shard,
        tokio_stream::wrappers::UnboundedReceiverStream::new(request_rx),
    );
    let send = |message: E| {
        request_tx
            .send(Ok(message))
            .map_err(|_| anyhow::anyhow!("the shard stopped reading requests"))
    };

    () = send(E::session_loop(session_loop))?;

    for (index, (join, transactions)) in sessions.into_iter().enumerate() {
        () = send(E::join(proto::Join {
            etcd_mod_revision: index as i64 + 1,
            shards: shards.to_vec(),
            ..join
        }))?;
        () = send(E::task(proto::Task {
            spec: spec.clone(),
            max_transactions: transactions,
            ..Default::default()
        }))?;

        loop {
            let response = match response_rx.recv().await {
                Some(response) => response.map_err(runtime_next::status_to_anyhow)?,
                None => anyhow::bail!("the shard ended its responses before Stopped"),
            };
            if response.stopped() {
                break;
            }
        }
    }

    // EOF ends the serve loop, which tears its disk down.
    std::mem::drop(request_tx);
    while let Some(response) = response_rx.recv().await {
        _ = response.map_err(runtime_next::status_to_anyhow)?;
    }
    Ok(())
}

/// The message of one task type's `Shard` stream.
trait Envelope: Default + Send + 'static {
    fn spawn(
        shard: &Shard,
        requests: tokio_stream::wrappers::UnboundedReceiverStream<tonic::Result<Self>>,
    ) -> mpsc::UnboundedReceiver<tonic::Result<Self>>;
    fn session_loop(session_loop: proto::SessionLoop) -> Self;
    fn join(join: proto::Join) -> Self;
    fn task(task: proto::Task) -> Self;
    fn stopped(&self) -> bool;
}

macro_rules! envelope {
    ($message:ty, $spawn:ident) => {
        impl Envelope for $message {
            fn spawn(
                shard: &Shard,
                requests: tokio_stream::wrappers::UnboundedReceiverStream<tonic::Result<Self>>,
            ) -> mpsc::UnboundedReceiver<tonic::Result<Self>> {
                shard.$spawn(requests)
            }
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
            fn stopped(&self) -> bool {
                self.stopped.is_some()
            }
        }
    };
}
envelope!(proto::Capture, spawn_capture);
envelope!(proto::Derive, spawn_derive);
envelope!(proto::Materialize, spawn_materialize);

/// Source events of a derivation or materialization, written as fixture segments.
/// Its clock and journal offsets advance across serve loops, so each serve loop's
/// events lie past the frontier it recovers.
struct Feed {
    bindings: Vec<shuffle::Binding>,
    sources: Vec<shuffle::Source>,
    validators: Vec<doc::Validator>,
    clock: uuid::Clock,
    journal_offsets: std::collections::HashMap<(String, u16), i64>,
}

impl Feed {
    fn new(task: &shuffle::proto::Task) -> Self {
        let (bindings, sources, validators, _) = segments::task_bindings(task).unwrap();

        Self {
            bindings,
            sources,
            validators,
            clock: uuid::Clock::from_time(std::time::SystemTime::now()),
            journal_offsets: Default::default(),
        }
    }

    /// Write each of `SESSIONS` into a directory of its own under `dir`, and queue
    /// its frontiers on `frontier_tx`, then a boundary. Returns each session's
    /// directory, and its segments, which must outlive the session.
    fn write(
        &mut self,
        dir: &str,
        frontier_tx: &mpsc::UnboundedSender<segments::FixtureItem>,
    ) -> (
        Vec<String>,
        Vec<(
            Vec<segments::ShardWriter>,
            Vec<shuffle::log::writer::SealedSegment>,
        )>,
    ) {
        let shards = segments::full_range_shards(1);
        let mut directories = Vec::new();
        let mut kept = Vec::new();

        for (index, &transactions) in SESSIONS.iter().enumerate() {
            let directory = std::path::Path::new(dir).join(format!("session-{index}"));
            () = std::fs::create_dir(&directory).unwrap();
            let mut writers = segments::open_shard_writers(&directory, 1).unwrap();
            let mut sealed = Vec::new();

            for _ in 0..transactions {
                let mut events = Vec::new();
                for _ in 0..EVENTS {
                    let event = serde_json::json!({"key": "a"});
                    events.push((0, self.clock.tick(), event));
                }
                let items: Vec<_> = events
                    .iter()
                    .map(|(binding, clock, event)| (*binding, *clock, event))
                    .collect();

                let frontier = segments::write_transaction_for_bindings(
                    &items,
                    &self.bindings,
                    &self.sources,
                    &mut self.validators,
                    &shards,
                    &mut writers,
                    &mut sealed,
                    &mut self.journal_offsets,
                    &mut bytes::BytesMut::new(),
                )
                .unwrap();
                () = frontier_tx
                    .send(segments::FixtureItem::Frontier(frontier))
                    .unwrap();
            }
            () = frontier_tx
                .send(segments::FixtureItem::Boundary { reached: None })
                .unwrap();

            directories.push(directory.to_str().unwrap().to_string());
            kept.push((writers, sealed));
        }
        (directories, kept)
    }
}

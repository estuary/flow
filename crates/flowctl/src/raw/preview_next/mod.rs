//! `flowctl preview` on top of the runtime-next + shuffle stack.
//!
//! Spawns one in-process tonic server hosting both `runtime_next::Service`
//! and `shuffle::Service` on a single ephemeral 127.0.0.1 port, then runs
//! N synthetic shards as tokio tasks each driving one long-lived SessionLoop.
//! Source documents come from real Gazette journal reads (authed via the
//! user's flowctl token); endpoint mutations go to the connector container
//! as in production.
use crate::local_specs;
use anyhow::Context;
use runtime_next::shard::rocksdb;

mod capture_driver;
mod derive_driver;
mod driver;
mod fixture;
mod logger;
mod publish;
mod services;
mod shards;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Preview {
    /// Path or URL to a Flow specification file.
    #[clap(long)]
    source: String,
    /// Name of the task to preview within the Flow specification file.
    /// Required if there are multiple tasks in --source specifications.
    #[clap(long)]
    name: Option<String>,
    /// Number of synthetic shards to drive in parallel. Default 1.
    /// For captures, N shards fan out as N independent connector instances.
    /// Many connectors ignore the key range and will duplicate output;
    /// use N=1 unless the connector partitions by key_begin/key_end.
    #[clap(long, default_value = "1")]
    shards: u32,
    /// How long should preview run before gracefully stopping?
    /// The default is no timeout. Note that the task may finish active
    /// transaction activity even after this timeout is reached.
    #[clap(long)]
    timeout: Option<humantime::Duration>,
    /// How many connector sessions should be run, and what is the target number
    /// of transactions for each session?
    ///
    /// Sessions are specified as a comma-separated list of the number of
    /// transactions for the ordered session. For a given session, a value less
    /// than zero means "unlimited transactions", though the session will still
    /// end upon a connector exit / EOF (when a capture) or timeout.
    ///
    /// For example, to run three sessions consisting of two transactions,
    /// then one transaction, and then unlimited transactions,
    /// use argument `--sessions 2,1,-1`.
    ///
    /// A session is stopped and the next started upon reaching the target number
    /// of transactions, or upon a timeout, or if the connector exits.
    ///
    /// The default is a single session with an unbounded number of transactions.
    #[clap(long, value_parser, value_delimiter = ',')]
    sessions: Option<Vec<isize>>,
    /// Path to a transactions fixture to feed in place of live collection data.
    /// Newline-delimited JSON: documents `["collection/name", {...}]` separated
    /// by `{"commit": true}` transaction markers. Fixtures are only for
    /// derivations and materializations, and require `--shards 1`.
    ///
    /// A regular file is read eagerly and may be split across `--sessions`.
    /// A named pipe (FIFO), or `-` for stdin, streams: transactions are fed
    /// incrementally as the producing writer emits them, in a single unbounded
    /// session which stops gracefully at stream EOF.
    #[clap(long)]
    fixture: Option<String>,
    /// Artificial delay between transactions, simulating back-pressure and
    /// encouraging reductions. The delay raises the task's minimum transaction
    /// duration, so each transaction batches at least `delay` of live input.
    /// Cannot be combined with `--fixture` (whose transaction boundaries are
    /// fixed by the fixture's own commit markers).
    #[clap(long)]
    delay: Option<humantime::Duration>,
    /// Docker network to run connector images.
    #[clap(long, default_value = "bridge")]
    network: String,
    /// Initial JSON connector state to seed the run with.
    /// When developing a connector, you may want to use --initial-state to pass
    /// in crafted state configurations you expect the connector to resume from.
    /// It seeds only the very first session of a run: once any connector state
    /// is persisted, later sessions resume from it instead.
    #[clap(long)]
    initial_state: Option<String>,
    /// Output state updates.
    /// Each committed connector state update is printed to stdout as a
    /// `["connectorState",<update>]` line. Requires `--shards 1`.
    #[clap(long, action)]
    output_state: bool,
    /// Output apply RPC description.
    /// Each connector Applied response is printed to stdout as a
    /// `["applied.actionDescription","<text>"]` line. Requires `--shards 1`.
    #[clap(long, action)]
    output_apply: bool,
    /// Output task logs in JSON format to stderr.
    #[clap(long, action)]
    log_json: bool,
    /// Loopback HTTP port hosting the service-kit admin dashboard
    /// (handler inventory, per-handler trace overrides, /metrics).
    #[clap(long)]
    debug_port: Option<u16>,
}

/// Harness controls threaded into each driver: the `--initial-state` seed (used
/// to pre-seed shard zero's RocksDB), the output-capturing publisher factory,
/// and the preview-rendering logger factory installed on the shard (and, for
/// materializations / derivations, the leader) Service. The logger carries the
/// `--output-state` / `--output-apply` behavior the legacy `flowctl preview`
/// flags expressed; the publisher captures captured / derived documents.
#[derive(Clone)]
struct Controls {
    initial_state_json: bytes::Bytes,
    publisher_factory: publish::PreviewPublisherFactory,
    logger_factory: logger::PreviewLoggerFactory,
}

/// Resolved task selected from the source specifications.
enum TaskSpec {
    Capture(proto_flow::flow::CaptureSpec),
    Materialization(proto_flow::flow::MaterializationSpec),
    Derivation(proto_flow::flow::CollectionSpec),
}

impl Preview {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        let Self {
            source,
            name,
            shards,
            timeout,
            sessions,
            fixture,
            delay,
            network,
            initial_state,
            output_state,
            output_apply,
            log_json,
            debug_port,
        } = self;

        let fixture = fixture.as_deref();
        let delay: Option<std::time::Duration> = delay.map(|d| d.into());

        if fixture.is_some() && *shards != 1 {
            anyhow::bail!("--fixture requires --shards 1");
        }
        if (*output_state || *output_apply) && *shards != 1 {
            anyhow::bail!("--output-state and --output-apply require --shards 1");
        }
        if fixture.is_some() && delay.is_some() {
            anyhow::bail!("--delay cannot be combined with --fixture");
        }

        let source_url = build::arg_source_to_url(source, false)?;

        let log_handler: fn(&::ops::Log) = if *log_json {
            ::ops::stderr_log_handler
        } else {
            ::ops::tracing_log_handler
        };

        // An explicit --initial-state value seeds shard zero's first session.
        let initial_state_json = match initial_state {
            None => bytes::Bytes::new(),
            Some(initial_state) => {
                let initial_state = models::RawValue::from_str(initial_state)
                    .context("initial state is not valid JSON")?;
                bytes::Bytes::from(initial_state.get().to_string())
            }
        };
        let controls = Controls {
            initial_state_json,
            publisher_factory: publish::PreviewPublisherFactory,
            logger_factory: logger::PreviewLoggerFactory::new(
                log_handler,
                *output_state,
                *output_apply,
            ),
        };

        let (_sources, _live, validations) =
            local_specs::load_and_validate_full(ctx, source_url.as_str(), network, log_handler)
                .await?;

        let task = resolve_task(&validations, name.as_deref())?;

        let timeout = timeout.map(|i| i.into());

        let session_targets: Vec<u32> = if let Some(s) = sessions {
            s.iter()
                .map(|i| {
                    if *i < 0 {
                        Ok(0)
                    } else {
                        u32::try_from(*i).context("--sessions values must fit in uint32")
                    }
                })
                .collect::<anyhow::Result<_>>()?
        } else {
            vec![0]
        };

        let stop_token = tokio_util::sync::CancellationToken::new();

        let result: anyhow::Result<()> = match task {
            TaskSpec::Capture(mut spec) => {
                anyhow::ensure!(
                    fixture.is_none(),
                    "--fixture is only supported for derivations and materializations",
                );
                if let Some(delay) = delay {
                    set_min_txn_duration(spec.shard_template.as_mut(), delay);
                }
                let run = services::Run::start_capture(
                    network.clone(),
                    *shards,
                    *debug_port,
                    ctx.registry.clone(),
                )
                .await?;
                let session_loop = capture_driver::run_sessions(
                    &run,
                    &spec,
                    session_targets,
                    controls.clone(),
                    stop_token.clone(),
                );
                tokio::pin!(session_loop);
                let result = run_with_timeout(session_loop, timeout, &stop_token).await;
                finish_output_state(&run, *output_state, result).await
            }
            TaskSpec::Materialization(mut spec) => {
                let run = services::Run::start_with_shuffle_leader(
                    ctx,
                    network.clone(),
                    *shards,
                    *debug_port,
                    ctx.registry.clone(),
                    fixture.is_some(),
                    controls.publisher_factory.clone(),
                    controls.logger_factory.clone(),
                )
                .await?;

                // Hold the fixture keepalive (writers/segments) for the life of
                // the session loop so its segment files stay readable.
                let (session_targets, fixture_dirs, session_stop, fixture_keepalive) =
                    prepare_sessions(
                        &run,
                        &mut spec,
                        |spec| spec.shard_template.as_mut(),
                        |spec| shuffle::proto::Task {
                            task: Some(shuffle::proto::task::Task::Materialization(spec.clone())),
                        },
                        fixture,
                        delay,
                        session_targets,
                        &stop_token,
                    )?;

                let session_loop = driver::run_sessions(
                    &run,
                    &spec,
                    session_targets,
                    fixture_dirs,
                    controls.clone(),
                    session_stop,
                );
                tokio::pin!(session_loop);
                let result = run_with_timeout(session_loop, timeout, &stop_token).await;
                let result = finish_fixtures(result, fixture_keepalive).await;
                finish_output_state(&run, *output_state, result).await
            }
            TaskSpec::Derivation(mut spec) => {
                let run = services::Run::start_with_shuffle_leader(
                    ctx,
                    network.clone(),
                    *shards,
                    *debug_port,
                    ctx.registry.clone(),
                    fixture.is_some(),
                    controls.publisher_factory.clone(),
                    controls.logger_factory.clone(),
                )
                .await?;

                let (session_targets, fixture_dirs, session_stop, fixture_keepalive) =
                    prepare_sessions(
                        &run,
                        &mut spec,
                        |spec| {
                            spec.derivation
                                .as_mut()
                                .and_then(|d| d.shard_template.as_mut())
                        },
                        |spec| shuffle::proto::Task {
                            task: Some(shuffle::proto::task::Task::Derivation(spec.clone())),
                        },
                        fixture,
                        delay,
                        session_targets,
                        &stop_token,
                    )?;

                let session_loop = derive_driver::run_sessions(
                    &run,
                    &spec,
                    session_targets,
                    fixture_dirs,
                    controls.clone(),
                    session_stop,
                );
                tokio::pin!(session_loop);
                let result = run_with_timeout(session_loop, timeout, &stop_token).await;
                let result = finish_fixtures(result, fixture_keepalive).await;
                finish_output_state(&run, *output_state, result).await
            }
        };

        // `run` drops here, aborting the tonic server and removing the
        // RocksDB / shuffle-log tempdirs.
        result
    }
}

/// Fixture state held for the life of the session loop. Both variants keep
/// shuffle-log writers/segments alive while the consumer reads (and unlinks)
/// them; `Streaming` also carries the feeder task, joined after the run to
/// surface stream errors.
enum FixtureKeepalive {
    Eager {
        _plan: fixture::FixturePlan,
    },
    Streaming {
        // Dropping the guard cancels the feeder's hold token: it releases its
        // writer/segments and exits with the stream's result.
        _hold: tokio_util::sync::DropGuard,
        feeder: tokio::task::JoinHandle<anyhow::Result<()>>,
    },
}

/// Prepare per-session inputs for a derivation or materialization preview.
///
/// With `--fixture`: force 1:1 fixture-transaction-to-runtime-transaction
/// boundaries, then either materialize a regular file into per-session shuffle
/// segments and start the frontier feeder (eager), or — for a FIFO or stdin —
/// stream it through a feeder task driving a single unbounded session that's
/// stopped at EOF via the returned session stop token (a child of `stop_token`,
/// so Ctrl-C and `--timeout` behave identically). Without a fixture: apply
/// `--delay` (if any) as the task's minimum transaction duration, batching live
/// reads into fewer, larger transactions.
fn prepare_sessions<S>(
    run: &services::Run,
    spec: &mut S,
    shard_template: impl FnOnce(&mut S) -> Option<&mut proto_gazette::consumer::ShardSpec>,
    build_task: impl FnOnce(&S) -> shuffle::proto::Task,
    fixture: Option<&str>,
    delay: Option<std::time::Duration>,
    session_targets: Vec<u32>,
    stop_token: &tokio_util::sync::CancellationToken,
) -> anyhow::Result<(
    Vec<u32>,
    Vec<String>,
    tokio_util::sync::CancellationToken,
    Option<FixtureKeepalive>,
)> {
    let Some(path) = fixture else {
        if let Some(delay) = delay {
            set_min_txn_duration(shard_template(spec), delay);
        }
        return Ok((session_targets, Vec::new(), stop_token.clone(), None));
    };

    force_single_transaction(shard_template(spec));
    let task = build_task(spec);

    if is_streaming_fixture(path)? {
        anyhow::ensure!(
            session_targets == [0],
            "a streaming --fixture (FIFO or stdin) runs exactly one unbounded session; omit --sessions or pass `--sessions -1`",
        );
        let frontier_tx = run
            .frontier_tx
            .clone()
            .expect("fixture run was started with a frontier sender");

        let session_stop = stop_token.child_token();
        let hold = tokio_util::sync::CancellationToken::new();
        let source = (path != "-").then(|| std::path::PathBuf::from(path));

        let (dir, feeder) = fixture::start_streaming(
            &task,
            source,
            std::path::Path::new(&run.shuffle_log_dir),
            frontier_tx,
            session_stop.clone(),
            hold.clone(),
        )?;
        return Ok((
            vec![0],
            vec![dir],
            session_stop,
            Some(FixtureKeepalive::Streaming {
                _hold: hold.drop_guard(),
                feeder,
            }),
        ));
    }

    let (targets, dirs, plan) = start_fixtures(run, task, path, session_targets)?;
    Ok((
        targets,
        dirs,
        stop_token.clone(),
        Some(FixtureKeepalive::Eager { _plan: plan }),
    ))
}

/// A streaming fixture is stdin (`-`) or a named pipe: its transactions are fed
/// incrementally as they're produced, rather than eagerly pre-planned.
fn is_streaming_fixture(path: &str) -> anyhow::Result<bool> {
    if path == "-" {
        return Ok(true);
    }
    let meta =
        std::fs::metadata(path).with_context(|| format!("inspecting fixture path {path:?}"))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::FileTypeExt;
        Ok(meta.file_type().is_fifo())
    }
    #[cfg(not(unix))]
    {
        let _ = meta;
        Ok(false)
    }
}

/// Conclude fixture feeding once the session loop has ended: release the
/// streaming feeder's segment keepalive and join it, surfacing a stream error
/// (e.g. a malformed fixture line) that otherwise manifests only as a
/// gracefully-stopped run.
async fn finish_fixtures(
    result: anyhow::Result<()>,
    keepalive: Option<FixtureKeepalive>,
) -> anyhow::Result<()> {
    let Some(FixtureKeepalive::Streaming { _hold, feeder }) = keepalive else {
        return result;
    };
    drop(_hold); // Cancels the feeder's hold token.
    let feeder_result = feeder
        .await
        .unwrap_or_else(|panic| Err(anyhow::anyhow!("fixture feeder panic: {panic}")));
    result.and(feeder_result)
}

/// On a successful `--output-state` run, emit the final reduced connector state.
/// A successful session result is replaced by a final-state read error; a failed
/// session result passes through unchanged (skip the final-state read).
async fn finish_output_state(
    run: &services::Run,
    output_state: bool,
    result: anyhow::Result<()>,
) -> anyhow::Result<()> {
    if !output_state || result.is_err() {
        return result;
    }
    emit_final_connector_state(run).await?;
    result
}

/// Re-open shard zero's RocksDB and emit its final reduced connector state as a
/// `--output-state` line. Safe to open directly: the runtime's shard serve loop
/// drops its `RocksDB` handle (releasing the exclusive lock) when its request
/// stream ends, which is strictly before its response stream reaches EOF — and
/// the session loop only returns once the driver has drained that EOF.
async fn emit_final_connector_state(run: &services::Run) -> anyhow::Result<()> {
    let state = read_preview_state(proto_flow::runtime::RocksDbDescriptor {
        rocksdb_path: run.rocksdb_path.clone(),
        rocksdb_env_memptr: 0,
    })
    .await
    .context("reading final connector state for --output-state")?;

    logger::emit_final_state(&state);
    Ok(())
}

/// Seed shard zero's RocksDB at `descriptor` with `initial_state_json` as the
/// connector-state base, then close it. Called for `--initial-state` before the
/// runtime opens the same path via its SessionLoop, so the runtime recovers the
/// seeded state on its first scan exactly as if a prior connector session had
/// persisted it. Production has no equivalent: the runtime seeds `{}` itself.
async fn seed_preview_state(
    descriptor: proto_flow::runtime::RocksDbDescriptor,
    initial_state_json: &[u8],
) -> anyhow::Result<()> {
    let db = rocksdb::RocksDB::open(Some(descriptor)).await?;
    _ = db.put_connector_state_base(initial_state_json).await?;
    Ok(())
}

/// Re-open shard zero's RocksDB at `descriptor` and return its reduced connector
/// state — the exact `Recover.connector_state_json` the runtime itself would
/// recover (empty if none was ever persisted). Called for `--output-state` after
/// the session loop has closed the runtime's own handle. Reuses the recovery
/// `scan`, so it stays consistent with how the runtime reads state.
async fn read_preview_state(
    descriptor: proto_flow::runtime::RocksDbDescriptor,
) -> anyhow::Result<bytes::Bytes> {
    let db = rocksdb::RocksDB::open(Some(descriptor)).await?;
    let (_db, recover) = db.scan(Vec::new()).await?;
    Ok(recover.connector_state_json)
}

/// Apply `--delay` to a live preview by raising the task's minimum transaction
/// duration: the leader holds each transaction open for at least `delay`,
/// batching source output into fewer, larger transactions. This is the
/// runtime-next analog of legacy preview's sleep between transaction polls.
fn set_min_txn_duration(
    shard_template: Option<&mut proto_gazette::consumer::ShardSpec>,
    delay: std::time::Duration,
) {
    let Some(shard_template) = shard_template else {
        return;
    };
    let min = pbjson_types::Duration {
        seconds: delay.as_secs() as i64,
        nanos: delay.subsec_nanos() as i32,
    };
    // Keep the close-policy band well-formed if the template's configured
    // maximum is below the requested minimum.
    if shard_template
        .max_txn_duration
        .as_ref()
        .map_or(true, |max| {
            (max.seconds, max.nanos) < (min.seconds, min.nanos)
        })
    {
        shard_template.max_txn_duration = Some(min.clone());
    }
    shard_template.min_txn_duration = Some(min);
}

/// Force one-transaction-per-checkpoint in the leader by collapsing the task's
/// transaction-duration window, so each fixture transaction commits as exactly
/// one runtime transaction (legacy fixture preview's 1:1 boundaries).
///
/// A literal `max_txn_duration` of zero would deadlock the leader: `HeadIdle`
/// gates the first checkpoint load on `open_age < max_txn_duration`, and a fresh
/// transaction's `open_age` is zero. The smallest positive duration loads one
/// checkpoint, after which the Load round's IO advances the clock past the bound
/// and the transaction closes. Applied only to fixture preview.
fn force_single_transaction(shard_template: Option<&mut proto_gazette::consumer::ShardSpec>) {
    if let Some(shard_template) = shard_template {
        shard_template.min_txn_duration = Some(pbjson_types::Duration {
            seconds: 0,
            nanos: 0,
        });
        shard_template.max_txn_duration = Some(pbjson_types::Duration {
            seconds: 0,
            nanos: 1,
        });
    }
}

/// Materialize a fixture into per-session shuffle log segments and spawn the
/// task that feeds its checkpoint frontiers to the fixture CheckpointOpener.
/// Returns fixture-bounded session targets, the per-session shuffle directories
/// (for the drivers' `Join`s), and the plan to keep alive for the run.
fn start_fixtures(
    run: &services::Run,
    task: shuffle::proto::Task,
    fixture_path: &str,
    requested_targets: Vec<u32>,
) -> anyhow::Result<(Vec<u32>, Vec<String>, fixture::FixturePlan)> {
    let mut plan = fixture::build(
        &task,
        std::path::Path::new(fixture_path),
        std::path::Path::new(&run.shuffle_log_dir),
        &requested_targets,
    )?;
    let session_targets = plan.session_targets.clone();
    let session_dirs = plan.session_dirs.clone();
    let session_frontiers = std::mem::take(&mut plan.session_frontiers);

    let frontier_tx = run
        .frontier_tx
        .clone()
        .expect("fixture run was started with a frontier sender");

    // Feed each session its frontiers, then a Boundary marker. The marker
    // bounds a session's consumption so a stopping leader's speculative
    // checkpoint consumes the marker rather than stealing the next session's
    // first frontier.
    tokio::spawn(async move {
        for frontiers in session_frontiers {
            for frontier in frontiers {
                if frontier_tx
                    .send(fixture::FixtureItem::Frontier(frontier))
                    .is_err()
                {
                    return; // The consumer went away.
                }
            }
            if frontier_tx
                .send(fixture::FixtureItem::Boundary { reached: None })
                .is_err()
            {
                return;
            }
        }
        // Dropping `frontier_tx` here signals end-of-fixtures to the replay
        // Session (relevant only once `run.frontier_tx` is also dropped).
    });

    Ok((session_targets, session_dirs, plan))
}

async fn run_with_timeout<F>(
    mut session_loop: std::pin::Pin<&mut F>,
    timeout: Option<std::time::Duration>,
    stop_token: &tokio_util::sync::CancellationToken,
) -> anyhow::Result<()>
where
    F: std::future::Future<Output = anyhow::Result<()>>,
{
    let timeout = timeout.unwrap_or_else(|| std::time::Duration::MAX);

    tokio::select! {
        result = &mut session_loop => result,
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Ctrl-C received; stopping active session");
            stop_token.cancel();
            session_loop.await
        }
        _ = tokio::time::sleep(timeout) => {
            tracing::info!(?timeout, "preview --timeout reached; stopping active session");
            stop_token.cancel();
            session_loop.await
        }
    }
}

fn resolve_task(validations: &tables::Validations, name: Option<&str>) -> anyhow::Result<TaskSpec> {
    let derivations_count = validations
        .built_collections
        .iter()
        .filter(|c| {
            c.spec
                .as_ref()
                .map(|s| s.derivation.is_some())
                .unwrap_or_default()
        })
        .count();
    let num_tasks = validations.built_captures.len()
        + validations.built_materializations.len()
        + derivations_count;

    if num_tasks == 0 {
        anyhow::bail!(
            "sourced specification files do not contain any tasks (captures, derivations, or materializations)",
        );
    }
    if num_tasks > 1 && name.is_none() {
        anyhow::bail!(
            "sourced specification files contain multiple tasks; use --name to identify the task",
        );
    }

    for row in validations.built_captures.iter() {
        if let Some(target) = name {
            if row.capture.as_str() != target {
                continue;
            }
        }
        let Some(spec) = &row.spec else { continue };
        return Ok(TaskSpec::Capture(spec.clone()));
    }

    for row in validations.built_materializations.iter() {
        if let Some(target) = name {
            if row.materialization.as_str() != target {
                continue;
            }
        }
        let Some(spec) = &row.spec else { continue };
        return Ok(TaskSpec::Materialization(spec.clone()));
    }

    for row in validations.built_collections.iter() {
        if let Some(target) = name {
            if row.collection.as_str() != target {
                continue;
            }
        }
        let Some(spec) = &row.spec else { continue };
        if spec.derivation.is_some() {
            return Ok(TaskSpec::Derivation(spec.clone()));
        }
    }

    if let Some(target) = name {
        anyhow::bail!("could not find capture, materialization, or derivation {target}");
    }
    anyhow::bail!("no capture, materialization, or derivation found in source");
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_is_streaming_fixture() {
        assert!(is_streaming_fixture("-").unwrap());

        let file = tempfile::NamedTempFile::new().unwrap();
        assert!(!is_streaming_fixture(file.path().to_str().unwrap()).unwrap());

        assert!(is_streaming_fixture("/does/not/exist").is_err());

        #[cfg(unix)]
        {
            let dir = tempfile::tempdir().unwrap();
            let fifo = dir.path().join("fifo");
            assert!(
                std::process::Command::new("mkfifo")
                    .arg(&fifo)
                    .status()
                    .unwrap()
                    .success()
            );
            assert!(is_streaming_fixture(fifo.to_str().unwrap()).unwrap());
        }
    }

    #[test]
    fn test_set_min_txn_duration() {
        let dur = |seconds, nanos| pbjson_types::Duration { seconds, nanos };

        // An unset maximum is raised alongside the minimum.
        let mut template = proto_gazette::consumer::ShardSpec::default();
        set_min_txn_duration(Some(&mut template), std::time::Duration::from_secs(10));
        assert_eq!(template.min_txn_duration, Some(dur(10, 0)));
        assert_eq!(template.max_txn_duration, Some(dur(10, 0)));

        // A maximum above the delay is left alone.
        template.max_txn_duration = Some(dur(30, 0));
        set_min_txn_duration(Some(&mut template), std::time::Duration::from_secs(10));
        assert_eq!(template.min_txn_duration, Some(dur(10, 0)));
        assert_eq!(template.max_txn_duration, Some(dur(30, 0)));

        // A maximum below the delay is raised to keep the band well-formed.
        template.max_txn_duration = Some(dur(5, 0));
        set_min_txn_duration(Some(&mut template), std::time::Duration::from_secs(10));
        assert_eq!(template.min_txn_duration, Some(dur(10, 0)));
        assert_eq!(template.max_txn_duration, Some(dur(10, 0)));
    }
}

//! The controller of a run: builds the catalog, lays out hosts as cgroup
//! subtrees, starts each host's sidecar and reactor processes, drives every
//! task's shard sessions, and samples measures until the run ends.
//!
//! A run ends cleanly at `--duration`, or on SIGINT / SIGTERM: every session is
//! stopped, and every process torn down. Any failure — a shard stream error, a
//! host process which exits — instead stops the run at once: the failure is
//! recorded in the event log, processes are killed, and the controller exits
//! non-zero. Nothing is restarted. Directories and logs are left in place.

mod broker;
mod sampler;
mod session;

use anyhow::Context;
use proto_flow::runtime as proto;
use std::path::{Path, PathBuf};

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Args {
    /// Topology file of the experiment. Runs are recorded under `runs/`
    /// alongside it.
    topology: PathBuf,
    /// Stop the run after this long. Without it, the run continues until
    /// SIGINT or SIGTERM (or a failure).
    #[clap(long)]
    duration: Option<humantime::Duration>,
    /// Suffix of the run ID, describing the run.
    #[clap(long)]
    label: Option<String>,
    /// Root of bulk run data: shuffle logs, RocksDBs, and snapshots. Put it on
    /// a local SSD. Defaults to `data/` alongside the topology.
    #[clap(long, env = "RUNTIME_LAB_DATA_ROOT")]
    data_root: Option<PathBuf>,
    /// flowctl profile whose credentials are used, unless FLOW_AUTH_TOKEN is set.
    #[clap(long, default_value = "default")]
    profile: String,
    /// Run directory of a prior run, whose shard-zero RocksDBs this run
    /// reopens in place (instead of starting from `snapshot` or empty).
    #[clap(long)]
    resume: Option<PathBuf>,
    /// Interval between samples of cgroups, metrics, and threads.
    #[clap(long, default_value = "1s")]
    sample_interval: humantime::Duration,
    /// gazette executable, which `mise run build:gazette` builds into `$GOBIN`.
    #[clap(long, env = "RUNTIME_LAB_GAZETTE", default_value = "gazette")]
    gazette: PathBuf,
    /// etcd executable, which mise installs (`mise which etcd`).
    #[clap(long, env = "RUNTIME_LAB_ETCD", default_value = "etcd")]
    etcd: PathBuf,
    /// Run ID, which names the run's directories and systemd scope.
    /// Defaults to the current UTC time, suffixed by `--label`. Scripts which
    /// start runs set it, to know where the run's manifest will appear.
    #[clap(long)]
    run_id: Option<String>,
}

/// Set within the systemd scope the controller re-executes itself into.
const IN_SCOPE_ENV: &str = "RUNTIME_LAB_IN_SCOPE";

pub fn run(args: Args, registry: service_kit::Registry) -> anyhow::Result<()> {
    // The run's cgroup tree lives in a delegated systemd scope, which the
    // controller creates by re-executing itself into it.
    if std::env::var_os(IN_SCOPE_ENV).is_none() {
        return exec_into_scope(&args);
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("controller")
        .enable_all()
        .build()?;
    let result = runtime.block_on(controller(args, registry));
    runtime.shutdown_timeout(std::time::Duration::from_secs(1));
    result
}

fn exec_into_scope(args: &Args) -> anyhow::Result<()> {
    use std::os::unix::process::CommandExt;

    let now = chrono::Utc::now().format("%Y%m%dT%H%M%S");
    let (run_id, append_run_id) = match (&args.run_id, &args.label) {
        (Some(run_id), _) => (run_id.clone(), false),
        (None, Some(label)) => (format!("{now}-{label}"), true),
        (None, None) => (now.to_string(), true),
    };
    anyhow::ensure!(
        !run_id.is_empty()
            && run_id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
        "run ID {run_id:?} (from --run-id or --label) must be [A-Za-z0-9_-]"
    );

    // The reference connectors are built beside this executable. Putting its
    // directory first on PATH resolves their `local:` commands, both in the
    // catalog build and in every host process.
    let exe = std::env::current_exe()?;
    let mut path = std::ffi::OsString::from(exe.parent().unwrap());
    if let Some(inherited) = std::env::var_os("PATH") {
        path.push(":");
        path.push(inherited);
    }

    let err = std::process::Command::new("systemd-run")
        .args([
            "--user",
            "--scope",
            "--quiet",
            "--collect",
            "-p",
            "Delegate=yes",
        ])
        .arg(format!("--unit=runtime-lab-{run_id}"))
        .arg("--")
        .arg(&exe)
        .args(std::env::args_os().skip(1))
        .args(append_run_id.then(|| format!("--run-id={run_id}")))
        .env(IN_SCOPE_ENV, "1")
        .env("PATH", path)
        .exec();
    Err(err).context("executing systemd-run (is this a systemd host with a user manager?)")
}

/// What a task needs to start: its built spec, Join topology, shard zero's
/// RocksDB symlink, its stats journal, and the partitions it restores.
struct TaskStart {
    spec: crate::catalog::TaskSpec,
    join_shards: Vec<proto::join::Shard>,
    rocksdb_link: PathBuf,
    stats_journal: proto_gazette::broker::JournalSpec,
    restore: Vec<proto_gazette::broker::JournalSpec>,
    journals: broker::TaskJournals,
}

async fn controller(args: Args, registry: service_kit::Registry) -> anyhow::Result<()> {
    let Args {
        topology: topology_path,
        duration,
        label: _,
        data_root,
        profile,
        resume,
        sample_interval,
        gazette: gazette_bin,
        etcd: etcd_bin,
        run_id,
    } = args;
    let run_id = run_id.context("missing --run-id")?;

    let topology_path = std::fs::canonicalize(&topology_path)
        .with_context(|| format!("resolving {}", topology_path.display()))?;
    let topology = crate::topology::Topology::load(&topology_path)?;
    let experiment_dir = topology_path.parent().unwrap().to_path_buf();

    let run_dir = experiment_dir.join("runs").join(&run_id);
    let data_root = match data_root {
        Some(root) => std::path::absolute(root)?,
        None => experiment_dir.join("data"),
    };
    let data_dir = data_root.join("runs").join(&run_id);
    crate::layout::ensure_outside_git(&run_dir)?;
    crate::layout::ensure_outside_git(&data_dir)?;
    anyhow::ensure!(
        !run_dir.exists(),
        "run directory {} already exists",
        run_dir.display()
    );
    anyhow::ensure!(
        !data_dir.exists(),
        "data directory {} already exists (from a prior run of this ID)",
        data_dir.display()
    );

    std::fs::create_dir_all(run_dir.join("samples"))?;
    std::fs::create_dir_all(&data_dir)?;
    std::fs::copy(&topology_path, run_dir.join("topology.yaml"))?;

    let events = crate::layout::Events::open(&run_dir)?;
    events.record(
        "runStarted",
        serde_json::json!({"runId": run_id, "runDir": run_dir, "dataDir": data_dir}),
    );
    // Every outcome from here on is recorded in the event log.
    let result = async {
        if let Some(warning) = crate::layout::storage_warning(&data_dir) {
            tracing::warn!("{warning}");
            events.record("storageWarning", serde_json::json!({"warning": warning}));
        }

        let cgroup_root = crate::cgroup::current()?;
        let missing = crate::cgroup::take_root(&cgroup_root)?;
        if !missing.is_empty() {
            let warning = format!(
                "cgroup controllers {missing:?} aren't delegated to this user, and their interface files can't be set. Run crates/runtime-lab/scripts/setup-cgroups.sh"
            );
            tracing::warn!("{warning}");
            events.record("cgroupWarning", serde_json::json!({"warning": warning}));
        }

        let session = crate::auth::start(&profile).await?;
        // Reference connectors take no secrets.
        let connector_router = runtime_local::local_router(
            String::new(),
            registry.clone(),
            std::sync::Arc::new(flow_client_next::secret_resolver::NoOp),
        );
        let catalog = experiment_dir.join(&topology.catalog);
        let mut specs = crate::catalog::build(&session, &catalog, connector_router).await?;
        events.record("catalogBuilt", serde_json::json!({"catalog": catalog}));

        let shard_plans = crate::topology::plan_shards(&topology);
        let prior = resume
            .as_deref()
            .map(crate::layout::Manifest::load)
            .transpose()?;

        // Unix socket paths are limited to 108 bytes, so they live in a short root.
        let sockets_dir = std::env::var_os("XDG_RUNTIME_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir())
            .join("runtime-lab")
            .join(&run_id);
        std::fs::create_dir_all(&sockets_dir)?;

        let mut manifest = crate::layout::Manifest {
            run_id: run_id.clone(),
            topology: topology_path.clone(),
            run_dir: run_dir.clone(),
            data_dir: data_dir.clone(),
            cgroup: cgroup_root.clone(),
            resumed: prior.as_ref().map(|prior| prior.run_dir.clone()),
            controller: crate::layout::ManifestProcess {
                pid: std::process::id(),
                cgroup: cgroup_root.join("controller"),
                ..Default::default()
            },
            ..Default::default()
        };

        // Resolve each task's spec, shard zero's RocksDB, and its Join topology.
        let mut task_starts: Vec<TaskStart> = Vec::new();
        for (task_index, (task_name, task)) in topology.tasks.iter().enumerate() {
            let mut spec = specs.remove(task_name).with_context(|| {
                format!("task {task_name} isn't a capture, materialization, or derivation of {catalog:?}")
            })?;
            // runtime-next's capture sessions are of a single, leaderless shard.
            anyhow::ensure!(
                !matches!(spec, crate::catalog::TaskSpec::Capture(_)) || task.shards.len() == 1,
                "capture {task_name} must have exactly one shard, not {}",
                task.shards.len()
            );
            if let Some(rate) = topology.max_append_rate {
                spec.set_max_append_rate(rate);
            }

            // Where shard zero's RocksDB comes from, and the journals file (if
            // any) of the partitions it wrote, which are restored with it.
            let (rocksdb_dir, start, restore_from) = match (&prior, &task.snapshot) {
                (Some(prior), _) => {
                    let prior_task = prior
                        .tasks
                        .get(task_name)
                        .with_context(|| format!("--resume run has no task {task_name}"))?;
                    (
                        prior_task.rocksdb_dir.clone(),
                        format!("resumed:{}", prior.run_id),
                        Some(prior_task.journals_file.clone()).filter(|p| p.exists()),
                    )
                }
                (None, Some(snapshot)) => {
                    let dir = data_dir.join("rocksdb").join(format!("t{task_index}"));
                    let journals = copy_snapshot(&experiment_dir, &data_root, snapshot, &dir)?;
                    (dir, format!("snapshot:{snapshot}"), journals)
                }
                (None, None) => {
                    let dir = data_dir.join("rocksdb").join(format!("t{task_index}"));
                    std::fs::create_dir_all(&dir)?;
                    (dir, "fresh".to_string(), None)
                }
            };
            let restore = match &restore_from {
                Some(path) => restore_journals(&spec, broker::load_journals(path)?),
                None => Vec::new(),
            };
            events.record(
                "taskStart",
                serde_json::json!({
                    "task": task_name, "start": start, "rocksdbDir": rocksdb_dir,
                    "restoredPartitions": restore.len(),
                }),
            );
            anyhow::ensure!(
                rocksdb_dir.is_dir(),
                "shard-zero RocksDB {} doesn't exist",
                rocksdb_dir.display()
            );

            // Shard zero removes the RocksDB path it's given as its stream
            // ends, as a production shard's directory is disposable (its
            // recovery log is the durable state). Here the directory *is* the
            // durable state, which a snapshot captures or a `--resume` reopens.
            // So the shard is given a symlink: `std::fs::remove_dir_all`
            // removes a symlink, and never what it points to.
            let rocksdb_link = data_dir.join("rocksdb").join(format!("t{task_index}.link"));
            std::fs::create_dir_all(data_dir.join("rocksdb"))?;
            std::os::unix::fs::symlink(&rocksdb_dir, &rocksdb_link)
                .with_context(|| format!("linking {}", rocksdb_link.display()))?;

            let stats_journal = broker::stats_journal(spec.ops_task_type(), task_name);
            let join_shards = join_shards(&spec, task, &stats_journal.name)?;
            let journals_file = data_dir.join("journals").join(format!("t{task_index}.json"));
            std::fs::create_dir_all(data_dir.join("journals"))?;
            let plans: Vec<_> = shard_plans
                .iter()
                .filter(|p| p.task_index == task_index)
                .collect();

            let mut manifest_shards = Vec::new();
            for (plan, join_shard) in plans.iter().zip(&join_shards) {
                let range = join_shard
                    .labeling
                    .as_ref()
                    .and_then(|l| l.range.as_ref())
                    .unwrap();
                let shuffle_dir = data_dir
                    .join("hosts")
                    .join(&plan.host)
                    .join("shuffle")
                    .join(&plan.label);
                std::fs::create_dir_all(&shuffle_dir)?;

                manifest_shards.push(crate::layout::ManifestShard {
                    index: plan.shard_index,
                    label: plan.label.clone(),
                    id: join_shard.id.clone(),
                    host: plan.host.clone(),
                    key_begin: format!("{:08x}", range.key_begin),
                    key_end: format!("{:08x}", range.key_end),
                    r_clock_begin: format!("{:08x}", range.r_clock_begin),
                    r_clock_end: format!("{:08x}", range.r_clock_end),
                    socket: sockets_dir.join(format!("{}.sock", plan.label)),
                    shuffle_dir,
                });
            }
            manifest.tasks.insert(
                task_name.clone(),
                crate::layout::ManifestTask {
                    kind: spec.task_type().to_string(),
                    start,
                    rocksdb_dir,
                    journals_file: journals_file.clone(),
                    shards: manifest_shards,
                },
            );
            let journals = broker::TaskJournals {
                collections: spec
                    .written_collections()
                    .into_iter()
                    .map(|c| c.name.clone())
                    .collect(),
                path: journals_file,
            };
            task_starts.push(TaskStart {
                spec,
                join_shards,
                rocksdb_link,
                stats_journal,
                restore,
                journals,
            });
        }

        // From here on every child process is ours to tear down, on any exit path.
        let mut children = Children::default();
        let result = async {
            let broker = start_gazette(
                &topology,
                &etcd_bin,
                &gazette_bin,
                &mut manifest,
                &mut children,
            )
            .await?;
            events.record("brokerReady", serde_json::json!({"endpoint": broker}));

            let client = broker::client(&broker);
            let journals: Vec<_> = task_starts
                .iter_mut()
                .flat_map(|start| {
                    std::iter::once(start.stats_journal.clone())
                        .chain(std::mem::take(&mut start.restore))
                })
                .collect();
            broker::create(&client, journals).await?;

            start_hosts(
                &topology,
                &shard_plans,
                &profile,
                &broker,
                &mut manifest,
                &mut children,
            )
            .await?;
            events.record(
                "hostsReady",
                serde_json::json!({"hosts": manifest.hosts.keys().collect::<Vec<_>>()}),
            );
            drive_run(
                &manifest,
                task_starts,
                client,
                &mut children,
                duration.map(Into::into),
                sample_interval.into(),
                &events,
            )
            .await
        }
        .await;

        // A failed run is torn down promptly: its state is as the failure left it.
        let grace = if result.is_ok() { 10 } else { 2 };
        children
            .terminate(std::time::Duration::from_secs(grace), &events)
            .await;
        let _ = std::fs::remove_dir_all(&sockets_dir);
        result
    }
    .await;

    match &result {
        Ok(()) => events.record("runStopped", serde_json::json!({"outcome": "ok"})),
        Err(err) => {
            tracing::error!(error = format!("{err:#}"), "run failed");
            events.record(
                "runFailed",
                serde_json::json!({"error": format!("{err:#}")}),
            )
        }
    }
    result
}

/// Start the run's etcd and broker, each into its own cgroup at the run's
/// root, and await their readiness. Returns the broker's endpoint.
async fn start_gazette(
    topology: &crate::topology::Topology,
    etcd_bin: &Path,
    gazette_bin: &Path,
    manifest: &mut crate::layout::Manifest,
    children: &mut Children,
) -> anyhow::Result<String> {
    let (run_dir, data_dir, cgroup_root) = (
        manifest.run_dir.clone(),
        manifest.data_dir.clone(),
        manifest.cgroup.clone(),
    );
    let (etcd_dir, fragments_dir, tmp_dir) = (
        data_dir.join("etcd"),
        data_dir.join("fragments"),
        data_dir.join("broker").join("tmp"),
    );
    for dir in [&etcd_dir, &fragments_dir, &tmp_dir] {
        std::fs::create_dir_all(dir)?;
    }
    crate::cgroup::create(&cgroup_root.join("etcd"), &topology.etcd.cgroup)?;
    crate::cgroup::create(&cgroup_root.join("broker"), &topology.broker.cgroup)?;

    let (client_port, peer_port, broker_port) = (
        broker::free_port()?,
        broker::free_port()?,
        broker::free_port()?,
    );
    let etcd_endpoint = format!("http://127.0.0.1:{client_port}");
    let broker_endpoint = format!("http://127.0.0.1:{broker_port}");

    let mut cmd = host_command(etcd_bin, &run_dir.join("etcd.log"), &tmp_dir)?;
    cmd.args(broker::etcd_args(&etcd_dir, client_port, peer_port))
        .args(&topology.etcd.args)
        .envs(&topology.etcd.env);
    let child = crate::cgroup::spawn_into(&mut cmd, &cgroup_root.join("etcd"))
        .with_context(|| format!("starting {}", etcd_bin.display()))?;
    manifest.etcd = crate::layout::ManifestProcess {
        pid: child.id().unwrap_or_default(),
        cgroup: cgroup_root.join("etcd"),
        admin_port: None,
        log: Some(run_dir.join("etcd.log")),
    };
    children.push("etcd".to_string(), child);

    let http = reqwest::Client::new();
    let health = format!("{etcd_endpoint}/health");
    children
        .await_probe("etcd", || async {
            matches!(http.get(&health).send().await, Ok(r) if r.status().is_success())
        })
        .await?;

    // Spools of not-yet-persisted fragments are temp files, on the data root.
    let mut cmd = host_command(gazette_bin, &run_dir.join("broker.log"), &tmp_dir)?;
    cmd.args(broker::broker_args(
        &fragments_dir,
        broker_port,
        &etcd_endpoint,
    ))
    .args(&topology.broker.args)
    .envs(&topology.broker.env);
    let child = crate::cgroup::spawn_into(&mut cmd, &cgroup_root.join("broker"))
        .with_context(|| format!("starting {}", gazette_bin.display()))?;
    manifest.broker = crate::layout::ManifestProcess {
        pid: child.id().unwrap_or_default(),
        cgroup: cgroup_root.join("broker"),
        admin_port: None,
        log: Some(run_dir.join("broker.log")),
    };
    children.push("broker".to_string(), child);

    let client = broker::client(&broker_endpoint);
    children
        .await_probe("broker", || async {
            client
                .list(proto_gazette::broker::ListRequest::default())
                .await
                .is_ok()
        })
        .await?;

    manifest.broker_endpoint = broker_endpoint.clone();
    manifest.fragments_dir = fragments_dir;
    Ok(broker_endpoint)
}

/// Lay out every host's cgroups, start its processes, and await their
/// readiness, recording each host in `manifest`, which is then written.
async fn start_hosts(
    topology: &crate::topology::Topology,
    shard_plans: &[crate::topology::ShardPlan],
    profile: &str,
    broker: &str,
    manifest: &mut crate::layout::Manifest,
    children: &mut Children,
) -> anyhow::Result<()> {
    let exe = std::env::current_exe()?;
    let (run_dir, data_dir, cgroup_root) = (
        manifest.run_dir.clone(),
        manifest.data_dir.clone(),
        manifest.cgroup.clone(),
    );

    // Lay out every host's cgroups and start its sidecar first: a reactor's
    // shards dial their sidecar, and the shard-zero sidecar's Leader.
    let mut started = Vec::new();
    for (name, host) in &topology.hosts {
        let cgroup = cgroup_root.join(name);
        crate::cgroup::create(&cgroup, &host.cgroup)?;
        crate::cgroup::enable_controllers(&cgroup)?;
        crate::cgroup::create(&cgroup.join("reactor"), &host.reactor.cgroup)?;
        crate::cgroup::create(&cgroup.join("sidecar"), &host.sidecar.cgroup)?;
        crate::cgroup::create(&cgroup.join("connectors"), &host.connectors.cgroup)?;

        let dir = run_dir.join("hosts").join(name);
        let host_data = data_dir.join("hosts").join(name);
        std::fs::create_dir_all(&dir)?;
        std::fs::create_dir_all(host_data.join("tmp"))?;

        let mut cmd = host_command(&exe, &dir.join("sidecar.log"), &host_data.join("tmp"))?;
        cmd.arg("sidecar")
            .arg(format!("--host={name}"))
            .arg(format!("--profile={profile}"))
            .arg(format!(
                "--ready-file={}",
                dir.join("sidecar.ready.json").display()
            ))
            .arg(format!("--broker={broker}"))
            .args(&host.sidecar.args)
            .envs(&host.sidecar.env);
        let child = crate::cgroup::spawn_into(&mut cmd, &cgroup.join("sidecar"))?;
        children.push(format!("{name}/sidecar"), child);

        started.push((name, host, cgroup, dir, host_data));
    }

    for (name, host, cgroup, dir, host_data) in started {
        let sidecar = children
            .await_ready(&format!("{name}/sidecar"), &dir.join("sidecar.ready.json"))
            .await?;
        let endpoint = sidecar
            .endpoint
            .clone()
            .context("sidecar reported no endpoint")?;

        let shards: Vec<_> = shard_plans.iter().filter(|p| &p.host == name).collect();
        let reactor = if shards.is_empty() {
            None
        } else {
            let mut cmd = host_command(&exe, &dir.join("reactor.log"), &host_data.join("tmp"))?;
            cmd.arg("reactor")
                .arg(format!("--host={name}"))
                .arg(format!(
                    "--ready-file={}",
                    dir.join("reactor.ready.json").display()
                ))
                .arg(format!("--broker={broker}"));
            for plan in &shards {
                let socket = &manifest.tasks[&plan.task].shards[plan.shard_index].socket;
                cmd.arg(format!(
                    "--shard={},{},{}",
                    plan.label,
                    plan.task,
                    socket.display()
                ));
            }
            cmd.args(&host.reactor.args)
                .envs(&host.reactor.env)
                .env(crate::connector::CGROUP_ENV, cgroup.join("connectors"));

            let child = crate::cgroup::spawn_into(&mut cmd, &cgroup.join("reactor"))?;
            children.push(format!("{name}/reactor"), child);
            Some(
                children
                    .await_ready(&format!("{name}/reactor"), &dir.join("reactor.ready.json"))
                    .await?,
            )
        };

        let process = |ready: crate::layout::Ready, role: &str| crate::layout::ManifestProcess {
            pid: ready.pid,
            cgroup: cgroup.join(role),
            admin_port: Some(ready.admin_port),
            log: Some(dir.join(format!("{role}.log"))),
        };
        manifest.hosts.insert(
            name.clone(),
            crate::layout::ManifestHost {
                reactor: reactor.map(|ready| process(ready, "reactor")),
                sidecar: process(sidecar, "sidecar"),
                endpoint,
                connectors_cgroup: cgroup.join("connectors"),
                cgroup,
                dir,
                data_dir: host_data,
            },
        );
    }
    manifest.write(&run_dir)
}

/// Sample the run, keep the broker's journals (stats tails, journals files,
/// fragment reclamation), and drive every task's sessions until a requested
/// stop (which stops every session), or a failure.
async fn drive_run(
    manifest: &crate::layout::Manifest,
    task_starts: Vec<TaskStart>,
    client: gazette::journal::Client,
    children: &mut Children,
    duration: Option<std::time::Duration>,
    sample_interval: std::time::Duration,
    events: &crate::layout::Events,
) -> anyhow::Result<()> {
    // Sample every node of the tree, and every host process.
    let mut nodes = vec![sampler::Node {
        name: "controller".to_string(),
        path: manifest.controller.cgroup.clone(),
    }];
    let mut processes = Vec::new();
    for (name, process, metrics_url) in [
        (
            "broker",
            &manifest.broker,
            Some(format!("{}/debug/metrics", manifest.broker_endpoint)),
        ),
        ("etcd", &manifest.etcd, None),
    ] {
        nodes.push(sampler::Node {
            name: name.to_string(),
            path: process.cgroup.clone(),
        });
        processes.push(sampler::Process {
            name: name.to_string(),
            pid: process.pid,
            metrics_url,
        });
    }
    for (name, host) in &manifest.hosts {
        nodes.push(sampler::Node {
            name: name.clone(),
            path: host.cgroup.clone(),
        });
        for role in ["reactor", "sidecar", "connectors"] {
            nodes.push(sampler::Node {
                name: format!("{name}/{role}"),
                path: host.cgroup.join(role),
            });
        }
        for (role, process) in [
            ("reactor", host.reactor.as_ref()),
            ("sidecar", Some(&host.sidecar)),
        ] {
            let Some(process) = process else { continue };
            let admin_port = process
                .admin_port
                .expect("host processes serve an admin surface");
            processes.push(sampler::Process {
                name: format!("{name}/{role}"),
                pid: process.pid,
                metrics_url: Some(format!("http://127.0.0.1:{admin_port}/metrics")),
            });
        }
    }

    // Background work which runs for the whole run. Each is part of the run's
    // record, or keeps it running, so any of them returning is a failure.
    let samples_dir = manifest.run_dir.join("samples");
    let journal_tasks: Vec<_> = task_starts.iter().map(|t| t.journals.clone()).collect();
    let stats_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(manifest.run_dir.join("stats.ndjson"))
        .context("opening stats.ndjson")?;
    let stats_file = std::sync::Arc::new(std::sync::Mutex::new(stats_file));

    let mut background = tokio::task::JoinSet::new();
    background.spawn(async move {
        sampler::run(samples_dir, sample_interval, nodes, processes)
            .await
            .context("sampling the run")
    });
    let journals = broker::JournalsSampler::new(
        client.clone(),
        &manifest.run_dir.join("samples"),
        journal_tasks.clone(),
        events.clone(),
    )?;
    background.spawn(async move {
        journals
            .run(sample_interval)
            .await
            .context("sampling journals")
    });
    for start in &task_starts {
        background.spawn(broker::tail_stats(
            client.clone(),
            start.stats_journal.name.clone(),
            stats_file.clone(),
        ));
    }
    background.spawn(broker::reclaim_fragments(
        manifest.fragments_dir.clone(),
        sample_interval.max(std::time::Duration::from_secs(10)),
    ));

    // Drive every task's sessions until a requested stop, or a failure.
    let stop = tokio_util::sync::CancellationToken::new();
    let mut drivers = tokio::task::JoinSet::new();
    for TaskStart {
        spec,
        join_shards,
        rocksdb_link,
        ..
    } in task_starts
    {
        let name = spec.name().to_string();
        let shards = manifest.tasks[&name]
            .shards
            .iter()
            .map(|shard| session::ShardRun {
                label: shard.label.clone(),
                socket: shard.socket.clone(),
                shuffle_dir: shard.shuffle_dir.to_string_lossy().into_owned(),
                shuffle_endpoint: manifest.hosts[&shard.host].endpoint.clone(),
            })
            .collect();
        let task = session::TaskRun {
            name: name.clone(),
            spec: spec.encode(),
            rocksdb_link,
            join_shards,
            shards,
        };
        let (stop, events) = (stop.clone(), events.clone());
        let driver = async move {
            match spec {
                crate::catalog::TaskSpec::Capture(_) => {
                    session::drive::<proto::Capture>(task, stop, events).await
                }
                crate::catalog::TaskSpec::Materialization(_) => {
                    session::drive::<proto::Materialize>(task, stop, events).await
                }
                crate::catalog::TaskSpec::Derivation(_) => {
                    session::drive::<proto::Derive>(task, stop, events).await
                }
            }
            .with_context(|| format!("task {name}"))
        };
        drivers.spawn(driver);
    }

    let requested_stop = async {
        tokio::select! {
            reason = crate::await_signal() => reason,
            () = tokio::time::sleep(duration.unwrap_or(std::time::Duration::MAX)) => "duration",
        }
    };
    tokio::pin!(requested_stop);
    let mut stopping = false;

    let result = loop {
        tokio::select! {
            reason = &mut requested_stop, if !stopping => {
                stopping = true;
                events.record("stopping", serde_json::json!({"reason": reason}));
                stop.cancel();
            }
            exited = children.any_exit() => {
                let (name, status) = exited;
                break Err(anyhow::anyhow!("process {name} exited unexpectedly ({status})"));
            }
            Some(joined) = background.join_next() => break Err(match joined {
                Ok(Err(err)) => err,
                Ok(Ok(never)) => match never {},
                Err(panic) => anyhow::anyhow!("background task panicked: {panic}"),
            }),
            joined = drivers.join_next() => match joined {
                None => break Ok(()),
                Some(Ok(Ok(()))) => {}
                Some(Ok(Err(err))) => break Err(attribute(err, children).await),
                Some(Err(panic)) => break Err(anyhow::anyhow!("task driver panicked: {panic}")),
            },
        }
    };
    drivers.abort_all();
    background.abort_all();

    // Journals files are rewritten every sample. A last one, as the run ends,
    // captures partitions as the run left them (if the broker still serves).
    let last = async {
        broker::JournalsSampler::new(
            client,
            &manifest.run_dir.join("samples"),
            journal_tasks,
            events.clone(),
        )?
        .sample()
        .await
    };
    match tokio::time::timeout(std::time::Duration::from_secs(5), last).await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => events.record(
            "journalsNotSaved",
            serde_json::json!({"error": format!("{err:#}")}),
        ),
        Err(_) => events.record(
            "journalsNotSaved",
            serde_json::json!({"error": "broker didn't answer within 5s"}),
        ),
    }
    result
}

/// A shard stream usually fails *because* a host process died, and its error
/// (say, an HTTP/2 protocol error) names only the symptom. Give the process a
/// moment to be reaped, and name it as the cause when it has.
async fn attribute(err: anyhow::Error, children: &mut Children) -> anyhow::Error {
    tokio::select! {
        (name, status) = children.any_exit() => {
            err.context(format!("host process {name} exited unexpectedly ({status})"))
        }
        () = tokio::time::sleep(std::time::Duration::from_millis(500)) => err,
    }
}

/// A process of the run (a host subcommand of this executable, the broker, or
/// etcd), logging to `log` and keeping its temporary files (non-zero shard
/// RocksDBs, connector scratch, fragment spools) in `tmp_dir` on the data
/// root, rather than wherever TMPDIR points.
///
/// It's placed in its own process group: a terminal's Ctrl-C signals the whole
/// foreground group, and would otherwise reach every host process directly,
/// which then exits on its own (a failed run) rather than through the
/// controller's stop of every session.
fn host_command(exe: &Path, log: &Path, tmp_dir: &Path) -> anyhow::Result<tokio::process::Command> {
    let log = std::fs::File::create(log).with_context(|| format!("creating {}", log.display()))?;
    let mut cmd = tokio::process::Command::new(exe);
    cmd.stdin(std::process::Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log)
        .env("TMPDIR", tmp_dir)
        .env_remove(IN_SCOPE_ENV)
        .process_group(0)
        .kill_on_drop(true);
    Ok(cmd)
}

/// Synthesize the Join's shard topology as production labels it: decoded from
/// the task's shard template (so spec shard settings like `maxTxnDuration`
/// and `shuffleDiskLimit` take effect), with the stats journal which
/// activation would add, and split evenly on key and r-clock.
fn join_shards(
    spec: &crate::catalog::TaskSpec,
    task: &crate::topology::Task,
    stats_journal: &str,
) -> anyhow::Result<Vec<proto::join::Shard>> {
    let template = spec
        .shard_labels()
        .context("built task is missing shard labels")?;
    let mut labeling = labels::shard::decode_labeling(template)?;
    labeling.stats_journal = stats_journal.to_string();
    // A local build has no publication which assigned a generation, so shard
    // IDs take the shape they'd carry in production under generation zero.
    let id_prefix = assemble::shard_id_prefix(models::Id::zero(), spec.name(), spec.task_type());
    let key_splits = task.shards.len() as u32 / task.rclock_splits;

    Ok(
        labels::shard::even_splits(&id_prefix, key_splits, task.rclock_splits)
            .into_iter()
            .zip(&task.shards)
            .map(|(split, host)| proto::join::Shard {
                id: split.id,
                labeling: Some(ops::ShardLabeling {
                    range: Some(split.range),
                    ..labeling.clone()
                }),
                reactor: Some(proto_gazette::broker::process_spec::Id {
                    zone: "local".to_string(),
                    suffix: host.clone(),
                }),
                etcd_create_revision: 1,
            })
            .collect(),
    )
}

/// Initialize shard zero's RocksDB `dir` from a named snapshot, which is
/// copied (never modified) so that every run from it starts identically.
/// A snapshot containing a `/` is a path, relative to the experiment directory.
///
/// A snapshot is a directory of `rocksdb/` and the `journals.json` of the
/// partitions the task wrote, which is returned. A snapshot which is itself a
/// RocksDB (it has a `CURRENT`) predates journals files, and restores none.
fn copy_snapshot(
    experiment_dir: &Path,
    data_root: &Path,
    snapshot: &str,
    dir: &Path,
) -> anyhow::Result<Option<PathBuf>> {
    let source = if snapshot.contains('/') {
        experiment_dir.join(snapshot)
    } else {
        data_root.join("snapshots").join(snapshot)
    };
    anyhow::ensure!(
        source.is_dir(),
        "snapshot {} doesn't exist",
        source.display()
    );
    std::fs::create_dir_all(dir.parent().unwrap())?;

    let (source, journals) = if source.join("CURRENT").exists() {
        (source, None)
    } else {
        let journals = source.join("journals.json");
        (
            source.join("rocksdb"),
            journals.exists().then_some(journals),
        )
    };
    anyhow::ensure!(
        source.is_dir(),
        "snapshot {} has no rocksdb/",
        source.display()
    );

    let status = std::process::Command::new("cp")
        .args(["-a", "--reflink=auto"])
        .arg(&source)
        .arg(dir)
        .status()
        .context("running cp")?;
    anyhow::ensure!(
        status.success(),
        "copying snapshot {} failed",
        source.display()
    );
    Ok(journals)
}

/// Partition specs of a prior run (or snapshot) to restore for `spec`. Each
/// takes the append rate of this run's partition template, so a restored
/// partition is throttled as a newly-created one is.
fn restore_journals(
    spec: &crate::catalog::TaskSpec,
    mut journals: Vec<proto_gazette::broker::JournalSpec>,
) -> Vec<proto_gazette::broker::JournalSpec> {
    let rates: std::collections::BTreeMap<&str, i64> = spec
        .written_collections()
        .into_iter()
        .filter_map(|c| {
            let rate = c.partition_template.as_ref()?.max_append_rate;
            Some((c.name.as_str(), rate))
        })
        .collect();

    for journal in &mut journals {
        let collection = journal
            .labels
            .as_ref()
            .and_then(|l| labels::expect_one(l, labels::COLLECTION).ok());
        if let Some(rate) = collection.and_then(|c| rates.get(c)) {
            journal.max_append_rate = *rate;
        }
    }
    journals
}

/// The run's host processes, by name.
#[derive(Default)]
struct Children(Vec<(String, tokio::process::Child)>);

impl Children {
    fn push(&mut self, name: String, child: tokio::process::Child) {
        self.0.push((name, child));
    }

    /// Resolve with the first child to exit, or never if there are none.
    async fn any_exit(&mut self) -> (String, std::process::ExitStatus) {
        if self.0.is_empty() {
            return std::future::pending().await;
        }
        let waits = self
            .0
            .iter_mut()
            .map(|(name, child)| Box::pin(async move { (name.clone(), child.wait().await) }));
        let ((name, status), _, _) = futures::future::select_all(waits).await;
        (name, status.expect("waiting on child"))
    }

    /// Await a `probe` of child `name` which succeeds, failing if any child
    /// exits first.
    async fn await_probe<F, Fut>(&mut self, name: &str, mut probe: F) -> anyhow::Result<()>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            anyhow::ensure!(
                tokio::time::Instant::now() < deadline,
                "{name} wasn't ready within a minute (see its log)"
            );
            tokio::select! {
                (exited, status) = self.any_exit() => {
                    anyhow::bail!("{exited} exited while starting ({status}); see its log");
                }
                ready = probe() => if ready {
                    return Ok(());
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    /// Await `ready_file` of child `name`, failing if any child exits first.
    async fn await_ready(
        &mut self,
        name: &str,
        ready_file: &Path,
    ) -> anyhow::Result<crate::layout::Ready> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            if let Ok(content) = std::fs::read(ready_file) {
                return serde_json::from_slice(&content)
                    .with_context(|| format!("parsing {}", ready_file.display()));
            }
            anyhow::ensure!(
                tokio::time::Instant::now() < deadline,
                "{name} wasn't ready within a minute (see its log)"
            );
            tokio::select! {
                (exited, status) = self.any_exit() => {
                    anyhow::bail!("{exited} exited while starting ({status}); see its log");
                }
                () = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
            }
        }
    }

    /// SIGTERM every live child, then SIGKILL any which outlive `grace`.
    ///
    /// The broker and etcd are SIGKILLed at once: a lone gazette broker's
    /// graceful exit awaits the hand-off of its journals to a peer, which it
    /// doesn't have, and neither holds state the run hasn't already recorded.
    async fn terminate(&mut self, grace: std::time::Duration, events: &crate::layout::Events) {
        for (name, child) in &self.0 {
            let signal = match name.as_str() {
                "broker" | "etcd" => libc::SIGKILL,
                _ => libc::SIGTERM,
            };
            if let Some(pid) = child.id() {
                unsafe { libc::kill(pid as i32, signal) };
            }
        }
        let deadline = tokio::time::Instant::now() + grace;
        for (name, child) in &mut self.0 {
            match tokio::time::timeout_at(deadline, child.wait()).await {
                Ok(_) => {}
                Err(_) => {
                    events.record(
                        "hostKilled",
                        serde_json::json!({"process": name, "reason": "outlived its SIGTERM grace period"}),
                    );
                    let _ = child.kill().await;
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::proto;

    // The lab relies on shard zero removing the symlink it's given as its
    // RocksDB path, and never the directory which the symlink names.
    #[tokio::test]
    async fn rocksdb_link_is_removed_and_directory_retained() {
        let parent = tempfile::tempdir().unwrap();
        let (dir, link) = (parent.path().join("t0"), parent.path().join("t0.link"));
        std::fs::create_dir(&dir).unwrap();
        std::os::unix::fs::symlink(&dir, &link).unwrap();

        let db = runtime_next::shard::rocksdb::RocksDB::open(Some(proto::RocksDbDescriptor {
            rocksdb_path: link.to_string_lossy().into_owned(),
            rocksdb_env_memptr: 0,
        }))
        .await
        .unwrap();
        std::mem::drop(db);

        assert!(std::fs::symlink_metadata(&link).is_err(), "link removed");
        assert!(dir.join("CURRENT").exists(), "directory retained");
    }
}

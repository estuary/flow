//! The IO of one VMM launch, from an admitted connector start to a dialed
//! connector-init: release what dead launches left in the state directory,
//! pull and inspect the connector's image with the VMM's podman, claim the
//! launch's ownership record, prepare its connector mount and `fv_<id>`
//! state, verify the host boundary, create the VMM's network, volume and
//! container, start it, and dial `init.sock` until connector-init's health is SERVING.
//!
//! One spawned task owns everything a launch creates, from its claim to its
//! release, and holds the record's lock throughout (see `record`). The
//! commands which create the network, volume and container are never
//! interrupted, so each client has exited before teardown looks for what it
//! made. The podman service may still finish a create after that, and
//! `release::recover` removes what it makes.
//! The task holds a clone of the session's log sink, so the session
//! ends only once its teardown has.

use super::{Eligible, Vmm, plan, record, release};
use anyhow::Context;
use proto_flow::runtime;
use std::time::Duration;

const ATTEMPTS: usize = 3;
/// The one deadline for every dial of a VMM's connector-init and its health.
const READY_TIMEOUT: Duration = Duration::from_secs(60);
const DIAL_INTERVAL: Duration = Duration::from_millis(100);
/// libkrun holds pre-listen connections unanswered for 5s. Retry sooner so
/// the first connector RPC can beat connector-init's 5s idle watchdog.
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(1);
/// How long teardown waits for podman's attached start client to leave, once
/// its container is removed.
const START_EXIT_TIMEOUT: Duration = Duration::from_secs(10);
const PUMP_TIMEOUT: Duration = Duration::from_secs(5);

/// Ties a VMM to the served stream: dropping it tells the launch's task to
/// tear the VMM down.
pub(crate) struct Guard {
    _stop: tokio::sync::oneshot::Sender<()>,
}

struct Input {
    vmm: Vmm,
    eligible: &'static Eligible,
    egress: Option<Vec<egress::AllowedName>>,
    image: String,
    log_level: ops::LogLevel,
    log_sink: crate::LogSink,
    plane: crate::Plane,
    task_name: String,
    secrets: std::collections::BTreeMap<String, String>,
    task_update: Option<crate::TaskUpdate>,
    build: Option<String>,
}

struct Launched {
    container: runtime::Container,
    channel: tonic::transport::Channel,
    codec: connector_init::Codec,
}

type LaunchedTx = tokio::sync::oneshot::Sender<anyhow::Result<Launched>>;

/// A launch's claim on what it creates.
struct Owned {
    /// The ownership record, locked for as long as this is held.
    file: std::fs::File,
    path: String,
    record: record::Record,
    /// What `podman create` printed.
    container: Option<String>,
    attached: Option<Attached>,
    refresh: Option<tokio::sync::oneshot::Sender<()>>,
}

struct Attached {
    /// podman's `start --attach` client, which exits with its container.
    child: async_process::Child,
    /// Reads the client's stderr, and holds a clone of the session's sink.
    pump: tokio::task::JoinHandle<()>,
}

/// Launch `image` as the eligible connector in a VMM of `vmm`'s
/// configuration, returning what an ordinary container start returns once
/// connector-init is dialed.
pub(crate) async fn start(
    ctx: &crate::protocol::StartContext,
    vmm: &Vmm,
    eligible: &'static Eligible,
    egress: Option<Vec<egress::AllowedName>>,
    image: &str,
    secrets: &std::collections::BTreeMap<String, String>,
    build: Option<&str>,
) -> anyhow::Result<(
    runtime::Container,
    tonic::transport::Channel,
    Guard,
    connector_init::Codec,
)> {
    let input = Input {
        vmm: vmm.clone(),
        eligible,
        egress,
        image: image.to_string(),
        log_level: ctx.log_level,
        log_sink: ctx.log_sink.clone(),
        plane: ctx.plane,
        task_name: ctx.task_name.clone(),
        secrets: secrets.clone(),
        task_update: ctx.task_update.clone(),
        build: build.map(str::to_string),
    };
    let (launched_tx, launched_rx) = tokio::sync::oneshot::channel();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(lifecycle(input, launched_tx, stop_rx));

    // Dropping this future, as an abandoned start does, drops the receiver,
    // which the task watches for.
    let Launched {
        container,
        channel,
        codec,
    } = launched_rx
        .await
        .context("the VMM launch ended without an outcome")??;

    Ok((container, channel, Guard { _stop: stop_tx }, codec))
}

async fn lifecycle(
    input: Input,
    mut launched_tx: LaunchedTx,
    stop_rx: tokio::sync::oneshot::Receiver<()>,
) {
    let mut owned = None;

    match launch(&input, &mut owned, &mut launched_tx).await {
        Ok(launched) => {
            // Serve until the session drops its Guard, unless the start was
            // abandoned just as the launch succeeded.
            if launched_tx.send(Ok(launched)).is_ok() {
                _ = stop_rx.await;
            }
            teardown(&input, owned).await;
        }
        Err(err) => {
            teardown(&input, owned).await;
            _ = launched_tx.send(Err(err));
        }
    }
}

async fn launch(
    input: &Input,
    owned: &mut Option<Owned>,
    launched_tx: &mut LaunchedTx,
) -> anyhow::Result<Launched> {
    let Input {
        vmm,
        eligible,
        egress,
        image,
        log_level,
        log_sink,
        plane,
        task_name,
        secrets,
        task_update,
        build,
    } = input;
    let podman = vmm.podman.as_str();

    match std::fs::metadata(&vmm.state_dir) {
        Ok(metadata) if metadata.is_dir() => (),
        Ok(_) => {
            return Err(precondition(anyhow::anyhow!(
                "CONNECTOR_VMM_STATE_DIR {} is not a directory",
                vmm.state_dir
            )));
        }
        Err(err) => {
            return Err(precondition(
                anyhow::Error::new(err)
                    .context(format!("CONNECTOR_VMM_STATE_DIR {}", vmm.state_dir)),
            ));
        }
    }

    unless_abandoned(launched_tx, async {
        release::recover(podman, &vmm.state_dir).await;
        Ok(())
    })
    .await?;

    // Ordinary admission's public-plane refusal of Python is not applied:
    // eligibility, which `vmm_for` decided, is narrower than it, and this
    // launch is the protection that refusal stands in for.
    let inspection = unless_abandoned(launched_tx, async {
        if !image.ends_with(":local") {
            crate::container::pull(podman, image, log_sink)
                .await
                .context("pulling image")?;
        }
        crate::container::engine_cmd(podman, &["image", "inspect", image.as_str()])
            .await
            .context("inspecting image")
    })
    .await?;

    let inspected = connector_init::inspect::Image::parse_from_json_slice(&inspection)
        .context("parsing image inspection")?;
    let declarations = crate::image::Declarations::parse(&inspected)?;
    // Eligibility already confines VMMs to approved first-party images. It
    // supplies the sandbox that ordinary public-plane Python admission requires.
    let policy = crate::policy::Image::check(crate::Plane::Private, image)?;
    crate::policy::check_secrets(
        task_name,
        secrets
            .iter()
            .map(|(name, pointer)| (name.as_str(), pointer.as_str())),
        crate::policy::SecretIdentity::Image {
            image,
            repository: policy.repository(),
            declared: &declarations.secrets,
        },
    )?;
    policy.usage_rate(
        declarations.runtime_protocol,
        declarations.declared_usage_rate,
    )?;

    let connector_init = locate_bin::locate_static("flow-connector-init")
        .context(
            "VMM execution runs a static flow-connector-init beside this program or on its PATH",
        )
        .map_err(precondition)?;
    let connector_mounts = connector_mounts(&std::env::temp_dir()).map_err(precondition)?;

    for attempt in 1..=ATTEMPTS {
        let plan = plan::plan(
            vmm,
            &plan::Launch {
                id: rand::random(),
                token: rand::random(),
                eligible,
                image,
                inspection: &inspection,
                connector_mounts: &connector_mounts,
                log_level: *log_level,
                plane: *plane,
                task_name,
                persistent_disk: None,
                egress: egress.as_deref(),
            },
        )
        .map_err(|err| crate::invalid_argument(format!("{err:#}")))?;

        // Each attempt plans the same egress.
        if attempt == 1 {
            log_sink
                .send(crate::build_log(
                    ops::LogLevel::Info,
                    &plan.egress.to_string(),
                    [],
                ))
                .await;
        }

        let ingress_image = (!plan.ingress.is_empty()).then_some(vmm.image.as_str());
        let file = match record::claim(
            &vmm.state_dir,
            &plan.name,
            &record::claim_line(&plan.name, &plan.token, &plan.mount, ingress_image),
        ) {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(precondition(
                    anyhow::Error::new(err).context(format!("claiming {}", plan.record)),
                ));
            }
        };
        let owner = owned.insert(Owned {
            file,
            path: plan.record.clone(),
            record: record::Record {
                name: plan.name.clone(),
                token: plan.token.clone(),
                mount: plan.mount.clone(),
                state: plan.state.clone(),
                created_mount: false,
                created_state: false,
                ingress_image: ingress_image.map(str::to_string),
            },
            container: None,
            attached: None,
            refresh: None,
        });

        if !make_dir(owner, record::Dir::Mount, &plan.mount, 0o711).map_err(precondition)? {
            teardown(input, owned.take()).await;
            continue;
        }
        write_mount(&plan, &connector_init, &inspection).map_err(precondition)?;
        owner.refresh = crate::protocol::start_task_update(
            task_update.as_ref(),
            eligible.task_type,
            task_name,
            build.as_deref(),
            std::path::Path::new(&plan.mount),
            log_sink,
        )
        .await?;

        let (state, mode) = &plan.directories[0];
        if !make_dir(owner, record::Dir::State, state, *mode).map_err(precondition)? {
            teardown(input, owned.take()).await;
            continue;
        }
        prepare(&plan, vmm.disk_mib).map_err(precondition)?;

        // Uncached, immediately before the network it protects, and from the
        // VMM image whose tables the host's must equal.
        unless_abandoned(launched_tx, async {
            crate::container::engine_cmd(podman, &plan.verify)
                .await
                .map(|_stdout| ())
                .map_err(|err| {
                    precondition(err.context(
                        "the host's VMM network boundary did not verify, \
                         so this data plane cannot run VMM connectors",
                    ))
                })
        })
        .await?;

        if let Err(err) = crate::container::engine_cmd(podman, &plan.network).await {
            // Another owner's network or bridge may hold the name.
            if attempt < ATTEMPTS {
                tracing::warn!(
                    error = format!("{err:#}"),
                    name = %plan.name,
                    interface = %plan.interface,
                    "creating a VMM network failed; retrying under a new id"
                );
                teardown(input, owned.take()).await;
                continue;
            }
            return Err(err.context("creating the VMM's network"));
        }
        if launched_tx.is_closed() {
            return Err(abandoned());
        }

        return run_vmm(input, owner, launched_tx, &plan, &inspection).await;
    }
    Err(precondition(anyhow::anyhow!(
        "found no unclaimed VMM id in {ATTEMPTS} attempts beneath {}",
        vmm.state_dir
    )))
}

async fn run_vmm(
    input: &Input,
    owner: &mut Owned,
    launched_tx: &mut LaunchedTx,
    plan: &plan::Plan,
    inspection: &[u8],
) -> anyhow::Result<Launched> {
    let Input {
        vmm,
        image,
        log_sink,
        task_name,
        ..
    } = input;

    if !plan.ingress.is_empty() {
        crate::container::engine_cmd(&vmm.podman, &plan.ingress)
            .await
            .context("installing the VMM host ingress permissions")?;
    }

    crate::container::engine_cmd(&vmm.podman, &plan.rootfs)
        .await
        .context("creating the VMM's private rootfs volume")?;
    if launched_tx.is_closed() {
        return Err(abandoned());
    }

    tracing::debug!(argv = ?plan.create, "creating a VMM's container");

    let created = crate::container::engine_cmd(&vmm.podman, &plan.create)
        .await
        .context("creating the VMM's container")?;
    let created = String::from_utf8_lossy(&created).trim().to_string();
    if created.len() != 64 || !created.bytes().all(|b| b.is_ascii_hexdigit()) {
        anyhow::bail!("podman create printed {created:?}, not a container ID");
    }
    owner.container = Some(created.clone());
    if launched_tx.is_closed() {
        return Err(abandoned());
    }

    let mut child: async_process::Child = crate::container::engine_command(&vmm.podman)
        .args(["start", "--attach", created.as_str()])
        .stdin(async_process::Stdio::null())
        // The guest's console, which libkrun echoes onto stderr when it panics.
        .stdout(async_process::Stdio::null())
        .stderr(async_process::Stdio::piped())
        .spawn()
        .with_context(|| format!("failed to run {} to start the VMM", vmm.podman))?
        .into();

    // The pump still frames connector-init's readiness byte, but anything in
    // the guest may write to its console: only init's health is readiness.
    let (ready_tx, _) = futures::channel::oneshot::channel();
    let pump = tokio::spawn(crate::container::pump_stderr(
        tokio::io::BufReader::new(child.stderr.take().expect("stderr is piped")),
        ready_tx,
        log_sink.clone(),
        image.clone(),
        {
            let quoted_task_name: bytes::Bytes = format!("\"{task_name}\"").into();
            move |log| crate::policy::sanitize_connector_log(&quoted_task_name, log)
        },
    ));
    owner.attached = Some(Attached { child, pump });
    let child = &mut owner.attached.as_mut().expect("attached just now").child;

    // The start client exits with its VMM, which then never becomes ready.
    let channel = unless_abandoned(launched_tx, async {
        tokio::select! {
            biased;
            exited = child.wait() => {
                let status = exited.context("waiting for the VMM's attached start client")?;
                anyhow::bail!(
                    "the VMM exited ({status}) before flow-connector-init answered its \
                     health check; the cause is in the preceding connector logs"
                )
            }
            channel = dial(&plan.socket) => channel,
        }
    })
    .await?;

    let codec = connector_init::inspect::Image::parse_from_json_slice(inspection)
        .context("parsing image inspection for runtime codec")?
        .runtime_codec();
    let inspected = connector_init::inspect::Image::parse_from_json_slice(inspection)?;
    let declarations = crate::image::Declarations::parse(&inspected)?;
    let usage_rate = crate::policy::Image::check(crate::Plane::Private, image)?.usage_rate(
        declarations.runtime_protocol,
        declarations.declared_usage_rate,
    )?;

    let ip_addr = if plan.ingress.is_empty() {
        String::new()
    } else {
        let address = crate::container::engine_cmd(
            &vmm.podman,
            &[
                "inspect",
                "--format",
                "{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                &created,
            ],
        )
        .await
        .context("inspecting the VMM container address")?;
        let address = String::from_utf8(address).context("VMM address is UTF-8")?;
        address
            .trim()
            .parse::<std::net::Ipv4Addr>()
            .context("VMM has an IPv4 address")?
            .to_string()
    };
    let container = runtime::Container {
        ip_addr,
        network_ports: declarations.network_ports,
        usage_rate: usage_rate.value,
        mapped_host_ports: Default::default(),
    };
    log_sink
        .send(crate::build_log(
            ops::LogLevel::Info,
            "started connector container",
            [
                ("image", crate::json_field(image)),
                ("container", crate::json_field(&container)),
                ("vmm", crate::json_field(&plan.name)),
            ],
        ))
        .await;

    Ok(Launched {
        container,
        channel,
        codec,
    })
}

/// Await `future` unless the start awaiting this launch is abandoned first.
/// Only for steps which create nothing the launch would own.
async fn unless_abandoned<T>(
    launched_tx: &mut LaunchedTx,
    future: impl std::future::Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    tokio::select! {
        result = future => result,
        () = launched_tx.closed() => Err(abandoned()),
    }
}

fn abandoned() -> anyhow::Error {
    anyhow::anyhow!("the connector start was abandoned")
}

/// The host socket can precede the guest server. Retry connection and health
/// failures, and dials left unanswered, within one deadline until
/// connector-init reports SERVING.
async fn dial(socket: &str) -> anyhow::Result<tonic::transport::Channel> {
    let endpoint = tonic::transport::Endpoint::from_shared(format!("unix:{socket}"))
        .context("a socket path is an endpoint")?
        .connect_timeout(Duration::from_secs(5))
        .http2_keep_alive_interval(Duration::from_secs(5))
        // As for an ordinary container: the task runtime is single-threaded.
        .keep_alive_timeout(Duration::from_secs(60));

    let mut failed = None;
    let dialed = tokio::time::timeout(READY_TIMEOUT, async {
        loop {
            let attempt = async {
                let channel = endpoint.connect().await.context("connecting")?;
                serving(channel.clone()).await?;
                anyhow::Ok(channel)
            };
            match tokio::time::timeout(ATTEMPT_TIMEOUT, attempt).await {
                Ok(Ok(channel)) => return channel,
                Ok(Err(err)) => failed = Some(err),
                Err(_elapsed) => {
                    failed = Some(anyhow::anyhow!("no answer within {ATTEMPT_TIMEOUT:?}"))
                }
            }
            tokio::time::sleep(DIAL_INTERVAL).await;
        }
    })
    .await;

    dialed.map_err(|_elapsed| {
        let timeout = "timeout waiting for the VMM to become ready";
        match failed {
            Some(err) => err
                .context(format!("the last dial of connector-init at {socket}"))
                .context(timeout),
            None => anyhow::anyhow!(timeout),
        }
    })
}

/// An empty service name checks connector-init itself without running a connector.
async fn serving(channel: tonic::transport::Channel) -> anyhow::Result<()> {
    let response = tonic_health::pb::health_client::HealthClient::new(channel)
        .check(tonic_health::pb::HealthCheckRequest::default())
        .await
        .context("checking health")?;

    match response.into_inner().status() {
        tonic_health::pb::health_check_response::ServingStatus::Serving => Ok(()),
        status => anyhow::bail!("connector-init's health is {}", status.as_str_name()),
    }
}

fn precondition(err: anyhow::Error) -> anyhow::Error {
    proto_grpc::status_to_anyhow(tonic::Status::failed_precondition(format!("{err:#}")))
}

/// The directory of connector mounts of the shared contract, under TMPDIR.
/// Each mount in it is bound read-only at its own path, and both are
/// traversable by all and listable by none, because a connector image
/// commonly runs as an unprivileged UID of its own choosing which must reach
/// the files it's told the names of without enumerating them. It's scoped to
/// this process's user, so that users sharing a TMPDIR don't contend over it.
fn connector_mounts(tmp: &std::path::Path) -> anyhow::Result<String> {
    use std::os::unix::fs::DirBuilderExt;

    // SAFETY: geteuid takes nothing and cannot fail.
    let euid = unsafe { libc::geteuid() };
    let mounts = tmp.join(format!("connector-mounts-{euid}"));

    // Other users may pre-create this name in a shared TMPDIR.
    // Create it without a writable-by-others window for concurrent launches.
    match std::fs::DirBuilder::new().mode(0o711).create(&mounts) {
        Ok(()) => (),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            check_mounts_owner(&mounts, euid)
                .with_context(|| format!("connector mounts directory {}", mounts.display()))?;
        }
        Err(err) => {
            return Err(anyhow::Error::new(err).context(format!("creating {}", mounts.display())));
        }
    }
    set_mode(&mounts, 0o711)?;

    mounts
        .into_os_string()
        .into_string()
        .map_err(|mounts| anyhow::anyhow!("the connector mounts directory {mounts:?} is not UTF-8"))
}

/// The connector mounts directory is this process's own: a real directory, not
/// a link, owned by this process's user, and writable by no one else.
fn check_mounts_owner(path: &std::path::Path, euid: u32) -> anyhow::Result<()> {
    use std::os::unix::fs::MetadataExt;

    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() {
        anyhow::bail!("is not a directory, or is a link to one");
    }
    if metadata.uid() != euid {
        anyhow::bail!(
            "is owned by uid {}, not this process's uid {euid}",
            metadata.uid()
        );
    }
    if metadata.mode() & 0o022 != 0 {
        anyhow::bail!("is writable by a user other than its owner");
    }
    Ok(())
}

/// Make `path` exclusively and mark it in the launch's record as `dir`,
/// before anything is put in it. False if it already exists, so that it isn't
/// the launch's to use or to remove.
fn make_dir(owner: &mut Owned, dir: record::Dir, path: &str, mode: u32) -> anyhow::Result<bool> {
    use std::os::unix::fs::DirBuilderExt;

    match std::fs::DirBuilder::new().mode(mode).create(path) {
        Ok(()) => (),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => return Ok(false),
        Err(err) => return Err(anyhow::Error::new(err).context(format!("creating {path}"))),
    }
    if let Err(err) = record::append(&mut owner.file, &record::created_line(dir)) {
        // Unmarked, no release would remove it: this launch knows it's its own.
        _ = std::fs::remove_dir(path);
        return Err(anyhow::Error::new(err).context(format!("marking {path} in {}", owner.path)));
    }
    match dir {
        record::Dir::Mount => owner.record.created_mount = true,
        record::Dir::State => owner.record.created_state = true,
    }
    // Past the umask.
    set_mode(path, mode)?;
    Ok(true)
}

fn write_mount(
    plan: &plan::Plan,
    connector_init: &std::path::Path,
    inspection: &[u8],
) -> anyhow::Result<()> {
    let [(init, init_mode), (inspect, inspect_mode)] = &plan.mount_files[..] else {
        unreachable!("a plan's mount holds connector-init and the inspection")
    };
    std::fs::copy(connector_init, init)
        .with_context(|| format!("copying {} to {init}", connector_init.display()))?;
    set_mode(init, *init_mode)?;

    write_file(inspect, inspection, *inspect_mode)
}

fn prepare(plan: &plan::Plan, disk_mib: u64) -> anyhow::Result<()> {
    use std::os::unix::fs::DirBuilderExt;

    for (dir, mode) in &plan.directories[1..] {
        std::fs::DirBuilder::new()
            .mode(*mode)
            .create(dir)
            .with_context(|| format!("creating {dir}"))?;
        set_mode(dir, *mode)?;
    }
    write_file(&plan.policy_path, &plan.policy, 0o444)?;
    check_scratch(&plan.scratch, disk_mib)
}

/// The VMM opens its disk `O_TMPFILE` in `scratch` and sizes it, which not
/// every filesystem can do. The length is sparse, so the check allocates
/// nothing, and the file has no name, so it's gone once closed.
fn check_scratch(scratch: &str, disk_mib: u64) -> anyhow::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_TMPFILE | libc::O_EXCL)
        .mode(0o600)
        .open(scratch)
        .with_context(|| format!("opening an O_TMPFILE scratch disk in {scratch}"))?;

    file.set_len(disk_mib << 20)
        .with_context(|| format!("sizing a scratch disk in {scratch} to {disk_mib} MiB"))
}

fn write_file(path: &str, content: &[u8], mode: u32) -> anyhow::Result<()> {
    std::fs::write(path, content).with_context(|| format!("writing {path}"))?;
    set_mode(path, mode)
}

fn set_mode(path: impl AsRef<std::path::Path>, mode: u32) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let path = path.as_ref();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .with_context(|| format!("setting the mode of {}", path.display()))
}

/// Stop the launch's VMM, then release what its record owns. What remains is
/// reported to the session and to this process's log, and its record is kept
/// for a later launch to release.
async fn teardown(input: &Input, owned: Option<Owned>) {
    let Some(Owned {
        file: _held,
        path,
        record,
        container,
        attached,
        refresh,
    }) = owned
    else {
        return;
    };
    let podman = input.vmm.podman.as_str();

    std::mem::drop(refresh);
    stop(podman, container.as_deref(), attached).await;

    let Err(failures) = release::release(podman, &input.vmm.state_dir, &path, &record).await else {
        return;
    };
    for (resource, error) in failures {
        tracing::error!(%resource, %error, record = %path, "failed to release a VMM resource");
        input
            .log_sink
            .send(crate::build_log(
                ops::LogLevel::Error,
                "failed to release a VMM resource",
                [
                    ("resource", crate::json_field(&resource)),
                    ("error", crate::json_field(&error)),
                    ("record", crate::json_field(&path)),
                ],
            ))
            .await;
    }
}

/// Stop the launch's container, if `create` made one, and see its start
/// client and stderr out. Removing the container is what stops it: a killed
/// client leaves its container running, and a client run through sudo
/// outlives a kill of sudo. A removal which fails here is retried, and
/// reported, by the release which follows.
async fn stop(podman: &str, container: Option<&str>, attached: Option<Attached>) {
    if let Some(id) = container {
        _ = release::command(podman, &["rm", "--force", "--time=0", "--ignore", id]).await;
    }
    let Some(Attached {
        mut child,
        mut pump,
    }) = attached
    else {
        return;
    };

    // The client's stderr closes as it exits, ending the pump. Neither may
    // outlast teardown, which the session is waiting on.
    if tokio::time::timeout(START_EXIT_TIMEOUT, child.wait())
        .await
        .is_err()
        || tokio::time::timeout(PUMP_TIMEOUT, &mut pump).await.is_err()
    {
        pump.abort();
    }
    std::mem::drop(child); // Killed, if it's still running: it creates nothing.
}

// The FIFO harness relies on Linux read-write semantics.
#[cfg(all(test, target_os = "linux"))]
mod readiness_tests;

#[cfg(test)]
mod test {
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;

    /// Set fixture modes explicitly so cases do not depend on the umask.
    #[test]
    fn connector_mounts() {
        // SAFETY: geteuid takes nothing and cannot fail.
        let name = format!("connector-mounts-{}", unsafe { libc::geteuid() });
        let dir = |mode: u32| {
            move |path: &Path| {
                std::fs::create_dir(path).unwrap();
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
            }
        };
        let link = move |path: &Path| {
            let target = path.with_extension("target");
            dir(0o711)(&target);
            std::os::unix::fs::symlink(&target, path).unwrap();
        };
        let dangling = |path: &Path| std::os::unix::fs::symlink("absent", path).unwrap();
        let file = |path: &Path| {
            std::fs::write(path, "").unwrap();
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o644)).unwrap();
        };

        let cases: Vec<(&str, Box<dyn Fn(&Path)>)> = vec![
            ("nothing", Box::new(|_: &Path| ())),
            ("a directory of mode 0700", Box::new(dir(0o700))),
            ("a directory of mode 0755", Box::new(dir(0o755))),
            ("a directory of mode 0775", Box::new(dir(0o775))),
            ("a directory of mode 0757", Box::new(dir(0o757))),
            ("a link to a directory of mode 0711", Box::new(link)),
            ("a dangling link", Box::new(dangling)),
            ("a file", Box::new(file)),
        ];

        let mut table = String::new();
        for (label, plant) in cases {
            let tmp = tempfile::tempdir().unwrap();
            let path = tmp.path().join(&name);
            plant(&path);

            let outcome = match super::connector_mounts(tmp.path()) {
                Ok(mounts) => {
                    assert_eq!(Path::new(&mounts), path);
                    "taken".to_string()
                }
                Err(err) => format!("refused: {err:#}"),
            };
            let mode = std::fs::metadata(&path).map_or("none".to_string(), |m| {
                format!("{:o}", m.permissions().mode() & 0o7777)
            });
            table.push_str(&format!("{label}: {outcome}; mode {mode}\n"));
            table = table
                .replace(tmp.path().to_str().unwrap(), "$TMPDIR")
                .replace(&name, "connector-mounts-$EUID");
        }
        insta::assert_snapshot!(table, @r"
        nothing: taken; mode 711
        a directory of mode 0700: taken; mode 711
        a directory of mode 0755: taken; mode 711
        a directory of mode 0775: refused: connector mounts directory $TMPDIR/connector-mounts-$EUID: is writable by a user other than its owner; mode 775
        a directory of mode 0757: refused: connector mounts directory $TMPDIR/connector-mounts-$EUID: is writable by a user other than its owner; mode 757
        a link to a directory of mode 0711: refused: connector mounts directory $TMPDIR/connector-mounts-$EUID: is not a directory, or is a link to one; mode 711
        a dangling link: refused: connector mounts directory $TMPDIR/connector-mounts-$EUID: is not a directory, or is a link to one; mode none
        a file: refused: connector mounts directory $TMPDIR/connector-mounts-$EUID: is not a directory, or is a link to one; mode 644
        ");
    }
}

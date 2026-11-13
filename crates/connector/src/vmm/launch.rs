//! The IO of one VMM launch, from an admitted connector start to a dialed
//! connector-init: pull and inspect the connector's image with the VMM's
//! podman, claim the launch's ownership record, prepare its connector mount
//! and `fv_<id>` state, verify the host boundary, create the VMM's network,
//! volume and container, wait for connector-init's stderr readiness byte,
//! and dial `init.sock`. A launch recovers nothing of others': `recovery` does.
//!
//! One spawned task owns everything a launch creates, from its claim to its
//! release, and holds the record's lock throughout (see `record`). The
//! commands which create the network, volume and container are never
//! interrupted, so each client has exited before teardown looks for what it
//! made. `release::recover` handles creates that finish later in the podman service.
//! The task holds a clone of the session's log sink, so the session
//! ends only once its teardown has. `drain` awaits every such task.

use super::{Eligible, TaskEgress, Vmm, plan, record, release};
use anyhow::Context;
use proto_flow::runtime;
use std::time::Duration;

/// The deadline for connector-init's readiness byte and the following dial.
const READY_TIMEOUT: Duration = Duration::from_secs(60);
/// How long teardown waits for podman's attached start client to leave, once
/// its container is removed.
const START_EXIT_TIMEOUT: Duration = Duration::from_secs(10);
const PUMP_TIMEOUT: Duration = Duration::from_secs(5);

/// Every launch's task in this process, for `drain`.
static LAUNCHES: std::sync::LazyLock<tokio_util::task::TaskTracker> =
    std::sync::LazyLock::new(tokio_util::task::TaskTracker::new);

/// Wait at most `timeout` for every launch's task to end, returning whether
/// they did. Launches which start meanwhile are waited for too.
pub(super) async fn drain(timeout: Duration) -> bool {
    LAUNCHES.close();
    tokio::time::timeout(timeout, LAUNCHES.wait()).await.is_ok()
}

/// Ties a VMM to the served stream: dropping it tells the launch's task to
/// tear the VMM down.
pub(crate) struct Guard {
    _stop: tokio::sync::oneshot::Sender<()>,
}

struct Input {
    vmm: Vmm,
    eligible: &'static Eligible,
    egress: TaskEgress,
    /// TCP ports of the task's built spec.
    ports: Vec<u32>,
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
    egress: TaskEgress,
    ports: Vec<u32>,
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
        ports,
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
    LAUNCHES.spawn(lifecycle(input, launched_tx, stop_rx));

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
        ports,
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

    check_state_owner(std::path::Path::new(&vmm.state_dir))
        .with_context(|| format!("CONNECTOR_VMM_STATE_DIR {}", vmm.state_dir))
        .map_err(precondition)?;

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
    let vmm_image = unless_abandoned(launched_tx, async {
        vmm_image_id(podman, &vmm.image, log_sink)
            .await
            .map_err(precondition)
    })
    .await?;

    let plan = plan::plan(
        vmm,
        &plan::Launch {
            id: rand::random(),
            token: rand::random(),
            eligible,
            image,
            vmm_image: &vmm_image,
            inspection: &inspection,
            connector_mounts: &connector_mounts,
            log_level: *log_level,
            plane: *plane,
            task_name,
            persistent_disk: None,
            egress,
            ports,
        },
    )
    .map_err(|err| crate::invalid_argument(format!("{err:#}")))?;

    log_sink
        .send(crate::build_log(
            ops::LogLevel::Info,
            &plan.egress.to_string(),
            [],
        ))
        .await;

    let ingress_image = (!plan.ingress.is_empty()).then_some(vmm_image.as_str());
    let file = record::claim(
        &vmm.state_dir,
        &plan.name,
        &record::claim_line(&plan.name, &plan.token, Some(&plan.mount), ingress_image),
    )
    .with_context(|| format!("claiming VMM ownership record {}", plan.record))
    .map_err(precondition)?;
    let owner = owned.insert(Owned {
        file,
        path: plan.record.clone(),
        record: record::Record {
            name: plan.name.clone(),
            token: plan.token.clone(),
            mount: Some(plan.mount.clone()),
            state: plan.state.clone(),
            created_mount: false,
            created_state: false,
            ingress_image: ingress_image.map(str::to_string),
        },
        container: None,
        attached: None,
        refresh: None,
    });

    make_dir(owner, record::Dir::Mount, &plan.mount, 0o711)
        .with_context(|| format!("claiming connector mount {}", plan.mount))
        .map_err(precondition)?;
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
    make_dir(owner, record::Dir::State, state, *mode)
        .with_context(|| format!("claiming VMM state directory {state}"))
        .map_err(precondition)?;
    prepare(&plan, vmm.disk_mib).map_err(precondition)?;

    // Uncached, immediately before the network it protects, and from the
    // VMM image whose tables the host's must equal.
    unless_abandoned(launched_tx, async {
        crate::container::engine_cmd(podman, &plan.verify)
            .await
            .map_err(|err| {
                precondition(err.context(
                    "the host's VMM network boundary did not verify, \
                     so this data plane cannot run VMM connectors",
                ))
            })
    })
    .await?;

    crate::container::engine_cmd(podman, &plan.network)
        .await
        .with_context(|| {
            format!(
                "creating the VMM network {} on bridge {}",
                plan.name, plan.interface
            )
        })?;
    if launched_tx.is_closed() {
        return Err(abandoned());
    }

    run_vmm(input, owner, launched_tx, &plan, &inspection).await
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
            .context("installing the VMM host permissions")?;
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
        .stdout(async_process::Stdio::null())
        .stderr(async_process::Stdio::piped())
        .spawn()
        .with_context(|| format!("failed to run {} to start the VMM", vmm.podman))?
        .into();

    let (ready_tx, ready_rx) = futures::channel::oneshot::channel();
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
                    "the VMM exited ({status}) before flow-connector-init was ready; \
                     the cause is in the preceding connector logs"
                )
            }
            channel = dial(&plan.socket, ready_rx) => channel,
        }
    })
    .await?;

    let inspected = connector_init::inspect::Image::parse_from_json_slice(inspection)
        .context("parsing image inspection for runtime codec")?;
    let codec = inspected.runtime_codec();
    let declarations = crate::image::Declarations::parse(&inspected)?;
    let usage_rate = crate::policy::Image::check(crate::Plane::Private, image)?.usage_rate(
        declarations.runtime_protocol,
        declarations.declared_usage_rate,
    )?;

    let ip_addr = if plan.ports.is_empty() {
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
        advertised_ports: declarations.advertised_ports,
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

/// The ID of the VMM image `image` names, pulling it only if it's missing, as
/// `podman run` would. A launch verifies, grants and runs this one image, and
/// records it for its grant's revoke: a tag moved meanwhile cannot verify
/// one image and launch another.
async fn vmm_image_id(
    podman: &str,
    image: &str,
    log_sink: &crate::LogSink,
) -> anyhow::Result<String> {
    let args = ["image", "inspect", "--format", "{{.Id}}", image];
    let id = match crate::container::engine_cmd(podman, &args).await {
        Ok(id) => id,
        Err(_missing) => {
            crate::container::pull(podman, image, log_sink)
                .await
                .with_context(|| format!("pulling VMM image {image}"))?;
            crate::container::engine_cmd(podman, &args)
                .await
                .with_context(|| format!("inspecting VMM image {image}"))?
        }
    };
    let id = String::from_utf8_lossy(&id).trim().to_string();
    if id.len() != 64 || !id.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        anyhow::bail!("VMM image {image} inspected as ID {id:?}");
    }
    Ok(id)
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

/// Waiting for the guest listener avoids libkrun holding a pre-listen dial
/// until its reaper closes it, consuming connector-init's idle window.
async fn dial(
    socket: &str,
    ready_rx: futures::channel::oneshot::Receiver<()>,
) -> anyhow::Result<tonic::transport::Channel> {
    tokio::time::timeout(READY_TIMEOUT, async {
        ready_rx.await.map_err(|_| {
            anyhow::anyhow!(
                "the VMM closed stderr before flow-connector-init started; \
                 the cause is in the preceding connector logs"
            )
        })?;

        tonic::transport::Endpoint::from_shared(format!("unix:{socket}"))
            .context("a socket path is an endpoint")?
            .connect_timeout(Duration::from_secs(5))
            .http2_keep_alive_interval(Duration::from_secs(5))
            // As for an ordinary container: the task runtime is single-threaded.
            .keep_alive_timeout(Duration::from_secs(60))
            .connect()
            .await
            .with_context(|| format!("failed to connect to VMM connector-init at {socket}"))
    })
    .await
    .context("timeout waiting for the VMM to become ready")?
}

/// This host cannot run VMMs, so it refuses the VMM execution requested of it.
fn precondition(err: anyhow::Error) -> anyhow::Error {
    super::refuse(tonic::Status::failed_precondition(format!("{err:#}")))
}

fn connector_mounts(tmp: &std::path::Path) -> anyhow::Result<String> {
    crate::protocol::connector_mounts(tmp)?
        .into_os_string()
        .into_string()
        .map_err(|mounts| anyhow::anyhow!("the connector mounts directory {mounts:?} is not UTF-8"))
}

pub(super) fn check_state_owner(path: &std::path::Path) -> anyhow::Result<()> {
    use std::os::unix::fs::MetadataExt;

    let metadata =
        crate::protocol::check_directory_owner(path, rustix::process::geteuid().as_raw())?;
    if metadata.mode() & 0o777 != 0o700 {
        anyhow::bail!("must have mode 0700, private to the launcher's user");
    }
    Ok(())
}

/// Make `path` exclusively and mark it in the launch's record as `dir`,
/// before anything is put in it. An existing path is never the launch's to
/// use or to remove.
fn make_dir(owner: &mut Owned, dir: record::Dir, path: &str, mode: u32) -> anyhow::Result<()> {
    use std::os::unix::fs::DirBuilderExt;

    std::fs::DirBuilder::new()
        .mode(mode)
        .create(path)
        .with_context(|| format!("creating {path}"))?;
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
    Ok(())
}

fn write_mount(
    plan: &plan::Plan,
    connector_init: &std::path::Path,
    inspection: &[u8],
) -> anyhow::Result<()> {
    let (init, init_mode) = &plan.connector_init;
    let (inspect, inspect_mode) = &plan.image_inspect;
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
/// for recovery to release.
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
    use std::time::Duration;

    /// `drain` waits for every launch's task, those started meanwhile too,
    /// and no longer than its timeout.
    #[tokio::test(start_paused = true)]
    async fn drain_awaits_launches_within_its_bound() {
        let (end_tx, end_rx) = tokio::sync::oneshot::channel::<()>();
        super::LAUNCHES.spawn(async move { _ = end_rx.await });

        let began = tokio::time::Instant::now();
        assert!(!super::drain(Duration::from_secs(30)).await);
        assert_eq!(began.elapsed(), Duration::from_secs(30), "bounded");

        super::LAUNCHES.spawn(tokio::time::sleep(Duration::from_secs(5)));
        std::mem::drop(end_tx);
        let began = tokio::time::Instant::now();
        assert!(super::drain(Duration::from_secs(30)).await);
        assert_eq!(
            began.elapsed(),
            Duration::from_secs(5),
            "until the last ended"
        );
    }

    #[test]
    fn state_directory_is_private() {
        let tmp = tempfile::tempdir().unwrap();
        let mut outcomes = Vec::new();
        for mode in [0o500, 0o700, 0o711, 0o750, 0o755, 0o770, 0o777] {
            std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(mode)).unwrap();
            outcomes.push((
                format!("{mode:04o}"),
                super::check_state_owner(tmp.path()).map_err(|err| err.to_string()),
            ));
        }
        let link = tmp.path().join("link");
        std::os::unix::fs::symlink(tmp.path(), &link).unwrap();
        outcomes.push((
            "symlink".to_string(),
            super::check_state_owner(&link).map_err(|err| err.to_string()),
        ));
        let other_uid = rustix::process::geteuid().as_raw() ^ 1;
        assert!(crate::protocol::check_directory_owner(tmp.path(), other_uid).is_err());
        insta::assert_debug_snapshot!(outcomes);
    }

    /// Set fixture modes explicitly so cases do not depend on the umask.
    #[test]
    fn connector_mounts() {
        let name = format!("connector-mounts-{}", rustix::process::geteuid().as_raw());
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
        nothing: taken; mode 700
        a directory of mode 0700: taken; mode 700
        a directory of mode 0755: taken; mode 700
        a directory of mode 0775: refused: connector mounts directory $TMPDIR/connector-mounts-$EUID: is writable by a user other than its owner; mode 775
        a directory of mode 0757: refused: connector mounts directory $TMPDIR/connector-mounts-$EUID: is writable by a user other than its owner; mode 757
        a link to a directory of mode 0711: refused: connector mounts directory $TMPDIR/connector-mounts-$EUID: is not a directory, or is a link to one; mode 711
        a dangling link: refused: connector mounts directory $TMPDIR/connector-mounts-$EUID: is not a directory, or is a link to one; mode none
        a file: refused: connector mounts directory $TMPDIR/connector-mounts-$EUID: is not a directory, or is a link to one; mode 644
        ");
    }
}

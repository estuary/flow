//! Launch line for the libkrun sandbox spike: `container::start` delegates here
//! when `FLOW_SANDBOX_SPIKE_POLICY` is set. The connector image is then not run
//! directly. It becomes the root filesystem of a micro-VM inside a helper
//! container, and `flow-connector-init` is reached over the Unix socket that
//! libkrun proxies to the guest's vsock port, rather than over TCP.
//!
//! Everything else -- the readiness protocol, the log decoder, the codec, and
//! the transaction driving above it -- is the unmodified runtime.
//!
//! Nothing here merges to master. `spike/CONTRACTS.md` is the source of truth
//! for the helper CLI, the environment variables, and the layout of the
//! per-connector reactor directory.

use anyhow::Context;
use proto_flow::runtime;

// Address the helper assigns the guest on its tap network. Reported for the
// runtime's own bookkeeping; nothing in the spike dials it.
const GUEST_IP: &str = "192.0.2.2";

/// Settings of a spike launch, read from the environment. `from_env` yields
/// `None` -- and the runtime is unchanged -- unless a policy path is set.
pub struct Settings {
    policy: std::path::PathBuf,
    helper_image: String,
    reactor_dir: std::path::PathBuf,
    memory_mib: u32,
    memory_overhead_mib: u32,
    vcpus: u32,
    disk_mib: u32,
    venv_dir: Option<std::path::PathBuf>,
    deps_image: Option<String>,
    deps_fstype: String,
    helper_args: Vec<String>,
}

impl Settings {
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        let Ok(policy) = std::env::var("FLOW_SANDBOX_SPIKE_POLICY") else {
            return Ok(None);
        };

        Ok(Some(Self {
            policy: policy.into(),
            helper_image: var_or(
                "FLOW_SANDBOX_SPIKE_HELPER_IMAGE",
                "localhost/flow-sandbox-helper:spike",
            ),
            reactor_dir: var_or(
                "FLOW_SANDBOX_SPIKE_REACTOR_DIR",
                "/var/tmp/flow-spike/reactor",
            )
            .into(),
            memory_mib: var_parse("FLOW_SANDBOX_SPIKE_MEMORY_MIB", 1024)?,
            memory_overhead_mib: var_parse("FLOW_SANDBOX_SPIKE_MEMORY_OVERHEAD_MIB", 256)?,
            vcpus: var_parse("FLOW_SANDBOX_SPIKE_VCPUS", 2)?,
            disk_mib: var_parse("FLOW_SANDBOX_SPIKE_DISK_MIB", 4096)?,
            venv_dir: std::env::var("FLOW_SANDBOX_SPIKE_VENV_DIR")
                .ok()
                .map(Into::into),
            deps_image: std::env::var("FLOW_SANDBOX_SPIKE_DEPS_IMAGE").ok(),
            deps_fstype: var_or("FLOW_SANDBOX_SPIKE_DEPS_FSTYPE", "ext4"),
            helper_args: var_or("FLOW_SANDBOX_SPIKE_HELPER_ARGS", "")
                .split_whitespace()
                .map(str::to_string)
                .collect(),
        }))
    }
}

/// Start the helper container for `image` and dial the connector-init it runs
/// inside the guest. Mirrors `container::start`, which delegates here.
pub async fn start<L: crate::Logger>(
    settings: Settings,
    image: &str,
    logger: L,
    log_level: ops::LogLevel,
    network: &str,
    task_name: &str,
    task_type: ops::TaskType,
) -> anyhow::Result<(
    runtime::Container,
    tonic::transport::Channel,
    super::Guard<L>,
    connector_init::Codec,
)> {
    let network = if network.is_empty() {
        "bridge"
    } else {
        network
    };
    let log_level = log_level.or(ops::LogLevel::Warn);

    // `<id>` of the per-connector reactor directory is this container's name.
    // It must be unique and must never be reused: a stale `sock/init.sock`
    // makes libkrun fail with EEXIST, and the helper does not unlink files it
    // does not own.
    let name = format!("fs_{:016x}", rand::random::<u64>());
    let dir = settings.reactor_dir.join(&name);

    let (init_dir, sock_dir, scratch_dir) =
        (dir.join("init"), dir.join("sock"), dir.join("scratch"));
    for sub in [&init_dir, &sock_dir, &scratch_dir] {
        tokio::fs::create_dir_all(sub)
            .await
            .with_context(|| format!("creating {}", sub.display()))?;
    }
    // Taken before anything else can fail, so an early return still cleans up.
    let dir_guard = DirGuard(dir.clone());

    // The helper wants a `/venv` even when the experiment has nothing to put
    // in it, so an empty directory stands in.
    let venv_dir = match settings.venv_dir {
        Some(venv_dir) => venv_dir,
        None => {
            let venv_dir = dir.join("venv");
            tokio::fs::create_dir_all(&venv_dir)
                .await
                .with_context(|| format!("creating {}", venv_dir.display()))?;
            venv_dir
        }
    };

    let connector_init_path = init_dir.join("flow-connector-init");
    let image_inspect_path = init_dir.join("image-inspect.json");

    let ((), (image_inspection, codec)) = futures::try_join!(
        super::find_connector_init_and_copy(&connector_init_path),
        super::inspect_image_and_copy(image, &image_inspect_path, &logger),
    )?;

    tokio::fs::copy(&settings.policy, init_dir.join("policy.json"))
        .await
        .with_context(|| format!("copying policy {}", settings.policy.display()))?;

    // The guest workload runs as the connector image's user, which is not ours.
    // The unmodified path gets this from a mode set on its temporary.
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(&image_inspect_path, std::fs::Permissions::from_mode(0o644))
            .await
            .context("setting mode of image-inspect.json")?;
    }

    let mut docker_args = vec![
        "run".to_string(),
        "--rm".to_string(),
        format!("--name={name}"),
        format!("--network={network}"),
        "--log-driver=none".to_string(),
        // The helper needs KVM for the guest, a tun device for the tap that
        // carries the guest's traffic, and NET_ADMIN to configure and firewall
        // it. ip_forward routes the tap out of the container's own veth.
        "--device".to_string(),
        "/dev/kvm".to_string(),
        "--device".to_string(),
        "/dev/net/tun".to_string(),
        "--cap-add".to_string(),
        "NET_ADMIN".to_string(),
        "--sysctl".to_string(),
        "net.ipv4.ip_forward=1".to_string(),
        // Strict reverse-path filtering on the tap, which does not exist yet at
        // container creation and cannot be set afterwards (/proc/sys is
        // read-only inside the container), so `conf.default` is the only route
        // to it. It drops a forged guest source before nftables is reached; the
        // anti-spoof rule stays as the second control (WP07).
        "--sysctl".to_string(),
        "net.ipv4.conf.default.rp_filter=1".to_string(),
        // Thread-through the logging configuration of the connector.
        "--env=LOG_FORMAT=json".to_string(),
        format!("--env=LOG_LEVEL={}", log_level.as_str_name()),
        // The cgroup holds the guest's RAM plus what the helper itself needs
        // outside of it: libkrun, the virtiofs threads, and the page cache of
        // the guest's root filesystem.
        "--memory".to_string(),
        format!("{}m", settings.memory_mib + settings.memory_overhead_mib),
        "--cpus".to_string(),
        settings.vcpus.to_string(),
        format!("--label=image={image}"),
        format!("--label=task-name={task_name}"),
        format!("--label=task-type={}", task_type.as_str_name()),
        // The connector image is the guest's root filesystem, served from
        // podman's per-container writable layer of it. Nothing overlays it
        // inside the guest, so the root is writable exactly as a container's is.
        format!("--mount=type=image,source={image},destination=/rootfs,rw=true"),
        format!(
            "--mount=type=bind,source={},target=/init,ro",
            init_dir.display()
        ),
        format!(
            "--mount=type=bind,source={},target=/venv,ro",
            venv_dir.display()
        ),
        format!(
            "--mount=type=bind,source={},target=/sock",
            sock_dir.display()
        ),
        format!(
            "--mount=type=bind,source={},target=/scratch-backing",
            scratch_dir.display()
        ),
    ];

    if let Ok(cgroup_parent) = std::env::var("CONNECTOR_CGROUP_PARENT") {
        docker_args.append(&mut vec!["--cgroup-parent".to_string(), cgroup_parent]);
    }

    let mut helper_args = vec![
        "--policy".to_string(),
        "/init/policy.json".to_string(),
        "--memory-mib".to_string(),
        settings.memory_mib.to_string(),
        "--vcpus".to_string(),
        settings.vcpus.to_string(),
        "--disk-mib".to_string(),
        settings.disk_mib.to_string(),
    ];

    if let Some(deps_image) = &settings.deps_image {
        docker_args.push(format!(
            "--mount=type=bind,source={deps_image},target=/deps.img,ro"
        ));
        helper_args.append(&mut vec![
            "--deps-image".to_string(),
            "/deps.img".to_string(),
            "--deps-fstype".to_string(),
            settings.deps_fstype,
        ]);
    }

    docker_args.push(settings.helper_image);
    docker_args.append(&mut helper_args);
    docker_args.extend(settings.helper_args);

    let process = super::spawn_and_await_ready(docker_args, task_name, &logger).await?;

    // libkrun listens on this socket and proxies it to the guest's vsock port,
    // so it exists from before the VM starts. The endpoint's authority is
    // required by tonic and never used.
    let sock_path = sock_dir.join("init.sock");
    let channel = tonic::transport::Endpoint::try_from("http://[::]:50051")
        .expect("formatting endpoint address")
        .http2_keep_alive_interval(std::time::Duration::from_secs(5))
        // Default is 20s. The task runtime is single-threaded and hyper checks
        // this timer before reading the pending PONG, so a long synchronous
        // stretch of shard work would otherwise be misread as a dead peer.
        .keep_alive_timeout(std::time::Duration::from_secs(60))
        .connect_with_connector(tower::service_fn({
            let sock_path = sock_path.clone();
            move |_| {
                let sock_path = sock_path.clone();
                async move {
                    let stream = tokio::net::UnixStream::connect(sock_path).await?;
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                }
            }
        }))
        .await
        .with_context(|| {
            format!(
                "failed to connect to helper connector-init at {}",
                sock_path.display()
            )
        })?;

    tracing::debug!(
        %image,
        init_sock = %sock_path.display(),
        image_inspection = ?ops::DebugJson(&image_inspection),
        ?codec,
        %task_name,
        ?task_type,
        "dialed sandbox helper"
    );

    // The guest's ports are not published anywhere: the spike exercises the
    // control channel only.
    let container = runtime::Container {
        ip_addr: GUEST_IP.to_string(),
        network_ports: Vec::new(),
        usage_rate: image_inspection.usage_rate,
        mapped_host_ports: Default::default(),
    };
    logger.event(crate::LogEvent::ContainerStarted {
        image,
        container: &container,
    });

    Ok((
        container,
        channel,
        super::Guard {
            _tmp_connector_init: None,
            _tmp_docker_inspect: None,
            _process: process,
            _spike_dir: Some(dir_guard),
            image: image.to_string(),
            logger,
        },
        codec,
    ))
}

/// Owns `<reactor_dir>/<id>` and removes it on drop. Best effort: `Child::drop`
/// only signals the helper, so the removal can race its exit. The bind mounts
/// pin the inodes until it does exit, which is why racing is harmless.
pub struct DirGuard(std::path::PathBuf);

impl Drop for DirGuard {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.0) {
            tracing::warn!(dir = %self.0.display(), %error, "failed to remove reactor directory");
        }
    }
}

fn var_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn var_parse(key: &str, default: u32) -> anyhow::Result<u32> {
    match std::env::var(key) {
        Err(_) => Ok(default),
        Ok(value) => value.parse().with_context(|| format!("parsing {key}")),
    }
}

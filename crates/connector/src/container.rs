//! Docker and Podman image mechanics: pull, inspect, run, dial
//! `flow-connector-init`, and tear down.
use crate::LogSink;
use anyhow::Context;
use futures::FutureExt;
use futures::channel::oneshot;
use std::collections::BTreeMap;
use tokio::io::AsyncBufReadExt;

// Port on which flow-connector-init listens for requests.
// This is its default, made explicit here.
const CONNECTOR_INIT_PORT: u16 = models::CONNECTOR_INIT_PORT;

// For now, we support only Linux amd64 connectors. `docker pull` must request
// it explicitly: under Docker's containerd image store a platform-less pull
// fetches only the host's variant, which `docker run --platform` cannot use.
pub(crate) const CONNECTOR_PLATFORM: &str = "linux/amd64";

// `flow-connector-init` is extracted from this image when a locally-built copy
// isn't found by `locate_bin` (dev/CI builds place one alongside the executable).
// TODO(johnny): Consider better packaging and versioning of `flow-connector-init`.
const CONNECTOR_INIT_IMAGE: &str = "ghcr.io/estuary/reactor:v0.6.15-15-g6954c5551";
const CONNECTOR_INIT_IMAGE_PATH: &str = "/usr/local/bin/flow-connector-init";

/// Bytes which a connector container may write to stderr over its lifetime.
/// Its expected writers, which [`pump_stderr`] lists, write little.
pub(crate) const STDERR_BUDGET: usize = 16 * 1024;

/// Options which have already been selected for a container execution.
pub(crate) struct RunParams {
    pub env: BTreeMap<String, String>,
    pub host_gateway_names: Vec<String>,
    pub labels: BTreeMap<String, String>,
    pub mount: std::path::PathBuf,
    pub network: String,
    /// Publish the `flow-connector-init` port and each of `network_ports`?
    pub publish_ports: bool,
    /// Network ports of the task's built spec.
    pub network_ports: Vec<u32>,
}

/// Lower-level facts of a running image connector.
pub(crate) struct RunningContainer {
    pub channel: tonic::transport::Channel,
    pub ip_addr: std::net::IpAddr,
    pub mapped_host_ports: BTreeMap<u32, String>,
    pub process: async_process::Child,
    /// See [`crate::Transport`].
    pub stderr_overflow: futures::future::BoxFuture<'static, anyhow::Error>,
}

/// A pulled and inspected image, and the capability to run it once.
pub(crate) struct ImageInspection {
    pub image: String,
    pub inspection: connector_init::inspect::Image,
    inspect_json: Vec<u8>,
}

/// Run an inspected image. The original mutable reference is intentionally
/// retained for compatibility across Docker and Podman; pinning the run to
/// the inspected image identity is a separate concern.
pub(crate) async fn run(
    inspection: ImageInspection,
    params: RunParams,
) -> anyhow::Result<RunningContainer> {
    let ImageInspection {
        image,
        inspect_json,
        ..
    } = inspection;
    let RunParams {
        env,
        host_gateway_names,
        labels,
        mount,
        network,
        publish_ports,
        network_ports,
    } = params;

    // The mount is bind-mounted at its own path, so container-side paths are
    // host-side paths and there is no fixed location anywhere.
    let mount = mount
        .to_str()
        .context("connector mount path is not valid UTF-8")?;
    let connector_init_path = format!("{mount}/flow-connector-init");
    let image_inspect_path = format!("{mount}/image-inspect.json");

    let ((), ()) = futures::try_join!(
        async {
            find_connector_init_and_copy(std::path::Path::new(&connector_init_path)).await?;
            crate::protocol::set_mount_mode(std::path::Path::new(&connector_init_path), 0o555)
        },
        async {
            tokio::fs::write(&image_inspect_path, &inspect_json)
                .await
                .context("writing docker inspect output")?;
            crate::protocol::set_mount_mode(std::path::Path::new(&image_inspect_path), 0o444)
        },
    )?;

    // This is default `docker run` behavior if --network is not provided.
    let network = if network.is_empty() {
        "bridge"
    } else {
        network.as_str()
    };
    // Generate a unique name for this container instance.
    let name = unique_container_name();

    let mut docker_args = vec![
        "run".to_string(),
        // Remove the docker container upon its exit.
        "--rm".to_string(),
        // Addressable name of this connector.
        format!("--name={name}"),
        // Network to which the container should attach.
        format!("--network={network}"),
        // The entrypoint into a connector is always flow-connector-init,
        // which will delegate to the actual entrypoint of the connector.
        format!("--entrypoint={connector_init_path}"),
        // Disable logging of connector containers.
        "--log-driver=none".to_string(),
        // The connector mount, at the same absolute path it has on the host.
        // Read-only: it is a host-to-connector channel, and future writable
        // areas will be nested mounts within it rather than a relaxation here.
        format!("--mount=type=bind,source={mount},target={mount},readonly"),
        // Cgroup memory / CPU resource limits.
        "--memory".to_string(),
        connector_memory_limit(),
        "--cpus".to_string(),
        connector_cpu_limit(),
        format!("--platform={CONNECTOR_PLATFORM}"),
    ];

    for (name, value) in env {
        docker_args.push(format!("--env={name}={value}"));
    }
    for name in host_gateway_names {
        docker_args.push(format!("--add-host={name}:host-gateway"));
    }
    for (name, value) in labels {
        docker_args.push(format!("--label={name}={value}"));
    }

    // When running locally, we publish ports so that connectors are accessible
    // on the host from Windows and MacOS (e.x. Docker Desktop).
    if publish_ports {
        // Support Docker Desktop in non-production contexts (for example, `flowctl`)
        // where the container IP is not directly addressable. As an alternative,
        // we ask Docker to provide mapped host ports, which are reported as
        // `mapped_host_ports` of the attached runtime::Container.
        // An empty host port asks Docker to assign a free ephemeral port as
        // the container starts, so it cannot race host-port allocations of
        // other starting containers.
        for port in std::iter::once(CONNECTOR_INIT_PORT as u32).chain(network_ports) {
            docker_args.push(format!("--publish=0.0.0.0::{port}"));
        }
    }

    if let Some(cgroup_parent) = std::env::var("CONNECTOR_CGROUP_PARENT").ok() {
        docker_args.append(&mut vec!["--cgroup-parent".to_string(), cgroup_parent]);
    }

    docker_args.append(&mut vec![
        // Image to run.
        image.clone(),
        // The following are arguments of flow-connector-init, not docker.
        format!("--image-inspect-json-path={image_inspect_path}"),
        format!("--port={CONNECTOR_INIT_PORT}"),
    ]);

    tracing::debug!(docker_args=?docker_args, "invoking docker");

    let mut process: async_process::Child = engine_command(&docker_cli())
        .args(docker_args)
        .stdin(async_process::Stdio::null())
        .stdout(async_process::Stdio::null())
        .stderr(async_process::Stdio::piped())
        .spawn()
        .context("failed to docker run the connector")?
        .into();

    // We've started the container and will need to inspect for its IP address.
    // Docker has unfortunate race handling and will happily return an empty IPAddress for
    // a created or even a running container while it's still performing background setup.
    // The only reliable way to determine if the container is "ready" is to wait for
    // our inner flow-connector-init process to produce its startup log.
    let (ready_tx, ready_rx) = oneshot::channel::<()>();
    let (overflow_tx, mut overflow_rx) = oneshot::channel::<()>();

    let stderr = process.stderr.take().unwrap();
    tokio::spawn(pump_stderr(
        tokio::io::BufReader::new(stderr),
        ready_tx,
        overflow_tx,
        STDERR_BUDGET,
        name.clone(),
    ));

    // Wait for container to become ready, or close its stderr (likely due to a crash),
    // or for a minute to elapse (timeout). Either is an internal failure,
    // whose cause is in this process's logs of the container's stderr.
    tokio::select! {
        _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
            return Err(crate::status_to_anyhow(tonic::Status::internal(format!(
                "timeout waiting for connector container {name} to become ready"
            ))));
        }
        ready = ready_rx => if ready.is_err() {
            // `pump_stderr` signals an overflow before it cancels readiness.
            if let Ok(Some(())) = overflow_rx.try_recv() {
                return Err(stderr_budget_exceeded(&name));
            }
            return Err(crate::status_to_anyhow(tonic::Status::internal(format!(
                "connector container {name} exited before flow-connector-init started"
            ))));
        },
    }
    let stderr_overflow = stderr_overflow(overflow_rx, name.clone());

    // Ask docker for network configuration that it assigned to the container.
    let (ip_addr, mapped_host_ports) = inspect_container_network(&name).await?;

    // Dial the gRPC endpoint hosted by `flow-connector-init` within the container context.
    let init_address = if let Some(addr) = mapped_host_ports.get(&(CONNECTOR_INIT_PORT as u32)) {
        format!("http://{addr}")
    } else {
        format!("http://{ip_addr}:{CONNECTOR_INIT_PORT}")
    };
    let channel = tonic::transport::Endpoint::new(init_address.clone())
        .expect("formatting endpoint address")
        .connect_timeout(std::time::Duration::from_secs(5))
        .http2_keep_alive_interval(std::time::Duration::from_secs(5))
        // Default is 20s. The task runtime is single-threaded and hyper checks
        // this timer before reading the pending PONG, so a long synchronous
        // stretch of shard work would otherwise be misread as a dead peer.
        .keep_alive_timeout(std::time::Duration::from_secs(60))
        .connect()
        .await
        .with_context(|| {
            format!("failed to connect to container connector-init at {init_address}")
        })?;

    // Low-level network / codec detail stays at debug; the user-facing "started"
    // event is reported to the log sink below, at info.
    tracing::debug!(
        %image,
        %init_address,
        %ip_addr,
        mapped_host_ports = ?ops::DebugJson(&mapped_host_ports),
        %name,
        "dialed connector container"
    );

    Ok(RunningContainer {
        ip_addr,
        mapped_host_ports,
        channel,
        process,
        stderr_overflow,
    })
}

/// Resolve to the error which fails a session once `pump_stderr` signals
/// `overflow_rx`, and otherwise never.
pub(crate) fn stderr_overflow(
    overflow_rx: oneshot::Receiver<()>,
    container: String,
) -> futures::future::BoxFuture<'static, anyhow::Error> {
    async move {
        match overflow_rx.await {
            Ok(()) => stderr_budget_exceeded(&container),
            // `pump_stderr` reached EOF within its budget.
            Err(oneshot::Canceled) => futures::future::pending().await,
        }
    }
    .boxed()
}

pub(crate) fn stderr_budget_exceeded(container: &str) -> anyhow::Error {
    crate::status_to_anyhow(tonic::Status::resource_exhausted(format!(
        "connector container {container} exceeded its stderr budget of {STDERR_BUDGET} bytes"
    )))
}

/// Read the stderr of `docker run`, or of a VMM's attached `podman start`,
/// until it closes, signalling `ready_tx` at flow-connector-init's readiness
/// byte.
///
/// Connector logs travel in-band on the `connector.Connector` stream, so
/// stderr carries only the container runtime's errors and flow-connector-init's
/// application logs (and, possibly, lines a connector writes to it directly).
/// A VMM adds libkrun's and its launcher's diagnostics.
/// None is about something the user controls, so each line is emitted as an
/// application warning naming the container.
///
/// Stderr may total `budget` bytes, counted from spawn. Reads are bounded by
/// what remains, so memory is too. A read which crosses the budget is emitted
/// truncated, and `overflow_tx` is signalled. The reader is then dropped, so
/// that further writes fail rather than cost this process anything.
pub(crate) async fn pump_stderr<R>(
    mut stderr: tokio::io::BufReader<R>,
    ready_tx: oneshot::Sender<()>,
    overflow_tx: oneshot::Sender<()>,
    budget: usize,
    container: String,
) where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut line = Vec::new();
    let mut ready_tx = Some(ready_tx);
    let mut remaining = budget;

    loop {
        // `flow-connector-init` binds its port and then writes a single
        // whitespace byte: our only signal that the container is up. Anything
        // before it is `docker run` talking -- pull progress, or a failure
        // such as a name conflict -- and mistaking that for readiness races
        // us into inspecting a container which doesn't exist yet.
        let first = match stderr.fill_buf().await {
            Ok([]) => None,
            Ok(buf) => Some(buf[0]),
            Err(error) => {
                tracing::error!(%error, %container, "failed to read from connector container stderr");
                None
            }
        };
        let Some(first) = first else { break };

        if first == b' ' && ready_tx.is_some() && remaining != 0 {
            stderr.consume(1);
            remaining -= 1;
            _ = ready_tx.take().unwrap().send(());
            continue;
        }

        line.clear();

        let read = tokio::io::AsyncReadExt::take(&mut stderr, remaining as u64 + 1)
            .read_until(b'\n', &mut line)
            .await;
        let read = match read {
            Err(error) => {
                tracing::error!(%error, %container, "failed to read from connector container stderr");
                break;
            }
            Ok(0) => break,
            Ok(read) => read,
        };

        if read > remaining {
            let line = String::from_utf8_lossy(&line[..remaining]);
            tracing::warn!(
                %container,
                line = line.trim_end(),
                "connector container stderr (truncated: exceeded its budget)"
            );
            _ = overflow_tx.send(());
            return;
        }
        remaining -= read;

        let line = String::from_utf8_lossy(&line);
        let line = line.trim_end();

        if !line.is_empty() {
            tracing::warn!(%container, line, "connector container stderr");
        }
    }
    // An un-sent `ready_tx` cancels on drop, telling the launch that stderr
    // closed before the container came up.
}

/// Generate a name for a connector container which is unique on this host.
fn unique_container_name() -> String {
    format!("fc_{:016x}", rand::random::<u64>())
}

fn docker_cli() -> String {
    std::env::var("DOCKER_CLI")
        .ok()
        .unwrap_or_else(|| "docker".to_string())
}

/// Build a command of the container engine `program`. Engine CLIs and
/// operator wrappers need their ambient configuration, but not the reactor's
/// signing, decryption, or Flow login authority.
pub(crate) fn engine_command(program: &str) -> async_process::Command {
    let mut command = async_process::Command::new(program);
    for name in [
        "CONSUMER_AUTH_KEYS",
        "BROKER_AUTH_KEYS",
        "SOPS_AGE_KEY",
        "FLOW_AUTH_TOKEN",
    ] {
        command.env_remove(name);
    }
    command
}

pub(crate) const DEFAULT_MEMORY_LIMIT: &str = "1g";
pub(crate) const DEFAULT_CPU_LIMIT: &str = "2";

fn connector_memory_limit() -> String {
    std::env::var("CONNECTOR_MEMORY_LIMIT")
        .ok()
        .unwrap_or_else(|| DEFAULT_MEMORY_LIMIT.to_string())
}

fn connector_cpu_limit() -> String {
    std::env::var("CONNECTOR_CPU_LIMIT")
        .ok()
        .unwrap_or_else(|| DEFAULT_CPU_LIMIT.to_string())
}

async fn docker_cmd<S>(args: &[S]) -> anyhow::Result<Vec<u8>>
where
    S: AsRef<std::ffi::OsStr> + std::fmt::Debug,
{
    engine_cmd(&docker_cli(), args).await
}

/// Run one command of the container engine `program` to completion,
/// returning its stdout, or failing with its stderr.
pub(crate) async fn engine_cmd<S>(program: &str, args: &[S]) -> anyhow::Result<Vec<u8>>
where
    S: AsRef<std::ffi::OsStr> + std::fmt::Debug,
{
    let output = async_process::output(engine_command(program).args(args))
        .await
        .with_context(|| format!("failed to run {program} command {args:?}"))?;

    if !output.status.success() {
        anyhow::bail!(
            "{program} command {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr),
        );
    }
    Ok(output.stdout)
}

/// Pull the `CONNECTOR_PLATFORM` variant of `image` with the container engine
/// `program`, retrying errors which look transient.
pub(crate) async fn pull(program: &str, image: &str, log_sink: &LogSink) -> anyhow::Result<()> {
    const MAX_RETRIES: u32 = 3;
    const RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(2);

    for attempt in 1..=MAX_RETRIES {
        let Err(err) = engine_cmd(
            program,
            &[
                "pull",
                image,
                "--quiet",
                &format!("--platform={CONNECTOR_PLATFORM}"),
            ],
        )
        .await
        else {
            return Ok(());
        };

        let err_str = format!("{err:#}");
        let is_transient = err_str.contains("TLS handshake timeout")
            || err_str.contains("connection reset")
            || err_str.contains("i/o timeout")
            || err_str.contains("unexpected EOF")
            // Docker's registry client gives up on a slow registry with
            // `request canceled (Client.Timeout exceeded while awaiting
            // headers)`, which reads as a client-side error but is really the
            // registry being slow to answer.
            || err_str.contains("Client.Timeout exceeded");

        if is_transient && attempt < MAX_RETRIES {
            log_sink
                .send(crate::build_log(
                    ops::LogLevel::Warn,
                    "transient error pulling image (will retry)",
                    [
                        ("image", crate::json_field(&image)),
                        ("attempt", crate::json_field(&attempt)),
                        ("error", crate::json_field(&err_str)),
                    ],
                ))
                .await;
            tokio::time::sleep(RETRY_DELAY).await;
        } else {
            return Err(err);
        }
    }
    unreachable!()
}

async fn inspect_container_network(
    name: &str,
) -> anyhow::Result<(std::net::IpAddr, BTreeMap<u32, String>)> {
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "PascalCase", deny_unknown_fields)]
    struct HostPort {
        host_ip: String,
        host_port: String,
    }

    #[derive(serde::Deserialize)]
    struct Output {
        status: String,
        ip: std::net::IpAddr,
        ports: BTreeMap<String, Option<Vec<HostPort>>>,
    }

    let output = docker_cmd(&[
        "inspect",
        "--format",
        r#"{
            "ip": "{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
            "ports": {{json .NetworkSettings.Ports}},
            "status": {{json .State.Status}}
        }"#,
        name,
    ])
    .await
    .context("failed to inspect a started docker container (did it crash?)")?;

    let output = String::from_utf8_lossy(&output);
    let Output { status, ip, ports } = serde_json::from_str(&output)
        .with_context(|| format!("malformed docker container inspection output: {output}"))?;

    if status != "running" {
        anyhow::bail!("container failed to start; did it crash? (docker status is {status:?})");
    }

    let mut mapped_host_ports = BTreeMap::new();

    for (container_port, mappings) in ports {
        let Some(mappings) = mappings else { continue };

        for HostPort { host_ip, host_port } in mappings {
            if container_port.ends_with("/udp") {
                continue; // Not supported.
            }

            // `podman` inspect output will use an empty HostIp to represent
            // dual-stack port bindings (either `::1` or `0.0.0.0`).
            // `docker` will always emit a non-empty IP.
            let host_ip = if host_ip.is_empty() {
                "127.0.0.1".to_string()
            } else {
                host_ip
            };

            let host_ip: std::net::IpAddr = host_ip
                .parse()
                .with_context(|| format!("failed to parse HostIp: {host_ip:?}"))?;

            // Technically, ports are allowed to appear without the '/tcp' suffix.
            let container_port = container_port
                .strip_suffix("/tcp")
                .unwrap_or(&container_port);

            let container_port = container_port.parse::<u16>().with_context(|| {
                format!("invalid port in inspected NetworkSettings.Ports '{container_port}'")
            })?;
            let host_port = host_port.parse::<u16>().with_context(|| {
                format!("invalid port in inspected NetworkSettings.Ports.*.HostPort '{host_port}'")
            })?;

            _ = mapped_host_ports.insert(
                container_port as u32,
                if host_ip.is_ipv6() {
                    format!("[{host_ip}]:{host_port}")
                } else {
                    format!("{host_ip}:{host_port}")
                },
            );
        }
    }

    Ok((ip, mapped_host_ports))
}

async fn find_connector_init_and_copy(tmp_path: &std::path::Path) -> anyhow::Result<()> {
    // If we can locate an installed flow-connector-init, use that.
    // This is common when developing or within a container workspace.
    if let Ok(connector_init) = locate_bin::locate_static("flow-connector-init") {
        tokio::fs::copy(connector_init, tmp_path).await?;
        return Ok(());
    }

    // Create -- but don't start -- a container.
    let name = format!("{}_fci", unique_container_name());
    docker_cmd(&[
        "create",
        &format!("--platform={CONNECTOR_PLATFORM}"),
        &format!("--name={name}"),
        CONNECTOR_INIT_IMAGE,
    ])
    .await?;

    // Ask docker to copy the binary to our temp location.
    docker_cmd(&[
        "cp",
        &format!("{name}:{CONNECTOR_INIT_IMAGE_PATH}"),
        &tmp_path.to_str().expect("temp is UTF-8"),
    ])
    .await?;

    // Clean up the created container.
    docker_cmd(&["rm", "--volumes", &name]).await?;

    Ok(())
}

/// Inspect the `CONNECTOR_PLATFORM` variant of `image`, which must already be
/// pulled. The result is mounted into the container as `/image-inspect.json`,
/// from which `flow-connector-init` derives the connector's real entrypoint.
///
/// Plain `docker inspect` reports the host platform's variant, which under the
/// containerd image store may not be present at all -- yielding an empty
/// `Config` that fails to parse. Only `docker image inspect` takes `--platform`.
/// `podman image inspect` has no such flag and needs none, as it stores the
/// concrete image that `--platform` pulled; hence the fallback.
async fn inspect_image(image: &str) -> anyhow::Result<Vec<u8>> {
    let platform = format!("--platform={CONNECTOR_PLATFORM}");

    match docker_cmd(&["image", "inspect", &platform, image]).await {
        Ok(output) => Ok(output),
        Err(error) => {
            tracing::debug!(
                %error,
                "`image inspect --platform` failed; retrying without it"
            );
            docker_cmd(&["image", "inspect", image]).await
        }
    }
}

/// Pull and inspect an image without otherwise preparing or starting it.
pub(crate) async fn pull_and_inspect(
    image: &str,
    log_sink: &LogSink,
) -> anyhow::Result<ImageInspection> {
    if !image.ends_with(":local") {
        pull(&docker_cli(), image, log_sink)
            .await
            .context("pulling image")?;
    }

    let inspect_json = inspect_image(image).await.context("inspecting image")?;
    let inspection = connector_init::inspect::Image::parse_from_json_slice(&inspect_json)
        .context("parsing image inspection")?;

    Ok(ImageInspection {
        image: image.to_string(),
        inspect_json,
        inspection,
    })
}

#[cfg(test)]
#[path = "container/engine_tests.rs"]
mod engine_tests;

#[cfg(test)]
mod test {
    use futures::StreamExt;
    use serde_json::json;

    #[tokio::test]
    async fn pump_stderr_signals_buffered_readiness() {
        use tokio::io::AsyncWriteExt;

        // Write everything before the pump starts, so its first read buffers
        // the whitespace byte behind a raw line. Holding `writer` open shows
        // that readiness doesn't wait for EOF.
        let (mut writer, reader) = tokio::io::duplex(1024);
        writer
            .write_all(b"WARN engine: first\n raw after readiness\n")
            .await
            .unwrap();

        let (ready_tx, ready_rx) = futures::channel::oneshot::channel();
        let (overflow_tx, _overflow_rx) = futures::channel::oneshot::channel();
        let pump = tokio::spawn(super::pump_stderr(
            tokio::io::BufReader::new(reader),
            ready_tx,
            overflow_tx,
            super::STDERR_BUDGET,
            "fc_0000000000000000".to_string(),
        ));

        tokio::time::timeout(std::time::Duration::from_secs(5), ready_rx)
            .await
            .expect("readiness while stderr is open")
            .unwrap();
        drop(writer);
        pump.await.unwrap();
    }

    #[tokio::test]
    async fn pump_stderr_cancels_readiness_of_an_early_exit() {
        let (ready, overflow, _drawn) = pump_budgeted(b"Error: no such image\n", 64).await;
        assert_eq!((ready, overflow), (false, false));
    }

    #[tokio::test]
    async fn pump_stderr_enforces_its_budget() {
        const BUDGET: usize = 64;
        let mut huge = vec![b' '];
        huge.extend(std::iter::repeat_n(b'z', 1 << 20));

        let cases: [(&str, Vec<u8>); 4] = [
            ("under budget", b"warn: a\n \nmore\n".to_vec()),
            (
                "crosses after readiness",
                [b" ".as_slice(), &[b'x'; 100], b"\n"].concat(),
            ),
            (
                "crosses before readiness",
                [[b'y'; 100].as_slice(), b"\n "].concat(),
            ),
            ("one newline-free write", huge),
        ];

        let mut outcomes = Vec::new();
        for (name, input) in cases {
            let (ready, overflow, drawn) = pump_budgeted(&input, BUDGET).await;

            // Reads are bounded by the budget, plus one byte which detects a
            // crossing, plus what `pump_budgeted`'s reader buffers.
            assert!(
                drawn <= BUDGET + 1 + READER_CAPACITY,
                "{name}: drew {drawn}"
            );
            outcomes.push(format!("{name}: ready={ready} overflow={overflow}"));
        }

        insta::assert_debug_snapshot!(outcomes, @r#"
        [
            "under budget: ready=true overflow=false",
            "crosses after readiness: ready=true overflow=true",
            "crosses before readiness: ready=false overflow=true",
            "one newline-free write: ready=true overflow=true",
        ]
        "#);
    }

    const READER_CAPACITY: usize = 16;

    /// Pump `input` with `budget`, and return whether it signalled readiness
    /// and overflow, and the bytes it drew from `input`.
    async fn pump_budgeted(input: &[u8], budget: usize) -> (bool, bool, usize) {
        let mut remaining = input;
        let (ready_tx, ready_rx) = futures::channel::oneshot::channel();
        let (overflow_tx, overflow_rx) = futures::channel::oneshot::channel();

        super::pump_stderr(
            tokio::io::BufReader::with_capacity(READER_CAPACITY, &mut remaining),
            ready_tx,
            overflow_tx,
            budget,
            "fc_0000000000000000".to_string(),
        )
        .await;

        (
            ready_rx.await.is_ok(),
            overflow_rx.await.is_ok(),
            input.len() - remaining.len(),
        )
    }

    #[tokio::test]
    async fn runs_an_inspected_image() {
        if super::docker_cmd(&["version"]).await.is_err() {
            // Most CI jobs don't provide a container engine.
            return;
        }

        let inspected = super::pull_and_inspect(
            "ghcr.io/estuary/source-http-ingest:dev",
            &crate::LogSink::tracing(),
        )
        .await
        .unwrap();
        let mount = crate::protocol::create_connector_mount().unwrap();
        let running = super::run(
            inspected,
            super::RunParams {
                env: std::collections::BTreeMap::from([(
                    "CONNECTOR_MOUNT".to_string(),
                    mount.path().to_string_lossy().into_owned(),
                )]),
                host_gateway_names: Vec::new(),
                labels: std::collections::BTreeMap::new(),
                mount: mount.path().to_owned(),
                network: String::new(),
                publish_ports: true,
                network_ports: vec![8080],
            },
        )
        .await
        .unwrap();

        assert_eq!(
            running
                .mapped_host_ports
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![8080, 49092]
        );

        let result = proto_grpc::connector::connector_client::ConnectorClient::new(running.channel)
            .connector(futures::stream::once(async move {
                serde_json::from_value::<crate::proto::Request>(json!({
                    "start": {"logLevel": "debug"},
                    "capture": {"spec": {"connectorType": "IMAGE", "config": {}}},
                }))
                .unwrap()
            }))
            .await;

        // TODO(johnny): Remove once `CONNECTOR_INIT_IMAGE` serves connector.Connector.
        let mut responses = match result {
            Err(status) if status.code() == tonic::Code::Unimplemented => return,
            result => result.unwrap().into_inner(),
        };

        // Logs may precede `Started`.
        let started = loop {
            let response = responses.next().await.unwrap().unwrap();
            if let Some(crate::proto::response::Kind::Started(started)) = response.kind {
                break started;
            }
        };
        assert!(matches!(
            started.spec,
            Some(crate::proto::response::started::Spec::Capture(_))
        ));
    }
}

//! Docker and Podman image mechanics: pull, inspect, run, dial
//! `flow-connector-init`, and tear down.
use crate::LogSink;
use anyhow::Context;
use futures::channel::oneshot;
use std::collections::BTreeMap;
use tokio::io::AsyncBufReadExt;

// Port on which flow-connector-init listens for requests.
// This is its default, made explicit here.
// This number was chosen because it seemed unlikely that a connector would try to use it.
// The main thing is that we want to avoid any common port numbers to avoid conflicts with
// connectors.
const CONNECTOR_INIT_PORT: u16 = 49092;

// For now, we support only Linux amd64 connectors. `docker pull` must request
// it explicitly: under Docker's containerd image store a platform-less pull
// fetches only the host's variant, which `docker run --platform` cannot use.
const CONNECTOR_PLATFORM: &str = "linux/amd64";

// `flow-connector-init` is extracted from this image when a locally-built copy
// isn't found by `locate_bin` (dev/CI builds place one alongside the executable).
// TODO(johnny): Consider better packaging and versioning of `flow-connector-init`.
const CONNECTOR_INIT_IMAGE: &str = "ghcr.io/estuary/reactor:v0.6.12-69-gb7eb6426711";
const CONNECTOR_INIT_IMAGE_PATH: &str = "/usr/local/bin/flow-connector-init";

/// Options which have already been selected for a container execution.
pub(crate) struct RunParams {
    pub env: BTreeMap<String, String>,
    pub labels: BTreeMap<String, String>,
    pub log_sink: LogSink,
    pub mount: std::path::PathBuf,
    pub network: String,
    pub publish_ports: bool,
}

/// Lower-level facts of a running image connector.
pub(crate) struct RunningContainer {
    pub channel: tonic::transport::Channel,
    pub ip_addr: std::net::IpAddr,
    pub mapped_host_ports: BTreeMap<u32, String>,
    pub process: async_process::Child,
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
pub(crate) async fn run<F>(
    inspection: ImageInspection,
    params: RunParams,
    transform_log: F,
) -> anyhow::Result<RunningContainer>
where
    F: Fn(ops::Log) -> ops::Log + Send + 'static,
{
    let ImageInspection {
        image,
        inspect_json,
        ..
    } = inspection;
    let RunParams {
        env,
        labels,
        log_sink,
        mount,
        network,
        publish_ports,
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
    for (name, value) in labels {
        docker_args.push(format!("--label={name}={value}"));
    }

    // When running locally, we publish ports so that connectors are accessible
    // on the host from Windows and MacOS (e.x. Docker Desktop).
    if publish_ports {
        docker_args.append(&mut vec![
            // Support Docker Desktop in non-production contexts (for example, `flowctl`)
            // where the container IP is not directly addressable. As an alternative,
            // we ask Docker to provide mapped host ports that are then advertised
            // in the attached runtime::Container description.
            // An empty host port asks Docker to assign a free ephemeral port as
            // the container starts. Unlike probing for a free port ourselves,
            // this cannot race host-port allocations of other starting containers.
            format!("--publish=0.0.0.0::{CONNECTOR_INIT_PORT}"),
            "--publish-all".to_string(),
        ]);
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

    let mut process: async_process::Child = async_process::Command::new(docker_cli())
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

    // Service process stderr by decoding ops::Logs into the log sink.
    let stderr = process.stderr.take().unwrap();
    let (pump_sink, pump_image) = (log_sink.clone(), image.clone());
    tokio::spawn(async move {
        let mut stderr = tokio::io::BufReader::new(stderr);
        let mut line = String::new();
        let mut ready_tx = Some(ready_tx);

        let decoder = ops::decode::Decoder::new(std::time::SystemTime::now);
        loop {
            // `flow-connector-init` binds its port and then writes a single
            // whitespace byte: our only signal that the container is up. Anything
            // before it is `docker run` talking -- pull progress, or a failure
            // such as a name conflict -- and mistaking that for readiness races
            // us into inspecting a container which doesn't exist yet.
            let first = match stderr.fill_buf().await {
                Ok([]) => None, // Clean EOF.
                Ok(buf) => Some(buf[0]),
                Err(error) => {
                    tracing::error!(%error, "failed to read from connector stderr");
                    None
                }
            };
            let Some(first) = first else { break };

            if first == b' ' && ready_tx.is_some() {
                stderr.consume(1); // Discard.
                _ = ready_tx.take().unwrap().send(()); // Signal that we're ready.
                continue;
            }

            line.clear();

            match stderr.read_line(&mut line).await {
                Err(error) => {
                    tracing::error!(%error, "failed to read from connector stderr");
                    break;
                }
                Ok(0) => break, // Clean EOF.
                Ok(_) => (),
            }

            let (log, consume) = decoder.line_to_log(&line, stderr.buffer());
            stderr.consume(consume);
            pump_sink.send(transform_log(log)).await;
        }
        // An un-sent `ready_tx` cancels on drop, telling `run()` that stderr
        // closed before the container came up. That case never logged a
        // "started connector container", so it gets no "stopped" either --
        // otherwise this pairs with it, and is the last record of the
        // container that the caller's sink sees.
        if ready_tx.is_none() {
            pump_sink
                .send(crate::build_log(
                    ops::LogLevel::Debug,
                    "stopped connector container",
                    [("image", crate::json_field(&pump_image))],
                ))
                .await;
        }
    });

    // Wait for container to become ready, or close its stderr (likely due to a crash),
    // or for a minute to elapse (timeout).
    tokio::select! {
        _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
            anyhow::bail!("timeout waiting for the container to become ready");
        }
        ready = ready_rx => if ready.is_err() {
            anyhow::bail!(
                "container exited before flow-connector-init started; \
                 the cause is in the preceding connector logs"
            );
        },
    }

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
    })
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

fn connector_memory_limit() -> String {
    std::env::var("CONNECTOR_MEMORY_LIMIT")
        .ok()
        .unwrap_or_else(|| "1g".to_string())
}

fn connector_cpu_limit() -> String {
    std::env::var("CONNECTOR_CPU_LIMIT")
        .ok()
        .unwrap_or_else(|| "2".to_string())
}

async fn docker_cmd<S>(args: &[S]) -> anyhow::Result<Vec<u8>>
where
    S: AsRef<std::ffi::OsStr> + std::fmt::Debug,
{
    let output = async_process::output(async_process::Command::new(docker_cli()).args(args))
        .await
        .with_context(|| format!("failed to run docker command {args:?}"))?;

    if !output.status.success() {
        anyhow::bail!(
            "docker command {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr),
        );
    }
    Ok(output.stdout)
}

async fn docker_pull(image: &str, log_sink: &LogSink) -> anyhow::Result<()> {
    const MAX_RETRIES: u32 = 3;
    const RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(2);

    for attempt in 1..=MAX_RETRIES {
        let Err(err) = docker_cmd(&[
            "pull",
            image,
            "--quiet",
            &format!("--platform={CONNECTOR_PLATFORM}"),
        ])
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
    if let Ok(connector_init) = locate_bin::locate("flow-connector-init") {
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
        docker_pull(image, log_sink)
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
mod test {
    use futures::StreamExt;
    use serde_json::json;

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
                env: std::collections::BTreeMap::from([
                    (
                        "CONNECTOR_MOUNT".to_string(),
                        mount.path().to_string_lossy().into_owned(),
                    ),
                    ("LOG_FORMAT".to_string(), "json".to_string()),
                    ("LOG_LEVEL".to_string(), "debug".to_string()),
                ]),
                labels: std::collections::BTreeMap::new(),
                log_sink: crate::LogSink::tracing(),
                mount: mount.path().to_owned(),
                network: String::new(),
                publish_ports: true,
            },
            |log| log,
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

        let mut responses =
            proto_grpc::capture::connector_client::ConnectorClient::new(running.channel)
                .capture(futures::stream::once(async move {
                    serde_json::from_value(json!({
                        "spec": {"connectorType": "IMAGE", "config": {}}
                    }))
                    .unwrap()
                }))
                .await
                .unwrap()
                .into_inner();
        let response = responses.next().await.unwrap().unwrap();

        assert!(matches!(
            response.kind,
            Some(proto_flow::capture::response::Kind::Spec(_))
        ));
    }
}

use crate::RuntimeProtocol;
use anyhow::Context;
use futures::channel::oneshot;
use proto_flow::{flow, runtime};
use std::collections::BTreeMap;
use tokio::io::AsyncBufReadExt;

// Port on which flow-connector-init listens for requests.
// This is its default, made explicit here.
// This number was chosen because it seemed unlikely that a connector would try to use it.
// The main thing is that we want to avoid any common port numbers to avoid conflicts with
// connectors.
const CONNECTOR_INIT_PORT: u16 = 49092;

/// Determines the protocol of an image. If the image has a `FLOW_RUNTIME_PROTOCOL` label,
/// then it's value is used. Otherwise, this will apply a simple heuristic based on the image name,
/// for backward compatibility purposes. An error will be returned if it fails to inspect the image
/// or parse the label.
pub async fn flow_runtime_protocol(image: &str) -> anyhow::Result<RuntimeProtocol> {
    let inspect_output = docker_cmd(&["inspect", image])
        .await
        .context("inspecting image")?;
    let inspect_json: serde_json::Value = serde_json::from_slice(&inspect_output)?;

    if let Some(label) = inspect_json
        .pointer("/Config/Labels/FLOW_RUNTIME_PROTOCOL")
        .and_then(|v| v.as_str())
    {
        RuntimeProtocol::try_from(label).map_err(|unknown| {
            anyhow::anyhow!("image labels specify unknown protocol FLOW_RUNTIME_PROTOCOL={unknown}")
        })
    } else {
        if image.starts_with("ghcr.io/estuary/materialize-") {
            Ok(RuntimeProtocol::Materialization)
        } else {
            Ok(RuntimeProtocol::Capture)
        }
    }
}

/// Start an image connector container, returning its description and a dialed tonic Channel.
/// The container is attached to the given `network`, and its logs are dispatched to `log_handler`.
/// `task_name` and `task_type` are used only to label the container.
pub async fn start(
    image: &str,
    log_handler: impl crate::LogHandler,
    log_level: Option<ops::LogLevel>,
    network: &str,
    task_name: &str,
    task_type: ops::TaskType,
) -> anyhow::Result<(runtime::Container, tonic::transport::Channel, Guard)> {
    // Many operational contexts only allow for docker volume mounts
    // from certain locations:
    //  * Docker for Mac restricts file shares to /User, /tmp, and a couple others.
    //  * Estuary's current K8s deployments use a separate docker daemon container
    //    within the pod, having a common /tmp tempdir volume.
    //
    // So, we use temporaries to ensure that files are readable within the container.
    let tmp_connector_init =
        tempfile::NamedTempFile::new().context("creating temp for flow-connector-init")?;
    let mut tmp_docker_inspect =
        tempfile::NamedTempFile::new().context("creating temp for docker inspect output")?;

    // Change mode of `docker_inspect` to be readable by all users.
    // This is required because the effective container user may have a different UID.
    #[cfg(unix)]
    {
        use std::os::unix::prelude::PermissionsExt;
        let mut perms = tmp_docker_inspect.as_file_mut().metadata()?.permissions();
        perms.set_mode(0o644);
        tmp_docker_inspect.as_file_mut().set_permissions(perms)?;
    }

    // Concurrently 1) find or fetch a copy of `flow-connector-init`, copying it
    // into a temp path, and 2) inspect the image, also copying into a temp path,
    // and parsing its advertised network ports.
    let ((), network_ports) = futures::try_join!(
        find_connector_init_and_copy(tmp_connector_init.path()),
        inspect_image_and_copy(image, tmp_docker_inspect.path()),
    )?;

    // Close our open files but retain a deletion guard.
    let tmp_connector_init = tmp_connector_init.into_temp_path();
    let tmp_docker_inspect = tmp_docker_inspect.into_temp_path();

    // This is default `docker run` behavior if --network is not provided.
    let network = if network == "" { "bridge" } else { network };
    let log_level = log_level.unwrap_or(ops::LogLevel::Warn);

    // Generate a unique name for this container instance.
    let name = unique_container_name();

    let mut process: async_process::Child = async_process::Command::new("docker")
        .args([
            "run",
            // Remove the docker container upon its exit.
            "--rm",
            // Addressable name of this connector.
            &format!("--name={name}"),
            // Network to which the container should attach.
            &format!("--network={}", network),
            // The entrypoint into a connector is always flow-connector-init,
            // which will delegate to the actual entrypoint of the connector.
            "--entrypoint=/flow-connector-init",
            // Mount the flow-connector-init binary and `docker inspect` output.
            &format!(
                "--mount=type=bind,source={},target=/flow-connector-init",
                tmp_connector_init.to_string_lossy()
            ),
            &format!(
                "--mount=type=bind,source={},target=/image-inspect.json",
                tmp_docker_inspect.to_string_lossy(),
            ),
            // Thread-through the logging configuration of the connector.
            "--env=LOG_FORMAT=json",
            &format!("--env=LOG_LEVEL={}", log_level.as_str_name()),
            // Cgroup memory / CPU resource limits.
            // TODO(johnny): we intend to tighten these down further, over time.
            "--memory=1g",
            "--cpus=2",
            // For now, we support only Linux amd64 connectors.
            "--platform=linux/amd64",
            // Attach labels that let us group connector resource usage under a few dimensions.
            &format!("--label=image={}", image),
            &format!("--label=task-name={}", task_name),
            &format!("--label=task-type={}", task_type.as_str_name()),
            // Support Docker Desktop in non-production contexts (for example, `flowctl`)
            // where the container IP is not directly addressable. As an alternative,
            // we ask Docker to provide mapped host ports that are then advertised
            // in the attached runtime::Container description.
            #[cfg(not(target_os = "linux"))]
            &format!("--publish=0.0.0.0:0:{CONNECTOR_INIT_PORT}"),
            #[cfg(not(target_os = "linux"))]
            "--publish-all",
            // Image to run.
            &image,
            // The following are arguments of flow-connector-init, not docker.
            "--image-inspect-json-path=/image-inspect.json",
            &format!("--port={CONNECTOR_INIT_PORT}"),
        ])
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

    // Service process stderr by decoding ops::Logs and sending to our handler.
    let stderr = process.stderr.take().unwrap();
    tokio::spawn(async move {
        let mut stderr = tokio::io::BufReader::new(stderr);
        let mut line = String::new();

        // Wait for a non-empty read of stderr to complete or EOF/error.
        // Note that `flow-connector-init` writes one whitespace byte on startup.
        if let Ok(_buf) = stderr.fill_buf().await {
            stderr.consume(1); // Discard whitespace byte.
        }
        std::mem::drop(ready_tx); // Signal that we're ready.

        loop {
            line.clear();

            match stderr.read_line(&mut line).await {
                Err(error) => {
                    tracing::error!(%error, "failed to read from connector stderr");
                    break;
                }
                Ok(0) => break, // Clean EOF.
                Ok(_) => (),
            }

            match serde_json::from_str(&line) {
                Ok(log) => log_handler(&log),
                Err(error) => {
                    tracing::error!(?error, %line, "failed to parse ops::Log from container");
                }
            }
        }
    });

    // Wait for container to become ready, or close its stderr (likely due to a crash),
    // or for thirty seconds to elapse (timeout).
    tokio::select! {
        _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
            anyhow::bail!("timeout waiting for the container to become ready");
        }
        _ = ready_rx => (),
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
        .connect()
        .await
        .context("failed to connect to connector-init inside of container")?;

    tracing::info!(
        %image,
        %init_address,
        %ip_addr,
        ?mapped_host_ports,
        %name,
        %task_name,
        ?task_type,
        "started connector"
    );

    Ok((
        runtime::Container {
            ip_addr: format!("{ip_addr}"),
            network_ports: network_ports.clone(),
            mapped_host_ports,
        },
        channel,
        Guard {
            _tmp_connector_init: tmp_connector_init,
            _tmp_docker_inspect: tmp_docker_inspect,
            _process: process,
        },
    ))
}

/// Guard contains a running image container instance,
/// which will be stopped and cleaned up when the Guard is dropped.
pub struct Guard {
    _tmp_connector_init: tempfile::TempPath,
    _tmp_docker_inspect: tempfile::TempPath,
    _process: async_process::Child,
}

fn unique_container_name() -> String {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    format!("fc_{:x}", n as u32)
}

async fn docker_cmd<S>(args: &[S]) -> anyhow::Result<Vec<u8>>
where
    S: AsRef<std::ffi::OsStr> + std::fmt::Debug,
{
    let output = async_process::output(async_process::Command::new("docker").args(args))
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

async fn inspect_container_network(
    name: &str,
) -> anyhow::Result<(std::net::IpAddr, BTreeMap<u32, String>)> {
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "PascalCase", deny_unknown_fields)]
    struct HostPort {
        host_ip: std::net::IpAddr,
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

fn parse_network_ports(content: &[u8]) -> anyhow::Result<Vec<flow::NetworkPort>> {
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    struct InspectConfig {
        /// According to the [OCI spec](https://github.com/opencontainers/image-spec/blob/d60099175f88c47cd379c4738d158884749ed235/config.md?plain=1#L125)
        /// `ExposedPorts` is a map where the keys are in the format `1234/tcp`, `456/udp`, or `789` (implicit default of tcp), and the values are
        /// empty objects. The choice of `serde_json::Value` here is meant to convey that the actual values are irrelevant.
        #[serde(default)]
        exposed_ports: BTreeMap<String, serde_json::Value>,
        #[serde(default)]
        labels: Option<BTreeMap<String, String>>,
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    struct InspectJson {
        config: InspectConfig,
    }

    // Deserialize into a destructured one-tuple.
    let (InspectJson {
        config: InspectConfig {
            exposed_ports,
            labels,
        },
    },) = serde_json::from_slice(&content).with_context(|| {
        format!(
            "failed to parse `docker inspect` output: {}",
            String::from_utf8_lossy(&content)
        )
    })?;

    let labels = labels.unwrap_or_default();
    let mut ports = Vec::new();

    for (exposed_port, _) in exposed_ports.iter() {
        // We're unable to support UDP at this time.
        if exposed_port.ends_with("/udp") {
            continue;
        }
        // Technically, the ports are allowed to appear without the '/tcp' suffix, though
        // I haven't actually observed that in practice.
        let exposed_port = exposed_port.strip_suffix("/tcp").unwrap_or(exposed_port);
        let number = exposed_port.parse::<u16>().with_context(|| {
            format!("invalid key in inspected Config.ExposedPorts '{exposed_port}'")
        })?;

        let protocol_label = format!("dev.estuary.port-proto.{number}");
        let protocol = labels.get(&protocol_label).cloned();

        let public_label = format!("dev.estuary.port-public.{number}");
        let public = labels
            .get(&public_label)
            .map(String::as_str)
            .unwrap_or("false");
        let public = public.parse::<bool>()
            .with_context(|| format!("invalid '{public_label}' label value: '{public}', must be either 'true' or 'false'"))?;

        ports.push(flow::NetworkPort {
            number: number as u32,
            protocol: protocol.unwrap_or_default(),
            public,
        });
    }

    Ok(ports)
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
        "--platform=linux/amd64",
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

async fn inspect_image_and_copy(
    image: &str,
    tmp_path: &std::path::Path,
) -> anyhow::Result<Vec<flow::NetworkPort>> {
    if !image.ends_with(":local") {
        _ = docker_cmd(&["pull", &image, "--quiet"]).await?;
    }
    let inspect_content = docker_cmd(&["inspect", &image]).await?;

    tokio::fs::write(tmp_path, &inspect_content)
        .await
        .context("writing docker inspect output")?;

    parse_network_ports(&inspect_content)
}

// TODO(johnny): Consider better packaging and versioning of `flow-connector-init`.
// TODO(johnny): Update tag once https://github.com/estuary/flow/pull/1167 is merged.
const CONNECTOR_INIT_IMAGE: &str = "ghcr.io/estuary/flow:v0.3.5-40-g1751ed2af";
const CONNECTOR_INIT_IMAGE_PATH: &str = "/usr/local/bin/flow-connector-init";

#[cfg(test)]
mod test {
    use super::{parse_network_ports, start};
    use futures::stream::StreamExt;
    use proto_flow::flow;
    use serde_json::json;

    #[tokio::test]
    async fn test_http_ingest_spec() {
        if let Err(_) = locate_bin::locate("flow-connector-init") {
            // Skip if `flow-connector-init` isn't available (yet). We're probably on CI.
            // This test is useful as a sanity check for local development
            // and we have plenty of other coverage during CI.
            return;
        }

        let (container, channel, _guard) = start(
            "ghcr.io/estuary/source-http-ingest:dev",
            ops::tracing_log_handler,
            Some(ops::LogLevel::Debug),
            "",
            "a-task-name",
            proto_flow::ops::TaskType::Capture,
        )
        .await
        .unwrap();

        let mut rx = proto_grpc::capture::connector_client::ConnectorClient::new(channel)
            .capture(futures::stream::once(async move {
                serde_json::from_value(json!({
                    "spec": {"connectorType": "IMAGE", "config": {}}
                }))
                .unwrap()
            }))
            .await
            .unwrap()
            .into_inner();

        let resp = rx
            .next()
            .await
            .expect("should get a spec response")
            .unwrap();

        assert!(resp.spec.is_some());

        assert_eq!(
            container.network_ports,
            [flow::NetworkPort {
                number: 8080,
                protocol: String::new(),
                public: true
            }]
        );

        #[cfg(target_os = "linux")]
        assert_eq!(container.mapped_host_ports, super::BTreeMap::new());
        #[cfg(not(target_os = "linux"))]
        assert_ne!(container.mapped_host_ports, super::BTreeMap::new());
    }

    #[tokio::test]
    async fn test_container_fails_to_start() {
        if let Err(_) = locate_bin::locate("flow-connector-init") {
            // Skip if `flow-connector-init` isn't available (yet). We're probably on CI.
            // This test is useful as a sanity check for local development
            // and we have plenty of other coverage during CI.
            return;
        }

        let Err(err) = start(
            "alpine", // Not a connector.
            ops::tracing_log_handler,
            Some(ops::LogLevel::Debug),
            "",
            "a-task-name",
            proto_flow::ops::TaskType::Capture,
        )
        .await
        else {
            panic!("didn't crash")
        };

        println!("{err:#}")
    }

    #[test]
    fn test_parsing_network_ports() {
        let fixture = json!([
            {
                "Config":{
                    "ExposedPorts": {"567/tcp":{}, "123/udp": {}, "789":{} },
                    "Labels":{"dev.estuary.port-public.567":"true","dev.estuary.port-proto.789":"h2"}
                }
            }
        ]);
        let ports = parse_network_ports(fixture.to_string().as_bytes()).unwrap();

        assert_eq!(
            ports,
            [
                flow::NetworkPort {
                    number: 567,
                    protocol: String::new(),
                    public: true
                },
                flow::NetworkPort {
                    number: 789,
                    protocol: "h2".to_string(),
                    public: false
                },
            ]
        );

        let fixture = json!([{"Invalid": "Inspection"}]);
        insta::assert_debug_snapshot!(parse_network_ports(fixture.to_string().as_bytes()).unwrap_err(), @r###"
        Error {
            context: "failed to parse `docker inspect` output: [{\"Invalid\":\"Inspection\"}]",
            source: Error("missing field `Config`", line: 1, column: 25),
        }
        "###);

        let fixture = json!([
            {
                "Config":{
                    "ExposedPorts": {"whoops":{}},
                }
            }
        ]);
        insta::assert_debug_snapshot!(parse_network_ports(fixture.to_string().as_bytes()).unwrap_err(), @r###"
        Error {
            context: "invalid key in inspected Config.ExposedPorts \'whoops\'",
            source: ParseIntError {
                kind: InvalidDigit,
            },
        }
        "###);

        let fixture = json!([
            {
                "Config":{
                    "ExposedPorts": {"111/tcp":{}},
                    "Labels":{"dev.estuary.port-public.111":"whoops"}
                }
            }
        ]);
        insta::assert_debug_snapshot!(parse_network_ports(fixture.to_string().as_bytes()).unwrap_err(), @r###"
        Error {
            context: "invalid \'dev.estuary.port-public.111\' label value: \'whoops\', must be either \'true\' or \'false\'",
            source: ParseBoolError,
        }
        "###);
    }
}

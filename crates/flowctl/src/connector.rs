use anyhow::anyhow;
use anyhow::Context;
use futures::{stream, Stream, TryStream};
use proto_flow::capture::{Request, Response};
use proto_grpc::capture::connector_client::ConnectorClient;
use std::{
    fs,
    pin::Pin,
    process::{Child, Command, Output, Stdio},
};
use tempfile::{tempdir, TempDir};

fn pull(image: &str) -> anyhow::Result<Output> {
    Command::new("docker")
        .args(["pull", image])
        .output()
        .map_err(|e| e.into())
}

fn inspect(image: &str) -> anyhow::Result<Output> {
    Command::new("docker")
        .args(["inspect", image])
        .output()
        .map_err(|e| e.into())
}

const CONNECTOR_INIT_PORT: u16 = 49092;

pub fn docker_spawn(image: &str, args: &[&str]) -> anyhow::Result<(Child, TempDir, u16)> {
    pull(image).context(format!("pulling {image}"))?;

    let inspect_output = inspect(image).context(format!("inspecting {image}"))?;

    let target_inspect = "/tmp/image-inspect.json";
    let dir = tempdir().context("creating temp directory")?;
    let host_inspect = dir.path().join("image-inspect.json");
    let host_inspect_str = host_inspect.clone().into_os_string().into_string().unwrap();

    fs::write(&host_inspect, inspect_output.stdout)?;
    let host_connector_init =
        locate_bin::locate("flow-connector-init").context("locating flow-connector-init")?;
    let host_connector_init_str = host_connector_init.into_os_string().into_string().unwrap();
    let target_connector_init = "/tmp/connector_init";

    let port = portpicker::pick_unused_port().expect("No ports free");

    let child = Command::new("docker")
        .args(
            [
                &[
                    "run",
                    "--rm",
                    "--entrypoint",
                    target_connector_init,
                    "--mount",
                    &format!(
                        "type=bind,source={host_connector_init_str},target={target_connector_init}"
                    ),
                    "--mount",
                    &format!("type=bind,source={host_inspect_str},target={target_inspect}"),
                    "--publish",
                    &format!("0.0.0.0:{}:{}/tcp", port, CONNECTOR_INIT_PORT),
                    image,
                    &format!("--image-inspect-json-path={target_inspect}"),
                    &format!("--port={CONNECTOR_INIT_PORT}"),
                ],
                args,
            ]
            .concat(),
        )
        .spawn()
        .context("spawning docker run child")?;

    Ok((child, dir, port))
}

async fn connector_client(port: u16) -> anyhow::Result<ConnectorClient<tonic::transport::Channel>> {
    loop {
        match ConnectorClient::connect(format!("tcp://127.0.0.1:{port}")).await {
            Ok(client) => return Ok(client),
            Err(_) => {
                std::thread::sleep(std::time::Duration::from_millis(1000));
                continue;
            }
        };
    }
}

pub async fn docker_run(image: &str, req: Request) -> anyhow::Result<Response> {
    let (_child, _dir, port) = docker_spawn(image, &[])?;

    let mut client = connector_client(port).await?;

    let mut response_stream = client.capture(stream::once(async { req })).await?;

    let response = response_stream.get_mut().message().await?;

    return response.ok_or(anyhow!("no response message"));
}

pub async fn docker_run_stream(
    image: &str,
    stream: Pin<Box<dyn Stream<Item = Request> + Send + Sync>>,
) -> anyhow::Result<
    Pin<Box<dyn TryStream<Item = anyhow::Result<Response>, Ok = Response, Error = anyhow::Error>>>,
> {
    let (_child, _dir, port) = docker_spawn(image, &[])?;
    let mut client = connector_client(port).await?;
    let response_stream = client.capture(stream).await?;

    Ok(Box::pin(stream::try_unfold(
        response_stream,
        |mut rs| async move {
            if let Some(msg) = rs.get_mut().message().await? {
                Ok(Some((msg, rs)))
            } else {
                Ok(None)
            }
        },
    )))
}

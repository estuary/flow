use anyhow::anyhow;
use std::{fs, process::{Command, Stdio, Output, Child}, io::Write};
use anyhow::Context;
use futures::stream;
use proto_flow::{capture::{request, Request, Response}, flow::capture_spec::ConnectorType};
use tempfile::{tempdir, TempDir};
use proto_grpc::capture::connector_client::ConnectorClient;
use tables::SqlTableObj;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Discover {
    /// Connector image to discover
    image: String,

    /// Tenant name
    #[clap(default_value_t = String::from("acmeCo"))]
    tenant: String,

    /// Docker network to run connector in
    #[clap(default_value_t = String::from("default"))]
    network: String,
}

pub async fn do_discover(_ctx: &mut crate::CliContext, Discover { image, tenant, network }: &Discover) -> anyhow::Result<()> {
    let connector_name = image.rsplit_once('/').expect("image must include slashes").1.split_once(':').expect("image must include tag").0;
    let config_file = format!("{tenant}/{connector_name}.config.yaml");

    match fs::read_to_string(config_file) {
        Ok(config) => {
            let discover_output = docker_run(image, Request {
                discover: Some(request::Discover {
                    connector_type: ConnectorType::Image.into(),
                    config_json: config,
                }),
                ..Default::default()
            }).await?;

            println!("{:#?}", discover_output);

        }

        Err(_) => {
            let spec_output = docker_run(image, Request {
                spec: Some(request::Spec {
                    connector_type: ConnectorType::Image.into(),
                    config_json: "{}".to_string(),
                }),
                ..Default::default()
            }).await?;
            println!("{:#?}", spec_output);

            let tmpdir_handle = tempfile::TempDir::new().context("creating tempdir")?;
            let tmpdir = tmpdir_handle.path();

            let builds_dir = tmpdir.join("builds");
            std::fs::create_dir(&builds_dir).context("creating builds directory")?;
            tracing::debug!(?builds_dir, "using build directory");

            std::fs::write(builds_dir.join("schema.yaml"), spec_output.spec.unwrap().config_schema_json)?;

            let mut catalog = models::Catalog::default();

            // Write our catalog source file within the build directory.
            std::fs::File::create(&builds_dir.join("flow.json"))
                .and_then(|mut f| f.write_all(serde_json::to_string_pretty(&catalog).unwrap().as_bytes()))
                .context("writing catalog file")?;

            let build_id = "test";
            let db_path = builds_dir.join("test");

            println!("builds_dir {:#?}", builds_dir);

            let out = Command::new(format!("flowctl-go"))
                .arg("api")
                .arg("build")
                .arg("--build-id")
                .arg(&build_id)
                .arg("--build-db")
                .arg(&db_path)
                .arg("--fs-root")
                .arg(&builds_dir)
                .arg("--network")
                .arg(network)
                .arg("--source")
                .arg("file:///schema.yaml")
                .arg("--source-type")
                .arg("jsonSchema")
                .arg("--log.level=debug")
                .arg("--log.format=color")
                .current_dir(tmpdir)
                .output()?;

            println!("build output {:#?}", out);

            // Inspect the database for build errors.
            let db = rusqlite::Connection::open_with_flags(
                &db_path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )?;

            let mut errors = tables::Errors::new();
            let mut built_captures = tables::BuiltCaptures::new();
            let mut captures = tables::Captures::new();
            let mut meta = tables::Meta::new();
            errors.load_all(&db).context("loading build errors")?;
            built_captures.load_all(&db).context("loading build built_captures")?;
            captures.load_all(&db).context("loading build captures")?;
            meta.load_all(&db).context("loading build meta")?;

            println!("{:#?}", errors);
            println!("{:#?}", built_captures);
        }
    }


    Ok(())
}

fn pull(image: &str) -> anyhow::Result<Output> {
    Command::new("docker")
        .args(["pull", image])
        .output().map_err(|e| e.into())
}

fn inspect(image: &str) -> anyhow::Result<Output> {
    Command::new("docker")
        .args(["inspect", image])
        .output().map_err(|e| e.into())
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
    let host_connector_init = quale::which("flow-connector-init").ok_or(anyhow::anyhow!("could not locate flow-connector-init"))?;
    let host_connector_init_str = host_connector_init.into_os_string().into_string().unwrap();
    let target_connector_init = "/tmp/connector_init";

    let port = portpicker::pick_unused_port().expect("No ports free");

    let child = Command::new("docker")
        .args([&[
              "run",
              "--interactive",
              "--init",
              "--rm",
              "--log-driver=none",
              "--mount",
              &format!("type=bind,source={host_inspect_str},target={target_inspect}"),
              "--mount",
              &format!("type=bind,source={host_connector_init_str},target={target_connector_init}"),
		      "--publish", &format!("0.0.0.0:{}:{}/tcp", port, CONNECTOR_INIT_PORT),
              "--entrypoint",
              target_connector_init,
              image,
              &format!("--image-inspect-json-path={target_inspect}"),
              &format!("--port={CONNECTOR_INIT_PORT}"),
        ], args].concat())
        .stderr(Stdio::inherit())
        .spawn()
        .context("spawning docker run child")?;

    Ok((child, dir, port))
}

pub async fn docker_run(image: &str, req: Request) -> anyhow::Result<Response> {
    let (_child, _dir, port) = docker_spawn(image, &[])?;

    loop {
        eprintln!("connecting to tcp://127.0.0.1:{port}");
        let mut client = match ConnectorClient::connect(format!("tcp://127.0.0.1:{port}")).await {
            Ok(client) => client,
            Err(_) => {
                std::thread::sleep(std::time::Duration::from_millis(1000));
                continue;
            }
        };

        let mut response_stream = client.capture(stream::once(async { req })).await?;

        let response = response_stream.get_mut().message().await?;

        return response.ok_or(anyhow!("no response message"))
    }
}

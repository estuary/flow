use anyhow::anyhow;
use serde::Serialize;
use std::{fs, process::{Command, Stdio, Output, Child}};
use anyhow::Context;
use futures::stream;
use models::{ Catalog, CaptureDef, CaptureEndpoint, ConnectorConfig, Capture, ShardTemplate, CaptureBinding, Collection, CollectionDef, CompositeKey, JsonPointer, Schema };
use proto_flow::{capture::{request, Request, Response}, flow::capture_spec::ConnectorType};
use tempfile::{tempdir, TempDir};
use proto_grpc::capture::connector_client::ConnectorClient;
use serde_json::{json, value::RawValue};
use std::collections::BTreeMap;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Discover {
    /// Connector image to discover
    image: String,

    /// Tenant name
    #[clap(default_value_t = String::from("acmeCo"))]
    tenant: String,
}

fn to_yaml<T: Serialize>(input: T) -> Result<String, anyhow::Error> {
    // This is necessary for serde_json to handle `RawValue`s properly
    let json_string = serde_json::to_string(&input)?;
    return Ok(serde_yaml::to_string(&serde_yaml::from_str::<serde_yaml::Value>(&json_string)?)?)
}

fn raw_json_to_yaml(input: &str) -> Result<String, anyhow::Error> {
    return Ok(to_yaml(serde_json::from_str::<serde_json::Value>(input)?)?);
}

fn raw_yaml_to_json(input: &str) -> Result<String, anyhow::Error> {
    return Ok(serde_json::to_string(&serde_yaml::from_str::<serde_yaml::Value>(input)?)?);
}

pub async fn do_discover(_ctx: &mut crate::CliContext, Discover { image, tenant }: &Discover) -> anyhow::Result<()> {
    let root = tenant;
    let connector_name = image.rsplit_once('/').expect("image must include slashes").1.split_once(':').expect("image must include tag").0;
    let config_file = format!("{connector_name}.config.yaml");
    let catalog_file = format!("{connector_name}.flow.yaml");

    let _ = std::fs::create_dir(&root);
    let _ = std::fs::create_dir(format!("{root}/collections"));

    // If config file exists, try a Discover RPC with the config file
    if let Ok(config) = fs::read_to_string(&format!("{root}/{config_file}")) {
        let discover_output = docker_run(image, Request {
            discover: Some(request::Discover {
                connector_type: ConnectorType::Image.into(),
                config_json: raw_yaml_to_json(&config)?,
            }),
            ..Default::default()
        }).await.context("connector discover")?;

        let bindings = discover_output.discovered.unwrap().bindings;

        let mut capture_bindings: Vec<CaptureBinding> = Vec::with_capacity(bindings.len());
        let mut collections: BTreeMap<Collection, CollectionDef> = BTreeMap::new();

        // Create a catalog with the discovered bindings
        for binding in bindings.iter() {
            let collection_name = format!("{tenant}/{}", binding.recommended_name);
            let schema_file = format!("collections/{}.schema.yaml", binding.recommended_name);
            let collection = Collection::new(collection_name);
            std::fs::write(&format!("{root}/{schema_file}"), raw_json_to_yaml(&binding.document_schema_json)?)?;

            capture_bindings.push(
                CaptureBinding {
                    target: collection.clone(),
                    resource: RawValue::from_string(binding.resource_config_json.clone())?.into(),
                }
            );

            collections.insert(collection,
                CollectionDef {
                    schema: Some(Schema::new(serde_json::from_value(json!(schema_file))?)),
                    write_schema: None,
                    read_schema: None,
                    key: CompositeKey::new(binding.key.iter().map(JsonPointer::new).collect::<Vec<JsonPointer>>()),
                    derive: None,
                    derivation: None,
                    projections: Default::default(),
                    journals: Default::default(),
                }
            );
        };

        let capture_name = format!("{tenant}/{connector_name}");
        let catalog = Catalog {
            captures: BTreeMap::from([
                (Capture::new(capture_name),
                CaptureDef {
                    endpoint: CaptureEndpoint::Connector(ConnectorConfig {
                        image: image.to_string(),
                        config: serde_json::value::to_raw_value(&config_file)?.into(),
                    }),
                    bindings: capture_bindings,
                    interval: CaptureDef::default_interval(),
                    shards: ShardTemplate::default(),
                })
            ]),
            collections,
            ..Default::default()
        };

        std::fs::write(&format!("{root}/{catalog_file}"), to_yaml(&catalog)?)?;
    } else {
        // Otherwise send a Spec RPC and use that to write a sample config file
        let spec_output = docker_run(image, Request {
            spec: Some(request::Spec {
                connector_type: ConnectorType::Image.into(),
                config_json: "{}".to_string(),
            }),
            ..Default::default()
        }).await?;

        let config_schema_json = serde_json::from_str::<serde_json::Value>(&spec_output.spec.unwrap().config_schema_json)?;

        let config = jsonschema_to_sample_json(&config_schema_json, "")?;

        std::fs::write(&format!("{root}/{config_file}"), to_yaml(&config)?)?;
    }


    Ok(())
}

fn jsonschema_to_sample_json(schema: &serde_json::Value, root_ptr: &str) -> Result<serde_json::Map<String, serde_json::Value>, anyhow::Error> {
    let mut map = serde_json::Map::new();
    let default_required = Vec::new();
    let required = schema.pointer(&format!("{root_ptr}/required"))
        .and_then(|rs| rs.as_array())
        .unwrap_or(&default_required)
        .iter()
        .filter_map(|x| match x {
            serde_json::Value::String(s) => Some(s),
            _ => None
        });

    for key in required {
        let ptr = format!("{root_ptr}/properties/{key}");
        let types = schema.pointer(&format!("{ptr}/type")).expect(&format!("required property {ptr} in schema"));
        let typ = match types {
            serde_json::Value::String(t) => t,
            serde_json::Value::Array(ts) => ts.iter().filter_map(|x| match x {
                serde_json::Value::String(t) if t != "null" => Some(t),
                _ => None,
            }).next().ok_or(anyhow!("null type for required property"))?,
            _ => return Err(anyhow!("unexpected `types`"))
        };

        if typ.as_str() == "object" {
            map.insert(key.to_string(), serde_json::Value::Object(jsonschema_to_sample_json(&schema, &ptr)?));
        } else {
            let default = schema.pointer(&format!("{root_ptr}/properties/{key}/default")).map(|v| v.clone());
            map.insert(key.to_string(), match typ.as_str() {
                "string" => default.unwrap_or(json!("")),
                "number" => default.unwrap_or(json!(0)),
                "integer" => default.unwrap_or(json!(0)),
                "array" => default.unwrap_or(json!([])),
                _ => json!(null),
            });
        }
    }

    Ok(map)
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
        let mut client = match ConnectorClient::connect(format!("tcp://127.0.0.1:{port}")).await {
            Ok(client) => client,
            Err(_) => {
                std::thread::sleep(std::time::Duration::from_millis(3000));
                continue;
            }
        };

        let mut response_stream = client.capture(stream::once(async { req })).await?;

        let response = response_stream.get_mut().message().await?;

        return response.ok_or(anyhow!("no response message"))
    }
}

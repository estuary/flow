use anyhow::Context;
use proto_flow::{derive, flow};
use serde_json::json;
use std::io::{BufRead, Write};
use std::{collections::BTreeMap, process::Stdio};

mod codegen;

pub fn run() -> anyhow::Result<()> {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let mut bin = std::io::BufReader::new(stdin);
    let mut line = String::new();

    // Handle Spec and Validate requests, breaking upon an Open.
    let open = loop {
        if bin.read_line(&mut line)? == 0 {
            return Ok(()); // Clean EOF.
        };
        let request: proto_flow::derive::Request = serde_json::from_str(&line)?;

        if let Some(_) = request.spec {
            stdout.write(
                &serde_json::to_vec(&derive::Response {
                    spec: Some(derive::response::Spec {
                        protocol: 3032023,
                        config_schema_json: "{}".to_string(),
                        resource_config_schema_json: "{}".to_string(),
                        documentation_url: "https://docs.estuary.dev".to_string(),
                        oauth2: None,
                    }),
                    ..Default::default()
                })
                .unwrap(),
            )?;
        } else if let Some(request) = request.validate {
            stdout.write(
                &serde_json::to_vec(&derive::Response {
                    validated: Some(validate(request)?),
                    ..Default::default()
                })
                .unwrap(),
            )?;
        } else if let Some(_) = request.open {
            break request;
        } else {
            anyhow::bail!("unexpected request {request:?}")
        }
        stdout.write("\n".as_bytes())?;
    };

    let collection = open.open.as_ref().unwrap().collection.as_ref().unwrap();
    let derivation = collection.derivation.as_ref().unwrap();

    let config = serde_json::from_str::<Config>(&derivation.config_json).unwrap();
    let transforms = derivation
        .transforms
        .iter()
        .map(|transform| {
            let flow::collection_spec::derivation::Transform {
                lambda_config_json,
                collection,
                name,
                ..
            } = transform;

            let lambda = if lambda_config_json == "null" {
                LambdaConfig { read_only: false }
            } else {
                serde_json::from_str::<LambdaConfig>(&lambda_config_json).unwrap()
            };

            (name.as_str(), collection.as_ref().unwrap(), lambda)
        })
        .collect::<Vec<_>>();

    let temp_dir = tempfile::TempDir::new().unwrap();
    let temp_dir = temp_dir.path();

    std::fs::write(
        temp_dir.join(TYPES_NAME),
        codegen::types_ts(&collection, &transforms),
    )?;
    std::fs::write(
        temp_dir.join("deno.json"),
        json!({"imports": {format!("flow/{}.ts", collection.name): format!("./{TYPES_NAME}")}})
            .to_string(),
    )?;
    std::fs::write(temp_dir.join(MODULE_NAME), config.module)?;
    std::fs::write(temp_dir.join(MAIN_NAME), codegen::main_ts(&transforms))?;

    let mut child = std::process::Command::new("deno")
        .stdin(Stdio::piped())
        .current_dir(temp_dir)
        .args(["run", "--allow-net=api.openai.com", MAIN_NAME])
        .spawn()?;

    // Forward `open` and the remainder of stdin to `deno`.
    let mut child_stdin = child.stdin.take().unwrap();
    let _ = std::thread::spawn(move || {
        let _ = child_stdin.write_all(&serde_json::to_vec(&open).unwrap());
        let _ = child_stdin.write_all("\n".as_bytes());
        let _ = std::io::copy(&mut bin.buffer(), &mut child_stdin);
        let _ = std::io::copy(&mut bin.into_inner(), &mut child_stdin);
    });

    let status = child.wait()?;
    if !status.success() {
        anyhow::bail!("deno failed with status {status:?}");
    }

    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    module: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LambdaConfig {
    read_only: bool,
}

fn validate(validate: derive::request::Validate) -> anyhow::Result<derive::response::Validated> {
    let derive::request::Validate {
        connector_type: _,
        collection,
        config_json,
        transforms,
        shuffle_key_types: _,
        project_root,
        import_map,
    } = &validate;

    let collection = collection.as_ref().unwrap();

    let config = serde_json::from_str::<Config>(&config_json)
        .with_context(|| format!("invalid derivation configuration: {config_json}"))?;

    let transforms = transforms
        .iter()
        .map(|transform| {
            let derive::request::validate::Transform {
                lambda_config_json,
                collection,
                name,
                shuffle_lambda_config_json,
            } = transform;

            let lambda = if lambda_config_json == "null" {
                LambdaConfig { read_only: false }
            } else {
                serde_json::from_str::<LambdaConfig>(&lambda_config_json)
                    .with_context(|| format!("invalid lambda configuration for transform {name}"))?
            };

            if !shuffle_lambda_config_json.is_empty() {
                anyhow::bail!("computed shuffles are not supported yet");
            }

            Ok((name.as_str(), collection.as_ref().unwrap(), lambda))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let transforms_response = transforms
        .iter()
        .map(
            |(_, _, LambdaConfig { read_only })| derive::response::validated::Transform {
                read_only: *read_only,
            },
        )
        .collect();

    let types_url = format!("{project_root}/{GENERATED_PREFIX}/{}.ts", collection.name);
    let types_content = codegen::types_ts(&collection, &transforms);

    let mut generated_files: Vec<(String, String)> = vec![
        (types_url.clone(), types_content.clone()),
        (
            format!("{project_root}/{DENO_NAME}"),
            serde_json::to_string_pretty(
                &json!({"imports": {"flow/": format!("./{GENERATED_PREFIX}/")}}),
            )
            .unwrap(),
        ),
    ];

    // Do we need to generate a module stub?
    if !config.module.chars().any(char::is_whitespace) {
        generated_files.push((
            config.module.clone(),
            codegen::stub_ts(&collection, &transforms),
        ));

        // There's no further validation we can do.
        return Ok(derive::response::Validated {
            transforms: transforms_response,
            generated_files: generated_files.into_iter().collect(),
        });
    }

    let temp_dir = tempfile::TempDir::new().unwrap();
    let temp_dir = temp_dir.path();

    std::fs::write(temp_dir.join(TYPES_NAME), generated_files[0].1.as_bytes())?;
    std::fs::write(
        temp_dir.join("deno.json"),
        json!({"imports": {format!("flow/{}.ts", collection.name): format!("./{TYPES_NAME}")}})
            .to_string(),
    )?;
    std::fs::write(temp_dir.join(MODULE_NAME), config.module)?;
    std::fs::write(temp_dir.join(MAIN_NAME), codegen::main_ts(&transforms))?;

    let output = std::process::Command::new("deno")
        .current_dir(temp_dir)
        .args(["check", MAIN_NAME])
        .output()
        .expect("The Deno runtime is a prerequisite for TypeScript but could not be found. Please install Deno from https://deno.com");

    if !output.status.success() {
        anyhow::bail!(rewrite_deno_stderr(
            output.stderr,
            temp_dir,
            &types_url,
            &import_map
        ));
    }

    Ok(derive::response::Validated {
        transforms: transforms_response,
        generated_files: generated_files.into_iter().collect(),
    })
}

fn rewrite_deno_stderr(
    stderr: Vec<u8>,
    temp_dir: &std::path::Path,
    types_url: &str,
    import_map: &BTreeMap<String, String>,
) -> String {
    tracing::info!(?import_map, ?types_url, "re-writing deno stderr");

    let mut stderr = String::from_utf8(stderr).unwrap();

    if let Some(import) = import_map.get("/using/typescript/module") {
        stderr = stderr.replace(
            url::Url::from_file_path(temp_dir.join(MODULE_NAME))
                .unwrap()
                .as_str(),
            import,
        );
    }

    stderr = stderr.replace(
        url::Url::from_file_path(temp_dir.join(TYPES_NAME))
            .unwrap()
            .as_str(),
        types_url,
    );

    stderr
}

const DENO_NAME: &str = "deno.json";
const GENERATED_PREFIX: &str = "flow_generated/typescript";
const MAIN_NAME: &str = "main.ts";
const MODULE_NAME: &str = "module.ts";
const TYPES_NAME: &str = "types.ts";

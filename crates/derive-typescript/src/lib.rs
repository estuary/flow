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
        line.clear();
        if bin.read_line(&mut line)? == 0 {
            return Ok(()); // Clean EOF.
        };
        let request: proto_flow::derive::Request = serde_json::from_str(&line)?;

        match &request.kind {
            Some(derive::request::Kind::Spec(_)) => {
                stdout.write(
                    &serde_json::to_vec(&derive::Response {
                        kind: Some(derive::response::Kind::Spec(Box::new(
                            derive::response::Spec {
                                protocol: 3032023,
                                config_schema_json: "{}".to_string().into(),
                                resource_config_schema_json: "{}".to_string().into(),
                                documentation_url: "https://docs.estuary.dev".to_string(),
                                oauth2: None,
                            },
                        ))),
                        ..Default::default()
                    })
                    .unwrap(),
                )?;
            }
            Some(derive::request::Kind::Validate(request)) => {
                stdout.write(
                    &serde_json::to_vec(&derive::Response {
                        kind: Some(derive::response::Kind::Validated(validate(request)?)),
                        ..Default::default()
                    })
                    .unwrap(),
                )?;
            }
            Some(derive::request::Kind::Open(_)) => break request,
            _ => anyhow::bail!("unexpected request {request:?}"),
        }
        stdout.write("\n".as_bytes())?;
    };

    let Some(derive::request::Kind::Open(open_kind)) = &open.kind else {
        unreachable!("loop breaks only on an Open request");
    };
    let collection = open_kind.collection.as_ref().unwrap();
    let derivation = collection.derivation.as_ref().unwrap();

    let config = serde_json::from_slice::<Config>(&derivation.config_json)
        .context("failed to parse derivation config")?;
    connector_environment::validate_deno(&config.environment)
        .context("invalid derivation environment")?;
    let transforms = derivation
        .resolved_transforms()
        .map(|(transform, resolved)| {
            let flow::collection_spec::derivation::Transform {
                lambda_config_json,
                collection: _,
                name,
                ..
            } = transform;

            let lambda = if lambda_config_json == "null" {
                LambdaConfig { read_only: false }
            } else {
                serde_json::from_slice::<LambdaConfig>(lambda_config_json).unwrap()
            };

            (name.as_str(), resolved.unwrap().0, lambda)
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

    tracing::debug!(
        environment = ?config.environment.keys().collect::<Vec<_>>(),
        "starting TypeScript derivation"
    );

    let mut command = deno_command(temp_dir, &config.environment);
    command.args(["run", "--allow-net=api.openai.com"]);
    if let Some(permission) = deno_environment_permission(&config.environment) {
        command.arg(permission);
    }
    let mut child = command.stdin(Stdio::piped()).arg(MAIN_NAME).spawn()?;

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
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    environment: std::collections::BTreeMap<String, String>,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LambdaConfig {
    read_only: bool,
}

fn validate(validate: &derive::request::Validate) -> anyhow::Result<derive::response::Validated> {
    let derive::request::Validate {
        connector_type: _,
        collection,
        config_json,
        transforms: _,
        shuffle_key_types: _,
        project_root,
        import_map,
        ..
    } = validate;

    let collection = collection.as_ref().unwrap();

    let config = serde_json::from_slice::<Config>(config_json)
        .context("invalid derivation configuration")?;
    connector_environment::validate_deno(&config.environment)
        .context("invalid derivation environment")?;

    let transforms = validate
        .resolved_transforms()
        .map(|(transform, resolved)| {
            let derive::request::validate::Transform {
                lambda_config_json,
                collection: _,
                name,
                shuffle_lambda_config_json,
                ..
            } = transform;

            let lambda = if lambda_config_json == "null" {
                LambdaConfig { read_only: false }
            } else {
                serde_json::from_slice::<LambdaConfig>(lambda_config_json)
                    .with_context(|| format!("invalid lambda configuration for transform {name}"))?
            };

            if !shuffle_lambda_config_json.is_empty() {
                anyhow::bail!("computed shuffles are not supported yet");
            }

            let (source, _identity) = resolved.context("transform missing source collection")?;

            Ok((name.as_str(), source, lambda))
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
    tracing::debug!(%types_content, "generated TS types");

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

    tracing::debug!(
        environment = ?config.environment.keys().collect::<Vec<_>>(),
        "validating TypeScript derivation"
    );

    let output = deno_command(temp_dir, &config.environment)
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

fn deno_command(
    project_dir: &std::path::Path,
    environment: &std::collections::BTreeMap<String, String>,
) -> std::process::Command {
    let mut command = std::process::Command::new("deno");
    command.current_dir(project_dir).envs(environment);
    command
}

fn deno_environment_permission(
    environment: &std::collections::BTreeMap<String, String>,
) -> Option<String> {
    (!environment.is_empty()).then(|| {
        format!(
            "--allow-env={}",
            environment.keys().cloned().collect::<Vec<_>>().join(",")
        )
    })
}

#[cfg(test)]
mod test {
    use super::{deno_command, deno_environment_permission};
    use std::collections::BTreeMap;

    #[test]
    fn deno_command_applies_environment() {
        let environment = BTreeMap::from([
            ("API_KEY".to_string(), "secret-value".to_string()),
            ("REGION".to_string(), "us-east-1".to_string()),
        ]);
        let command = deno_command(std::path::Path::new("/tmp/project"), &environment);
        let actual: BTreeMap<_, _> = command
            .get_envs()
            .map(|(name, value)| {
                (
                    name.to_str().unwrap(),
                    value.and_then(std::ffi::OsStr::to_str).unwrap(),
                )
            })
            .collect();

        assert_eq!(
            actual,
            BTreeMap::from([("API_KEY", "secret-value"), ("REGION", "us-east-1"),])
        );
        assert_eq!(
            command.get_current_dir(),
            Some(std::path::Path::new("/tmp/project"))
        );
    }

    #[test]
    fn deno_permission_is_an_exact_sorted_allowlist() {
        assert_eq!(deno_environment_permission(&BTreeMap::new()), None);
        assert_eq!(
            deno_environment_permission(&BTreeMap::from([
                ("REGION".to_string(), "us-east-1".to_string()),
                ("API_KEY".to_string(), "secret-value".to_string()),
            ])),
            Some("--allow-env=API_KEY,REGION".to_string())
        );
    }
}

const DENO_NAME: &str = "deno.json";
const GENERATED_PREFIX: &str = "flow_generated/typescript";
const MAIN_NAME: &str = "main.ts";
const MODULE_NAME: &str = "module.ts";
const TYPES_NAME: &str = "types.ts";

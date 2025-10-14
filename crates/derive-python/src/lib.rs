use anyhow::Context;
use proto_flow::derive;
use std::io::{BufRead, Write};

pub fn run() -> anyhow::Result<()> {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let mut bin = std::io::BufReader::new(stdin);
    let mut line = String::new();

    // Handle Spec and Validate requests, breaking upon an Open.
    let _open = loop {
        if bin.read_line(&mut line)? == 0 {
            return Ok(()); // Clean EOF.
        };
        let request: proto_flow::derive::Request = serde_json::from_str(&line)?;

        if let Some(_) = request.spec {
            stdout.write(
                &serde_json::to_vec(&derive::Response {
                    spec: Some(derive::response::Spec {
                        protocol: 3032023,
                        config_schema_json: "{}".to_string().into(),
                        resource_config_schema_json: "{}".to_string().into(),
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

    // For now, just return success. Later phases will implement actual runtime execution.
    tracing::info!("Open request received - runtime execution not yet implemented");

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
        project_root: _,
        import_map: _,
        ..
    } = &validate;

    let collection = collection.as_ref().unwrap();

    let config = serde_json::from_slice::<Config>(config_json)
        .with_context(|| format!("invalid derivation configuration: {config_json:?}"))?;

    let transforms = transforms
        .iter()
        .map(|transform| {
            let derive::request::validate::Transform {
                lambda_config_json,
                collection,
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

    tracing::info!(
        collection_name = %collection.name,
        module = %config.module,
        transform_count = transforms.len(),
        "validating Python derivation"
    );

    // For Phase 1, return minimal validation response without generated files
    // Later phases will generate Pydantic types and validate with pyright
    Ok(derive::response::Validated {
        transforms: transforms_response,
        generated_files: Default::default(),
    })
}

use anyhow::Context;
use proto_flow::{derive, flow};
use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::process::Stdio;

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

    // Extract collection and derivation from Open message
    let collection = open
        .open
        .as_ref()
        .unwrap()
        .collection
        .as_ref()
        .context("Open request missing collection")?;
    let derivation = collection
        .derivation
        .as_ref()
        .context("Collection missing derivation")?;

    let config = serde_json::from_slice::<Config>(&derivation.config_json)
        .context("Failed to parse derivation config")?;

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
                serde_json::from_slice::<LambdaConfig>(lambda_config_json)
                    .context("Failed to parse lambda config")?
            };

            Ok((name.as_str(), collection.as_ref().unwrap(), lambda))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let (temp, gen_dir) = setup_temp_project(
        collection,
        &transforms,
        &config.module,
        &config.dependencies,
    )?;

    tracing::debug!(
        temp_dir = ?temp.path(),
        dependency_count = config.dependencies.len(),
        "wrote generated files to temp directory"
    );

    let mut child = std::process::Command::new("uv")
        .stdin(Stdio::piped())
        .current_dir(temp.path())
        .env("PYTHONPATH", gen_dir.to_str().unwrap())
        .args(["run", MAIN_NAME])
        .spawn()?;

    // Forward `open` and the remainder of stdin to the program.
    let mut child_stdin = child.stdin.take().unwrap();
    let _ = std::thread::spawn(move || {
        let _ = child_stdin.write_all(&serde_json::to_vec(&open).unwrap());
        let _ = child_stdin.write_all("\n".as_bytes());
        let _ = std::io::copy(&mut bin.buffer(), &mut child_stdin);
        let _ = std::io::copy(&mut bin.into_inner(), &mut child_stdin);
    });

    let status = child.wait()?;
    if !status.success() {
        anyhow::bail!("python failed with status: {status:?}");
    }

    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    module: String,
    #[serde(default)]
    dependencies: std::collections::BTreeMap<String, String>,
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

    let module_path_parts: Vec<String> = codegen::module_path_parts(&collection.name).collect();
    let types_url = format!(
        "{project_root}/{GENERATED_PREFIX}/{}/__init__.py",
        module_path_parts.join("/")
    );
    let mut generated_files = std::collections::BTreeMap::new();

    for (init_path, init_content) in codegen::package_init_files(&collection.name, project_root) {
        generated_files.insert(init_path, init_content);
    }
    generated_files.insert(
        types_url.clone(),
        codegen::types_py(collection, &transforms),
    );
    generated_files.insert(
        format!("{project_root}/pyproject.toml"),
        generate_pyproject_toml(&collection.name, &config.dependencies),
    );
    generated_files.insert(
        format!("{project_root}/pyrightconfig.json"),
        generate_pyrightconfig(None), // `None` means: use relative path.
    );

    // Do we need to generate a module stub?
    if !config.module.chars().any(char::is_whitespace) {
        generated_files.insert(
            config.module.clone(),
            codegen::stub_py(&collection, &transforms),
        );

        // There's no further validation we can do.
        return Ok(derive::response::Validated {
            transforms: transforms_response,
            generated_files,
        });
    }

    tracing::debug!(
        file_count = generated_files.len(),
        "generated Python types and package files"
    );

    let (temp, gen_dir) = setup_temp_project(
        collection,
        &transforms,
        &config.module,
        &config.dependencies,
    )?;

    let syntax_check = std::process::Command::new("uv")
        .current_dir(temp.path())
        .env("PYTHONPATH", gen_dir.to_str().unwrap())
        .args(["run", "-m", "py_compile", "module.py", "main.py"])
        .output()?;

    if !syntax_check.status.success() {
        let stderr = String::from_utf8_lossy(&syntax_check.stderr);
        anyhow::bail!(
            "Python syntax check failed:\n{}",
            rewrite_python_stderr(&stderr, temp.path(), &types_url, &import_map)
        );
    }

    let type_check = std::process::Command::new("uv")
        .current_dir(temp.path())
        .env("PYTHONPATH", gen_dir.to_str().unwrap())
        .args(["run", "pyright", MODULE_NAME, MAIN_NAME])
        .output()?;

    if !type_check.status.success() {
        let stderr = String::from_utf8_lossy(&type_check.stderr);
        let stdout = String::from_utf8_lossy(&type_check.stdout);
        let combined = format!("{}{}", stdout, stderr);
        anyhow::bail!(
            "Python type check failed:\n{}",
            rewrite_python_stderr(&combined, temp.path(), &types_url, &import_map)
        );
    }

    tracing::info!(
        collection_name = %collection.name,
        "validation successful"
    );

    Ok(derive::response::Validated {
        transforms: transforms_response,
        generated_files,
    })
}

/// Rewrite Python error messages to replace temp paths with project paths.
fn rewrite_python_stderr(
    stderr: &str,
    temp_dir: &std::path::Path,
    types_url: &str,
    import_map: &BTreeMap<String, String>,
) -> String {
    tracing::info!(?import_map, ?types_url, "re-writing python stderr");

    let mut stderr = stderr.to_string();

    if let Some(import) = import_map.get("/using/python/module") {
        stderr = stderr.replace(
            url::Url::from_file_path(temp_dir.join(MODULE_NAME))
                .unwrap()
                .as_str(),
            import,
        );
    }

    stderr = stderr.replace(
        url::Url::from_file_path(temp_dir.join("__init__.py"))
            .unwrap()
            .as_str(),
        types_url,
    );

    stderr
}

/// Setup temp directory structure for connector execution or validation.
/// Returns (temp_dir, python_gen_dir_path).
fn setup_temp_project(
    collection: &flow::CollectionSpec,
    transforms: &[(&str, &flow::CollectionSpec, LambdaConfig)],
    module: &str,
    dependencies: &std::collections::BTreeMap<String, String>,
) -> anyhow::Result<(tempfile::TempDir, std::path::PathBuf)> {
    let temp = tempfile::TempDir::new()?;
    let gen_dir = temp.path().join("flow_generated").join("python");

    // Write generated types file with proper directory structure
    let module_path_parts: Vec<String> = codegen::module_path_parts(&collection.name).collect();
    std::fs::create_dir_all(gen_dir.join(module_path_parts.join("/")))?;

    // Create __init__.py files in all parent modules, and the leaf types __init__.py
    for i in 0..module_path_parts.len() - 1 {
        let init_path = gen_dir
            .join(module_path_parts[..=i].join("/"))
            .join("__init__.py");
        std::fs::write(init_path, "")?;
    }
    std::fs::write(
        gen_dir
            .join(module_path_parts.join("/"))
            .join("__init__.py"),
        codegen::types_py(collection, transforms),
    )?;

    // Write user module
    std::fs::write(temp.path().join(MODULE_NAME), module)?;

    // Write main.py runtime wrapper
    std::fs::write(
        temp.path().join(MAIN_NAME),
        codegen::main_py(collection, transforms, "module"),
    )?;

    // Write pyproject.toml for dependency management
    std::fs::write(
        temp.path().join("pyproject.toml"),
        generate_pyproject_toml(&collection.name, dependencies),
    )?;

    // Write pyrightconfig.json for type checking (with absolute path for temp context)
    std::fs::write(
        temp.path().join("pyrightconfig.json"),
        generate_pyrightconfig(Some(gen_dir.to_str().unwrap())),
    )?;

    Ok((temp, gen_dir))
}

/// Generate pyrightconfig.json for IDE type checking.
/// If `absolute_path` is provided, uses it for extraPaths (connector context).
/// Otherwise uses GENERATED_PREFIX for relative paths (user development context).
fn generate_pyrightconfig(absolute_path: Option<&str>) -> String {
    let extra_paths = if let Some(path) = absolute_path {
        vec![path.to_string()]
    } else {
        vec![GENERATED_PREFIX.to_string()]
    };

    let config = serde_json::json!({
        "extraPaths": extra_paths,
        "typeCheckingMode": "strict",
    });

    serde_json::to_string_pretty(&config).unwrap()
}

/// Generate pyproject.toml with project dependencies and IDE configuration.
fn generate_pyproject_toml(
    collection_name: &str,
    dependencies: &std::collections::BTreeMap<String, String>,
) -> String {
    let mut result = String::new();

    let mut dependencies = dependencies.clone();

    // `pydantic` and `pyright` are required dependencies.
    if dependencies.get("pydantic").is_none() {
        dependencies.insert("pydantic".to_string(), ">=2.0".to_string());
    }
    if dependencies.get("pyright").is_none() {
        dependencies.insert("pyright".to_string(), ">=1.1".to_string());
    }

    result.push_str("[project]\n");
    result.push_str(&format!(
        "name = \"{}\"\n",
        collection_name.replace('/', "-")
    ));
    result.push_str("version = \"0.1.0\"\n");
    result.push_str("requires-python = \">=3.12\"\n");
    result.push_str("dependencies = [\n");

    for (package, version) in dependencies {
        result.push_str(&format!("    \"{}{}\",\n", package, version));
    }
    result.push_str("]\n\n");

    // Add tool configuration for language servers
    result.push_str(
        r#"[tool.pylsp-mypy]
enabled = true

# IDE configuration is in pyrightconfig.json
"#,
    );

    result
}

const GENERATED_PREFIX: &str = "flow_generated/python";
const MAIN_NAME: &str = "main.py";
const MODULE_NAME: &str = "module.py";

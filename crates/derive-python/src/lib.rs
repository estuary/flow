use anyhow::Context;
use proto_flow::{derive, flow};
use python_connector::{Install, LOCK_FILE, PYPROJECT, Project, Spec};
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
        line.clear();
        if bin.read_line(&mut line)? == 0 {
            return Ok(()); // Clean EOF.
        };
        let request: proto_flow::derive::Request = serde_json::from_str(&line)?;

        let response = match request.kind {
            Some(derive::request::Kind::Spec(spec)) => {
                derive::response::Kind::Spec(Box::new(spec_response(&spec.config_json)?))
            }
            Some(derive::request::Kind::Validate(validate)) => {
                derive::response::Kind::Validated(validate_derivation(&validate)?)
            }
            Some(derive::request::Kind::Open(open)) => break open,
            kind => anyhow::bail!("unexpected request {kind:?}"),
        };
        stdout.write_all(
            &serde_json::to_vec(&derive::Response {
                kind: Some(response),
                ..Default::default()
            })
            .unwrap(),
        )?;
        stdout.write_all("\n".as_bytes())?;
    };

    let collection = open
        .collection
        .as_ref()
        .context("Open request missing collection")?;
    let derivation = collection
        .derivation
        .as_ref()
        .context("Collection missing derivation")?;

    let transforms = resolve_transforms(
        derivation
            .resolved_transforms()
            .map(|(transform, resolved)| (transform.name.as_str(), resolved)),
    )?;
    let config = Config::parse(&derivation.config_json)?.context("missing derivation module")?;
    let project = stage(&config, Install::of_request(false, config.has_lock()))?;
    write_derivation(&project, collection, &transforms, &config.spec)?;

    // User code sees only its own configuration: that of a sentinel, without
    // the sentinel, or the empty configuration of a legacy derivation (whose
    // `module` and `dependencies` aren't configuration).
    let mut open: serde_json::Value = serde_json::from_str(&line)?;
    if let Some(user) = open.pointer_mut("/open/collection/derivation/config") {
        *user = serde_json::Value::Object(config.user.clone());
    }

    tracing::debug!(temp_dir = ?project.root(), "starting Python derivation");

    let mut child = project
        .command(&["python", MAIN_NAME])
        .stdin(Stdio::piped())
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

/// Configuration of a Python derivation.
#[derive(Debug)]
struct Config {
    /// The user's own configuration, which the derivation parses.
    user: serde_json::Map<String, serde_json::Value>,
    /// Name of the derived collection. Absent in legacy configurations.
    collection: Option<String>,
    /// Source of the user's module, or the URL of a module which doesn't yet exist.
    module: String,
    /// Additional files, keyed on their path relative to the project root.
    files: BTreeMap<String, String>,
    /// Dependencies of a legacy configuration, which has no `pyproject.toml`.
    dependencies: BTreeMap<String, String>,
    /// Declared connector spec, or the default of a legacy configuration.
    spec: Spec,
}

impl Config {
    /// Parse a connector configuration, returning None if it isn't the
    /// configuration of a derivation (such as the `{}` of an image Spec).
    ///
    /// Built specifications which predate the `_python` sentinel have a
    /// top-level `module` and optional `files` and `dependencies`,
    /// and are accepted until they're re-published.
    fn parse(config_json: &[u8]) -> anyhow::Result<Option<Self>> {
        if config_json.is_empty() {
            return Ok(None);
        }
        let (user, sentinel) = python_connector::split_sentinel(config_json)
            .context("invalid derivation configuration")?;

        if let Some(sentinel) = sentinel {
            #[derive(serde::Deserialize)]
            struct Sentinel {
                collection: String,
                module: String,
                #[serde(default)]
                files: BTreeMap<String, String>,
            }
            let spec = Spec::of_sentinel(&sentinel)?;
            let Sentinel {
                collection,
                module,
                files,
            } = serde_json::from_value(sentinel).context("invalid `_python` configuration")?;

            return Ok(Some(Self {
                user,
                collection: Some(collection),
                module,
                files,
                dependencies: BTreeMap::new(),
                spec,
            }));
        }

        #[derive(serde::Deserialize)]
        struct Legacy {
            module: String,
            #[serde(default)]
            files: BTreeMap<String, serde_json::Value>,
            #[serde(default)]
            dependencies: BTreeMap<String, String>,
            #[serde(default)]
            environment: BTreeMap<String, String>,
        }
        if !user.contains_key("module") {
            return Ok(None);
        }
        let Legacy {
            module,
            files,
            dependencies,
            environment,
        } = serde_json::from_value(serde_json::Value::Object(user))
            .context("invalid derivation configuration")?;

        if !environment.is_empty() {
            anyhow::bail!(
                "derivation `environment` is no longer supported: use `config` with a `secrets` stanza"
            );
        }

        Ok(Some(Self {
            user: serde_json::Map::new(),
            collection: None,
            module,
            files: files
                .into_iter()
                .map(|(key, value)| {
                    let content = serialize_legacy_file(&key, &value);
                    (key, content)
                })
                .collect(),
            dependencies,
            spec: Spec::default(),
        }))
    }

    /// Is the module an unresolved URL, as when `flowctl generate` is
    /// asked to stub a module which doesn't yet exist?
    fn is_module_missing(&self) -> bool {
        !self.module.chars().any(char::is_whitespace)
    }

    /// Does the project list its own lock?
    fn has_lock(&self) -> bool {
        self.files.contains_key(LOCK_FILE)
    }

    /// Python source paths of this derivation, relative to the project root.
    fn python_sources(&self) -> Vec<&str> {
        [MODULE_NAME, MAIN_NAME]
            .into_iter()
            .chain(
                self.files
                    .keys()
                    .filter(|key| key.ends_with(".py"))
                    .map(String::as_str),
            )
            .collect()
    }

    fn default_pyproject(&self) -> String {
        let name = self.collection.as_deref().unwrap_or("derivation");
        python_connector::default_pyproject(&name.replace('/', "-"), &self.dependencies)
    }
}

/// Legacy `files` were written as YAML or JSON if they weren't text.
fn serialize_legacy_file(key: &str, value: &serde_json::Value) -> String {
    if let serde_json::Value::String(content) = value {
        return content.clone();
    }
    if key.ends_with(".yaml") || key.ends_with(".yml") {
        serde_yaml::to_string(value).expect("a Value always serializes as YAML")
    } else {
        serde_json::to_string_pretty(value).expect("a Value always serializes")
    }
}

/// Answer a Spec from the spec declared by the derivation's model,
/// without staging or running its code. A legacy configuration
/// (or the `{}` of a bare image Spec) declares none.
fn spec_response(config_json: &[u8]) -> anyhow::Result<derive::response::Spec> {
    let spec = Config::parse(config_json)?
        .map(|config| config.spec)
        .unwrap_or_default();

    Ok(derive::response::Spec {
        protocol: python_connector::PROTOCOL,
        config_schema_json: spec.config_schema.to_string().into(),
        resource_config_schema_json: spec.resource_config_schema.to_string().into(),
        documentation_url: "https://docs.estuary.dev".to_string(),
        oauth2: spec
            .oauth2
            .map(serde_json::from_value)
            .transpose()
            .context("invalid `spec.oauth2`")?,
    })
}

fn validate_derivation(
    validate: &derive::request::Validate,
) -> anyhow::Result<derive::response::Validated> {
    let derive::request::Validate {
        collection,
        config_json,
        project_root,
        ..
    } = validate;

    let collection = collection.as_ref().context("Validate missing collection")?;
    let config = Config::parse(config_json)?.context("missing derivation module")?;
    let project_root = project_root.trim_end_matches('/');

    let transforms = resolve_transforms(
        validate
            .resolved_transforms()
            .map(|(transform, resolved)| {
                if !transform.shuffle_lambda_config_json.is_empty() {
                    anyhow::bail!("computed shuffles are not supported yet");
                }
                Ok((transform.name.as_str(), resolved))
            })
            .collect::<anyhow::Result<Vec<_>>>()?
            .into_iter(),
    )?;

    let mut generated_files: BTreeMap<String, String> = python_connector::module_files(
        &collection.name,
        codegen::types_py(collection, &transforms, &config.spec)?,
    )
    .into_iter()
    .map(|(path, content)| (format!("{project_root}/{path}"), content))
    .collect();
    if !config.files.contains_key(PYPROJECT) {
        generated_files.insert(
            format!("{project_root}/{PYPROJECT}"),
            config.default_pyproject(),
        );
    }

    // Do we need to generate a module stub? There's no further validation we
    // can do, and no code to decide whether its transforms are read-only.
    if config.is_module_missing() {
        generated_files.insert(
            config.module.clone(),
            codegen::stub_py(collection, &transforms),
        );
        return Ok(derive::response::Validated {
            transforms: vec![Default::default(); transforms.len()],
            generated_files,
        });
    }

    let project = stage(&config, Install::of_request(true, config.has_lock()))?;
    write_derivation(&project, collection, &transforms, &config.spec)?;

    if project.has_tool("pyright") {
        project
            .run(
                &[&["pyright"], config.python_sources().as_slice()].concat(),
                None,
            )
            .map_err(|err| anyhow::anyhow!("Python type check failed:\n{err}"))?;
    }

    // The derivation's own `validate` decides whether each transform is
    // read-only, and may fail validation outright.
    let validated = project
        .run(
            &["python", MAIN_NAME, "validate"],
            Some(&serde_json::to_vec(&validate_input(&config, validate)).unwrap()),
        )
        .map_err(|err| anyhow::anyhow!("derivation validation failed:\n{err}"))?;
    let mut validated = parse_validated(&validated)?;

    if validated.transforms.len() != transforms.len() {
        anyhow::bail!(
            "derivation `validate` returned {} transforms, but the derivation has {}",
            validated.transforms.len(),
            transforms.len(),
        );
    }
    if !config.has_lock() {
        if let Some(lock) = project.lock() {
            generated_files.insert(format!("{project_root}/{LOCK_FILE}"), lock.to_string());
        }
    }
    validated.generated_files = generated_files;

    tracing::info!(collection_name = %collection.name, "validation successful");

    Ok(validated)
}

/// Parse the Validated response which `main.py validate` printed. Anything
/// else the derivation printed (such as a stray `print()`) is included in an
/// error, so that it can be diagnosed.
fn parse_validated(stdout: &[u8]) -> anyhow::Result<derive::response::Validated> {
    serde_json::from_slice(stdout).with_context(|| {
        const MAX: usize = 4096;
        let stdout = String::from_utf8_lossy(stdout);
        let truncated: String = stdout.chars().take(MAX).collect();
        let ellipsis = if truncated.len() < stdout.len() {
            "..."
        } else {
            ""
        };

        format!(
            "failed to parse the derivation's Validated response; is the module \
             printing to stdout? Its output was:\n{truncated}{ellipsis}"
        )
    })
}

/// Input of `main.py validate`: the derivation's own configuration, and
/// the `Request.Validate` of its generated types.
fn validate_input(config: &Config, validate: &derive::request::Validate) -> serde_json::Value {
    let transforms: Vec<serde_json::Value> = validate
        .transforms
        .iter()
        .map(|transform| {
            serde_json::json!({
                "name": transform.name,
                "resourceConfig": resource_config(&transform.lambda_config_json),
            })
        })
        .collect();

    serde_json::json!({
        "config": config.user,
        "validate": {
            "name": validate.collection.as_ref().map(|c| c.name.as_str()),
            "transforms": transforms,
        },
    })
}

/// The resource configuration of a transform is its `lambda`,
/// which is an empty object if it's omitted.
fn resource_config(lambda_config_json: &[u8]) -> serde_json::Value {
    if lambda_config_json.is_empty() || lambda_config_json == b"null" {
        return serde_json::json!({});
    }
    serde_json::from_slice(lambda_config_json).unwrap_or(serde_json::Value::Null)
}

/// Resolve transforms into their names and source collections.
fn resolve_transforms<'a>(
    transforms: impl Iterator<Item = (&'a str, Option<proto_flow::linked::Resolved<'a>>)>,
) -> anyhow::Result<Vec<(&'a str, &'a flow::CollectionSpec)>> {
    transforms
        .map(|(name, resolved)| {
            let (source, _identity) = resolved.context("transform missing source collection")?;
            Ok((name, source))
        })
        .collect()
}

/// Stage the project of a derivation, and install its dependencies.
/// Its generated types and `main.py` are written by `write_derivation`.
fn stage(config: &Config, install: Install) -> anyhow::Result<Project> {
    let mut project = Project::stage(
        config
            .files
            .iter()
            .map(|(path, content)| (path.as_str(), content.as_str())),
    )?;
    if !project.exists(PYPROJECT) {
        project.write(PYPROJECT, &config.default_pyproject())?;
    }
    project.write(MODULE_NAME, &config.module)?;
    project.install(install)?;

    tracing::debug!(temp_dir = ?project.root(), ?install, "staged Python derivation");

    Ok(project)
}

/// Write the generated types and `main.py` of a derivation into its project.
fn write_derivation(
    project: &Project,
    collection: &flow::CollectionSpec,
    transforms: &[(&str, &flow::CollectionSpec)],
    spec: &Spec,
) -> anyhow::Result<()> {
    let types = codegen::types_py(collection, transforms, spec)?;

    for (path, content) in python_connector::module_files(&collection.name, types) {
        project.write(&path, &content)?;
    }
    project.write(
        MAIN_NAME,
        &codegen::main_py(collection, transforms, "module"),
    )
}

const MAIN_NAME: &str = "main.py";
const MODULE_NAME: &str = "module.py";

#[cfg(test)]
mod test {
    use super::{Config, spec_response};

    #[test]
    fn configurations_are_parsed() {
        let current = br#"{
            "region": "north",
            "_python": {
                "collection": "acmeCo/orders",
                "module": "class Derivation: pass\n",
                "files": {"lib/geo.py": "def region_for(doc):\n    return doc\n"},
                "spec": {"configSchema": {"type": "object"}, "resourceConfigSchema": {}}
            }
        }"#;
        let legacy = br#"{
            "module": "class Derivation: pass\n",
            "files": {
                "lib/geo.py": "def region_for(doc):\n    return doc\n",
                "data/regions.json": {"north": 1},
                "data/config.yaml": {"retries": 3}
            },
            "dependencies": {"httpx": ">=0.27"}
        }"#;

        let current = Config::parse(current).unwrap().unwrap();
        let legacy = Config::parse(legacy).unwrap().unwrap();

        insta::assert_debug_snapshot!((
            &current,
            current.python_sources(),
            &legacy,
            Config::parse(b"{}").unwrap().is_none(),
            Config::parse(b"").unwrap().is_none(),
            Config::parse(br#"{"module": "m.py", "environment": {"A": "b"}}"#)
                .unwrap_err()
                .to_string(),
        ));
    }

    #[test]
    fn unparseable_validated_responses_include_output() {
        let err = super::parse_validated(b"debugging!\n{\"transforms\":[{}]}\n").unwrap_err();
        insta::assert_snapshot!(format!("{err:#}"));
        assert_eq!(
            super::parse_validated(br#"{"transforms":[{}]}"#)
                .unwrap()
                .transforms
                .len(),
            1
        );
    }

    #[test]
    fn specs_are_answered_from_the_sentinel() {
        let declared = serde_json::json!({
            "apiKey": "hunter2",
            "_python": {
                "collection": "acmeCo/orders",
                "module": "class Derivation: pass\n",
                "spec": {
                    "configSchema": {
                        "type": "object",
                        "properties": {"apiKey": {"type": "string", "secret": true}},
                    },
                    "resourceConfigSchema": {
                        "type": "object",
                        "properties": {"readOnly": {"type": "boolean", "default": false}},
                    },
                    "oauth2": {
                        "provider": "acme",
                        "authUrlTemplate": "https://acme.example/authorize",
                        "accessTokenUrlTemplate": "https://acme.example/token",
                        "accessTokenResponseMap": {"access_token": "/access_token"},
                    },
                },
            },
        });

        let responses = serde_json::to_value([
            spec_response(declared.to_string().as_bytes()).unwrap(),
            // Legacy configurations (and a bare image Spec) declare no spec.
            spec_response(br#"{"module": "class Derivation: pass\n"}"#).unwrap(),
            spec_response(b"{}").unwrap(),
        ])
        .unwrap();
        insta::assert_json_snapshot!(responses);
    }
}

use anyhow::Context;
use models::custom_connector::{Language, Sentinel, Spec};
use proto_flow::{derive, flow};
use python_connector::{ENTRY, Install, Project};
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
    let mut open = loop {
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
                derive::response::Kind::Validated(validate_derivation(*validate)?)
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

    // `collection` and its `derivation` are taken from `open` to be staged,
    // and are restored after `config_json` is rewritten.
    let mut collection = open
        .collection
        .take()
        .context("Open request missing collection")?;
    let mut derivation = collection
        .derivation
        .take()
        .context("Collection missing derivation")?;

    let transforms = resolve_transforms(
        derivation
            .resolved_transforms()
            .map(|(transform, resolved)| (transform.name.as_str(), resolved)),
    )?;
    let config = Config::parse(&derivation.config_json, &collection.name)?
        .context("missing derivation module")?;
    let project = stage(
        &config.sentinel,
        &collection,
        &transforms,
        Install::of_request(false, config.sentinel.has_lock(Language::Python)),
    )?;

    // User code sees only its own configuration: that of a sentinel, without
    // the sentinel, or the empty configuration of a legacy derivation (whose
    // `module` and `dependencies` aren't configuration).
    derivation.config_json = serde_json::to_vec(&config.user).unwrap().into();
    collection.derivation = Some(derivation);
    open.collection = Some(collection);

    let open = derive::Request {
        kind: Some(derive::request::Kind::Open(open)),
        ..Default::default()
    };

    tracing::debug!(temp_dir = ?project.root(), "starting Python derivation");

    let mut child = project
        .command(&["python", ENTRY])
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
    /// Sentinel of the derivation, whose declared spec has its defaults
    /// resolved. A listed file which failed to load is given a starter.
    sentinel: Sentinel,
}

impl Config {
    /// Parse the connector configuration of derivation `task`, returning
    /// None if it isn't the configuration of a derivation (such as the `{}`
    /// of an image Spec).
    ///
    /// Built specifications which predate the `_python` sentinel have a
    /// top-level `module`, and are accepted until they're re-published.
    /// They're staged as the files which validation builds a legacy model
    /// with (`Language::legacy_files`). Their `dependencies` and
    /// `environment` are no longer supported, and must be empty.
    fn parse(config_json: &[u8], task: &str) -> anyhow::Result<Option<Self>> {
        if config_json.is_empty() {
            return Ok(None);
        }
        let (user, sentinel) = Sentinel::split(config_json, Language::Python)
            .context("invalid derivation configuration")?;

        if let Some(mut sentinel) = sentinel {
            sentinel.spec = sentinel.spec.resolve_derivation();
            return Ok(Some(Self { user, sentinel }));
        }

        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Legacy {
            module: String,
            #[serde(default)]
            dependencies: serde_json::Map<String, serde_json::Value>,
            #[serde(default)]
            environment: serde_json::Map<String, serde_json::Value>,
        }
        if !user.contains_key("module") {
            return Ok(None);
        }
        let Legacy {
            module,
            dependencies,
            environment,
        } = serde_json::from_value(serde_json::Value::Object(user))
            .context("invalid derivation configuration")?;

        if !dependencies.is_empty() {
            anyhow::bail!(
                "dependencies are no longer supported; declare them in a `pyproject.toml` listed in `files`"
            );
        }
        if !environment.is_empty() {
            anyhow::bail!(
                "environment is no longer supported; use `config` with a `secrets` stanza"
            );
        }

        Ok(Some(Self {
            user: serde_json::Map::new(),
            sentinel: Sentinel {
                task: task.to_string(),
                files: Language::Python.legacy_files(task, module),
                spec: Spec::default().resolve_derivation(),
            },
        }))
    }

    /// Python source paths of this derivation, relative to the project root,
    /// which are type-checked: its generated entry point, and its own files.
    fn python_sources(&self) -> Vec<&str> {
        [ENTRY]
            .into_iter()
            .chain(
                self.sentinel
                    .files
                    .keys()
                    .filter(|key| key.ends_with(".py"))
                    .map(String::as_str),
            )
            .collect()
    }
}

/// Answer a Spec from the spec declared by the derivation's model,
/// without staging or running its code. A legacy configuration
/// (or the `{}` of a bare image Spec) declares none.
fn spec_response(config_json: &[u8]) -> anyhow::Result<derive::response::Spec> {
    let sentinel = if config_json.is_empty() {
        None
    } else {
        Sentinel::split(config_json, Language::Python)
            .context("invalid derivation configuration")?
            .1
    };
    let spec = sentinel
        .map(|sentinel| sentinel.spec)
        .unwrap_or_default()
        .resolve_derivation();
    let schema_json = |schema: Option<models::Schema>| {
        schema
            .expect("resolved by resolve_derivation")
            .get()
            .to_string()
            .into()
    };

    Ok(derive::response::Spec {
        protocol: models::custom_connector::PROTOCOL,
        config_schema_json: schema_json(spec.config_schema),
        resource_config_schema_json: schema_json(spec.resource_config_schema),
        documentation_url: "https://docs.estuary.dev".to_string(),
        // `models::custom_connector::OAuth2` mirrors `flow::OAuth2`.
        oauth2: spec.oauth2.map(|oauth2| {
            serde_json::from_value(serde_json::to_value(oauth2).unwrap())
                .expect("OAuth2 of a Spec is a flow::OAuth2")
        }),
    })
}

fn validate_derivation(
    mut validate: derive::request::Validate,
) -> anyhow::Result<derive::response::Validated> {
    let task = validate
        .collection
        .as_ref()
        .context("Validate missing collection")?
        .name
        .clone();
    let config =
        Config::parse(&validate.config_json, &task)?.context("missing derivation module")?;
    let sentinel = &config.sentinel;
    // User code sees only its own configuration, as of an Open (see `run`).
    validate.config_json = serde_json::to_vec(&config.user).unwrap().into();

    let derive::request::Validate {
        collection,
        project_root,
        ..
    } = &validate;

    let collection = collection.as_ref().unwrap();
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
        codegen::types_py(collection, &transforms, &sentinel.spec)?,
    )
    .into_iter()
    .map(|(path, content)| (format!("{project_root}/{path}"), content))
    .collect();

    // A project having listed files which don't exist yet can't be run.
    // They're given starters, and there's no code to decide whether its
    // transforms are read-only. Its missing files are load errors, and it
    // can't be published.
    let missing = sentinel.missing();
    if !missing.is_empty() {
        for path in missing {
            generated_files.insert(
                format!("{project_root}/{path}"),
                starter(sentinel, collection, &transforms, path),
            );
        }
        return Ok(derive::response::Validated {
            transforms: vec![Default::default(); transforms.len()],
            generated_files,
        });
    }

    let project = stage(
        sentinel,
        collection,
        &transforms,
        Install::of_request(true, sentinel.has_lock(Language::Python)),
    )?;

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
            &["python", ENTRY, "validate"],
            Some(&serde_json::to_vec(&validate).unwrap()),
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
    // A lock resolved by this session is returned to be baked into the
    // built specification.
    if !sentinel.has_lock(Language::Python) {
        if let Some(lock) = project.lock() {
            generated_files.insert(
                format!("{project_root}/{}", Language::Python.lock()),
                lock.to_string(),
            );
        }
    }
    validated.generated_files = generated_files;

    tracing::info!(collection_name = %collection.name, "validation successful");

    Ok(validated)
}

/// Starter content of a listed file at `path` which doesn't exist yet:
/// a working derivation as its entry, a project manifest, or else empty.
fn starter(
    sentinel: &Sentinel,
    collection: &flow::CollectionSpec,
    transforms: &[(&str, &flow::CollectionSpec)],
    path: &str,
) -> String {
    if path == Language::Python.entry(sentinel.dir()) {
        codegen::starter_py(collection, transforms)
    } else if path == Language::Python.manifest() {
        Language::Python.starter_manifest(sentinel.dir())
    } else {
        String::new()
    }
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

/// Stage the project of a derivation with its generated types and entry
/// point, and install its dependencies.
fn stage(
    sentinel: &Sentinel,
    collection: &flow::CollectionSpec,
    transforms: &[(&str, &flow::CollectionSpec)],
    install: Install,
) -> anyhow::Result<Project> {
    let types = codegen::types_py(collection, transforms, &sentinel.spec)?;
    let mut generated = python_connector::module_files(&collection.name, types);
    generated.push((
        ENTRY.to_string(),
        codegen::main_py(collection, transforms, sentinel.dir()),
    ));

    let mut project = Project::stage(
        sentinel
            .files
            .iter()
            .filter_map(|(path, content)| Some((path.as_str(), content.as_deref()?)))
            .chain(
                generated
                    .iter()
                    .map(|(path, content)| (path.as_str(), content.as_str())),
            ),
    )?;
    project.install(install)?;

    tracing::debug!(temp_dir = ?project.root(), ?install, "staged Python derivation");

    Ok(project)
}

#[cfg(test)]
mod test {
    use super::{Config, spec_response};

    #[test]
    fn configurations_are_parsed() {
        let current = br#"{
            "region": "north",
            "_python": {
                "task": "acmeCo/orders",
                "files": {
                    "pyproject.toml": "[project]\n",
                    "orders/__init__.py": "class Derivation: pass\n",
                    "orders/geo.py": "def region_for(doc):\n    return doc\n",
                    "orders/missing.py": null
                },
                "spec": {"configSchema": {"type": "object"}}
            }
        }"#;
        let legacy = br#"{
            "module": "class Derivation: pass\n",
            "dependencies": {}
        }"#;

        let current = Config::parse(current, "acmeCo/orders").unwrap().unwrap();
        let legacy = Config::parse(legacy, "acmeCo/orders").unwrap().unwrap();

        insta::assert_debug_snapshot!((
            &current,
            current.python_sources(),
            current.sentinel.missing(),
            &legacy,
            Config::parse(b"{}", "acmeCo/orders").unwrap().is_none(),
            Config::parse(b"", "acmeCo/orders").unwrap().is_none(),
            format!(
                "{:#}",
                Config::parse(
                    br#"{"module": "m.py", "dependencies": {"httpx": ">=0.27"}}"#,
                    "acmeCo/orders"
                )
                .unwrap_err()
            ),
            format!(
                "{:#}",
                Config::parse(
                    br#"{"module": "m.py", "environment": {"A": "b"}}"#,
                    "acmeCo/orders"
                )
                .unwrap_err()
            ),
            format!(
                "{:#}",
                Config::parse(br#"{"module": "m.py", "other": true}"#, "acmeCo/orders")
                    .unwrap_err()
            ),
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
                "task": "acmeCo/orders",
                "spec": {
                    "configSchema": {
                        "type": "object",
                        "properties": {"apiKey": {"type": "string", "secret": true}},
                    },
                    "resourceConfigSchema": {
                        "type": "object",
                        "properties": {"window": {"type": "string"}},
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
            // A sentinel which declares nothing has the connector's defaults.
            spec_response(br#"{"_python": {"task": "acmeCo/orders"}}"#).unwrap(),
            // Legacy configurations (and a bare image Spec) declare no spec.
            spec_response(br#"{"module": "class Derivation: pass\n"}"#).unwrap(),
            spec_response(b"{}").unwrap(),
        ])
        .unwrap();
        insta::assert_json_snapshot!(responses);
    }
}

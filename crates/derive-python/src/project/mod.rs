//! A derivation which is a project of `files` (a `ProjectConfig`).

use anyhow::Context;
use models::custom_connector;
use proto_flow::{derive, flow};
use python_connector::{DOCUMENTATION_URL, ENTRY, Install, Project, ProjectConfig, Staged};
use std::collections::BTreeMap;
use std::io::{BufRead, Write};

mod codegen;

/// Is `request` of a derivation which is a project? Only a `ProjectConfig`
/// has the `SENTINEL` property.
pub fn is_project(request: &derive::Request) -> bool {
    let config_json = match &request.kind {
        Some(derive::request::Kind::Spec(spec)) => &spec.config_json,
        Some(derive::request::Kind::Validate(validate)) => &validate.config_json,
        Some(derive::request::Kind::Open(open)) => match open
            .collection
            .as_ref()
            .and_then(|collection| collection.derivation.as_ref())
        {
            Some(derivation) => &derivation.config_json,
            None => return false,
        },
        _ => return false,
    };
    serde_json::from_slice::<BTreeMap<String, serde::de::IgnoredAny>>(config_json)
        .is_ok_and(|config| config.contains_key(python_connector::SENTINEL))
}

/// Serve the requests of a project: `request`, and then those of `requests`.
pub fn run(
    request: derive::Request,
    mut requests: std::io::BufReader<std::io::Stdin>,
) -> anyhow::Result<()> {
    let mut stdout = std::io::stdout();
    let mut line = String::new();
    let mut next = Some(request);

    // Handle Spec and Validate requests, breaking upon an Open.
    let mut open = loop {
        let request = match next.take() {
            Some(request) => request,
            None => {
                line.clear();
                if requests.read_line(&mut line)? == 0 {
                    return Ok(()); // Clean EOF.
                }
                serde_json::from_str(&line).context("parsing connector request")?
            }
        };

        let response = match request.kind {
            Some(derive::request::Kind::Spec(spec)) => {
                derive::response::Kind::Spec(Box::new(spec_response(&spec.config_json)))
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

    let project = project_of(&derivation.config_json)?;
    let staged = stage(
        collection,
        &project,
        &transforms,
        Install::of_request(false, project.has_lock()),
    )?;
    std::mem::drop(transforms);

    // User code sees only its own configuration, without the sentinel.
    strip_sentinels(open.collection.as_mut());

    let open = derive::Request {
        kind: Some(derive::request::Kind::Open(open)),
        ..Default::default()
    };
    tracing::debug!(temp_dir = ?staged.root(), "starting Python derivation");
    staged.hand_off(&["python", ENTRY], &open, requests)
}

/// Parse the Project of a derivation's configuration, whose declared spec
/// has its defaults resolved.
fn project_of(config_json: &[u8]) -> anyhow::Result<Project> {
    let ProjectConfig { mut project, .. } = serde_json::from_slice(config_json)
        .with_context(|| format!("invalid `{}` configuration", python_connector::SENTINEL))?;
    project.spec = resolve_spec(project.spec);
    Ok(project)
}

/// Remove the `_python` sentinel from the configuration of the derivation of
/// `collection`, before it's handed to user code.
fn strip_sentinels(collection: Option<&mut flow::CollectionSpec>) {
    if let Some(derivation) = collection.and_then(|collection| collection.derivation.as_mut()) {
        models::custom_connector::strip_sentinel(
            &mut derivation.config_json,
            python_connector::SENTINEL,
        );
    }
}

/// Resolve the schemas which a derivation's declared `spec` omits to this
/// connector's defaults: `Spec::config_schema_or_default`, and a transform
/// `lambda` of `{readOnly: boolean}`.
fn resolve_spec(mut spec: custom_connector::Spec) -> custom_connector::Spec {
    let config_schema = spec.config_schema_or_default().to_string();

    spec.config_schema = Some(models::Schema::new(
        models::RawValue::from_string(config_schema).unwrap(),
    ));
    spec.resource_config_schema.get_or_insert_with(|| {
        models::Schema::new(models::RawValue::from_value(&serde_json::json!({
            "type": "object",
            "properties": {
                "readOnly": {
                    "type": "boolean",
                    "default": false,
                    "description": "Does this transform never publish documents?",
                },
            },
        })))
    });
    spec
}

/// Answer a Spec from the spec declared by a ProjectConfig, without staging
/// or running its code.
fn spec_response(config_json: &[u8]) -> derive::response::Spec {
    let custom_connector::Spec {
        config_schema,
        resource_config_schema,
        oauth2,
        // Validation builds network ports. They aren't part of a Spec response.
        network_ports: _,
    } = resolve_spec(python_connector::declared_spec(config_json));

    let schema_json = |schema: Option<models::Schema>| {
        schema
            .expect("resolved by resolve_spec")
            .get()
            .to_string()
            .into()
    };

    derive::response::Spec {
        protocol: python_connector::PROTOCOL,
        config_schema_json: schema_json(config_schema),
        resource_config_schema_json: schema_json(resource_config_schema),
        documentation_url: DOCUMENTATION_URL.to_string(),
        oauth2: oauth2.as_ref().map(python_connector::oauth2),
    }
}

fn validate_derivation(
    mut validate: derive::request::Validate,
) -> anyhow::Result<derive::response::Validated> {
    let project = project_of(&validate.config_json)?;

    let derive::request::Validate {
        collection,
        project_root,
        ..
    } = &validate;

    let collection = collection.as_ref().unwrap();
    let project_root = project_root.trim_end_matches('/');

    if validate
        .transforms
        .iter()
        .any(|transform| !transform.shuffle_lambda_config_json.is_empty())
    {
        anyhow::bail!("computed shuffles are not supported yet");
    }
    let transforms = resolve_transforms(
        validate
            .resolved_transforms()
            .map(|(transform, resolved)| (transform.name.as_str(), resolved)),
    )?;
    let defaults = default_validated(&validate, &project.spec)?;
    let mut generated_files = python_connector::project_urls(
        project_root,
        [generated_module(collection, &project.spec, &transforms)?],
    );

    // A project having listed files which don't exist yet can't be run.
    // They're given starters, and the derivation has its defaults. Its
    // missing files are load errors, and it can't be published.
    if !project.missing().is_empty() {
        let dir = custom_connector::task_dir(&collection.name);

        generated_files.extend(python_connector::starter_files(
            &project,
            project_root,
            dir,
            codegen::starter_py(&collection.name, &transforms),
            python_connector::starter_manifest(dir),
        ));
        return Ok(derive::response::Validated {
            transforms: defaults,
            generated_files,
        });
    }

    let staged = stage(
        collection,
        &project,
        &transforms,
        Install::of_request(true, project.has_lock()),
    )?;
    std::mem::drop(transforms);
    staged.check()?;

    // User code sees only its own configuration, without the sentinel.
    models::custom_connector::strip_sentinel(&mut validate.config_json, python_connector::SENTINEL);
    strip_sentinels(validate.last_collection.as_mut());

    let input = serde_json::json!({
        "validate": validate,
        "validated": derive::response::Validated {
            transforms: defaults.clone(),
            generated_files: BTreeMap::new(),
        },
    });
    let output = staged.run(
        &["python", ENTRY, "validate"],
        &serde_json::to_vec(&input).unwrap(),
    )?;
    let validated: derive::response::Validated = serde_json::from_slice(&output)
        .context("parsing the Validated of `Derivation.validate`")?;

    if validated.transforms.len() != defaults.len() {
        anyhow::bail!(
            "Derivation.validate left {} transforms, but the derivation has {}",
            validated.transforms.len(),
            defaults.len(),
        );
    }

    Ok(derive::response::Validated {
        transforms: validated.transforms,
        generated_files,
    })
}

/// Default Validated transforms. A transform is read-only if its resource
/// configuration (`lambda`) has a true explicit or defaulted `readOnly`.
fn default_validated(
    validate: &derive::request::Validate,
    spec: &custom_connector::Spec,
) -> anyhow::Result<Vec<derive::response::validated::Transform>> {
    let (_, shape) = python_connector::config_shape(
        spec.resource_config_schema
            .as_ref()
            .expect("resolved by resolve_spec")
            .get(),
    )
    .context("invalid `resourceConfigSchema`")?;

    let ptr = json::Pointer::from_str("/readOnly");

    Ok(validate
        .transforms
        .iter()
        .map(|transform| {
            let lambda = serde_json::from_slice::<serde_json::Value>(&transform.lambda_config_json)
                .unwrap_or_default();
            let read_only = python_connector::query_config(&shape, &ptr, &lambda)
                .and_then(serde_json::Value::as_bool)
                .unwrap_or_default();

            derive::response::validated::Transform { read_only }
        })
        .collect())
}

/// Resolve transforms into their names and source collections.
fn resolve_transforms<'a>(
    transforms: impl Iterator<Item = (&'a str, Option<proto_flow::linked::Resolved<'a>>)>,
) -> anyhow::Result<Vec<codegen::Transform<'a>>> {
    let transforms = transforms
        .map(|(name, resolved)| {
            let (source, _identity) = resolved.context("transform missing source collection")?;
            Ok((name, source))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    codegen::Transform::of(transforms)
}

/// Stage the project of a derivation of `collection` with its generated
/// module and entry point, and `install` its dependencies.
fn stage(
    collection: &flow::CollectionSpec,
    project: &Project,
    transforms: &[codegen::Transform],
    install: Install,
) -> anyhow::Result<Staged> {
    let task = &collection.name;
    let generated = [
        generated_module(collection, &project.spec, transforms)?,
        (
            ENTRY.to_string(),
            codegen::main_py(task, transforms, custom_connector::task_dir(task)),
        ),
    ];
    Staged::of_project(project, &generated, install)
}

/// File of the module generated for a derivation of `collection`, relative
/// to the project.
fn generated_module(
    collection: &flow::CollectionSpec,
    spec: &custom_connector::Spec,
    transforms: &[codegen::Transform],
) -> anyhow::Result<(String, String)> {
    let content = codegen::types_py(collection, spec, transforms)
        .context("failed to generate the derivation's types")?;
    Ok((python_connector::module_path(&collection.name), content))
}

#[cfg(test)]
mod test {
    use super::{default_validated, is_project, project_of, resolve_spec, spec_response};
    use models::custom_connector::Spec;
    use proto_flow::derive::request;
    use serde_json::json;

    #[test]
    fn transforms_default_to_their_read_only() {
        let validate = request::Validate {
            transforms: [
                r#"{}"#,
                r#"{"readOnly": false}"#,
                r#"{"readOnly": true}"#,
                "null",
            ]
            .into_iter()
            .map(|lambda| request::validate::Transform {
                lambda_config_json: lambda.into(),
                ..Default::default()
            })
            .collect(),
            ..Default::default()
        };
        let read_only = |spec: serde_json::Value| {
            let spec = resolve_spec(serde_json::from_value::<Spec>(spec).unwrap());
            default_validated(&validate, &spec)
                .unwrap()
                .into_iter()
                .map(|transform| transform.read_only)
                .collect::<Vec<_>>()
        };

        // The default `readOnly` is false.
        insta::assert_debug_snapshot!(read_only(json!({})), @"
        [
            false,
            false,
            true,
            false,
        ]
        ");
        // An omitted `readOnly` is its declared default, as in `ResourceConfig`.
        // A null `lambda` has no properties, and so no defaults.
        insta::assert_debug_snapshot!(read_only(json!({"resourceConfigSchema": {
            "type": "object",
            "properties": {"readOnly": {"type": "boolean", "default": true}},
        }})), @"
        [
            true,
            false,
            true,
            false,
        ]
        ");
    }

    #[test]
    fn projects_are_distinguished_by_their_sentinel() {
        let spec = |config: serde_json::Value| {
            serde_json::from_value(json!({"spec": {"connectorType": "IMAGE", "config": config}}))
                .unwrap()
        };
        let open = |config: serde_json::Value| {
            serde_json::from_value(json!({"open": {"collection": {
                "name": "acmeCo/orders",
                "derivation": {"connectorType": "IMAGE", "config": config},
            }}}))
            .unwrap()
        };

        assert!(is_project(&spec(json!({"_python": {}}))));
        assert!(is_project(&open(json!({"module": "user", "_python": {}}))));

        // A legacy module, or the `{}` of a bare image Spec, isn't a project.
        assert!(!is_project(&spec(
            json!({"module": "class Derivation: pass\n"})
        )));
        assert!(!is_project(&open(
            json!({"module": "class Derivation: pass\n"})
        )));
        assert!(!is_project(&spec(json!({}))));
    }

    #[test]
    fn configurations_are_parsed() {
        let current = br#"{
            "region": "north",
            "_python": {
                "files": {
                    "pyproject.toml": "[project]\n",
                    "orders/__init__.py": "class Derivation: pass\n",
                    "orders/geo.py": "def region_for(doc):\n    return doc\n",
                    "orders/missing.py": null
                },
                "spec": {"configSchema": {"type": "object"}}
            }
        }"#;
        let parse = |config: &[u8]| project_of(config).map_err(|err| format!("{err:#}"));
        let current = parse(current).unwrap();

        insta::assert_debug_snapshot!((
            &current,
            parse(br#"{"_python": {"files": {"a.json": {"not": "text"}}}}"#),
            parse(b"{}"),
            parse(b""),
        ));
    }

    #[test]
    fn specs_are_answered_from_the_sentinel() {
        let declared = serde_json::json!({
            "apiKey": "hunter2",
            "_python": {
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
            spec_response(declared.to_string().as_bytes()),
            // A sentinel which declares nothing has the connector's defaults.
            spec_response(br#"{"_python": {}}"#),
            // An invalid configuration, which Validate reports, has the
            // connector's defaults.
            spec_response(br#"{"_python": {"files": []}}"#),
        ])
        .unwrap();
        insta::assert_json_snapshot!(responses);
    }
}

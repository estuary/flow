use anyhow::Context;
use models::custom_connector::{Spec, task_dir};
use proto_flow::capture::{Request, Response, request, response};
use python_connector::{DOCUMENTATION_URL, Install, Project, ProjectConfig, Staged};
use std::io::{BufRead, Write};

pub fn run() -> anyhow::Result<()> {
    let mut requests = std::io::BufReader::new(std::io::stdin());
    let mut stdout = std::io::stdout();
    let mut line = String::new();

    // Handle Spec, Discover, Validate, and Apply requests, breaking upon an Open.
    let mut open = loop {
        line.clear();
        if requests.read_line(&mut line)? == 0 {
            return Ok(()); // Clean EOF.
        }
        let request: Request = serde_json::from_str(&line).context("parsing connector request")?;

        let response = match &request.kind {
            Some(request::Kind::Spec(spec)) => {
                response::Kind::Spec(Box::new(spec_response(&spec.config_json)))
            }
            Some(request::Kind::Open(_)) => break request,
            _ => unary(request)?,
        };
        stdout.write_all(
            &serde_json::to_vec(&Response {
                kind: Some(response),
                ..Default::default()
            })
            .unwrap(),
        )?;
        stdout.write_all("\n".as_bytes())?;
    };

    let (task, project) = task_of(&open)?;
    let staged = stage(
        &task,
        &project,
        Install::of_request(false, project.has_lock()),
    )?;
    strip_sentinels(&mut open);

    tracing::debug!(temp_dir = ?staged.root(), "starting Python capture");
    staged.hand_off(&["python", python_connector::ENTRY], &open, requests)
}

/// Answer a Discover, Validate, or Apply by running the user's connector,
/// except for the Validate of a project with missing files (`starter_validated`).
fn unary(mut request: Request) -> anyhow::Result<response::Kind> {
    let (task, project) = task_of(&request)?;

    let validate = match &request.kind {
        Some(request::Kind::Validate(validate)) if !project.missing().is_empty() => {
            return Ok(response::Kind::Validated(starter_validated(
                &task, &project, validate,
            )?));
        }
        Some(request::Kind::Validate(validate)) => {
            Some(validate.project_root.trim_end_matches('/').to_string())
        }
        _ => None,
    };

    let staged = stage(
        &task,
        &project,
        Install::of_request(validate.is_some(), project.has_lock()),
    )?;
    strip_sentinels(&mut request);

    let Some(project_root) = validate else {
        return invoke(&staged, &request);
    };
    staged.check()?;

    let response = invoke(&staged, &request)?;
    let response::Kind::Validated(mut validated) = response else {
        anyhow::bail!("connector returned {response:?} to a Validate, which expects Validated");
    };
    validated.generated_files =
        python_connector::project_urls(&project_root, [generated_module(&task, &project.spec)?]);

    Ok(response::Kind::Validated(validated))
}

/// The capture name and project of a request which runs user code.
/// The name decides the capture's directory and generated module.
fn task_of(request: &Request) -> anyhow::Result<(String, Project)> {
    use request::Kind;

    let named = match &request.kind {
        Some(Kind::Discover(discover)) => Some((&discover.name, &discover.config_json)),
        Some(Kind::Validate(validate)) => Some((&validate.name, &validate.config_json)),
        Some(Kind::Apply(apply)) => apply.capture.as_ref().map(|c| (&c.name, &c.config_json)),
        Some(Kind::Open(open)) => open.capture.as_ref().map(|c| (&c.name, &c.config_json)),
        kind => anyhow::bail!("unexpected request {kind:?}"),
    };
    let (name, config_json) = named.context("connector request is missing its capture")?;

    let ProjectConfig { project, .. } = serde_json::from_slice(config_json)
        .with_context(|| format!("invalid `{}` configuration", python_connector::SENTINEL))?;

    Ok((name.clone(), project))
}

/// Remove the `_python` sentinel from each configuration of `request`,
/// before it's handed to user code.
fn strip_sentinels(request: &mut Request) {
    use request::Kind;

    let mut configs: Vec<&mut bytes::Bytes> = Vec::new();
    match &mut request.kind {
        Some(Kind::Discover(discover)) => configs.push(&mut discover.config_json),
        Some(Kind::Validate(validate)) => {
            configs.push(&mut validate.config_json);
            configs.extend(validate.last_capture.as_mut().map(|c| &mut c.config_json));
        }
        Some(Kind::Apply(apply)) => {
            configs.extend(apply.capture.as_mut().map(|c| &mut c.config_json));
            configs.extend(apply.last_capture.as_mut().map(|c| &mut c.config_json));
        }
        Some(Kind::Open(open)) => {
            configs.extend(open.capture.as_mut().map(|c| &mut c.config_json));
            configs.push(&mut open.sealed_config_json);
        }
        Some(Kind::Spec(_) | Kind::Acknowledge(_)) | None => {}
    }
    for config_json in configs {
        models::custom_connector::strip_sentinel(config_json, python_connector::SENTINEL);
    }
}

/// Stage the project of capture `task` with its generated module and entry
/// point, and `install` its dependencies.
fn stage(task: &str, project: &Project, install: Install) -> anyhow::Result<Staged> {
    let generated = [
        generated_module(task, &project.spec)?,
        (python_connector::ENTRY.to_string(), main_py(task_dir(task))),
    ];
    Staged::of_project(project, &generated, install)
}

/// Run the user's connector for a single request, returning its single response.
/// The connector exits upon reading the end of its input.
fn invoke(staged: &Staged, request: &Request) -> anyhow::Result<response::Kind> {
    let mut input = serde_json::to_vec(request).unwrap();
    input.push(b'\n');

    let output = staged.run(&["python", python_connector::ENTRY], &input)?;
    let responses: Vec<&[u8]> = output
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .collect();

    let [response] = responses.as_slice() else {
        anyhow::bail!(
            "connector returned {} responses to a request which expects one",
            responses.len(),
        );
    };
    let response: Response =
        serde_json::from_slice(response).context("parsing connector response")?;
    response
        .kind
        .context("connector returned a response without a kind")
}

/// Answer the Validate of a project which has listed files that don't exist
/// yet, without running its code: it has starters of those files, and its
/// generated module. A project in this state can't be published, as its
/// missing files are also load errors.
fn starter_validated(
    task: &str,
    project: &Project,
    validate: &request::Validate,
) -> anyhow::Result<response::Validated> {
    let project_root = validate.project_root.trim_end_matches('/');
    let dir = task_dir(task);

    let bindings = (0..validate.bindings.len())
        .map(|index| response::validated::Binding {
            resource_path: vec![format!("binding-{index}")],
        })
        .collect();

    let mut generated_files =
        python_connector::project_urls(project_root, [generated_module(task, &project.spec)?]);

    generated_files.extend(python_connector::starter_files(
        project,
        project_root,
        dir,
        include_str!("starters/__init__.py").replace(
            "GENERATED_MODULE",
            &python_connector::module_parts(task).join("."),
        ),
        // TRANSITIONAL: estuary-cdk isn't yet clean under strict pyright.
        // Delete this override once it is, so that every starter is strict.
        python_connector::starter_manifest(dir).replace(
            r#"typeCheckingMode = "strict""#,
            r#"typeCheckingMode = "standard""#,
        ),
    ));

    Ok(response::Validated {
        bindings,
        generated_files,
    })
}

/// Answer a Spec from the spec declared by a ProjectConfig, without staging
/// or running its code. It has no resource path pointers, which are
/// deprecated: a connector returns the path of each binding instead, in
/// Validated and Discovered responses.
fn spec_response(config_json: &[u8]) -> response::Spec {
    let spec = python_connector::declared_spec(config_json);

    let resource_config_schema = match &spec.resource_config_schema {
        Some(schema) => schema.get().to_string(),
        None => stock_resource_config_schema().to_string(),
    };

    response::Spec {
        protocol: python_connector::PROTOCOL,
        config_schema_json: spec.config_schema_or_default().to_string().into(),
        resource_config_schema_json: resource_config_schema.into(),
        documentation_url: DOCUMENTATION_URL.to_string(),
        oauth2: spec.oauth2.as_ref().map(python_connector::oauth2),
        resource_path_pointers: Vec::new(),
    }
}

/// Resource configuration schema of a capture which doesn't declare one:
/// that of the CDK's stock `ResourceConfig`, with its `name` annotated as the
/// binding's resource path.
fn stock_resource_config_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "description": "Name of this resource",
                "x-collection-name": true,
            },
            "interval": {
                "type": "string",
                "format": "duration",
                "default": "PT0S",
                "description": "Interval between updates for this resource",
                "nonsensitive": true,
            },
        },
        "required": ["name"],
    })
}

/// File of the module generated for capture `task`, relative to the project:
/// its `EndpointConfig` and `ResourceConfig` types, and its CDK `SPEC`.
fn generated_module(task: &str, spec: &Spec) -> anyhow::Result<(String, String)> {
    let content = generated_module_py(task, spec)
        .context("failed to generate the capture's types from its `spec`")?;
    Ok((python_connector::module_path(task), content))
}

/// Source of the module generated for `capture` from its declared `spec`.
fn generated_module_py(capture: &str, spec: &Spec) -> anyhow::Result<String> {
    use python_connector::pydantic::python_literal;

    let endpoint_config = python_connector::endpoint_config_py(spec)?;
    let (resource_config, has_path) = resource_config_py(
        spec.resource_config_schema
            .as_ref()
            .map(|schema| schema.get()),
    )?;

    let pointers = if has_path {
        "ResourceConfig.PATH_POINTERS"
    } else {
        "[]"
    };
    let oauth2 = match &spec.oauth2 {
        // The CDK's OAuth2Spec requires properties which a Spec may omit.
        Some(oauth2) => {
            let mut oauth2 = serde_json::to_value(oauth2).unwrap();
            for (property, default) in [
                ("accessTokenBody", serde_json::json!("")),
                ("accessTokenHeaders", serde_json::json!({})),
                ("accessTokenResponseMap", serde_json::json!({})),
            ] {
                if oauth2.get(property).is_none() {
                    oauth2[property] = default;
                }
            }
            format!("OAuth2Spec.model_validate({})", python_literal(&oauth2))
        }
        None => "None".to_string(),
    };
    let config_schema: serde_json::Value =
        serde_json::from_str(spec.config_schema_or_default()).unwrap();
    let resource_config_schema = match &spec.resource_config_schema {
        Some(schema) => schema.to_value(),
        None => stock_resource_config_schema(),
    };

    let body = format!(
        r#"{endpoint_config}{resource_config}
# The connector specification of the capture. A Spec is answered from the
# capture's model and not by this connector, so `spec()` returns it only so
# that the connector is complete.
SPEC = ConnectorSpec(
    configSchema={config_schema},
    resourceConfigSchema={resource_config_schema},
    documentationUrl="{DOCUMENTATION_URL}",
    resourcePathPointers={pointers},
    oauth2={oauth2},
)
"#,
        config_schema = python_literal(&config_schema),
        resource_config_schema = python_literal(&resource_config_schema),
    );

    // Import only what's used, as a project's pyright may check this module.
    let cdk = [
        "from estuary_cdk.capture import common",
        if spec.oauth2.is_some() {
            "from estuary_cdk.flow import ConnectorSpec, OAuth2Spec"
        } else {
            "from estuary_cdk.flow import ConnectorSpec"
        },
    ];
    let imports = [python_connector::imports_py(&body, &[]), cdk.join("\n")]
        .into_iter()
        .filter(|imports| !imports.is_empty())
        .collect::<Vec<_>>()
        .join("\n");

    Ok(format!(
        "\"\"\"Generated types of capture {capture}, from its declared `spec`.\"\"\"\n\
         {imports}\n\n\n{body}"
    ))
}

/// Python source of the capture's `ResourceConfig`, of its declared `schema`,
/// and whether it has `PATH_POINTERS` (and a `path()`).
///
/// An undeclared schema is the CDK's stock `common.ResourceConfig`, which CDK
/// helpers (such as `open_binding`) are typed by. A declared schema generates
/// a `common.BaseResourceConfig` subclass. Its `path()` returns the properties
/// of its `resource_path`, if it has one. If not, the user declares a subclass
/// having a `path()` of its own, or a resource configuration of their own.
fn resource_config_py(schema: Option<&str>) -> anyhow::Result<(String, bool)> {
    let Some(schema) = schema else {
        let source = "# `spec.resourceConfigSchema` isn't declared, so each binding's resource\n\
                      # configuration is the CDK's stock `ResourceConfig`.\n\
                      ResourceConfig = common.ResourceConfig\n";
        return Ok((source.to_string(), true));
    };

    let (mapper, shape) =
        python_connector::config_shape(schema).context("invalid `resourceConfigSchema`")?;
    let path = resource_path(&shape);
    let mut mapping = mapper
        .map_shape(shape, "ResourceConfig")
        .context("invalid `resourceConfigSchema`")?;

    let Some(class) = mapping
        .classes
        .iter_mut()
        .find(|class| class.name == "ResourceConfig")
    else {
        anyhow::bail!(
            "`resourceConfigSchema` must be an object, as a capture's resource configuration is a CDK `BaseResourceConfig`"
        );
    };
    class.base = Some("common.BaseResourceConfig".to_string());

    // A field which would shadow an attribute of `BaseResourceConfig` is
    // prefixed and aliased, as are unusable names of `pydantic.BaseModel`.
    for field in &mut class.fields {
        if BASE_RESOURCE_CONFIG_ATTRIBUTES.contains(&field.name.as_str()) {
            field.alias.get_or_insert_with(|| field.name.clone());
            field.name = format!("m_{}", field.name);
        }
    }

    let mut source = String::new();

    if let Some(path) = &path {
        let pointers: Vec<String> = path
            .iter()
            .map(|property| {
                json::Pointer(vec![json::ptr::Token::Property(property.clone())]).to_string()
            })
            .collect();

        let attributes: Vec<String> = path
            .iter()
            .map(|property| {
                let field = class
                    .fields
                    .iter()
                    .find(|field| field.alias.as_ref().unwrap_or(&field.name) == property)
                    .expect("a resource path property is a field");
                format!("self.{}", field.name)
            })
            .collect();

        class.body = vec![
            String::new(),
            format!(
                "PATH_POINTERS: _typing.ClassVar[list[str]] = {}",
                python_connector::pydantic::python_literal(&serde_json::json!(pointers))
            ),
            String::new(),
            "def path(self) -> list[str]:".to_string(),
            "    \"\"\"Resource path of this binding: its `x-schema-name` (if any),".to_string(),
            "    followed by its `x-collection-name`.\"\"\"".to_string(),
            format!("    return [{}]", attributes.join(", ")),
        ];
    } else {
        source.push_str(
            "# The resource schema doesn't have exactly one `x-collection-name` property\n\
             # (and at most one `x-schema-name`), each a required string, so\n\
             # `ResourceConfig` has no `path()`: declare a subclass having\n\
             # `PATH_POINTERS` and a `path()` of its own.\n",
        );
    }
    source.push_str("# Generated for the resource configuration of `spec.resourceConfigSchema`.\n");
    mapping.render(&mut source);

    Ok((source, path.is_some()))
}

/// Properties of the resource path of a capture's resource configuration:
/// its `x-schema-name` (if any), followed by its `x-collection-name`. There's
/// a path only if the configuration has exactly one `x-collection-name`
/// property and at most one `x-schema-name`, each a required string, which
/// `path()` can return without further logic.
fn resource_path(shape: &doc::Shape) -> Option<Vec<String>> {
    let annotated = |annotation: &str| -> Option<Vec<String>> {
        shape
            .object
            .properties
            .iter()
            .filter(|prop| prop.shape.annotations.get(annotation) == Some(&serde_json::json!(true)))
            .map(|prop| {
                (prop.is_required && prop.shape.type_ == json::schema::types::STRING)
                    .then(|| prop.name.to_string())
            })
            .collect()
    };
    let collection = annotated("x-collection-name")?;
    let schema = annotated("x-schema-name")?;

    if collection.len() != 1 || schema.len() > 1 {
        return None;
    }
    Some(schema.into_iter().chain(collection).collect())
}

/// Attributes of the CDK's `BaseResourceConfig`, which a field of a generated
/// `ResourceConfig` mustn't shadow.
const BASE_RESOURCE_CONFIG_ATTRIBUTES: [&str; 3] = ["path", "PATH_POINTERS", "meta_"];

/// Generated entry point of the capture, which runs the `Connector`
/// exported by the capture's directory `dir`.
fn main_py(dir: &str) -> String {
    format!(
        r#""""Entry point of a capture, generated by Estuary. It runs the `Connector`
which the capture's directory exports."""
import asyncio
import importlib.util
import pathlib
import sys
import typing


{load_task}

asyncio.run(task.Connector().serve())
"#,
        load_task = python_connector::load_task_py(dir),
    )
}

#[cfg(test)]
mod test {
    use super::*;

    fn sentinel(spec: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "files": {"pyproject.toml": "[project]\n", "source-acme/__init__.py": ""},
            "spec": spec,
        })
    }

    #[test]
    fn missing_files_are_given_starters() {
        let mut config = serde_json::json!({"_python": sentinel(serde_json::json!({}))});
        config["_python"]["files"] = serde_json::json!({
            "pyproject.toml": null,
            "source-acme/__init__.py": null,
            "source-acme/models.py": null,
            "source-acme/api.py": "# Exists, and isn't given a starter.\n",
            // A lock has no starter.
            "uv.lock": null,
        });
        let request: Request = serde_json::from_value(serde_json::json!({"validate": {
            "name": "acmeCo/source-acme",
            "config": config,
            "bindings": [{"resourceConfig": {"name": "greetings"}}],
            "projectRoot": "file:///project/acmeCo/",
        }}))
        .unwrap();
        let (task, project) = task_of(&request).unwrap();
        let Some(request::Kind::Validate(validate)) = &request.kind else {
            unreachable!()
        };

        let response = Response {
            kind: Some(response::Kind::Validated(
                starter_validated(&task, &project, validate).unwrap(),
            )),
            ..Default::default()
        };
        insta::assert_json_snapshot!(serde_json::to_value(response).unwrap());
    }

    #[test]
    fn tasks_of_requests() {
        let config = serde_json::json!({"apiKey": "k", "_python": sentinel(serde_json::json!({}))});
        let name = "acmeCo/source-acme";

        let cases: Vec<_> = [
            serde_json::json!({"discover": {"name": name, "config": config}}),
            serde_json::json!({"validate": {"name": name, "config": config}}),
            serde_json::json!({"apply": {"capture": {"name": name, "config": config}}}),
            serde_json::json!({"open": {"capture": {"name": name, "config": config}}}),
            serde_json::json!({"apply": {}}),
            serde_json::json!({"discover": {"name": name, "config": {"apiKey": "k"}}}),
            serde_json::json!({"discover": {"name": name, "config": {"_python": {"files": []}}}}),
            serde_json::json!({"acknowledge": {}}),
        ]
        .into_iter()
        .map(|request| {
            let request: Request = serde_json::from_value(request).unwrap();
            task_of(&request)
                .map(|(name, project)| format!("{name}: {:?}", project.files.keys()))
                .map_err(|err| format!("{err:#}"))
        })
        .collect();

        insta::assert_debug_snapshot!(cases, @r#"
        [
            Ok(
                "acmeCo/source-acme: [\"pyproject.toml\", \"source-acme/__init__.py\"]",
            ),
            Ok(
                "acmeCo/source-acme: [\"pyproject.toml\", \"source-acme/__init__.py\"]",
            ),
            Ok(
                "acmeCo/source-acme: [\"pyproject.toml\", \"source-acme/__init__.py\"]",
            ),
            Ok(
                "acmeCo/source-acme: [\"pyproject.toml\", \"source-acme/__init__.py\"]",
            ),
            Err(
                "connector request is missing its capture",
            ),
            Err(
                "invalid `_python` configuration: missing field `_python` at line 1 column 14",
            ),
            Err(
                "invalid `_python` configuration: invalid type: sequence, expected a map at line 1 column 20",
            ),
            Err(
                "unexpected request Some(Acknowledge(Acknowledge { checkpoints: 0 }))",
            ),
        ]
        "#);
    }

    #[test]
    fn sentinels_are_stripped() {
        let config = serde_json::json!({"apiKey": "k", "_python": sentinel(serde_json::json!({}))});
        let capture = serde_json::json!({"name": "acmeCo/source-acme", "config": config});

        let requests: Vec<Request> = [
            serde_json::json!({"discover": {"config": config}}),
            serde_json::json!({"validate": {"config": config, "lastCapture": capture}}),
            serde_json::json!({"apply": {"capture": capture, "lastCapture": capture}}),
            serde_json::json!({"open": {"capture": capture, "sealedConfig": config}}),
        ]
        .into_iter()
        .map(|request| {
            let mut request: Request = serde_json::from_value(request).unwrap();
            strip_sentinels(&mut request);
            request
        })
        .collect();

        insta::assert_json_snapshot!(serde_json::to_value(requests).unwrap());
    }

    #[test]
    fn specs_are_answered_from_the_sentinel() {
        let declared = serde_json::json!({"_python": sentinel(serde_json::json!({
            "configSchema": {"type": "object", "properties": {"token": {"type": "string", "secret": true}}},
            "resourceConfigSchema": {"type": "object", "properties": {"name": {"type": "string", "x-collection-name": true}}},
            "oauth2": {"provider": "acme", "authUrlTemplate": "https://acme.example/authorize", "accessTokenUrlTemplate": "https://acme.example/token"},
        }))});

        // A spec which declares nothing has the defaults of the connector.
        let responses = [
            spec_response(declared.to_string().as_bytes()),
            spec_response(br#"{"_python": {}}"#),
        ]
        .map(|spec| Response {
            kind: Some(response::Kind::Spec(Box::new(spec))),
            ..Default::default()
        });
        insta::assert_json_snapshot!(serde_json::to_value(responses).unwrap());
    }

    #[test]
    fn generated_module_of_a_declared_spec() {
        let spec: Spec = serde_json::from_value(serde_json::json!({
            "configSchema": {
                "type": "object",
                "properties": {
                    "greeting": {"type": "string", "default": "Hello"},
                    "count": {"type": "integer", "default": 10, "minimum": 0},
                },
            },
            "resourceConfigSchema": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "x-collection-name": true},
                    "interval": {"type": "string", "format": "duration", "default": "PT0S", "x-python-type": "datetime.timedelta"},
                },
                "required": ["name"],
            },
            "oauth2": {
                "provider": "acme",
                "authUrlTemplate": "https://acme.example/authorize",
                "accessTokenUrlTemplate": "https://acme.example/token",
            },
        }))
        .unwrap();

        insta::assert_snapshot!(generated_module_py("acmeCo/source-acme", &spec).unwrap());
    }

    #[test]
    fn generated_module_and_entry_of_defaults() {
        insta::assert_snapshot!(format!(
            "{}\n=====\n{}",
            generated_module_py("acmeCo/source-acme", &Spec::default()).unwrap(),
            main_py("source-acme"),
        ));
    }

    #[test]
    fn resource_configs_and_their_paths() {
        let resource = |properties: serde_json::Value, required: serde_json::Value| {
            let schema = serde_json::json!({
                "type": "object",
                "properties": properties,
                "required": required,
            });
            let (source, has_path) = resource_config_py(Some(&schema.to_string())).unwrap();
            format!("{source}# has_path: {has_path}\n")
        };

        insta::assert_snapshot!(
            [
                // A required schema name precedes the collection name.
                resource(
                    serde_json::json!({
                        "schema": {"type": "string", "x-schema-name": true},
                        "_table": {"type": "string", "x-collection-name": true},
                        "interval": {"type": "string", "format": "duration", "default": "PT0S", "x-python-type": "datetime.timedelta"},
                    }),
                    serde_json::json!(["schema", "_table"]),
                ),
                // Without a single collection name, there's no generated path.
                resource(
                    serde_json::json!({
                        "a": {"type": "string", "x-collection-name": true},
                        "b": {"type": "string", "x-collection-name": true},
                    }),
                    serde_json::json!([]),
                ),
                // Nor if a path property isn't a required string.
                resource(
                    serde_json::json!({
                        "schema": {"type": "string", "x-schema-name": true},
                        "table": {"type": "string", "x-collection-name": true},
                    }),
                    serde_json::json!(["table"]),
                ),
                resource(
                    serde_json::json!({"table": {"type": ["string", "null"], "x-collection-name": true}}),
                    serde_json::json!(["table"]),
                ),
                // Fields don't shadow attributes of `BaseResourceConfig`,
                // and a path property may be one of them.
                resource(
                    serde_json::json!({
                        "path": {"type": "string", "x-collection-name": true},
                        "meta_": {"type": "string"},
                    }),
                    serde_json::json!(["path"]),
                ),
            ]
            .join("\n=====\n")
        );

        // A capture's resource configuration is a class.
        let err = resource_config_py(Some(r#"{"type": "string"}"#)).unwrap_err();
        insta::assert_snapshot!(format!("{err:#}"), @"`resourceConfigSchema` must be an object, as a capture's resource configuration is a CDK `BaseResourceConfig`");
    }
}

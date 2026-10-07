use anyhow::Context;
use python_connector::{Files, LOCK_FILE, PYPROJECT, Project, Resources, Spec, Use};
use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::process::Stdio;

/// Archive of the Estuary CDK against which this connector is tested,
/// which starter projects depend upon.
// TODO(before merge): this pins commit e89e2c34 of the unmerged
// `johnny/capture-python-cdk` branch of estuary/connectors. Repin it to a
// commit of `main` once that branch merges.
pub const CDK_URL: &str =
    "https://github.com/estuary/connectors/archive/e89e2c34426d37cd5bc503e8f623fca001f3b98d.tar.gz";

/// Locations of the connector configuration within each kind of request.
/// Each has the `_python` sentinel removed before it's handed to user code.
const CONFIG_POINTERS: &[&str] = &[
    "/spec/config",
    "/discover/config",
    "/validate/config",
    "/validate/lastCapture/config",
    "/apply/capture/config",
    "/apply/lastCapture/config",
    "/open/capture/config",
    "/open/sealedConfig",
];

/// The `_python` sentinel of a capture's configuration.
#[derive(Debug)]
struct Sentinel {
    /// Name of the capture, which names its directory and generated module.
    capture: String,
    /// Files of the project, keyed on their path relative to the project root.
    files: Files,
    /// Declared connector spec of the capture.
    spec: Spec,
}

impl Sentinel {
    fn parse(sentinel: &serde_json::Value) -> anyhow::Result<Self> {
        #[derive(serde::Deserialize)]
        struct Fields {
            capture: String,
        }
        let Fields { capture } =
            serde_json::from_value(sentinel.clone()).context("invalid `_python` configuration")?;

        Ok(Self {
            capture,
            files: python_connector::text_files(sentinel.get("files"))?,
            spec: Spec::of_sentinel(sentinel)?,
        })
    }

    /// Directory of the capture within its project, which is loaded as a package.
    fn dir(&self) -> &str {
        self.capture.rsplit('/').next().unwrap()
    }

    fn has_lock(&self) -> bool {
        matches!(self.files.get(LOCK_FILE), Some(Some(_)))
    }

    /// Listed files which failed to load (as they don't exist yet).
    fn missing(&self) -> Vec<&str> {
        self.files
            .iter()
            .filter(|(_, content)| content.is_none())
            .map(|(path, _)| path.as_str())
            .collect()
    }
}

pub fn run() -> anyhow::Result<()> {
    let mut requests = std::io::BufReader::new(std::io::stdin());
    let mut stdout = std::io::stdout();
    let mut line = String::new();

    // The project is staged once per session, by its first request which
    // runs user code, and is then installed as each request requires.
    let mut session: Option<(Sentinel, Project)> = None;

    loop {
        line.clear();
        if requests.read_line(&mut line)? == 0 {
            return Ok(()); // Clean EOF.
        }
        let mut request: serde_json::Value =
            serde_json::from_str(&line).context("parsing connector request")?;
        let sentinel = sentinel_of(&request)?;

        // Spec is answered from the sentinel's declared `spec` (or generically,
        // as for a bare image), and never stages the project.
        if request.get("spec").is_some() {
            let spec = sentinel.map(|sentinel| sentinel.spec).unwrap_or_default();
            write_line(&mut stdout, &spec_response(&spec))?;
            continue;
        }
        if session.is_none() {
            let sentinel =
                sentinel.context("capture-python requires a Python project configuration")?;

            if let Some(validate) = request.get("validate")
                && !sentinel.missing().is_empty()
            {
                write_line(&mut stdout, &starter_validated(&sentinel, validate)?)?;
                continue;
            }
            let project = stage(&sentinel)?;
            session = Some((sentinel, project));
        }
        let (sentinel, project) = session.as_mut().unwrap();

        for ptr in CONFIG_POINTERS {
            python_connector::strip_sentinel_at(&mut request, ptr);
        }

        if request.get("open").is_some() {
            project.prepare(Use::Open, sentinel.has_lock())?;
            tracing::debug!(temp_dir = ?project.root(), "starting Python capture");

            return hand_off(project, &request, requests);
        }

        let validate = request.get("validate").cloned();
        let use_ = if validate.is_some() {
            Use::Validate
        } else {
            Use::Unary
        };
        project.prepare(use_, sentinel.has_lock())?;

        if validate.is_some() {
            check_project(project)?;
        }
        let mut response = unary(project, &request)?;

        if let Some(validate) = &validate {
            let mut files = generated_files(sentinel, &project_root(validate))?;

            // A lock resolved by this session is returned to be baked
            // into the built specification.
            if let (false, Some(lock)) = (sentinel.has_lock(), project.lock()) {
                files.insert(
                    format!("{}/{LOCK_FILE}", project_root(validate)),
                    lock.to_string(),
                );
            }
            response["validated"]["generatedFiles"] = serde_json::json!(files);
        }
        write_line(&mut stdout, &response)?;
    }
}

/// The sentinel of a request's configuration, if it has one.
fn sentinel_of(request: &serde_json::Value) -> anyhow::Result<Option<Sentinel>> {
    CONFIG_POINTERS
        .iter()
        .find_map(|ptr| request.pointer(&format!("{ptr}/{}", python_connector::SENTINEL)))
        .map(Sentinel::parse)
        .transpose()
}

/// Stage the project of a capture with its generated module and entry point.
fn stage(sentinel: &Sentinel) -> anyhow::Result<Project> {
    let mut staged = generated_module(sentinel)?;
    staged.push((python_connector::ENTRY.to_string(), main_py(sentinel.dir())));

    Project::stage(
        sentinel
            .files
            .iter()
            .filter_map(|(path, content)| Some((path.as_str(), content.as_deref()?)))
            .chain(
                staged
                    .iter()
                    .map(|(path, content)| (path.as_str(), content.as_str())),
            ),
    )
}

/// Run the user's connector for a single request, returning its single response.
/// The connector exits upon reading the end of its input.
fn unary(project: &Project, request: &serde_json::Value) -> anyhow::Result<serde_json::Value> {
    let mut child = project
        .command(&["python", python_connector::ENTRY])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .context("failed to start the connector")?;

    let mut stdin = child.stdin.take().unwrap();
    write_line(&mut stdin, request)?;
    std::mem::drop(stdin);

    let output = child.wait_with_output()?;
    let responses: Vec<&[u8]> = output
        .stdout
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .collect();

    let [response] = responses.as_slice() else {
        anyhow::bail!(
            "connector returned {} responses to a request which expects one (exit status: {:?})",
            responses.len(),
            output.status,
        );
    };
    if !output.status.success() {
        anyhow::bail!("connector failed with status: {:?}", output.status);
    }
    serde_json::from_slice(response).context("parsing connector response")
}

/// Hand off the session to the user's connector, which runs the capture:
/// it writes its responses directly to stdout, and is given the Open and the
/// remainder of stdin.
fn hand_off(
    project: &Project,
    open: &serde_json::Value,
    requests: std::io::BufReader<std::io::Stdin>,
) -> anyhow::Result<()> {
    let mut child = project
        .command(&["python", python_connector::ENTRY])
        .stdin(Stdio::piped())
        .spawn()
        .context("failed to start the connector")?;

    // The connector may exit before consuming all of its requests,
    // in which case its exit status is the error to report.
    let mut child_stdin = child.stdin.take().unwrap();
    let open = open.clone();
    let _ = std::thread::spawn(move || {
        let _ = write_line(&mut child_stdin, &open);
        let _ = std::io::copy(&mut requests.buffer(), &mut child_stdin);
        let _ = std::io::copy(&mut requests.into_inner(), &mut child_stdin);
    });

    let status = child.wait()?;
    if !status.success() {
        anyhow::bail!("connector failed with status: {status:?}");
    }
    Ok(())
}

/// Type-check the project with its own pyright (and configuration),
/// if its `dev` dependency group provides one.
fn check_project(project: &Project) -> anyhow::Result<()> {
    if !project.has_tool("pyright") {
        return Ok(());
    }
    project
        .run(&["pyright"], None)
        .map_err(|err| anyhow::anyhow!("Python type check failed:\n{err}"))?;

    Ok(())
}

/// Answer the Validate of a project which has listed files that don't exist
/// yet, without running its code: it has starters of those files, and its
/// generated module. A project in this state can't be published, as its
/// missing files are also load errors.
fn starter_validated(
    sentinel: &Sentinel,
    validate: &serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    let project_root = project_root(validate);

    let bindings: Vec<serde_json::Value> = validate
        .get("bindings")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
        .map(|(index, _)| serde_json::json!({"resourcePath": [format!("binding-{index}")]}))
        .collect();

    let mut files = generated_files(sentinel, &project_root)?;
    for path in sentinel.missing() {
        files.insert(format!("{project_root}/{path}"), starter(sentinel, path));
    }

    Ok(serde_json::json!({"validated": {
        "bindings": bindings,
        "generatedFiles": files,
    }}))
}

/// Root of the user's project, from a Validate request.
fn project_root(validate: &serde_json::Value) -> String {
    validate
        .get("projectRoot")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .trim_end_matches('/')
        .to_string()
}

/// Spec response of the capture's declared spec, or of its defaults. It has
/// no resource path pointers, which are deprecated: a connector returns the
/// path of each binding instead, in Validated and Discovered responses.
fn spec_response(spec: &Spec) -> serde_json::Value {
    let resource_config_schema = spec
        .resource_config_schema
        .clone()
        .unwrap_or_else(stock_resource_config_schema);

    let mut response = serde_json::json!({
        "protocol": python_connector::PROTOCOL,
        "configSchema": spec.config_schema(),
        "resourceConfigSchema": resource_config_schema,
        "documentationUrl": "https://docs.estuary.dev",
    });
    if let Some(oauth2) = &spec.oauth2 {
        response["oauth2"] = oauth2.clone();
    }
    serde_json::json!({"spec": response})
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

fn write_line(w: &mut impl Write, value: &serde_json::Value) -> std::io::Result<()> {
    let mut buf = serde_json::to_vec(value).unwrap();
    buf.push(b'\n');
    w.write_all(&buf)?;
    w.flush()
}

/// Generated files of a Validated response, keyed on their URL within the
/// user's project: the capture's generated module, for the user's IDE.
fn generated_files(
    sentinel: &Sentinel,
    project_root: &str,
) -> anyhow::Result<BTreeMap<String, String>> {
    Ok(generated_module(sentinel)?
        .into_iter()
        .map(|(path, content)| (format!("{project_root}/{path}"), content))
        .collect())
}

/// Files of the module generated for the capture, relative to the project:
/// its `EndpointConfig` and `ResourceConfig` types, and its CDK `SPEC`.
fn generated_module(sentinel: &Sentinel) -> anyhow::Result<Vec<(String, String)>> {
    let content = generated_module_py(&sentinel.capture, &sentinel.spec)
        .context("failed to generate the capture's types from its `spec`")?;
    Ok(python_connector::module_files(&sentinel.capture, content))
}

/// Source of the module generated for `capture` from its declared `spec`.
fn generated_module_py(capture: &str, spec: &Spec) -> anyhow::Result<String> {
    use python_connector::pydantic::python_literal;

    let types = python_connector::config_types_py(spec, Resources::Capture)?;

    let pointers = if types.has_path {
        "ResourceConfig.PATH_POINTERS"
    } else {
        "[]"
    };
    let oauth2 = match &spec.oauth2 {
        // The CDK's OAuth2Spec requires properties which a Spec may omit.
        Some(oauth2) => {
            let mut oauth2 = oauth2.clone();
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
    let resource_config_schema = spec
        .resource_config_schema
        .clone()
        .unwrap_or_else(stock_resource_config_schema);

    let body = format!(
        r#"{types}
# The connector specification of the capture. A Spec is answered from the
# capture's model and not by this connector, so `spec()` returns it only so
# that the connector is complete.
SPEC = ConnectorSpec(
    configSchema={config_schema},
    resourceConfigSchema={resource_config_schema},
    documentationUrl="https://docs.estuary.dev",
    resourcePathPointers={pointers},
    oauth2={oauth2},
)
"#,
        types = types.source,
        config_schema = python_literal(&spec.config_schema()),
        resource_config_schema = python_literal(&resource_config_schema),
    );

    // Import only what's used, as a project's pyright may check this module.
    let mut cdk = Vec::new();
    if types.uses_common {
        cdk.push("from estuary_cdk.capture import common");
    }
    cdk.push(if spec.oauth2.is_some() {
        "from estuary_cdk.flow import ConnectorSpec, OAuth2Spec"
    } else {
        "from estuary_cdk.flow import ConnectorSpec"
    });
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

/// Starter content of a listed file at `path` which doesn't exist yet:
/// a working capture as its entry, a project manifest, or else empty.
fn starter(sentinel: &Sentinel, path: &str) -> String {
    let dir = sentinel.dir();

    if path == format!("{dir}/__init__.py") {
        let generated_module = python_connector::module_parts(&sentinel.capture).join(".");
        include_str!("starters/__init__.py").replace("GENERATED_MODULE", &generated_module)
    } else if path == PYPROJECT {
        include_str!("starters/pyproject.toml")
            .replace("PROJECT_NAME", &project_name(dir))
            .replace("CDK_URL", CDK_URL)
    } else {
        String::new()
    }
}

/// Name of a starter project, which is a valid distribution name.
fn project_name(dir: &str) -> String {
    dir.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

#[cfg(test)]
mod test {
    use super::*;

    fn sentinel(spec: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "capture": "acmeCo/source-acme",
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
        });
        let validate = serde_json::json!({
            "name": "acmeCo/source-acme",
            "config": config,
            "bindings": [{"resourceConfig": {"name": "greetings"}}],
            "projectRoot": "file:///project/acmeCo/",
        });
        let sentinel = Sentinel::parse(&config["_python"]).unwrap();

        insta::assert_json_snapshot!(starter_validated(&sentinel, &validate).unwrap());
    }

    #[test]
    fn sentinels_of_requests() {
        let config = serde_json::json!({"apiKey": "k", "_python": sentinel(serde_json::json!({}))});

        let sentinels: Vec<_> = [
            serde_json::json!({"spec": {"config": {}}}),
            serde_json::json!({"discover": {"config": config}}),
            serde_json::json!({"open": {"capture": {"config": config}}}),
            serde_json::json!({"acknowledge": {}}),
        ]
        .iter()
        .map(|request| {
            sentinel_of(request)
                .unwrap()
                .map(|sentinel| (sentinel.capture.clone(), sentinel.dir().to_string()))
        })
        .collect();

        insta::assert_debug_snapshot!(sentinels, @r#"
        [
            None,
            Some(
                (
                    "acmeCo/source-acme",
                    "source-acme",
                ),
            ),
            Some(
                (
                    "acmeCo/source-acme",
                    "source-acme",
                ),
            ),
            None,
        ]
        "#);

        // A sentinel must name its capture.
        let err =
            sentinel_of(&serde_json::json!({"discover": {"config": {"_python": {}}}})).unwrap_err();
        insta::assert_snapshot!(format!("{err:#}"), @"invalid `_python` configuration: missing field `capture`");
    }

    #[test]
    fn specs_are_answered_from_the_sentinel() {
        let declared = Sentinel::parse(&sentinel(serde_json::json!({
            "configSchema": {"type": "object", "properties": {"token": {"type": "string", "secret": true}}},
            "resourceConfigSchema": {"type": "object", "properties": {"name": {"type": "string", "x-collection-name": true}}},
            "oauth2": {"provider": "acme", "authUrlTemplate": "https://acme.example/authorize"},
        })))
        .unwrap();

        // A spec which declares nothing has the defaults of the connector.
        insta::assert_json_snapshot!([
            spec_response(&declared.spec),
            spec_response(&Spec::default())
        ]);
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
                    "interval": {"type": "string", "format": "duration", "default": "PT0S"},
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
}

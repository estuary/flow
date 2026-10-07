use anyhow::Context;
use python_connector::{Install, LOCK_FILE, PYPROJECT, Project, Resources, Spec};
use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::process::Stdio;
use std::sync::{Arc, Mutex};

/// Archive of the Estuary CDK against which this connector is tested,
/// which is written into scaffolded projects.
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
    /// Name of the capture, which names its generated module.
    capture: String,
    /// Package of the user's connector, which is run as `python -m <package>`.
    package: String,
    /// Files of the project, keyed on their path relative to the project root.
    files: BTreeMap<String, String>,
    /// Declared connector spec of the capture.
    spec: Spec,
}

impl Sentinel {
    fn parse(sentinel: &serde_json::Value) -> anyhow::Result<Self> {
        #[derive(serde::Deserialize)]
        struct Fields {
            #[serde(default)]
            capture: Option<String>,
            package: String,
            #[serde(default)]
            files: BTreeMap<String, String>,
        }
        let Fields {
            capture,
            package,
            files,
        } = serde_json::from_value(sentinel.clone()).context("invalid `_python` configuration")?;

        Ok(Self {
            // The package is a sufficient name of a module generated
            // for a configuration which doesn't name its capture.
            capture: capture.unwrap_or_else(|| package.clone()),
            package,
            files,
            spec: Spec::of_sentinel(sentinel)?,
        })
    }

    fn has_lock(&self) -> bool {
        self.files.contains_key(LOCK_FILE)
    }
}

pub fn run() -> anyhow::Result<()> {
    let mut requests = std::io::BufReader::new(std::io::stdin());
    let mut stdout = std::io::stdout();
    let mut line = String::new();

    let Some((mut first, sentinel)) = answer_specs(&mut requests, &mut stdout)? else {
        return Ok(()); // Clean EOF.
    };

    let missing: Vec<String> = required_files(&sentinel.package)
        .into_iter()
        .filter(|path| !sentinel.files.contains_key(path))
        .collect();

    if !missing.is_empty() {
        return scaffold(first, &sentinel, &missing, requests, stdout);
    }

    // Stage and install the project, and then start the user's connector.
    let generated = generated_module(&sentinel)?;

    let mut project = Project::stage(
        sentinel
            .files
            .iter()
            .chain(generated.iter().map(|(path, content)| (path, content)))
            .map(|(path, content)| (path.as_str(), content.as_str())),
    )?;
    let install = install_of(&first, &sentinel);
    project.install(install)?;

    tracing::debug!(temp_dir = ?project.root(), ?install, "starting Python capture");

    let mut child = project
        .command(&["python", "-m", &sentinel.package])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .context("failed to start the connector")?;

    // Generated files which are added to the connector's next Validated response.
    let validated_files: Arc<Mutex<Option<BTreeMap<String, String>>>> = Default::default();

    let child_stdout = child.stdout.take().unwrap();
    let responses = {
        let validated_files = validated_files.clone();
        std::thread::spawn(move || forward_responses(child_stdout, stdout, validated_files))
    };

    let mut child_stdin = child.stdin.take().unwrap();
    let mut request = Some(std::mem::take(&mut first));

    loop {
        let mut next = match request.take() {
            Some(request) => request,
            None => {
                line.clear();
                if requests.read_line(&mut line)? == 0 {
                    break; // Clean EOF.
                }
                serde_json::from_str(&line).context("parsing connector request")?
            }
        };

        if let Some(validate) = next.get("validate") {
            let project_root = project_root(validate);
            check_project(&project)?;

            // The generated module is returned for the user's IDE, and a lock
            // resolved by this session to be baked into the built specification
            // (and written into the user's project).
            let mut files: BTreeMap<String, String> = generated
                .iter()
                .map(|(path, content)| (format!("{project_root}/{path}"), content.clone()))
                .collect();

            if let (false, Some(lock)) = (sentinel.has_lock(), project.lock()) {
                files.insert(format!("{project_root}/{LOCK_FILE}"), lock.to_string());
            }
            *validated_files.lock().unwrap() = Some(files);
        }

        for ptr in CONFIG_POINTERS {
            python_connector::strip_sentinel_at(&mut next, ptr);
        }
        // The connector may exit before consuming all of its requests,
        // in which case its exit status is the error to report.
        if write_line(&mut child_stdin, &next).is_err() {
            break;
        }
    }
    std::mem::drop(child_stdin);

    let status = child.wait()?;
    responses.join().unwrap()?;

    if !status.success() {
        anyhow::bail!("connector failed with status: {status:?}");
    }
    Ok(())
}

/// Answer the session's Spec requests until another request, returning it
/// and the sentinel of its configuration, or None at a clean EOF.
///
/// The runtime opens each session with a Spec carrying the sealed
/// configuration. Specs are answered from the sentinel's declared `spec`
/// (or generically, as for a bare image), and the project is staged and the
/// user's connector started only upon another request.
fn answer_specs(
    requests: &mut impl BufRead,
    stdout: &mut impl Write,
) -> anyhow::Result<Option<(serde_json::Value, Sentinel)>> {
    let mut line = String::new();

    loop {
        line.clear();
        if requests.read_line(&mut line)? == 0 {
            return Ok(None);
        }
        let request: serde_json::Value =
            serde_json::from_str(&line).context("parsing connector request")?;

        let sentinel = CONFIG_POINTERS
            .iter()
            .find_map(|ptr| request.pointer(&format!("{ptr}/{}", python_connector::SENTINEL)))
            .map(Sentinel::parse)
            .transpose()?;

        if request.get("spec").is_some() {
            let response = match &sentinel {
                Some(sentinel) => spec_response(&sentinel.spec),
                None => generic_spec(),
            };
            write_line(stdout, &response)?;
            continue;
        }
        let Some(sentinel) = sentinel else {
            anyhow::bail!("capture-python requires a Python project configuration");
        };
        return Ok(Some((request, sentinel)));
    }
}

/// Install mode of the project, by the first request which needs it.
fn install_of(first: &serde_json::Value, sentinel: &Sentinel) -> Install {
    Install::of_request(first.get("validate").is_some(), sentinel.has_lock())
}

/// Copy responses of the connector to stdout, adding `validated_files`
/// to a Validated response. Other responses (such as Captured documents)
/// are copied without being parsed.
fn forward_responses(
    child_stdout: std::process::ChildStdout,
    stdout: std::io::Stdout,
    validated_files: Arc<Mutex<Option<BTreeMap<String, String>>>>,
) -> anyhow::Result<()> {
    let mut responses = std::io::BufReader::new(child_stdout);
    let mut stdout = std::io::BufWriter::new(stdout.lock());
    let mut line = Vec::new();

    loop {
        line.clear();
        if responses.read_until(b'\n', &mut line)? == 0 {
            break;
        }

        if line.starts_with(br#"{"validated""#) {
            let mut response: serde_json::Value =
                serde_json::from_slice(&line).context("parsing Validated response")?;

            if let Some(files) = validated_files.lock().unwrap().take() {
                response["validated"]["generatedFiles"] = serde_json::json!(files);
            }
            line = serde_json::to_vec(&response).unwrap();
            line.push(b'\n');
        }
        stdout.write_all(&line)?;

        // Flush once no further responses are ready, which batches
        // bursts of Captured documents into fewer writes.
        if responses.buffer().is_empty() {
            stdout.flush()?;
        }
    }
    stdout.flush()?;

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

/// Answer the requests of a project which is missing required files:
/// its Spec from the declared spec, and its Validate with scaffolding of the
/// project (and its generated module). The scaffolding must then be added
/// to the capture's `files`.
fn scaffold(
    first: serde_json::Value,
    sentinel: &Sentinel,
    missing: &[String],
    mut requests: impl BufRead,
    mut stdout: impl Write,
) -> anyhow::Result<()> {
    let mut line = String::new();
    let mut request = Some(first);

    loop {
        let next = match request.take() {
            Some(request) => request,
            None => {
                line.clear();
                if requests.read_line(&mut line)? == 0 {
                    return Ok(());
                }
                serde_json::from_str(&line).context("parsing connector request")?
            }
        };

        if next.get("spec").is_some() {
            write_line(&mut stdout, &spec_response(&sentinel.spec))?;
            continue;
        }
        let Some(validate) = next.get("validate") else {
            anyhow::bail!(
                "the capture's project is missing {}: run `flowctl generate` to scaffold it",
                missing.join(", ")
            );
        };
        let project_root = project_root(validate);

        let bindings: Vec<serde_json::Value> = validate
            .get("bindings")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .enumerate()
            .map(|(index, _)| serde_json::json!({"resourcePath": [format!("binding-{index}")]}))
            .collect();

        let generated_files: BTreeMap<String, String> =
            templates(&sentinel.capture, &sentinel.package)
                .into_iter()
                .filter(|(path, _)| !sentinel.files.contains_key(path))
                .chain(generated_module(sentinel)?)
                .map(|(path, content)| (format!("{project_root}/{path}"), content))
                .collect();

        write_line(
            &mut stdout,
            &serde_json::json!({"validated": {
                "bindings": bindings,
                "generatedFiles": generated_files,
            }}),
        )?;
    }
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

/// Spec response of the capture's declared spec. It has no resource path
/// pointers, which are deprecated: a connector returns the path of each
/// binding instead, in Validated and Discovered responses.
fn spec_response(spec: &Spec) -> serde_json::Value {
    let mut response = serde_json::json!({
        "protocol": python_connector::PROTOCOL,
        "configSchema": spec.config_schema,
        "resourceConfigSchema": spec.resource_config_schema,
        "documentationUrl": "https://docs.estuary.dev",
    });
    if let Some(oauth2) = &spec.oauth2 {
        response["oauth2"] = oauth2.clone();
    }
    serde_json::json!({"spec": response})
}

/// Spec of a configuration which isn't of a capture, such as a bare image.
fn generic_spec() -> serde_json::Value {
    spec_response(&Spec::default())
}

fn write_line(w: &mut impl Write, value: &serde_json::Value) -> std::io::Result<()> {
    let mut buf = serde_json::to_vec(value).unwrap();
    buf.push(b'\n');
    w.write_all(&buf)?;
    w.flush()
}

/// Files which every capture project must have.
fn required_files(package: &str) -> [String; 3] {
    [
        PYPROJECT.to_string(),
        format!("{package}/__init__.py"),
        format!("{package}/__main__.py"),
    ]
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

    let pointers = match &types.path_pointers {
        Some(_) => "ResourceConfig.PATH_POINTERS".to_string(),
        None => "[]".to_string(),
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
        config_schema = python_literal(&spec.config_schema),
        resource_config_schema = python_literal(&spec.resource_config_schema),
    );

    // Import only what's used, as a project's pyright may check this module.
    let mut cdk = Vec::new();
    if body.contains("common.") {
        cdk.push("from estuary_cdk.capture import common");
    }
    cdk.push(if body.contains("OAuth2Spec") {
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

/// Starter `spec.configSchema` of a scaffolded project, which describes the
/// endpoint configuration that its [`templates`] read.
pub fn starter_config_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "greeting": {
                "type": "string",
                "title": "Greeting",
                "description": "Greeting of each captured document",
                "default": "Hello",
            },
            "count": {
                "type": "integer",
                "title": "Count",
                "description": "Number of greetings to capture",
                "default": 10,
                "minimum": 0,
            },
        },
    })
}

/// Starter files of a new project, which implement a working capture.
pub fn templates(capture: &str, package: &str) -> Vec<(String, String)> {
    let generated_module = python_connector::module_parts(capture).join(".");
    let render = |template: &str| {
        template
            .replace("PROJECT_NAME", &package.replace('_', "-"))
            .replace("GENERATED_MODULE", &generated_module)
            .replace("PACKAGE", package)
            .replace("CDK_URL", CDK_URL)
    };
    vec![
        (
            PYPROJECT.to_string(),
            render(include_str!("templates/pyproject.toml")),
        ),
        (
            format!("{package}/__init__.py"),
            render(include_str!("templates/__init__.py")),
        ),
        (
            format!("{package}/__main__.py"),
            render(include_str!("templates/__main__.py")),
        ),
        (
            format!("{package}/api.py"),
            render(include_str!("templates/api.py")),
        ),
        (
            format!("{package}/models.py"),
            render(include_str!("templates/models.py")),
        ),
        (
            format!("{package}/resources.py"),
            render(include_str!("templates/resources.py")),
        ),
    ]
}

#[cfg(test)]
mod test {
    use super::*;

    fn sentinel(spec: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "capture": "acmeCo/source-acme",
            "package": "source_acme",
            "files": {"pyproject.toml": "[project]\n"},
            "spec": spec,
        })
    }

    #[test]
    fn missing_projects_are_scaffolded() {
        let config = serde_json::json!({"_python": sentinel(serde_json::json!({}))});
        let requests = [
            serde_json::json!({"spec": {"connectorType": "IMAGE", "config": config}}),
            serde_json::json!({"validate": {
                "name": "acmeCo/source-acme",
                "config": config,
                "bindings": [{"resourceConfig": {"name": "greetings"}}],
                "projectRoot": "file:///project/acmeCo/",
            }}),
        ];
        let requests: Vec<u8> = requests
            .iter()
            .flat_map(|r| [serde_json::to_vec(r).unwrap(), b"\n".to_vec()].concat())
            .collect();

        let sentinel = Sentinel::parse(&config["_python"]).unwrap();
        let mut first = String::new();
        let mut requests = std::io::Cursor::new(requests);
        requests.read_line(&mut first).unwrap();

        let mut out = Vec::new();
        scaffold(
            serde_json::from_str(&first).unwrap(),
            &sentinel,
            &["source_acme/__init__.py".to_string()],
            requests,
            &mut out,
        )
        .unwrap();

        let out: Vec<serde_json::Value> = out
            .split(|b| *b == b'\n')
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_slice(l).unwrap())
            .collect();

        // The existing pyproject.toml is not scaffolded.
        insta::assert_json_snapshot!(out);
    }

    #[test]
    fn specs_are_answered_until_another_request() {
        let with_lock = {
            let mut sentinel = sentinel(serde_json::json!({"configSchema": {"type": "object"}}));
            sentinel["files"]["uv.lock"] = serde_json::json!("version = 1\n");
            sentinel
        };
        let without_lock = sentinel(serde_json::json!({}));

        let session = |requests: Vec<serde_json::Value>| {
            let input: Vec<u8> = requests
                .iter()
                .flat_map(|r| [serde_json::to_vec(r).unwrap(), b"\n".to_vec()].concat())
                .collect();
            let mut input = std::io::Cursor::new(input);
            let mut out = Vec::new();

            // Specs never stage the project: the first other request is
            // returned to start the connector, and requests after it are unread.
            let first = answer_specs(&mut input, &mut out).unwrap();
            let unread = input.get_ref().len() - input.position() as usize;
            let responses: Vec<serde_json::Value> = out
                .split(|b| *b == b'\n')
                .filter(|l| !l.is_empty())
                .map(|l| serde_json::from_slice(l).unwrap())
                .collect();

            (
                responses.len(),
                first.map(|(first, sentinel)| {
                    (
                        first.as_object().unwrap().keys().next().unwrap().clone(),
                        install_of(&first, &sentinel),
                    )
                }),
                unread > 0,
            )
        };

        insta::assert_debug_snapshot!([
            // A bare image Spec, and a session's Spec, are answered alone.
            session(vec![serde_json::json!({"spec": {"config": {}}})]),
            session(vec![
                serde_json::json!({"spec": {"config": {"_python": with_lock}}}),
                serde_json::json!({"spec": {"config": {"_python": with_lock}}}),
                serde_json::json!({"open": {"capture": {"config": {"_python": with_lock}}}}),
                serde_json::json!({"acknowledge": {}}),
            ]),
            session(vec![
                serde_json::json!({"spec": {"config": {"_python": with_lock}}}),
                serde_json::json!({"validate": {"config": {"_python": with_lock}}}),
            ]),
            session(vec![
                serde_json::json!({"spec": {"config": {"_python": without_lock}}}),
                serde_json::json!({"validate": {"config": {"_python": without_lock}}}),
            ]),
            session(vec![
                serde_json::json!({"discover": {"config": {"_python": without_lock}}})
            ]),
        ]);

        let mut input = std::io::Cursor::new(b"{\"discover\": {\"config\": {}}}\n".to_vec());
        let err = answer_specs(&mut input, &mut Vec::new()).unwrap_err();
        insta::assert_snapshot!(format!("{err:#}"), @"capture-python requires a Python project configuration");
    }

    #[test]
    fn specs_are_answered_from_the_sentinel() {
        let declared = Sentinel::parse(&sentinel(serde_json::json!({
            "configSchema": {"type": "object", "properties": {"token": {"type": "string", "secret": true}}},
            "resourceConfigSchema": {"type": "object", "properties": {"name": {"type": "string", "x-collection-name": true}}},
            "oauth2": {"provider": "acme", "authUrlTemplate": "https://acme.example/authorize"},
        })))
        .unwrap();

        insta::assert_json_snapshot!([spec_response(&declared.spec), generic_spec()]);
    }

    #[test]
    fn generated_module_of_a_declared_spec() {
        let spec: Spec = serde_json::from_value(serde_json::json!({
            "configSchema": starter_config_schema(),
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
}

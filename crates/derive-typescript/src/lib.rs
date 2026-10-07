use anyhow::Context;
use proto_flow::{derive, flow};
use serde_json::json;
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

        let response = match &request.kind {
            Some(derive::request::Kind::Spec(request)) => {
                derive::response::Kind::Spec(Box::new(spec_response(&request.config_json)?))
            }
            Some(derive::request::Kind::Validate(request)) => {
                derive::response::Kind::Validated(validate(request)?)
            }
            Some(derive::request::Kind::Open(_)) => break request,
            _ => anyhow::bail!("unexpected request {request:?}"),
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

    // User code reads only its own configuration,
    // and needn't be re-sent the module's own code.
    let config = {
        let Some(derive::request::Kind::Open(open_kind)) = &mut open.kind else {
            unreachable!("loop breaks only on an Open request");
        };
        let derivation = open_kind
            .collection
            .as_mut()
            .and_then(|collection| collection.derivation.as_mut())
            .expect("Open of a derivation has a derivation");

        let config =
            parse_config(&derivation.config_json).context("failed to parse derivation config")?;
        derivation.config_json = serde_json::to_string(&config.user).unwrap().into();
        config
    };

    let Some(derive::request::Kind::Open(open_kind)) = &open.kind else {
        unreachable!("loop breaks only on an Open request");
    };
    let collection = open_kind.collection.as_ref().unwrap();
    let derivation = collection.derivation.as_ref().unwrap();

    let transforms = derivation
        .resolved_transforms()
        .map(|(transform, resolved)| (transform.name.as_str(), resolved.unwrap().0))
        .collect::<Vec<_>>();

    let temp_dir = tempfile::TempDir::new().unwrap();
    let temp_dir = temp_dir.path();

    stage_project(
        temp_dir,
        &collection.name,
        &codegen::types_ts(collection, &transforms, &config.spec)?,
        &config.module,
        &transforms,
    )?;

    tracing::debug!("starting TypeScript derivation");

    let mut child = deno_command(temp_dir)
        .args(DENO_RUN_ARGS)
        .arg(MAIN_NAME)
        .stdin(Stdio::piped())
        .spawn()
        .context(DENO_MISSING)?;

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

/// Connector configuration, parsed from either of its shapes:
///
/// * The user's `config` with a pushed-down `_typescript` sentinel property
///   of the derived collection, its module, and its declared spec.
/// * The legacy `{module}` shape of built specs which predate the sentinel,
///   which has no user configuration and declares no spec.
#[derive(Debug)]
struct Config {
    module: String,
    // User configuration, less the sentinel.
    user: serde_json::Map<String, serde_json::Value>,
    spec: Spec,
}

/// Declared connector spec of a derivation, which mirrors its Spec response.
#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct Spec {
    #[serde(default = "empty_schema")]
    config_schema: serde_json::Value,
    #[serde(default = "empty_schema")]
    resource_config_schema: serde_json::Value,
    #[serde(default)]
    oauth2: Option<serde_json::Value>,
}

impl Default for Spec {
    fn default() -> Self {
        Self {
            config_schema: empty_schema(),
            resource_config_schema: empty_schema(),
            oauth2: None,
        }
    }
}

fn empty_schema() -> serde_json::Value {
    json!({})
}

/// The `_typescript` sentinel. Its `collection` is not read, as a Validate
/// or Open also carries its collection.
#[derive(serde::Deserialize)]
struct Sentinel {
    module: String,
    #[serde(default)]
    spec: Option<Spec>,
}

fn parse_config(config_json: &[u8]) -> anyhow::Result<Config> {
    let mut user: serde_json::Map<String, serde_json::Value> =
        serde_json::from_slice(config_json).context("config is not a JSON object")?;

    if let Some(sentinel) = user.remove(SENTINEL) {
        let Sentinel { module, spec } =
            serde_json::from_value(sentinel).with_context(|| format!("invalid `{SENTINEL}`"))?;

        return Ok(Config {
            module,
            user,
            spec: spec.unwrap_or_default(),
        });
    }

    #[derive(serde::Deserialize)]
    struct Legacy {
        module: String,
        #[serde(default)]
        environment: serde_json::Map<String, serde_json::Value>,
    }
    let Legacy {
        module,
        environment,
    } = serde_json::from_value(serde_json::Value::Object(user))
        .with_context(|| format!("config has neither `{SENTINEL}` nor a legacy `module`"))?;

    if !environment.is_empty() {
        anyhow::bail!("environment is no longer supported; use `config` with a `secrets` stanza");
    }

    Ok(Config {
        module,
        user: serde_json::Map::new(),
        spec: Spec::default(),
    })
}

/// Answer a Spec from the spec declared by the derivation's model, without
/// loading its module. A legacy configuration, or the empty configuration of
/// a bare image Spec, declares none.
fn spec_response(config_json: &[u8]) -> anyhow::Result<derive::response::Spec> {
    #[derive(serde::Deserialize)]
    struct Partial {
        #[serde(rename = "_typescript")]
        sentinel: Option<Sentinel>,
    }
    let sentinel = if config_json.is_empty() {
        None
    } else {
        serde_json::from_slice::<Partial>(config_json)
            .context("invalid derivation config")?
            .sentinel
    };
    let spec = sentinel
        .and_then(|sentinel| sentinel.spec)
        .unwrap_or_default();

    Ok(derive::response::Spec {
        protocol: 3032023,
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

fn validate(validate: &derive::request::Validate) -> anyhow::Result<derive::response::Validated> {
    let derive::request::Validate {
        connector_type: _,
        collection,
        config_json,
        transforms: _,
        shuffle_key_types: _,
        project_root,
        ..
    } = validate;

    let collection = collection.as_ref().unwrap();

    let config = parse_config(config_json).context("invalid derivation configuration")?;

    let transforms = validate
        .resolved_transforms()
        .map(|(transform, resolved)| {
            if !transform.shuffle_lambda_config_json.is_empty() {
                anyhow::bail!("computed shuffles are not supported yet");
            }
            let (source, _identity) = resolved.context("transform missing source collection")?;

            Ok((transform.name.as_str(), source))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let types_path = types_path(&collection.name);
    let types_url = format!("{project_root}/{types_path}");
    let types_content = codegen::types_ts(collection, &transforms, &config.spec)?;
    tracing::debug!(%types_content, "generated TS types");

    let generated_files: Vec<(String, String)> = vec![
        (types_url.clone(), types_content.clone()),
        (
            format!("{project_root}/{DENO_NAME}"),
            serde_json::to_string_pretty(
                &json!({"imports": {"flow/": format!("./{GENERATED_PREFIX}/")}}),
            )
            .unwrap(),
        ),
    ];

    // Do we need to generate a module stub? There's no further validation we
    // can do, and no code to decide whether its transforms are read-only.
    if is_unresolved(&config.module) {
        let mut generated_files = generated_files;
        generated_files.push((
            config.module.clone(),
            codegen::stub_ts(collection, &transforms),
        ));

        return Ok(derive::response::Validated {
            transforms: vec![Default::default(); transforms.len()],
            generated_files: generated_files.into_iter().collect(),
        });
    }

    let temp_dir = tempfile::TempDir::new().unwrap();
    let temp_dir = temp_dir.path();

    stage_project(
        temp_dir,
        &collection.name,
        &types_content,
        &config.module,
        &transforms,
    )?;

    tracing::debug!("validating TypeScript derivation");

    let output = deno_command(temp_dir)
        .args(["check", MAIN_NAME])
        .output()
        .context(DENO_MISSING)?;

    if !output.status.success() {
        anyhow::bail!(rewrite_deno_stderr(
            &String::from_utf8_lossy(&output.stderr),
            temp_dir,
            &types_path,
        ));
    }

    // The derivation's own `validate` decides whether each transform is
    // read-only, and may fail validation outright.
    let validated = deno_validate(temp_dir, &types_path, &validate_input(&config, validate))
        .context("derivation validation failed")?;
    let mut validated: derive::response::Validated = serde_json::from_slice(&validated)
        .context("failed to parse the derivation's Validated response")?;

    if validated.transforms.len() != transforms.len() {
        anyhow::bail!(
            "derivation `validate` returned {} transforms, but the derivation has {}",
            validated.transforms.len(),
            transforms.len(),
        );
    }
    validated.generated_files = generated_files.into_iter().collect();

    Ok(validated)
}

/// Input of `main.ts validate`: the derivation's own configuration, and
/// the `Validate` of its generated types.
fn validate_input(config: &Config, validate: &derive::request::Validate) -> serde_json::Value {
    let transforms: Vec<serde_json::Value> = validate
        .transforms
        .iter()
        .map(|transform| {
            json!({
                "name": transform.name,
                "resourceConfig": resource_config(&transform.lambda_config_json),
            })
        })
        .collect();

    json!({
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
        return json!({});
    }
    serde_json::from_slice(lambda_config_json).unwrap_or(serde_json::Value::Null)
}

/// A module without whitespace is a relative URL which the catalog couldn't
/// resolve, rather than inline code.
fn is_unresolved(module: &str) -> bool {
    !module.chars().any(char::is_whitespace)
}

fn types_path(collection: &str) -> String {
    format!("{GENERATED_PREFIX}/{collection}.ts")
}

/// Stage a Deno project of the derivation's `module`, its `types` (which the
/// module imports as `flow/{collection}.ts`), and its `main.ts`.
fn stage_project(
    dir: &std::path::Path,
    collection: &str,
    types: &str,
    module: &str,
    transforms: &[(&str, &flow::CollectionSpec)],
) -> anyhow::Result<()> {
    std::fs::write(dir.join(TYPES_NAME), types)?;
    std::fs::write(
        dir.join(DENO_NAME),
        json!({"imports": {format!("flow/{collection}.ts"): format!("./{TYPES_NAME}")}})
            .to_string(),
    )?;
    std::fs::write(dir.join(MODULE_NAME), module)?;
    std::fs::write(dir.join(MAIN_NAME), codegen::main_ts(transforms))?;
    Ok(())
}

/// Run `main.ts validate` of the staged project under the permissions of a
/// derivation, returning the Validated response it prints.
fn deno_validate(
    dir: &std::path::Path,
    types_path: &str,
    input: &serde_json::Value,
) -> anyhow::Result<Vec<u8>> {
    let mut child = deno_command(dir)
        .args(DENO_RUN_ARGS)
        .args([MAIN_NAME, "validate"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context(DENO_MISSING)?;

    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(&serde_json::to_vec(input).unwrap())?;
    std::mem::drop(stdin);

    let output = child.wait_with_output()?;

    if !output.status.success() {
        anyhow::bail!(rewrite_deno_stderr(
            &String::from_utf8_lossy(&output.stderr),
            dir,
            types_path,
        ));
    }
    Ok(output.stdout)
}

/// Rewrite paths of the temp project to be relative to it, and map its
/// types to their `types_path` within the user's project.
fn rewrite_deno_stderr(stderr: &str, temp_dir: &std::path::Path, types_path: &str) -> String {
    let url = url::Url::from_directory_path(temp_dir).expect("temp_dir is absolute");
    let path = format!("{}/", temp_dir.display());

    // The URL goes first, because it contains the path.
    stderr
        .replace(url.join(TYPES_NAME).unwrap().as_str(), types_path)
        .replace(url.as_str(), "")
        .replace(&path, "")
}

fn deno_command(project_dir: &std::path::Path) -> std::process::Command {
    let mut command = std::process::Command::new("deno");
    command.current_dir(project_dir);
    command
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn stderr_paths_are_relative_to_the_project() {
        let stderr = concat!(
            "error: TS2322 at file:///tmp/.tmpABC/module.ts:12:5\n",
            "    at file:///tmp/.tmpABC/types.ts:40:3\n",
            "    at /tmp/.tmpABC/main.ts:7:1\n",
        );
        insta::assert_snapshot!(rewrite_deno_stderr(
            stderr,
            std::path::Path::new("/tmp/.tmpABC"),
            "flow_generated/typescript/acmeCo/orders.ts",
        ));
    }

    #[test]
    fn config_shapes() {
        let parse = |config: serde_json::Value| {
            parse_config(config.to_string().as_bytes()).map_err(|err| format!("{err:#}"))
        };

        insta::assert_debug_snapshot!([
            // The current shape, with user configuration beside the sentinel.
            parse(json!({
                "apiKey": "hunter2",
                "module": "a user property",
                "_typescript": {
                    "collection": "acmeCo/orders",
                    "module": "export class Derivation {}",
                    "spec": {"configSchema": {"type": "object"}},
                },
            })),
            // The legacy shape, which has no user configuration.
            parse(json!({"module": "export class Derivation {}"})),
            parse(json!({"module": "export class Derivation {}", "environment": {}})),
            parse(json!({"module": "export class Derivation {}", "environment": {"KEY": "value"}})),
            // A sentinel without a spec declares the default.
            parse(
                json!({"_typescript": {"collection": "acmeCo/orders", "module": "export class Derivation {}"}})
            ),
            // Errors.
            parse(json!({"_typescript": {"collection": "acmeCo/orders"}})),
            parse(json!({"apiKey": "hunter2"})),
            parse(json!(["not", "an", "object"])),
        ]);
    }

    #[test]
    fn specs_are_answered_from_the_sentinel() {
        let declared = json!({
            "apiKey": "hunter2",
            "_typescript": {
                "collection": "acmeCo/orders",
                "module": "export class Derivation {}",
                "spec": {
                    "configSchema": {
                        "type": "object",
                        "properties": {"apiKey": {"type": "string", "secret": true}},
                    },
                    "resourceConfigSchema": {
                        "type": "object",
                        "properties": {"readOnly": {"type": "boolean", "default": false}},
                    },
                },
            },
        });
        let responses = serde_json::to_value([
            spec_response(declared.to_string().as_bytes()).unwrap(),
            // Legacy configurations (and a bare image Spec) declare no spec.
            spec_response(br#"{"module": "export class Derivation {}"}"#).unwrap(),
            spec_response(b"{}").unwrap(),
            spec_response(b"").unwrap(),
        ])
        .unwrap();

        insta::assert_snapshot!(serde_json::to_string_pretty(&responses).unwrap());
    }

    #[test]
    fn generated_sources() {
        let source = flow::CollectionSpec {
            name: "acmeCo/source".to_string(),
            ..Default::default()
        };
        let derived = flow::CollectionSpec {
            name: "acmeCo/orders".to_string(),
            ..Default::default()
        };
        let transforms = [("fromOrders", &source), ("from-refunds", &source)];

        insta::assert_snapshot!("main", codegen::main_ts(&transforms));
        insta::assert_snapshot!("stub", codegen::stub_ts(&derived, &transforms));

        // Types of documents and of the declared spec.
        let source = flow::CollectionSpec {
            name: "acmeCo/source".to_string(),
            write_schema_json: json!({
                "type": "object",
                "properties": {"id": {"type": "string"}},
                "required": ["id"],
            })
            .to_string()
            .into(),
            ..Default::default()
        };
        let derived = flow::CollectionSpec {
            name: "acmeCo/orders".to_string(),
            write_schema_json: json!({
                "type": "object",
                "properties": {"id": {"type": "string"}, "limit": {"type": "integer"}},
                "required": ["id", "limit"],
            })
            .to_string()
            .into(),
            ..Default::default()
        };
        let spec = Spec {
            config_schema: json!({
                "type": "object",
                "properties": {
                    "apiKey": {"type": "string", "secret": true},
                    "limit": {"type": "integer", "default": 3},
                },
                "required": ["apiKey"],
            }),
            ..Default::default()
        };
        insta::assert_snapshot!(
            "types",
            codegen::types_ts(&derived, &[("fromOrders", &source)], &spec).unwrap()
        );
    }

    // Tests below run Deno, and are skipped where it's not installed (as in CI).
    fn has_deno() -> bool {
        let found = deno_command(std::path::Path::new("/"))
            .arg("--version")
            .output()
            .is_ok();
        if !found {
            eprintln!("skipping test because `deno` isn't installed");
        }
        found
    }

    const MODULE: &str = r#"
import { IDerivation, Document, EndpointConfig, Open, SourceFromOrders, Validate, Validated } from 'flow/acmeCo/orders.ts';

export class Derivation extends IDerivation {
    constructor(open: Open, readonly config: EndpointConfig) {
        super(open, config);
    }
    static override validate(validate: Validate, config: EndpointConfig): Validated {
        if (config.limit !== undefined && config.limit > 100) {
            throw new Error("limit is too large");
        }
        return super.validate(validate, config);
    }
    fromOrders(read: { doc: SourceFromOrders }): Document[] {
        return [{ id: read.doc.id, limit: this.config.limit ?? 3 }];
    }
}
"#;

    fn validate_request(module: &str, user: serde_json::Value) -> derive::request::Validate {
        let collection = |name: &str, schema: serde_json::Value| {
            json!({
                "name": name,
                "writeSchema": schema,
                "key": ["/id"],
                "uuidPtr": "/_meta/uuid",
            })
        };
        let mut config = user;
        config[SENTINEL] = json!({
            "collection": "acmeCo/orders",
            "module": module,
            "spec": {
                "configSchema": {
                    "type": "object",
                    "properties": {
                        "apiKey": {"type": "string", "secret": true},
                        "limit": {"type": "integer", "default": 3},
                    },
                    "required": ["apiKey", "limit"],
                },
                "resourceConfigSchema": {
                    "type": "object",
                    "properties": {"readOnly": {"type": "boolean", "default": false}},
                },
            },
        });
        // Parse from text, as raw JSON fields don't deserialize from a Value.
        serde_json::from_str(
            &json!({
                "connectorType": "TYPESCRIPT",
                "config": config,
                "collection": collection("acmeCo/orders", json!({
                    "type": "object",
                    "properties": {"id": {"type": "string"}, "limit": {"type": "integer"}},
                    "required": ["id", "limit"],
                })),
                "transforms": [{
                    "name": "fromOrders",
                    "collection": collection("acmeCo/source", json!({
                        "type": "object",
                        "properties": {"id": {"type": "string"}},
                        "required": ["id"],
                    })),
                    "lambdaConfig": {"readOnly": true},
                }],
                "projectRoot": "file:///project",
            })
            .to_string(),
        )
        .unwrap()
    }

    #[test]
    fn modules_which_predate_config_still_check() {
        if !has_deno() {
            return;
        }
        // A module written for the V1 interface, whose constructor
        // takes only `open`.
        const LEGACY: &str = r#"
import { IDerivation, Document, SourceFromOrders } from 'flow/acmeCo/orders.ts';

export class Derivation extends IDerivation {
    constructor(open: { state: unknown }) {
        super(open as never);
    }
    fromOrders(read: { doc: SourceFromOrders }): Document[] {
        return [{ id: read.doc.id, limit: 1 }];
    }
}
"#;
        const UNTYPED: &str = r#"
import { IDerivation, Document, Open, SourceFromOrders } from 'flow/acmeCo/orders.ts';

export class Derivation extends IDerivation {
    constructor(open: Open) {
        super(open);
    }
    fromOrders(read: { doc: SourceFromOrders }): Document[] {
        return [{ id: read.doc.id, limit: 1 }];
    }
}
"#;
        for module in [LEGACY, UNTYPED] {
            let validated = validate(&validate_request(module, json!({"apiKey": "k"}))).unwrap();
            assert_eq!(validated.transforms.len(), 1);
        }
    }

    #[test]
    fn validate_checks_types_and_runs_the_derivation() {
        if !has_deno() {
            return;
        }
        let collection = |name: &str, schema: serde_json::Value| {
            json!({
                "name": name,
                "writeSchema": schema,
                "key": ["/id"],
                "uuidPtr": "/_meta/uuid",
            })
        };
        let request = |user: serde_json::Value| -> derive::request::Validate {
            let mut config = user;
            config[SENTINEL] = json!({
                "collection": "acmeCo/orders",
                "module": MODULE,
                "spec": {
                    "configSchema": {
                        "type": "object",
                        "properties": {
                            "apiKey": {"type": "string", "secret": true},
                            "limit": {"type": "integer", "default": 3},
                        },
                        "required": ["apiKey"],
                    },
                    "resourceConfigSchema": {
                        "type": "object",
                        "properties": {"readOnly": {"type": "boolean", "default": false}},
                    },
                },
            });
            // Parse from text, as raw JSON fields don't deserialize from a Value.
            serde_json::from_str(
                &json!({
                    "connectorType": "TYPESCRIPT",
                    "config": config,
                    "collection": collection("acmeCo/orders", json!({
                        "type": "object",
                        "properties": {"id": {"type": "string"}, "limit": {"type": "integer"}},
                        "required": ["id", "limit"],
                    })),
                    "transforms": [{
                        "name": "fromOrders",
                        "collection": collection("acmeCo/source", json!({
                            "type": "object",
                            "properties": {"id": {"type": "string"}},
                            "required": ["id"],
                        })),
                        "lambdaConfig": {"readOnly": true},
                    }],
                    "projectRoot": "file:///project",
                })
                .to_string(),
            )
            .unwrap()
        };

        let validated = validate(&request(json!({"apiKey": "hunter2"}))).unwrap();
        assert_eq!(validated.transforms.len(), 1);
        assert!(validated.transforms[0].read_only);

        let err = validate(&request(json!({"apiKey": "hunter2", "limit": 500}))).unwrap_err();
        assert!(format!("{err:#}").contains("limit is too large"), "{err:#}");
    }
}

const DENO_MISSING: &str = "The Deno runtime is a prerequisite for TypeScript but could not be found. Please install Deno from https://deno.com";
// Permissions of every `deno run` of the module, so that it loads alike
// whether validating or running.
const DENO_RUN_ARGS: [&str; 2] = ["run", "--allow-net=api.openai.com"];
const DENO_NAME: &str = "deno.json";
const GENERATED_PREFIX: &str = "flow_generated/typescript";
const MAIN_NAME: &str = "main.ts";
const MODULE_NAME: &str = "module.ts";
const SENTINEL: &str = "_typescript";
const TYPES_NAME: &str = "types.ts";

use anyhow::Context;
use models::custom_connector::{Language, Sentinel, Spec};
use proto_flow::{derive, flow};
use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::process::Stdio;
use typescript_connector::{DENO_MISSING, ESTUARY_PATH, ESTUARY_TS, Project};

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
            kind @ Some(derive::request::Kind::Open(_)) => {
                break derive::Request { kind, ..request };
            }
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

    // User code reads only its own configuration,
    // and needn't be re-sent the project's own files.
    let config = {
        let Some(derive::request::Kind::Open(open_kind)) = &mut open.kind else {
            unreachable!("loop breaks only on an Open request");
        };
        let collection = open_kind
            .collection
            .as_mut()
            .expect("Open of a derivation has a collection");
        let derivation = collection
            .derivation
            .as_mut()
            .expect("Open of a derivation has a derivation");

        let config = parse_config(&derivation.config_json, &collection.name)
            .context("failed to parse derivation config")?;
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

    // A lock is baked or listed, except in legacy built specs (which run
    // unfrozen, as they always have) and projects having no dependencies.
    let project = stage(
        &config.sentinel,
        &collection.name,
        &codegen::types_ts(collection, &transforms, &config.sentinel.spec)?,
        &transforms,
    )?;

    tracing::debug!(temp_dir = ?project.root(), "starting TypeScript derivation");

    let mut child = project
        .command(DENO_PERMISSIONS, &[])
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
///   of the derived collection, its project `files`, and its declared spec.
/// * The legacy `{module}` shape of built specs which predate the sentinel,
///   which has no user configuration and declares no spec. It's staged as
///   the files which validation builds a legacy model with
///   (`Language::legacy_files`).
#[derive(Debug)]
struct Config {
    // User configuration, less the sentinel.
    user: serde_json::Map<String, serde_json::Value>,
    /// Sentinel of the derivation, whose declared spec has its defaults
    /// resolved. A listed file which failed to load is given a starter.
    sentinel: Sentinel,
}

/// Parse the connector configuration of derivation `task`.
fn parse_config(config_json: &[u8], task: &str) -> anyhow::Result<Config> {
    let (user, sentinel) = Sentinel::split(config_json, Language::Typescript)?;

    if let Some(mut sentinel) = sentinel {
        sentinel.spec = sentinel.spec.resolve_derivation();
        return Ok(Config { user, sentinel });
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
    } = serde_json::from_value(serde_json::Value::Object(user)).with_context(|| {
        format!(
            "config has neither `{}` nor a legacy `module`",
            Language::Typescript.sentinel()
        )
    })?;

    if !environment.is_empty() {
        anyhow::bail!("environment is no longer supported; use `config` with a `secrets` stanza");
    }

    Ok(Config {
        user: serde_json::Map::new(),
        sentinel: Sentinel {
            task: task.to_string(),
            files: Language::Typescript.legacy_files(task, module),
            spec: Spec::default().resolve_derivation(),
        },
    })
}

/// Answer a Spec from the spec declared by the derivation's model, without
/// loading its module. A legacy configuration, or the empty configuration of
/// a bare image Spec, declares none.
fn spec_response(config_json: &[u8]) -> anyhow::Result<derive::response::Spec> {
    let sentinel = if config_json.is_empty() {
        None
    } else {
        Sentinel::split(config_json, Language::Typescript)
            .context("invalid derivation config")?
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
        parse_config(&validate.config_json, &task).context("invalid derivation configuration")?;
    // User code reads only its own configuration, as of an Open (see `run`).
    validate.config_json = serde_json::to_vec(&config.user).unwrap().into();

    let derive::request::Validate {
        collection,
        project_root,
        ..
    } = &validate;

    let collection = collection.as_ref().unwrap();
    let project_root = project_root.trim_end_matches('/');

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

    let types_content = codegen::types_ts(collection, &transforms, &config.sentinel.spec)?;
    tracing::debug!(%types_content, "generated TS types");

    let mut generated_files: BTreeMap<String, String> = [
        (
            format!("{project_root}/{}", types_path(&collection.name)),
            types_content.clone(),
        ),
        (
            format!("{project_root}/{ESTUARY_PATH}"),
            ESTUARY_TS.to_string(),
        ),
        (
            format!("{project_root}/{DERIVE_PATH}"),
            DERIVE_TS.to_string(),
        ),
    ]
    .into();

    // A project having listed files which don't exist yet can't be run.
    // They're given starters, and there's no code to decide whether its
    // transforms are read-only. Its missing files are load errors, and it
    // can't be published.
    let missing = config.sentinel.missing();
    if !missing.is_empty() {
        for path in missing {
            let content = if path == Language::Typescript.entry(config.sentinel.dir()) {
                codegen::starter_ts(collection, &transforms)
            } else if path == Language::Typescript.manifest() {
                Language::Typescript.starter_manifest(config.sentinel.dir())
            } else {
                String::new()
            };
            generated_files.insert(format!("{project_root}/{path}"), content);
        }
        return Ok(derive::response::Validated {
            transforms: vec![Default::default(); transforms.len()],
            generated_files,
        });
    }

    let project = stage(
        &config.sentinel,
        &collection.name,
        &types_content,
        &transforms,
    )?;

    tracing::debug!(temp_dir = ?project.root(), "validating TypeScript derivation");

    project.check(&Language::Typescript.entry(config.sentinel.dir()))?;

    // The derivation's own `validate` decides whether each transform is
    // read-only, and may fail validation outright.
    let validated = project
        .run(
            DENO_PERMISSIONS,
            &["validate"],
            &serde_json::to_vec(&validate).unwrap(),
        )
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

    // Deno resolved a lock of a project which doesn't list one, as it ran.
    if !config.sentinel.has_lock(Language::Typescript)
        && let Some(lock) = project.lock()?
    {
        generated_files.insert(
            format!("{project_root}/{}", Language::Typescript.lock()),
            lock,
        );
    }
    validated.generated_files = generated_files;

    Ok(validated)
}

/// Path of the generated types of a derived `collection`, which its module
/// imports as `flow/{collection}.ts` through the project's `deno.json`.
fn types_path(collection: &str) -> String {
    format!("{}/{collection}.ts", typescript_connector::GENERATED_PREFIX)
}

/// Stage a Deno project of the derivation's `files`, its generated `types`,
/// and its generated entry point, which imports the derivation's `mod.ts`.
/// It's frozen if it has a lock.
fn stage(
    sentinel: &Sentinel,
    collection: &str,
    types: &str,
    transforms: &[(&str, &flow::CollectionSpec)],
) -> anyhow::Result<Project> {
    let entry = format!("../{}", Language::Typescript.entry(sentinel.dir()));
    let main = codegen::main_ts(transforms, &entry);
    let types_path = types_path(collection);

    Project::stage(
        sentinel
            .files
            .iter()
            .filter_map(|(path, content)| Some((path.as_str(), content.as_deref()?)))
            .chain([
                (types_path.as_str(), types),
                (ESTUARY_PATH, ESTUARY_TS),
                (DERIVE_PATH, DERIVE_TS),
                (typescript_connector::ENTRY, main.as_str()),
            ]),
        sentinel.has_lock(Language::Typescript),
    )
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    #[test]
    fn config_shapes() {
        let parse = |config: serde_json::Value| {
            parse_config(config.to_string().as_bytes(), "acmeCo/orders")
                .map_err(|err| format!("{err:#}"))
        };

        insta::assert_debug_snapshot!([
            // The current shape, with user configuration beside the sentinel.
            parse(json!({
                "apiKey": "hunter2",
                "module": "a user property",
                "_typescript": {
                    "task": "acmeCo/orders",
                    "files": {"deno.json": "{}", "orders/mod.ts": "export class Derivation {}", "orders/missing.ts": null},
                    "spec": {"configSchema": {"type": "object"}},
                },
            })),
            // The legacy shape, which has no user configuration.
            parse(json!({"module": "export class Derivation {}"})),
            parse(json!({"module": "export class Derivation {}", "environment": {}})),
            parse(json!({"module": "export class Derivation {}", "environment": {"KEY": "value"}})),
            // Errors.
            parse(json!({"_typescript": {"files": {}}})),
            parse(json!({"apiKey": "hunter2"})),
            parse(json!(["not", "an", "object"])),
        ]);
    }

    #[test]
    fn specs_are_answered_from_the_sentinel() {
        let declared = json!({
            "apiKey": "hunter2",
            "_typescript": {
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
                },
            },
        });
        let responses = serde_json::to_value([
            spec_response(declared.to_string().as_bytes()).unwrap(),
            // A sentinel which declares nothing has the connector's defaults.
            spec_response(br#"{"_typescript": {"task": "acmeCo/orders"}}"#).unwrap(),
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

        insta::assert_snapshot!("main", codegen::main_ts(&transforms, "../orders/mod.ts"));
        insta::assert_snapshot!("starter", codegen::starter_ts(&derived, &transforms));

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
        let spec = Spec::resolve_derivation(
            serde_json::from_value(json!({
                "configSchema": {
                    "type": "object",
                    "properties": {
                        "apiKey": {"type": "string", "secret": true},
                        "limit": {"type": "integer", "default": 3},
                    },
                    "required": ["apiKey"],
                },
            }))
            .unwrap(),
        );
        insta::assert_snapshot!(
            "types",
            codegen::types_ts(&derived, &[("fromOrders", &source)], &spec).unwrap()
        );
    }

    // Tests below run Deno, and are skipped where it's not installed (as in CI).
    // Those of locks also fetch a package from JSR.
    fn has_deno() -> bool {
        let found = std::process::Command::new("deno")
            .arg("--version")
            .output()
            .is_ok();
        if !found {
            eprintln!("skipping test because `deno` isn't installed");
        }
        found
    }

    const MODULE: &str = r#"
import { IDerivation, Document, EndpointConfig, Open, Read, SourceFromOrders, Validate, Validated } from 'flow/acmeCo/orders.ts';
import { limitOf } from './helpers.ts';

export class Derivation extends IDerivation {
    readonly config: EndpointConfig;

    constructor(open: Open) {
        super(open);
        this.config = open.collection.derivation.config;
    }
    static override validate(validate: Validate): Validated {
        if (limitOf(validate.config) > 100) {
            throw new Error("limit is too large");
        }
        if (validate.transforms[0].collection.name != "acmeCo/source") {
            throw new Error("transform isn't linked to its source");
        }
        return super.validate(validate);
    }
    fromOrders(read: Read<SourceFromOrders>): Document[] {
        return [{ id: read.doc.id, limit: limitOf(this.config) }];
    }
}
"#;

    const HELPERS: &str = r#"
import { EndpointConfig } from 'flow/acmeCo/orders.ts';

export function limitOf(config: EndpointConfig): number {
    return config.limit ?? 3;
}
"#;

    fn validate_request(
        files: serde_json::Value,
        user: serde_json::Value,
    ) -> derive::request::Validate {
        let collection = |name: &str, schema: serde_json::Value| {
            json!({
                "name": name,
                "writeSchema": schema,
                "key": ["/id"],
                "uuidPtr": "/_meta/uuid",
            })
        };
        let mut config = user;
        config[Language::Typescript.sentinel()] = json!({
            "task": "acmeCo/orders",
            "files": files,
            "spec": {
                "configSchema": {
                    "type": "object",
                    "properties": {
                        "apiKey": {"type": "string", "secret": true},
                        "limit": {"type": "integer", "default": 3},
                    },
                    "required": ["apiKey"],
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

    fn project(module: &str) -> serde_json::Value {
        json!({
            "deno.json": Language::Typescript.starter_manifest("orders"),
            "orders/mod.ts": module,
            "orders/helpers.ts": HELPERS,
        })
    }

    #[test]
    fn missing_files_are_given_starters() {
        let request = validate_request(
            json!({"deno.json": null, "orders/mod.ts": null, "orders/helpers.ts": null}),
            json!({"apiKey": "k"}),
        );
        let validated = validate_derivation(request).unwrap();

        insta::assert_debug_snapshot!(validated.generated_files.keys().collect::<Vec<_>>());

        if !has_deno() {
            return;
        }
        // The starters are a working derivation.
        let files: serde_json::Map<String, serde_json::Value> = validated
            .generated_files
            .into_iter()
            .map(|(url, content)| {
                let path = url.strip_prefix("file:///project/").unwrap().to_string();
                (path, json!(content))
            })
            .collect();
        let validated = validate_derivation(validate_request(
            serde_json::Value::Object(files),
            json!({"apiKey": "k"}),
        ))
        .unwrap();
        assert_eq!(validated.transforms.len(), 1);
    }

    #[test]
    fn modules_which_predate_config_still_check() {
        if !has_deno() {
            return;
        }
        // A module written for the V1 interface, whose constructor takes only
        // `open`, typed by its master-era shape, and whose flush() returns
        // a bare `Document[]` or a `FlushResponse`.
        const LEGACY: &str = r#"
import { IDerivation, Document, SourceFromOrders } from 'flow/acmeCo/orders.ts';

export class Derivation extends IDerivation {
    constructor(open: { state: unknown; range?: { keyBegin?: number; keyEnd?: number } }) {
        super(open);
    }
    fromOrders(read: { doc: SourceFromOrders }): Document[] {
        return [{ id: read.doc.id, limit: 1 }];
    }
    override async flush(): Promise<Document[]> {
        return [];
    }
}
"#;
        const UNTYPED: &str = r#"
import { IDerivation, Document, FlushResponse, Open, SourceFromOrders } from 'flow/acmeCo/orders.ts';

export class Derivation extends IDerivation {
    constructor(open: Open) {
        super(open);
    }
    fromOrders(read: { doc: SourceFromOrders }): Document[] {
        return [{ id: read.doc.id, limit: 1 }];
    }
    override async flush(): Promise<FlushResponse> {
        return { published: [], more: false };
    }
}
"#;
        for module in [LEGACY, UNTYPED] {
            let validated =
                validate_derivation(validate_request(project(module), json!({"apiKey": "k"})))
                    .unwrap();
            assert_eq!(validated.transforms.len(), 1);
        }
    }

    #[test]
    fn validate_checks_types_and_runs_the_derivation() {
        if !has_deno() {
            return;
        }
        let request = |user: serde_json::Value| validate_request(project(MODULE), user);

        let validated = validate_derivation(request(json!({"apiKey": "hunter2"}))).unwrap();
        assert_eq!(validated.transforms.len(), 1);
        assert!(validated.transforms[0].read_only);

        let err =
            validate_derivation(request(json!({"apiKey": "hunter2", "limit": 500}))).unwrap_err();
        assert!(format!("{err:#}").contains("limit is too large"), "{err:#}");

        // A type error is reported at its path within the project.
        let err = validate_derivation(validate_request(
            project(&MODULE.replace("limitOf(this.config)", "\"not a number\"")),
            json!({"apiKey": "hunter2"}),
        ))
        .unwrap_err();
        assert!(format!("{err:#}").contains("orders/mod.ts"), "{err:#}");
    }

    #[test]
    fn locks_are_resolved_or_used_as_listed() {
        if !has_deno() {
            return;
        }
        let lock_name = Language::Typescript.lock();
        let lock_url = format!("file:///project/{lock_name}");
        let request = |lock: Option<&str>| {
            let mut files = project(MODULE);
            files["orders/helpers.ts"] = json!(HELPERS.replace(
                "export function",
                "import { assert } from 'jsr:@std/assert@1.0.10';\nassert(true);\nexport function",
            ));
            if let Some(lock) = lock {
                files[lock_name] = json!(lock);
            }
            validate_request(files, json!({"apiKey": "k"}))
        };

        // A project having no dependencies resolves no lock.
        let validated =
            validate_derivation(validate_request(project(MODULE), json!({"apiKey": "k"}))).unwrap();
        assert!(!validated.generated_files.contains_key(&lock_url));

        // A project which doesn't list a lock is given the one Deno resolved.
        let validated = validate_derivation(request(None)).unwrap();
        let lock = &validated.generated_files[&lock_url];
        assert!(lock.contains("jsr:@std/assert@1.0.10"), "{lock}");

        // A listed lock is used as-is, and isn't generated.
        let validated = validate_derivation(request(Some(lock))).unwrap();
        assert!(!validated.generated_files.contains_key(&lock_url));

        // A listed lock which doesn't pin every import fails validation.
        let err = validate_derivation(request(Some(r#"{"version": "5"}"#))).unwrap_err();
        assert!(
            format!("{err:#}").contains(
                "deno.lock is out of date with its imports: run `deno install --entrypoint orders/mod.ts`"
            ),
            "{err:#}"
        );
    }
}

// Permissions of every `deno run` of the module, so that it loads alike
// whether validating or running.
const DENO_PERMISSIONS: &[&str] = &["--allow-net=api.openai.com"];
// Protocol types of derivations, beside the platform's `estuary.ts`.
const DERIVE_PATH: &str = "flow_generated/typescript/derive.ts";
const DERIVE_TS: &str = include_str!("derive.ts");

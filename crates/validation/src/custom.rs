//! User-authored custom connectors: Python captures, and Python and
//! TypeScript derivations. Each is rewritten at build time into an ordinary
//! image connector, whose configuration is the user's `config` with a
//! pushed-down `models::custom_connector::Sentinel` property carrying the
//! task's name, its `files`, and its declared `spec`. A derivation model
//! having a legacy `module` is built as `files` (its module beside a starter
//! manifest), and is stored as authored.
//!
//! A task's `files` are relative to its specification's directory, which is
//! Validate's `project_root`. Each task has one directory there, named
//! verbatim for the final component of the task's name, which holds its
//! entry file. A custom connector answers Spec from the sentinel's `spec`
//! (resolving a schema it omits to the connector's own default), and never
//! runs user code to do so.

use super::{Error, Scope};
use models::custom_connector::{Language, task_dir};
use proto_flow::{capture, flow};
use std::collections::BTreeMap;

/// Shard flag which overrides the image tag of every custom connector,
/// such as `local` for a locally-built image.
pub const IMAGE_TAG_FLAG: &str = "custom-image-tag";

pub const CAPTURE_PYTHON_IMAGE: &str = "ghcr.io/estuary/capture-python";
pub const DERIVE_PYTHON_IMAGE: &str = "ghcr.io/estuary/derive-python";
pub const DERIVE_TYPESCRIPT_IMAGE: &str = "ghcr.io/estuary/derive-typescript";

/// Image tag of a custom connector: an explicit `custom-image-tag` flag, or
/// else `stable`. Derivations of the V1 runtime instead use the frozen `dev`
/// images, which predate `files` and `config`. There is no `dev` image of
/// capture-python, which serves either runtime.
pub fn image_tag<'s>(
    shards: &'s models::ShardTemplate,
    catalog_type: models::CatalogType,
) -> &'s str {
    let default = match catalog_type {
        models::CatalogType::Collection if !shards.uses_runtime_v2(catalog_type) => "dev",
        _ => "stable",
    };
    super::flag_value(&shards.flags, IMAGE_TAG_FLAG).unwrap_or(default)
}

/// Map a capture's endpoint into the connector Spec request used by
/// validation, discovery, and local flowctl commands.
pub fn capture_spec_request(
    capture: &models::Capture,
    endpoint: &models::CaptureEndpoint,
    shards: &models::ShardTemplate,
) -> capture::request::Spec {
    let (connector_type, config_json) = match endpoint {
        models::CaptureEndpoint::Connector(config) => (
            flow::capture_spec::ConnectorType::Image,
            serde_json::to_string(config).unwrap(),
        ),
        models::CaptureEndpoint::Local(config) => (
            flow::capture_spec::ConnectorType::Local,
            serde_json::to_string(config).unwrap(),
        ),
        models::CaptureEndpoint::Python(python) => (
            flow::capture_spec::ConnectorType::Image,
            serde_json::to_string(&capture_python_connector(capture, python, shards)).unwrap(),
        ),
    };
    capture::request::Spec {
        connector_type: connector_type as i32,
        config_json: config_json.into(),
    }
}

/// Resolve a Python capture into its image connector.
/// The `capture` names its directory and its generated module.
pub fn capture_python_connector(
    capture: &models::Capture,
    python: &models::CapturePython,
    shards: &models::ShardTemplate,
) -> models::ConnectorConfig {
    let models::CapturePython {
        files,
        config,
        spec,
    } = python;

    models::ConnectorConfig {
        image: format!(
            "{CAPTURE_PYTHON_IMAGE}:{}",
            image_tag(shards, models::CatalogType::Capture)
        ),
        config: with_sentinel(
            config,
            Language::Python.sentinel(),
            &models::custom_connector::Sentinel {
                task: capture.to_string(),
                files: files.inline().cloned().unwrap_or_default(),
                spec: spec.clone(),
            },
        ),
    }
}

/// Resolve a Python derivation into its image connector.
/// The derived `collection` names its directory and its generated module.
pub fn derive_python_connector(
    collection: &models::Collection,
    python: &models::DeriveUsingPython,
    shards: &models::ShardTemplate,
) -> models::ConnectorConfig {
    let models::DeriveUsingPython {
        files,
        config,
        spec,
        module,
        dependencies: _,
        environment: _,
    } = python;
    let legacy = module
        .as_ref()
        .map(|module| legacy_files(Language::Python, collection, module));

    derive_connector(
        DERIVE_PYTHON_IMAGE,
        Language::Python,
        collection,
        legacy.as_ref().unwrap_or(files),
        config,
        spec,
        shards,
    )
}

/// Resolve a TypeScript derivation into its image connector.
/// The derived `collection` names its directory and its generated module.
pub fn derive_typescript_connector(
    collection: &models::Collection,
    typescript: &models::DeriveUsingTypescript,
    shards: &models::ShardTemplate,
) -> models::ConnectorConfig {
    let models::DeriveUsingTypescript {
        files,
        config,
        spec,
        module,
        environment: _,
    } = typescript;
    let legacy = module
        .as_ref()
        .map(|module| legacy_files(Language::Typescript, collection, module));

    derive_connector(
        DERIVE_TYPESCRIPT_IMAGE,
        Language::Typescript,
        collection,
        legacy.as_ref().unwrap_or(files),
        config,
        spec,
        shards,
    )
}

fn derive_connector(
    image: &str,
    language: Language,
    collection: &models::Collection,
    files: &models::custom_connector::Files,
    config: &models::RawValue,
    spec: &models::custom_connector::Spec,
    shards: &models::ShardTemplate,
) -> models::ConnectorConfig {
    let tag = image_tag(shards, models::CatalogType::Collection);

    // The frozen `dev` image understands only its original configuration,
    // a single module, which is the derivation's entry file.
    let config = if tag == "dev" {
        let entry = language.entry(task_dir(collection));
        let module = files
            .inline()
            .and_then(|files| files.get(&entry))
            .cloned()
            .flatten()
            .unwrap_or_default();

        models::RawValue::from_value(&serde_json::json!({ "module": module }))
    } else {
        with_sentinel(
            config,
            language.sentinel(),
            &models::custom_connector::Sentinel {
                task: collection.to_string(),
                files: files.inline().cloned().unwrap_or_default(),
                spec: spec.clone(),
            },
        )
    };
    models::ConnectorConfig {
        image: format!("{image}:{tag}"),
        config,
    }
}

/// Validate the `spec` of a custom connector. Each declared schema must
/// compile, else every start of the task would fail when its connector
/// configuration is examined for IAM authentication.
///
/// `config` isn't validated against `configSchema`: secrets aren't merged
/// into it until the task is started, and the connector's own types (and not
/// its declared schema) are what decide whether a configuration is usable.
pub fn walk_spec(scope: Scope, spec: &models::custom_connector::Spec, errors: &mut tables::Errors) {
    let models::custom_connector::Spec {
        config_schema,
        resource_config_schema,
        oauth2: _,
    } = spec;

    for (prop, schema) in [
        ("configSchema", config_schema),
        ("resourceConfigSchema", resource_config_schema),
    ] {
        let Some(schema) = schema else {
            continue;
        };
        if let Err(err) = super::schema::Schema::new(schema.get().as_bytes()) {
            err.push(scope.push_prop(prop), errors);
        }
    }
}

/// Compose the built configuration of a custom connector: the user's
/// `config` object, extended with the `sentinel` property.
fn with_sentinel(
    config: &models::RawValue,
    sentinel: &str,
    value: &models::custom_connector::Sentinel,
) -> models::RawValue {
    let mut config = match serde_json::from_str::<serde_json::Value>(config.get()) {
        Ok(serde_json::Value::Object(config)) => config,
        _ => serde_json::Map::new(), // Reported by `walk_config`.
    };
    config.insert(sentinel.to_string(), serde_json::to_value(value).unwrap());

    models::RawValue::from_value(&serde_json::Value::Object(config))
}

/// Validate the user `config` of a custom connector.
pub fn walk_config(
    scope: Scope,
    config: &models::RawValue,
    sentinel: &str,
    errors: &mut tables::Errors,
) {
    match serde_json::from_str::<serde_json::Value>(config.get()) {
        Ok(serde_json::Value::Object(config)) => {
            if config.contains_key(sentinel) {
                Error::CustomConfig {
                    detail: format!(
                        "cannot have the property `{sentinel}`, which is reserved by the connector"
                    ),
                }
                .push(scope, errors);
            }
            // The build injects the sentinel into the configuration,
            // which would invalidate the MAC of a `sops` document.
            if config.contains_key("sops") {
                Error::CustomConfig {
                    detail: "cannot be sealed with `sops`: use a plaintext configuration with a `secrets` stanza instead".to_string(),
                }
                .push(scope, errors);
            }
        }
        // A config which failed to load is left as its URL,
        // and its load error has already been reported.
        Ok(serde_json::Value::String(_)) => {}
        _ => Error::CustomConfig {
            detail: "must be an object".to_string(),
        }
        .push(scope, errors),
    }
}

/// Validate the `files` of a custom task `name`. Each path must be
/// well-formed and not reserved, and its manifest and entry file must be
/// listed: their absence is how a new task is bootstrapped, by an error which
/// names the files to list.
pub fn walk_files(
    scope: Scope,
    language: Language,
    name: &str,
    files: &models::custom_connector::Files,
    errors: &mut tables::Errors,
) {
    for (index, path) in files.paths().enumerate() {
        let scope = match files {
            models::custom_connector::Files::Indirect(_) => scope.push_item(index),
            models::custom_connector::Files::Inline(_) => scope.push_prop(path),
        };
        if !models::custom_connector::Files::is_valid_path(path) || language.is_reserved(path) {
            Error::CustomFilePath {
                path: path.to_string(),
                reserved: language
                    .reserved()
                    .iter()
                    .map(|dir| format!("`{dir}/`"))
                    .collect::<Vec<_>>()
                    .join(" or "),
            }
            .push(scope, errors);
        }
    }

    let missing: Vec<String> = [
        language.manifest().to_string(),
        language.entry(task_dir(name)),
    ]
    .into_iter()
    .filter(|required| !files.paths().any(|path| path == required))
    .map(|path| format!("`{path}`"))
    .collect();

    if !missing.is_empty() {
        Error::CustomFilesMissing {
            paths: missing.join(" and "),
        }
        .push(scope, errors);
    }
}

/// Require that a derivation model having a legacy `module` is wholly legacy,
/// and that no model has the removed `dependencies` or `environment`.
pub fn walk_legacy_fields(scope: Scope, using: &models::DeriveUsing, errors: &mut tables::Errors) {
    let no_dependencies = BTreeMap::new();
    let (scope, module, files, config, spec, dependencies, environment) = match using {
        models::DeriveUsing::Python(python) => (
            scope.push_prop("python"),
            &python.module,
            &python.files,
            &python.config,
            &python.spec,
            &python.dependencies,
            &python.environment,
        ),
        models::DeriveUsing::Typescript(typescript) => (
            scope.push_prop("typescript"),
            &typescript.module,
            &typescript.files,
            &typescript.config,
            &typescript.spec,
            &no_dependencies,
            &typescript.environment,
        ),
        _ => return,
    };

    if !dependencies.is_empty() {
        Error::CustomRemovedField {
            field: "dependencies",
            replacement: "declare them in a `pyproject.toml` listed in `files`",
        }
        .push(scope.push_prop("dependencies"), errors);
    }
    if !environment.is_empty() {
        Error::CustomRemovedField {
            field: "environment",
            replacement: "use `config`, with a `secrets` stanza for sensitive values",
        }
        .push(scope.push_prop("environment"), errors);
    }
    if module.is_none() {
        return;
    }
    for (field, present) in [
        ("files", !files.is_empty()),
        ("config", config.get().trim() != "{}"),
        ("spec", !spec.is_empty()),
    ] {
        if present {
            Error::CustomLegacyMixed {}.push(scope.push_prop(field), errors);
        }
    }
}

/// Files which a derivation model having a legacy `module` is built as:
/// its module is the derivation's entry file, beside a starter manifest.
fn legacy_files(
    language: Language,
    collection: &models::Collection,
    module: &models::RawValue,
) -> models::custom_connector::Files {
    // A module which failed to load is its unresolved URL, and its load error
    // has already been reported.
    let content = serde_json::from_str::<String>(module.get()).unwrap_or_default();

    models::custom_connector::Files::Inline(language.legacy_files(collection, content))
}

/// Bake the dependency lock of a Validated response into the sentinel
/// files of a built image connector configuration, and remove it from the
/// generated files: a lock is part of the built task, and is never written
/// among the user's files. A lock which the user's `files` list is theirs,
/// and is never replaced. It's left among the generated files, where it's
/// the starter of a listed lock which doesn't exist yet
/// (`retain_generated_files`).
pub fn bake_generated_files(
    config_json: &bytes::Bytes,
    language: Language,
    project_root: &str,
    generated_files: &mut BTreeMap<String, String>,
) -> bytes::Bytes {
    let lock_file = language.lock();
    let lock_url = format!("{project_root}/{lock_file}");
    if !generated_files.contains_key(&lock_url) {
        return config_json.clone();
    }
    let mut config: serde_json::Value =
        serde_json::from_slice(config_json).expect("built config is JSON");

    // `config_json` is a serialized image ConnectorConfig.
    let Some(files) = config
        .pointer_mut(&format!("/config/{}/files", language.sentinel()))
        .and_then(serde_json::Value::as_object_mut)
    else {
        return config_json.clone();
    };
    if files.contains_key(lock_file) {
        return config_json.clone();
    }
    let lock = generated_files.remove(&lock_url).unwrap();
    files.insert(lock_file.to_string(), serde_json::Value::String(lock));

    serde_json::to_vec(&config).unwrap().into()
}

/// Retain only the generated files which the platform owns: those under
/// `flow_generated/`, and starters of files which the task lists but which
/// don't exist yet. Any other generated file would overwrite a user's file,
/// and is dropped. Note the frozen `dev` images generate a manifest and editor
/// configuration, which is now removed because a `files` sibling in the same
/// directory could list it as its own.
pub fn retain_generated_files(
    collection: &models::Collection,
    project_root: &str,
    files: &models::custom_connector::Files,
    generated_files: &mut BTreeMap<String, String>,
) {
    // Compare parsed URLs, which resolve `..` segments.
    let parse = |url: &str| url::Url::parse(url).map(String::from).unwrap_or_default();

    let owned_dir = parse(&format!("{project_root}/flow_generated/"));
    let starters: Vec<String> = files
        .inline()
        .into_iter()
        .flatten()
        .filter(|(_, content)| content.is_none())
        .map(|(path, _)| parse(&format!("{project_root}/{path}")))
        .collect();

    generated_files.retain(|url, _content| {
        let parsed = parse(url);
        let owned = parsed.starts_with(&owned_dir) || starters.contains(&parsed);
        if !owned {
            tracing::warn!(
                %collection,
                %url,
                "dropping a generated file outside of `flow_generated/`, which isn't the starter of a missing listed file"
            );
        }
        owned
    });
}

#[cfg(test)]
mod test {
    use super::*;

    fn shards(pairs: &[(&str, &str)]) -> models::ShardTemplate {
        models::ShardTemplate {
            flags: pairs
                .iter()
                .map(|(k, v)| (models::Token::new(*k), models::Token::new(*v)))
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn image_tags_and_built_configs() {
        let typescript: models::DeriveUsingTypescript = serde_json::from_value(serde_json::json!({
            "files": {"deno.json": "{}", "derived/mod.ts": "export class Derivation {}"},
            "config": {"apiKey": "secret"},
        }))
        .unwrap();
        let mut python: models::DeriveUsingPython = serde_json::from_value(serde_json::json!({
            "files": {"pyproject.toml": "[project]\n", "derived/__init__.py": "class Derivation: pass\n"},
            "config": {"region": "north"},
        }))
        .unwrap();
        // A listed file which failed to load is `null`.
        if let models::custom_connector::Files::Inline(files) = &mut python.files {
            files.insert("derived/helpers.py".to_string(), None);
        }
        let mut capture: models::CapturePython = serde_json::from_value(serde_json::json!({
            "files": {"source-acme/__init__.py": ""},
            "config": {"credentials": {"client_id": "an-id"}},
            "spec": {
                "configSchema": {
                    "type": "object",
                    "properties": {"credentials": {"type": "object"}},
                },
                "oauth2": {
                    "provider": "acme",
                    "authUrlTemplate": "https://acme.example/authorize",
                    "accessTokenUrlTemplate": "https://acme.example/token",
                },
            },
        }))
        .unwrap();
        // A listed file which failed to load is `null`.
        if let models::custom_connector::Files::Inline(files) = &mut capture.files {
            files.insert("pyproject.toml".to_string(), None);
        }
        let name = models::Capture::new("acmeCo/sources/source-acme");
        let derived = models::Collection::new("acmeCo/derived");

        let v1 = shards(&[]);
        let v2 = shards(&[(models::ENABLE_RUNTIME_V2, "true")]);
        let local = shards(&[
            (models::ENABLE_RUNTIME_V2, "true"),
            (IMAGE_TAG_FLAG, "local"),
        ]);

        insta::assert_debug_snapshot!([
            derive_typescript_connector(&derived, &typescript, &v1),
            derive_typescript_connector(&derived, &typescript, &v2),
            derive_typescript_connector(&derived, &typescript, &local),
            derive_python_connector(&derived, &python, &v1),
            derive_python_connector(&derived, &python, &v2),
            // capture-python has no `dev` image.
            capture_python_connector(
                &name,
                &capture,
                &shards(&[(models::ENABLE_RUNTIME_V2, "false")])
            ),
            capture_python_connector(&name, &capture, &local),
        ]);
    }

    #[test]
    fn legacy_modules_are_built_as_files() {
        let collection = models::Collection::new("acmeCo/nested/orders");

        let python: models::DeriveUsingPython = serde_json::from_value(serde_json::json!({
            "module": "class Derivation(IDerivation):\n    pass\n",
            "dependencies": {},
        }))
        .unwrap();
        let typescript: models::DeriveUsingTypescript = serde_json::from_value(serde_json::json!({
            "module": "export class Derivation extends IDerivation {}\n",
        }))
        .unwrap();

        let v1 = shards(&[]);
        let v2 = shards(&[(models::ENABLE_RUNTIME_V2, "true")]);

        insta::assert_debug_snapshot!([
            derive_python_connector(&collection, &python, &v1),
            derive_python_connector(&collection, &python, &v2),
            derive_typescript_connector(&collection, &typescript, &v1),
            derive_typescript_connector(&collection, &typescript, &v2),
        ]);
    }

    #[test]
    fn generated_lock_is_baked() {
        let root = "file:///project/acmeCo";
        let config: bytes::Bytes =
            r#"{"image":"i","config":{"a":1,"_python":{"task":"p","files":{"pyproject.toml":"x"}}}}"#.into();

        let generated = BTreeMap::from([
            (
                "file:///project/acmeCo/uv.lock".to_string(),
                "lock".to_string(),
            ),
            (
                "file:///project/acmeCo/other.py".to_string(),
                "not baked".to_string(),
            ),
        ]);
        let mut files = generated.clone();
        let baked = bake_generated_files(&config, Language::Python, root, &mut files);

        assert_eq!(
            std::str::from_utf8(&baked).unwrap(),
            r#"{"config":{"_python":{"files":{"pyproject.toml":"x","uv.lock":"lock"},"task":"p"},"a":1},"image":"i"}"#
        );
        // The lock is never written into the user's project.
        assert_eq!(
            files.keys().collect::<Vec<_>>(),
            ["file:///project/acmeCo/other.py"]
        );

        // A lock which the user lists is never replaced, and is left among
        // the generated files. It's the starter of a listed lock which doesn't
        // exist yet (`null`).
        for listed in [r#""mine""#, "null"] {
            let config: bytes::Bytes = format!(
                r#"{{"image":"i","config":{{"_python":{{"task":"p","files":{{"uv.lock":{listed}}}}}}}}}"#
            )
            .into();
            let mut files = generated.clone();
            let baked = bake_generated_files(&config, Language::Python, root, &mut files);
            assert_eq!(baked, config);
            assert_eq!(files, generated);
        }
        // A TypeScript project's lock is baked into its own sentinel.
        let config: bytes::Bytes =
            r#"{"image":"i","config":{"_typescript":{"task":"c","files":{"deno.json":"x"}}}}"#
                .into();
        let mut files = BTreeMap::from([(
            "file:///project/acmeCo/deno.lock".to_string(),
            "lock".to_string(),
        )]);
        let baked = bake_generated_files(&config, Language::Typescript, root, &mut files);
        assert_eq!(
            std::str::from_utf8(&baked).unwrap(),
            r#"{"config":{"_typescript":{"files":{"deno.json":"x","deno.lock":"lock"},"task":"c"}},"image":"i"}"#
        );
        assert!(files.is_empty());
    }

    #[test]
    fn generated_files_are_retained_if_owned() {
        let root = "file:///project/acmeCo";
        let mut files: models::custom_connector::Files =
            serde_json::from_value(serde_json::json!({
                "deno.json": "{}",
                "orders/mod.ts": "export class Derivation {}",
            }))
            .unwrap();
        // Listed files which failed to load are `null`.
        if let models::custom_connector::Files::Inline(files) = &mut files {
            files.insert("orders/helpers.ts".to_string(), None);
            files.insert("deno.lock".to_string(), None);
        }
        let mut generated: BTreeMap<String, String> = [
            "flow_generated/typescript/acmeCo/orders.ts",
            "flow_generated/typescript/estuary.ts",
            "orders/helpers.ts",
            "deno.lock",
            // A listed file which exists is the user's.
            "deno.json",
            "orders/mod.ts",
            // Neither owned nor listed.
            "pyrightconfig.json",
            "flow_generated/../deno.json",
        ]
        .into_iter()
        .map(|path| (format!("{root}/{path}"), String::new()))
        .collect();

        retain_generated_files(
            &models::Collection::new("acmeCo/orders"),
            root,
            &files,
            &mut generated,
        );
        insta::assert_debug_snapshot!(generated.keys().collect::<Vec<_>>(), @r#"
        [
            "file:///project/acmeCo/deno.lock",
            "file:///project/acmeCo/flow_generated/typescript/acmeCo/orders.ts",
            "file:///project/acmeCo/flow_generated/typescript/estuary.ts",
            "file:///project/acmeCo/orders/helpers.ts",
        ]
        "#);

        // A legacy model lists no files, and keeps only `flow_generated/`.
        let mut legacy: BTreeMap<String, String> = [
            ("flow_generated/python/acmeCo/orders/__init__.py", ""),
            ("pyproject.toml", "master-era manifest"),
        ]
        .into_iter()
        .map(|(path, content)| (format!("{root}/{path}"), content.to_string()))
        .collect();

        retain_generated_files(
            &models::Collection::new("acmeCo/orders"),
            root,
            &models::custom_connector::Files::default(),
            &mut legacy,
        );
        assert_eq!(
            legacy.keys().collect::<Vec<_>>(),
            ["file:///project/acmeCo/flow_generated/python/acmeCo/orders/__init__.py"]
        );
    }
}

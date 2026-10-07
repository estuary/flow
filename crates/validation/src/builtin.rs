//! Built-in connectors: user-authored Python captures, and Python and
//! TypeScript derivations. Each is rewritten at build time into an ordinary
//! image connector, whose configuration is the user's `config` with a pushed-down
//! sentinel property carrying the user's code and its declared `spec`.
//!
//! A built-in connector answers Spec from the sentinel's `spec`, which has its
//! defaults applied here, and never runs user code to do so.

use super::{Error, Scope};
use proto_flow::{capture, flow};
use std::collections::BTreeMap;

/// Shard flag which overrides the image tag of every built-in connector,
/// such as `local` for a locally-built image.
pub const IMAGE_TAG_FLAG: &str = "builtin-image-tag";
/// Sentinel property of a Python connector's configuration.
pub const PYTHON_SENTINEL: &str = "_python";
/// Sentinel property of a TypeScript connector's configuration.
pub const TYPESCRIPT_SENTINEL: &str = "_typescript";
/// Lock of a Python project. A Validated lock is baked into the built
/// specification, unless the user's `files` list one of their own.
pub const LOCK_FILE: &str = "uv.lock";

pub const CAPTURE_PYTHON_IMAGE: &str = "ghcr.io/estuary/capture-python";
pub const DERIVE_PYTHON_IMAGE: &str = "ghcr.io/estuary/derive-python";
pub const DERIVE_TYPESCRIPT_IMAGE: &str = "ghcr.io/estuary/derive-typescript";

/// Image tag of a built-in connector: an explicit `builtin-image-tag` flag, or
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
/// The `capture` names the module of its generated types.
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
            PYTHON_SENTINEL,
            serde_json::json!({
                "capture": capture,
                "package": python_package(capture),
                "files": inline_files(files),
                "spec": resolve_spec(spec, capture_resource_config_schema),
            }),
        ),
    }
}

/// Resolve a Python derivation into its image connector.
/// The derived `collection` names the module of its generated types.
pub fn derive_python_connector(
    collection: &models::Collection,
    python: &models::DeriveUsingPython,
    shards: &models::ShardTemplate,
) -> models::ConnectorConfig {
    let models::DeriveUsingPython {
        module,
        files,
        config,
        spec,
        dependencies: _,
    } = python;
    let tag = image_tag(shards, models::CatalogType::Collection);

    // The frozen `dev` image understands only its original configuration.
    let config = if tag == "dev" {
        models::RawValue::from_value(&serde_json::json!({"module": module}))
    } else {
        with_sentinel(
            config,
            PYTHON_SENTINEL,
            serde_json::json!({
                "collection": collection,
                "module": module,
                "files": inline_files(files),
                "spec": resolve_spec(spec, derive_resource_config_schema),
            }),
        )
    };
    models::ConnectorConfig {
        image: format!("{DERIVE_PYTHON_IMAGE}:{tag}"),
        config,
    }
}

/// Resolve a TypeScript derivation into its image connector.
pub fn derive_typescript_connector(
    collection: &models::Collection,
    typescript: &models::DeriveUsingTypescript,
    shards: &models::ShardTemplate,
) -> models::ConnectorConfig {
    let models::DeriveUsingTypescript {
        module,
        config,
        spec,
    } = typescript;
    let tag = image_tag(shards, models::CatalogType::Collection);

    let config = if tag == "dev" {
        models::RawValue::from_value(&serde_json::json!({"module": module}))
    } else {
        with_sentinel(
            config,
            TYPESCRIPT_SENTINEL,
            serde_json::json!({
                "collection": collection,
                "module": module,
                "spec": resolve_spec(spec, derive_resource_config_schema),
            }),
        )
    };
    models::ConnectorConfig {
        image: format!("{DERIVE_TYPESCRIPT_IMAGE}:{tag}"),
        config,
    }
}

/// Resolve the `spec` of a built-in connector into the `spec` of its sentinel,
/// which mirrors the connector's Spec response and has its defaults applied.
fn resolve_spec(
    spec: &models::BuiltinSpec,
    default_resource_config_schema: fn() -> serde_json::Value,
) -> serde_json::Value {
    let models::BuiltinSpec {
        config_schema,
        resource_config_schema,
        oauth2,
    } = spec;

    let mut resolved = serde_json::json!({
        "configSchema": config_schema
            .as_ref()
            .map(|schema| schema.to_value())
            .unwrap_or_else(|| serde_json::json!({})),
        "resourceConfigSchema": resource_config_schema
            .as_ref()
            .map(|schema| schema.to_value())
            .unwrap_or_else(default_resource_config_schema),
    });
    if let Some(oauth2) = oauth2 {
        resolved["oauth2"] = serde_json::to_value(oauth2).unwrap();
    }
    resolved
}

/// Default resource configuration schema of a Python capture: the CDK's stock
/// `ResourceConfig`, with its `name` annotated as the binding's resource path.
pub fn capture_resource_config_schema() -> serde_json::Value {
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

/// Default resource configuration schema of a built-in derivation, which is
/// the `lambda` of each of its transforms.
pub fn derive_resource_config_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "readOnly": {
                "type": "boolean",
                "default": false,
                "description": "Does this transform never publish documents?",
            },
        },
    })
}

/// Validate the `spec` of a built-in connector. Each declared schema must
/// compile, else every start of the task would fail when its connector
/// configuration is examined for IAM authentication.
///
/// `config` isn't validated against `configSchema`: secrets aren't merged
/// into it until the task is started, and the connector's own types (and not
/// its declared schema) are what decide whether a configuration is usable.
pub fn walk_spec(scope: Scope, spec: &models::BuiltinSpec, errors: &mut tables::Errors) {
    let models::BuiltinSpec {
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

/// Python package of a capture: the final component of its name, sanitized
/// into a Python identifier (`acmeCo/source-acme` => `source_acme`).
pub fn python_package(capture: &models::Capture) -> String {
    let base = capture.rsplit('/').next().unwrap();

    let mut package: String = base
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();

    if package.starts_with(|c: char| c.is_ascii_digit()) {
        package.insert(0, '_');
    }
    package
}

/// Root of a user-authored project: the directory of the specification
/// which lists its `files`, and relative to which they're resolved.
pub fn project_root(scope: &url::Url) -> url::Url {
    let mut root = scope.clone();
    root.set_fragment(None);
    root.join("./").unwrap_or(root)
}

/// Compose the built configuration of a built-in connector: the user's
/// `config` object, extended with the `sentinel` property.
fn with_sentinel(
    config: &models::RawValue,
    sentinel: &str,
    value: serde_json::Value,
) -> models::RawValue {
    let mut config = match serde_json::from_str::<serde_json::Value>(config.get()) {
        Ok(serde_json::Value::Object(config)) => config,
        _ => serde_json::Map::new(), // Reported by `walk_config`.
    };
    config.insert(sentinel.to_string(), value);

    models::RawValue::from_value(&serde_json::Value::Object(config))
}

fn inline_files(files: &models::ProjectFiles) -> BTreeMap<&str, &str> {
    files
        .inline()
        .into_iter()
        .flatten()
        .map(|(path, content)| (path.as_str(), content.as_str()))
        .collect()
}

/// Validate the user `config` of a built-in connector.
pub fn walk_config(
    scope: Scope,
    config: &models::RawValue,
    sentinel: &str,
    errors: &mut tables::Errors,
) {
    match serde_json::from_str::<serde_json::Value>(config.get()) {
        Ok(serde_json::Value::Object(config)) => {
            if config.contains_key(sentinel) {
                Error::ConfigReservedProperty {
                    property: sentinel.to_string(),
                }
                .push(scope, errors);
            }
            // The build injects the sentinel into the configuration,
            // which would invalidate the MAC of a `sops` document.
            if config.contains_key("sops") {
                Error::BuiltinConfigSops {}.push(scope, errors);
            }
        }
        // A config which failed to load is left as its URL,
        // and its load error has already been reported.
        Ok(serde_json::Value::String(_)) => {}
        _ => Error::ConfigNotObject {}.push(scope, errors),
    }
}

/// Validate the `files` of a user-authored project.
pub fn walk_files(
    scope: Scope,
    files: &models::ProjectFiles,
    is_reserved: impl Fn(&str) -> bool,
    errors: &mut tables::Errors,
) {
    for (index, path) in files.paths().enumerate() {
        let scope = match files {
            models::ProjectFiles::Indirect(_) => scope.push_item(index),
            models::ProjectFiles::Inline(_) => scope.push_prop(path),
        };
        super::indexed::walk_name(scope, "project file", path, models::Name::regex(), errors);

        if path
            .split('/')
            .any(|segment| segment == "." || segment == "..")
        {
            Error::ProjectFileDotSegment {
                path: path.to_string(),
            }
            .push(scope, errors);
        }
        if is_reserved(path) {
            Error::ProjectFileReserved {
                path: path.to_string(),
            }
            .push(scope, errors);
        }
    }
}

/// Paths reserved within the `files` of every Python project: its virtual
/// environment, and the modules generated from its declared spec.
/// A listed `uv.lock` is allowed: it's the user's own pin of the project's
/// dependencies, which is used verbatim.
pub fn is_reserved_python_path(path: &str) -> bool {
    matches!(path, ".venv" | "flow_generated")
        || path.starts_with(".venv/")
        || path.starts_with("flow_generated/")
}

/// Paths reserved within the `files` of a Python derivation.
pub fn is_reserved_derive_python_path(path: &str) -> bool {
    is_reserved_python_path(path) || matches!(path, "main.py" | "module.py" | "module/__init__.py")
}

/// Files which the `files` of a Python capture must include.
pub fn required_capture_python_files(package: &str) -> [String; 3] {
    [
        "pyproject.toml".to_string(),
        format!("{package}/__init__.py"),
        format!("{package}/__main__.py"),
    ]
}

/// Bake generated files of a Validated response which the platform owns
/// (the dependency lock) into the `sentinel` files of a built image
/// connector configuration. A lock which the user's `files` list is theirs,
/// and is never replaced.
pub fn bake_generated_files(
    config_json: &bytes::Bytes,
    sentinel: &str,
    project_root: &url::Url,
    generated_files: &BTreeMap<String, String>,
) -> bytes::Bytes {
    let Ok(lock_url) = project_root.join(LOCK_FILE) else {
        return config_json.clone();
    };
    let Some(lock) = generated_files.get(lock_url.as_str()) else {
        return config_json.clone();
    };
    let mut config: serde_json::Value =
        serde_json::from_slice(config_json).expect("built config is JSON");

    // `config_json` is a serialized image ConnectorConfig.
    let Some(files) = config
        .pointer_mut(&format!("/config/{sentinel}/files"))
        .and_then(serde_json::Value::as_object_mut)
    else {
        return config_json.clone();
    };
    if files.contains_key(LOCK_FILE) {
        return config_json.clone();
    }
    files.insert(
        LOCK_FILE.to_string(),
        serde_json::Value::String(lock.clone()),
    );

    serde_json::to_vec(&config).unwrap().into()
}

/// Python projects of the draft: the scope of each task, and its `files`.
fn python_projects(
    draft: &tables::DraftCatalog,
) -> impl Iterator<Item = (&url::Url, &models::ProjectFiles)> {
    let captures = draft.captures.iter().filter_map(|row| {
        let models::CaptureEndpoint::Python(python) = &row.model.as_ref()?.endpoint else {
            return None;
        };
        Some((&row.scope, &python.files))
    });
    let derivations = draft.collections.iter().filter_map(|row| {
        let models::DeriveUsing::Python(python) = &row.model.as_ref()?.derive.as_ref()?.using
        else {
            return None;
        };
        Some((&row.scope, &python.files))
    });
    captures.chain(derivations)
}

/// Walk the `files` of all Python projects of the draft, requiring that files
/// which resolve to a common URL, as sibling tasks sharing a `pyproject.toml`
/// do, also have identical content. Tasks which share a project root also
/// share its `uv.lock`, so either all of them list it, or none do.
pub fn walk_shared_files(draft: &tables::DraftCatalog, errors: &mut tables::Errors) {
    let mut seen: BTreeMap<url::Url, (&str, &url::Url)> = BTreeMap::new();
    // Project roots, and a task scope which does and doesn't list its lock.
    let mut locks: BTreeMap<url::Url, (Option<&url::Url>, Option<&url::Url>)> = BTreeMap::new();

    for (scope, files) in python_projects(draft) {
        let Some(files) = files.inline() else {
            continue;
        };
        let root = project_root(scope);

        let entry = locks.entry(root.clone()).or_default();
        if files.contains_key(LOCK_FILE) {
            entry.0.get_or_insert(scope);
        } else {
            entry.1.get_or_insert(scope);
        }

        for (path, content) in files {
            let Ok(resource) = root.join(path) else {
                continue;
            };
            match seen.get(&resource) {
                Some((other, _)) if *other == content.as_str() => {}
                Some((_, other_scope)) => Error::ProjectFileConflict {
                    resource: resource.clone(),
                    other: (*other_scope).clone(),
                }
                .push(Scope::new(scope), errors),
                None => {
                    seen.insert(resource, (content.as_str(), scope));
                }
            }
        }
    }

    for (root, scopes) in locks {
        let (Some(with), Some(without)) = scopes else {
            continue;
        };
        Error::ProjectLockMismatch {
            root,
            with: with.clone(),
        }
        .push(Scope::new(without), errors);
    }
}

/// URLs of the `uv.lock` files which Python projects of the draft list.
/// A generated lock is never written at such a URL: the listed lock is the
/// user's own.
pub fn listed_locks(draft: &tables::DraftCatalog) -> std::collections::BTreeSet<String> {
    python_projects(draft)
        .filter(|(_, files)| files.paths().any(|path| path == LOCK_FILE))
        .filter_map(|(scope, _)| project_root(scope).join(LOCK_FILE).ok())
        .map(|url| url.to_string())
        .collect()
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
        let typescript = models::DeriveUsingTypescript {
            module: models::RawValue::from_str("\"mod.ts\"").unwrap(),
            config: models::RawValue::from_str(r#"{"apiKey":"secret"}"#).unwrap(),
            spec: Default::default(),
        };
        let python: models::DeriveUsingPython = serde_json::from_value(serde_json::json!({
            "module": "class Derivation: pass\n",
            "files": {"pyproject.toml": "[project]\n"},
            "config": {"region": "north"},
        }))
        .unwrap();
        let capture: models::CapturePython = serde_json::from_value(serde_json::json!({
            "files": {"source_acme/__init__.py": ""},
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
    fn python_packages() {
        let packages: Vec<_> = [
            "acmeCo/source-acme",
            "acmeCo/nested/Widgets.v2",
            "acmeCo/9lives",
        ]
        .into_iter()
        .map(|name| python_package(&models::Capture::new(name)))
        .collect();
        assert_eq!(packages, ["source_acme", "Widgets_v2", "_9lives"]);
    }

    #[test]
    fn generated_lock_is_baked() {
        let root = url::Url::parse("file:///project/acmeCo/").unwrap();
        let config: bytes::Bytes =
            r#"{"image":"i","config":{"a":1,"_python":{"package":"p","files":{"pyproject.toml":"x"}}}}"#.into();

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
        let baked = bake_generated_files(&config, PYTHON_SENTINEL, &root, &generated);

        assert_eq!(
            std::str::from_utf8(&baked).unwrap(),
            r#"{"config":{"_python":{"files":{"pyproject.toml":"x","uv.lock":"lock"},"package":"p"},"a":1},"image":"i"}"#
        );

        // A lock which the user lists is never replaced.
        let config: bytes::Bytes =
            r#"{"image":"i","config":{"_python":{"package":"p","files":{"uv.lock":"mine"}}}}"#
                .into();
        let baked = bake_generated_files(&config, PYTHON_SENTINEL, &root, &generated);
        assert_eq!(baked, config);
    }
}

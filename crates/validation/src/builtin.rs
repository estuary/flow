//! Built-in connectors: user-authored Python captures, and Python and
//! TypeScript derivations. Each is rewritten at build time into an ordinary
//! image connector, whose configuration is the user's `config` with a pushed-down
//! sentinel property carrying the task's name, its project `files`, and its
//! declared `spec`.
//!
//! A project is its listed `files`. Each task has one directory of the project,
//! named verbatim for the final component of the task's name, which holds its
//! entry file. A built-in connector answers Spec from the sentinel's `spec`
//! (resolving a schema it omits to the connector's own default), and never
//! runs user code to do so.

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

/// Language of a built-in connector's project.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Language {
    Python,
    Typescript,
}

impl Language {
    /// Manifest of a project, at its root.
    pub fn manifest(self) -> &'static str {
        match self {
            Self::Python => "pyproject.toml",
            Self::Typescript => "deno.json",
        }
    }

    /// Entry file of a task, within its directory `dir`.
    pub fn entry(self, dir: &str) -> String {
        match self {
            Self::Python => format!("{dir}/__init__.py"),
            Self::Typescript => format!("{dir}/mod.ts"),
        }
    }

    /// Is `path` reserved within a project? Generated files are written
    /// under `flow_generated/`, and Python's virtual environment is `.venv/`.
    pub fn is_reserved(self, path: &str) -> bool {
        let reserved: &[&str] = match self {
            Self::Python => &[".venv", "flow_generated"],
            Self::Typescript => &["flow_generated"],
        };
        reserved
            .iter()
            .any(|r| path == *r || path.strip_prefix(r).is_some_and(|p| p.starts_with('/')))
    }
}

/// Directory of a built-in task within its project: the final component of
/// its name, verbatim (`acmeCo/source-acme` has directory `source-acme`).
pub fn task_dir(name: &str) -> &str {
    name.rsplit('/').next().unwrap()
}

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
            PYTHON_SENTINEL,
            serde_json::json!({
                "capture": capture,
                "files": sentinel_files(files),
                "spec": spec,
            }),
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
        module: _,
        dependencies: _,
    } = python;

    derive_connector(
        DERIVE_PYTHON_IMAGE,
        Language::Python,
        PYTHON_SENTINEL,
        collection,
        files,
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
        module: _,
    } = typescript;

    derive_connector(
        DERIVE_TYPESCRIPT_IMAGE,
        Language::Typescript,
        TYPESCRIPT_SENTINEL,
        collection,
        files,
        config,
        spec,
        shards,
    )
}

fn derive_connector(
    image: &str,
    language: Language,
    sentinel: &str,
    collection: &models::Collection,
    files: &models::ProjectFiles,
    config: &models::RawValue,
    spec: &models::BuiltinSpec,
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
            sentinel,
            serde_json::json!({
                "collection": collection,
                "files": sentinel_files(files),
                "spec": spec,
            }),
        )
    };
    models::ConnectorConfig {
        image: format!("{image}:{tag}"),
        config,
    }
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

/// Files of a sentinel. A listed file which failed to load is `null`,
/// for which the connector returns starter content.
fn sentinel_files(files: &models::ProjectFiles) -> BTreeMap<&str, Option<&str>> {
    files
        .inline()
        .into_iter()
        .flatten()
        .map(|(path, content)| (path.as_str(), content.as_deref()))
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

/// Validate the `files` of the project of a built-in task `name`. Each path
/// must be well-formed and not reserved, and its manifest and entry file must
/// be listed: their absence is how a new project is bootstrapped, by an error
/// which names the files to list.
pub fn walk_files(
    scope: Scope,
    language: Language,
    name: &str,
    files: &models::ProjectFiles,
    errors: &mut tables::Errors,
) {
    for (index, path) in files.paths().enumerate() {
        let scope = match files {
            models::ProjectFiles::Indirect(_) => scope.push_item(index),
            models::ProjectFiles::Inline(_) => scope.push_prop(path),
        };
        if !models::ProjectFiles::is_valid_path(path) {
            Error::ProjectFilePath {
                path: path.to_string(),
            }
            .push(scope, errors);
        }
        if language.is_reserved(path) {
            Error::ProjectFileReserved {
                path: path.to_string(),
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
        Error::ProjectFilesMissing {
            paths: missing.join(" and "),
        }
        .push(scope, errors);
    }
}

/// Migrate a derivation model having a legacy `module` into the form of
/// `files`, returning a description of the fix. Its module becomes the
/// derivation's entry file, and a project which doesn't list its manifest is
/// given the manifest which legacy derivations ran with.
pub fn migrate_legacy_module(
    collection: &models::Collection,
    using: &mut models::DeriveUsing,
) -> Option<String> {
    let (language, module, files, manifest) = match using {
        models::DeriveUsing::Python(python) => {
            let module = python.module.take()?;
            let manifest = legacy_pyproject(collection, &python.dependencies);
            python.dependencies.clear();
            (Language::Python, module, &mut python.files, manifest)
        }
        models::DeriveUsing::Typescript(typescript) => {
            let module = typescript.module.take()?;
            (
                Language::Typescript,
                module,
                &mut typescript.files,
                LEGACY_DENO_JSON.to_string(),
            )
        }
        _ => return None,
    };
    let entry = language.entry(task_dir(collection));

    // A module which failed to load is its unresolved URL, and its load error
    // has already been reported. It's migrated verbatim, as legacy connectors
    // received it: a migrated model may be stored, and must never have a `null`.
    let content = match serde_json::from_str::<String>(module.get()) {
        Ok(content) => content,
        Err(_) => module.get().to_string(),
    };

    let mut inline = match std::mem::take(files) {
        models::ProjectFiles::Inline(inline) => inline,
        // Validated models have inline files, but they're not required to.
        models::ProjectFiles::Indirect(paths) => paths.into_iter().map(|p| (p, None)).collect(),
    };

    let mut fix = format!("migrated `module` into `files` as {entry}");
    inline.insert(entry, Some(content));

    if !inline.contains_key(language.manifest()) {
        fix.push_str(&format!(", with a default {}", language.manifest()));
        inline.insert(language.manifest().to_string(), Some(manifest));
    }
    *files = models::ProjectFiles::Inline(inline);

    Some(fix)
}

/// `pyproject.toml` of a migrated legacy Python derivation. It's frozen to
/// reproduce what legacy derivations ran with, and isn't a starter project.
fn legacy_pyproject(
    collection: &models::Collection,
    dependencies: &BTreeMap<String, String>,
) -> String {
    let mut dependencies = dependencies.clone();
    dependencies
        .entry("pydantic".to_string())
        .or_insert_with(|| ">=2".to_string());

    let dependencies: String = dependencies
        .iter()
        .map(|(package, version)| format!("    \"{package}{version}\",\n"))
        .collect();

    LEGACY_PYPROJECT
        .replace("NAME", &collection.replace('/', "-"))
        .replace("DEPENDENCIES", &dependencies)
}

const LEGACY_PYPROJECT: &str = r#"[project]
name = "NAME"
version = "0.1.0"
requires-python = ">=3.14,<4"
dependencies = [
DEPENDENCIES]

[dependency-groups]
dev = ["pyright>=1.1"]

[tool.uv]
package = false
exclude-newer = "7 days"

[tool.pyright]
typeCheckingMode = "strict"
extraPaths = ["flow_generated/python"]
"#;

/// `deno.json` of a migrated legacy TypeScript derivation, which maps the
/// `flow/` imports of legacy modules to their generated types.
const LEGACY_DENO_JSON: &str = r#"{
  "imports": {
    "flow/": "./flow_generated/typescript/"
  }
}
"#;

/// Bake the dependency lock of a Validated response into the `sentinel`
/// files of a built image connector configuration, and remove it from the
/// generated files: a lock is part of the built task, and is never written
/// into the user's project. A lock which the user's `files` list is theirs,
/// and is never replaced.
pub fn bake_generated_files(
    config_json: &bytes::Bytes,
    sentinel: &str,
    project_root: &url::Url,
    generated_files: &mut BTreeMap<String, String>,
) -> bytes::Bytes {
    let Ok(lock_url) = project_root.join(LOCK_FILE) else {
        return config_json.clone();
    };
    let Some(lock) = generated_files.remove(lock_url.as_str()) else {
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
    files.insert(LOCK_FILE.to_string(), serde_json::Value::String(lock));

    serde_json::to_vec(&config).unwrap().into()
}

/// Built-in tasks of the draft which have a directory of their project:
/// the scope and name of each. A legacy derivation having a `module` has no
/// directory of its own until it's migrated, which happens only as it's built.
fn builtin_tasks(draft: &tables::DraftCatalog) -> impl Iterator<Item = (&url::Url, &str)> {
    let captures = draft.captures.iter().filter_map(|row| {
        let models::CaptureEndpoint::Python(_) = &row.model.as_ref()?.endpoint else {
            return None;
        };
        Some((&row.scope, row.capture.as_str()))
    });
    let derivations = draft.collections.iter().filter_map(|row| {
        let is_legacy = match &row.model.as_ref()?.derive.as_ref()?.using {
            models::DeriveUsing::Python(python) => python.module.is_some(),
            models::DeriveUsing::Typescript(typescript) => typescript.module.is_some(),
            _ => return None,
        };
        (!is_legacy).then(|| (&row.scope, row.collection.as_str()))
    });
    captures.chain(derivations)
}

/// Require that built-in tasks of the draft which share a project root have
/// distinct directories. Tasks of the control plane each have their own root,
/// so only those of local specifications may collide.
pub fn walk_task_dirs(draft: &tables::DraftCatalog, errors: &mut tables::Errors) {
    let mut seen: BTreeMap<(url::Url, &str), &str> = BTreeMap::new();

    for (scope, name) in builtin_tasks(draft) {
        let key = (project_root(scope), task_dir(name));

        if let Some(other) = seen.get(&key) {
            Error::ProjectDirCollision {
                dir: key.1.to_string(),
                other: other.to_string(),
            }
            .push(Scope::new(scope), errors);
        } else {
            seen.insert(key, name);
        }
    }
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
        let python: models::DeriveUsingPython = serde_json::from_value(serde_json::json!({
            "files": {"pyproject.toml": "[project]\n", "derived/__init__.py": "class Derivation: pass\n"},
            "config": {"region": "north"},
        }))
        .unwrap();
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
        if let models::ProjectFiles::Inline(files) = &mut capture.files {
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
    fn task_dirs_and_paths() {
        let cases: Vec<_> = ["acmeCo/source-acme", "acmeCo/nested/2024-orders", "single"]
            .into_iter()
            .map(|name| {
                (
                    task_dir(name),
                    Language::Python.entry(task_dir(name)),
                    Language::Typescript.entry(task_dir(name)),
                )
            })
            .collect();

        insta::assert_debug_snapshot!(cases, @r#"
        [
            (
                "source-acme",
                "source-acme/__init__.py",
                "source-acme/mod.ts",
            ),
            (
                "2024-orders",
                "2024-orders/__init__.py",
                "2024-orders/mod.ts",
            ),
            (
                "single",
                "single/__init__.py",
                "single/mod.ts",
            ),
        ]
        "#);

        let reserved: Vec<_> = [
            ".venv",
            ".venv/lib",
            ".venvy",
            "flow_generated/x",
            "a/.venv",
        ]
        .into_iter()
        .map(|path| {
            (
                path,
                Language::Python.is_reserved(path),
                Language::Typescript.is_reserved(path),
            )
        })
        .collect();
        assert_eq!(
            reserved,
            [
                (".venv", true, false),
                (".venv/lib", true, false),
                (".venvy", false, false),
                ("flow_generated/x", true, true),
                ("a/.venv", false, false),
            ]
        );
    }

    #[test]
    fn legacy_modules_are_migrated() {
        let collection = models::Collection::new("acmeCo/nested/orders");

        let mut python = models::DeriveUsing::Python(
            serde_json::from_value(serde_json::json!({
                "module": "class Derivation(IDerivation):\n    pass\n",
                "dependencies": {"httpx": ">=0.27"},
            }))
            .unwrap(),
        );
        let mut typescript = models::DeriveUsing::Typescript(
            serde_json::from_value(serde_json::json!({
                "module": "export class Derivation extends IDerivation {}\n",
            }))
            .unwrap(),
        );
        // A listed manifest is kept, and an unresolved module is migrated as-is.
        let mut listed = models::DeriveUsing::Python(
            serde_json::from_value(serde_json::json!({
                "module": "file:///project/orders.py",
                "files": {"pyproject.toml": "[project]\n"},
            }))
            .unwrap(),
        );
        let mut current = models::DeriveUsing::Python(
            serde_json::from_value(serde_json::json!({
                "files": {"pyproject.toml": "[project]\n"},
            }))
            .unwrap(),
        );

        let fixes = [
            migrate_legacy_module(&collection, &mut python),
            migrate_legacy_module(&collection, &mut typescript),
            migrate_legacy_module(&collection, &mut listed),
            migrate_legacy_module(&collection, &mut current),
        ];
        insta::assert_debug_snapshot!((fixes, python, typescript, listed, current));
    }

    #[test]
    fn generated_lock_is_baked() {
        let root = url::Url::parse("file:///project/acmeCo/").unwrap();
        let config: bytes::Bytes =
            r#"{"image":"i","config":{"a":1,"_python":{"capture":"p","files":{"pyproject.toml":"x"}}}}"#.into();

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
        let baked = bake_generated_files(&config, PYTHON_SENTINEL, &root, &mut files);

        assert_eq!(
            std::str::from_utf8(&baked).unwrap(),
            r#"{"config":{"_python":{"capture":"p","files":{"pyproject.toml":"x","uv.lock":"lock"}},"a":1},"image":"i"}"#
        );
        // The lock is never written into the user's project.
        assert_eq!(
            files.keys().collect::<Vec<_>>(),
            ["file:///project/acmeCo/other.py"]
        );

        // A lock which the user lists is never replaced.
        let config: bytes::Bytes =
            r#"{"image":"i","config":{"_python":{"capture":"p","files":{"uv.lock":"mine"}}}}"#
                .into();
        let mut files = generated.clone();
        let baked = bake_generated_files(&config, PYTHON_SENTINEL, &root, &mut files);
        assert_eq!(baked, config);
        assert_eq!(files.len(), 1);
    }
}

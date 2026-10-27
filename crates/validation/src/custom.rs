//! User-authored custom connectors, in Python and TypeScript.
//! Each is rewritten at build time into an ordinary image connector. Its
//! configuration is a `ProjectConfig` of its language's crate (such as
//! `python_connector::ProjectConfig`): the user's `config`, with a
//! pushed-down sentinel property carrying the task's `files` and its declared
//! `spec`. A derivation model having a legacy `module` is instead built as a
//! `ModuleConfig`, which images that predate `ProjectConfig` also read, and
//! is stored as authored.
//!
//! A task's `files` are relative to its specification's directory, which is
//! Validate's `project_root`. Each task has one directory there, named
//! verbatim for the final component of the task's name, which holds its
//! entry file. A custom connector answers Spec from the declared `spec`
//! (resolving a schema it omits to the connector's own default), and never
//! runs user code to do so.

use super::{Error, Scope};
use models::custom_connector::task_dir;
use std::collections::BTreeMap;

/// Shard flag which overrides the image tag of every custom connector,
/// such as `local` for a locally-built image.
pub const IMAGE_TAG_FLAG: &str = "custom-image-tag";

pub const DERIVE_PYTHON_IMAGE: &str = "ghcr.io/estuary/derive-python";
pub const DERIVE_TYPESCRIPT_IMAGE: &str = "ghcr.io/estuary/derive-typescript";

/// Image tag of a custom connector: an explicit `custom-image-tag` flag, or
/// else `stable`. Derivations of the V1 runtime instead use the frozen `dev`
/// images, which predate `files` and `config`.
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

/// Resolve a Python derivation into its image connector.
pub fn derive_python_connector(
    python: &models::DeriveUsingPython,
    shards: &models::ShardTemplate,
) -> models::ConnectorConfig {
    let models::DeriveUsingPython {
        files,
        config,
        spec,
        module,
        dependencies,
        environment: _,
    } = python;

    // A legacy model passes its `module` through as it always has. The frozen
    // `dev` images read only this configuration. A `stable` image reads it
    // whether or not it predates `ProjectConfig`.
    let config = match module {
        Some(module) => models::RawValue::from_string(
            serde_json::to_string(&python_connector::ModuleConfig {
                module: module_content(module),
                dependencies: dependencies.clone(),
            })
            .unwrap(),
        )
        .unwrap(),
        None => project_config(files, config, spec),
    };
    image_connector(
        DERIVE_PYTHON_IMAGE,
        models::CatalogType::Collection,
        config,
        shards,
    )
}

/// Resolve a TypeScript derivation into its image connector, whose
/// configuration is its model.
pub fn derive_typescript_connector(
    typescript: &models::DeriveUsingTypescript,
    shards: &models::ShardTemplate,
) -> models::ConnectorConfig {
    image_connector(
        DERIVE_TYPESCRIPT_IMAGE,
        models::CatalogType::Collection,
        models::RawValue::from_string(serde_json::to_string(typescript).unwrap()).unwrap(),
        shards,
    )
}

fn image_connector(
    image: &str,
    catalog_type: models::CatalogType,
    config: models::RawValue,
    shards: &models::ShardTemplate,
) -> models::ConnectorConfig {
    models::ConnectorConfig {
        image: format!("{image}:{}", image_tag(shards, catalog_type)),
        config,
    }
}

/// Content of a legacy `module`. Loading inlines its content as a string,
/// and a `module` which isn't one (a model error) is passed as its JSON.
fn module_content(module: &models::RawValue) -> String {
    serde_json::from_str(module.get()).unwrap_or_else(|_| module.get().to_string())
}

/// Built `ProjectConfig` of a Python task's `files`, `config`, and `spec`.
fn project_config(
    files: &models::custom_connector::Files,
    config: &models::RawValue,
    spec: &models::custom_connector::Spec,
) -> models::RawValue {
    let config = python_connector::ProjectConfig {
        project: python_connector::Project {
            files: files.inline().cloned().unwrap_or_default(),
            spec: spec.clone(),
        },
        // A non-object is reported by `walk_config`.
        config: serde_json::from_str(config.get()).unwrap_or_default(),
    };
    models::RawValue::from_string(serde_json::to_string(&config).unwrap()).unwrap()
}

/// Validate the `files`, `config`, and `spec` of a custom Python task `name`,
/// whose `python` stanza is at `scope`.
pub fn walk_python(
    scope: Scope,
    name: &str,
    files: &models::custom_connector::Files,
    config: &models::RawValue,
    spec: &models::custom_connector::Spec,
    errors: &mut tables::Errors,
) {
    walk_files(
        scope.push_prop("files"),
        python_connector::MANIFEST,
        python_connector::entry,
        python_connector::RESERVED,
        name,
        files,
        errors,
    );
    walk_config(
        scope.push_prop("config"),
        config,
        python_connector::SENTINEL,
        errors,
    );
    walk_spec(scope.push_prop("spec"), spec, errors);
}

/// Validate the `spec` of a custom connector. Each declared schema must
/// compile, else every start of the task would fail when its connector
/// configuration is examined for IAM authentication.
///
/// `config` isn't validated against `configSchema`: secrets aren't merged
/// into it until the task is started, and the connector's own types (and not
/// its declared schema) are what decide whether a configuration is usable.
fn walk_spec(scope: Scope, spec: &models::custom_connector::Spec, errors: &mut tables::Errors) {
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

/// Validate the user `config` of a custom connector.
fn walk_config(
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

/// Validate the `files` of a custom task `name`, whose language has a
/// project `manifest`, an `entry` file of each task directory, and `reserved`
/// directories. Each path must be well-formed and not reserved, and its
/// manifest and entry file must be listed: their absence is how a new task is
/// bootstrapped, by an error which names the files to list.
fn walk_files(
    scope: Scope,
    manifest: &str,
    entry: fn(&str) -> String,
    reserved: &[&str],
    name: &str,
    files: &models::custom_connector::Files,
    errors: &mut tables::Errors,
) {
    let is_reserved = |path: &str| {
        reserved
            .iter()
            .any(|r| path == *r || path.strip_prefix(r).is_some_and(|p| p.starts_with('/')))
    };

    for (index, path) in files.paths().enumerate() {
        let scope = match files {
            models::custom_connector::Files::Indirect(_) => scope.push_item(index),
            models::custom_connector::Files::Inline(_) => scope.push_prop(path),
        };
        if !models::custom_connector::Files::is_valid_path(path) || is_reserved(path) {
            Error::CustomFilePath {
                path: path.to_string(),
                reserved: reserved
                    .iter()
                    .map(|dir| format!("`{dir}/`"))
                    .collect::<Vec<_>>()
                    .join(" or "),
            }
            .push(scope, errors);
        }
    }

    let missing: Vec<String> = [manifest.to_string(), entry(task_dir(name))]
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

/// Reject a derivation model's `environment`. Require that a model having a
/// legacy `module` is wholly legacy, and that only such a model has
/// `dependencies`. Removed fields still parse, so that a model having one is
/// rejected here rather than failing to parse.
pub fn walk_legacy_fields(scope: Scope, using: &models::DeriveUsing, errors: &mut tables::Errors) {
    let (language, environment) = match using {
        models::DeriveUsing::Python(python) => ("python", &python.environment),
        models::DeriveUsing::Typescript(typescript) => ("typescript", &typescript.environment),
        _ => return,
    };
    let scope = scope.push_prop(language);

    if !environment.is_empty() {
        Error::CustomRemovedField {
            field: "environment",
            replacement: "use `config`, with a `secrets` stanza for sensitive values",
        }
        .push(scope.push_prop("environment"), errors);
    }

    let models::DeriveUsing::Python(models::DeriveUsingPython {
        module,
        files,
        config,
        spec,
        dependencies,
        ..
    }) = using
    else {
        return;
    };

    if module.is_none() {
        if !dependencies.is_empty() {
            Error::CustomRemovedField {
                field: "dependencies",
                replacement: "declare them in a `pyproject.toml` listed in `files`",
            }
            .push(scope.push_prop("dependencies"), errors);
        }
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

/// Retain only the generated files which the platform owns: those under
/// `flow_generated/`, and starters of files which the task lists but which
/// don't exist yet. Any other generated file would overwrite a user's file,
/// and is dropped. Images which predate ProjectConfig generate a manifest and
/// editor configuration of a legacy model, which a `files` sibling in the
/// same directory could list as its own.
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
            tracing::debug!(
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
        let mut python: models::DeriveUsingPython = serde_json::from_value(serde_json::json!({
            "files": {"pyproject.toml": "[project]\n", "derived/__init__.py": "class Derivation: pass\n"},
            "config": {"region": "north"},
        }))
        .unwrap();
        // A listed file which failed to load is `null`.
        if let models::custom_connector::Files::Inline(files) = &mut python.files {
            files.insert("derived/helpers.py".to_string(), None);
        }

        let v2 = shards(&[(models::ENABLE_RUNTIME_V2, "true")]);
        let local = shards(&[
            (models::ENABLE_RUNTIME_V2, "true"),
            (IMAGE_TAG_FLAG, "local"),
        ]);

        insta::assert_debug_snapshot!([
            derive_python_connector(&python, &v2),
            derive_python_connector(&python, &local),
        ]);
    }

    #[test]
    fn legacy_modules_pass_through() {
        let python: models::DeriveUsingPython = serde_json::from_value(serde_json::json!({
            "module": "class Derivation(IDerivation):\n    pass\n",
            "dependencies": {"httpx": ">=0.27"},
        }))
        .unwrap();
        let typescript: models::DeriveUsingTypescript = serde_json::from_value(serde_json::json!({
            "module": "export class Derivation extends IDerivation {}\n",
        }))
        .unwrap();

        let v1 = shards(&[]);
        let v2 = shards(&[(models::ENABLE_RUNTIME_V2, "true")]);

        insta::assert_debug_snapshot!([
            derive_python_connector(&python, &v1),
            derive_python_connector(&python, &v2),
            derive_typescript_connector(&typescript, &v1),
            derive_typescript_connector(&typescript, &v2),
        ]);
    }

    #[test]
    fn generated_files_are_retained_if_owned() {
        let root = "file:///project/acmeCo";
        let mut files: models::custom_connector::Files =
            serde_json::from_value(serde_json::json!({
                "pyproject.toml": "[project]\n",
                "orders/__init__.py": "class Derivation: pass\n",
            }))
            .unwrap();
        // Listed files which failed to load are `null`.
        if let models::custom_connector::Files::Inline(files) = &mut files {
            files.insert("orders/helpers.py".to_string(), None);
        }
        let mut generated: BTreeMap<String, String> = [
            "flow_generated/main.py",
            "flow_generated/python/acmeCo/orders/__init__.py",
            "orders/helpers.py",
            // A listed file which exists is the user's.
            "pyproject.toml",
            "orders/__init__.py",
            // Neither owned nor listed.
            "pyrightconfig.json",
            "flow_generated/../pyproject.toml",
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
            "file:///project/acmeCo/flow_generated/main.py",
            "file:///project/acmeCo/flow_generated/python/acmeCo/orders/__init__.py",
            "file:///project/acmeCo/orders/helpers.py",
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

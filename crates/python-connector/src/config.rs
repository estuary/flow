//! Built configurations of a Python connector, and the layout of its project.
//!
//! Validation builds a Python task into one of two configuration shapes.
//! A `ProjectConfig` is the user's `config` plus the `SENTINEL` property,
//! which holds the task's `Project`. A `ModuleConfig` is the frozen shape of
//! a derivation having a legacy `module`, which the derivation connector
//! serves as it did before `ProjectConfig`.

use models::custom_connector::Spec;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Property of a `ProjectConfig` which holds its `Project`.
pub const SENTINEL: &str = "_python";

/// Manifest of a project, at its root.
pub const MANIFEST: &str = "pyproject.toml";

/// Lock of a project's dependencies, at its root. It's the user's to list in
/// `files`, and a project which doesn't resolves its dependencies anew for
/// each request. A listed lock which doesn't exist yet has no starter: the
/// user creates it with `uv lock`.
pub const LOCK: &str = "uv.lock";

/// Directories reserved within a project. Generated files are written under
/// `flow_generated/`, and the project's virtual environment is `.venv/`.
pub const RESERVED: &[&str] = &[".venv", "flow_generated"];

/// Dependency specifier of the Estuary CDK which Python connectors build
/// against. Starter and legacy projects list it in `dependencies`.
pub const CDK_DEPENDENCY: &str = "estuary-cdk>=0.3.0.dev79";

/// Entry file of a task, within its directory `dir`.
pub fn entry(dir: &str) -> String {
    format!("{dir}/__init__.py")
}

/// Starter manifest of a project whose task directory is `dir`. It has the
/// dependencies which generated types import. It resolves packages under a
/// one-day cooldown, and type-checks strictly. The user owns it once their
/// `files` list it.
pub fn starter_manifest(dir: &str) -> String {
    // A distribution name, which can't have other characters.
    let name: String = dir
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect();

    include_str!("pyproject.toml")
        .replace("PROJECT_NAME", &name)
        .replace("CDK_DEPENDENCY", CDK_DEPENDENCY)
}

/// Built configuration of a task's project.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ProjectConfig {
    #[serde(rename = "_python")]
    pub project: Project,
    /// The user's own configuration, which the connector hands to user code.
    #[serde(flatten)]
    pub config: BTreeMap<String, serde_json::Value>,
}

/// Project of a task: its files, and its declared spec.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct Project {
    /// Contents of the task's files, keyed on their path relative to the
    /// specification. A listed file which couldn't be fetched is `None`.
    #[serde(default)]
    pub files: BTreeMap<String, Option<String>>,
    /// Specification declared by the task's model.
    #[serde(default)]
    pub spec: Spec,
}

/// Built configuration of a derivation having a legacy `module`.
/// Its shape is frozen: it's that of models which predate `files`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ModuleConfig {
    /// Content of the derivation's module.
    pub module: String,
    /// Package dependencies of the module: a version specifier of each
    /// package name, such as `{"httpx": ">=0.27"}`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub dependencies: BTreeMap<String, String>,
}

impl Project {
    /// Listed files which failed to load (as they don't exist yet).
    pub fn missing(&self) -> Vec<&str> {
        self.files
            .iter()
            .filter(|(_, content)| content.is_none())
            .map(|(path, _)| path.as_str())
            .collect()
    }

    /// Does the project list a lock which exists?
    pub fn has_lock(&self) -> bool {
        matches!(self.files.get(LOCK), Some(Some(_)))
    }
}

#[cfg(test)]
mod test {
    use super::{ModuleConfig, ProjectConfig};

    #[test]
    fn configurations_are_parsed() {
        let current = br#"{
            "credentials": {"token": "secret"},
            "module": "a user property",
            "_python": {
                "files": {"a.py": "x = 1\n", "b.py": null, "uv.lock": "lock"},
                "spec": {"configSchema": {"type": "object"}}
            }
        }"#;
        let current: ProjectConfig = serde_json::from_slice(current).unwrap();

        insta::assert_debug_snapshot!((
            current.project.missing(),
            current.project.has_lock(),
            serde_json::to_string(&current).unwrap(),
            &current,
        ));

        let project = |config: &[u8]| {
            serde_json::from_slice::<ProjectConfig>(config).map_err(|err| err.to_string())
        };
        let module = |config: &[u8]| {
            serde_json::from_slice::<ModuleConfig>(config).map_err(|err| err.to_string())
        };
        insta::assert_debug_snapshot!(
            "other_shapes",
            (
                // Master-era shapes.
                module(br#"{"module": "MODULE", "dependencies": {}}"#),
                module(br#"{"module": "MODULE", "dependencies": {"httpx": ">=0.27"}}"#),
                module(br#"{"module": "MODULE", "other": true}"#),
                // Only a configuration having the sentinel is a ProjectConfig.
                project(br#"{"module": "MODULE"}"#),
                project(br#"{"_python": {"files": {"a.json": {"not": "text"}}}}"#),
            )
        );
    }
}

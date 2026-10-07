use super::{BuiltinSpec, ProjectFiles, RawValue};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A capture implemented by a user-authored Python connector,
/// written with the Estuary connector development kit (CDK).
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CapturePython {
    /// # Files of this Python project.
    /// The project's package is named for the final component of the capture
    /// name (`acmeCo/source-acme` has package `source_acme`), and must have
    /// `<package>/__init__.py` and `<package>/__main__.py` files.
    /// A `pyproject.toml` declaring the project's dependencies is also required.
    /// A `uv.lock` pins its dependencies exactly, and is otherwise resolved
    /// when the capture is published. `.venv/` and `flow_generated/` are reserved.
    #[serde(default, skip_serializing_if = "ProjectFiles::is_empty")]
    pub files: ProjectFiles,
    /// # Endpoint configuration of the connector.
    /// The configuration is described by `spec.configSchema`, and is parsed
    /// by the connector's endpoint configuration model.
    /// It may not have a `_python` property.
    #[serde(
        default = "super::project_files::empty_config",
        skip_serializing_if = "super::project_files::is_empty_config"
    )]
    pub config: RawValue,
    /// # Connector specification of this capture.
    /// Its schemas describe the capture's `config` and the resource
    /// configuration of each binding, and generate the `EndpointConfig`
    /// and `ResourceConfig` types of the connector.
    #[serde(default, skip_serializing_if = "BuiltinSpec::is_empty")]
    pub spec: BuiltinSpec,
}

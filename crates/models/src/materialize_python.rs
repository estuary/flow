use super::{RawValue, custom_connector};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A materialization implemented by a user-authored Python connector.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MaterializePython {
    /// # Files of this materialization's Python project.
    /// The materialization's directory is named for the final component of the
    /// materialization's name: `acmeCo/materialize-acme` has directory
    /// `materialize-acme/`. Its `materialize-acme/__init__.py` defines a
    /// `Materialization` class, which directly subclasses the generated
    /// `IMaterialization`.
    /// The project's `pyproject.toml` declares its dependencies. If you list a
    /// `uv.lock`, it pins them exactly. Create or update it with `uv lock`.
    /// Otherwise, they're resolved anew each time the materialization starts.
    #[serde(default, skip_serializing_if = "custom_connector::Files::is_empty")]
    pub files: custom_connector::Files,
    /// # Configuration of this materialization.
    /// An object described by `spec.configSchema`, which your `Materialization`
    /// class receives. Supply sensitive values through the `secrets` stanza.
    #[serde(
        default = "custom_connector::empty_config",
        skip_serializing_if = "custom_connector::is_empty_config"
    )]
    pub config: RawValue,
    /// # Connector specification of this materialization.
    /// Its `configSchema` describes the materialization's `config`, and its
    /// `resourceConfigSchema` describes the `resource` of each binding.
    #[serde(default, skip_serializing_if = "custom_connector::Spec::is_empty")]
    pub spec: custom_connector::Spec,
}

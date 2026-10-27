use super::{RawValue, custom_connector};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A capture implemented by a user-authored Python connector,
/// written with the Estuary connector development kit (CDK).
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CapturePython {
    /// # Files of this capture's Python project.
    /// The capture's directory is named for the final component of the
    /// capture's name: `acmeCo/source-acme` has directory `source-acme/`.
    /// Its `source-acme/__init__.py` defines a `Connector` class, which
    /// subclasses the CDK's `BaseCaptureConnector`.
    /// The project's `pyproject.toml` declares its dependencies. If you list a
    /// `uv.lock`, it pins them exactly. Create or update it with `uv lock`.
    /// Otherwise, they're resolved anew each time the capture starts.
    #[serde(default, skip_serializing_if = "custom_connector::Files::is_empty")]
    pub files: custom_connector::Files,
    /// # Configuration of this capture.
    /// An object described by `spec.configSchema`, which your `Connector`
    /// class receives. Supply sensitive values through the `secrets` stanza.
    #[serde(
        default = "custom_connector::empty_config",
        skip_serializing_if = "custom_connector::is_empty_config"
    )]
    pub config: RawValue,
    /// # Connector specification of this capture.
    /// Its `configSchema` describes the capture's `config`, and its
    /// `resourceConfigSchema` describes the `resource` of each binding.
    #[serde(default, skip_serializing_if = "custom_connector::Spec::is_empty")]
    pub spec: custom_connector::Spec,
}

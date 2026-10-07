use super::{RawValue, custom_connector};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DeriveUsingPython {
    /// # Deprecated: list the derivation's module in `files`.
    /// A relative URL of a Python module, or its inline content.
    /// Publishing moves it into `files` as the derivation's `__init__.py`.
    /// It can't be combined with `files`, `config`, or `spec`.
    // It's serialized only if present, so that `flowctl` can send a local
    // legacy model to the control plane, which migrates it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "DeriveUsingPython::module_schema")]
    pub module: Option<RawValue>,
    /// # Files of this derivation's Python project.
    /// The derivation's code lives in a directory named for the last component
    /// of the collection's name: `acmeCo/orders` has directory `orders/`.
    /// Its `orders/__init__.py` defines a `Derivation` class, which directly
    /// subclasses the generated `IDerivation`. The project's `pyproject.toml`
    /// declares its dependencies. If you list a `uv.lock`, it pins them exactly.
    /// Otherwise, they're resolved anew each time the derivation is published.
    #[serde(default, skip_serializing_if = "custom_connector::Files::is_empty")]
    pub files: custom_connector::Files,
    /// # Configuration of this derivation.
    /// An object described by `spec.configSchema`, which your `Derivation`
    /// class receives. Supply sensitive values through the `secrets` stanza.
    #[serde(
        default = "custom_connector::empty_config",
        skip_serializing_if = "custom_connector::is_empty_config"
    )]
    pub config: RawValue,
    /// # Connector specification of this derivation.
    /// Its `configSchema` describes the derivation's `config`, and its
    /// `resourceConfigSchema` describes the `lambda` of each transform.
    #[serde(default, skip_serializing_if = "custom_connector::Spec::is_empty")]
    pub spec: custom_connector::Spec,
    /// # Deprecated: declare dependencies in a `pyproject.toml` listed in `files`.
    /// They're allowed only alongside a deprecated `module`, and publishing
    /// writes them into the derivation's `pyproject.toml`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    #[schemars(schema_with = "DeriveUsingPython::dependencies_schema")]
    pub dependencies: BTreeMap<String, String>,
}

impl DeriveUsingPython {
    fn dependencies_schema(_: &mut schemars::generate::SchemaGenerator) -> schemars::Schema {
        from_value(json!({
            "type": "object",
            "additionalProperties": { "type": "string" },
            "deprecated": true,
        }))
        .unwrap()
    }

    fn module_schema(generator: &mut schemars::generate::SchemaGenerator) -> schemars::Schema {
        let url_schema = super::RelativeUrl::json_schema(generator);

        from_value(json!({
            "oneOf": [
                url_schema,
                {
                    "type": "string",
                    "contentMediaType": "text/x.python",
                }
            ],
            "deprecated": true,
        }))
        .unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::DeriveUsingPython;

    #[test]
    fn legacy_fields_round_trip_until_migrated() {
        // Models of master always serialized a `module` and `dependencies`.
        let mut legacy: DeriveUsingPython = serde_json::from_value(serde_json::json!({
            "module": "module.py",
            "dependencies": {"httpx": ">=0.27"},
        }))
        .unwrap();
        assert_eq!(
            serde_json::to_string(&legacy).unwrap(),
            r#"{"module":"module.py","dependencies":{"httpx":">=0.27"}}"#
        );

        // Once validation migrates them, they're gone.
        legacy.module = None;
        legacy.dependencies.clear();
        assert_eq!(serde_json::to_string(&legacy).unwrap(), r#"{}"#);
    }
}

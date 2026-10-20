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
    /// It's used as the derivation's `__init__.py`, beside a default
    /// `pyproject.toml`. It can't be combined with `files`, `config`, or `spec`.
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
    // Removed: dependencies are declared by a `pyproject.toml` listed in `files`.
    // Validation requires that it's empty.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    #[schemars(skip)]
    pub dependencies: BTreeMap<String, String>,
    // Removed: use `config`, with a `secrets` stanza for sensitive values.
    // Validation requires that it's empty.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    #[schemars(skip)]
    pub environment: BTreeMap<String, String>,
}

impl DeriveUsingPython {
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
    fn legacy_fields_round_trip() {
        // Models of master always serialized a `module` and `dependencies`,
        // and an `environment` only if it had one.
        let legacy: DeriveUsingPython = serde_json::from_value(serde_json::json!({
            "module": "module.py",
            "dependencies": {},
        }))
        .unwrap();
        assert_eq!(
            serde_json::to_string(&legacy).unwrap(),
            r#"{"module":"module.py"}"#
        );

        // Non-empty removed fields still parse, for validation to reject.
        let legacy: DeriveUsingPython = serde_json::from_value(serde_json::json!({
            "module": "module.py",
            "dependencies": {"httpx": ">=0.27"},
            "environment": {"KEY": "value"},
        }))
        .unwrap();
        assert_eq!(
            serde_json::to_string(&legacy).unwrap(),
            r#"{"module":"module.py","dependencies":{"httpx":">=0.27"},"environment":{"KEY":"value"}}"#
        );
    }
}

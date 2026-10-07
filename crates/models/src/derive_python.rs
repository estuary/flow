use super::{BuiltinSpec, ProjectFiles, RawValue};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DeriveUsingPython {
    /// # Deprecated: list the derivation's module in `files`.
    /// A relative URL of a Python module, or its inline content. Validation
    /// migrates it into `files`, so a validated model never has one.
    // It's serialized only if present, so that `flowctl` can send a local
    // legacy model to the control plane, which migrates it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "DeriveUsingPython::module_schema")]
    pub module: Option<RawValue>,
    /// # Files of this Python project.
    /// The derivation's directory is named for the final component of the
    /// derived collection's name (`acmeCo/orders` has directory `orders/`),
    /// and its `orders/__init__.py` exports a `Derivation` class which directly
    /// subclasses the generated `IDerivation`. A `pyproject.toml` declaring the
    /// project's dependencies is also required. A `uv.lock` pins its
    /// dependencies exactly, and is otherwise resolved when the derivation is
    /// published. `.venv/` and `flow_generated/` are reserved.
    #[serde(default, skip_serializing_if = "ProjectFiles::is_empty")]
    pub files: ProjectFiles,
    /// # Configuration of this derivation.
    /// The configuration is described by `spec.configSchema`, and is delivered
    /// to the Derivation class. It may not have a `_python` property.
    #[serde(
        default = "super::project_files::empty_config",
        skip_serializing_if = "super::project_files::is_empty_config"
    )]
    pub config: RawValue,
    /// # Connector specification of this derivation.
    /// Its schemas describe the derivation's `config` and the `lambda`
    /// of each transform, and generate the types delivered to the module.
    #[serde(default, skip_serializing_if = "BuiltinSpec::is_empty")]
    pub spec: BuiltinSpec,
    /// # Deprecated: declare dependencies in a `pyproject.toml` listed in `files`.
    /// Validation migrates the dependencies of a model having a `module` into
    /// its `pyproject.toml`, so a validated model never has them.
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

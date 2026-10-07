use super::{BuiltinSpec, ProjectFiles, RawValue};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DeriveUsingPython {
    /// # Python module implementing this derivation.
    /// Module is either a relative URL of a Python module file,
    /// or is an inline representation of a Python module.
    /// The module must have an exported Derivation class which
    /// extends the generated IDerivation base class.
    #[schemars(schema_with = "DeriveUsingPython::module_schema")]
    pub module: RawValue,
    /// # Additional files of this derivation.
    /// Files are placed alongside the module. A `pyproject.toml` declares the
    /// project's dependencies, and a default is used if it's absent.
    /// A `uv.lock` pins its dependencies exactly, and is otherwise resolved
    /// when the derivation is published.
    /// `module.py`, `module/__init__.py`, `main.py`, `.venv/`,
    /// and `flow_generated/` are reserved.
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
    /// # Removed: declare dependencies in a `pyproject.toml` listed in `files`.
    /// Models of derivations which predate `files` always serialized an empty
    /// `dependencies`, which is accepted (only) while empty. It's never
    /// serialized, so a re-published model drops it.
    #[serde(default, skip_serializing)]
    #[schemars(schema_with = "DeriveUsingPython::dependencies_schema")]
    pub dependencies: BTreeMap<String, String>,
}

impl DeriveUsingPython {
    fn dependencies_schema(_: &mut schemars::generate::SchemaGenerator) -> schemars::Schema {
        from_value(json!({
            "type": "object",
            "additionalProperties": { "type": "string" },
            "maxProperties": 0,
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
            ]
        }))
        .unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::DeriveUsingPython;

    #[test]
    fn legacy_empty_dependencies_are_accepted_and_dropped() {
        // Models of master always serialized an empty `dependencies`.
        let legacy: DeriveUsingPython = serde_json::from_value(serde_json::json!({
            "module": "module.py",
            "dependencies": {},
        }))
        .unwrap();
        assert!(legacy.dependencies.is_empty());
        assert_eq!(
            serde_json::to_string(&legacy).unwrap(),
            r#"{"module":"module.py"}"#
        );

        // A non-empty map parses, so that validation can explain the error.
        let non_empty: DeriveUsingPython = serde_json::from_value(serde_json::json!({
            "module": "module.py",
            "dependencies": {"httpx": ">=0.27"},
        }))
        .unwrap();
        assert_eq!(non_empty.dependencies.len(), 1);
        assert_eq!(
            serde_json::to_string(&non_empty).unwrap(),
            r#"{"module":"module.py"}"#
        );
    }
}

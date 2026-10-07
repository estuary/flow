use super::{BuiltinSpec, RawValue};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DeriveUsingTypescript {
    /// # TypeScript module implementing this derivation.
    /// Module is either a relative URL of a TypeScript module file,
    /// or is an inline representation of a Typescript module.
    /// The module must have an exported Derivation class which
    /// extends the generated IDerivation base class.
    #[schemars(schema_with = "DeriveUsingTypescript::module_schema")]
    pub module: RawValue,
    /// # Configuration of this derivation.
    /// The configuration is described by `spec.configSchema`, and is delivered
    /// to the Derivation class. It may not have a `_typescript` property.
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
}

impl DeriveUsingTypescript {
    fn module_schema(generator: &mut schemars::generate::SchemaGenerator) -> schemars::Schema {
        let url_schema = super::RelativeUrl::json_schema(generator);

        from_value(json!({
            "oneOf": [
                url_schema,
                {
                    "type": "string",
                    "contentMediaType": "text/x.typescript",
                }
            ]
        }))
        .unwrap()
    }
}

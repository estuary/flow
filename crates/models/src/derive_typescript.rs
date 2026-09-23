use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DeriveUsingTypescript {
    /// # TypeScript module implementing this derivation.
    /// Module is either a relative URL of a TypeScript module file,
    /// or is an inline representation of a Typescript module.
    /// The module must have a exported Derivation variable which
    /// is an instance implementing the corresponding Derivation
    /// interface.
    #[schemars(schema_with = "DeriveUsingTypescript::module_schema")]
    pub module: super::RawValue,
    /// # Environment variables made available to the TypeScript module.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub environment: BTreeMap<String, String>,
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

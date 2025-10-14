use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DeriveUsingPython {
    /// # Python module implementing this derivation.
    /// Module is either a relative URL of a Python module file,
    /// or is an inline representation of a Python module.
    /// The module must have an exported Derivation class which
    /// extends the generated IDerivation base class.
    #[schemars(schema_with = "DeriveUsingPython::module_schema")]
    pub module: super::RawValue,
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
            ]
        }))
        .unwrap()
    }
}

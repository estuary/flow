use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DeriveUsingSqlite {
    /// # Ordered migrations which are used to initialize the database.
    /// Migrations may be provided as an inline string,
    /// or as a relative URL to a file containing the migration SQL.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(schema_with = "DeriveUsingSqlite::migrations_schema")]
    pub migrations: Vec<super::RawValue>,
}

impl DeriveUsingSqlite {
    fn migrations_schema(generator: &mut schemars::generate::SchemaGenerator) -> schemars::Schema {
        let url_schema = super::RelativeUrl::json_schema(generator);

        from_value(json!({
            "type": "array",
            "items": {
                "oneOf": [
                    url_schema,
                    {
                        "type": "string",
                        "contentMediaType": "text/x.sql",
                    }
                ]
            }
        }))
        .unwrap()
    }
}

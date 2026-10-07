use super::{BuiltinSpec, ProjectFiles, RawValue};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DeriveUsingTypescript {
    /// # Deprecated: list the derivation's module in `files`.
    /// A relative URL of a TypeScript module, or its inline content.
    /// Validation migrates it into `files`, so a validated model never has one.
    // It's serialized only if present, so that `flowctl` can send a local
    // legacy model to the control plane, which migrates it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "DeriveUsingTypescript::module_schema")]
    pub module: Option<RawValue>,
    /// # Files of this TypeScript project.
    /// The derivation's directory is named for the final component of the
    /// derived collection's name (`acmeCo/orders` has directory `orders/`),
    /// and its `orders/mod.ts` exports a `Derivation` class which extends the
    /// generated `IDerivation`. A `deno.json` is also required, which maps
    /// `flow/` to `./flow_generated/typescript/` so that modules may import
    /// their generated types. `flow_generated/` is reserved.
    #[serde(default, skip_serializing_if = "ProjectFiles::is_empty")]
    pub files: ProjectFiles,
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
            ],
            "deprecated": true,
        }))
        .unwrap()
    }
}

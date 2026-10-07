use super::{RawValue, custom_connector};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DeriveUsingTypescript {
    /// # Deprecated: list the derivation's module in `files`.
    /// A relative URL of a TypeScript module, or its inline content.
    /// Publishing moves it into `files` as the derivation's `mod.ts`.
    /// It can't be combined with `files`, `config`, or `spec`.
    // It's serialized only if present, so that `flowctl` can send a local
    // legacy model to the control plane, which migrates it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "DeriveUsingTypescript::module_schema")]
    pub module: Option<RawValue>,
    /// # Files of this derivation's TypeScript project.
    /// The derivation's code lives in a directory named for the last component
    /// of the collection's name: `acmeCo/orders` has directory `orders/`.
    /// Its `orders/mod.ts` exports a `Derivation` class, which extends the
    /// generated `IDerivation`. The project's `deno.json` maps `flow/` to
    /// `./flow_generated/typescript/`, from which your code imports its
    /// generated types. If you list a `deno.lock`, it pins dependencies exactly.
    /// Otherwise, they're resolved anew each time the derivation is published.
    #[serde(default, skip_serializing_if = "custom_connector::Files::is_empty")]
    pub files: custom_connector::Files,
    /// # Configuration of this derivation.
    /// An object described by `spec.configSchema`, which your `Derivation`
    /// class receives. Supply sensitive values through the `secrets` stanza.
    /// It can't be encrypted with `sops`, or have a `_typescript` property.
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

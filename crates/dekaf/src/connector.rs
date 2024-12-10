use anyhow::{bail, Context};
use proto_flow::materialize;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema, Copy)]
#[serde(rename_all = "snake_case")]
pub enum DeletionMode {
    // Handles deletions using the regular Kafka upsert envelope, where a deletion
    // is represented by a record containing the key that was deleted, and a null value.
    Kafka,
    // Handles deletions by passing through the full deletion document as it exists
    // in the source collection, as well as including a new field `_meta/is_deleted`
    // which is defined as the number `1` on deletions, and `0` otherwise.
    #[serde(rename = "cdc")]
    CDC,
}

impl Default for DeletionMode {
    fn default() -> Self {
        Self::Kafka
    }
}

/// Configures the behavior of a whole dekaf task
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
pub struct DekafConfig {
    /// Whether or not to expose topic names in a strictly Kafka-compliant format
    /// for systems that require it. Off by default.
    pub strict_topic_names: bool,
    /// The password that will authenticate Kafka consumers to this task.
    // TODO(jshearer): Uncomment when schemars 1.0 is out and we upgrade
    // #[schemars(extend("secret" = true))]
    #[schemars(schema_with = "token_secret")]
    pub token: String,
    /// How to handle deletion events. "Default" emits them as regular Kafka
    /// tombstones with null values, and "Header" emits then as a kafka document
    /// with empty string and `_is_deleted` header set to `1`. Setting this value
    /// will also cause all other non-deletions to have an `_is_deleted` header of `0`.
    pub deletions: DeletionMode,
}

/// Configures a particular binding in a Dekaf-type materialization
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
pub struct DekafResourceConfig {
    /// The exposed name of the topic that maps to this binding. This
    /// will be exposed through the Kafka metadata/discovery APIs.
    #[schemars(schema_with = "collection_name")]
    pub topic_name: String,
}

fn collection_name(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    serde_json::from_value(serde_json::json!({
        "x-collection-name": true,
    }))
    .unwrap()
}

fn token_secret(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    serde_json::from_value(serde_json::json!({
        "title": "Dekaf Auth Token",
        "secret": true,
    }))
    .unwrap()
}

pub async fn unary_materialize(
    request: materialize::Request,
) -> anyhow::Result<materialize::Response> {
    use proto_flow::materialize::response::validated;
    if let Some(mut validate) = request.validate {
        serde_json::de::from_str::<DekafConfig>(&validate.config_json)
            .context("validating endpoint config")?;

        // Largely copied from crates/validation/src/noop.rs
        let validated_bindings = std::mem::take(&mut validate.bindings)
            .into_iter()
            .enumerate()
            .map(|(i, b)| {
                let resource_path = vec![format!("binding-{}", i)];
                let constraints = b
                    .collection
                    .expect("collection must exist")
                    .projections
                    .into_iter()
                    .map(|proj| {
                        (
                            proj.field,
                            validated::Constraint {
                                r#type: validated::constraint::Type::FieldOptional as i32,
                                reason: "Dekaf allows everything for now".to_string(),
                            },
                        )
                    })
                    .collect::<BTreeMap<_, _>>();
                validated::Binding {
                    constraints,
                    resource_path,
                    delta_updates: false,
                }
            })
            .collect::<Vec<_>>();

        return Ok(materialize::Response {
            validated: Some(materialize::response::Validated {
                bindings: validated_bindings,
            }),
            ..Default::default()
        });
    } else {
        bail!("Unhandled request type")
    }
}

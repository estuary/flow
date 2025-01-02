use anyhow::{bail, Context};
use futures::future::try_join_all;
use models::RawValue;
use proto_flow::{flow::materialization_spec, materialize};
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
    /// The password that will authenticate Kafka consumers to this task.
    // TODO(jshearer): Uncomment when schemars 1.0 is out and we upgrade
    // #[schemars(extend("secret" = true))]
    #[schemars(schema_with = "token_secret")]
    pub token: String,
    /// How to handle deletion events. "Default" emits them as regular Kafka
    /// tombstones with null values, and "Header" emits then as a kafka document
    /// with empty string and `_is_deleted` header set to `1`. Setting this value
    /// will also cause all other non-deletions to have an `_is_deleted` header of `0`.
    #[serde(default)]
    #[schemars(title = "Deletion Mode")]
    pub deletions: DeletionMode,
    /// Whether or not to expose topic names in a strictly Kafka-compliant format
    /// for systems that require it. Off by default.
    #[serde(default)]
    #[schemars(title = "Strict Topic Names")]
    pub strict_topic_names: bool,
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
        "type": "string"
    }))
    .unwrap()
}

fn token_secret(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    serde_json::from_value(serde_json::json!({
        "title": "Auth Token",
        "secret": true,
        "type": "string",
        "order": 0
    }))
    .unwrap()
}

pub async fn unary_materialize(
    request: materialize::Request,
) -> anyhow::Result<materialize::Response> {
    if let Some(_) = request.spec {
        let config_schema = schemars::schema_for!(DekafConfig);
        let resource_schema = schemars::schema_for!(DekafResourceConfig);

        return Ok(materialize::Response {
            spec: Some(materialize::response::Spec {
                protocol: 3032023,
                config_schema_json: serde_json::to_string(&config_schema)?,
                resource_config_schema_json: serde_json::to_string(&resource_schema)?,
                documentation_url:
                    "https://docs.estuary.dev/guides/dekaf_reading_collections_from_kafka"
                        .to_string(),
                oauth2: None,
            }),
            ..Default::default()
        });
    } else if let Some(mut validate) = request.validate {
        use proto_flow::materialize::response::validated;
        match materialization_spec::ConnectorType::try_from(validate.connector_type)? {
            materialization_spec::ConnectorType::Dekaf => {}
            other => bail!("invalid connector type: {}", other.as_str_name()),
        };

        let parsed_outer_config =
            serde_json::from_str::<models::DekafConfig>(&validate.config_json)
                .context("validating dekaf config")?;

        let _parsed_inner_config = serde_json::from_value::<DekafConfig>(
            unseal::decrypt_sops(&parsed_outer_config.config)
                .await
                .context(format!(
                    "decrypting dekaf endpoint config for variant {}",
                    parsed_outer_config.variant
                ))?
                .to_value(),
        )
        .context(format!(
            "validating dekaf endpoint config for variant {}",
            parsed_outer_config.variant
        ))?;

        let validated_bindings = try_join_all(
            std::mem::take(&mut validate.bindings)
                .into_iter()
                .map(|binding| {
                    let resource_config_str = binding.resource_config_json.clone();
                    let endpoint_config = parsed_outer_config.clone();
                    async move {
                        let resource_config = serde_json::from_value::<DekafResourceConfig>(
                            unseal::decrypt_sops(&RawValue::from_str(&resource_config_str)?)
                                .await
                                .context(format!(
                                    "decrypting dekaf resource config for variant {}",
                                    endpoint_config.variant
                                ))?
                                .to_value(),
                        )
                        .context(format!(
                            "validating dekaf resource config for variant {}",
                            endpoint_config.variant
                        ))?;

                        let collection = binding.collection.expect("collection must exist");

                        let mut constraints = BTreeMap::new();

                        for proj in collection.projections {
                            constraints.insert(
                                proj.field,
                                validated::Constraint {
                                    r#type: validated::constraint::Type::LocationRecommended as i32,
                                    reason: "All data are recommended for Dekaf materialization"
                                        .to_string(),
                                },
                            );
                        }

                        Ok::<proto_flow::materialize::response::validated::Binding, anyhow::Error>(
                            validated::Binding {
                                constraints,
                                resource_path: vec![resource_config.topic_name],
                                delta_updates: false,
                            },
                        )
                    }
                }),
        )
        .await?;

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

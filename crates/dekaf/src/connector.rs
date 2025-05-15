use anyhow::{bail, Context};
use futures::StreamExt;
use proto_flow::{
    flow::{self, materialization_spec},
    materialize::{self, response::validated::constraint},
};
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

pub fn connector<R>(
    mut request_rx: R,
) -> impl futures::Stream<Item = anyhow::Result<materialize::Response>> + Send
where
    R: futures::Stream<Item = materialize::Request> + Send + Unpin + 'static,
{
    coroutines::try_coroutine(|mut co| async move {
        while let Some(request) = request_rx.next().await {
            let response = if let Some(_) = request.spec {
                let config_schema = schemars::schema_for!(DekafConfig);
                let resource_schema = schemars::schema_for!(DekafResourceConfig);

                materialize::Response {
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
                }
            } else if let Some(mut validate) = request.validate {
                use proto_flow::materialize::response::validated;
                match materialization_spec::ConnectorType::try_from(validate.connector_type)? {
                    materialization_spec::ConnectorType::Dekaf => {}
                    other => bail!("invalid connector type: {}", other.as_str_name()),
                };

                let parsed_outer_config =
                    serde_json::from_str::<models::DekafConfig>(&validate.config_json)
                        .context("validating dekaf config")?;

                let parsed_inner_config = serde_json::from_value::<DekafConfig>(
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

                let validated_bindings = std::mem::take(&mut validate.bindings)
                    .into_iter()
                    .map(|binding| {
                        let resource_config = serde_json::from_str::<DekafResourceConfig>(
                            binding.resource_config_json.as_str(),
                        )
                        .context(format!(
                            "validating dekaf resource config for variant {}",
                            parsed_outer_config.variant.clone()
                        ))?;

                        let constraints = binding
                            .collection
                            .context("collection must exist")?
                            .projections
                            .iter()
                            .map(|projection| {
                                (
                                    projection.field.clone(),
                                    constraint_for_projection(
                                        &projection,
                                        &resource_config,
                                        &parsed_inner_config,
                                        validate.last_materialization.as_ref(),
                                    ),
                                )
                            })
                            .collect::<BTreeMap<_, _>>();

                        Ok::<proto_flow::materialize::response::validated::Binding, anyhow::Error>(
                            validated::Binding {
                                constraints,
                                resource_path: vec![resource_config.topic_name],
                                delta_updates: false,
                            },
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                materialize::Response {
                    validated: Some(materialize::response::Validated {
                        bindings: validated_bindings,
                    }),
                    ..Default::default()
                }
            } else {
                bail!("Unhandled request type")
            };

            () = co.yield_(response).await;
        }
        Ok(())
    })
}

// Largely lifted from materialize-kafka
// TODO(jshearer): Expose this logic somewhere that materialize-kafka can use it
fn constraint_for_projection(
    projection: &flow::Projection,
    resource_config: &DekafResourceConfig,
    endpoint_config: &DekafConfig,
    last_spec: Option<&flow::MaterializationSpec>,
) -> materialize::response::validated::Constraint {
    let mut constraint = if !avro::AVRO_FIELD_RE.is_match(&projection.field) {
        // We shouldn't continue to recommend this field if it contains illegal characters,
        // even if it was previously selected.
        return materialize::response::validated::Constraint {
            r#type: constraint::Type::FieldForbidden.into(),
            reason: format!(
                "Field name {} contains characters forbidden in AVRO schemas. It must match the regex '{}'. If you still want to include this field, you can create a projection for this field on the source collection with a name that matches the regex. See https://go.estuary.dev/projections for more information.",
                projection.field,
                avro::AVRO_FIELD_RE.as_str()
            ),
        };
    } else if projection.is_primary_key {
        materialize::response::validated::Constraint {
            r#type: constraint::Type::LocationRecommended.into(),
            reason: "Primary key locations should usually be materialized".to_string(),
        }
    } else if projection.ptr.is_empty() {
        materialize::response::validated::Constraint {
            r#type: constraint::Type::FieldOptional.into(),
            reason: "The root document may be materialized".to_string(),
        }
    } else if projection.field == "_is_deleted"
        && matches!(endpoint_config.deletions, DeletionMode::CDC)
    {
        materialize::response::validated::Constraint {
            r#type: constraint::Type::FieldForbidden.into(),
            reason: "Cannot materialize input data to '_is_deleted' when using CDC deletions mode as it will be generated by Dekaf".to_string(),
        }
    } else if projection
        .inference
        .as_ref()
        .map(|inf| inf.types.len() == 1 && inf.types[0] == "null")
        .unwrap_or(false)
    {
        materialize::response::validated::Constraint {
            r#type: constraint::Type::FieldForbidden.into(),
            reason: "Cannot materialize null-only location".to_string(),
        }
    } else if projection.field == "flow_published_at"
        || !projection.ptr.strip_prefix("/").unwrap().contains("/")
    {
        materialize::response::validated::Constraint {
            r#type: constraint::Type::LocationRecommended.into(),
            reason: "Top-level locations should usually be materialized".to_string(),
        }
    } else {
        materialize::response::validated::Constraint {
            r#type: constraint::Type::FieldOptional.into(),
            reason: "This field may be materialized".to_string(),
        }
    };

    // Continue to recommend previously selected fields even if they would have
    // otherwise been optional.
    if let Some(last_spec) = last_spec {
        let last_binding = last_spec
            .bindings
            .iter()
            .find(|b| b.resource_path[0] == resource_config.topic_name);

        if let Some(last_binding) = last_binding {
            let last_field_selection = last_binding
                .field_selection
                .as_ref()
                .expect("prior binding must have field selection");

            if projection.ptr.is_empty() && !last_field_selection.document.is_empty() {
                constraint = materialize::response::validated::Constraint {
                    r#type: constraint::Type::LocationRecommended.into(),
                    reason: "This location is the document of the current materialization"
                        .to_string(),
                }
            } else if last_field_selection
                .values
                .binary_search(&projection.field)
                .is_ok()
            {
                constraint = materialize::response::validated::Constraint {
                    r#type: constraint::Type::LocationRecommended.into(),
                    reason: "This location is part of the current materialization".to_string(),
                }
            }
        };
    };

    constraint
}

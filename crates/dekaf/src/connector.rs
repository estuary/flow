use anyhow::{bail, Context};
use proto_flow::materialize;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Configures the behavior of a whole dekaf task
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct DekafConfig {
    /// Whether or not to expose topic names in a strictly Kafka-compliant format
    /// for systems that require it. Off by default.
    pub strict_topic_names: bool,
}

/// Configures a particular binding in a Dekaf-type materialization
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct DekafResourceConfig {
    /// The exposed name of the topic that maps to this binding. This
    /// will be exposed through the Kafka metadata/discovery APIs.
    pub topic_name: String,
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

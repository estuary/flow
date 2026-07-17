use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

/// # Triggers
/// Webhook triggers that fire upon materialization transaction completion.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Triggers {
    /// # Minimum interval between trigger deliveries.
    /// A burst of transactions within the interval collapses into a single
    /// delivery covering the full span.
    #[schemars(
        schema_with = "super::duration_schema",
        extend("nonsensitive" = true)
    )]
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    pub interval: Option<Duration>,
    /// # Trigger Configurations
    /// Webhook triggers to fire when new data is materialized,
    /// keyed on a user-assigned trigger name.
    pub config: BTreeMap<super::Token, TriggerConfig>,
    // SOPS encryption metadata (internal, not user-facing).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(skip)]
    pub sops: Option<super::RawValue>,
}

/// Configuration for a webhook trigger that fires when new data is
/// materialized to the endpoint.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TriggerConfig {
    /// # URL of the webhook endpoint.
    pub url: String,
    /// # HTTP method to use for the webhook request.
    #[serde(default)]
    #[schemars(extend("default" = "POST"))]
    pub method: HttpMethod,
    /// # HTTP headers to include in the webhook request.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    #[schemars(schema_with = "secret_string_map_schema")]
    pub headers: BTreeMap<String, String>,
    /// # Handlebars template for the JSON payload body.
    #[schemars(extend("nonsensitive" = true))]
    pub payload_template: String,
    /// # Request timeout for each delivery attempt.
    /// The task is failed if all attempts are exhausted without a successful delivery.
    #[serde(default = "default_timeout", with = "humantime_serde")]
    #[schemars(
        schema_with = "super::duration_schema",
        extend("default" = "30s", "nonsensitive" = true)
    )]
    pub timeout: Duration,
    /// # Maximum number of delivery attempts (including the initial attempt).
    /// The task is failed if all attempts are exhausted without a successful delivery.
    #[serde(default = "default_max_attempts")]
    #[schemars(extend("default" = 3_u32, "nonsensitive" = true))]
    pub max_attempts: u32,
}

/// HTTP method for the webhook request.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
pub enum HttpMethod {
    POST,
    PUT,
    PATCH,
}

impl Default for HttpMethod {
    fn default() -> Self {
        HttpMethod::POST
    }
}

fn default_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_max_attempts() -> u32 {
    3
}

/// Template variables for webhook trigger rendering, computed from transaction state.
/// Persisted to RocksDB during StartCommit for at-least-once delivery guarantees.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TriggerVariables {
    pub collection_names: Vec<String>,
    pub connector_image: String,
    pub materialization_name: String,
    pub flow_published_at_min: String,
    pub flow_published_at_max: String,
    pub run_id: String,
}

impl TriggerVariables {
    /// Return an instance with placeholder values for template validation.
    pub fn placeholder() -> Self {
        Self {
            collection_names: vec!["acmeCo/example/collection".to_string()],
            connector_image: "ghcr.io/estuary/materialize-example:v1".to_string(),
            materialization_name: "acmeCo/example/materialization".to_string(),
            flow_published_at_min: "2024-01-01T00:00:00Z".to_string(),
            flow_published_at_max: "2024-01-01T00:01:00Z".to_string(),
            run_id: "2024-01-01T00:00:00.000Z".to_string(),
        }
    }

    /// Merge `other` into `self`, widening this window to cover both. Used to
    /// collapse a burst of debounced transactions into a single delivery.
    pub fn merge(&mut self, other: &TriggerVariables) {
        for name in &other.collection_names {
            if !self.collection_names.contains(name) {
                self.collection_names.push(name.clone());
            }
        }
        self.collection_names.sort();

        if other.flow_published_at_min < self.flow_published_at_min {
            self.flow_published_at_min = other.flow_published_at_min.clone();
        }
        if other.flow_published_at_max > self.flow_published_at_max {
            self.flow_published_at_max = other.flow_published_at_max.clone();
        }
    }
}

/// Render a single payload template string with the given context.
/// Uses strict mode (unknown variables are errors) and no HTML escaping.
/// The context is a JSON value that should contain the trigger variables
/// and optionally a `headers` map from the trigger config.
pub fn render_payload_template(
    template: &str,
    context: &serde_json::Value,
) -> anyhow::Result<String> {
    let mut hb = handlebars::Handlebars::new();
    hb.set_strict_mode(true);
    hb.register_escape_fn(handlebars::no_escape);

    hb.register_template_string("t", template)?;
    Ok(hb.render("t", context)?)
}

/// Build a template rendering context from trigger variables and a trigger's
/// headers. Headers are exposed as `{{headers.Name}}` in templates, allowing
/// secret values to be injected into payloads.
pub fn build_template_context(
    variables: &TriggerVariables,
    headers: &std::collections::BTreeMap<String, String>,
) -> serde_json::Value {
    let mut context = serde_json::to_value(variables).expect("TriggerVariables must serialize");
    context["headers"] = serde_json::to_value(headers).expect("headers must serialize");
    context
}

fn secret_string_map_schema(_gen: &mut schemars::generate::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "object",
        "additionalProperties": {
            "type": "string",
            "secret": true
        }
    })
}

/// Returns the JSON Schema for `Triggers` as a `serde_json::Value`.
/// Used by the encryption layer, which encrypts values annotated
/// `"secret": true` (header values), and by `sops.overlay` validation,
/// which permits post-encryption modification only of locations
/// annotated `"nonsensitive": true`.
pub fn triggers_schema() -> serde_json::Value {
    let settings = schemars::generate::SchemaSettings::draft2019_09();
    let schema = schemars::SchemaGenerator::new(settings).root_schema_for::<Triggers>();
    serde_json::to_value(schema).unwrap()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn trigger_config_schema_snapshot() {
        let settings = schemars::generate::SchemaSettings::draft2019_09();
        let schema = schemars::SchemaGenerator::new(settings).root_schema_for::<Triggers>();
        insta::assert_json_snapshot!("trigger-config-schema", schema);
    }

    #[test]
    fn trigger_config_round_trip() {
        let triggers: Triggers = serde_json::from_value(serde_json::json!({
            "interval": "30m",
            "config": {
                "onCommit": {
                    "url": "https://example.com/webhook",
                    "headers": {"Authorization": "Bearer secret"},
                    "payloadTemplate": "{}",
                    "timeout": "45s",
                    "maxAttempts": 5,
                },
            },
        }))
        .unwrap();

        insta::assert_json_snapshot!("round-trip", serde_json::to_value(&triggers).unwrap());
    }
}

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// # Triggers
/// Webhook triggers that fire upon materialization transaction completion.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Triggers {
    /// # Trigger Configurations
    /// List of webhook triggers to fire when new data is materialized.
    pub config: Vec<TriggerConfig>,
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
    pub payload_template: String,
    /// # Request timeout in seconds.
    #[serde(default = "default_timeout_secs")]
    #[schemars(extend("default" = 30_u32))]
    pub timeout_secs: u32,
    /// # Maximum number of delivery attempts (including the initial attempt).
    #[serde(default = "default_max_attempts")]
    #[schemars(extend("default" = 3_u32))]
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

fn default_timeout_secs() -> u32 {
    30
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
    pub flow_run_id: String,
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
            flow_run_id: "00000000-0000-0000-0000-000000000000".to_string(),
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

/// Original values of HMAC-excluded fields for a single trigger config,
/// captured by `strip_hmac_excluded_fields` and restored by
/// `restore_hmac_excluded_fields`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HmacExcludedOriginals {
    pub payload_template: String,
    pub timeout_secs: u32,
    pub max_attempts: u32,
}

/// Replace HMAC-excluded fields in each trigger config with placeholder values,
/// returning the original values. Call `restore_hmac_excluded_fields` after the
/// SOPS operation to put the originals back.
pub fn strip_hmac_excluded_fields(triggers: &mut Triggers) -> Vec<HmacExcludedOriginals> {
    triggers
        .config
        .iter_mut()
        .map(|config| HmacExcludedOriginals {
            payload_template: std::mem::take(&mut config.payload_template),
            timeout_secs: std::mem::replace(&mut config.timeout_secs, 0),
            max_attempts: std::mem::replace(&mut config.max_attempts, 0),
        })
        .collect()
}

/// Restore original values for HMAC-excluded fields after a SOPS operation.
pub fn restore_hmac_excluded_fields(
    triggers: &mut Triggers,
    originals: Vec<HmacExcludedOriginals>,
) {
    for (config, orig) in triggers.config.iter_mut().zip(originals) {
        config.payload_template = orig.payload_template;
        config.timeout_secs = orig.timeout_secs;
        config.max_attempts = orig.max_attempts;
    }
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
/// Used by the encryption layer; carries `"secret": true` annotations
/// on header values so that SOPS encrypts only those values.
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
    fn strip_and_restore_hmac_excluded_fields() {
        let mut triggers = Triggers {
            config: vec![TriggerConfig {
                url: "https://example.com/webhook".to_string(),
                method: HttpMethod::POST,
                headers: [("Authorization".to_string(), "Bearer secret".to_string())]
                    .into_iter()
                    .collect(),
                payload_template: "my template".to_string(),
                timeout_secs: 45,
                max_attempts: 5,
            }],
            sops: None,
        };
        let original = triggers.clone();

        let originals = strip_hmac_excluded_fields(&mut triggers);
        insta::assert_json_snapshot!("stripped", serde_json::to_value(&triggers).unwrap());
        restore_hmac_excluded_fields(&mut triggers, originals);
        assert_eq!(triggers, original);
    }
}

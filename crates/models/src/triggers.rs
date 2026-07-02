use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

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
    /// # Request timeout for each delivery attempt.
    /// The task is failed if all attempts are exhausted without a successful delivery.
    #[serde(default = "default_timeout", with = "humantime_serde")]
    #[schemars(schema_with = "super::duration_schema", extend("default" = "30s"))]
    pub timeout: Duration,
    /// # Maximum number of delivery attempts (including the initial attempt).
    /// The task is failed if all attempts are exhausted without a successful delivery.
    #[serde(default = "default_max_attempts")]
    #[schemars(extend("default" = 3_u32))]
    pub max_attempts: u32,
    /// # Minimum interval between deliveries for this trigger.
    #[schemars(schema_with = "super::duration_schema")]
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    pub interval: Option<Duration>,
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

/// Persisted trigger parameters, tolerating both wire formats that have been
/// written to the runtime's durable "trigger-params" key:
///
/// - `PerConfig`: the current format — an accumulated window per trigger,
///   written by runtimes with per-config debounce.
/// - `Single`: the legacy format — one window, fired by every configured
///   trigger. Written by the V1 runtime and by pre-debounce V2 runtimes.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum PersistedTriggerParams {
    PerConfig(BTreeMap<String, TriggerVariables>),
    Single(TriggerVariables),
}

impl PersistedTriggerParams {
    /// Reduce to a single window spanning all pending deliveries (V1 style).
    /// Returns None if there is nothing pending.
    pub fn into_merged(self) -> Option<TriggerVariables> {
        match self {
            Self::Single(variables) => Some(variables),
            Self::PerConfig(map) => map.into_values().reduce(|mut acc, v| {
                acc.merge(&v);
                acc
            }),
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
    pub timeout: Duration,
    pub max_attempts: u32,
    pub interval: Option<Duration>,
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
            timeout: std::mem::replace(&mut config.timeout, Duration::ZERO),
            max_attempts: std::mem::replace(&mut config.max_attempts, 0),
            interval: std::mem::take(&mut config.interval),
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
        config.timeout = orig.timeout;
        config.max_attempts = orig.max_attempts;
        config.interval = orig.interval;
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

    // Both persisted wire formats decode unambiguously, and a per-config map
    // merges down to the single window the V1 runtime fires with.
    #[test]
    fn persisted_trigger_params_decodes_both_formats() {
        let single = TriggerVariables::placeholder();
        let legacy_blob = serde_json::to_vec(&single).unwrap();
        let decoded: PersistedTriggerParams = serde_json::from_slice(&legacy_blob).unwrap();
        assert_eq!(decoded, PersistedTriggerParams::Single(single.clone()));
        assert_eq!(decoded_merged(&legacy_blob), Some(single.clone()));

        let mut early = single.clone();
        early.flow_published_at_min = "2023-01-01T00:00:00Z".to_string();
        early.collection_names = vec!["acmeCo/other".to_string()];
        let map: BTreeMap<String, TriggerVariables> = [
            ("POST https://a".to_string(), single.clone()),
            ("POST https://b".to_string(), early),
        ]
        .into();
        let map_blob = serde_json::to_vec(&map).unwrap();
        let decoded: PersistedTriggerParams = serde_json::from_slice(&map_blob).unwrap();
        assert_eq!(decoded, PersistedTriggerParams::PerConfig(map));

        let merged = decoded_merged(&map_blob).unwrap();
        assert_eq!(merged.flow_published_at_min, "2023-01-01T00:00:00Z");
        assert_eq!(
            merged.collection_names,
            vec![
                "acmeCo/example/collection".to_string(),
                "acmeCo/other".to_string()
            ],
        );

        assert_eq!(decoded_merged(b"{}"), None);
    }

    fn decoded_merged(blob: &[u8]) -> Option<TriggerVariables> {
        serde_json::from_slice::<PersistedTriggerParams>(blob)
            .unwrap()
            .into_merged()
    }

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
                timeout: Duration::from_secs(45),
                max_attempts: 5,
                interval: Some(Duration::from_secs(1800)),
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

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

/// The current, name-keyed representation of a task's trigger configurations.
type TriggerConfigMap = BTreeMap<super::Token, TriggerConfig>;

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
    #[schemars(with = "TriggerConfigMap")]
    pub config: TriggerConfigs,
    // SOPS encryption metadata (internal, not user-facing).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(skip)]
    pub sops: Option<super::RawValue>,
}

/// The set of a task's trigger configurations.
///
/// This (de)serializes in whichever shape it holds. New configs are always
/// [`TriggerConfigs::Map`] (name-keyed). [`TriggerConfigs::Legacy`] exists only
/// so that a pre-overlay sealed config — which stored `config` as an ordered
/// list — round-trips byte-faithfully through the control plane, preserving its
/// SOPS structure and MAC until it is re-published in the current form.
///
/// The `Triggers` JSON Schema advertises only the map form (via the field's
/// `schemars(with = ...)`), so the legacy shape is accepted on input but never
/// offered to users.
#[derive(Serialize, Clone, Debug, PartialEq)]
#[serde(untagged)]
pub enum TriggerConfigs {
    /// Current form: triggers keyed on a user-assigned name.
    Map(TriggerConfigMap),
    /// Legacy pre-overlay form: an ordered list. See the type-level docs and
    /// [`LegacyTriggers`]. Remove once no legacy configs remain.
    Legacy(Vec<TriggerConfig>),
}

impl<'de> Deserialize<'de> for TriggerConfigs {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Dispatch on the concrete JSON shape rather than using an untagged
        // enum, so that a malformed map still yields a precise field-level error
        // (e.g. "unknown field") instead of "didn't match any variant".
        let value = serde_json::Value::deserialize(deserializer)?;
        let result = match value {
            serde_json::Value::Array(_) => {
                serde_json::from_value(value).map(TriggerConfigs::Legacy)
            }
            _ => serde_json::from_value(value).map(TriggerConfigs::Map),
        };
        result.map_err(serde::de::Error::custom)
    }
}

impl TriggerConfigs {
    pub fn is_empty(&self) -> bool {
        match self {
            TriggerConfigs::Map(m) => m.is_empty(),
            TriggerConfigs::Legacy(v) => v.is_empty(),
        }
    }

    /// Normalize into the current name-keyed form, synthesizing `trigger{N}`
    /// names by position for a legacy list.
    pub fn into_map(self) -> TriggerConfigMap {
        match self {
            TriggerConfigs::Map(m) => m,
            TriggerConfigs::Legacy(v) => legacy_list_into_map(v),
        }
    }

    /// Borrow as `(name, config)` pairs, synthesizing `trigger{N}` names for a
    /// legacy list. Used by validation, which runs before normalization.
    pub fn iter_named(&self) -> Vec<(String, &TriggerConfig)> {
        match self {
            TriggerConfigs::Map(m) => m.iter().map(|(k, v)| (k.to_string(), v)).collect(),
            TriggerConfigs::Legacy(v) => v
                .iter()
                .enumerate()
                .map(|(i, c)| (format!("trigger{i}"), c))
                .collect(),
        }
    }
}

/// Build a `Map` config, so existing call sites and tests can `.collect()` a
/// map of triggers directly into the field.
impl FromIterator<(super::Token, TriggerConfig)> for TriggerConfigs {
    fn from_iter<I: IntoIterator<Item = (super::Token, TriggerConfig)>>(iter: I) -> Self {
        TriggerConfigs::Map(iter.into_iter().collect())
    }
}

fn legacy_list_into_map(list: Vec<TriggerConfig>) -> TriggerConfigMap {
    list.into_iter()
        .enumerate()
        .map(|(i, cfg)| (super::Token::new(format!("trigger{i}")), cfg))
        .collect()
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

// ---------------------------------------------------------------------------
// Backwards compatibility for pre-overlay ("legacy") trigger configs.
//
// Before the `sops.overlay` migration, trigger configs stored `config` as an
// ordered list, and excluded the tunable fields (`payloadTemplate`, `timeout`,
// `maxAttempts`, and the top-level `interval`) from the SOPS MAC by replacing
// them with placeholder values before encryption and restoring them afterward.
//
// These types and helpers let such configs continue to decrypt until they are
// re-published in the current format. They mirror the exact strip/restore
// behavior of the old encryption path, so the reconstructed document matches
// what the MAC was computed over. Remove this section once no legacy configs
// remain (confirmed via a census of live specs).
// ---------------------------------------------------------------------------

/// A pre-overlay trigger config, whose `config` is an ordered list. Deserialized
/// from the raw sealed document so its SOPS structure is preserved for decryption.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LegacyTriggers {
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    pub interval: Option<Duration>,
    pub config: Vec<TriggerConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sops: Option<super::RawValue>,
}

impl LegacyTriggers {
    /// Convert a decrypted legacy config into the current [`Triggers`] form,
    /// synthesizing `trigger{N}` names for each list entry by position.
    pub fn into_triggers(self) -> Triggers {
        Triggers {
            interval: self.interval,
            config: TriggerConfigs::Map(legacy_list_into_map(self.config)),
            sops: self.sops,
        }
    }
}

/// Original values of the fields excluded from the legacy SOPS MAC, captured by
/// [`strip_hmac_excluded_fields`] and restored by [`restore_hmac_excluded_fields`].
#[derive(Debug, Clone)]
pub struct HmacExcludedOriginals {
    pub payload_template: String,
    pub timeout: Duration,
    pub max_attempts: u32,
    pub interval: Option<Duration>,
}

/// Replace the MAC-excluded fields with the placeholder values the legacy
/// encryption path used, returning the originals. This reconstructs the exact
/// document the SOPS MAC was computed over, so `decrypt_sops` verifies.
pub fn strip_hmac_excluded_fields(triggers: &mut LegacyTriggers) -> Vec<HmacExcludedOriginals> {
    let interval = std::mem::take(&mut triggers.interval);
    triggers
        .config
        .iter_mut()
        .map(|config| HmacExcludedOriginals {
            payload_template: std::mem::take(&mut config.payload_template),
            timeout: std::mem::replace(&mut config.timeout, Duration::ZERO),
            max_attempts: std::mem::replace(&mut config.max_attempts, 0),
            interval,
        })
        .collect()
}

/// Restore the original MAC-excluded field values after `decrypt_sops`.
pub fn restore_hmac_excluded_fields(
    triggers: &mut LegacyTriggers,
    originals: Vec<HmacExcludedOriginals>,
) {
    triggers.interval = originals.first().and_then(|orig| orig.interval);
    for (config, orig) in triggers.config.iter_mut().zip(originals) {
        config.payload_template = orig.payload_template;
        config.timeout = orig.timeout;
        config.max_attempts = orig.max_attempts;
    }
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

    #[test]
    fn legacy_list_config_round_trips_and_normalizes() {
        // A pre-overlay (list-shaped) config must round-trip byte-faithfully as a
        // list, so the sealed SOPS document it came from is preserved for decryption.
        let json = serde_json::json!({
            "config": [
                {"url": "https://a.example.com", "method": "POST", "payloadTemplate": "{}", "timeout": "30s", "maxAttempts": 3},
                {"url": "https://b.example.com", "method": "POST", "payloadTemplate": "{}", "timeout": "30s", "maxAttempts": 3},
            ],
        });
        let triggers: Triggers = serde_json::from_value(json.clone()).unwrap();
        assert!(matches!(triggers.config, TriggerConfigs::Legacy(_)));

        // The re-serialized form is still a list at the same paths - this is what
        // keeps the sealed SOPS structure (header ciphertext paths + MAC) intact.
        let reserialized = serde_json::to_value(&triggers).unwrap();
        assert!(reserialized["config"].is_array());
        assert_eq!(reserialized, json);

        // Normalization synthesizes positional `trigger{N}` names.
        let names: Vec<String> = triggers
            .config
            .into_map()
            .into_keys()
            .map(|t| t.to_string())
            .collect();
        assert_eq!(names, vec!["trigger0".to_string(), "trigger1".to_string()]);
    }

    #[test]
    fn map_config_deserializes_as_map() {
        let triggers: Triggers = serde_json::from_value(serde_json::json!({
            "config": {"onCommit": {"url": "https://example.com", "payloadTemplate": "{}"}},
        }))
        .unwrap();
        assert!(matches!(triggers.config, TriggerConfigs::Map(_)));
    }
}

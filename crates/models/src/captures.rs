use super::{Collection, ConnectorConfig, RawValue, ShardTemplate};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Duration;

/// A Capture binds an external system and target (e.x., a SQL table or cloud storage bucket)
/// from which data should be continuously captured, with a Flow collection into that captured
/// data is ingested. Multiple Captures may be bound to a single collection, but only one
/// capture may exist for a given endpoint and target.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CaptureDef {
    /// # Continuously keep the collection spec and schema up-to-date
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_discover: Option<AutoDiscover>,
    /// # Endpoint to capture from.
    pub endpoint: CaptureEndpoint,
    /// # Bound collections to capture from the endpoint.
    pub bindings: Vec<CaptureBinding>,
    /// # Interval of time between invocations of the capture.
    /// Configured intervals are applicable only to connectors which are
    /// unable to continuously tail their source, and which instead produce
    /// a current quantity of output and then exit. Flow will start the
    /// connector again after the given interval of time has passed.
    ///
    /// Intervals are relative to the start of an invocation and not its completion.
    /// For example, if the interval is five minutes, and an invocation of the
    /// capture finishes after two minutes, then the next invocation will be started
    /// after three additional minutes.
    #[serde(
        default = "CaptureDef::default_interval",
        with = "humantime_serde",
        skip_serializing_if = "CaptureDef::is_default_interval"
    )]
    #[schemars(schema_with = "super::duration_schema")]
    pub interval: Duration,
    /// # Template for shards of this capture task.
    #[serde(default, skip_serializing_if = "ShardTemplate::is_empty")]
    pub shards: ShardTemplate,
}

/// Settings to determine how Flow should stay abreast of ongoing changes to collections and schemas.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AutoDiscover {
    /// Automatically add new bindings discovered from the source.
    #[serde(default)]
    add_new_bindings: bool,
    /// Whether to automatically evolve collections and/or materialization
    /// bindings to handle changes to collections that would otherwise be
    /// incompatible with the existing catalog.
    #[serde(default)]
    evolve_incompatible_collections: bool,
}

/// An endpoint from which Flow will capture.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum CaptureEndpoint {
    /// # A Connector.
    Connector(ConnectorConfig),
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(example = "CaptureBinding::example")]
pub struct CaptureBinding {
    /// # Endpoint resource to capture from.
    pub resource: RawValue,
    /// # Name of the collection to capture into.
    // Note(johnny): If we need to add details about how data is written to a
    // target, we should turn this into a Target enum as has already been done
    // with Source (used by Materialization & Derive).
    pub target: Target,
}

impl CaptureDef {
    pub fn default_interval() -> Duration {
        Duration::from_secs(300) // 5 minutes.
    }
    fn is_default_interval(interval: &Duration) -> bool {
        *interval == Self::default_interval()
    }

    pub fn example() -> Self {
        Self {
            auto_discover: Some(AutoDiscover {
                add_new_bindings: true,
                evolve_incompatible_collections: true,
            }),
            endpoint: CaptureEndpoint::Connector(ConnectorConfig::example()),
            bindings: vec![CaptureBinding::example()],
            interval: Self::default_interval(),
            shards: ShardTemplate::default(),
        }
    }
}

impl CaptureBinding {
    pub fn example() -> Self {
        Self {
            resource: serde_json::from_value(json!({"stream": "a_stream"})).unwrap(),
            target: Some(Collection::new("target/collection")).into(),
        }
    }
}

/// Target represents the destination side of a capture binding. It can be
/// either the name of a Flow collection (e.g. "acmeCo/foo/bar") or `null` to
/// disable the binding.
#[derive(Debug, Clone)]
pub struct Target(Option<Collection>);

impl schemars::JsonSchema for Target {
    fn schema_name() -> String {
        "Target".to_string()
    }

    fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        let collection_schema = gen.subschema_for::<Collection>();
        serde_json::from_value(json!({
            "oneOf": [
                collection_schema,
                {
                    "title": "Disabled binding",
                    "description": "a null target indicates that the binding is disabled and will be ignored at runtime",
                    "type": "null",
                }
            ]
        })).unwrap()
    }

    fn is_referenceable() -> bool {
        false
    }
}

impl std::ops::Deref for Target {
    type Target = Option<Collection>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Option<Collection>> for Target {
    fn from(value: Option<Collection>) -> Self {
        Target(value)
    }
}

impl<'de> serde::Deserialize<'de> for Target {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value: Option<Collection> = Deserialize::deserialize(deserializer)?;
        Ok(value.into())
    }
}

impl serde::Serialize for Target {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.0.as_ref() {
            Some(collection) => collection.serialize(serializer),
            // 'unit' is represented as an explicit null in serde_json.
            None => serializer.serialize_unit(),
        }
    }
}

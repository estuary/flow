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
    pub target: Option<Collection>,
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
            target: Some(Collection::new("target/collection")),
        }
    }
}

use protocol::flow::EndpointType;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Duration;

use super::{Collection, ConnectorConfig, Object, ShardTemplate};

/// A Capture binds an external system and target (e.x., a SQL table or cloud storage bucket)
/// from which data should be continuously captured, with a Flow collection into that captured
/// data is ingested. Multiple Captures may be bound to a single collection, but only one
/// capture may exist for a given endpoint and target.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
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

impl CaptureDef {
    pub fn default_interval() -> Duration {
        Duration::from_secs(300) // 5 minutes.
    }

    pub fn example() -> Self {
        Self {
            endpoint: CaptureEndpoint::Connector(ConnectorConfig::example()),
            bindings: vec![CaptureBinding::example()],
            interval: Self::default_interval(),
            shards: ShardTemplate::default(),
        }
    }

    fn is_default_interval(interval: &Duration) -> bool {
        *interval == Self::default_interval()
    }
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(example = "CaptureBinding::example")]
pub struct CaptureBinding {
    /// # Endpoint resource to capture from.
    pub resource: Object,
    /// # Name of the collection to capture into.
    pub target: Collection,
}

impl CaptureBinding {
    pub fn example() -> Self {
        Self {
            resource: json!({"stream": "a_stream"}).as_object().unwrap().clone(),
            target: Collection::new("target/collection"),
        }
    }
}

/// An Endpoint connector used for Flow captures.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum CaptureEndpoint {
    /// # A Connector.
    #[serde(alias = "airbyteSource")]
    Connector(ConnectorConfig),
    /// # A push ingestion.
    Ingest(IngestConfig),
}

impl CaptureEndpoint {
    pub fn endpoint_type(&self) -> EndpointType {
        match self {
            Self::Connector(_) => EndpointType::AirbyteSource,
            Self::Ingest(_) => EndpointType::Ingest,
        }
    }
}

/// Ingest source specification.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct IngestConfig {}
